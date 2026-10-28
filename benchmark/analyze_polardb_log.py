#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析 PolarDB PASE 时序日志，按 backend PID 提取各步骤耗时，并据此按
“SQL 类型 / 是否使用查询计划缓存 / 是否使用查询结果缓存”分组汇总，
得到与 pgvector 分析同等口径的 parse/analyze/plan/exec/vector_in 耗时统计表(CSV)。
分组结构(与 polardb 配置一致, 完全以日志时序为准): 每类 SQL 类型一段,
段内 16 个 PID = 8 组(预热,查询各一), cache 档位 0..3, 档位内 result_cache 先 off 后 on:
      i  cache  res_cache
      0  0(direct)        0
      1  0(direct)        1
      2  1(custom)        0
      3  1(custom)        1
      4  2(generic)       0
      5  2(generic)       1
      6  3(generic+pase)  0
      7  3(generic+pase)  1

输入日志格式假设(由 PASE 服务端日志配置决定, 字段顺序/措辞变化需同步改正则):
- 行首 "<日期> <时间>[.<小数秒>] <用户名> [PID]"; 组1 为时间戳字符串, 组2 为 PID。
  与 pgvector 脚本不同, 本脚本按 PID 数值排序划分批次, 不依赖时间戳比较。
- pase_stmt: command <n> parse <p> ms, analyze <a> ms, plan <pl> ms,
  execute_plus_setup <e> ms —— 一条语句的四段耗时, 单位 ms。
- pase_in: [mode <m>, ]dim <d>, parse+convert <v> ms —— 向量入参解析+转换, ms。
- pase_vec_f4 / pase_text_i_i: dim <d>, convert <v> ms —— 数组/文本路径的转换, ms,
  与 pase_in 一起计入 vin(与 pgvector 的 vi 列口径对齐)。
- pase_execute_query eval_params=<e>ms get_cached_plan=<g>ms name=<stmt>。
- PREPARE polardb_ps_<sql_type>_<rand6>; plan cache MISS|HIT (<custom|generic> plan);
  result cache HIT|STORE。

输出 CSV:
- 默认 results/csv/polardb_log_analysis-<table>.csv(--out 可覆盖), 每张日志只输出
  一张表(按日志组合在结果 CSV 中命中最多者判定)。
- 一行 = 一个 (sql_type, parse_mode) 段内的一个 (cache, res_cache) 组合。
- 字段与单位:
    # 行号; PID(warm/s) 预热/查询两个 PID; sql_type; cache(0..3);
    res_cache(0/1); parse_mode; proto(simple/prep);
    exec#(w/s) 预热/查询段的 pase_stmt 条数; parse_ms/analyze_ms/plan_ms/
    exec_ms 为毫秒均值, 预热/查询以 "w/s" 成对输出; eval_ms/gcp_ms 为
    execute_query 的毫秒均值对; vi#(w/s) 向量转换次数; vi_ms(w/s) 毫秒均值对;
    hv#(w/s) 与 hv_ms(w/s) 当前恒为 "0|0"/"-|-"(PolarDB 日志无 halfvec 埋点);
    est_QPS 由查询段 (steps-1)/test_duration 估算(次/秒); JSON_RPS 固定 "-";
    csv_RPS 取自结果 CSV rps 列(次/秒); run_date 预热 PID 的日期(YYYY-MM-DD)。
- 所有 *ms(w/s) 均值均沿用 avg(): 每段去掉第一条样本后平均(去首次热路径)。

命令行:
    python analyze_polardb_log.py [--log 日志路径] [--csv 结果 CSV] [--out 输出 CSV]

已知局限:
- 完全依赖"每 (sql_type, parse_mode) 块 = 连续 16 个 PID = 8 组合 × (预热, 查询)"
  的固定布局; 若 client 的 PID 顺序/预热策略改变, 块长与配对会整体错位。
- cache=0 的组合不走命名 PREPARE, 无法打标签; 只能以块内首个带标签 PID 向前
  回退 4 个 PID 来凑块, 因此非常依赖日志完整且同一时刻只有一个测试在跑。
- 只检查 PID 数值顺序(假定 PID 随连接单调递增, 且不回收/回绕), 跨多轮历史日志
  时按块逐个输出, 不做时间批次切分。
- 正则不匹配的行静默丢弃(只做 "pase" 子串预过滤), 不做格式校验告警。
- 结果 CSV 缺失/读不到时 combos 为空, 日志仍会被解析, 但表名退化为 "polardb",
  csv_RPS 全为 "-"。
"""
import re
import os
import sys
import json
import csv
import argparse
from collections import OrderedDict
from datetime import datetime

# ---- 路径常量 ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))       # benchmark/ 目录
RESULTS_DIR = os.path.join(BASE_DIR, "results", "csv")     # 结果 CSV 与输出目录
LOG_PATH = "/home/polar/tmp_polardb_pg_17_primary/log/postgresql_error.log"  # PASE 日志
CSV_PATH = os.path.join(RESULTS_DIR, "pgvector_benchmark_results.csv")       # 结果 CSV
OUT_DIR = RESULTS_DIR                                       # 汇总 CSV 输出目录

# ---- 日志行正则 ----
# 行首时间戳 + 用户名 + [PID]; 组1 = 时间戳(可带小数秒, 仅记录不比较),
# 组2 = backend PID(本脚本的分组与排序键)。
RE_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+\S+\s+\[(\d+)\]")
# pase_stmt 四段耗时; 组1..4 = parse/analyze/plan/execute_plus_setup, 单位 ms。
# command 后的编号被忽略(同一条语句可能打印多次, 不去重)。
RE_STMT = re.compile(
    r"pase_stmt: command \d+ parse ([\d.]+) ms, analyze ([\d.]+) ms, "
    r"plan ([\d.]+) ms, execute_plus_setup ([\d.]+) ms")
# pase_in: 组1 = 可选 parse mode 名(无 mode 前缀时为 None), 组2 = parse+convert ms。
RE_IN = re.compile(r"pase_in: (?:mode (\w+), )?dim \d+, parse\+convert ([\d.]+) ms")
# 数组/文本向量路径的转换耗时; 组1 = convert ms, 与 pase_in 一起并入 vin。
RE_VI_ARRAY = re.compile(r"pase_(?:vec_f4|text_i_i): dim \d+, convert ([\d.]+) ms")
# execute_query: 组1 = eval_params ms, 组2 = get_cached_plan ms, 组3 = 语句名。
RE_EQ = re.compile(
    r"pase_execute_query eval_params=([\d.]+)ms get_cached_plan=([\d.]+)ms name=(\S+)")
# PREPARE 语句名; 组1 = 完整名字(polardb_ps_<sql_type>_<rand6>)。
RE_PREP = re.compile(r"PREPARE (polardb_ps_[A-Za-z0-9_]+)")
# 计划缓存事件; 组1 = 计划类型(custom/generic), MISS/HIT 本身不区分(只记集合)。
RE_PC = re.compile(r"plan cache (?:MISS|HIT) \(([a-z]+) plan")
# 结果缓存被使用(命中或写入)即视为该 PID 开启了 result cache。
RE_RC_ON = re.compile(r"result cache (?:HIT|STORE)")


def stmt_sql_type(name):
    """从 PREPARE 名提取 sql_type 基名: polardb_ps_<sql_type>_<rand6> -> <sql_type>。

    参数: name —— PREPARE 语句名(RE_PREP 抓到的完整名字)。
    返回: 剥掉 "polardb_ps_" 前缀与最后一个 "_" 之后的随机后缀; 前缀不符/为空
          返回 None。用 rsplit("_", 1) 只剥最后一段, 保留 sql_type 自带的下划线;
          若名字本就没有随机后缀, 会误剥掉最后一段。
    """
    if not name or not name.startswith("polardb_ps_"):
        return None
    return name[len("polardb_ps_"):].rsplit("_", 1)[0]


def avg(values):
    """去掉第一条样本后求均值(去首次热路径); 空/单样本返回 None。

    参数: values —— 按发生顺序排列的耗时列表, 单位 ms。
    返回: float 均值(ms) 或 None。该口径与 pgvector 分析脚本保持一致。
    """
    if len(values) <= 1:
        return None
    return sum(values[1:]) / float(len(values) - 1)


def fmt(v, nd=3):
    """格式化数值: None -> "-"; 否则保留 nd 位小数(默认 3, 耗时单位 ms)。"""
    return "-" if v is None else "%.*f" % (nd, v)


def parse_log(log_path):
    """解析整份 PASE 日志, 按 backend PID 聚合各类事件。

    参数: log_path —— PASE 服务端日志路径(postgresql_error.log)。
    返回: OrderedDict {str(pid): rec}, 按 PID 首次出现顺序:
        ts_first/ts_last 首末行时间戳(字符串, 不参与比较);
        steps  [(parse, analyze, plan, execute_plus_setup) ms, ...];
        vin    向量解析/转换耗时列表(ms), 合并 pase_in 与 pase_vec_f4/text_i_i;
        eq     [(eval_params_ms, get_cached_plan_ms, stmt_name), ...];
        prep   出现过的 PREPARE 名集合; pc 出现过的计划类型集合(custom/generic);
        rc_on  是否见过 result cache HIT/STORE; modes 出现过的 parse mode 集合。
    异常: 文件读取失败向上抛 OSError; 单行格式不匹配时静默跳过(不计数、不告警)。
    """
    data = OrderedDict()
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            # 廉价子串预过滤: 非 PASE 埋点行直接跳过
            if "pase" not in line:
                continue
            m = RE_TS.match(line)
            if not m:
                continue              # 无行首时间戳/PID 的续行无法归属, 丢弃
            ts, pid = m.group(1), m.group(2)
            rec = data.get(pid)
            if rec is None:
                # 每个 PID 一份累加器; set 类字段只关心“出现过什么”, 天然去重
                rec = data[pid] = {
                    "ts_first": ts, "ts_last": ts,
                    "steps": [], "vin": [], "eq": [],
                    "prep": set(), "pc": set(), "rc_on": False,
                    "modes": set(),
                }
            rec["ts_last"] = ts
            # 计时事件: 同一行只归入一类, 保证 steps/vin/eq 计数不重复
            if "pase_stmt:" in line:
                sm = RE_STMT.search(line)
                if sm:
                    # (parse, analyze, plan, execute_plus_setup), 单位 ms
                    rec["steps"].append(tuple(float(x) for x in sm.groups()))
            elif "pase_in:" in line:
                im = RE_IN.search(line)
                if im:
                    rec["vin"].append(float(im.group(2)))   # parse+convert ms
                    if im.group(1):
                        # 记录该 PID 用了哪种解析模式; 后面用“单一 mode”识别测量 PID
                        rec["modes"].add(im.group(1))
            elif "pase_execute_query" in line:
                em = RE_EQ.search(line)
                if em:
                    rec["eq"].append((float(em.group(1)), float(em.group(2)), em.group(3)))
            elif "pase_vec_f4:" in line or "pase_text_i_i:" in line:
                am = RE_VI_ARRAY.search(line)
                if am:
                    # 数组/文本转换耗时同样计入 vin(与 pgvector 的 vi 同口径)
                    rec["vin"].append(float(am.group(1)))
            # 以下三个标记可能出现在任意 PASE 行上, 因此不放进上面的 elif 链
            pm = RE_PREP.search(line)
            if pm:
                rec["prep"].add(pm.group(1))      # 完整语句名, 供切块时取 sql_type
            pcm = RE_PC.search(line)
            if pcm:
                rec["pc"].add(pcm.group(1))       # custom / generic, MISS/HIT 不区分
            if RE_RC_ON.search(line):
                rec["rc_on"] = True               # 任一结果缓存事件即视为启用
    return data


def read_combos(csv_path):
    """读取 benchmark 结果 CSV, 返回可按 (表, sql, cache, rc, parse_mode) 匹配的记录。

    参数: csv_path —— 结果 CSV 路径(通常 pgvector_benchmark_results.csv)。
    返回: list[dict], 每行一个:
        table(experiment_name, 空则 "polardb") / sql_type / cache / rc /
        ts(timestamp 字符串) / parse_mode(空则 "-") / qps(取自 rps, 次/秒) /
        test_duration(原始字符串, 秒)。
    过滤: sql_type 为空、cache/rc 不能转 int、cache 不在 0..3 或 rc 不在 0/1 的行
          静默跳过(汇总行/脏行); 文件不存在时告警并返回空列表。
    """
    combos = []
    if not os.path.exists(csv_path):
        print("[WARN] 结果 CSV 不存在: %s" % csv_path, file=sys.stderr)
        return combos
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f):
            sql = (r.get("sql_type") or "").strip()
            if not sql:
                continue
            try:
                cache = int((r.get("use_cache") or "").strip())
                rc = int((r.get("use_query_cache") or "").strip())
            except (TypeError, ValueError):
                continue
            # cache 档位须落在 0..3, rc 0/1, 与日志分组结构一致
            if cache < 0 or cache > 3 or rc not in (0, 1):
                continue
            combos.append({
                "table": (r.get("experiment_name") or "").strip() or "polardb",
                "sql_type": sql,
                "cache": cache,
                "rc": rc,
                "ts": (r.get("timestamp") or "").strip(),
                # 日志里 cache=0 的 pase_in 可能没有 mode 前缀, 用 "-" 统一占位
                "parse_mode": (r.get("vector_in_parse_mode") or "").strip() or "-",
                "qps": (r.get("rps") or "").strip(),
                "test_duration": (r.get("test_duration") or "").strip(),
            })
    return combos


def _num(v):
    """尽力转 float; None/空串/非数字等失败情况静默返回 None(不告警)。"""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# config.json 路径(在函数之间定义, 不影响使用; 供 expected_groups/主流程读取)
CONFIG_PATH = os.path.join(BASE_DIR, "configurations", "config.json")


def _parse_ts(s):
    """解析日志时间戳字符串; 仅用于诊断/辅助, 失败静默返回 None。

    参数: s —— 形如 "YYYY-MM-DD HH:MM:SS[.ffffff]" 的字符串。
    返回: datetime 或 None。依次尝试带/不带微秒两种格式(当前主流程不使用该值)。
    """
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def expected_groups(cfg_path, table_name):
    """从 config.json 折出该表一轮完整测试预期的组数(engine=polardb), 无法确定则返回 None。

    参数: cfg_path —— config.json 路径; table_name —— 表名(engine 须为 polardb)。
    返回: int 组数 = cache 档数 × result_cache 档数 × parse_mode 数 × sql_type 数
          × search_number(即日志预期行数); 文件不可读、表不存在或任一维度为空时
          返回 None(此时主流程跳过一致性校验, 只打印实际组数)。
    说明: 组数直接按 upload_params 各键的列表长度相乘; 这些键应配置为 list。
    """
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return None
    for item in cfg:
        if item.get("name") != table_name or item.get("engine") != "polardb":
            continue
        up = item.get("upload_params") or {}
        n_cache = len(up.get("enable_query_plan_cache", []) or [])
        n_rc = len(up.get("use_query_cache", []) or [])
        n_mode = len(up.get("vector_in_parse_mode", []) or [])
        n_sql = len(up.get("sql_type", []) or [])
        n_search = int(up.get("search_number", 1) or 1)
        # 任一维度缺失就无法确定组数, 放弃校验
        if not (n_cache and n_rc and n_mode and n_sql):
            return None
        return n_cache * n_rc * n_mode * n_sql * n_search
    return None


def config_test_duration(cfg_path, table_name):
    """从 config.json 取该表一轮完整测试的 test_duration(秒)。

    参数: cfg_path —— config.json 路径; table_name —— 表名(engine 须为 polardb)。
    返回: float 秒; 表不存在/字段缺失/无法转 float/config 读取失败时返回 None。
    说明: 只读 search_params.test_duration(要求是 dict; 列表写法取不到则返回 None)。
    """
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return None
    for item in cfg:
        if item.get("name") != table_name or item.get("engine") != "polardb":
            continue
        v = (item.get("search_params") or {}).get("test_duration")
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    return None


def main():
    """命令行入口: 解析日志 -> 按 16 PID 块切分 -> 与结果 CSV 匹配 -> 写单表 CSV。

    返回: 0 成功; 1 日志不存在或没有任何包含 steps 的测量 PID / 无有效结果。
    异常: 日志读取失败向上抛 OSError; config/结果 CSV 的异常在各自函数内吞掉。
    """
    ap = argparse.ArgumentParser(description="汇总 PolarDB PASE 各步骤耗时并分组统计。")
    ap.add_argument("--log", default=LOG_PATH, help="pase 服务端日志路径")
    ap.add_argument("--csv", default=CSV_PATH,
                    help="benchmark 结果 CSV(提供真实 table_name/plan_cache/result_cache/qps)")
    ap.add_argument("--out", default=None,
                    help="汇总 CSV 输出路径(默认 results/csv/polardb_log_analysis-<table>.csv)")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print("[ERR] 日志不存在: %s" % args.log, file=sys.stderr)
        return 1

    pid_data = parse_log(args.log)
    combos = read_combos(args.csv)
    print("log: %s  pids=%d  combos(csv)=%d"
          % (args.log, len(pid_data), len(combos)), file=sys.stderr)

    def step_pair(pid, idx):
        """取某 PID 的 steps 第 idx 段耗时均值(ms, 去首样本); pid 为空返回 None。

        idx: 0=parse, 1=analyze, 2=plan, 3=execute_plus_setup。
        """
        if pid is None:
            return None
        rec = pid_data[pid]
        return avg([s[idx] for s in rec["steps"]])

    def vin_pair(pid):
        """取某 PID 的向量解析/转换耗时均值(ms, 去首样本); pid 为空返回 None。"""
        if pid is None:
            return None
        return avg(pid_data[pid]["vin"])

    def eq_ms_pair(pid, idx):
        """取某 PID 的 execute_query 第 idx 项均值(ms); idx 0=eval_params 1=gcp。"""
        if pid is None:
            return None
        return avg([e[idx] for e in pid_data[pid]["eq"]])

    def cell(w, s):
        """把预热/查询两个均值拼成 "w|s" 单元格; None 显示为 "-"。"""
        return "%s|%s" % (fmt(w), fmt(s))

    # 表头: *_ms(w/s) 均为毫秒均值对; exec#/vi#/hv# 为条数对; est_QPS/csv_RPS 次/秒
    header = [
        "#", "PID(warm/s)", "sql_type", "cache", "res_cache", "parse_mode",
        "proto", "exec#(w/s)", "parse_ms(w/s)", "analyze_ms(w/s)",
        "plan_ms(w/s)", "exec_ms(w/s)", "eval_ms(w/s)", "gcp_ms(w/s)",
        "vi#(w/s)", "vi_ms(w/s)", "hv#(w/s)", "hv_ms(w/s)",
        "est_QPS", "JSON_RPS", "csv_RPS", "run_date",
    ]

    # ---- 以 PID(backend 会话身份)分割轮次/段, 不依赖时间 ----
    # PostgreSQL 的 backend PID 随连接创建单调递增, 因此 PID 号序 == 会话/执行先后序。
    # 只有“单 parse_mode + 单一 sql_type 的 PREPARE”的 PID 才是测量 PID; 预热/设置
    # (mode 为空或多个, 或没有 sql) 的 PID 不作为测量, 同时天然当作不同 sql 段的边界。
    def _pid_mode(pid):
        """PID 的 parse mode: 恰好一种时返回该值, 零种/多种(预热/设置连接)返回 None。"""
        modes = pid_data[pid].get("modes") or set()
        return next(iter(modes)) if len(modes) == 1 else None

    def _pid_sqls(pid):
        """PID 中出现过的 sql_type 集合(排序去重); 无 PREPARE 名时为空列表。"""
        return sorted({stmt_sql_type(x) for x in pid_data[pid].get("prep") or () if stmt_sql_type(x)})

    # 只有打印过 pase_stmt 的 PID 才算测量 PID; 其余(纯连接/设置)不参与分块
    order = [p for p in pid_data if pid_data[p].get("steps")]
    order.sort(key=lambda p: int(p))          # 以 PID 号(会话顺序)排, 不用时间

    # 设置环境变量 POLDB_DEBUG=1 可打印前 44 个 PID 的标签, 用于排查块切分错位
    if os.environ.get("POLDB_DEBUG"):
        for n in range(min(len(order), 44)):
            _p = order[n]
            _r = pid_data[_p]
            print("DBG %-4d pid=%s mode=%s sqls=%s steps=%d ts=%s"
                  % (n, _p, sorted(_r.get("modes") or set()),
                     _pid_sqls(_p), len(_r["steps"]), _r["ts_first"]), file=sys.stderr)

    # 每 (sql, parse_mode) 块 = 连续 16 个 PID = 8 组合 × (预热, 查询)。
    # 其中 plan_cache=0 的 2 组(块内前 4 个 PID)不走命名 PREPARE、无 sql 标签,
    # 故以“带 (单 mode + 单 sql) 标签的 PID”定位块首, 再向前补 4 个无标签 PID 凑满 16。
    BLOCK_PID = 16
    _olabeled = []                            # (idx, mode, sql): 用于定位块的无歧义 PID
    for idx, p in enumerate(order):
        mode = _pid_mode(p)
        sqls = _pid_sqls(p)
        # 仅“单一 parse_mode + 单一 sql_type”的 PID 才能无歧义地定位块
        if mode is not None and len(sqls) == 1:
            _olabeled.append((idx, mode, sqls[0]))
    segments = []
    i = 0
    # 把 _olabeled 中相邻且 (mode, sql) 相同的 PID 归为同一块
    while i < len(_olabeled):
        mode, sql = _olabeled[i][1], _olabeled[i][2]
        j = i
        while j < len(_olabeled) and _olabeled[j][1] == mode and _olabeled[j][2] == sql:
            j += 1
        start_idx = _olabeled[i][0]           # 块内首个带标签 PID(idx0..3 为 cache=0 的无标签 PID)
        # 向前补 4 个无标签 PID(cache=0 的两组), 再向后取到 16 个
        lo = max(0, start_idx - 4)
        hi = min(len(order), start_idx + BLOCK_PID - 4)
        pids16 = order[lo:hi]
        segments.append([mode, sql, pids16])
        i = j
    # mode 首次出现/变化顺序, 用于人工核对块布局是否符合预期
    _mo = []
    for s in segments:
        if not _mo or _mo[-1] != s[0]:
            _mo.append(s[0])
    print("  标签PID=%d/%d, 完整块=%d(每块应%d PID; mode 顺序: %s)"
          % (len(_olabeled), len(order), len(segments), BLOCK_PID, " -> ".join(_mo)),
          file=sys.stderr)
    for _si, _s in enumerate(segments):
        print("    seg%02d mode=%-12s sql=%-20s n=%d pids=%s"
              % (_si, _s[0], _s[1], len(_s[2]), _s[2]), file=sys.stderr)

    # 结果 CSV 中 polardb 组合的匹配(按 日志 parse_mode + sql_type 基名 + cache + rc):
    # 优先 config 的 test_duration, 其次最新时间戳, 命中与日志同轮的那条。
    def combo_lookup(sql, cache, rc, mode, combos, config_td):
        """在结果 CSV 中找与(日志 sql/cache/rc/mode)一致的记录。

        优先取 test_duration 与 config 一致的那一条(真正对应本轮 60s 测试的 qps),
        其次取时间戳最新的一条, 避免命中历史旧轮/冒烟轮。

        参数: sql/cache/rc/mode —— 日志侧键; combos —— 该表的 CSV 记录池;
              config_td —— config 中的 test_duration(秒, 可能为 None)。
        返回: 命中的 CSV dict 或 None。sql_type 允许 CSV 里带后缀(前缀匹配)。
        """
        cand = []
        for c in combos:
            if c["parse_mode"] != mode or c["cache"] != cache or c["rc"] != rc:
                continue
            # sql_type 精确相等, 或 CSV 侧为 "<sql>_xxx" 变体(缓存名可能带后缀)
            if c["sql_type"] != sql and not c["sql_type"].startswith(sql + "_"):
                continue
            cand.append(c)
        if not cand:
            return None
        # 先把 test_duration 与本轮 config 一致的挑出来; 没有则退回全部候选
        exact = [c for c in cand if _num(c.get("test_duration")) == config_td]
        src = exact or cand
        src = [c for c in src if c.get("ts")]
        if src:
            src.sort(key=lambda c: c["ts"])
            return src[-1]
        return cand[0]

    # 每个被测表各产出一张 CSV (文件名带表名, 与 pgvector 一致)
    tables_order = [c["table"] for c in combos]
    _seen = set()
    # 保序去重: _seen.add 返回 None(假), 故条件只在首次遇到时成立
    tables_order = [t for t in tables_order if not (t in _seen or _seen.add(t))]
    if not tables_order:
        print("[WARN] 结果 CSV 未给出可用表名, 将以 polardb 命名输出。", file=sys.stderr)
        tables_order = ["polardb"]

    def build_rows(table):
        """为指定表生成全部输出行(仅用于判断这张表与日志的匹配程度)。

        参数: table —— 结果 CSV 中的表名; 只取该表的记录做组合匹配。
        返回: list[list], 每个 (sql 段, cache, rc) 组合一行; 段尾 PID 不足的组合
              直接不产出(不补空行), 因此行数可能小于 8 × 段数。
        """
        rows, k = [], 0
        pool = [c for c in combos if c["table"] == table]
        ctd = config_test_duration(CONFIG_PATH, table)
        for seg in segments:
            mode, sql, pids = seg[0], seg[1], seg[2]
            # 组合下标 -> (cache, rc): 0/1,2/3,4/5,6/7 依次对应 cache=0..3, rc 0/1
            cache_rc = [(i // 2, i % 2) for i in range(8)]
            for i, (cache, rc) in enumerate(cache_rc):
                if 2 * i + 1 >= len(pids):
                    continue      # 该 sql 段尾部 PID 不足时, 缺失的组合不再补行
                # 每个组合固定 2 个 PID: 偶数位预热(w), 奇数位查询(s)
                w_pid, s_pid = pids[2 * i], pids[2 * i + 1]
                recw, recs = pid_data[w_pid], pid_data[s_pid]
                n_w, n_s = len(recw["steps"]), len(recs["steps"])
                vi_w, vi_s = len(recw["vin"]), len(recs["vin"])
                # cache=0 走简单查询(无 PREPARE), 其余档位为命名 PREPARE/EXECUTE
                proto = "simple" if cache == 0 else "prep"
                run_date = recw["ts_first"][:10]     # 只取日期部分 YYYY-MM-DD
                info = combo_lookup(sql, cache, rc, mode, pool, ctd)
                parse_mode = mode
                csv_qps = (info or {}).get("qps") or "-"
                # est_QPS: 由日志查询段 steps(去首次) / 测试时长 估算, 与 pgvector 口径一致
                est_qps = "-"
                if info and info.get("test_duration"):
                    try:
                        td = float(info["test_duration"])
                    except (TypeError, ValueError):
                        td = 0.0
                    # (n_s - 1) 与 avg() 的“去首样本”口径一致; td 单位秒 => 次/秒
                    if td > 0 and n_s > 1:
                        est_qps = "%d" % round((n_s - 1) / td)
                k += 1
                rows.append([
                    k,
                    "%s/%s" % (w_pid, s_pid),        # 预热/查询 PID 对
                    sql, cache, rc, parse_mode, proto,
                    "w=%d / s=%d" % (n_w, n_s),      # 预热/查询段 pase_stmt 条数
                    cell(step_pair(w_pid, 0), step_pair(s_pid, 0)),   # parse ms
                    cell(step_pair(w_pid, 1), step_pair(s_pid, 1)),   # analyze ms
                    cell(step_pair(w_pid, 2), step_pair(s_pid, 2)),   # plan ms
                    cell(step_pair(w_pid, 3), step_pair(s_pid, 3)),   # exec ms
                    cell(eq_ms_pair(w_pid, 0), eq_ms_pair(s_pid, 0)), # eval ms
                    cell(eq_ms_pair(w_pid, 1), eq_ms_pair(s_pid, 1)), # gcp ms
                    "%d|%d" % (vi_w, vi_s),          # 向量转换次数
                    cell(vin_pair(w_pid), vin_pair(s_pid)),          # 向量转换 ms
                    "0|0", "-|-",                    # PolarDB 日志无 halfvec 埋点, 占位
                    est_qps, "-", csv_qps, run_date, # JSON_RPS 未接入, 固定 "-"
                ])
        return rows

    # 一张日志对应一张被测表: 选“组合信息匹配数最多”的表(即日志实际所属的表),
    # 输出一张干净的单表 CSV, 避免多表叠加/互相覆盖。
    best_table, best_rows, best_hits = None, [], -1
    for table in tables_order:
        rows = build_rows(table)
        # 用“日志组合在结果 CSV 最新一轮中真实命中(csv_RPS 非空)”判定日志所属表,
        # 而非 parse_mode(它来自日志、恒非空, 无区分度)
        hits = sum(1 for r in rows if (r[20] or "") != "-")
        if hits > best_hits:
            best_table, best_rows, best_hits = table, rows, hits
    if best_table is None or not best_rows:
        print("[WARN] 无有效结果, 未生成任何 CSV。", file=sys.stderr)
        return 1
    out_path = args.out or os.path.join(OUT_DIR, "polardb_log_analysis-%s.csv" % best_table)
    # newline="" 交给 csv 模块处理换行, 避免额外空行
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(best_rows)
    # 组数交叉校验: 与 config 折出的预期组数比较, 不一致只告警, 仍照常写文件
    _exp = expected_groups(CONFIG_PATH, best_table)
    if _exp is not None:
        if len(best_rows) == _exp:
            print("[OK] 已保存: %s  (表=%s, %d 组, 与 config.json 一致)"
                  % (out_path, best_table, len(best_rows)))
        else:
            print("[WARN] 组数与 config.json 不符: 解析 %d 组, 配置预期 %d 组"
                  % (len(best_rows), _exp), file=sys.stderr)
            print("[OK] 已保存: %s  (表=%s, %d 组)" % (out_path, best_table, len(best_rows)))
    else:
        print("[OK] 已保存: %s  (表=%s, %d 组)" % (out_path, best_table, len(best_rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())