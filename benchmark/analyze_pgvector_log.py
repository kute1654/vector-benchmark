#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
汇总 pgvector 的 PGVECTOR_TIMING 时序日志, 按固定测试网格分组:
循环序 vector_in_parse_mode -> sql_type -> enable_query_plan_cache -> use_query_cache
(见 engine/base_client/client.py 与 configurations/config.json)。每个
(cache, use_query_cache) 组合 = 1 预热 + 1 查询 PID(search_number=1), 输出
parse/analyze/plan/exec/eval/gcp/vi/hv 耗时与次数(均去掉首次热路径), 并用结果
CSV(experiment_name/sql_type/vector_in_parse_mode/use_cache/use_query_cache/rps/
test_duration)交叉校验 est_QPS。

要点:
- PID 即后端进程号, 每个测试执行单元新建连接 => 独立 PID; 仅含 PGVECTOR_TIMING 的行记账。
- 每次连接会先跑若干 setup(SET...)非向量 simple_query, 按 cache 档位剔除
  (cache 0 -> 5 条, 1/2/3 -> 7 条, 见 sq_drop_count), 再对余下向量查询统计。
- 数组类 SQL 类型在 cache=0/2 走 array_to_vector, 耗时计入 vi 列。

用法: python analyze_pgvector_log.py [--log ..] [--csv ..] [--out ..] [--dataset ..]
"""
import re
import os
import sys
import json
import csv
import argparse
from datetime import datetime
from collections import OrderedDict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = "/usr/local/pgsql/data/log/postgresql.log"
RESULTS_DIR = os.path.join(BASE_DIR, "results")
CONFIG_PATH = os.path.join(BASE_DIR, "configurations", "config.json")

# 精确的日志行正则
RE_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.\d+\s+(\S+)\s+\[(\d+)\]")
RE_SQ = re.compile(
    r"PGVECTOR_TIMING\] simple_query parse=([\d.]+)ms analyze=([\d.]+)ms "
    r"plan=([\d.]+)ms exec=([\d.]+)ms"
)
RE_EQ = re.compile(
    r"PGVECTOR_TIMING\] execute_query eval_params=([\d.]+)ms "
    r"get_cached_plan=([\d.]+)ms name=(\S+)"
)
RE_VI = re.compile(r"PGVECTOR_TIMING\] vector_in .*?ms=([\d.]+)")
RE_HV = re.compile(r"PGVECTOR_TIMING\] halfvec_in .*?ms=([\d.]+)")
# array_int_cast / array_real_cast / array_cast / array_to_vector_fn / with_array_cast
# 这些 SQL 类型把实参经 ARRAY[...] / real[] 数组转换语义走 array_to_vector(源码
# pgvector/src/vector.c 的 array_to_vector 已输出 PGVECTOR_TIMING), 而非 vector_in,
# 因此 cache=0/2 时应作为“向量转换耗时”计入 vi, 与 vector_in 列口径一致。
RE_ATV = re.compile(r"PGVECTOR_TIMING\] array_to_vector .*?ms=([\d.]+)")

# 单个 PID 至少应含的 PGVECTOR_TIMING 记录数。正常搜索 PID 具备持续的向量查询
# (QPS 通常 >10), 记录数远大于 40; 记录过少的 PID 是连接建立后即中断/截断的残缺
# 单元, 其耗时/次数统计无意义, 直接在解析阶段剔除。
MIN_PID_RECORDS = 40

# 相邻 PID 开始时间差超过该秒数视为一次新的独立测试批次(日志因未清空会累积多轮
# 运行与 PG 重启碎片)。预热约 5s + 查询约 test_duration(60s), 故同一连续批次内
# 相邻 PID 开始时间差 <= 61s; 大于该值即为批次边界。
GAP_MAX = 61


# --------------------------------------------------------------------------- #
# 结果 CSV / 配置读取
# --------------------------------------------------------------------------- #
def load_config(path=CONFIG_PATH):
    """读取 configurations/config.json, 返回条目列表(容错)。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"  (warn) 无法读取配置 {path}: {e}", file=sys.stderr)
        return []


def read_combos(csv_path):
    """读取 pgvector 结果 CSV, 返回可按 (parse_mode, sql_type, cache, rc) 匹配的列表。"""
    combos = []
    if not os.path.exists(csv_path):
        print(f"  (warn) 结果 CSV 不存在: {csv_path}", file=sys.stderr)
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
            if cache < 0 or cache > 3 or rc not in (0, 1):
                continue
            combos.append({
                "table": (r.get("experiment_name") or "").strip() or "pgvector",
                "sql_type": sql, "cache": cache, "rc": rc,
                "parse_mode": (r.get("vector_in_parse_mode") or "").strip(),
                "qps": (r.get("rps") or "").strip(),
                "test_duration": (r.get("test_duration") or "").strip(),
                "ts": (r.get("timestamp") or "").strip(),
            })
    return combos


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def config_cell_counts(cfg, table):
    """折出 config.json 中某表一轮完整网格的预期组数; 未知返回 None。"""
    for item in cfg:
        if item.get("name") != table or str(item.get("engine")) != "pgvector":
            continue
        up = item.get("upload_params") or {}
        return (len(up.get("vector_in_parse_mode", []) or [])
                * len(up.get("sql_type", []) or [])
                * len(up.get("enable_query_plan_cache", []) or [])
                * len(up.get("use_query_cache", []) or [])
                * int(up.get("search_number", 1) or 1))
    return None


def config_test_duration(cfg, table):
    """取 config.json 中 pgvector 条目 table 的 search test_duration(秒); 未知 None。"""
    for item in cfg:
        if str(item.get("engine")) != "pgvector" or item.get("name") != table:
            continue
        v = (item.get("search_params") or {}).get("test_duration")
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    return None


def resolve_table(cfg, n_pids, target=None):
    """推断被测 pgvector 表: target 优先, 否则取 config 中网格(组数×2 PID)与日志 PID 数一致者。"""
    if target:
        return target
    for item in cfg:
        if str(item.get("engine")) != "pgvector":
            continue
        n = config_cell_counts(cfg, item.get("name"))
        if n and n * len(LAYOUT["pid_roles"]) == n_pids:
            return item.get("name")
    return None


# --------------------------------------------------------------------------- #
# 日志解析(每次直接解析日志文件)
# --------------------------------------------------------------------------- #
def parse_log(log_path=LOG_PATH, min_records=MIN_PID_RECORDS):
    """解析整份日志,返回 {pid: rec}。

    rec = {ts_first, ts_last, n, sq:[(p,a,pl,e)...], eq:[(eval,gcp,name)...],
           vi:[ms...], hv:[ms...], stmt_name}
    仅保留含 PGVECTOR_TIMING 的行,先做子串快速过滤再正则匹配。统计每个 PID 的
    计时记录条数 n, 记录数少于 min_records 的 PID(连接建立后即中断的残缺单元,
    其次数/耗时统计不可靠)直接剔除。
    """
    data = OrderedDict()
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if "PGVECTOR_TIMING]" not in line:
                continue
            m = RE_TS.match(line)
            if not m:
                continue
            ts, pid = m.group(1), m.group(3)
            rec = data.get(pid)
            if rec is None:
                rec = data[pid] = {"ts_first": ts, "ts_last": ts, "n": 0, "sq": [],
                                   "eq": [], "vi": [], "hv": [], "stmt_name": None}
            rec["ts_last"] = ts
            rec["n"] += 1
            if "simple_query" in line:
                sm = RE_SQ.search(line)
                if sm:
                    rec["sq"].append(tuple(float(x) for x in sm.groups()))
            elif "execute_query" in line:
                em = RE_EQ.search(line)
                if em:
                    rec["eq"].append((float(em.group(1)), float(em.group(2)), em.group(3)))
                    rec["stmt_name"] = em.group(3)
            elif "halfvec_in" in line:
                hm = RE_HV.search(line)
                if hm:
                    rec["hv"].append(float(hm.group(1)))
            elif "vector_in" in line:
                vm = RE_VI.search(line)
                if vm:
                    rec["vi"].append(float(vm.group(1)))
            elif "array_to_vector" in line:
                am = RE_ATV.search(line)
                if am:
                    rec["vi"].append(float(am.group(1)))
    dropped = [(pid, rec["n"]) for pid, rec in data.items() if rec["n"] < min_records]
    for pid, _ in dropped:
        del data[pid]
    if dropped:
        sample = ", ".join(f"{p}({n})" for p, n in sorted(dropped)[:6])
        print(f"  (warn) 剔除记录数 <{min_records} 的残缺 PID: 共 {len(dropped)} 个 "
              f"[{sample}]", file=sys.stderr)
    return data


def avg(values):
    """去第一次后求均值, 空/单样本返回 None"""
    if len(values) <= 1:
        return None
    rest = values[1:]
    return sum(rest) / float(len(rest))


# --------------------------------------------------------------------------- #
# 固定测试网格(参数化布局)
# --------------------------------------------------------------------------- #
# 客户端 engine/base_client/client.py::_run_pgvector_experiment 按
# parse_mode -> sql_type -> cache -> use_query_cache 的顺序展开; 每个内层
# (cache, use_query_cache) 组合新建 len(pid_roles) 个后端 PID(warmup+search),
# cache=0 不 PREPARE(无名, simple 协议), cache=1/2/3 走命名 PREPARE(有名)。
# 块长 = len(caches) * len(use_query_caches) * len(pid_roles)。
#
# 该布局来自实测日志: 每 (mode, sql_type) 块 = 4 cache × 2 use_query_cache × 2 PID
# (warmup+search) = 16 PID, 依序为 [cache0 无名 4][cache1 有名 4][cache2 有名 4]
# [cache3 有名 4]。测试配置固定, 故网格可直接定死; 若配置变化, 只改下面 LAYOUT
# (或 CLI 覆盖)即可, 无需改动配对逻辑。
LAYOUT = {
    "parse_modes": ["strtof", "inline", "fast_float"],
    "sql_types": [
        "text_literal", "text_cast", "vector_fn",
        "array_int_cast", "array_real_cast", "array_cast",
        "array_to_vector_fn", "halfvec_literal",
        "with_text_literal", "with_array_cast", "with_vector_fn",
    ],
    # 计划缓存档位(对应 GUC enable_query_plan_cache, 写入结果 JSON 的 use_query_plan_cache):
    # 0=不 PREPARE(无名, simple 协议), 1/2/3=命名 PREPARE(prep 协议)。
    "caches": [0, 1, 2, 3],
    # 查询结果缓存(对应 use_query_cache); 与 cache 构成内层两维。
    "use_query_caches": [0, 1],
    # search_number=1 => 每个 (cache, use_query_cache) 组合 = warmup + search 两个 PID
    "pid_roles": ["warmup", "search"],
}


def layout_cells(pids, pid_data):
    """按 LAYOUT 网格把有序 PID 切成 cell。

    循环序 = vector_in_parse_mode -> sql_type -> enable_query_plan_cache -> use_query_cache
    (见 engine/base_client/client.py 的 pgvector 实验展开)。每个
    (mode, sql_type, cache, use_query_cache) 得到 len(pid_roles) 个 PID(warmup+search),
    构成一个 cell。
    返回 { (mode, sql_type, cache, uqc): {"warmup": [...], "search": [...]} }。
    块内 PID 序号 == cell 序, 直接按索引切片(比原 run_date 近邻配对更稳);
    日志在某处截断时停止, 剩余 PID 不再消费。
    """
    cells = {}
    n_pid = len(LAYOUT["pid_roles"])
    idx = 0
    for mode in LAYOUT["parse_modes"]:
        for st in LAYOUT["sql_types"]:
            for cache in LAYOUT["caches"]:
                for uqc in LAYOUT["use_query_caches"]:
                    group = pids[idx: idx + n_pid]
                    if len(group) < n_pid:
                        print(f"  (info) 网格耗尽: mode={mode} sql={st} cache={cache} "
                              f"uqc={uqc} 缺 {n_pid - len(group)} 个 PID, 停止切片。",
                              file=sys.stderr)
                        return cells
                    idx += n_pid
                    roles = {"warmup": [], "search": []}
                    for pid, role in zip(group, LAYOUT["pid_roles"]):
                        roles[role].append(pid)
                    cells[(mode, st, cache, uqc)] = roles
    return cells


def split_contiguous(pids, pid_data, gap_max=GAP_MAX):
    """按时间连续性把已按 ts_first 排序的 pids 切成段。

    相邻 PID 开始时间差 > gap_max 秒即视为批次边界。返回 list[list[pid]],
    每个子段为一次时间连续测试批次的 PID(保持原时序)。
    """
    def to_epoch(ts):
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp()

    groups, cur, prev = [], [], None
    for p in pids:
        ts = pid_data[p]["ts_first"]
        if prev is not None and (to_epoch(ts) - to_epoch(prev)) > gap_max:
            groups.append(cur)
            cur = []
        cur.append(p)
        prev = ts
    if cur:
        groups.append(cur)
    return groups


def combo_lookup(mode, sql, cache, uqc, combos, table=None):
    """在结果 CSV 中按 (table?, parse_mode, sql_type, use_cache, use_query_cache) 匹配, 取最新时间戳一行。"""
    best, best_ts = None, ""
    for c in combos:
        if table and c.get("table") != table:
            continue
        if c.get("parse_mode") != mode or c.get("sql_type") != sql:
            continue
        if c.get("cache") != cache or c.get("rc") != uqc:
            continue
        ts = c.get("ts") or ""
        if best is None or ts > best_ts:
            best, best_ts = c, ts
    return best


# --------------------------------------------------------------------------- #
# 汇总
# --------------------------------------------------------------------------- #
def sq_drop_count(cache):
    """当前 use_query_plan_cache 档位下, 剔除的【前端非向量】simple_query 条数。

    每个后端连接建立后都会先跑若干 setup(SET...等)语句, 它们是 simple_query 但
    并非向量查询; 前两档(cache=0 direct )固定 5 条, 后两档
    (cache=2/3 generic / cache=1 custom)比前两档多一条 prepare/setup, 为 7 条。cache 未知
    (如 --no-pair 模式无对应结果元数据)时回退到旧口径 1(仅去掉首次), 保持兼容。
    """
    if cache is None:
        return 1
    return 7 if int(cache) in (1, 2, 3) else 5


def summarize(pid_rec, cache=None):
    """对单个 PID 的执行单元汇总。

    先按 use_query_plan_cache 档位剔除前端非向量 simple_query(cache 0 -> 5 条,
    cache 1/2/3 -> 7 条, 见 sq_drop_count), 再对余下的向量查询做耗时/次数统计;
    各耗时均值沿用 avg() 的"去掉第一条"口径(去除首次热路径)。
    """
    sq = pid_rec["sq"][sq_drop_count(cache):]
    vi = pid_rec["vi"]
    hv = pid_rec["hv"]
    eq = pid_rec["eq"]
    return {
        "exec_count": len(sq),                    # 剔除前端非向量语句后的向量查询条数
        "exec_count_excl_first": len(sq),
        "parse_avg": avg([x[0] for x in sq]),
        "analyze_avg": avg([x[1] for x in sq]),
        "plan_avg": avg([x[2] for x in sq]),
        "exec_avg": avg([x[3] for x in sq]),
        "eq_count": len(eq),                      # PREPARE(EXECUTE) 执行条数
        "eval_avg": avg([x[0] for x in eq]),
        "gcp_avg": avg([x[1] for x in eq]),
        "vi_count": len(vi),
        "vi_avg": avg(vi),
        "hv_count": len(hv),
        "hv_avg": avg(hv),
        "stmt_name": pid_rec.get("stmt_name"),
    }


def fmt(v, nd=3):
    if v is None:
        return "-"
    return f"{v:.{nd}f}"


def wslash(a, b):
    """把 warmup/search 两个值格式化为 'a|b'。"""
    return f"{a}|{b}"


def main():
    ap = argparse.ArgumentParser(description="汇总 pgvector 时序日志并与结果 CSV 交叉验证。")
    ap.add_argument("--log", default=LOG_PATH, help="PostgreSQL 日志路径")
    ap.add_argument("--csv", default=None,
                    help="pgvector 结果 CSV 路径(默认 results/pgvector_benchmark_results.csv)")
    ap.add_argument("--out", default=None, help="输出 CSV 路径")
    ap.add_argument("--config", default=CONFIG_PATH, help="configurations/config.json 路径")
    ap.add_argument("--engine", default=None,
                    help="被测 pgvector 引擎名(默认按 PID 数与 config 网格自动推断)")
    ap.add_argument("--test-duration", type=float, default=None,
                    help="search 时长(秒), 用于 est_QPS; 默认取 config search_params.test_duration")
    ap.add_argument("--min-records", type=int, default=MIN_PID_RECORDS,
                    help="留存的 PID 单条最少计时记录数, 低于此值的残缺 PID 剔除(默认 40)")
    ap.add_argument("--gap-max", type=float, default=GAP_MAX,
                    help="相邻 PID 开始时间差超过该秒数视为新批次, 只取最后一组连续批次(默认 61)")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print(f"[ERR] 日志不存在: {args.log}", file=sys.stderr)
        return 1

    csv_path = args.csv or os.path.join(RESULTS_DIR, "pgvector_benchmark_results.csv")
    pid_data = parse_log(args.log, min_records=args.min_records)
    combos = read_combos(csv_path)
    print(f"log: {args.log}  pids={len(pid_data)}  combos(csv)={len(combos)}",
          file=sys.stderr)

    # 日志内全部含计时记录的 PID 视为当前批次(pgvector 按 PID 区分批次, 非时间)。
    pids = sorted(
        [p for p, v in pid_data.items() if (v["sq"] or v["eq"] or v["vi"] or v["hv"])],
        key=lambda p: pid_data[p]["ts_first"])
    # 日志跨多轮未清空时会把多批次+PG 重启碎片混在一起; 按时间连续性切段, 只取
    # 最后一组连续批次(相邻 PID 开始时间差 <= gap_max 为同一批)。
    groups = split_contiguous(pids, pid_data, args.gap_max)
    pids = groups[-1] if groups else []
    if len(groups) > 1:
        print(f"  (info) 日志含 {len(groups)} 段时间连续批次, 仅取最后一组 "
              f"{len(pids)} 个 PID, 丢弃前 {sum(len(g) for g in groups[:-1])} 个",
              file=sys.stderr)
    if not pids:
        print("[ERR] 日志中未发现 PGVECTOR_TIMING 记录(日志可能已轮转/截断)。",
              file=sys.stderr)
        return 1

    header = ["#", "PID(warm/s)", "sql_type", "cache", "res_cache", "parse_mode", "proto",
              "exec#(w/s)", "parse_ms(w/s)", "analyze_ms(w/s)", "plan_ms(w/s)",
              "exec_ms(w/s)", "eval_ms(w/s)", "gcp_ms(w/s)", "vi#(w/s)", "vi_ms(w/s)",
              "hv#(w/s)", "hv_ms(w/s)", "est_QPS", "CSV_RPS", "run_date"]
    cells = layout_cells(pids, pid_data)
    block_len = (len(LAYOUT["caches"]) * len(LAYOUT["use_query_caches"])
                 * len(LAYOUT["pid_roles"]))
    if cells and len(pids) % block_len != 0:
        print(f"  (warn) PID 数 {len(pids)} 不是块长 {block_len} 的整数倍, 末尾可能残留",
              file=sys.stderr)

    rows, i = [], 0

    # 被测表: config 网格(组数×2 PID)与日志 PID 数一致者; 未命中则取 CSV 首表, 数据源均来自 config。
    cfg = load_config(args.config)
    table = resolve_table(cfg, len(pids), args.engine)
    if not table:
        table = next((c["table"] for c in combos if c["table"]), "pgvector")
        print(f"  (warn) 未推断出与 PID 数匹配的 pgvector 表, 复用 CSV 首表: {table}",
              file=sys.stderr)
    dataset = next((i_.get("dataset") or i_.get("name") for i_ in cfg
                    if i_.get("name") == table), table)
    td = args.test_duration or config_test_duration(cfg, table)
    print(f"  table={table} dataset={dataset} search_td={td}s", file=sys.stderr)

    for mode in LAYOUT["parse_modes"]:
        for sql in LAYOUT["sql_types"]:
            for cache in LAYOUT["caches"]:
                for uqc in LAYOUT["use_query_caches"]:
                    roles = cells.get((mode, sql, cache, uqc))
                    if not roles:
                        continue
                    info = combo_lookup(mode, sql, cache, uqc, combos, table)
                    w_pid, s_pid = roles["warmup"][0], roles["search"][0]
                    sw = summarize(pid_data[w_pid], cache)
                    ss = summarize(pid_data[s_pid], cache)
                    dur = td or _num((info or {}).get("test_duration"))
                    rps = _num((info or {}).get("qps")) if ((info or {}).get("qps") or "").strip() else None
                    est_qps = (ss.get("exec_count") or 0) / dur if dur else None
                    i += 1
                    if rps and est_qps and abs(est_qps - rps) / rps > 0.20:
                        print(f"  (warn) #{i} {sql} cache={cache} uqc={uqc}: "
                              f"est_QPS={est_qps:.0f} 与 CSV_RPS={rps:.0f} 偏差 >20%",
                              file=sys.stderr)
                    proto = "prep" if cache else "simple"
                    rows.append([
                        i, f"{w_pid}/{s_pid}", sql, cache, uqc, mode, proto,
                        f"w={sw.get('exec_count')} / s={ss.get('exec_count')}",
                        wslash(fmt(sw.get("parse_avg")), fmt(ss.get("parse_avg"))),
                        wslash(fmt(sw.get("analyze_avg")), fmt(ss.get("analyze_avg"))),
                        wslash(fmt(sw.get("plan_avg")), fmt(ss.get("plan_avg"))),
                        wslash(fmt(sw.get("exec_avg")), fmt(ss.get("exec_avg"))),
                        wslash(fmt(sw.get("eval_avg"), 4), fmt(ss.get("eval_avg"), 4)),
                        wslash(fmt(sw.get("gcp_avg"), 4), fmt(ss.get("gcp_avg"), 4)),
                        wslash(sw.get("vi_count"), ss.get("vi_count")),
                        wslash(fmt(sw.get("vi_avg")), fmt(ss.get("vi_avg"))),
                        wslash(sw.get("hv_count"), ss.get("hv_count")),
                        wslash(fmt(sw.get("hv_avg")), fmt(ss.get("hv_avg"))),
                        f"{est_qps:.0f}" if est_qps else "-",
                        f"{rps:.0f}" if rps else "-",
                        pid_data[w_pid]["ts_first"][:10],
                    ])

    if not rows:
        print("[WARN] 无有效格子, 未生成 CSV。", file=sys.stderr)
        return 1

    out_path = args.out or os.path.join(RESULTS_DIR, f"pgvector_log_analysis-{dataset}.csv")
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow([str(x) for x in row])

    _exp = config_cell_counts(cfg, table)
    msg = f"[OK] 已保存: {out_path}  表={table}  {len(rows)} 行"
    if _exp is not None and len(rows) != _exp:
        msg += f"  (config 预期 {_exp} 组, 前缀/截断所致)"
    print(msg)
    return 0


if __name__ == "__main__":
    sys.exit(main())