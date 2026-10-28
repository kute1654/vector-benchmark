#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
汇总 pgvector 的 PGVECTOR_TIMING 时序日志, 按 config.json 的实测网格分组:
    sql_type -> enable_query_plan_cache -> use_query_cache -> vector_in_parse_mode
(见 engine/base_client/client.py::_run_pgvector_experiment)。每个格子内再按
search_params 展开出的 searcher 组合(parallel/top/ef_s)分行, 每行统计
warmup + search_number 个查询连接, 输出 parse/analyze/plan/exec/eval/gcp/vi/hv
耗时与次数(均去掉首次热路径), 并用结果 CSV 的 rps 交叉校验 est_QPS。

sql_type 的判定(用户要求: cache=0 无 PREPARE 名, >0 才有):
- enable_query_plan_cache > 0 时, 每个连接都会留下
    [PLAN_CACHE_TRACE] ACTION=... query=PREPARE pgv_ps_<sql_type>_<rand6>(...)
    [PGVECTOR_TIMING] execute_query ... name=pgv_ps_<sql_type>_<rand6>
  从语句名剥掉 `pgv_ps_` 前缀与 6 位随机后缀即得 sql_type。
- enable_query_plan_cache = 0 时不 PREPARE, 日志里没有语句名; 此时利用客户端
  “同一 sql_type 的 4 个缓存档位在日志中连续排列”的布局, 用该块内有名 PID 的
  sql_type 回填整块(块首无名档即 cache=0)。

动态性:
- sql_type 的数量与顺序以日志实际出现的为准(cache>0 的 PREPARE 名), 不写死;
  只跑了一部分 sql_type(测试尚未结束)时也只输出已完成的块。
- 被测表由三者共同判定: 日志 sql_type 集合、向量事件维度 dim(128/256/768/960)、
  网格 PID 数; 若 config 的 search_params 与实际不符, 再按日志 PID 数反推 per_cell
  与 searcher 数。日志中只有十余条记录的截断 PID 仍然保留(它占一个格位, 丢掉会
  让后续所有块错位), 默认 --min-records=1。

要点:
- PID 即后端进程号, 每个测试执行单元新建连接 => 独立 PID; PGVECTOR_TIMING 行记账,
  PLAN_CACHE_TRACE 行只补充语句名/缓存动作, 不计入记录数。
- 每次连接先跑若干 setup(SET...)非向量 simple_query, 在首个向量事件(vi/hv/atv/eq)
  之前按实际条数剔除(cache 0 -> 4 条, 1/2/3 -> 6 条), 再统计余下向量查询。
- 数组类 SQL 类型在 cache=0 走 array_to_vector, 耗时计入 vi 列。

用法: python analyze_pgvector_log.py [--log ..] [--csv ..] [--out ..] [--engine ..]

输入日志格式假设(与 client 打印格式强绑定, 改打印格式必须同步改正则):
- 行首为 "<日期> <时间>.<毫秒> <用户名> [PID]"; RE_TS 只取秒级时间戳, 毫秒丢弃。
- 计时行形如:
    PGVECTOR_TIMING] simple_query parse=<p>ms analyze=<a>ms plan=<pl>ms exec=<e>ms
    PGVECTOR_TIMING] execute_query eval_params=<e>ms get_cached_plan=<g>ms name=<stmt>
    PGVECTOR_TIMING] vector_in|halfvec_in|array_to_vector ... dim=<d> ... ms=<v>
- 追踪行形如: [PLAN_CACHE_TRACE] ACTION=<动作> ... query=PREPARE <名字>(
- 依赖服务端开启 log_min_duration_statement(一般为 0)、log_statement 及
  auto_explain; 缺少某类行时只是对应列统计为空, 不会报错。

输出 CSV:
- 默认 results/csv/pgvector_log_analysis-<dataset>.csv, 可用 --out 覆盖。
- 一行 = 一个格子里一个 searcher(warmup 不计入参数格, 已在切分前剥离)。
- 字段与单位:
    # 行号; PID 该 searcher 的搜索 PID(逗号分隔); sql_type; cache(cache 档位);
    res_cache(result cache 0/1); parse_mode; proto(simple/prep);
    exec# 向量查询条数; parse_ms/analyze_ms/plan_ms/exec_ms 为毫秒均值;
    eval_ms(get_cached_plan 前一步)/gcp_ms(get_cached_plan) 毫秒均值;
    vi# 向量转换次数; vi_ms 毫秒均值(vector_in 与 array_to_vector 合并);
    hv#/hv_ms halfvec_in 的次数与毫秒均值;
    est_QPS 由日志 exec# / (时长×PID 数) 估算(次/秒); CSV_RPS 取结果 CSV rps(次/秒);
    run_date 该 searcher 首个 PID 的日期(仅 YYYY-MM-DD); plan_mode 由 trace 动作推断;
    search_cfg searcher 的 parallel/top/ef_s 摘要。
- 所有 *_ms 均值都遵循 avg(): 去掉每个序列的第一条样本后再平均(去首次热路径)。

已知局限:
- 纯正则解析: 日志格式/字段顺序微变即匹配失败; 因为按子串分派, 正则不中时该行被
  静默忽略, 不报错, 只体现为计数偏少。
- 只认秒级时间戳, 同一秒内的 PID 只能依赖文件出现顺序。
- 日志未清空时含多轮历史; 只保留最后一个时间连续批次, 其余丢弃(有 stderr 提示)。
- cache=0 的 PID 无 PREPARE 名, sql_type 依赖同块有名 PID 回填; 若无 config 且整块
  无名, 会退化为日志推断(默认 cache 0/1/2/3、每档 2 PID), 精度有限。
- test_duration 未知时 est_QPS 输出 "-"; 与 CSV_RPS 偏差 >20% 只告警不修正。
"""
import re
import os
import sys
import json
import csv
import argparse
import itertools
from datetime import datetime
from collections import OrderedDict, Counter

# ---- 路径常量 ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))         # benchmark/ 目录
LOG_PATH = "/usr/local/pgsql/data/log/postgresql.log"         # pgvector 服务端日志默认路径
RESULTS_DIR = os.path.join(BASE_DIR, "results/csv")           # 结果 CSV 与输出 CSV 所在目录
CONFIG_PATH = os.path.join(BASE_DIR, "configurations", "config.json")  # 实测网格配置

# ---- 日志行正则 ----
# 行首: "<日期> <时间>.<毫秒> <用户名> [PID]"。
#   组1 = 秒级时间戳(毫秒被丢弃, 用于批次切分与 run_date)
#   组2 = 用户名(不使用); 组3 = backend PID, 即连接级唯一标识, 本脚本按它分组
RE_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.\d+\s+(\S+)\s+\[(\d+)\]")
# simple_query 四段耗时; 组 1..4 = parse / analyze / plan / exec, 单位 ms
RE_SQ = re.compile(
    r"PGVECTOR_TIMING\] simple_query parse=([\d.]+)ms analyze=([\d.]+)ms "
    r"plan=([\d.]+)ms exec=([\d.]+)ms"
)
# execute_query(命名 PREPARE + EXECUTE 形态)的耗时:
#   组1 = eval_params(绑定参数求值) ms; 组2 = get_cached_plan ms; 组3 = 语句名
# 语句名用于反推 sql_type, 也是判定该 PID 是否走了 prepared 协议的依据。
RE_EQ = re.compile(
    r"PGVECTOR_TIMING\] execute_query eval_params=([\d.]+)ms "
    r"get_cached_plan=([\d.]+)ms name=(\S+)"
)
# vector_in/halfvec_in: ".*?ms=" 非贪婪跳过中间的 dim= 等字段, 组1 为耗时 ms
RE_VI = re.compile(r"PGVECTOR_TIMING\] vector_in .*?ms=([\d.]+)")
RE_HV = re.compile(r"PGVECTOR_TIMING\] halfvec_in .*?ms=([\d.]+)")
# array_int_cast / array_real_cast / array_cast / array_to_vector_fn / with_array_cast
# 这些 SQL 类型把实参经 ARRAY[...] / real[] 数组转换语义走 array_to_vector(源码
# pgvector/src/vector.c 的 array_to_vector 已输出 PGVECTOR_TIMING), 而非 vector_in,
# 因此 cache=0 时应作为“向量转换耗时”计入 vi, 与 vector_in 列口径一致。
# 组1 = array_to_vector 耗时 ms。
RE_ATV = re.compile(r"PGVECTOR_TIMING\] array_to_vector .*?ms=([\d.]+)")
# 向量解析事件都带 dim=<维度>; 组1 为维度, 用于按维度判定当前被测表(128/256/768/960...)。
RE_DIM = re.compile(r"\bdim=(\d+)")

# 计划缓存追踪: 动作计数用于校验 cache 档位, PREPARE 语句名用于判定 sql_type。
# 组1 = 动作名, 如 USE_CUSTOM / USE_GENERIC / BUILD_GENERIC(用于 infer_plan_mode)。
RE_TRACE_ACTION = re.compile(r"\[PLAN_CACHE_TRACE\] ACTION=(\w+)")
# 从 trace 行里抓 PREPARE 语句名; 组1 为名字, 末尾的 "("(参数列表起点)被吃掉。
RE_TRACE_PREPARE = re.compile(r"query=PREPARE\s+(\S+?)\s*\(")
# PREPARE 语句名: pgv_ps_<sql_type>_<6 位小写字母/数字随机后缀>
# 组1 = sql_type。`.+` 贪婪匹配, 保证剥掉的是最后 6 位随机后缀; 名字不合格式返回 None。
RE_PREPARED_NAME = re.compile(r"^(?:pgv_ps|polardb_ps)_(.+)_[a-z0-9]{6}$")

# 单个 PID 至少应含的 PGVECTOR_TIMING 记录数。正常搜索 PID 有上千条记录, 但日志
# 轮转/测试刚开始时首个(或末尾) PID 可能只留下十余条, 它仍然是网格中的合法单元。
# 只有当 PID 完全没有计时记录(例如只剩连接初始化)时才剔除, 否则会少算一个格位并
# 导致后续按 PID 数推断被测表失败。默认 1 = 不主动剔除有效 PID。
MIN_PID_RECORDS = 1

# 相邻 PID 开始时间差超过该秒数视为一次新的独立测试批次(日志因未清空会累积多轮
# 运行与 PG 重启碎片)。同一批次内相邻 PID 的间隔约为一个查询时长(预热->search 约
# 5s, search_i -> search_{i+1} 约 test_duration), 故阈值取
# max(GAP_MAX, 本配置最大 test_duration + 11), 见 gap_threshold()。
GAP_MAX = 61


# --------------------------------------------------------------------------- #
# 结果 CSV / 配置读取
# --------------------------------------------------------------------------- #
def load_config(path=CONFIG_PATH):
    """读取 configurations/config.json, 返回条目列表(容错)。

    参数:
        path: config 路径, 默认模块级 CONFIG_PATH。
    返回:
        顶层是 JSON 数组时返回 list[dict]; 是对象或其它类型时返回 []; 读取/解析
        异常全部吞掉(仅 stderr 打 warn), 以便后续退回日志推断网格。
    异常: 不向外抛出。
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"  (warn) 无法读取配置 {path}: {e}", file=sys.stderr)
        return []


def read_combos(csv_path):
    """读取 pgvector 结果 CSV, 返回可按 (sql_type, cache, rc, parse_mode) 匹配的列表。

    参数:
        csv_path: 结果 CSV 路径(默认 results/csv/pgvector_benchmark_results.csv)。
    返回:
        list[dict], 每行一个 dict, 键为 table/dataset/vector_size/sql_type/cache/rc/
        parse_mode/qps/test_duration/parallel/top/ef_s/ts。
        - qps 取自 CSV 的 rps 列, 单位次/秒; test_duration 保留原始字符串。
        - sql_type 为空, 或 use_cache/use_query_cache 不能转 int 的行被静默跳过
          (汇总行、残缺行), 不告警。
        - 文件不存在时告警并返回空列表; 用 errors="replace" 避免编码问题中断。
    """
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
            combos.append({
                "table": (r.get("experiment_name") or "").strip() or "pgvector",
                "dataset": (r.get("dataset") or "").strip(),
                "vector_size": (r.get("vector_size") or "").strip(),
                "sql_type": sql, "cache": cache, "rc": rc,
                "parse_mode": (r.get("vector_in_parse_mode") or "").strip(),
                "qps": (r.get("rps") or "").strip(),
                "test_duration": (r.get("test_duration") or "").strip(),
                "parallel": (r.get("parallel") or "").strip(),
                "top": (r.get("top") or "").strip(),
                "ef_s": (r.get("ef_s") or "").strip(),
                "ts": (r.get("timestamp") or "").strip(),
            })
    return combos


def _num(v):
    """尽力转 float; None/空串/非数字等失败情况静默返回 None(不告警)。"""
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_list(v):
    """把标量包成单元素列表(None -> []), 用于兼容 config 中标量/列表两种写法。"""
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def _expand_dict(params):
    """复刻 client_factory._expand_params: 对 dict 中 list 值做笛卡尔积。

    参数:
        params: 参数字典; 值为 list 的键参与展开, 其余键作为固定值保留。
    返回:
        list[dict]。非 dict 或空 dict 返回 [params]; 没有 list 值时也返回 [params]。
        展开顺序按 dict 插入顺序(与 client 一致), 保证 searcher 顺序/标签不错位。
    """
    if not isinstance(params, dict) or not params:
        return [params]
    keys, values, fixed = [], [], {}
    for k, v in params.items():
        if isinstance(v, list):
            keys.append(k)
            values.append(v)
        else:
            fixed[k] = v
    if not keys:
        return [params]
    out = []
    # itertools.product 的右端键变化最快, 与配置书写顺序的展开语义一致
    for combo in itertools.product(*values):
        merged = dict(fixed)
        merged.update(zip(keys, combo))
        out.append(merged)
    return out


def expand_searchers(search_params):
    """复刻 client_factory._create_searchers 的展开顺序, 返回 searcher 参数列表。

    参数:
        search_params: config 中某表的 search_params, 可为 None / dict / list[dict]。
    返回:
        list[dict], 元素形如 {<base 键>, "params": {...}}; 展开顺序与 client 相同,
        决定每格 PID 与 searcher 的对应关系。无效条目跳过; 全空时回退
        [{"params": {}}]。

    每个 searcher 在每格内产生 search_number 个查询 PID; 第一个有 test_duration>0
    的 searcher 还会为整格贡献 1 个预热 PID(client 取最后一个 duration searcher)。
    """
    if search_params is None:
        items = [{}]
    elif isinstance(search_params, list):
        items = search_params
    else:
        items = [search_params]
    out = []
    for sp in items:
        if not isinstance(sp, dict):
            continue
        # 外层(parallel/top/test_duration 等)先展开, 内层 params(ef_s 等)后展开;
        # 顺序必须与 client_factory 一致, 否则按 PID 顺序套标签会整体错位。
        base = {k: v for k, v in sp.items() if k != "params"}
        params = sp.get("params")
        if not isinstance(params, dict):
            params = {}
        for eb in _expand_dict(base):
            for ep in _expand_dict(params):
                out.append({**eb, "params": ep})
    return out or [{"params": {}}]


def searcher_label(sp):
    """searcher 的 parallel/top/ef_s 摘要, 用于输出与 CSV 匹配。

    参数: sp —— expand_searchers() 产出的单个 searcher dict。
    返回: "parallel=.../top=.../ef_s=..." 形式的展示字符串; 三者都缺省时返回 "-"。
          ef_s 与 ef_search 两个键名都接受, 按顺序取到的第一个有值者。
    """
    bits = []
    for k in ("parallel", "top"):
        if sp.get(k) is not None:
            bits.append(f"{k}={sp[k]}")
    p = sp.get("params") or {}
    for k in ("ef_s", "ef_search"):
        if p.get(k) is not None:
            bits.append(f"{k}={p[k]}")
    return "/".join(bits) or "-"


def config_grid(cfg, table):
    """折出 config.json 中某表一轮完整网格; 该表不存在/非 pgvector 返回 None。

    参数:
        cfg: load_config() 返回的 list[dict]。
        table: 表名(匹配 config 的 name); 只接受 engine == "pgvector" 的条目。
    返回:
        dict 或 None。grid = {
          sql_types, caches, result_caches, parse_modes,
          search_number, searchers, labels, has_warmup, prewarm, per_cell,
        }
        caches 兼容新键 enable_query_plan_cache 与旧键 use_cache;
        per_cell = len(searchers) * search_number, 即每 (sql,cache,rc,mode) 组合的
        搜索 PID 数(不含预热); prewarm = 0/1, 表示整表网格最前方的前置预热 PID 数。
        search_number 非法或缺失时按 1 处理。
    """
    for item in cfg:
        if item.get("name") != table or str(item.get("engine")) != "pgvector":
            continue
        up = item.get("upload_params") or {}
        sql_types = [str(x) for x in _as_list(up.get("sql_type", ["text_literal"]))]
        # config 用 ck 风格键 enable_query_plan_cache, 旧配置用 use_cache。
        caches = [int(x) for x in _as_list(
            up.get("enable_query_plan_cache", up.get("use_cache", [0])))]
        rcs = [int(x) for x in _as_list(up.get("use_query_cache", [0]))]
        pms = ["" if x is None else str(x)
               for x in _as_list(up.get("vector_in_parse_mode", [None]))]
        try:
            sn = int(up.get("search_number", 1) or 1)
        except (TypeError, ValueError):
            sn = 1
        searchers = expand_searchers(item.get("search_params"))
        has_warmup = any(_is_duration_searcher(s) for s in searchers)
        # 预热已改为"每表一次"(client 以 _table_warmup_done 标志控制), 预热连接只
        # 在整个网格最前方产生 1 个独立 PID, 因此 grid 级只计 prewarm=1, 各参数格
        # per_cell 不再含预热。
        return {
            "sql_types": sql_types,
            "caches": caches,
            "result_caches": rcs,
            "parse_modes": pms,
            "search_number": max(1, sn),
            "searchers": searchers,
            "labels": [searcher_label(s) for s in searchers],
            "has_warmup": has_warmup,
            "prewarm": 1 if has_warmup else 0,      # 整表网格最前方的前置预热 PID 数
            "per_cell": len(searchers) * max(1, sn),  # 每格搜索 PID 数(不含预热)
        }
    return None


def _is_duration_searcher(sp):
    """判断 searcher 是否配置了正的 test_duration(秒); 非法值视为否。"""
    try:
        return int((sp or {}).get("test_duration", 0) or 0) > 0
    except (TypeError, ValueError):
        return False


def grid_cell_count(grid):
    """一轮网格的 (sql,cache,rc,parse_mode) 组数(不含 per_cell 的 searcher/PID 维度)。"""
    return (len(grid["sql_types"]) * len(grid["caches"])
            * len(grid["result_caches"]) * len(grid["parse_modes"]))


def grid_pid_count(grid):
    """一轮网格(含前置整表预热)的 PID 总数。

    预热已改为"每表一次": 网格最前方 grid["prewarm"](0/1) 个 PID 是把索引装入内存的
    整表预热, 之后才是各 (sql,cache,rc,parse_mode) 单元格的搜索 PID。
    单位: 个 PID。
    """
    return ((grid.get("prewarm", 0) or 0)
            + (len(grid["sql_types"]) * len(grid["caches"])
               * len(grid["result_caches"]) * len(grid["parse_modes"]))
            * grid["per_cell"])


def config_test_duration(cfg, table):
    """取 config 中某 pgvector 表首个有值的 search test_duration(秒); 未知 None。

    遍历该表展开后的 searcher, 返回第一个可转 float 的 test_duration; 全部非法或
    表不存在时返回 None。单位: 秒。
    """
    for item in cfg:
        if str(item.get("engine")) != "pgvector" or item.get("name") != table:
            continue
        for sp in expand_searchers(item.get("search_params")):
            try:
                return float(sp.get("test_duration"))
            except (TypeError, ValueError):
                continue
        return None
    return None


def gap_threshold(cfg, base=GAP_MAX):
    """批次切分阈值: 至少 base, 且不小于 config 中最大 test_duration + 11s。

    search_number>1 时相邻查询 PID 的间隔 ≈ test_duration; test_duration=60 的
    配置若固定用 61s 判界, 会把同一批误切。配置文件不可用时退回 base。

    参数: cfg —— config 条目列表; base —— 最小阈值(秒, 默认 GAP_MAX)。
    返回: 秒数, max(base, 所有正 test_duration + 11)。
    """
    tds = []
    for item in cfg:
        if str(item.get("engine")) != "pgvector":
            continue
        for sp in expand_searchers(item.get("search_params")):
            td = _num(sp.get("test_duration"))
            if td and td > 0:
                tds.append(td)
    return max([base] + [td + 11 for td in tds]) if tds else base


def dataset_dim(dataset):
    """从数据集名猜向量维度: nytimes-256-angular -> 256; 1000k 这类行数不算。

    参数: dataset —— 数据集名(可含 - / _ 分隔的多段)。
    返回: 维度 int 或 None。只取 2~5 位、且后面紧跟 -/_ 的数字段(排除 1000k 这类
          行数后缀); 维度是名称最后一段(如 "sift-128")时识别不出, 返回 None。
    """
    if not dataset:
        return None
    m = re.search(r"(?:^|[-_])(\d{2,5})(?![\dk])[-_]", dataset)
    return int(m.group(1)) if m else None


def resolve_table(cfg, n_pids, target=None, combos=None, observed_sqls=None, dim=None):
    """推断被测 pgvector 表。

    优先 target; 否则在 config 的 pgvector 条目中依次筛选:
      1. 日志里出现的 sql_type 必须都能在该表 config 的 sql_type 中找到
         (config 多出的类型可容忍: 可能是本轮还没跑到的后续块);
      2. 日志向量事件的维度必须与表维度一致 —— 维度取结果 CSV 的 vector_size,
         无 CSV 行时回退数据集名(128/256/768/960...);
      3. 网格 PID 数优先精确匹配 n_pids; 若是测试尚未跑完的前缀, 取预期 PID 数
         最小且大于 n_pids 的候选;
      4. 仍多解时用结果 CSV 的 experiment_name 消歧。

    参数:
        cfg: config 条目列表; n_pids: 日志中当前批次的 PID 数(D 段后的 pids);
        target: --engine 显式指定的表名, 命中即直接返回;
        combos: read_combos() 的结果, 提供 vector_size/table 线索;
        observed_sqls: 日志实际出现的 sql_type 顺序;
        dim: 日志向量事件中最常见的维度。
    返回: 表名或 None(找不到任何候选)。
    """
    if target:
        return target
    sqlset = set(observed_sqls or [])
    cands = []
    for item in cfg:
        if str(item.get("engine")) != "pgvector":
            continue
        name = item.get("name")
        grid = config_grid(cfg, name)
        if not grid:
            continue
        # 条件1: 日志 sql_type 必须是该表 config 的子集(容忍 config 多出的未跑到类型)
        if sqlset and not sqlset <= set(grid["sql_types"]):
            continue
        # 条件2: 维度优先取结果 CSV 的 vector_size(同一表可能有多个值, 取最小),
        # 无 CSV 行时回退数据集名; 两者都有时以 CSV 为准且要求日志维度命中。
        csv_dims = {c["vector_size"] for c in (combos or [])
                    if c["table"] == name and c["vector_size"]}
        if csv_dims:
            if dim is not None and str(dim) not in csv_dims:
                continue
            tdim = int(sorted(csv_dims)[0])
        else:
            tdim = dataset_dim(item.get("dataset"))
            if dim is not None and tdim is not None and tdim != int(dim):
                continue
        cands.append({"name": name, "npids": grid_pid_count(grid),
                      "nsql": len(grid["sql_types"]), "dim": tdim})
    if not cands:
        return None
    # 条件3: PID 数精确匹配; 测试未跑完时取预期 PID 数最小的“大于实测”候选;
    # 都满足不了就保留全部候选交给后续条件。
    pool = [c for c in cands if c["npids"] == n_pids]
    if not pool:
        pool = [c for c in cands if c["npids"] > n_pids] or cands
    # 条件4: 结果 CSV 中出现过的表名优先。
    names = {c["table"] for c in (combos or [])}
    in_csv = [c for c in pool if c["name"] in names]
    if in_csv:
        pool = in_csv
    # 再按 sql_type 数量消歧, 最后按预期 PID 数升序取首个。
    if observed_sqls:
        same_n = [c for c in pool if c["nsql"] == len(observed_sqls)]
        if same_n:
            pool = same_n
    pool.sort(key=lambda c: c["npids"])
    if len(pool) > 1:
        print(f"  (warn) 多张表同时匹配(pid={n_pids}, dim={dim}, "
              f"sql={len(observed_sqls or [])}): {[c['name'] for c in pool]}; 取首个",
              file=sys.stderr)
    return pool[0]["name"]


# --------------------------------------------------------------------------- #
# 日志解析(每次直接解析日志文件)
# --------------------------------------------------------------------------- #
def stmt_sql_type(name):
    """从 PREPARE 语句名提取 sql_type: pgv_ps_<sql_type>_<rand6> -> <sql_type>。

    参数: name —— execute_query 的 name= 值或 trace 行 PREPARE 后的语句名。
    返回: sql_type 字符串; name 为空或不符合 RE_PREPARED_NAME(需 pgv_ps_/polardb_ps_
          前缀, 且以 6 位小写字母数字随机后缀结尾)时返回 None。
    """
    if not name:
        return None
    m = RE_PREPARED_NAME.match(name)
    return m.group(1) if m else None


def rec_sql_type(rec):
    """优先用 execute_query 的 name, 回退到 PLAN_CACHE_TRACE 的 PREPARE 名。

    参数: rec —— parse_log() 里单个 PID 的记录(含 stmt_name / prep_name)。
    返回: sql_type 或 None(两个来源都解析不出)。cache=0 的 PID 两者都为空, 正是
          靠这一点判定“无名档”。
    """
    for name in (rec.get("stmt_name"), rec.get("prep_name")):
        st = stmt_sql_type(name)
        if st:
            return st
    return None


def parse_log(log_path=LOG_PATH, min_records=MIN_PID_RECORDS):
    """解析整份日志,返回 {pid: rec}。

    rec = {ts_first, ts_last, n, events:[kind...], sq:[(p,a,pl,e)...],
           eq:[(eval,gcp,name)...], vi:[ms...], hv:[ms...],
           stmt_name, prep_name, actions:Counter}
    仅保留含 PGVECTOR_TIMING / PLAN_CACHE_TRACE 的行,先做子串快速过滤再正则匹配。
    统计每个 PID 的计时记录数 n, 记录数少于 min_records 的 PID 剔除(默认 1, 即只
    剔除完全没有计时记录的 PID; 日志轮转/测试起始处只有十余条记录的 PID 仍是合法
    格位, 不能按 40 之类的阈值丢弃, 否则 PID 总数与 config 网格对不上)。
    PLAN_CACHE_TRACE 行不计入 n。rec["dims"] 汇总向量解析事件的维度。

    参数:
        log_path: 服务端日志路径。
        min_records: PID 保留阈值(计时行数), 默认 MIN_PID_RECORDS=1。
    返回:
        OrderedDict {str(pid): rec}, 按 PID 首次出现顺序; 所有耗时单位为 ms,
        events 保留各计时事件的按行顺序(供 setup_len 判断首个向量事件之前的前导 sq)。
    异常:
        文件不存在/无读权限时直接向上抛 OSError, 由调用方事先检查。
    """
    data = OrderedDict()
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            # 先用极廉价的子串判断过滤掉绝大多数无关行, 再跑正则
            is_timing = "PGVECTOR_TIMING]" in line
            is_trace = "[PLAN_CACHE_TRACE]" in line
            if not (is_timing or is_trace):
                continue
            m = RE_TS.match(line)
            if not m:
                continue                      # 无行首时间戳的续行无法归属 PID, 跳过
            # 组3 = backend PID: 同一 PID 即同一次连接, 也是 config 网格的一个格位
            ts, pid = m.group(1), m.group(3)
            rec = data.get(pid)
            if rec is None:
                # 首次见到该 PID: 初始化累加器; dims 是 vi/hv/atv 维度的计数器
                rec = data[pid] = {
                    "ts_first": ts, "ts_last": ts, "n": 0, "events": [],
                    "sq": [], "eq": [], "vi": [], "hv": [],
                    "stmt_name": None, "prep_name": None, "actions": Counter(),
                    "dims": Counter(),
                }
            rec["ts_last"] = ts
            if is_trace:
                # trace 行只补充动作计数与 PREPARE 名, 不计入 n, 且必须 continue,
                # 否则会被下面的计时分支误当成本行还有计时字段。
                am = RE_TRACE_ACTION.search(line)
                if am:
                    rec["actions"][am.group(1)] += 1
                pm = RE_TRACE_PREPARE.search(line)
                if pm:
                    rec["prep_name"] = pm.group(1)
                continue
            rec["n"] += 1                     # 仅计时行计数: 用于残缺 PID 剔除
            # 每条计时行只属于一类; 按事件名分派, 正则不匹配时静默丢弃(不计数)
            if "simple_query" in line:
                sm = RE_SQ.search(line)
                if sm:
                    # 组1..4 = parse/analyze/plan/exec, 单位 ms
                    rec["sq"].append(tuple(float(x) for x in sm.groups()))
                    rec["events"].append("sq")
            elif "execute_query" in line:
                em = RE_EQ.search(line)
                if em:
                    # (eval_params_ms, get_cached_plan_ms, stmt_name)
                    rec["eq"].append((float(em.group(1)), float(em.group(2)), em.group(3)))
                    rec["stmt_name"] = em.group(3)   # 最后一次出现的名字即本连接用的语句
                    rec["events"].append("eq")
            elif "halfvec_in" in line:
                hm = RE_HV.search(line)
                if hm:
                    rec["hv"].append(float(hm.group(1)))
                    rec["events"].append("hv")
                    dm = RE_DIM.search(line)
                    if dm:
                        rec["dims"][int(dm.group(1))] += 1   # 维度投票, 判定被测表
            elif "vector_in" in line:
                vm = RE_VI.search(line)
                if vm:
                    rec["vi"].append(float(vm.group(1)))
                    rec["events"].append("vi")
                    dm = RE_DIM.search(line)
                    if dm:
                        rec["dims"][int(dm.group(1))] += 1
            elif "array_to_vector" in line:
                # 数组类类型在 cache=0 走这里; 与 vector_in 同口径并入 vi
                am = RE_ATV.search(line)
                if am:
                    rec["vi"].append(float(am.group(1)))
                    rec["events"].append("atv")      # 单独标记, 仍算向量事件
                    dm = RE_DIM.search(line)
                    if dm:
                        rec["dims"][int(dm.group(1))] += 1
    # 剔除计时记录不足的 PID; 只带 trace 没有计时的 PID(n=0)也会被剔掉
    dropped = [(pid, rec["n"]) for pid, rec in data.items() if rec["n"] < min_records]
    for pid, _ in dropped:
        del data[pid]
    if dropped:
        sample = ", ".join(f"{p}({n})" for p, n in sorted(dropped)[:6])
        print(f"  (warn) 剔除记录数 <{min_records} 的残缺 PID: 共 {len(dropped)} 个 "
              f"[{sample}]", file=sys.stderr)
    return data


def avg(values):
    """去第一次后求均值, 空/单样本返回 None

    参数: values —— 按发生顺序排列的耗时列表(通常首个样本是冷启动)。
    返回: 去掉 values[0] 后的算术均值; 长度 <= 1 时返回 None。
    单位与输入一致(本模块均为 ms); 该口径用于避开每条连接的首个热路径样本。
    """
    if len(values) <= 1:
        return None
    rest = values[1:]
    return sum(rest) / float(len(rest))


# --------------------------------------------------------------------------- #
# 网格切片
# --------------------------------------------------------------------------- #
def log_sql_order(pids, pid_data):
    """日志中 sql_type 的首次出现顺序(cache=0 无名不参与)。

    这就是本轮实际跑到的 sql_type 序列, 数量可能小于 config 的全量列表(测试
    尚未跑完)或与之不同; 切片时以日志为准, 不再写死 sql_type 的数量。

    参数: pids —— 按时间(PID 开始时间)排好序的 PID 列表;
          pid_data —— parse_log() 的 {pid: rec}。
    返回: list[str], sql_type 首次出现的顺序; 全部 PID 都无名(cache=0)时返回 []。
    """
    out = []
    for p in pids:
        st = rec_sql_type(pid_data[p])
        if st and st not in out:
            out.append(st)
    return out


def log_dim(pid_data, pids):
    """日志向量解析事件(vi/hv/atv)中最常见的 dim, 用于判定被测表。

    参数: pid_data —— {pid: rec}; pids —— 参与统计的 PID 列表(当前批次)。
    返回: 出现次数最多的维度 int; 没有任何带 dim 的事件时返回 None。
          若两种维度票数相同, Counter.most_common 取先出现者。
    """
    cnt = Counter()
    for p in pids:
        cnt.update(pid_data[p].get("dims") or {})
    return cnt.most_common(1)[0][0] if cnt else None


def adapt_grid(grid, n_pids, sqls):
    """按日志实测的 sql_type 数量/每块 PID 数校正 config 网格。

    当 config 的 search_params 与本轮实际不符(只跑了一部分 searcher, 或表参数
    不同)时, 用日志 PID 数反推每格 PID 数 per_cell, 并同步 searcher 数量与标签。
    仅在 PID 数能被 sql_type 数整除、且块长仍满足 cache × rc × mode 结构时校正。
    返回 (grid, 每个 sql_type 块的 PID 数 per_sql)。

    参数:
        grid: config_grid()/log_grid() 得到的网格(不会被原地修改)。
        n_pids: 日志当前批次的总 PID 数(含整表前置预热)。
        sqls: 日志实际出现的 sql_type 序列; 为空时沿用 grid 里的。
    返回:
        (新 grid, per_sql)。per_sql 单位为 PID 数, 即一个 sql_type 块的长度 =
        slots(cache×rc×mode) × per_cell。
    """
    grid = dict(grid)
    if sqls:
        grid["sql_types"] = list(sqls)
    # slots = 一个 sql_type 块内 (cache, result_cache, parse_mode) 的组合数
    slots = (len(grid["caches"]) * len(grid["result_caches"])
             * len(grid["parse_modes"]))
    per_cell_cfg = grid["per_cell"]
    per_sql = slots * per_cell_cfg
    n_sql = len(grid["sql_types"])
    # 日志 PID 总数含前置整表预热(若有): 反推每格 PID 数先从总数中剔除预热。
    prewarm = grid.get("prewarm", 0) or 0
    grid_pids = n_pids - prewarm
    # 只在不整除时无法均分块长, 直接维持 config 网格(初值 per_sql 已算好)
    if n_sql and grid_pids and grid_pids % n_sql == 0:
        cand = grid_pids // n_sql     # 日志实测的每 sql_type 块长
        # 只有在与 config 不同、且仍能按 slots 均分时才认为需要校正
        if cand != per_sql and cand % slots == 0:
            per_cell = cand // slots
            sn = grid["search_number"]
            n_search = per_cell          # 预热已剥离, per_cell 即搜索 PID 数
            if n_search > 0 and n_search % sn == 0:
                n_sea = n_search // sn   # 反推的 searcher 数
                # 标签/searcher 按反推数量截断, 不足补默认值, 保证下标访问安全
                labels = list(grid.get("labels") or [])[:n_sea]
                labels += ["-"] * (n_sea - len(labels))
                searchers = list(grid.get("searchers") or [])[:n_sea]
                searchers += [{"params": {}}] * (n_sea - len(searchers))
                grid.update(per_cell=per_cell, labels=labels,
                            searchers=searchers)
                per_sql = cand
                print(f"  (warn) 网格与日志不符: config 每 sql_type {slots}档 x "
                      f"{per_cell_cfg} PID, 日志 {n_sql} sql x {cand} PID; "
                      f"按日志校正(per_cell={per_cell}, searcher={n_sea})",
                      file=sys.stderr)
    return grid, per_sql


def block_sql_type(block, pid_data):
    """取一个 sql_type 块内有名 PID 出现最多的 sql_type; 全无名返回 None。

    参数: block —— 一个 sql_type 块内的 PID 列表; pid_data —— {pid: rec}。
    返回: 多数 sql_type; 块内没有任何带 PREPARE 名的 PID(cache=0 块或日志截断)
          时返回 None, 由调用方回退到 config 顺序。多值同时打印告警。
    """
    counts = Counter()
    for p in block:
        st = rec_sql_type(pid_data[p])
        if st:
            counts[st] += 1
    if not counts:
        return None
    if len(counts) > 1:
        print(f"  (warn) 同一块内出现多个 sql_type: {dict(counts)}, 取多数",
              file=sys.stderr)
    return counts.most_common(1)[0][0]


def assign_roles(group, grid):
    """按 client 的 PID 顺序切角色: 每格只含各 searcher 的 search_number 个搜索 PID。

    整表前置预热已由 build_cells 在网格切片前剥离, 参数格不再含 warmup。

    参数: group —— 恰好 per_cell 个 PID(该格全部搜索 PID, 已按时间排序);
          grid —— 当前网格(取 search_number / searchers)。
    返回: {"warmup": [], "searches": [[pid...], ...]}; warmup 恒为空, 仅为兼容
          旧调用方保留。group 按 searcher 顺序每 sn 个一组切开。
    """
    roles = {"warmup": [], "searches": []}
    sn = grid["search_number"]
    for si in range(len(grid["searchers"])):
        # 第 si 个 searcher 的搜索 PID 段: [si*sn, (si+1)*sn); 调用方保证长度足够
        roles["searches"].append(group[si * sn: (si + 1) * sn])
    return roles


def build_cells(pids, pid_data, grid, per_sql=None):
    """按网格把有序 PID 切成 cell, 并用日志里的 PREPARE 名回填 sql_type。

    块长 per_sql = len(caches) * len(result_caches) * len(parse_modes) * per_cell;
    sql_type 序列取自 grid["sql_types"](已由 adapt_grid 按日志实际数量/顺序校正)。
    块内无名档(cache=0)继承该块的 sql_type。日志在块中间截断时, 保留已完整的
    格子, 丢弃末尾不足一格的 PID。
    返回 OrderedDict: (sql_type, cache, rc, pm) -> {"warmup":[pid], "searches":[[pid]]}

    参数:
        pids: 当前批次、按 ts_first 排序的 PID 列表(含前置预热)。
        pid_data: {pid: rec}(用于回填 sql_type, 不修改)。
        grid: 已校正的网格; per_sql: 每个 sql_type 块长度(PID 数), None 则按网格算。
    说明: 同一 (sql,cache,rc,pm) 若因日志 sql_type 重复而再次出现, 后者覆盖前者。
    """
    per_cell = grid["per_cell"]
    if per_sql is None:
        # 未显式给出块长时, 按 config 的 slots × per_cell 计算
        per_sql = (len(grid["caches"]) * len(grid["result_caches"])
                   * len(grid["parse_modes"]) * per_cell)
    cells = OrderedDict()
    # 整表前置预热(每表一次): 网格最前方 prewarm 个 PID 只是把索引装入内存, 不属于
    # 任何参数格, 按用户要求忽略, 从后续 PID 开始按 per_cell 切分参数格。
    pids = list(pids)
    prewarm = grid.get("prewarm", 0) or 0
    # 整表预热不属于任何参数格: 从头部切掉, 之后每 per_cell 个 PID 才对应一格
    if prewarm and len(pids) > prewarm:
        print(f"  (info) 忽略整表前置预热 PID: {','.join(str(p) for p in pids[:prewarm])}",
              file=sys.stderr)
        pids = pids[prewarm:]
    idx = 0
    # 每个 sql_type 占一段连续的 per_sql 个 PID
    for cfg_sql in grid["sql_types"]:
        block = pids[idx: idx + per_sql]
        if not block:
            break
        # cache=0 档无名, 用同块多数名回填; 整块也无名时沿用 config 顺序
        sql = block_sql_type(block, pid_data) or cfg_sql
        if sql != cfg_sql:
            print(f"  (warn) 日志块 sql_type={sql} 与 config 顺序 {cfg_sql} 不符, "
                  f"以日志为准", file=sys.stderr)
        off, truncated = 0, False
        # 块内按 cache -> result_cache -> parse_mode 的嵌套顺序逐格切 per_cell 个 PID
        for cache in grid["caches"]:
            for rc in grid["result_caches"]:
                for pm in grid["parse_modes"]:
                    group = block[off: off + per_cell]
                    if len(group) < per_cell:
                        # 不足一格: 该块余下格子及后续 sql 块都不再切分
                        truncated = True
                        break
                    off += per_cell
                    cells[(sql, cache, rc, pm)] = assign_roles(group, grid)
                if truncated:
                    break
            if truncated:
                break
        if truncated:
            print(f"  (info) 网格在 sql_type={sql} 处截断: 该块仅 {len(block)}/{per_sql} "
                  f"个 PID, 已保留其中完整的 {off // per_cell} 格, 后续块不再切分。",
                  file=sys.stderr)
            break
        idx += per_sql        # 前进到下一个 sql_type 块
    return cells


def log_grid(pids, pid_data):
    """config 不可用时的兜底网格: 仅凭日志语句名反推 sql_type 顺序。

    缓存档位无法从日志直接读出, 按 pgvector 约定的 0/1/2/3 与 2 个 PID/档假设。

    参数: pids —— 当前批次 PID(按时间排序); pid_data —— {pid: rec}。
    返回: 一个兜底 grid(dict), sql_types 取日志顺序, 全无名时为 ["unknown"];
          固定 caches=[0,1,2,3]、result_caches=[0]、per_cell=2、prewarm=1。
    """
    sqls = []
    for p in pids:
        st = rec_sql_type(pid_data[p])
        if st and st not in sqls:
            sqls.append(st)
    print(f"  (warn) 无法从 config 得到网格, 回退为日志推断: sql_types={sqls or ['unknown']} "
          f"caches=[0,1,2,3] result_caches=[0]", file=sys.stderr)
    return {
        "sql_types": sqls or ["unknown"],
        "caches": [0, 1, 2, 3],
        "result_caches": [0],
        "parse_modes": [""],
        "search_number": 1,
        "searchers": [{"params": {}}],
        "labels": ["-"],
        "has_warmup": True,
        "prewarm": 1,
        "per_cell": 2,
    }


def split_contiguous(pids, pid_data, gap_max=GAP_MAX):
    """按时间连续性把已按 ts_first 排序的 pids 切成段。

    相邻 PID 开始时间差 > gap_max 秒即视为批次边界。返回 list[list[pid]],
    每个子段为一次时间连续测试批次的 PID(保持原时序)。

    参数: pids —— 已按 ts_first 排序的 PID; pid_data —— {pid: rec};
          gap_max —— 切分阈值(秒, 由 gap_threshold() 向上调整过)。
    返回: list[list[pid]], 按时间顺序; pids 为空时返回 []。
    说明: 只比较相邻 PID 的 ts_first(秒级), 同一秒内不会切分; 本地时区解析。
    """
    def to_epoch(ts):
        """把 "YYYY-MM-DD HH:MM:SS" 转成 Unix 秒; 仅用于相邻 PID 的时间差比较。"""
        # 日志时间戳无时区, strptime 按本地时区解释; 仅用于相邻差值, 时区不影响结果
        return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp()

    groups, cur, prev = [], [], None
    for p in pids:
        ts = pid_data[p]["ts_first"]
        # 与上一个 PID 的间隔超过阈值 => 历史批次/重启碎片, 收束当前段另起一段
        if prev is not None and (to_epoch(ts) - to_epoch(prev)) > gap_max:
            groups.append(cur)
            cur = []
        cur.append(p)
        prev = ts
    if cur:
        groups.append(cur)
    return groups


def _param_match(row, searcher):
    """CSV 行与 searcher 的 parallel/top/ef_s 是否一致(任一侧缺省视为一致)。

    参数: row —— read_combos() 的一条 CSV 记录(字符串值);
          searcher —— expand_searchers() 的一个 searcher(ef_s 在内层 params)。
    返回: bool。只比较双方都有值的键, 任一侧为空/不可转数字则跳过该键;
          三个键都无冲突时返回 True。
    """
    for key in ("parallel", "top"):
        rv, sv = _num(row.get(key)), _num(searcher.get(key))
        if rv is not None and sv is not None and int(rv) != int(sv):
            return False
    rv = _num(row.get("ef_s"))
    sv = _num((searcher.get("params") or {}).get("ef_s"))
    if rv is not None and sv is not None and int(rv) != int(sv):
        return False
    return True


def combo_lookup(sql, cache, rc, pm, searcher, combos, table=None):
    """在结果 CSV 中按 (table?, sql_type, use_cache, use_query_cache, parse_mode) 匹配,
    优先取 parallel/top/ef_s 一致且时间戳最新的一行。

    参数:
        sql/cache/rc/pm: 日志侧推断出的组合键(cache 为 cache 档位, rc 为 0/1);
        searcher: 当前 searcher, 用于在候选里优先挑参数一致者(None 表示不筛);
        combos: read_combos() 的结果; table: 限定表名(None 表示不限)。
    返回: 命中的 CSV dict 或 None。时间戳为字符串 "YYYY-MM-DD HH:MM:SS", 按字典序
          取最大即最新; 同秒多条时取遍历到的最后一条。
    """
    cands = [
        c for c in combos
        if (not table or c.get("table") == table)
        and c.get("sql_type") == sql and c.get("cache") == cache
        and c.get("rc") == rc and c.get("parse_mode") == pm
    ]
    if not cands:
        return None
    if searcher is not None:
        params_hit = [c for c in cands if _param_match(c, searcher)]
        if params_hit:
            cands = params_hit
    return max(cands, key=lambda c: c.get("ts") or "")


# --------------------------------------------------------------------------- #
# 汇总
# --------------------------------------------------------------------------- #
def sq_drop_count(cache):
    """setup 阶段非向量 simple_query 条数的经验回退值。

    正常情况下由 setup_len() 从日志实测(首个子查询前导 sq): cache=0 -> 4 条,
    cache=1/2/3 -> 5 条 SET + 1 条 PREPARE = 6 条。仅当整个 PID 没有向量事件
    (空单元) 时无法实测, 用该回退值兜底。cache 未知时按 cache=0 处理。

    参数: cache —— cache 档位(int 或 None)。
    返回: 4(cache=0 或未知) / 6(cache=1/2/3), 单位条。
    """
    if cache is None:
        return 4
    return 6 if int(cache) in (1, 2, 3) else 4


def setup_len(rec, cache=None):
    """连接建立后、首个向量事件之前的非向量 simple_query 条数。

    实测优先: 数第一个 vi/hv/atv/eq 之前的 sq; 若该 PID 完全没有向量事件,
    回退 sq_drop_count(cache) 并受 sq 总数约束。

    参数: rec —— 单个 PID 的记录(events 为按行顺序的事件类型序列);
          cache —— 兜底用的 cache 档位。
    返回: 需要从 sq 列表前端剔除的条数(int)。events 里 sq 之后遇到任意非 sq 事件
          即停止计数并返回; 全为 sq(空单元)时取 min(前导数, 经验值, sq 总数)。
    """
    n = 0
    for kind in rec.get("events", []):
        if kind != "sq":
            return n          # 碰到首个向量事件, 之前累计的 sq 即 setup 语句
        n += 1
    # events 全是 sq: 没有向量事件可定位, 用经验值兜底但不超出实际 sq 条数
    return min(n, sq_drop_count(cache), len(rec.get("sq", [])))


def summarize(pid_recs, cache=None):
    """对一个 cell 的 1..N 个 PID 汇总(跨 PID 池化样本)。

    先按 setup_len() 剔除每个 PID 前端非向量 simple_query, 再对余下向量查询做
    耗时/次数统计; 各耗时均值沿用 avg() 的"去掉第一条"口径(去除首次热路径)。

    参数: pid_recs —— 单个 rec 或 rec 列表(同格多个 search PID 池化);
          cache —— 空单元时 setup 条数的兜底档位。
    返回: dict, 字段与单位:
        exec_count 向量查询条数; parse_avg/analyze_avg/plan_avg/exec_avg 为 sq 的
        四段毫秒均值; eq_count 为 EXECUTE 条数; eval_avg/gcp_avg 为 execute_query
        的毫秒均值; vi_count/vi_avg 向量转换(含 array_to_vector)次数与毫秒均值;
        hv_count/hv_avg halfvec_in 次数与毫秒均值。均值可能为 None(样本 <= 1)。
    """
    if isinstance(pid_recs, dict):
        pid_recs = [pid_recs]
    sq, eq, vi, hv = [], [], [], []
    for rec in pid_recs:
        # 剔除前端 SET/PREPARE 等 setup simple_query, 只保留真正的向量查询
        sq.extend(rec["sq"][setup_len(rec, cache):])
        eq.extend(rec["eq"])
        vi.extend(rec["vi"])
        hv.extend(rec["hv"])
    return {
        "exec_count": len(sq),                    # 剔除前端非向量语句后的向量查询条数
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
    }


def infer_plan_mode(pid_recs):
    """由 PLAN_CACHE_TRACE 动作推断实际计划模式: auto / custom / generic。

    参数: pid_recs —— 单个 rec 或 rec 列表(同格多个 search PID)。
    返回: "auto"(custom 与 generic 动作都有) / "custom" / "generic" / "-"
          (没有任何 trace 动作, 如 cache=0) / "mixed"(各 PID 推断结果不一致)。
    说明: BUILD_GENERIC 与 USE_GENERIC 一起归入 generic。
    """
    if isinstance(pid_recs, dict):
        pid_recs = [pid_recs]
    modes = set()
    for rec in pid_recs:
        a = rec.get("actions") or {}
        if not a:
            continue
        custom = a.get("USE_CUSTOM", 0)
        generic = a.get("USE_GENERIC", 0) + a.get("BUILD_GENERIC", 0)
        # 两种动作在同一次会话都出现 => auto; 否则按出现的那种归类, 都没有记 "-"
        modes.add("auto" if (custom and generic)
                  else "custom" if custom
                  else "generic" if generic else "-")
    modes.discard("-")                 # "-" 不参与 mixed 判定
    if not modes:
        return "-"
    return modes.pop() if len(modes) == 1 else "mixed"


def fmt(v, nd=3):
    """格式化数值: None -> "-"; 否则保留 nd 位小数(默认 3, 耗时单位 ms)。"""
    if v is None:
        return "-"
    return f"{v:.{nd}f}"


def main():
    """命令行入口: 解析日志 -> 推断被测表/网格 -> 切格子 -> 汇总并写 CSV。

    返回: 0 成功; 1 日志不存在、无 PGVECTOR_TIMING 记录或无有效格子。
    异常: 文件读取失败未捕获(会向上抛 OSError); 参数解析失败由 argparse 退出。
    """
    ap = argparse.ArgumentParser(description="汇总 pgvector 时序日志并与结果 CSV 交叉验证。")
    ap.add_argument("--log", default=LOG_PATH, help="PostgreSQL 日志路径")
    ap.add_argument("--csv", default=None,
                    help="pgvector 结果 CSV 路径(默认 results/csv/pgvector_benchmark_results.csv)")
    ap.add_argument("--out", default=None, help="输出 CSV 路径")
    ap.add_argument("--config", default=CONFIG_PATH, help="configurations/config.json 路径")
    ap.add_argument("--engine", default=None,
                    help="被测 pgvector 引擎名(默认按 PID 数与 config 网格自动推断)")
    ap.add_argument("--test-duration", type=float, default=None,
                    help="search 时长(秒), 默认取 config/CSV; 用于 est_QPS")
    ap.add_argument("--min-records", type=int, default=MIN_PID_RECORDS,
                    help="PID 最少计时记录数, 低于此值才剔除(默认 1, 即保留只有十余条"
                         "记录的截断 PID; 切勿用 40 之类的阈值, 否则 PID 数对不上网格)")
    ap.add_argument("--gap-max", type=float, default=GAP_MAX,
                    help="相邻 PID 开始时间差超过该秒数视为新批次, 只取最后一组连续批次(默认 61)")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print(f"[ERR] 日志不存在: {args.log}", file=sys.stderr)
        return 1

    csv_path = args.csv or os.path.join(RESULTS_DIR, "pgvector_benchmark_results.csv")
    cfg = load_config(args.config)
    pid_data = parse_log(args.log, min_records=args.min_records)
    combos = read_combos(csv_path)
    print(f"log: {args.log}  pids={len(pid_data)}  combos(csv)={len(combos)}",
          file=sys.stderr)

    # 日志内全部含计时记录的 PID 视为当前批次(pgvector 按 PID 区分批次, 非时间)。
    # n=0(只留下 PLAN_CACHE_TRACE)的 PID 不进入网格, 否则会多占格位导致整体错位。
    pids = sorted(
        [p for p, v in pid_data.items() if v["n"] > 0],
        key=lambda p: pid_data[p]["ts_first"])
    # 日志跨多轮未清空时会把多批次+PG 重启碎片混在一起; 按时间连续性切段, 只取
    # 最后一组连续批次(相邻 PID 开始时间差 <= gap_max 为同一批)。
    gap_max = gap_threshold(cfg, args.gap_max)
    if gap_max != args.gap_max:
        print(f"  (info) 批次切分阈值按 config 的 test_duration 提高到 {gap_max:.0f}s",
              file=sys.stderr)
    groups = split_contiguous(pids, pid_data, gap_max)
    pids = groups[-1] if groups else []        # 只分析最后一个连续批次
    if len(groups) > 1:
        print(f"  (info) 日志含 {len(groups)} 段时间连续批次, 仅取最后一组 "
              f"{len(pids)} 个 PID, 丢弃前 {sum(len(g) for g in groups[:-1])} 个",
              file=sys.stderr)
    if not pids:
        print("[ERR] 日志中未发现 PGVECTOR_TIMING 记录(日志可能已轮转/截断)。",
              file=sys.stderr)
        return 1

    # 日志实测信息: 实际跑到的 sql_type 序列(数量可变)与向量维度。
    obs_sqls = log_sql_order(pids, pid_data)
    dim = log_dim(pid_data, pids)

    # 被测表: 用 sql_type 集合 + 维度 + PID 数共同判定; 未命中再退化为日志推断。
    table = resolve_table(cfg, len(pids), args.engine, combos, obs_sqls, dim)
    if not table:
        table = next((c["table"] for c in combos if c["table"]), None)
        if table:
            print(f"  (warn) 未推断出与日志(sql_type={len(obs_sqls)}, dim={dim}) "
                  f"匹配的 pgvector 表, 复用 CSV 首表: {table}", file=sys.stderr)
    grid = config_grid(cfg, table) if table else None
    if not grid:
        grid = log_grid(pids, pid_data)       # config 不可用: 用日志/约定兜底
    # 以日志实际的 sql_type 数量/顺序为准, 并在必要时校正每格 PID 数。
    grid, per_sql = adapt_grid(grid, len(pids), obs_sqls or grid["sql_types"])
    # 输出文件名用数据集名: 优先 config 的 dataset, 其次表名, 最后 "unknown"
    dataset = next((i_.get("dataset") or i_.get("name") for i_ in cfg
                    if i_.get("name") == table), table or "unknown")
    config_td = config_test_duration(cfg, table) if table else None
    print(f"  table={table} dataset={dataset} dim={dim} "
          f"grid={len(grid['sql_types'])}sql x {len(grid['caches'])}cache x "
          f"{len(grid['result_caches'])}rc x {len(grid['parse_modes'])}mode "
          f"x {len(grid['searchers'])}searcher(sn={grid['search_number']}) "
          f"= {grid_pid_count(grid)} PID", file=sys.stderr)

    cells = build_cells(pids, pid_data, grid, per_sql)
    if not cells:
        print("[WARN] 无有效格子, 未生成 CSV。", file=sys.stderr)
        return 1
    if grid_pid_count(grid) != len(pids):
        print(f"  (warn) 日志 PID 数 {len(pids)} 与 config 网格预期 "
              f"{grid_pid_count(grid)} 不一致(截断/配置变更)", file=sys.stderr)

    # 表头: 所有 *_ms 为毫秒均值; exec#/vi#/hv# 为条数; est_QPS/CSV_RPS 为次/秒;
    # run_date 为 YYYY-MM-DD(取该 searcher 首个 PID 的日期, 不含时分秒)。
    header = ["#", "PID", "sql_type", "cache", "res_cache", "parse_mode", "proto",
              "exec#", "parse_ms", "analyze_ms", "plan_ms",
              "exec_ms", "eval_ms", "gcp_ms", "vi#", "vi_ms",
              "hv#", "hv_ms", "est_QPS", "CSV_RPS", "run_date",
              "plan_mode", "search_cfg"]

    rows, i = [], 0
    for key, cell in cells.items():
        sql, cache, rc, pm = key        # pm 为 "" 表示未配置/未记录 parse_mode
        # assigns_roles 已把每格的 PID 按 searcher 顺序切好; 一个 searcher 一行
        for si, s_pids in enumerate(cell["searches"]):
            if not s_pids:
                continue                # 网格校正后可能留下空 searcher 槽位
            s_recs = [pid_data[p] for p in s_pids]
            ss = summarize(s_recs, cache)   # 跨该 searcher 的 search PID 池化统计
            searcher = grid["searchers"][si] if si < len(grid["searchers"]) else None
            s_mode = infer_plan_mode(s_recs)

            # 缓存档位与 PLAN_CACHE_TRACE 动作交叉校验(cache=0 无 trace, 跳过)。
            expected_mode = {1: "auto", 2: "custom", 3: "generic"}.get(int(cache))
            if expected_mode and s_mode not in ("-", expected_mode):
                print(f"  (warn) {sql} cache={cache}: 日志动作推断为 {s_mode}, "
                      f"config 期望 {expected_mode}", file=sys.stderr)

            info = combo_lookup(sql, cache, rc, pm, searcher, combos, table)
            # 测试时长(秒)优先级: searcher 自身 -> 命令行/config -> 结果 CSV
            dur = _num((searcher or {}).get("test_duration"))
            if dur is None:
                dur = args.test_duration or config_td
            if dur is None:
                dur = _num((info or {}).get("test_duration"))
            rps = _num((info or {}).get("qps"))
            # search_number 个查询 PID 顺序执行, 每个各跑 dur 秒。
            est_qps = (ss.get("exec_count") or 0) / (dur * len(s_pids)) if dur else None

            i += 1
            if rps and est_qps and abs(est_qps - rps) / rps > 0.20:
                print(f"  (warn) #{i} {sql} cache={cache} rc={rc} mode={pm or '-'}: "
                      f"est_QPS={est_qps:.0f} 与 CSV_RPS={rps:.0f} 偏差 >20%",
                      file=sys.stderr)
            # 出现过 execute_query 即视为 prepared 协议; 否则为简单查询协议
            proto = "prep" if ss.get("eq_count") else "simple"
            rows.append([
                i, ",".join(s_pids), sql, cache, rc, pm, proto,
                ss.get("exec_count"),              # 向量查询条数
                fmt(ss.get("parse_avg")),          # ms, 去首样本均值
                fmt(ss.get("analyze_avg")),        # ms
                fmt(ss.get("plan_avg")),           # ms
                fmt(ss.get("exec_avg")),           # ms
                fmt(ss.get("eval_avg"), 4),        # ms, 保留 4 位(通常很小)
                fmt(ss.get("gcp_avg"), 4),         # ms, 保留 4 位
                ss.get("vi_count"),                # 向量转换次数(含 array_to_vector)
                fmt(ss.get("vi_avg")),             # ms
                ss.get("hv_count"),                # halfvec_in 次数
                fmt(ss.get("hv_avg")),             # ms
                f"{est_qps:.0f}" if est_qps else "-",   # 次/秒; 0/None 均输出 "-"
                f"{rps:.0f}" if rps else "-",           # 次/秒(取自结果 CSV)
                s_recs[0]["ts_first"][:10],        # 该 searcher 首 PID 的日期
                s_mode,
                grid["labels"][si] if si < len(grid["labels"]) else "-",
            ])

    if not rows:
        print("[WARN] 无有效格子, 未生成 CSV。", file=sys.stderr)
        return 1

    out_path = args.out or os.path.join(RESULTS_DIR, f"pgvector_log_analysis-{dataset}.csv")
    # newline="" 交给 csv 模块自行处理换行, 避免 Windows 风格空行; 所有字段转字符串
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow([str(x) for x in row])

    # 预期行数 = 格子数 × 每格 searcher 数; 不等通常意味着日志前缀/截断
    expected = grid_cell_count(grid) * len(grid["searchers"])
    msg = f"[OK] 已保存: {out_path}  表={table}  {len(rows)} 行"
    if len(rows) != expected:
        msg += f"  (预期 {expected} 行, 前缀/截断所致)"
    print(msg)
    return 0


if __name__ == "__main__":
    sys.exit(main())