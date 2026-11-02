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
- 布局自动检测: 日志里有名 PID 的 sql_type 序列按“前 1/3 与中 1/3 相同”判定为
  cache_major(cache 外层、sql_type 内层), 这是 pgvector-benchmark.sh 的循环顺序;
  否则为 sql_major(sql_type 外层、cache 内层), 对应 config/run.py 驱动的实验。
  cache_major 时直接按日志实测反推网格(sql_type 取日志名、cache 固定 0/1/2/3、
  每格 PID 数 = 搜索 PID 总数 / 格数), 忽略 config 的 search_params; 每格若有
  多个 PID(pgbench 主跑 + 延迟探针), 只取记录最多的主跑, 一格格一行。
- 被测表由三者共同判定: 日志 sql_type 集合、向量事件维度 dim(128/256/768/960)、
  网格 PID 数。恢复向量转换插桩后 dim 重新可得并参与表推断; 旧版(移除事件)
  日志 dim 为 None, 表推断退化为 sql_type 集合 + PID 数 + 结果 CSV 消歧
  (--engine 可显式指定)。
  --engine 也可直接传 PG 表名(benchmark_1536_1m), 会按向量维度映射到 config 条目。
  若 config 的 search_params 与实际不符, 再按日志 PID 数反推 per_cell
  与 searcher 数。搜索执行单元(向量查询 PID)按向量转换事件判定: 含
  vector_in/halfvec_in/array_to_vector 且事件数 >1; 连接初始化/整表预热/连跑
  多条非向量语句的终端一律剔除, 不再依赖记录数启发式(--min-records 仅作显式
  传入时的额外下限)。旧版无向量事件的日志自动回退记录数启发式。

要点:
- PID 即后端进程号, 每个测试执行单元新建连接 => 独立 PID; PGVECTOR_TIMING 行记账,
  PLAN_CACHE_TRACE 行只补充语句名/缓存动作, 不计入记录数。
- 向量查询与非向量查询的区分: 每条向量查询后紧随一条向量转换事件
  (vector_in/halfvec_in/array_to_vector), 非向量语句(SET/PREPARE/BEGIN/COMMIT)
  没有; summarize 按 "simple_query 后紧跟向量事件" 配对, 只统计真正的向量查询
  (setup 与间杂非向量语句一律剔除, 含延迟探针的 BEGIN/COMMIT)。
- 旧日志(曾移除向量事件的版本)无该标记: 退回"首个向量事件之前的前导 sq 条数"
  实测, 完全无向量事件的 PID(cache=0 旧日志)按经验值 sq_drop_count 兜底剥离。
- 数组类 SQL 类型在 cache=0 走 array_to_vector, 耗时与 vector_in 同口径计入 vi 列。

用法: python analyze_pgvector_log.py [--log ..] [--csv ..] [--out ..] [--engine ..]
      [--batches last|all|N] [--aggregate]
      多轮同负载日志(循环跑 N 次未清日志)用 --batches all --aggregate 按批次平均合并。

输入日志格式假设(与 client 打印格式强绑定, 改打印格式必须同步改正则):
- 行首为 "<日期> <时间>.<毫秒> <用户名> [PID]"; RE_TS 只取秒级时间戳, 毫秒丢弃。
- 计时行形如:
    PGVECTOR_TIMING] simple_query parse=<p>ms analyze=<a>ms plan=<pl>ms exec=<e>ms
    PGVECTOR_TIMING] execute_query eval_params=<e>ms get_cached_plan=<g>ms name=<stmt>
    PGVECTOR_TIMING] vector_in|halfvec_in|array_to_vector ... dim=<d> ... ms=<v>
  第三类(vector_in 系)为向量转换事件: 每条向量查询的计时行之后紧随一条,
  非向量语句没有 —— 据此按事件配对区分向量查询与非向量查询, dim= 用于判定
  被测表维度。旧版(移除该类)日志解析时自动回退条数启发式。
- 追踪行形如: [PLAN_CACHE_TRACE] ACTION=<动作> ... query=PREPARE <名字>(
  PREPARE 名后缀为 6 位及以上字母数字(client 用 6 位随机串, pgvector-benchmark.sh
  用 8~10 位数字), 正则按 `[a-z0-9]{6,}` 兼容两种。
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
    (旧版曾移除向量转换计时的日志下这四列为 0/"-", 恢复插桩后重新有值);
    est_QPS 由单条查询总耗时(parse+analyze+plan+exec) 的倒数 1000/总ms 估算(次/秒);
    CSV_RPS 取结果 CSV 的 rps(run.py)或 qps_avg(pgvector-benchmark.sh), 单位次/秒;
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
- est_QPS 是串行口径(单条总耗时的倒数); 多并发时实际吞吐会更高, 与 CSV_RPS
  (并发下的实测值)偏差 >20% 只告警不修正。
- 同一格在不同轮次的实际执行时长可能不同(实测有 10s/10000 条与 20s/20000 条),
  故 exec# 跨轮平均后可能不是整数(如 13335.3); 这不影响耗时均值与 est_QPS。
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
# vector_in/halfvec_in/array_to_vector: 向量转换事件, ".*?ms=" 非贪婪跳过中间的
# dim= 等字段, 组1 为耗时 ms。每条向量查询之后紧随一条该类事件, 是区分向量查询
# 与非向量查询(SET/PREPARE/BEGIN 等)的标记: summarize 按 "sq 后紧跟向量事件"
# 配对筛选向量查询(split_vector_sq); dim= 同时用于判定被测表维度。旧版插桩曾
# 移除这些事件, 正则与解析分支保留以兼容旧日志(无事件的 PID 走启发式兜底)。
RE_VI = re.compile(r"PGVECTOR_TIMING\] vector_in .*?ms=([\d.]+)")
RE_HV = re.compile(r"PGVECTOR_TIMING\] halfvec_in .*?ms=([\d.]+)")
# array_int_cast / array_real_cast / array_cast / array_to_vector_fn / with_array_cast
# 这些 SQL 类型把实参经 ARRAY[...] / real[] 数组转换语义走 array_to_vector(源码
# pgvector/src/vector.c 的 array_to_vector 已输出 PGVECTOR_TIMING), 而非 vector_in,
# 因此 cache=0 时应作为“向量转换耗时”计入 vi, 与 vector_in 列口径一致。
# 组1 = array_to_vector 耗时 ms。
RE_ATV = re.compile(r"PGVECTOR_TIMING\] array_to_vector .*?ms=([\d.]+)")
# 向量解析事件都带 dim=<维度>; 组1 为维度, 用于按维度判定被测表(128/256/768/960...)。
# 恢复向量转换插桩后 dim 重新可得并参与 resolve_table 表推断; 旧版(移除事件)
# 日志 dim 为 None, 表推断仅靠 --engine / 日志 sql_type / PID 数 / 结果 CSV 消歧。
RE_DIM = re.compile(r"\bdim=(\d+)")

# 计划缓存追踪: 动作计数用于校验 cache 档位, PREPARE 语句名用于判定 sql_type。
# 组1 = 动作名, 如 USE_CUSTOM / USE_GENERIC / BUILD_GENERIC(用于 infer_plan_mode)。
RE_TRACE_ACTION = re.compile(r"\[PLAN_CACHE_TRACE\] ACTION=(\w+)")
# 从 trace 行里抓 PREPARE 语句名; 组1 为名字, 末尾的 "("(参数列表起点)被吃掉。
RE_TRACE_PREPARE = re.compile(r"query=PREPARE\s+(\S+?)\s*\(")
# PREPARE 语句名: pgv_ps_<sql_type>_<随机后缀>
# 后缀实测有两种: client 的 6 位小写字母/数字, 以及 pgvector-benchmark.sh 的
# 8~10 位纯数字随机串(如 pgv_ps_text_literal_357327506), 故放宽为 6 位及以上。
# 组1 = sql_type。`.+` 贪婪匹配, 保证剥掉的是最后一段随机后缀; 名字不合格式返回 None。
RE_PREPARED_NAME = re.compile(r"^(?:pgv_ps|polardb_ps)_(.+)_[a-z0-9]{6,}$")

# 单个 PID 至少应含的 PGVECTOR_TIMING 记录数。[兼容保留] 记录数不再是搜索/非搜索
# PID 的判定依据(同一终端可能连跑多条非向量语句, 记录数无法区分), 判定改由向量
# 转换事件数承担(见 MIN_VECTOR_EVENTS / is_search_pid); 本阈值仅在显式传入 >1 时
# 作为额外的计时行数下限过滤, 默认 1 = 不按记录数过滤。
MIN_PID_RECORDS = 1

# 相邻 PID 开始时间差超过该秒数视为一次新的独立测试批次(日志因未清空会累积多轮
# 运行与 PG 重启碎片)。同一批次内相邻 PID 的间隔约为一个查询时长(预热->search 约
# 5s, search_i -> search_{i+1} 约 test_duration), 故阈值取
# max(GAP_MAX, 本配置最大 test_duration + 11), 见 gap_threshold()。
GAP_MAX = 61

# ---- 搜索执行单元(向量查询 PID)判定 ----
# 向量转换事件类型(事件序列里的标记名): vector_in/halfvec_in/array_to_vector。
# 每条向量查询之后服务端紧随一条该类事件; 非向量语句(SET/PREPARE/BEGIN/COMMIT、
# 同一终端连跑的多条非向量语句等)没有 —— 据此区分向量查询与非向量查询。
VECTOR_MARKS = ("vi", "hv", "atv")
# 判定为搜索执行单元所需的最少向量转换事件数(用户口径: 含向量事件且数量不只
# 一个, 即 >1)。连接初始化/整表预热/纯非向量终端的事件数为 0, 偶发单条向量
# 查询的终端同样排除。
MIN_VECTOR_EVENTS = 2

# 判定 auto 档的判据: USE_CUSTOM 相对 BUILD_GENERIC 的比值(每个 PREPARE 平均被
# PG 选为 custom plan 的次数)。实测(主跑 / 探针):
#   auto 档:      USE_CUSTOM/BUILD_GENERIC ≈ 3.0  (18/6, 6/2)
#   generic 档:   USE_CUSTOM/BUILD_GENERIC ≈ 0.5  (3/6, 1/2)
#   custom 档:    USE_GENERIC=0, USE_CUSTOM≈1000+
# 不能用 USE_CUSTOM 的绝对次数当判据: 执行时间短时 generic 档主跑每个事务也会
# 留 1 次 USE_CUSTOM, 累积到 3 次就会越过">=3"阈值被误判成 auto (cache=3 消失)。
AUTO_CUSTOM_PER_BUILD = 2.0


def vector_event_count(rec):
    """一个 PID 的向量转换事件(vector_in/halfvec_in/array_to_vector)条数。"""
    return sum(1 for k in rec.get("events", ()) if k in VECTOR_MARKS)


def is_search_pid(rec):
    """判定 PID 是否向量搜索执行单元: 向量转换事件数 >= MIN_VECTOR_EVENTS(即 >1)。

    取代旧的记录数/sq 条数启发式(MIN_PID_RECORDS、"sq<=1 且无 execute_query"、
    头部 sq<100 预热跳过等): 同一终端可能连跑多条非向量语句, 记录数无法区分;
    向量转换事件只随向量查询出现, 是可靠标记。
    """
    return vector_event_count(rec) >= MIN_VECTOR_EVENTS


def vector_mode(pid_data):
    """日志整体是否为带向量转换事件的插桩版本。

    旧版插桩(曾移除 vector_in 系事件)的日志里任何 PID 都没有向量事件, 此时
    is_search_pid 会把全部 PID 判为非搜索 —— 调用方需回退旧的记录数启发式。
    返回: True = 新插桩日志(按向量事件判定); False = 旧日志(回退启发式)。
    """
    return any(vector_event_count(r) > 0 for r in pid_data.values())


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


# 结果 CSV 有两种来源, 表头不同, 这里统一成日志侧的 cache/rc/qps:
#   - run.py 客户端 -> pgvector_benchmark_results.csv(下划线):
#       use_cache(0~3) / use_query_cache(0/1) / vector_in_parse_mode / rps
#   - pgvector-benchmark.sh -> pgvector-benchmark-results.csv(连字符):
#       plan_mode(raw/prepare_*) / result_mode(off/on) / (无 parse_mode) / qps_avg
PLAN_MODE_TO_CACHE = {"raw": 0, "prepare_auto": 1,
                      "prepare_custom": 2, "prepare_generic": 3}


def _row_cache_rc(r):
    """从结果 CSV 的一行取 (cache, rc); 兼容两种表头, 无法解析返回 (None, None)。

    参数: r —— csv.DictReader 的一行。
    返回: (cache, rc) 两个 int; 若既无 use_cache/use_query_cache 也无
          plan_mode/result_mode(汇总行等), 返回 (None, None)。
    """
    try:
        return (int((r.get("use_cache") or "").strip()),
                int((r.get("use_query_cache") or "").strip()))
    except (TypeError, ValueError):
        pass
    cache = PLAN_MODE_TO_CACHE.get((r.get("plan_mode") or "").strip().lower())
    rc = {"on": 1, "off": 0}.get((r.get("result_mode") or "").strip().lower())
    return cache, rc


def read_combos(csv_path):
    """读取 pgvector 结果 CSV, 返回可按 (sql_type, cache, rc, parse_mode) 匹配的列表。

    参数:
        csv_path: 结果 CSV 路径(默认 results/csv/pgvector-benchmark-results.csv)。
    返回:
        list[dict], 每行一个 dict, 键为 table/dataset/vector_size/sql_type/cache/rc/
        parse_mode/qps/test_duration/parallel/top/ef_s/ts。
        - 兼容两种表头(见 _row_cache_rc): run.py 与 pgvector-benchmark.sh 的结果
          CSV 都能读; cache 由 use_cache 或 plan_mode 归一, rc 由 use_query_cache
          或 result_mode 归一。
        - qps 取自 rps 列(run.py)或 qps_avg 列(bash 脚本), 单位次/秒。
        - sql_type 为空或 cache/rc 无法解析的行被静默跳过(汇总行、残缺行)。
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
            cache, rc = _row_cache_rc(r)
            if cache is None or rc is None:
                continue
            combos.append({
                "table": ((r.get("experiment_name") or r.get("table_name")
                           or "").strip() or "pgvector"),
                "dataset": (r.get("dataset") or "").strip(),
                "vector_size": ((r.get("vector_size") or r.get("dimension")
                                 or "").strip()),
                "sql_type": sql, "cache": cache, "rc": rc,
                "parse_mode": (r.get("vector_in_parse_mode") or "").strip(),
                "qps": ((r.get("rps") or r.get("qps_avg") or "").strip()),
                "test_duration": ((r.get("test_duration") or r.get("elapsed_avg_s")
                                   or "").strip()),
                "parallel": ((r.get("parallel") or r.get("concurrency")
                              or "").strip()),
                "top": ((r.get("top") or r.get("top_k") or "").strip()),
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


# --------------------------------------------------------------------------- #
# --engine 传 PG 侧表名时的映射
# --------------------------------------------------------------------------- #
# gen_step_time_report.py 传的是 PG 里的表名(benchmark_1536_1m 等), 而 config 的
# name 是 pgvector-dbpedia-openai-1000k-angular; 二者靠"向量维度"对应:
#   benchmark_1536_1m -> 1536 -> datasets.json 中 dbpedia-openai-1000k-angular
#   -> config 中 dataset 为该数据集名的 pgvector 条目。
DATASETS_PATH = os.path.join(BASE_DIR, "datasets", "datasets.json")


def load_datasets(path=DATASETS_PATH):
    """读取 datasets/datasets.json(容错), 返回 list[dict]; 失败返回 []。"""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def table_dim(table):
    """从 PG 表名解析向量维度: benchmark_1536_1m -> 1536, benchmark_sift_128 -> 128。

    参数: table —— PG 表名(benchmark_<维度>_<条数> 或 benchmark_sift_<维度>)。
    返回: 维度 int; 名字不符合约定时返回 None。
    """
    m = re.match(r"benchmark_(?:sift_)?(\d+)", table or "", re.I)
    return int(m.group(1)) if m else None


def match_engine_by_table(cfg, table):
    """把 PG 表名映射到 config 的 pgvector 条目 name。

    匹配顺序:
      1. 本身就是 config 的 name -> 原样返回;
      2. 解析表名维度, 用 datasets.json 找同维度的数据集名, 再找 dataset 命中该
         数据集的 pgvector 条目;
      3. datasets.json 不可用时退回 dataset_dim() 解析条目 dataset 名里的维度。

    参数: cfg —— config 条目列表; table —— --engine 传入的表名/引擎名。
    返回: 命中的 config name; 无法映射时返回 None(调用方保持原值走日志推断)。
    """
    if not table:
        return None
    if any(i.get("name") == table for i in cfg):
        return table
    dim = table_dim(table)
    if dim is None:
        return None
    ds_names = {d.get("name") for d in load_datasets()
                if d.get("vector_size") == dim}
    hits = [i.get("name") for i in cfg
            if str(i.get("engine")) == "pgvector"
            and (str(i.get("dataset")) in ds_names
                 or dataset_dim(i.get("dataset")) == dim)]
    if not hits:
        return None
    if len(hits) > 1:
        print(f"  (warn) 表名 {table} 匹配到多个引擎: {hits}; 取首个", file=sys.stderr)
    return hits[0]


def resolve_table(cfg, n_pids, target=None, combos=None, observed_sqls=None, dim=None):
    """推断被测 pgvector 表。

    优先 target; 否则在 config 的 pgvector 条目中依次筛选:
      1. 日志里出现的 sql_type 必须都能在该表 config 的 sql_type 中找到
         (config 多出的类型可容忍: 可能是本轮还没跑到的后续块);
      2. 日志向量事件的维度必须与表维度一致 —— 维度取结果 CSV 的 vector_size,
         无 CSV 行时回退数据集名(128/256/768/960...); 旧日志无向量事件
         (dim=None) 时跳过维度校验, 仅靠其余条件消歧;
      3. 网格 PID 数优先精确匹配 n_pids; 若是测试尚未跑完的前缀, 取预期 PID 数
         最小且大于 n_pids 的候选;
      4. 仍多解时用结果 CSV 的 experiment_name 消歧。

    参数:
        cfg: config 条目列表; n_pids: 日志中当前批次的 PID 数(D 段后的 pids);
        target: --engine 显式指定的表名, 命中即直接返回;
        combos: read_combos() 的结果, 提供 vector_size/table 线索;
        observed_sqls: 日志实际出现的 sql_type 顺序;
        dim: 日志向量事件(vi/hv/atv)中最常见的维度; 旧版无向量事件的日志为 None。
    返回: 表名或 None(找不到任何候选)。
    """
    if target:
        # --engine 可能是 PG 表名(benchmark_1536_1m), 先映射到 config 条目名;
        # 映射不到时保留原值(后续 config_grid 返回 None, 由日志推断兜底)。
        mapped = match_engine_by_table(cfg, target)
        if mapped and mapped != target:
            print(f"  (info) --engine {target} 按向量维度映射到 config 条目: {mapped}",
                  file=sys.stderr)
        return mapped or target
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
        print(f"  (warn) 多张表同时匹配(pid={n_pids}, "
              f"dim={dim if dim is not None else '无(日志无向量事件)'}, "
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
          前缀, 且以 6 位及以上字母数字随机后缀结尾)时返回 None。
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
    统计每个 PID 的计时记录数 n。记录数不用于区分搜索/非搜索 PID(同一终端可能
    连跑多条非向量语句): 判定由向量转换事件数承担(is_search_pid, vector_in/
    halfvec_in/array_to_vector 事件数 >1); min_records 仅在显式 >1 时作额外下限。
    PLAN_CACHE_TRACE 行不计入 n。rec["dims"] 汇总向量解析事件的维度。

    参数:
        log_path: 服务端日志路径。
        min_records: 额外的计时行数下限(仅在 >1 时生效), 默认 1 = 不按记录数过滤。
    返回:
        OrderedDict {str(pid): rec}, 按 PID 首次出现顺序; 所有耗时单位为 ms,
        events 保留各计时事件的按行顺序(供 split_vector_sq 事件配对与 setup_len
        前导判断)。
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
    # 记录数不再是搜索/非搜索的判定依据(同一终端可能连跑多条非向量语句);
    # 默认 min_records=1 时不过滤, 搜索执行单元由调用方按向量转换事件数判定
    # (见 is_search_pid)。仅当显式传入 >1 的阈值时作为额外的计时行数下限。
    if min_records > 1:
        dropped = [(pid, rec["n"]) for pid, rec in data.items()
                   if rec["n"] < min_records]
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


def detect_layout(pids, pid_data):
    """判断日志的格子排列方式: "cache_major"(cache 外层) 或 "sql_major"(sql_type 外层)。

    pgvector-benchmark.sh(pgvector-benchmark.sh 的主循环)是 cache 外层、sql_type
    内层; config/run.py 驱动的实验则是 sql_type 外层、cache 内层。两种布局的日志
    必须用不同切分方式, 否则所有格子都会错位。

    判据: 折叠有名 PID 的连续同名段(同一格的 pgbench 主跑 + 探针、同档多连接)后,
    cache_major 每跑完一整轮 sql_type 再进入下一档, 段序列是同一有序类型表的重复;
    sql_major 同一 sql_type 的各 cache 档连续, 折叠后每个类型只出现一次。故若
    段序列至少重复两轮且前两轮完全相同, 判 cache_major。这比旧的"前 1/3 与中 1/3
    相同"稳健: 后者在日志含多轮测试或部分轮次时会被打乱。

    参数: pids —— 当前批次 PID(按时间排序); pid_data —— {pid: rec}。
    返回: "cache_major" / "sql_major"(序列太短或只有一轮有名块时保守返回 sql_major)。
    """
    seq = [rec_sql_type(pid_data[p]) for p in pids]
    seq = [s for s in seq if s]
    if len(seq) < 4:
        return "sql_major"
    runs = []
    for s in seq:
        if not runs or runs[-1] != s:
            runs.append(s)
    types = []
    for s in runs:
        if s not in types:
            types.append(s)
    t = len(types)
    if t >= 2 and len(runs) >= 2 * t and runs[:t] == runs[t:2 * t]:
        return "cache_major"
    return "sql_major"


def log_grid_cache_major(pids, pid_data):
    """按 cache 外层 / sql_type 内层布局反推网格(pgvector-benchmark.sh 的日志)。

    该脚本每个 (cache 档, sql_type) 组合产生 2 个 PID(pgbench 主跑 + 延迟探针),
    cache 档位固定为 0/1/2/3(4 档), sql_type 取自日志里的 PREPARE 名。

    参数: pids —— 当前批次 PID(按时间排序); pid_data —— {pid: rec}。
    返回: grid(dict, layout="cache_major"); 无有名 PID 或搜索 PID 不足以切出
          至少一格时返回 None(由调用方回退到 config 网格或旧兜底)。
    """
    sqls = []
    for p in pids:
        st = rec_sql_type(pid_data[p])
        if st and st not in sqls:
            sqls.append(st)
    if not sqls:
        return None
    # 搜索 PID 按向量转换事件判定(事件数 >1); 旧版无向量事件的日志回退
    # "连接初始化(sq<=1 且无 execute_query)不是搜索 PID"启发式
    if vector_mode(pid_data):
        search_pids = [p for p in pids if is_search_pid(pid_data[p])]
    else:
        search_pids = [p for p in pids
                       if not (len(pid_data[p]["sq"]) <= 1 and not pid_data[p]["eq"])]
    caches = [0, 1, 2, 3]
    slots = len(caches) * len(sqls)
    if not search_pids or len(search_pids) < slots:
        return None
    per_cell = len(search_pids) // slots
    if per_cell < 1:
        return None
    if len(search_pids) % slots:
        print(f"  (warn) cache_major 布局: {len(search_pids)} 个搜索 PID 不能被 "
              f"{len(caches)}cache x {len(sqls)}sql 整除, 按每格 {per_cell} 个切分并"
              f"丢弃余数", file=sys.stderr)
    print(f"  (info) 检测到 cache_major 布局(pgvector-benchmark.sh 日志): "
          f"{len(sqls)}sql x {len(caches)}cache x 每格 {per_cell} PID", file=sys.stderr)
    return {
        "layout": "cache_major",
        "sql_types": sqls,
        "caches": caches,
        "result_caches": [0],
        "parse_modes": [""],
        "search_number": 1,
        "searchers": [{"params": {}}],
        "labels": ["-"],
        "has_warmup": False,
        "prewarm": 0,
        "per_cell": per_cell,
    }


def build_cells_cache_major(pids, pid_data, grid):
    """按 cache 外层 / sql_type 内层切格, 每格一行(适配 pgvector-benchmark.sh 日志)。

    每格含 per_cell 个 PID(实测为 pgbench 主跑 + 延迟探针): 只取记录数最多的主跑
    参与统计, 探针不混入均值, 保证每格恰好一行。cache=0 档无名, 按 grid 的
    sql_type 顺序回填; 有名档用日志实际 sql_type 校验顺序。

    参数: pids —— 当前批次 PID(按时间排序); pid_data —— {pid: rec}; grid ——
          log_grid_cache_major() 返回的网格。
    返回: OrderedDict: (sql_type, cache, 0, "") -> {"warmup": [], "searches": [[pid]]}
    """
    cells = OrderedDict()
    per_cell = max(1, int(grid.get("per_cell") or 1))
    # 搜索 PID 按向量转换事件判定; 旧版日志回退连接初始化启发式(同 log_grid_cache_major)
    if vector_mode(pid_data):
        pids = [p for p in pids if is_search_pid(pid_data[p])]
    else:
        pids = [p for p in pids
                if not (len(pid_data[p]["sq"]) <= 1 and not pid_data[p]["eq"])]
    idx, truncated, cache = 0, False, 0
    for cache in grid["caches"]:
        for cfg_sql in grid["sql_types"]:
            group = pids[idx: idx + per_cell]
            if len(group) < per_cell:
                truncated = True
                break
            idx += per_cell
            sql = block_sql_type(group, pid_data) or cfg_sql
            if sql != cfg_sql:
                print(f"  (warn) 日志块 sql_type={sql} 与推断顺序 {cfg_sql} 不符, "
                      f"以日志为准", file=sys.stderr)
            main_pid = max(group, key=lambda p: len(pid_data[p]["sq"]))
            cells[(sql, cache, 0, "")] = {"warmup": [], "searches": [[main_pid]]}
        if truncated:
            break
    if truncated:
        print(f"  (info) cache_major 布局在 cache={cache} 处截断, 已保留 "
              f"{len(cells)} 格", file=sys.stderr)
    return cells


def build_cells_sql_dynamic(pids, pid_data, raw_phase=None):
    """完全日志驱动的动态切格(不依赖 config 网格)。

    适用于残缺/中断的日志与 SQL_TYPES 动态变化的情况:
      - sql_type 块按日志 PREPARE 名的实际出现顺序切分(数量/顺序动态);
      - 搜索执行单元按向量转换事件判定(含 vector_in/halfvec_in/array_to_vector
        且事件数 >1), 连接初始化/整表预热/非向量终端一律剔除;
      - 块内每个 PID 按其 PLAN_CACHE_TRACE 动作(auto/custom/generic)分档,
        动作映射 cache=1/2/3;
      - 每格收编该 (sql_type, cache) 档实际跑过的全部 PID —— 执行次数(轮数)
        完全由日志决定, 1 个 PID 也成格, 不要求与 config 网格对齐。

    参数: raw_phase —— raw 档(无名 PID)的归属方式:
      - None(sql_major 布局): 无名 PID 挂起, 由随后出现的同名块收编
        (run.py 驱动时同一 sql_type 的 cache=0 档紧邻 cache>0 档);
      - "head"(cache_major 布局, pgvector-benchmark.sh): raw 档(cache=0)没有
        PREPARE 名, 全部无名 PID 都是它。位置可能是全部有名块之前, 也可能夹在
        有名块中间(取决于 CACHE_PROFILES 顺序: prepare_auto_off 排在 raw_off
        之前时, raw 的 sql_type 只能由前面的 auto 块提供), 日志未清空时还会混有
        多轮测试。故按"连续无名段"收集, 每段独立对应一遍 sql_type 顺序: 首个是
        整表预热(每表一次), 其余按 "主跑 + 探针" 两两一组(只跑 psql 探针时
        1 个 PID 一格)按序对应到日志里出现的 sql_type。若不这样单独处理, 中间
        那段会被随后第一个有名块整段收编, raw 只解析出一组并造成格子错位。
    返回: (cells OrderedDict, 动态 grid dict)。
    """
    # 搜索执行单元按向量转换事件判定: 含 vector_in/halfvec_in/array_to_vector 且
    # 事件数 >1。连接初始化/整表预热/连跑多条非向量语句的终端一律剔除, 不再依赖
    # 记录数启发式(同一终端的非向量语句条数不定, 记录数无法区分)。
    if vector_mode(pid_data):
        ps = [p for p in pids if is_search_pid(pid_data[p])]
        dropped = [p for p in pids if not is_search_pid(pid_data[p])]
        if dropped:
            print(f"  (info) 忽略 {len(dropped)} 个非向量查询 PID(无 vector_in/"
                  f"halfvec_in/array_to_vector 或事件数 <=1): "
                  f"{','.join(str(p) for p in dropped[:8])}"
                  f"{'...' if len(dropped) > 8 else ''}", file=sys.stderr)
    else:
        # 旧版插桩日志(全无向量事件): 回退记录数启发式 —— 连接初始化
        # (sq<=1 且无 execute_query)与头部无名低记录 PID(整表预热)剔除
        def is_init(p):
            r = pid_data[p]
            return len(r["sq"]) <= 1 and not r["eq"]

        ps = [p for p in pids if not is_init(p)]
        k = 0
        while (k < len(ps) and not rec_sql_type(pid_data[ps[k]])
               and len(pid_data[ps[k]]["sq"]) < 100):
            k += 1
        if k:
            print(f"  (info) 忽略整表前置预热 PID: {','.join(str(p) for p in ps[:k])}",
                  file=sys.stderr)
            ps = ps[k:]

    # cache_major 布局下 raw 档(cache=0)没有 PREPARE 名, 所有无名 PID 都属于它。
    # raw 档在日志里的位置随 CACHE_PROFILES 顺序而变: raw_off 第一个时在全部有名
    # 块之前; prepare_auto_off 排在 raw_off 之前时(raw 的 sql_type 只能由它提供)
    # 则夹在有名块中间。日志未清空时还会混有多轮测试, 每轮 raw 档是一段连续无名
    # PID。因此按"连续无名段"收集, 每段独立按 sql_type 顺序对应, 而不是整段塞给
    # 随后的第一个有名块(那会只解析出一组 raw 并造成格子错位)。
    raw_runs = []
    if raw_phase == "head":
        # 用户口径: 从首次出现 prepare_* 有名块开始记录。之前的无名 PID 是连接
        # 初始化与整表预热(如 WARMUP_TIMELIMIT 的预热 PID), 一律丢弃。
        first_named = next((i for i, p in enumerate(ps)
                            if rec_sql_type(pid_data[p])), None)
        if first_named:
            print(f"  (info) 丢弃首个有名块之前的 {first_named} 个无名 PID"
                  f"(连接初始化/整表预热): "
                  f"{','.join(str(p) for p in ps[:first_named])}", file=sys.stderr)
            ps = ps[first_named:]
        cur = []
        for p in ps:
            if rec_sql_type(pid_data[p]):
                if cur:
                    raw_runs.append(cur)
                    cur = []
            else:
                cur.append(p)
        if cur:
            raw_runs.append(cur)
        ps = [p for p in ps if rec_sql_type(pid_data[p])]

    # 有名 PID 按名连续分段; sql_major 布局下无名 PID 挂起, 由随后出现的同名块
    # 收编(同一 sql_type 的 raw 档紧邻其 prep 档)。
    blocks = []            # {"sql": 名, "pids": [有名], "raw": [无名]}
    pending_raw = []
    for p in ps:
        st = rec_sql_type(pid_data[p])
        if st:
            if blocks and blocks[-1]["sql"] == st:
                blocks[-1]["pids"].append(p)
            else:
                blocks.append({"sql": st, "pids": [p], "raw": []})
            if pending_raw:
                blocks[-1]["raw"].extend(pending_raw)
                pending_raw = []
        else:
            pending_raw.append(p)
    if pending_raw:
        blocks.append({"sql": "unknown", "pids": [], "raw": list(pending_raw)})
        print(f"  (warn) {len(pending_raw)} 个无名 PID 后无同名块, 归入 unknown 块 "
              f"(raw 档 sql_type 未知)", file=sys.stderr)

    # 前导 raw 组按序对应日志中的 sql_type 顺序(raw 阶段与 prep 阶段都按
    # SQL_TYPES 顺序执行); 超出部分归入 unknown 兜底格。需在 raw 分组之前算出,
    # 因为"每组合 1 个 PID 还是 2 个"的判据要用到 sql_type 个数。
    sql_order = []
    for b in blocks:
        if b["sql"] != "unknown" and b["sql"] not in sql_order:
            sql_order.append(b["sql"])

    # 每个连续无名段(raw 档一轮)分组: 主跑 + 探针每组合固定 2 个 PID, 按段内顺序
    # 两两配对。整表预热(每次 pgvector-benchmark.sh 调用一次, 单连接)在搜索 PID
    # 流里表现为长度为 1 的无名段, 按段长剔除; 日志尾部未跑完的 raw 段整段跳过。
    raw_group_sets = []
    cnt = {p: vector_event_count(pid_data[p]) for run in raw_runs for p in run}
    # 每轮 raw 档应含 2 * sql_type 数 个 PID(主跑 + 探针, 每组合 2 个)。注意预热
    # PID 不只出现在日志开头: 每次 pgvector-benchmark.sh 调用都会先做 WARMUP_TIMELIMIT
    # 的整表预热(单连接, 只留 1 个无名搜索 PID), 它夹在各轮 raw 段之间, 段长为 1。
    expected_raw = 2 * len(sql_order)
    for ridx, run in enumerate(raw_runs):
        if not run:
            continue
        if len(run) == 1:
            print(f"  (info) 跳过 raw 段中的单 PID 整表预热 {run[0]}"
                  f"({cnt[run[0]]} 条向量查询, 非主跑+探针组合), 不参与分格",
                  file=sys.stderr)
            continue
        if ridx == len(raw_runs) - 1 and len(run) < expected_raw:
            print(f"  (info) raw 档最后一轮仅 {len(run)}/{expected_raw} 个 PID"
                  f"(尚未跑完), 整轮跳过", file=sys.stderr)
            continue
        groups = [run[i:i + 2] for i in range(0, len(run), 2)]
        if len(run) % 2 == 1:
            print("  (warn) raw 档 PID 数为奇数, 丢弃最后一个未配对 PID", file=sys.stderr)
            groups = groups[:-1]
        raw_group_sets.append(groups)

    cells = OrderedDict()

    def _add_raw(key_sql, grp):
        """raw 档(cache=0)格子: 同名格已存在时并入, 避免 unknown 兜底格被覆盖。"""
        key = (key_sql, 0, 0, "")
        if key in cells:
            cells[key]["searches"][0].extend(grp)
        else:
            cells[key] = {"warmup": [], "searches": [list(grp)]}

    def _add_named(key_sql, cache, pids):
        """prep 档格子: 同名同档位格已存在时并入(日志未清空时含多轮, 需按轮累加,
        而不是用最后一轮的块覆盖前面的轮次)。"""
        key = (key_sql, cache, 0, "")
        if key in cells:
            cells[key]["searches"][0].extend(pids)
        else:
            cells[key] = {"warmup": [], "searches": [list(pids)]}

    mode_to_cache = {"auto": 1, "custom": 2, "generic": 3}
    for b in blocks:
        sql = b["sql"]
        if b["raw"]:
            _add_raw(sql, b["raw"])
        # 同一块内(连续同名)的 PID 是同一档位的主跑 + 探针, 档位必须按整块
        # 合并判定: 逐 PID 判时, 探针首次执行只留 1 次 USE_CUSTOM 会被误判成
        # generic, 同一组被拆成 cache=1 与 cache=3 两行。
        act = infer_plan_mode([pid_data[p] for p in b["pids"]])
        if act in mode_to_cache:
            _add_named(sql, mode_to_cache[act], b["pids"])
        else:
            print(f"  (warn) sql_type={sql}: {len(b['pids'])} 个 PID 无计划缓存动作"
                  f"痕迹, 合并为一格(cache 标记为 1)", file=sys.stderr)
            _add_named(sql, 1, b["pids"])

    # 每段 raw 组都从 sql_order 头部开始对应(同一轮 raw 档按 SQL_TYPES 顺序执行);
    # 超出部分(raw 阶段多跑/顺序不符)归入 unknown。
    if raw_group_sets:
        leftover, n_excess = [], 0
        for groups in raw_group_sets:
            for i, grp in enumerate(groups):
                if i < len(sql_order):
                    _add_raw(sql_order[i], grp)
                else:
                    n_excess += 1
                    leftover.extend(grp)
        if leftover:
            print(f"  (warn) raw 档有 {n_excess} 组超出日志 sql_type 数"
                  f"({len(sql_order)}), {len(leftover)} 个 PID 归入 unknown",
                  file=sys.stderr)
            _add_raw("unknown", leftover)

    sqls = []
    for (sql, _c, _r, _m) in cells:
        if sql not in sqls:
            sqls.append(sql)
    dyn_grid = {
        "layout": "dynamic",
        "sql_types": sqls or ["unknown"],
        "caches": sorted({c for (_s, c, _r, _m) in cells}) or [0],
        "result_caches": [0],
        "parse_modes": [""],
        "search_number": 1,
        "searchers": [{"params": {}}],
        "labels": ["-"],
        "has_warmup": False,
        "prewarm": 0,
        "per_cell": 1,
    }
    return cells, dyn_grid


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


def combo_lookup(sql, cache, rc, pm, searcher, combos, table=None, dim=None):
    """在结果 CSV 中按 (table?, sql_type, use_cache, use_query_cache, parse_mode) 匹配,
    优先取 parallel/top/ef_s 一致且时间戳最新的一行。

    参数:
        sql/cache/rc/pm: 日志侧推断出的组合键(cache 为 cache 档位, rc 为 0/1);
        searcher: 当前 searcher, 用于在候选里优先挑参数一致者(None 表示不筛);
        combos: read_combos() 的结果; table: 限定表名(None 表示不限);
        dim: 日志实测的向量维度, 用于跨表候选消歧(表名与维度缺一不可时最可靠)。
    返回: 命中的 CSV dict 或 None。时间戳为字符串 "YYYY-MM-DD HH:MM:SS", 按字典序
          取最大即最新; 同秒多条时取遍历到的最后一条。
    """
    cands = [
        c for c in combos
        if c.get("sql_type") == sql and c.get("cache") == cache
        and c.get("rc") == rc and c.get("parse_mode") == pm
    ]
    if not cands:
        return None
    # 维度消歧: 同一 (sql_type, cache, rc) 在结果 CSV 里会跨多张物理表出现
    # (如 benchmark_960_1m 与 benchmark_sift_128 都有 text_literal/cache=1),
    # 只用表名匹配会因 table 归一为引擎名而失效; 日志实测维度(128/256/768/960/
    # 1536)与 CSV 的 vector_size 唯一对应, 先按它筛掉其它表。
    if dim is not None:
        hits = [c for c in cands if c.get("vector_size") == str(dim)]
        if hits:
            cands = hits
    if table and cands:
        # 表名可能对不上: pgvector-benchmark.sh 的 CSV 写的是物理表名
        # (benchmark_1536_1m), 而日志侧 table 是按 config 归一后的引擎名。
        # 精确匹配优先; 找不到时仅当候选都来自同一张表才降级, 避免跨表串味。
        hits = [c for c in cands if c.get("table") == table]
        if hits:
            cands = hits
        elif len({c.get("table") for c in cands}) > 1:
            return None
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

    仅在 PID 没有任何向量转换事件时使用(旧版曾移除 vector_in 系事件的日志):
    cache=0 -> 4 条, cache=1/2/3 -> 5 条 SET + 1 条 PREPARE = 6 条。新日志每条
    向量查询后紧跟 vector_in 系事件, summarize 按事件配对精确剥离, 不用本经验值。
    cache 未知时按 cache=0 处理。

    参数: cache —— cache 档位(int 或 None)。
    返回: 4(cache=0 或未知) / 6(cache=1/2/3), 单位条。
    """
    if cache is None:
        return 4
    return 6 if int(cache) in (1, 2, 3) else 4


def setup_len(rec, cache=None):
    """[兜底口径] 无向量转换事件的 PID 的 setup 非向量 simple_query 条数。

    仅当 PID 没有任何向量转换事件时才会被调用(旧版曾移除 vector_in 系事件的
    日志、空单元等): 数首个非 sq 事件之前的 sq; events 全为 sq 时回退
    sq_drop_count(cache) 经验值并受 sq 总数约束。新日志(每条向量查询后紧跟
    vector_in 系事件)由 summarize 按事件配对精确区分, 不经过本函数。

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


def split_vector_sq(rec):
    """按事件配对一个 PID 的 simple_query, 精确区分向量查询与非向量查询。

    新版插桩在每条向量查询的服务端日志里, 于其 PGVECTOR_TIMING simple_query 行
    之后紧随一条向量转换事件(vector_in/halfvec_in/array_to_vector); 非向量语句
    (SET/PREPARE/BEGIN/COMMIT 等)没有该事件。因此事件序列中 "sq 且下一个事件是
    向量转换" 的即为向量查询, 其余 sq 为 setup 或间杂的非向量语句 —— 即使出现在
    首个向量查询之后(如延迟探针的 BEGIN/COMMIT)也能被准确剔除。

    参数: rec —— parse_log() 里单个 PID 的记录(events 与 sq 同源: 第 j 个 "sq"
          事件对应 sq[j])。
    返回: (vector_sq, other_sq) 两个 (p,a,pl,e) 元组列表; 该 PID 没有任何向量
          转换事件(旧版日志)时返回 (None, None), 由调用方回退 setup_len 启发式。
    """
    events = rec.get("events") or []
    if not any(k in VECTOR_MARKS for k in events):
        return None, None
    sq = rec.get("sq", [])
    vector, other = [], []
    j = 0                          # events 里第 j 个 "sq" 事件对应 sq[j]
    for i, kind in enumerate(events):
        if kind != "sq":
            continue
        nxt = events[i + 1] if i + 1 < len(events) else None
        (vector if nxt in VECTOR_MARKS else other).append(sq[j])
        j += 1
    return vector, other


def summarize(pid_recs, cache=None):
    """对一个 cell 的 1..N 个 PID 汇总(跨 PID 池化样本)。

    向量查询筛选: 新日志按 split_vector_sq() 的 "sq 后紧跟向量转换事件" 配对,
    精确区分向量查询与非向量查询(SET/PREPARE/BEGIN 等一律剔除); 旧日志(无向量
    事件)回退 setup_len() 前导剥离。各耗时均值沿用 avg() 的"去掉第一条"口径
    (去除首次热路径)。

    参数: pid_recs —— 单个 rec 或 rec 列表(同格多个 search PID 池化);
          cache —— 空单元时 setup 条数的兜底档位。
    返回: dict, 字段与单位:
        exec_count 向量查询条数; parse_avg/analyze_avg/plan_avg/exec_avg 为 sq 的
        四段毫秒均值; eq_count 为 EXECUTE 条数; eval_avg/gcp_avg 为 execute_query
        的毫秒均值; vi_count/vi_avg 向量转换(含 array_to_vector)次数与毫秒均值;
        hv_count/hv_avg halfvec_in 次数与毫秒均值。均值可能为 None(样本 <= 1);
        恢复向量转换插桩后 vi/hv 重新有值(旧版日志为 0/None)。
    """
    if isinstance(pid_recs, dict):
        pid_recs = [pid_recs]
    sq, eq, vi, hv = [], [], [], []
    for rec in pid_recs:
        vsq, _ = split_vector_sq(rec)
        if vsq is not None:
            # 新日志: 事件配对筛选, setup 与间杂非向量语句(BEGIN/COMMIT 等)全部剔除
            sq.extend(vsq)
        else:
            # 旧日志/无向量事件的 PID: 回退"首个向量事件前导 sq"启发式
            sq.extend(rec["sq"][setup_len(rec, cache):])
        eq.extend(rec["eq"])
        vi.extend(rec["vi"])
        hv.extend(rec["hv"])
    return {
        "exec_count": len(sq),                    # 配对筛出的向量查询条数
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

    参数: pid_recs —— 单个 rec 或 rec 列表(同一档位的主跑 + 探针等)。
    返回: "auto" / "custom" / "generic" / "-"(没有任何 trace 动作, 如 cache=0)。
    说明: BUILD_GENERIC 本身是 PREPARE 期的固有动作, 不直接参与判据, 但用作
        分母: auto 档 PG 会先走若干次 custom plan 再转 generic, 每个 PREPARE
        的 USE_CUSTOM/BUILD_GENERIC ≈ 3; generic 档强制走 generic, 只留 ≈0.5。
        该比值在 TIMELIMIT 2s/10s、n=1/1000 下都稳定, 而绝对次数会随事务数增长
        (短测试里 generic 主跑也能累积到 3 次 USE_CUSTOM), 故不用绝对阈值。
        多个 rec 取比值最大者(主跑与探针各算各的, 任一方呈 auto 特征即判 auto)。
    """
    if isinstance(pid_recs, dict):
        pid_recs = [pid_recs]
    ratio = 0.0
    custom_max = 0
    generic = 0
    for rec in pid_recs:
        a = rec.get("actions") or {}
        c = a.get("USE_CUSTOM", 0)
        b = a.get("BUILD_GENERIC", 0)
        custom_max = max(custom_max, c)
        generic += a.get("USE_GENERIC", 0)
        if b:
            ratio = max(ratio, c / b)
    if not (custom_max or generic):
        return "-"
    # USE_GENERIC 一次都没有 => custom 档(每次执行都重新规划)
    if not generic:
        return "custom"
    return "auto" if ratio >= AUTO_CUSTOM_PER_BUILD else "generic"


def fmt(v, nd=3):
    """格式化数值: None -> "-"; 否则保留 nd 位小数(默认 3, 耗时单位 ms)。"""
    if v is None:
        return "-"
    return f"{v:.{nd}f}"


# 输出 CSV 表头(main 与多批次聚合共用)
HEADER = ["#", "PID", "sql_type", "cache", "res_cache", "parse_mode", "proto",
          "exec#", "parse_ms", "analyze_ms", "plan_ms",
          "exec_ms", "eval_ms", "gcp_ms", "vi#", "vi_ms",
          "hv#", "hv_ms", "est_QPS", "CSV_RPS", "run_date",
          "plan_mode", "search_cfg"]

# 聚合时按均值合并的数值列: 列下标 -> 小数位(0 表示取整; exec#/vi#/hv# 为条数)
_AVG_COLS = {7: 1, 8: 3, 9: 3, 10: 3, 11: 3, 12: 4, 13: 4, 14: 1, 15: 3, 16: 1, 17: 3, 18: 0, 19: 0}


def _avg_str(vals, nd):
    """均值格式化: 无有效值 -> "-"; 整数列(nd=0 或均值恰为整数)输出整数。"""
    if not vals:
        return "-"
    a = sum(vals) / len(vals)
    if nd == 0 or abs(a - round(a)) < 1e-9:
        return f"{int(round(a))}"
    return f"{a:.{nd}f}"


def analyze_batch(pids, pid_data, grid, per_sql, combos, table, dim=None):
    """对单个连续批次(一轮基准运行)构建格子并产出分析行, 返回 rows 列表(无效时为 [])。

    从原 main 内联逻辑抽出, 供单批次与 --aggregate 多批次逐批分析共用。
    """
    if grid.get("layout") == "dynamic":
        # 日志驱动: SQL_type 顺序/缓存档/执行次数均按日志动态切分(见函数 docstring);
        # raw_phase="head" 表示 cache_major 的前导 raw 段(预热 + 各组合主跑/探针)。
        cells, grid = build_cells_sql_dynamic(pids, pid_data, raw_phase=grid.get("raw_phase"))
    elif grid.get("layout") == "cache_major":
        cells = build_cells_cache_major(pids, pid_data, grid)
    else:
        cells = build_cells(pids, pid_data, grid, per_sql)
    if not cells:
        print("[WARN] 无有效格子, 未生成 CSV。", file=sys.stderr)
        return []
    if grid.get("layout") != "dynamic" and grid_pid_count(grid) != len(pids):
        print(f"  (warn) 日志 PID 数 {len(pids)} 与 config 网格预期 "
              f"{grid_pid_count(grid)} 不一致(截断/配置变更)", file=sys.stderr)

    rows, i = [], 0
    # 输出顺序按格子内首个 PID 的开始时间(chronological): raw(cache=0) 档在日志里
    # 夹在 auto 与 custom 之间, 不能因为它后建格就统一排到末尾。
    def _cell_ts(item):
        cell = item[1]
        ts = [pid_data[p]["ts_first"] for sp in cell["searches"] for p in sp]
        return min(ts) if ts else ""

    for key, cell in sorted(cells.items(), key=_cell_ts):
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

            info = combo_lookup(sql, cache, rc, pm, searcher, combos, table, dim)
            rps = _num((info or {}).get("qps"))
            # 单条查询的总执行耗时 = parse + analyze + plan + exec(毫秒, 各项取
            # 去首样本均值); 串行执行时 QPS ≈ 1000 / 总耗时(ms)。
            # 不再用 “条数 / 测试时长”: 条数与时长都受批次影响(实测同一格在两轮
            # 里可能分别是 10s/10000 条与 20s/20000 条), 相除后各格会退化成同一个
            # 数; 单条耗时与并发无关, 逐格可比。
            total_ms = sum(v for v in (ss.get("parse_avg"), ss.get("analyze_avg"),
                                       ss.get("plan_avg"), ss.get("exec_avg"))
                           if v is not None)
            est_qps = (1000.0 / total_ms) if total_ms else None

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
                ss.get("vi_count"),                # 向量转换次数(vector_in/array_to_vector)
                fmt(ss.get("vi_avg")),             # ms(旧版无向量事件日志恒 "-")
                ss.get("hv_count"),                # halfvec_in 次数
                fmt(ss.get("hv_avg")),             # ms(旧版无向量事件日志恒 "-")
                f"{est_qps:.0f}" if est_qps else "-",   # 次/秒; 0/None 均输出 "-"
                f"{rps:.0f}" if rps else "-",           # 次/秒(取自结果 CSV)
                s_recs[0]["ts_first"][:10],        # 该 searcher 首 PID 的日期
                s_mode,
                grid["labels"][si] if si < len(grid["labels"]) else "-",
            ])
    return rows


def merge_batch_rows(all_rows):
    """跨批次(多轮同负载)合并分析行: 按格子键平均数值列, 其他列取最早批次值。

    格子键 = (sql_type, cache, res_cache, parse_mode, plan_mode, search_cfg);
    PID 列合并去重; proto 出现过 prep 即 prep; "#" 按合并后顺序重排。
    """
    buckets = {}
    for rows in all_rows:
        for row in rows:
            key = (row[2], row[3], row[4], row[5], row[21], row[22])
            buckets.setdefault(key, []).append(row)
    merged = []
    for key, rs in buckets.items():          # py3.7+ dict 保持首次出现顺序
        first = rs[0]
        out = [0, "", first[2], first[3], first[4], first[5],
               "prep" if any(r[6] == "prep" for r in rs) else "simple"]
        for c, nd in _AVG_COLS.items():
            vals = []
            for r in rs:
                try:
                    vals.append(float(r[c]))
                except (TypeError, ValueError):
                    pass
            out.append(_avg_str(vals, nd))
        out.append(first[20])                # run_date: 最早批次日期
        out.append(first[21])
        out.append(first[22])
        # PID 列: 跨批次去重合并(数量大但仅为溯源信息)
        out[1] = ",".join(dict.fromkeys(p for r in rs for p in r[1].split(",")))
        merged.append(out)
    for i, row in enumerate(merged, 1):
        row[0] = i
    return merged


def main():
    """命令行入口: 解析日志 -> 推断被测表/网格 -> 切格子 -> 汇总并写 CSV。

    返回: 0 成功; 1 日志不存在、无 PGVECTOR_TIMING 记录或无有效格子。
    异常: 文件读取失败未捕获(会向上抛 OSError); 参数解析失败由 argparse 退出。
    """
    ap = argparse.ArgumentParser(description="汇总 pgvector 时序日志并与结果 CSV 交叉验证。")
    ap.add_argument("--log", default=LOG_PATH, help="PostgreSQL 日志路径")
    ap.add_argument("--csv", default=None,
                    help="pgvector 结果 CSV 路径(默认 results/csv/"
                         "pgvector-benchmark-results.csv, 也兼容 run.py 的"
                         " pgvector_benchmark_results.csv)")
    ap.add_argument("--out", default=None, help="输出 CSV 路径")
    ap.add_argument("--config", default=CONFIG_PATH, help="configurations/config.json 路径")
    ap.add_argument("--engine", default=None,
                    help="被测 pgvector 引擎名(默认按 PID 数与 config 网格自动推断)")
    ap.add_argument("--min-records", type=int, default=MIN_PID_RECORDS,
                    help="[兼容保留] 额外的 PID 计时行数下限, 仅在 >1 时生效; 搜索/"
                         "非搜索 PID 的判定已改为按向量转换事件数(vector_in/halfvec_in/"
                         "array_to_vector 事件数 >1), 不再依赖记录数")
    ap.add_argument("--gap-max", type=float, default=GAP_MAX,
                    help="相邻 PID 开始时间差超过该秒数视为新批次, 只取最后一组连续批次(默认 61)")
    ap.add_argument("--batches", default="last",
                    help="日志中连续批次的选取: last(默认=旧行为, 仅最后一组) / all(全部) / 正整数N(最后N组)")
    ap.add_argument("--aggregate", action="store_true",
                    help="把选中的多个批次(同一负载循环跑多轮的日志)按格子平均合并为一组输出")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print(f"[ERR] 日志不存在: {args.log}", file=sys.stderr)
        return 1

    # 默认用 pgvector-benchmark.sh 的结果 CSV(连字符); run.py 客户端的
    # pgvector_benchmark_results.csv(下划线)可用 --csv 显式指定。
    csv_path = args.csv or os.path.join(RESULTS_DIR, "pgvector-benchmark-results.csv")
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
    # 布局判定(用全部 PID, 与批次选取无关): sql_major 布局将走日志驱动的动态切格。
    layout = detect_layout(pids, pid_data)
    # 批次选取: last=旧行为(仅最后一组); all/N=多组; 多组仅在与 --aggregate 搭配时逐批分析。
    # cache_major/sql_major 两种日志驱动布局都不依赖批次对齐(慢查询会把同一组合
    # 的日志按 gap 切成碎片段, 并非真实批次边界), 因此合并全部连续批次再切格。
    if layout in ("sql_major", "cache_major") and len(groups) > 1:
        sel_groups = groups
        print(f"  (info) {layout} 布局按日志动态切分: 合并全部 {len(groups)} 组"
              f"连续批次(共 {sum(len(g) for g in groups)} 个 PID)", file=sys.stderr)
    elif not groups:
        sel_groups = []
    elif args.batches == "last":
        sel_groups = groups[-1:]
    elif args.batches == "all":
        sel_groups = groups
    else:
        sel_groups = groups[-max(1, int(args.batches)):]
        if len(groups) > 1 and args.batches == "last":
            print(f"  (info) 日志含 {len(groups)} 段时间连续批次, 仅取最后一组 "
                  f"{len(sel_groups[-1])} 个 PID, 丢弃前 {sum(len(g) for g in groups[:-1])} 个",
                  file=sys.stderr)
    if (len(sel_groups) > 1 and not args.aggregate
            and layout not in ("sql_major", "cache_major")):
        print(f"  (warn) 选中 {len(sel_groups)} 组批次但未加 --aggregate, 仅分析最后一组",
              file=sys.stderr)
        sel_groups = sel_groups[-1:]
    if layout in ("sql_major", "cache_major"):
        # 日志驱动切格用全部批次的 PID(表名/网格推断与切格都以全量为准)
        pids = [p for g in sel_groups for p in g] if sel_groups else []
    else:
        pids = sel_groups[-1] if sel_groups else []   # 表/网格推断用最后一个连续批次
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
    # 两种布局都走日志驱动的动态切格: sql_type 的数量/顺序、缓存档位、每格执行
    # 次数都以日志实测为准, config 只用于表名推断与 CSV_RPS 交叉验证。
    # cache_major(pgvector-benchmark.sh) 的 raw 档集中在全部有名块之前(整表预热
    # 一次, 再每个组合 主跑+延迟探针), 用 raw_phase="head" 让切格函数单独处理该
    # 前导段; 旧的"按位置每格 N 个 PID 均分"在探针混入或组合失败时必然错位
    # (83 个搜索 PID 分 44 格, 余数被丢弃), 已不再使用。
    per_sql = None
    grid = {
        "layout": "dynamic",
        "raw_phase": "head" if layout == "cache_major" else None,
        "sql_types": obs_sqls or ["unknown"],
        "caches": [0, 1, 2, 3],
        "result_caches": [0],
        "parse_modes": [""],
        "search_number": 1,
        "searchers": [{"params": {}}],
        "labels": ["-"],
        "prewarm": 0,
        "per_cell": 1,
    }
    if layout == "cache_major":
        print("  (info) 检测到 cache_major 布局(pgvector-benchmark.sh 日志): "
              "raw 档按前导段分组, prep 档按 PLAN_CACHE_TRACE 动作分档", file=sys.stderr)
    # 输出文件名用数据集名: 优先 config 的 dataset, 其次表名, 最后 "unknown"
    dataset = next((i_.get("dataset") or i_.get("name") for i_ in cfg
                    if i_.get("name") == table), table or "unknown")
    if grid.get("layout") == "dynamic":
        print(f"  table={table} dataset={dataset} dim={dim} "
              f"grid=动态(sql_types={len(grid['sql_types'])}, 档位/轮数按日志切分)",
              file=sys.stderr)
    else:
        print(f"  table={table} dataset={dataset} dim={dim} "
              f"grid={len(grid['sql_types'])}sql x {len(grid['caches'])}cache x "
              f"{len(grid['result_caches'])}rc x {len(grid['parse_modes'])}mode "
              f"x {len(grid['searchers'])}searcher(sn={grid['search_number']}) "
              f"= {grid_pid_count(grid)} PID", file=sys.stderr)

    if (args.aggregate and len(sel_groups) > 1
            and grid.get("layout") not in ("dynamic", "cache_major")):
        # 多批次(同一负载循环跑多轮)聚合: 每批独立建格分析, 再按格子平均合并。
        # 动态/cache_major 切格不走此路径: pids 已拍平全部批次, 单次切格即为全局收编。
        all_rows = []
        for gi, g in enumerate(sel_groups, 1):
            if layout == "cache_major":
                # 网格已按日志实测反推(cache 外层), 各轮布局一致, 直接复用
                grid_g, per_sql_g = grid, per_sql
            else:
                grid_g = config_grid(cfg, table) if table else None
                if not grid_g:
                    grid_g = log_grid(g, pid_data)    # config 不可用: 用日志/约定兜底
                grid_g, per_sql_g = adapt_grid(grid_g, len(g),
                                               obs_sqls or grid_g["sql_types"])
            rows_g = analyze_batch(g, pid_data, grid_g, per_sql_g, combos, table, dim)
            if rows_g:
                print(f"  (info) 批次 {gi}/{len(sel_groups)}: {len(rows_g)} 行",
                      file=sys.stderr)
                all_rows.append(rows_g)
        if not all_rows:
            print("[WARN] 无有效格子, 未生成 CSV。", file=sys.stderr)
            return 1
        rows = merge_batch_rows(all_rows)
    else:
        rows = analyze_batch(pids, pid_data, grid, per_sql, combos, table, dim)
        if not rows:
            print("[WARN] 无有效格子, 未生成 CSV。", file=sys.stderr)
            return 1

    out_path = args.out or os.path.join(RESULTS_DIR, f"pgvector_log_analysis-{dataset}.csv")
    # newline="" 交给 csv 模块自行处理换行, 避免 Windows 风格空行; 所有字段转字符串
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADER)
        for row in rows:
            w.writerow([str(x) for x in row])

    # 预期行数 = 格子数 × 每格 searcher 数; 不等通常意味着日志前缀/截断。
    # 动态切格下行数即格子数, 无需对齐提示。
    if grid.get("layout") == "dynamic":
        expected = len(rows)
    else:
        expected = grid_cell_count(grid) * len(grid["searchers"])
    msg = f"[OK] 已保存: {out_path}  表={table}  {len(rows)} 行"
    if len(rows) != expected:
        msg += f"  (预期 {expected} 行, 前缀/截断所致)"
    print(msg)
    return 0


if __name__ == "__main__":
    sys.exit(main())