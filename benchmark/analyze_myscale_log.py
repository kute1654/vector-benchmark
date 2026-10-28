#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析 MyScale 服务器时序日志（/var/log/myscale-server/myscale-server.err.log）。

说明：
  服务器 executeQuery 时序日志不记录 SQL 文本与 SET/DROP 语句，因此无法直接从
  日志判断批次 / profile / sql_type。

  预热方式（现状）：**每张表只预热一次**，而不是每次查询/每个批次前预热一次。
  预热是整表网格最前方的一段长突发（client 以 _table_warmup_done 控制，
  对应 analyze_pgvector_log.py 的 prewarm 剥离；sh 则是 Step 5 逐表预热）。
  因此"每个批次头部都有一次写缓存冷查询"的旧前提已不成立：冷写锚点的数量与
  位置不再可预测（服务端计划缓存跨运行存续时，甚至整批都命中旧缓存）。

  批次边界按两种依据切分（run_db 自动选择）：
    1) 首选【SET 簇切分】(新预热方式, client.py/run.py 驱动的运行)：
       每个 searcher 格子开始前，客户端会先连发一组会话级 SET 语句，日志里
       表现为一簇连续"管理记录"（无 Interpreter、无 reuse*，仅 build ast /
       check query_plan / prepare query plan 前段）。以"SET 簇 + 直到下一簇
       的 SELECT 主体"为一个格子；预热与旧表遗留的格子都在最前方，取最后
       N 个格子即与结果 CSV 的 N 行一一对应，profile/sql_type 按 CSV 行序
       翻译（client CSV 无 settings_profile 列时按缓存参数列推导）。
       每个格子若含 k 个 searcher（同格多次压测），按 len//N 归并。
    2) 兜底【冷锚模板】(sh 每轮迭代都重放 SET 行, SET 簇不再是格边界)：
       * plan_cache 每组在最前, 其"写计划缓存冷查询"(setAst+setPlan) 锁定测试起点,
         可忽略开始前的作废干扰日志; 每组 sql_type 的 4 个写缓存冷写严格交替
         [setAst, normalizedAST, setAst, normalizedAST], 用滑窗模板对齐去掉干扰;
       * off(5 批): 起点 = 该组区间内第一条不含任何缓存阶段的普通 SELECT;
       * query_off(5 批): 起点 = 窗口内第一条 "reuse query result", 向前回溯纳入
         前导结果未命中/SET;
       * profile 顺序与 clickhouse-benchmark.sh 主循环一致:
         plan_cache, off, only_plan_cache, query_off, query_plan_cache,
         query_only_plan_cache (sql_type 外层、profile 内层)。

  各步骤耗时 -> 每个阶段收集该批全部查询的计时段(us)，去掉最大值与最小值
  后取平均(ms)；若该阶段只出现 1 个值则直接取其耗时。冷查询(写缓存)的
  极端大值会被 max 削除，因此无需单独区分冷/热时间。
"""
import re, os, sys, csv, argparse
from bisect import bisect_left
from collections import OrderedDict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results", "csv")

# 各数据库(结果文件前缀, 默认日志路径)。命名与 engine/base_client/client.py 中的
# 输出文件一致: MyScale 写入 benchmark_results.csv, ClickHouse 写入
# clickhouse_benchmark_results.csv。
DB_RESULT_PREFIX = {
    "myscale": "benchmark_results.csv",
    "clickhouse": "clickhouse_benchmark_results.csv",
}
# 在执行之前需要把日志中前面的一段有干扰的日志（首条正确的日志的buildQueryPipeline的耗时一般2ms以上或1ms以上，且后续几个buildQueryPipeline的耗时基本一致）清除，否则会导致解析出问题
DB_LOG_PATH = {
    "myscale": "/var/log/myscale-server/myscale-server.err.log",
    "clickhouse": "/var/log/clickhouse-server/clickhouse-server.err.log",
}

SQL_TYPES = ["normal", "cast", "cast_array", "raw_bytes", "raw_bytes_x"]
PROFILE_ORDER = ["plan_cache", "off", "only_plan_cache",
                 "query_off", "query_plan_cache", "query_only_plan_cache"]

# 各数据库在各自 test 脚本 SETTINGS_GROUPS 中的实际 profile 名称
# (内部统一用 PROFILE_ORDER 的"规范名"驱动逻辑, 输出时翻译为数据库实际名称)。
PROFILE_NAME_MAP = {
    "myscale": {
        "off": "off", "plan_cache": "plan_cache", "only_plan_cache": "only_plan_cache",
        "query_off": "query_off", "query_plan_cache": "query_plan_cache",
        "query_only_plan_cache": "query_only_plan_cache",
    },
    "clickhouse": {
        "off": "ck_off", "plan_cache": "ck_plan_cache", "only_plan_cache": "ck_only_plan_cache",
        "query_off": "result_off", "query_plan_cache": "result_plan_cache",
        "query_only_plan_cache": "result_only_plan_cache",
    },
}

# 注意: 阶段耗时可能是**小数微秒**(如 "build query plan:0.09us", 亚微秒精度),
# 若只匹配 \d+us 这类行会整条失配、该阶段全部丢失, 故用 (\d+(?:\.\d+)?)。
RE_TIMING = re.compile(
    r"^(\d{4})\.(\d{2})\.(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d+)\s+\[ *(\d+) \]\s+"
    r"\{([0-9a-fA-F-]+)\}\s+<\w+>\s+(\w+):\s*(.+?):(\d+(?:\.\d+)?)us\s*$"
)
STAGE_COLS = ["build_ast", "check_qp", "prep_plan", "build_plan", "pipe",
              "after_pipe", "reuse_ast", "reuse_plan", "reuse_result",
              "set_ast", "set_plan", "normalized_ast"]
STAGE_MAP = {
    "build ast": "build_ast", "check query_plan": "check_qp",
    "prepare query plan": "prep_plan", "build query plan": "build_plan",
    "reuse ast": "reuse_ast", "reuse query plan": "reuse_plan",
    "reuse query result": "reuse_result",
    "buildQueryPipeline": "pipe", "after buildQueryPipeline": "after_pipe",
    "setAst": "set_ast", "setPlan": "set_plan", "normalizedAST": "normalized_ast",
}


def line_ts_us(g):
    """把日志行首 ``YYYY.MM.DD HH:MM:SS.uuuuuu`` 的分组转成整数微秒时间戳。

    只用于同一份日志内部的相对比较，不追求 Unix 纪元绝对值；因此把日期
    压成 ``YYYYMMDD`` 后乘上一天的微秒数，避免跨天时出现负数差。
    """
    y, mo, d, hh, mm, ss, sus = g[:7]
    # 一天的微秒数 = 86400 * 1e6；若写成 8.64e9 (少一个 0)，一天内最大 8.64e10us
    # 会溢出到"下一天"，跨天日志排序互相穿插。
    return (((int(y) * 10000 + int(mo) * 100 + int(d)) * 86400000000) +
            ((int(hh) * 3600 + int(mm) * 60 + int(ss)) * 1000000) + int(sus))


def parse_log(log_path):
    """收集每条查询(query_id)的起始时间、组件前缀集合与各阶段计时段(us)。

    返回 {qid: rec}，其中 rec["comps"] 记录该 query 出现过的 executeQuery /
    InterpreterSelectWithUnionQuery / InterpreterSelectQueryAnalyzer 前缀，用于
    区分真正的 SELECT 与 SET/DROP 管理语句。rec["raws"] 保留该查询出现的全部
    原始日志行(切分点核对用)。
    """
    queries = OrderedDict()
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            tm = RE_TIMING.match(line)
            if not tm:
                continue
            y, mo, d, hh, mm, ss, sus, thread, qid, comp, stage, us = tm.groups()
            ts = line_ts_us(tm.groups())
            rec = queries.get(qid)
            if rec is None:
                rec = queries[qid] = {"ts_us": ts, "thread": thread,
                                      "comps": set(), "stages": OrderedDict(),
                                      "raws": []}
            elif ts < rec["ts_us"]:
                rec["ts_us"] = ts
            rec["comps"].add(comp)
            rec["stages"][stage] = float(us)
            rec["raws"].append(line.rstrip("\n"))
            if len(rec["raws"]) > 200:
                del rec["raws"][:-200]
    return queries


def print_split_points(batches, queries):
    """打印每个切分点的原始日志行（即该批首条查询的完整日志行），
    供人工对照 myscale-server.err.log 原样核对锚点是否正确。"""
    print("\n==== 切分点核对 (共 %d 批) ====" % len(batches), file=sys.stdout)
    for bi, (prof, sql, qids) in enumerate(batches):
        if not qids:
            continue
        r = queries[qids[0]]
        print(f"\n--- 批 #{bi+1}: profile={prof}  sql_type={sql}  (切分点=该批首条日志) ---",
              file=sys.stdout)
        for raw in r["raws"]:
            print("   " + raw, file=sys.stdout)


# 写计划缓存(仅含计划缓存 profile 的批次头冷查询)与"管理语句(SET/DROP)"的判定。
#  - 真 SELECT:  日志会出现 InterpreterSelectWithUnionQuery / InterpreterSelectQueryAnalyzer
#    前缀, 或 含 reuse* 阶段。
#  - 管理语句:   无 Interpreter、无 reuse，只有 executeQuery 的下述四个阶段之一。
SELECT_COMP_MARKERS = ("InterpreterSelectWithUnionQuery", "InterpreterSelectQueryAnalyzer")

def is_cold_write(rec):
    """判断记录是否为"首次建计划"的冷查询。

    ``setAst``/``setPlan``/``normalizedAST`` 都表示这条查询在写缓存，而
    不是复用缓存，因此可作为批次切分时的冷写锚点。
    """
    return any(s in rec["stages"] for s in ("setAst", "setPlan", "normalizedAST"))

def _reuse_plan_is_real_select(st):
    """only_plan_cache / query_only_plan_cache 中 "reuse query plan" 是否为真 SELECT。

    真正的 SELECT 在复用计划后必须重建执行流水线: "buildQueryPipeline" 出现在
    "reuse query plan" 之后且耗时 > 0us。仅出现 "reuse query plan"、无 buildQueryPipeline
    或 0us 的语句只是杂散日志(缓存命中的空壳), 不应计入 SELECT。
    """
    keys = list(st.keys())
    try:
        i_plan = keys.index("reuse query plan")
        i_pipe = keys.index("buildQueryPipeline")
    except ValueError:
        return False
    return i_pipe > i_plan and st.get("buildQueryPipeline", 0) > 0

def is_select(rec):
    """判断一条日志记录是否对应真正的 SELECT（而非 SET/DROP 等管理语句）。

    优先看 Interpreter 组件前缀；没有前缀时退化为看复用阶段，并用
    ``_reuse_plan_is_real_select`` 排除"只复用了计划但没建流水线"的空壳。
    """
    if rec["comps"] and any(m in rec["comps"] for m in SELECT_COMP_MARKERS):
        return True
    st = rec["stages"]
    if "reuse query plan" in st:
        # 无 Interpreter 前缀时靠 reuse* 判定: result_only_plan_cache 等场景下
        # "reuse query plan" 后必须跟着非 0us 的 buildQueryPipeline, 否则排除非 SELECT。
        return _reuse_plan_is_real_select(st)
    return any(s in st for s in ("reuse ast", "reuse query result"))


def sorted_entries(qlist, queries):
    """按时间排序的日志条目 [(ts_us, qid, rec), ...]，两种切分方式共用。"""
    entries = [(queries[qid]["ts_us"], qid, queries[qid]) for qid in qlist]
    entries.sort(key=lambda e: e[0])
    return entries


# ---- SET 簇切分（新"每表一次预热"方式的首选切分） ----
# client.py 每个 searcher 格子开始前都会连发一组会话级 SET（实测固定 10 条），
# 日志里表现为一簇连续管理记录；预热格子与旧表遗留格子都排在最前方，
# 取末尾与结果 CSV 行数等量的格子即可对齐。阈值取 3：单条杂散管理语句
# (如偶发 SET) 不应被当成格边界，真格边界至少有一整组 SET。
MIN_SET_CLUSTER = 3

def is_manage(rec):
    """管理语句(SET/DROP 等)：没有 Interpreter 前缀、也没有任何 reuse* 阶段。"""
    return not is_select(rec)

def cut_batches_cells(entries, min_cluster=MIN_SET_CLUSTER):
    """按"SET 簇"把日志切成格子：返回 [ [entry,...], ... ]（时间序）。

    一个格子 = 一簇连续管理记录(>= min_cluster 条) + 直到下一簇之前的全部查询。
    与冷锚模板不同，这里不要求每批开头出现写缓存冷查询，因此每张表只预热一次
    的新预热方式（格子头部计划缓存可能直接命中，无冷写）也能正确切分。
    纯管理记录、不含任何 SELECT 的格子（如会话关闭后的尾部 SET）直接丢弃。
    """
    n = len(entries)
    manage = [is_manage(e[2]) for e in entries]
    bounds = []                      # 每簇 SET 的起始下标
    i = 0
    while i < n:
        if manage[i]:
            j = i
            while j < n and manage[j]:
                j += 1
            if j - i >= min_cluster:
                bounds.append(i)
            i = j
        else:
            i += 1
    if not bounds:
        return []
    cells = []
    if bounds[0] > 0:
        cells.append(entries[:bounds[0]])        # 首簇之前的前导干扰
    for idx, b in enumerate(bounds):
        end = bounds[idx + 1] if idx + 1 < len(bounds) else n
        cells.append(entries[b:end])
    return [c for c in cells if any(not is_manage(e[2]) for e in c)]


def cut_batches_b1(qlist, queries, sql_types):
    """兜底切分（sh 每轮重放 SET、SET 簇不再是格边界时）：冷锚模板，
    用日志阶段锚点切出 len(sql_types)*6 个批次。

    批次严格按 sh 脚本主循环顺序产生：
        外层 sql_type(5 个) -> 内层 SETTINGS_GROUPS(6 个)，共 30 批。
    每个 sql_type 内都是相同的固定 6 组模板(与 clickhouse-benchmark.sh
    SETTINGS_GROUPS 顺序一致)：
        组1 plan_cache:       首次 = 写缓存冷查询(含 setAst)，其后全为 reuse ast 模板
        组2 off:              build query plan + buildQueryPipeline(无缓存)
        组3 only_plan_cache:  首次 = 写缓存冷查询(含 normalizedAST)，其后 reuse plan 模板
        组4 query_off:        同 off + 增加 reuse query result
        组5 query_plan_cache: setAst + reuse query result
        组6 query_only_plan_cache: normalizedAST + reuse query result
    因此每个 sql_type 恰好有 4 个"写计划缓存冷查询"锚点
    (2 个含 setAst + 2 个含 normalizedAST)，交替顺序为
    [setAst, normalizedAST, setAst, normalizedAST]，正好每 4 个冷锚归 1 个 sql_type。
    plan_cache 被安排在每组最前: 它的批首是写缓存冷查询(setAst)，是日志里最直观
    的强锚点, 即使测试开始前有一段作废/干扰日志, 也能用第一个冷锚精确锁定测试起点,
    不会像 off 那样(无任何缓存标记)难以和前面的普通查询区分。
    off / query_off(10 批)不写缓存，其批首取所在组"管理语句簇后紧跟 SELECT"的簇首。

    返回 [ (profile, sql_type, [query...]) ]，顺序与脚本/结果 CSV 完全一致
    (sql_type 外层、profile 内层)。
    """
    # 第一次遍历：按时间收集条目，标记写缓存冷查询 / 管理语句 / 真 SELECT。
    entries = sorted_entries(qlist, queries)          # (ts, qid, rec)

    cold_xs = []                      # 写缓存冷查询: (ts, has_setAst)
    for ts, qid, rec in entries:
        if is_cold_write(rec):
            cold_xs.append((ts, "setAst" in rec["stages"]))

    # 滑窗模板对齐: 每组 sql_type 的 4 个写缓存冷写严格交替
    #   [setAst, normalizedAST, setAst, normalizedAST]
    # (plan_cache/query_plan_cache 用 setAst, only/query_only 用 normalizedAST)。
    # 日志最开头可能存在测试前作废的干扰写缓存, 使 4 冷锚序列整体前移一个,
    # 导致各 profile 对号错位。这里滑动找出使"最多连续组严格匹配模板"的起点,
    # 丢弃 start 之前的干扰冷锚, 得到严格对齐的正式 colds。
    template = (True, False, True, False)          # setAst, normalizedAST, ...
    best = (0, 0)                                  # (最多匹配组数, 起点下标)
    for st_ in range(len(cold_xs)):
        rem = cold_xs[st_:]
        groups = len(rem) // 4
        if groups < 1:
            break
        matched = 0
        for k in range(groups):
            if any(rem[k * 4 + j][1] != template[j] for j in range(4)):
                break
            matched = k + 1
        if matched > best[0]:
            best = (matched, st_)
    colds = cold_xs[best[1]: best[1] + best[0] * 4]

    # ---- 基于查询特征的批首锚点 (不猜时间) ----
    # 只有 query_off / query_plan_cache / query_only_plan_cache 开了结果缓存,
    # 它们的查询才会含 "reuse query result"。
    # 只有 off 同时关闭计划/结果缓存, 它的查询是"无任何缓存标记"的普通 SELECT。
    # 只有 plan_cache/query_plan_cache 用 setAst; only/query_only 用 normalizedAST。
    starts = [e[0] for e in entries]
    n = len(entries)

    def _has_plan_reuse_or_cold(rec):
        """批次头判据：该记录要么在建计划缓存，要么复用了计划缓存。"""
        if is_cold_write(rec):
            return True           # setAst/normalizedAST/setPlan
        return "reuse query plan" in rec["stages"]

    def _plain_select(rec):
        """off 组普通 SELECT: 真 SELECT 且不含任何缓存阶段(reuse*/setAst/setPlan/
        normalizedAST)。off 是六组里唯一"无任何缓存标记"的组, 据此可唯一锁定其起点。"""
        if not is_select(rec):
            return False
        st = rec["stages"]
        return not any(s in st for s in ("setAst", "setPlan", "normalizedAST",
                                         "reuse ast", "reuse query plan",
                                         "reuse query result"))

    n_groups = len(colds) // 4
    plan = []                         # (ts, profile, sql_type)
    for g in range(n_groups):
        g4 = colds[g * 4:(g + 1) * 4]
        if len(g4) < 4:
            continue
        pc_ts, op_ts, qp_ts, qo_ts = (x[0] for x in g4)
        sql = sql_types[g] if g < len(sql_types) else f"sql_{g}"

        # off 批首: off 紧随 plan_cache 之后, 是[pc_ts, op_ts)区间内唯一不含缓存
        #    阶段(setAst/normalizedAST/reuse*)的普通 SELECT 组, 取该区间第一条
        #    普通查询为起点。plan_cache 冷锚(pc_ts, setAst) 锁定测试起点, 其
        #    reuse ast 热查询留在 plan_cache 批, 不被划给 off, 亦无需猜时间。
        off_head = pc_ts
        j = bisect_left(starts, pc_ts)
        while j < n and starts[j] < op_ts:
            if _plain_select(entries[j][2]):
                off_head = entries[j][0]
                break
            j += 1

        # query_off 批首 = 窗口 [op, qp) 内第一条 "reuse query result"。
        #    只有 query_off/query_plan_cache/query_only_plan_cache 开结果缓存,
        #    而 qp 之前只有 query_off, 故第一条 reuse query result 即 query_off
        #    的结果命中段起点; 再从该处回溯纳入 query_off 的前导结果未命中/SET。
        #    回溯只跨越"无计划缓存标记"的查询(SET 与普通 miss), 一遇到
        #    only_plan_cache 的 reuse query plan/normalizedAST 即停，故不会被
        #    only_plan_cache 区域里的杂散 manage 查询骗到。
        first_reuse = -1
        j = bisect_left(starts, op_ts)
        while j < n and starts[j] < qp_ts:
            if "reuse query result" in entries[j][2]["stages"]:
                first_reuse = j
                break
            j += 1
        if first_reuse < 0:
            qoff_head = (op_ts + qp_ts) / 2.0   # 兜底: 取窗口中点
        else:
            k = first_reuse
            while k - 1 >= 0 and starts[k - 1] > op_ts:
                rprev = entries[k - 1][2]
                if _has_plan_reuse_or_cold(rprev):
                    break                    # 已是 only_plan_cache 的复用计划查询
                if "reuse query result" in rprev["stages"]:
                    break
                k -= 1
            qoff_head = entries[k][0]

        plan.append((pc_ts, "plan_cache", sql))
        plan.append((off_head, "off", sql))
        plan.append((op_ts, "only_plan_cache", sql))
        plan.append((qoff_head, "query_off", sql))
        plan.append((qp_ts, "query_plan_cache", sql))
        plan.append((qo_ts, "query_only_plan_cache", sql))

    # 按时间排序 = 脚本循环顺序(sql_type 外层、profile 内层)。
    plan.sort(key=lambda x: x[0])
    heads = [p for p, _, _ in plan]
    e_times = [e[0] for e in entries]
    batches = []
    for i in range(len(heads)):
        lo = heads[i]
        hi = heads[i + 1] if i + 1 < len(heads) else float("inf")
        a = bisect_left(e_times, lo)
        b = bisect_left(e_times, hi)
        batches.append(entries[a:b])
    # 装配 profile/sql_type：按已经排好序的 (ts, profile, sql_type) 直接对应。
    out = []
    for (_, prof, sql), seg in zip(plan, batches):
        out.append((prof, sql, [e[1] for e in seg]))
    return out


# client.py 写的结果 CSV 没有 settings_profile 列, 但缓存参数列等价地编码了
# profile: use_query_plan_cache / vector_query_plan_cache 取 0=off, 1=plan_cache,
# 2=only_plan_cache; use_query_cache=1 再加 "query_" 前缀(query_* 三组)。
CACHE_MODE_TO_PROFILE = {0: "off", 1: "plan_cache", 2: "only_plan_cache"}

def _canon_profile(db, row):
    """把一行结果 CSV 翻译成 PROFILE_ORDER 的规范 profile 名。

    有 settings_profile 列(sh 结果)时按 PROFILE_NAME_MAP 反查;
    否则(client.py 结果)按缓存参数列推导。推不出时返回 ""(该行跳过)。
    """
    prof = (row.get("settings_profile") or "").strip()
    if prof:
        inv = {v: k for k, v in (PROFILE_NAME_MAP.get(db) or {}).items()}
        return inv.get(prof, prof)
    mode = None
    for col in ("use_query_plan_cache", "vector_query_plan_cache"):
        v = (row.get(col) or "").strip()
        if v:
            try:
                mode = int(float(v))
            except ValueError:
                mode = None
            break
    if mode not in CACHE_MODE_TO_PROFILE:
        return ""
    prof = CACHE_MODE_TO_PROFILE[mode]
    try:
        qcache = int(float((row.get("use_query_cache") or "0").strip() or 0))
    except ValueError:
        qcache = 0
    return "query_" + prof if qcache else prof

def load_result_rows(db, csv_path):
    """按**行序**(= 执行顺序)返回结果 CSV 数据行的标签列表。

    每行 -> {"profile"(规范名), "sql_type", "rps", "concurrency", "row_count"}。
    client.py 按执行顺序逐行追加, 因此行序与日志里的格子(SET 簇)顺序一致,
    调用方用末尾 N 行与日志末尾 N 个格子对齐; 预热不产生结果行, 天然错位在前方。
    """
    out = []
    if not os.path.exists(csv_path):
        return out
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        rows = [r for r in csv.DictReader(f)]
    for row in rows:
        prof = _canon_profile(db, row)
        sql = (row.get("sql_type") or "").strip()
        if not prof or not sql:
            continue
        try:
            qps = float(row.get("rps", 0) or row.get("qps_avg", 0) or 0)
        except (TypeError, ValueError):
            qps = 0.0
        if qps <= 0:
            try:
                qps = float(row.get("run_1", 0) or 0)
            except (TypeError, ValueError):
                qps = 0.0
        out.append({"profile": prof, "sql_type": sql, "rps": qps,
                    "concurrency": row.get("concurrency", "") or row.get("parallel", ""),
                    "row_count": row.get("row_count", "")})
    return out


def _ms(us):
    """微秒转毫秒；None 原样返回（保持表格里的"-"语义）。"""
    return None if us is None else us / 1000.0


def fmt(v, nd=3):
    """按小数位格式化数值；None 显示为 ``-``，便于对齐表格。

    阶段列以 ms 计, 但有些阶段(如 build query plan)只有亚微秒量级(0.1us =
    0.0001ms), 固定 4 位小数会显示成 0.0000 像丢了值; 这里对非零小值自动
    增加小数位(最多 8 位), 保证至少保留两位有效数字。
    """
    if v is None:
        return "-"
    if v != 0:
        av = abs(v)
        while nd < 8 and av < 10 ** (-nd) * 10:   # 首位有效数字后留一位
            nd += 1
    return f"{v:.{nd}f}"


def _trimmed_avg_us(vals):
    """阶段耗时(us)汇总：去掉最大值与最小值后取平均。
    仅 1 个值 -> 直接取该值；0 个值 -> None。
    仅 2 个值(去两端后为空) -> 回退为两者平均。
    """
    if not vals:
        return None
    vs = sorted(vals)
    if len(vs) == 1:
        return vs[0]
    if len(vs) == 2:
        return (vs[0] + vs[1]) / 2.0
    inner = vs[1:-1]
    return sum(inner) / float(len(inner))


def summarize(qslice):
    """统计一个批次。

    只统计真正的 SELECT 查询(排除 SET/DROP 管理语句)，各阶段耗时去掉最大值与
    最小值后取平均(ms)；
    并统计缓存命中/未命中次数：
      - 结果缓存命中   -> 含 "reuse query result" 的查询数；
      - 计划缓存未命中 -> 含 setAst/setPlan/normalizedAST(写计划缓存) 的查询数；
      - 结果缓存未命中 -> 其余真正执行的查询数(不含结果命中、不含写缓存)。
    """
    if not qslice:
        return None
    selects = [q for q in qslice if is_select(q)]

    prox = OrderedDict((c, []) for c in STAGE_COLS)
    for rec in selects:
        for raw, col in STAGE_MAP.items():
            if raw in rec["stages"]:
                prox[col].append(rec["stages"][raw])
    stage_ms = {c: _ms(_trimmed_avg_us(v)) for c, v in prox.items()}

    result_hit = sum(1 for q in selects if "reuse query result" in q["stages"])
    plan_miss = sum(1 for q in selects if is_cold_write(q))
    # 冷写缓存的那条语句(计划缓存 miss)同时就是结果缓存 miss, 二者是同一句。
    # 因此 result_miss 不应再减去 plan_miss, 直接 = select_n - result_hit。
    result_miss = len(selects) - result_hit
    return {
        "stage_ms": stage_ms,
        "select_n": len(selects),
        "result_hit": result_hit,
        "result_miss": result_miss,
        "plan_miss": plan_miss,
    }


def pick_latest_result(db):
    """在 RESULTS_DIR 下挑选该数据库"最新"的结果文件(按修改时间, 含 .bak 备份)。
    找不到则回退到基础结果文件名。
    """
    import glob
    base = DB_RESULT_PREFIX[db]
    pat = os.path.join(RESULTS_DIR, base + "*")
    files = [p for p in glob.glob(pat) if os.path.isfile(p)]
    if not files:
        fallback = os.path.join(RESULTS_DIR, base)
        return fallback if os.path.exists(fallback) else None
    files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return files[0]


def read_result_meta(csv_path):
    """从结果 CSV 末行读取 (engine, table_name)。

    CSV 会累积多张表/多轮运行；日志没有时间列时对应的是**最新一次运行**，即
    CSV 的末行（最后 30 行）所在的那张表，而不是首行。读取失败时返回 (None, None)。
    """
    if not os.path.exists(csv_path):
        return (None, None)
    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            rows = [r for r in csv.DictReader(f)]
        last = rows[-1]
        return ((last.get("engine") or "").strip() or None,
                (last.get("table_name") or "").strip() or None)
    except Exception:
        pass
    return (None, None)


def _render(rows, header):
    """把二维表渲染成等宽文本：先按列求最大宽度，再逐行左对齐拼接。

    纯文本输出便于直接贴进报告或 diff；不依赖第三方表格库。
    """
    width = [len(h) for h in header]
    for row in rows:
        for c in range(len(header)):
            width[c] = max(width[c], len(str(row[c])))

    def fmtline(vals):
        """按预计算列宽把一行单元格左对齐，用 `` | `` 分隔。"""
        return " | ".join(str(v).ljust(width[i]) for i, v in enumerate(vals))

    lines = [fmtline(header)]
    lines.append("-+-".join("-" * w for w in width))
    for r in rows:
        lines.append(fmtline(r))
    return "\n".join(lines)


def run_db(db, args):
    """对单个数据库跑完整流程：选日志/结果 CSV → 切批 → 交叉校验 → 写输出。

    ``args.csv`` 未给出时自动挑选该数据库最新的结果文件；日志与 CSV 的
    批次必须一一对应，因此切批数量默认取 CSV 行数。
    """
    # 默认取该数据库"最新"结果文件(未显式指定 --csv 时)。
    if args.csv:
        csv_path = os.path.abspath(args.csv)
    else:
        csv_path = pick_latest_result(db)
        if csv_path is None:
            csv_path = os.path.join(RESULTS_DIR, DB_RESULT_PREFIX[db])
        print(f"+ 数据库[{db}] 采用最新结果文件: {csv_path}", file=sys.stderr)

    result_rows = load_result_rows(db, csv_path)
    exp = args.batch or (len(result_rows) if result_rows
                         else len(PROFILE_ORDER) * len(SQL_TYPES))

    engine, table_name = read_result_meta(csv_path)
    if not engine:
        engine = db
    if not table_name:
        table_name = "result"

    log_path = args.log or DB_LOG_PATH[db]
    out_path = args.out or os.path.join(
        RESULTS_DIR, f"{engine}-{table_name}-log-analysis.csv")

    print(f"log: {log_path}", file=sys.stderr)
    queries = parse_log(log_path)
    print(f"  + 解析到 {len(queries)} 条查询执行", file=sys.stderr)
    qlist = sorted(queries.keys(), key=lambda q: (queries[q]["ts_us"], id(q)))

    # 首选 SET 簇切分: 结果 CSV 行序 = 执行顺序, 用末尾 N 行标签对齐日志末尾
    # N 个格子; 每表只预热一次的预热格子与旧表遗留格子都在前方, 被"取末尾"自然
    # 丢弃。并行/多 searcher 可能把一个格子切成 k 个连续格子(各会话各发一簇
    # SET), 按 k=len//N 归并(k<=4); 格子数远超行数(sh 每轮重放 SET)则回退冷锚模板。
    labels = result_rows[-exp:] if result_rows else []
    batches = []
    cells = cut_batches_cells(sorted_entries(qlist, queries))
    if labels and len(cells) < len(labels) and len(cells) >= 2:
        # 日志已滚动: 只保留了最近一段。预热格(每表一次)也在段首时, 它不是结果
        # 行, 先剥掉(查询量远超其余格子的中位数数倍), 剩余格子与 CSV 末尾行对齐。
        counts = [sum(1 for e in c if not is_manage(e[2])) for c in cells]
        med = sorted(counts)[len(counts) // 2]
        for i, n in enumerate(counts):
            if n >= 3 * max(med, 1):
                print(f"  + 日志已滚动, 剥离预热格 #{i} (SELECT {n} 条, "
                      f"其余格子中位数 {med})", file=sys.stderr)
                cells = cells[:i] + cells[i + 1:]
                break
    if labels and cells and len(cells) >= len(labels):
        k = len(cells) // len(labels)
        if k <= 4:
            tail = cells[len(cells) - len(labels) * k:]
            for i, lab in enumerate(labels):
                qids = [qid for c in tail[i * k:(i + 1) * k]
                        for (_ts, qid, _rec) in c]
                batches.append((lab["profile"], lab["sql_type"], qids))
            print(f"  + SET 簇切分: 共 {len(cells)} 格, 每批归并 {k} 格, "
                  f"切出 {len(batches)} 个批次 (与结果 CSV 行对齐)", file=sys.stderr)
    elif labels and cells and len(cells) >= 2:
        # 滚动后只剩末尾若干格: 与 CSV 末尾同数量行对齐(首格可能被截断)。
        labs = labels[len(labels) - len(cells):]
        for c, lab in zip(cells, labs):
            batches.append((lab["profile"], lab["sql_type"],
                            [qid for (_ts, qid, _rec) in c]))
        print(f"  + SET 簇切分(日志已滚动): 末尾 {len(cells)} 格与 CSV 末尾 "
              f"{len(cells)} 行对齐, 切出 {len(batches)} 个批次", file=sys.stderr)
    if not batches:
        batches = cut_batches_b1(qlist, queries, SQL_TYPES)
        print(f"  + 冷锚模板切分出 {len(batches)} 个批次 (期望 {exp})", file=sys.stderr)
    if len(batches) != exp:
        print(f"  + 注意: 实际批次 {len(batches)} 与期望 {exp} 不一致", file=sys.stderr)

    if args.print_splits:
        print_split_points(batches, queries)

    header = ["#", "profile", "sql_type"] + \
             STAGE_COLS + ["select_n", "result_hit_n", "result_miss_n", "plan_miss_n"]
    has_plan_cache = {"plan_cache", "only_plan_cache",
                      "query_plan_cache", "query_only_plan_cache"}
    rows = []
    for bi, (prof, sql, qids) in enumerate(batches):
        if bi >= exp:
            break
        b = [queries[q] for q in qids]
        s = summarize(b)
        if s is None:
            continue

        is_query = prof.startswith("query_")
        prof_name = (PROFILE_NAME_MAP.get(db) or {}).get(prof, prof)
        row = [bi + 1, prof_name, sql]
        row += [fmt(s["stage_ms"][c], 4) for c in STAGE_COLS]
        row += [s["select_n"]]
        row += [s["result_hit"] if is_query else "-"]
        row += [s["result_miss"] if is_query else "-"]
        row += [s["plan_miss"] if prof in has_plan_cache else "-"]
        rows.append(row)

    print("\n" + _render(rows, header), file=sys.stdout)
    print(f"\n[OK] 输出 {len(rows)} 行, 保存: {out_path}", file=sys.stderr)

    if out_path:
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            for r in rows:
                w.writerow([str(x) for x in r])
        print(f"[OK] 已保存({db}): {out_path}", file=sys.stderr)
    return 0


def main():
    """命令行入口：解析参数并对 myscale / clickhouse / both 依次执行分析。"""
    ap = argparse.ArgumentParser(
        description="按批次汇总时序日志, 结合结果 CSV 交叉验证。支持 myscale / clickhouse。")
    ap.add_argument("--db", choices=["myscale", "clickhouse", "both"],
                    default="both", help="要处理的数据\u5e93(both=两个都处理, 默认)")
    ap.add_argument("--log", default=None, help="时序日志路径(缺省=按数据库自动确定)")
    ap.add_argument("--csv", default=None, help="结果 CSV(缺省=该数据库最新结果文件)")
    ap.add_argument("--out", default=None, help="输出 CSV(缺省=结果目录下 含表名 的文件名)")
    ap.add_argument("--batch", type=int, default=None,
                    help="期望批次号数(缺省=结果 CSV 行数, 默认 30)")
    ap.add_argument("--print-splits", action="store_true",
                    help="打印每个切分点批次头的完整原始日志行, 供人工核对锚点")
    args = ap.parse_args()

    dbs = ["myscale", "clickhouse"] if args.db == "both" else [args.db]
    rc = 0
    for db in dbs:
        print("==== " + db + " ====", file=sys.stderr)
        try:
            rc = run_db(db, args) or rc
        except Exception as e:
            print(f"[ERROR] 处理 {db} 失败: {e}", file=sys.stderr)
            rc = rc or 1
    return rc


if __name__ == "__main__":
    sys.exit(main())