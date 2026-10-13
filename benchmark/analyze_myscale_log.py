#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析 MyScale 服务器时序日志（/var/log/myscale-server/myscale-server.err.log）。

说明：
  服务器 executeQuery 时序日志不记录 SQL 文本与 SET/DROP 语句，因此无法直接从
  日志判断批次 / profile / sql_type。这里采用两条依据：
    1) 批次边界 -> 依靠日志阶段锚点：
       * plan_cache 每组在最前, 其"写计划缓存冷查询"(setAst+setPlan) 锁定测试起点,
         可忽略开始前的作废干扰日志; 每组 sql_type 的 4 个写缓存冷写严格交替
         [setAst, normalizedAST, setAst, normalizedAST], 用滑窗模板对齐去掉干扰;
       * off(5 批): 起点 = 该组区间内第一条不含任何缓存阶段的普通 SELECT;
       * query_off(5 批): 起点 = 窗口内第一条 "reuse query result", 向前回溯纳入
         前导结果未命中/SET;
       * profile 顺序与 clickhouse-benchmark.sh 主循环一致:
         plan_cache, off, only_plan_cache, query_off, query_plan_cache,
         query_only_plan_cache (sql_type 外层、profile 内层)。
    2) 各步骤耗时 -> 每个阶段收集该批全部查询的计时段(us)，去掉最大值与最小值
       后取平均(ms)；若该阶段只出现 1 个值则直接取其耗时。冷查询(写缓存)的
       极端大值会被 max 削除，因此无需单独区分冷/热时间。
"""
import re, os, sys, csv, argparse
from bisect import bisect_left
from collections import OrderedDict

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results")

# 各数据库(结果文件前缀, 默认日志路径)。
DB_RESULT_PREFIX = {
    "myscale": "myscale-benchmark-results.csv",
    "clickhouse": "clickhouse-benchmark-results.csv",
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
        "query_only_plafn_cache": "query_only_plan_cache",
    },
    "clickhouse": {
        "off": "ck_off", "plan_cache": "ck_plan_cache", "only_plan_cache": "ck_only_plan_cache",
        "query_off": "result_off", "query_plan_cache": "result_plan_cache",
        "query_only_plan_cache": "result_only_plan_cache",
    },
}

RE_TIMING = re.compile(
    r"^(\d{4})\.(\d{2})\.(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d+)\s+\[ *(\d+) \]\s+"
    r"\{([0-9a-fA-F-]+)\}\s+<\w+>\s+(\w+):\s*(.+?):(\d+)us\s*$"
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
    y, mo, d, hh, mm, ss, sus = g[:7]
    return (((int(y) * 10000 + int(mo) * 100 + int(d)) * 8640000000) +
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
            rec["stages"][stage] = int(us)
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
    if rec["comps"] and any(m in rec["comps"] for m in SELECT_COMP_MARKERS):
        return True
    st = rec["stages"]
    if "reuse query plan" in st:
        # 无 Interpreter 前缀时靠 reuse* 判定: result_only_plan_cache 等场景下
        # "reuse query plan" 后必须跟着非 0us 的 buildQueryPipeline, 否则排除非 SELECT。
        return _reuse_plan_is_real_select(st)
    return any(s in st for s in ("reuse ast", "reuse query result"))


def cut_batches_b1(qlist, queries, sql_types):
    """方案 B2：用日志阶段锚点切出 len(sql_types)*6 个批次。

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
    entries = []                      # (ts, qid, rec)
    for qid in qlist:
        rec = queries[qid]
        entries.append((rec["ts_us"], qid, rec))
    entries.sort(key=lambda e: e[0])

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


def load_result_csv(csv_path):
    """按行序返回结果 CSV 的 (profile,sql_type) -> {rps, ...} 映射。

    结果 CSV 会追加多张基准表、多轮运行的结果（含被 test 脚本注释掉的旧分组）。
    日志没有时间列，因此这里只取**最后 30 行数据**（= 当前最新一次完整的
    6 profile x 5 sql_type 运行；6 x 5 = 30），与日志对应。
    """
    out = OrderedDict()
    if not os.path.exists(csv_path):
        return out
    with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
        rows = [r for r in csv.DictReader(f)]
    for row in rows[-30:]:
        prof = (row.get("settings_profile") or "").strip()
        sql = (row.get("sql_type") or "").strip()
        if not prof or not sql:
            continue
        try:
            qps = float(row.get("qps_avg", 0) or 0)
        except (TypeError, ValueError):
            qps = 0.0
        if qps <= 0:
            try:
                qps = float(row.get("run_1", 0) or 0)
            except (TypeError, ValueError):
                qps = 0.0
        out[(prof, sql)] = {"rps": qps,
                            "concurrency": row.get("concurrency", ""),
                            "row_count": row.get("row_count", "")}
    return out


def _ms(us):
    return None if us is None else us / 1000.0


def fmt(v, nd=3):
    return "-" if v is None else f"{v:.{nd}f}"


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
    width = [len(h) for h in header]
    for row in rows:
        for c in range(len(header)):
            width[c] = max(width[c], len(str(row[c])))

    def fmtline(vals):
        return " | ".join(str(v).ljust(width[i]) for i, v in enumerate(vals))

    lines = [fmtline(header)]
    lines.append("-+-".join("-" * w for w in width))
    for r in rows:
        lines.append(fmtline(r))
    return "\n".join(lines)


def run_db(db, args):
    # 默认取该数据库"最新"结果文件(未显式指定 --csv 时)。
    if args.csv:
        csv_path = os.path.abspath(args.csv)
    else:
        csv_path = pick_latest_result(db)
        if csv_path is None:
            csv_path = os.path.join(RESULTS_DIR, DB_RESULT_PREFIX[db])
        print(f"+ 数据库[{db}] 采用最新结果文件: {csv_path}", file=sys.stderr)

    result_map = load_result_csv(csv_path)
    exp = args.batch or (len(PROFILE_ORDER) * len(SQL_TYPES))

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
    batches = cut_batches_b1(qlist, queries, SQL_TYPES)
    print(f"  + 按锚点切出 {len(batches)} 个批次 (期望 {exp})", file=sys.stderr)
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