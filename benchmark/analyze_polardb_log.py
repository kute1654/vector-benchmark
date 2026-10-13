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
"""
import re
import os
import sys
import json
import csv
import argparse
from collections import OrderedDict
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RESULTS_DIR = os.path.join(BASE_DIR, "results")
LOG_PATH = "/home/polar/tmp_polardb_pg_17_primary/log/postgresql_error.log"
CSV_PATH = os.path.join(RESULTS_DIR, "pgvector_benchmark_results.csv")
OUT_DIR = RESULTS_DIR

RE_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?)\s+\S+\s+\[(\d+)\]")
RE_STMT = re.compile(
    r"pase_stmt: command \d+ parse ([\d.]+) ms, analyze ([\d.]+) ms, "
    r"plan ([\d.]+) ms, execute_plus_setup ([\d.]+) ms")
RE_IN = re.compile(r"pase_in: (?:mode (\w+), )?dim \d+, parse\+convert ([\d.]+) ms")
RE_VI_ARRAY = re.compile(r"pase_(?:vec_f4|text_i_i): dim \d+, convert ([\d.]+) ms")
RE_EQ = re.compile(
    r"pase_execute_query eval_params=([\d.]+)ms get_cached_plan=([\d.]+)ms name=(\S+)")
RE_PREP = re.compile(r"PREPARE (polardb_ps_[A-Za-z0-9_]+)")
RE_PC = re.compile(r"plan cache (?:MISS|HIT) \(([a-z]+) plan")
RE_RC_ON = re.compile(r"result cache (?:HIT|STORE)")


def stmt_sql_type(name):
    if not name or not name.startswith("polardb_ps_"):
        return None
    return name[len("polardb_ps_"):].rsplit("_", 1)[0]


def avg(values):
    if len(values) <= 1:
        return None
    return sum(values[1:]) / float(len(values) - 1)


def fmt(v, nd=3):
    return "-" if v is None else "%.*f" % (nd, v)


def parse_log(log_path):
    data = OrderedDict()
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if "pase" not in line:
                continue
            m = RE_TS.match(line)
            if not m:
                continue
            ts, pid = m.group(1), m.group(2)
            rec = data.get(pid)
            if rec is None:
                rec = data[pid] = {
                    "ts_first": ts, "ts_last": ts,
                    "steps": [], "vin": [], "eq": [],
                    "prep": set(), "pc": set(), "rc_on": False,
                    "modes": set(),
                }
            rec["ts_last"] = ts
            if "pase_stmt:" in line:
                sm = RE_STMT.search(line)
                if sm:
                    rec["steps"].append(tuple(float(x) for x in sm.groups()))
            elif "pase_in:" in line:
                im = RE_IN.search(line)
                if im:
                    rec["vin"].append(float(im.group(2)))
                    if im.group(1):
                        rec["modes"].add(im.group(1))
            elif "pase_execute_query" in line:
                em = RE_EQ.search(line)
                if em:
                    rec["eq"].append((float(em.group(1)), float(em.group(2)), em.group(3)))
            elif "pase_vec_f4:" in line or "pase_text_i_i:" in line:
                am = RE_VI_ARRAY.search(line)
                if am:
                    rec["vin"].append(float(am.group(1)))
            pm = RE_PREP.search(line)
            if pm:
                rec["prep"].add(pm.group(1))
            pcm = RE_PC.search(line)
            if pcm:
                rec["pc"].add(pcm.group(1))
            if RE_RC_ON.search(line):
                rec["rc_on"] = True
    return data


def read_combos(csv_path):
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
                "parse_mode": (r.get("vector_in_parse_mode") or "").strip() or "-",
                "qps": (r.get("rps") or "").strip(),
                "test_duration": (r.get("test_duration") or "").strip(),
            })
    return combos


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


CONFIG_PATH = os.path.join(BASE_DIR, "configurations", "config.json")


def _parse_ts(s):
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def expected_groups(cfg_path, table_name):
    """从 config.json 折出该表一轮完整测试预期的组数(engine=polardb), 无法确定则返回 None。"""
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
        if not (n_cache and n_rc and n_mode and n_sql):
            return None
        return n_cache * n_rc * n_mode * n_sql * n_search
    return None


def config_test_duration(cfg_path, table_name):
    """从 config.json 取该表一轮完整测试的 test_duration(秒)。"""
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
    ap = argparse.ArgumentParser(description="汇总 PolarDB PASE 各步骤耗时并分组统计。")
    ap.add_argument("--log", default=LOG_PATH, help="pase 服务端日志路径")
    ap.add_argument("--csv", default=CSV_PATH,
                    help="benchmark 结果 CSV(提供真实 table_name/plan_cache/result_cache/qps)")
    ap.add_argument("--out", default=None,
                    help="汇总 CSV 输出路径(默认 results/polardb_log_analysis-<table>.csv)")
    args = ap.parse_args()

    if not os.path.exists(args.log):
        print("[ERR] 日志不存在: %s" % args.log, file=sys.stderr)
        return 1

    pid_data = parse_log(args.log)
    combos = read_combos(args.csv)
    print("log: %s  pids=%d  combos(csv)=%d"
          % (args.log, len(pid_data), len(combos)), file=sys.stderr)

    def step_pair(pid, idx):
        if pid is None:
            return None
        rec = pid_data[pid]
        return avg([s[idx] for s in rec["steps"]])

    def vin_pair(pid):
        if pid is None:
            return None
        return avg(pid_data[pid]["vin"])

    def eq_ms_pair(pid, idx):
        if pid is None:
            return None
        return avg([e[idx] for e in pid_data[pid]["eq"]])

    def cell(w, s):
        return "%s|%s" % (fmt(w), fmt(s))

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
        modes = pid_data[pid].get("modes") or set()
        return next(iter(modes)) if len(modes) == 1 else None

    def _pid_sqls(pid):
        return sorted({stmt_sql_type(x) for x in pid_data[pid].get("prep") or () if stmt_sql_type(x)})

    order = [p for p in pid_data if pid_data[p].get("steps")]
    order.sort(key=lambda p: int(p))          # 以 PID 号(会话顺序)排, 不用时间

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
        if mode is not None and len(sqls) == 1:
            _olabeled.append((idx, mode, sqls[0]))
    segments = []
    i = 0
    while i < len(_olabeled):
        mode, sql = _olabeled[i][1], _olabeled[i][2]
        j = i
        while j < len(_olabeled) and _olabeled[j][1] == mode and _olabeled[j][2] == sql:
            j += 1
        start_idx = _olabeled[i][0]           # 块内首个带标签 PID(idx0..3 为 cache=0 的无标签 PID)
        lo = max(0, start_idx - 4)
        hi = min(len(order), start_idx + BLOCK_PID - 4)
        pids16 = order[lo:hi]
        segments.append([mode, sql, pids16])
        i = j
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
        """
        cand = []
        for c in combos:
            if c["parse_mode"] != mode or c["cache"] != cache or c["rc"] != rc:
                continue
            if c["sql_type"] != sql and not c["sql_type"].startswith(sql + "_"):
                continue
            cand.append(c)
        if not cand:
            return None
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
    tables_order = [t for t in tables_order if not (t in _seen or _seen.add(t))]
    if not tables_order:
        print("[WARN] 结果 CSV 未给出可用表名, 将以 polardb 命名输出。", file=sys.stderr)
        tables_order = ["polardb"]

    def build_rows(table):
        rows, k = [], 0
        pool = [c for c in combos if c["table"] == table]
        ctd = config_test_duration(CONFIG_PATH, table)
        for seg in segments:
            mode, sql, pids = seg[0], seg[1], seg[2]
            cache_rc = [(i // 2, i % 2) for i in range(8)]
            for i, (cache, rc) in enumerate(cache_rc):
                if 2 * i + 1 >= len(pids):
                    continue      # 该 sql 段尾部 PID 不足时, 缺失的组合不再补行
                w_pid, s_pid = pids[2 * i], pids[2 * i + 1]
                recw, recs = pid_data[w_pid], pid_data[s_pid]
                n_w, n_s = len(recw["steps"]), len(recs["steps"])
                vi_w, vi_s = len(recw["vin"]), len(recs["vin"])
                proto = "simple" if cache == 0 else "prep"
                run_date = recw["ts_first"][:10]
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
                    if td > 0 and n_s > 1:
                        est_qps = "%d" % round((n_s - 1) / td)
                k += 1
                rows.append([
                    k,
                    "%s/%s" % (w_pid, s_pid),
                    sql, cache, rc, parse_mode, proto,
                    "w=%d / s=%d" % (n_w, n_s),
                    cell(step_pair(w_pid, 0), step_pair(s_pid, 0)),
                    cell(step_pair(w_pid, 1), step_pair(s_pid, 1)),
                    cell(step_pair(w_pid, 2), step_pair(s_pid, 2)),
                    cell(step_pair(w_pid, 3), step_pair(s_pid, 3)),
                    cell(eq_ms_pair(w_pid, 0), eq_ms_pair(s_pid, 0)),
                    cell(eq_ms_pair(w_pid, 1), eq_ms_pair(s_pid, 1)),
                    "%d|%d" % (vi_w, vi_s),
                    cell(vin_pair(w_pid), vin_pair(s_pid)),
                    "0|0", "-|-",
                    est_qps, "-", csv_qps, run_date,
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
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(best_rows)
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