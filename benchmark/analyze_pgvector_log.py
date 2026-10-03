#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析PGVECTOR时序日志，按 backend PID 汇总不同SQL语句的 parse/analyze/plan/exec/vector_in
平均时间(去除每次执行单元内的第一次,即去掉缓存初始化/首次热路径)与执行次数，
并结合 pgvector 结果CSV/JSON 的 RPS(QPS) 做验证，最终生成一张综合表格。

设计依据
--------
- 日志在 `/usr/local/pgsql/data/log/postgresql.log` 中，由 vector 模块输出：
      simple_query parse=..ms analyze=..ms plan=..ms exec=..ms
      vector_in     mode=.. dim=.. len=.. ms=.. at character ..
  PID 即后端进程号，每个测试执行单元(warmup 或 search)都会新建连接 => 独立 PID，
  因此不同 PID 之间无共用标识，只能按时间先后与客户端脚本的执行顺序比对。
- 客户端 client.py 中对每个参数组合:
      1) 预热(warmup): 新连接 ~1s
      2) 实际查询(search): 新连接，时长=test_duration(=1s)，产生结果 JSON/CSV，含 RPS
  故每个组合值对应 (warmup PID, search PID) 两个 PID，且两者 PID 不同。
- 每个组合会写一个 pgvector-hnsw-laion-768-1m-ip-search-*.json 结果文件(run_date 升序
  即为客户端执行顺序)，其 RPS 来自实际查询, 可用来验证日志中 exec 次数≈RPS。
"""
import re
import os
import sys
import glob
import json
import csv
import argparse
from collections import defaultdict, OrderedDict

LOG_PATH = "/usr/local/pgsql/data/log/postgresql.log"
CACHE_PATH = "/tmp/pgvector_timing_cache.json"
RESULTS_DIR = "/home/vector-benchmark/benchmark/results"
JSON_GLOB = os.path.join(RESULTS_DIR, "pgvector-hnsw-laion-768-1m-ip-search-*.json")
CSV_PGVECTOR = os.path.join(RESULTS_DIR, "pgvector_benchmark_results.csv")
OUT_TSV = os.path.join(RESULTS_DIR, "pgvector_log_analysis.tsv")

# 精确的日志行正则
RE_TS = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\.\d+\s+(\S+)\s+\[(\d+)\]")
RE_SQ = re.compile(
    r"PGVECTOR_TIMING\] simple_query parse=([\d.]+)ms analyze=([\d.]+)ms "
    r"plan=([\d.]+)ms exec=([\d.]+)ms"
)
RE_VI = re.compile(r"PGVECTOR_TIMING\] vector_in .*?ms=([\d.]+)")


def parse_log(log_path=LOG_PATH):
    """解析整份日志，返回 {pid: {'ts_first':.., 'ts_last':.., 'sq': [(p,a,pl,e),...], 'vi':[ms,...]}}"""
    data = OrderedDict()
    for line in open(log_path, "r", encoding="utf-8", errors="replace"):
        if "PGVECTOR_TIMING] simple_query" not in line and "PGVECTOR_TIMING] vector_in" not in line:
            continue
        m = RE_TS.match(line)
        if not m:
            continue
        ts, _, pid = m.group(1), m.group(2), m.group(3)
        rec = data.setdefault(pid, {"ts_first": ts, "ts_last": ts, "sq": [], "vi": []})
        rec["ts_last"] = ts
        if "simple_query" in line:
            sm = RE_SQ.search(line)
            if sm:
                rec["sq"].append(tuple(float(x) for x in sm.groups()))
        else:
            vm = RE_VI.search(line)
            if vm:
                rec["vi"].append(float(vm.group(1)))
    return data


def parse_log_cached(log_path=LOG_PATH, force=False):
    """解析日志并缓存到 /tmp, 二次运行快速读取。缓存含 sq 元组列表，序列化为列表。"""
    if not force and os.path.exists(CACHE_PATH):
        try:
            raw = json.load(open(CACHE_PATH, "r", encoding="utf-8"))
            data = OrderedDict()
            for pid, v in raw.items():
                data[pid] = {
                    "ts_first": v["ts_first"],
                    "ts_last": v["ts_last"],
                    "sq": [tuple(x) for x in v["sq"]],
                    "vi": [float(x) for x in v["vi"]],
                }
            return data
        except Exception:
            pass
    data = parse_log(log_path)
    try:
        raw = {pid: {"ts_first": v["ts_first"], "ts_last": v["ts_last"],
                     "sq": [list(x) for x in v["sq"]], "vi": v["vi"]}
               for pid, v in data.items()}
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(raw, f)
    except Exception as e:
        print(f"  (warn) 缓存写出失败: {e}", file=sys.stderr)
    return data


def avg(values):
    """去第一次后求均值, 空/单样本返回 None"""
    if len(values) <= 1:
        return None
    rest = values[1:]
    return sum(rest) / float(len(rest))


def build_result_meta(t_min=None, t_max=None):
    """读取结果 JSON(按 run_date 升序 -> 客户端执行顺序), 及 CSV 中 RPS 回退。

    仅保留 run_date 落在日志时间段 [t_min, t_max] 内的文件(即与本次被测日志同一批运行)，
    避免混入历史其它批次的结果文件导致错位。
    """
    metas = []
    for f in glob.glob(JSON_GLOB):
        # 先按文件名里的时间戳预筛(格式 ...-2026-10-01-21-31-02.json), 跳过明显在窗口外的文件
        if t_min and t_max:
            m = re.search(r"-(\d{4}-\d{2}-\d{2})-(\d{2})-(\d{2})-(\d{2})\.json$", f)
            if m:
                ymd, hh, mm, ss = m.group(1), m.group(2), m.group(3), m.group(4)
                fs = f"{ymd} {hh}:{mm}:{ss}"
                if fs < t_min or fs > t_max:
                    continue
        try:
            d = json.load(open(f, "r", encoding="utf-8"))
        except Exception:
            continue
        rd = (d.get("meta") or {}).get("run_date")
        if not rd:
            continue
        if t_min and rd < t_min:
            continue
        if t_max and rd > t_max:
            continue
        sp = d.get("index_search_parameter") or {}
        sr = d.get("search_results") or {}
        metas.append({
            "run_date": rd,
            "sql_type": sp.get("sql_type"),
            "use_cache": sp.get("use_query_plan_cache"),
            "result_cache": sp.get("use_result_cache"),
            "parse_mode": sp.get("vector_in_parse_mode"),
            "dims": sp.get("dims"),
            "test_duration": sp.get("test_duration"),
            "rps": sr.get("average_rps", sr.get("rps", 0)),
            "mean_time": sr.get("mean_time", 0),
        })
    metas.sort(key=lambda x: x["run_date"] or "")
    return metas


def build_csv_rps_lookup(t_min=None, t_max=None):
    """从 pgvector_benchmark_results.csv 读取 RPS 供回退/校验, 按 (sql_type,cache,res) 聚合。

    仅聚合 timestamp 落在日志时间窗 [t_min, t_max] 内的行(即本次被测同批运行)，
    避免混入 09-22 以来历史批次的同键行导致 csv_RPS 失真(看起来像"用了缓存")。
    """
    lookup = defaultdict(list)
    if not os.path.exists(CSV_PGVECTOR):
        return lookup
    with open(CSV_PGVECTOR, "r", encoding="utf-8", errors="replace") as f:
        r = csv.DictReader(f)
        for row in r:
            ts = (row.get("timestamp") or "").strip()
            if t_min and t_max:
                # CSV timestamp 形如 "2026-10-02 11:54:16.123", 截取到秒比较
                ts_s = ts[:19]
                if ts_s < t_min or ts_s > t_max:
                    continue
            key = (row.get("sql_type"), row.get("use_cache"), row.get("use_query_cache"))
            try:
                lookup[key].append(float(row.get("rps", 0) or 0))
            except (TypeError, ValueError):
                pass
    return lookup


def summarize(pid_rec):
    """对单个 PID 的执行单元汇总(已去除该单元首次执行)"""
    sq = pid_rec["sq"]
    vi = pid_rec["vi"]
    return {
        "exec_count": len(sq),                    # simple_query 执行条数
        "exec_count_excl_first": max(0, len(sq) - 1),
        "parse_avg": avg([x[0] for x in sq]),
        "analyze_avg": avg([x[1] for x in sq]),
        "plan_avg": avg([x[2] for x in sq]),
        "exec_avg": avg([x[3] for x in sq]),
        "vi_count": len(vi),
        "vi_avg": avg(vi),
    }


def fmt(v, nd=3):
    if v is None:
        return "-"
    return f"{v:.{nd}f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default=LOG_PATH)
    ap.add_argument("--no-pair", action="store_true",
                    help="不自动两两配对(warmup/search), 每个PID独立成行")
    ap.add_argument("--out", default=OUT_TSV)
    ap.add_argument("--refresh-cache", action="store_true",
                    help="强制重新解析日志并刷新缓存")
    args = ap.parse_args()

    print("parsing log ...", file=sys.stderr)
    pid_data = parse_log_cached(args.log, force=args.refresh_cache)
    # 按首次出现时间排序
    pids = sorted(pid_data.keys(), key=lambda p: pid_data[p]["ts_first"])
    print(f"  + {len(pids)} backends(PID) with timing records", file=sys.stderr)

    # 日志时间窗(精确到秒), 用于过滤结果 JSON 到同批运行
    t_min = min(pid_data[p]["ts_first"][:19] for p in pids)
    t_max = max(pid_data[p]["ts_last"][:19] for p in pids)

    metas = build_result_meta(t_min, t_max)
    csv_rps = build_csv_rps_lookup(t_min, t_max)
    print(f"  + {len(metas)} result JSON files (run_date 升序 = 执行顺序) "
          f"in [{t_min}, {t_max}]", file=sys.stderr)

    rows = []
    if args.no_pair:
        for pid in pids:
            s = summarize(pid_data[pid])
            tap = s["exec_count_excl_first"]
            est_qps = (tap / 1.0) if tap else 0.0
            rows.append([pid, "-", s["exec_count"], fmt(s["parse_avg"]), fmt(s["analyze_avg"]),
                         fmt(s["plan_avg"]), fmt(s["exec_avg"]), s["vi_count"], fmt(s["vi_avg"]),
                         f"{est_qps:.0f}", "", "", "", "", ""])
    else:
        n_pairs = min(len(pids) // 2, len(metas))
        for i in range(n_pairs):
            pid_w, pid_s = pids[2 * i], pids[2 * i + 1]
            sw = summarize(pid_data[pid_w])
            ss = summarize(pid_data[pid_s])
            meta = metas[i]
            # 验证: 实际查询 PID 的 exec 次数(去首次) / test_duration 应≈ RPS(每秒执行SQL条数)
            tap = ss["exec_count_excl_first"]
            td = meta.get("test_duration") or 1.0
            est_qps = (tap / td) if tap else 0.0
            rps = meta["rps"] or None
            match = "-"
            if rps and est_qps:
                match = f"{100 * abs(est_qps - rps) / rps:.1f}%"
            sql = meta.get("sql_type") or "?"
            cache = meta.get("use_cache")
            res = meta.get("result_cache")
            pmod = meta.get("parse_mode")
            # 从 CSV 回退校验 RPS(同 sql_type+cache+res 的均值)
            csv_r = csv_rps.get((sql, str(cache), str(res)))
            csv_avg = (sum(csv_r) / len(csv_r)) if csv_r else None
            rows.append([
                i + 1,                    # 序号
                f"{pid_w}/{pid_s}",       # warmup/search PID
                sql, cache, res, pmod,
                f"w={sw['exec_count']} / s={ss['exec_count']}",  # exec 次数
                f"{fmt(sw['parse_avg'])}|{fmt(ss['parse_avg'])}",
                f"{fmt(sw['analyze_avg'])}|{fmt(ss['analyze_avg'])}",
                f"{fmt(sw['plan_avg'])}|{fmt(ss['plan_avg'])}",
                f"{fmt(sw['exec_avg'])}|{fmt(ss['exec_avg'])}",
                f"{sw['vi_count']}|{ss['vi_count']}",
                f"{fmt(sw['vi_avg'])}|{fmt(ss['vi_avg'])}",
                f"{est_qps:.0f}",
                f"{rps:.0f}" if rps else "-",
                fmt(csv_avg, 1) if csv_avg else "-",
                meta.get("run_date", ""),
            ])

    header = (["#", "PID(warm/s)", "sql_type", "cache", "res_cache", "parse_mode",
               "exec#(w/s)", "parse_ms(w/s)", "analyze_ms(w/s)", "plan_ms(w/s)",
               "exec_ms(w/s)", "vector_in#(w/s)", "vector_in_ms(w/s)",
               "est_QPS", "JSON_RPS[csv_RPS]", "", "run_date"])
    # 重构表头使最后一列对齐
    header = ["#", "PID(warm/s)", "sql_type", "cache", "res_cache", "parse_mode",
              "exec#(w/s)", "parse_ms(w/s)", "analyze_ms(w/s)", "plan_ms(w/s)",
              "exec_ms(w/s)", "vi#(w/s)", "vi_ms(w/s)",
              "est_QPS", "JSON_RPS", "csv_RPS", "run_date"]
    width = [len(h) for h in header]
    for row in rows:
        for c, h in enumerate(header):
            width[c] = max(width[c], len(str(row[c])))
    def fmtline(vals):
        return " | ".join(str(v).ljust(width[i]) for i, v in enumerate(vals))

    lines = [fmtline(header)]
    lines.append("-+-".join("-" * w for w in width))
    for row in rows:
        lines.append(fmtline(row))
    text = "\n".join(lines)
    print(text)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\t".join(header) + "\n")
        for row in rows:
            f.write("\t".join(str(x) for x in row) + "\n")
    print(f"\n[OK] 表格已保存: {args.out}")


if __name__ == "__main__":
    main()