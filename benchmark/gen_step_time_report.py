# -*- coding: utf-8 -*-
"""
向量数据库耗时分析：读 run-benchmark-loop.sh 产出的 log-analysis CSV -> 生成阶段耗时占比图。

默认只输出第一组图（不使用查询计划缓存，阶段只取核心耗时；sql_type 只画内置清单）：
  clickhouse_share_off.png       ClickHouse 5 张表 × 默认 sql_type，profile=ck_off;
                                 阶段 build_ast/check_qp/prep_plan/build_plan/pipe/after_pipe
  myscale_share_off.png          MyScale   同上，profile=off（口径同 ck_off）
  pgvector_1000_share_off.png    pgvector  5 张表 × 默认 sql_type，cache=0;
                                 阶段 parse/analyze/plan/exec
  pgvector_1_share_off.png       pgvector  同上，1 条 SQL 的负载
单表模式（-t/-n）对应：<engine>_share_off_<表名>-<条数>.png / pgvector_share_off_<...>.png

第二组（仅 --cache-charts 时输出）：显示各计划缓存档位（阶段额外包含缓存相关耗时）
  CK/MS 3 档: ck_off / ck_only_plan_cache / ck_plan_cache，阶段在 6 段基础上加
  reuse_ast/reuse_plan/normalized_ast；全量模式按档出图
  clickhouse_share_cache_<profile>.png / myscale_share_cache_<profile>.png
  pgvector 4 档: cache=0/1/2/3，阶段 parse/analyze/plan/exec/eval/gcp；
  全量模式按档出图 pgvector_{1000,1}_share_cache_{0,1,2,3}.png；
  单表模式在同一张图内并排显示 4 档：pgvector_share_cache_<表名>-<条数>.png，
  CK/MS 单表模式为 <engine>_share_cache_<表名>-<条数>.png（并排 3 档）。

每张图：横轴按 sql_type 分组，组内每根柱对应一张表（全量模式）或一个缓存档
（单表模式），柱内按阶段堆叠并归一化为 100%（段内标注占比），柱顶标注 ms/query。

数据源（results/csv/，缺失时回退 results/bak/；文件名与 run-benchmark-loop.sh 一致）：
  clickhouse-<表名>-log-analysis-1000.csv            CK（n=1 组合循环脚本跳过）
  myscale-<表名>-log-analysis-1000.csv               MyScale
  pgvector_log_analysis-<表名>-{1,1000}.csv           pgvector，cache/res_cache 列
缺失的表自动跳过；全量模式不会因某张表还没跑完而报错。

用法：
  python gen_step_time_report.py                      # 只画默认图(第一组)
  python gen_step_time_report.py --cache-charts       # 默认图 + 各缓存档位图
  python gen_step_time_report.py -t <表名> -n <条数>  # 解析 PG 日志模式:
      解析 /usr/local/pgsql/data/log/postgresql.log 中累计的多轮运行
      (循环跑 N 次基准未清日志), 按批次平均合并, 输出
      results/csv/pgvector_log_analysis-<表名>-<条数>.csv 并画占比图
      (results/charts/pgvector_share_off_<表名>-<条数>.png;
       加 --cache-charts 再输出 pgvector_share_cache_<表名>-<条数>.png)
  python gen_step_time_report.py --engine clickhouse -t <表名> -n <条数>   # CK/MS 模式:
      读循环脚本产出的 results/csv/<engine>-<表名>-log-analysis-<条数>.csv
      (每轮基准一组数据, 默认 10 组), 同一 sql_type 的各组按阶段取算术平均后
      画占比图 (results/charts/<engine>_share_off_<表名>-<条数>.png;
       加 --cache-charts 再输出 <engine>_share_cache_<表名>-<条数>.png);
      无缓存档 profile (clickhouse=ck_off / myscale=off)
可选: --pglog <路径> 指定日志; --runs all|N 指定合并的批次数; --no-chart 只出 CSV;
      --sql-types "a b" 指定要画的 sql_type(空格/逗号分隔, 顺序即横轴顺序;
      默认空=各引擎内置默认清单, CSV 中其余形态不画; 不存在的形态自动跳过)。
"""
import argparse
import csv
import os
import re
import subprocess
import sys

BASE = os.path.dirname(os.path.abspath(__file__))                            # 脚本目录
RAW = os.path.join(BASE, 'results', 'csv')                                   # 数据目录
BAK = os.path.join(BASE, 'results', 'bak')                                   # 表头迁移/覆盖前的备份目录
CHART_DIR = os.path.join(BASE, 'results', 'charts')                          # 图输出目录


def load_mpl():
    """延迟加载 matplotlib(容器内常未安装); 成功返回 True, 失败提示并返回 False。

    画图不是 CSV 汇总的前置条件, 因此把 import 推迟到画图时, 保证日志解析
    模式在没有 matplotlib 的环境下也能写出报告 CSV。
    """
    try:
        import matplotlib
        matplotlib.use('Agg')
        globals()['plt'] = __import__('matplotlib.pyplot', fromlist=['pyplot'])
        globals()['Patch'] = __import__('matplotlib.patches', fromlist=['patches']).Patch
        return True
    except ImportError:
        print('(warn) 未安装 matplotlib, 跳过画图; CSV 报告不受影响 '
              '(如需图表: pip3 install matplotlib)', file=sys.stderr)
        return False


def resolve_input(name):
    """先在 results/csv/ 找输入 CSV，找不到再回退到 results/bak/（旧结果会被轮转到这里）。"""
    path = os.path.join(RAW, name)
    if os.path.exists(path):
        return path
    path = os.path.join(BAK, name)
    if os.path.exists(path):
        return path
    raise FileNotFoundError('missing log-analysis CSV: %s (looked in results/csv and results/bak)' % name)


def resolve_optional(name):
    """同 resolve_input, 但文件不存在时返回 None(全量模式跳过尚未跑完的表)。"""
    try:
        return resolve_input(name)
    except FileNotFoundError:
        return None

# ---------------- 数据文件（文件名与 run-benchmark-loop.sh 的产出保持一致） ----------------
# CK/MS 循环脚本产出 <engine>-<表名>-log-analysis-<条数>.csv（n=1 组合跳过，固定 1000）；
# pgvector 循环脚本产出 pgvector_log_analysis-<表名>-<条数>.csv。
CK_TABLES = ['Benchmark_128_10m', 'Benchmark_256_290k', 'Benchmark_768_1m',
             'Benchmark_960_1m', 'Benchmark_1536_1m']
PG_TABLES = ['benchmark_sift_128', 'benchmark_256_290k', 'benchmark_768_1m',
             'benchmark_960_1m', 'benchmark_1536_1m']
CK_ROW_COUNT = 1000            # CK/MS 循环只跑 n=1000 组合


def loop_csv(engine, table, n):
    """run-benchmark-loop.sh 产出的日志分析 CSV 文件名。"""
    if engine == 'pgvector':
        return 'pgvector_log_analysis-%s-%s.csv' % (table, n)
    return '%s-%s-log-analysis-%s.csv' % (engine, table, n)


def table_dim(name):
    """从表名解析向量维度: benchmark_1536_1m / Benchmark_128_10m -> 1536 / 128。"""
    m = re.match(r'benchmark_(?:sift_)?(\d+)', name or '', re.I)
    return m.group(1) if m else None


def table_color(name, fallback):
    """表名 -> 柱色(按维度取 DIM_COLOR, 未知维度用 fallback)。"""
    return DIM_COLOR.get(table_dim(name) or '', fallback)

# sql_type 的默认绘制清单（题设指定）：默认图只画这里列出的形态。可用命令行
# --sql-types 覆盖（传了就按参数清单/顺序画），CSV 中没跑到的形态自动跳过。
CK_SQL_TYPES = ['normal', 'cast', 'cast_array', 'raw_bytes']
PG_SQL_TYPES = ['text_literal', 'array_int_cast', 'array_real_cast', 'with_array_cast']

# MyScale profile 名 -> CK 口径名（本脚本只用无计划缓存那一档：off ≈ ck_off）
MS_PMAP = {'off': 'ck_off', 'only_plan_cache': 'ck_only_plan_cache', 'plan_cache': 'ck_plan_cache'}
OFF_PROF = 'ck_off'                       # CK/MyScale 不使用查询计划缓存的 profile（折算公式全为 one）

# CK/MyScale 12 个耗时步骤列；ck_off 形态下只有 CK_OFF_STEPS 六列参与折算
STEPS12 = ['build_ast', 'check_qp', 'prep_plan', 'build_plan', 'pipe', 'after_pipe',
           'reuse_ast', 'reuse_plan', 'reuse_result', 'set_ast', 'set_plan', 'normalized_ast']
CK_OFF_STEPS = ['build_ast', 'check_qp', 'prep_plan', 'build_plan', 'pipe', 'after_pipe']
STEP_COLOR12 = {'build_ast': '#4e79a7', 'check_qp': '#f28e2b', 'prep_plan': '#e15759',
                'build_plan': '#76b7b2', 'pipe': '#2c5fc4', 'after_pipe': '#59a14f',
                'reuse_ast': '#edc948', 'reuse_plan': '#b07aa1', 'reuse_result': '#ff9da7',
                'set_ast': '#9c755f', 'set_plan': '#bab0ac', 'normalized_ast': '#86bcb6'}
LIGHT12 = {'build_plan', 'reuse_ast', 'reuse_result', 'set_plan', 'normalized_ast', 'after_pipe'}

# pgvector 7 个耗时步骤列（cache=0 时 eval/gcp 一般为空，绘图前按非空过滤）
STEPS7 = ['parse', 'analyze', 'plan', 'eval', 'gcp', 'exec', 'vin']
STEP_COLOR7 = {'parse': '#b07aa1', 'analyze': '#f28e2b', 'plan': '#e15759', 'eval': '#76b7b2',
               'gcp': '#9c755f', 'exec': '#2c5fc4', 'vin': '#edc948'}
LIGHT7 = {'vin', 'eval'}
# pgvector 占比图只画 4 个核心阶段: 不再显示 eval/gcp/vin 的占比, 柱顶总耗时也
# 只按这 4 段求和(否则柱子占比合计不足 100%)。
PG_CHART_STEPS = ['parse', 'analyze', 'plan', 'exec']
# 第二组图: 横向比较 4 种计划缓存档(cache=0/1/2/3); 计划缓存开启后多出 eval
# (参数求值, 未命中缓存) 与 gcp(get_cached_plan, 命中缓存), 因此阶段取这 6 段。
PG_CACHE_STEPS = ['parse', 'analyze', 'plan', 'exec', 'eval', 'gcp']
PG_CACHE_ORDER = ['0', '1', '2', '3']

# CK/MyScale 第二组图: 3 种计划缓存档(ck_off / ck_only_plan_cache / ck_plan_cache),
# 在无缓存 6 段基础上加入计划复用阶段。
CK_CACHE_STEPS = ['build_ast', 'check_qp', 'prep_plan', 'build_plan', 'pipe', 'after_pipe',
                  'reuse_ast', 'reuse_plan', 'normalized_ast']
CK_CACHE_ORDER = ['ck_off', 'ck_only_plan_cache', 'ck_plan_cache']

# 第二组图的柱色: pg 按 cache 档位, CK/MS 按 profile 名
CACHE_COLORS = {'0': '#9aa5b1', '1': '#8fb3e8', '2': '#f2a65e', '3': '#0ea5a4',
                'ck_off': '#9aa5b1', 'ck_only_plan_cache': '#8fb3e8', 'ck_plan_cache': '#0ea5a4'}

# 5 个维度（组内柱）的颜色
DIM_COLOR = {'128': '#9aa5b1', '256': '#8fb3e8', '768': '#2c5fc4', '960': '#f2a65e', '1536': '#0ea5a4'}


def to_float(x):
    """把 CSV 字符串转 float；空串与占位符 ``-`` 视为缺失（None）。"""
    x = (x or '').strip()
    return None if x in ('', '-') else float(x)


def steady(x):
    """取 warm|steady 双值中的 steady 部分；'-' / '-|-' -> None"""
    x = (x or '').strip()
    if x in ('', '-'):
        return None
    if '|' in x:
        x = x.split('|')[-1].strip()
    return None if x in ('', '-') else float(x)


def steady_exec_n(x):
    """从 ``"<次数> / s=<秒>"`` 形态里取 steady 段的秒数；缺失返回 None。"""
    x = (x or '').strip()
    if '/ s=' in x:
        x = x.split('/ s=')[-1].strip()
    return None if x in ('', '-') else float(x)


def split_sql_types(s):
    """把 --sql-types 的输入(空格/逗号/中文逗号分隔)拆成清单; 空串 -> []。"""
    return [x for x in s.replace('，', ',').replace(',', ' ').split() if x]


def ordered_sql_types(found, preferred, wanted=None):
    """按 CSV 实际出现的 sql_type 生成绘制清单。

    wanted: --sql-types 传入的形态清单; 非空时只画其中的形态(顺序以 wanted 为准)。
    默认(wanted 为空)只画 preferred 内置清单里的形态, CSV 里其它形态不再追加,
    保证默认图形态固定; CSV 中没有的形态自动跳过。
    """
    seen = list(found)
    wanted = wanted or preferred
    types = [s for s in wanted if s in seen]
    dropped = [s for s in seen if s not in types]
    missing = [s for s in wanted if s not in seen]
    if dropped:
        print('  (info) 未在绘制清单中、已过滤: %s' % dropped, file=sys.stderr)
    if missing:
        print('  (info) 清单中无数据、已跳过: %s' % missing, file=sys.stderr)
    return types


def union_sql_types(datas, preferred, wanted=None):
    """多个维度数据的 sql_type 并集(按首次出现顺序去重)后套用 ordered_sql_types。"""
    found = []
    for data in datas:
        for st in data:
            if st not in found:
                found.append(st)
    return ordered_sql_types(found, preferred, wanted)


def load_ck_off(path, prof):
    """读 CK/MyScale 某维度 CSV 的 ck_off（或映射后的 off）行。

    返回 {sql_type: {'values': {step: ms}, 'total': ms}}；ck_off 折算公式全为 one，
    直接使用原始列值。仅保留 CK_OFF_STEPS 中该形态下不为空（非 '-'）的步骤。
    sql_type 以 CSV 实际存在的为准(不写死全量列表), 缺失形态不报错。
    """
    want = set(CK_OFF_STEPS)
    out = {}
    with open(path, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            p = MS_PMAP.get(r['profile'].strip(), r['profile'].strip())
            st = r['sql_type'].strip()
            if p != prof or not st:
                continue
            vals, present = {}, set()
            for s in CK_OFF_STEPS:
                v = to_float(r.get(s))
                vals[s] = v if v is not None else 0.0
                if v is not None:
                    present.add(s)
            out[st] = {'values': vals, 'total': round(sum(vals.values()), 6), 'present': present}
    return out


def load_ck_profiles_avg(path, steps):
    """读 CK/MyScale 分析 CSV 的全部计划缓存档行, 多轮数据按 (profile, sql_type) 平均。

    循环脚本每轮基准对同一 (profile, sql_type) 各产生一行, R 轮共 R 组数据; 本函数把
    同一组合的各行按阶段列取算术平均 (某轮缺失的阶段 '-' 不参与该阶段均值)。
    profile 经 MS_PMAP 统一成 CK 口径名(off->ck_off 等)。
    返回 {profile: {sql_type: {'values','total','present','rounds'}}}。
    """
    acc = {}
    with open(path, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            p = MS_PMAP.get(r['profile'].strip(), r['profile'].strip())
            st = r['sql_type'].strip()
            if not p or not st:
                continue
            per = acc.setdefault(p, {}).setdefault(st, {s: [] for s in steps})
            for s in steps:
                v = to_float(r.get(s))
                if v is not None:
                    per[s].append(v)
    out = {}
    for p, by_sql in acc.items():
        out[p] = {}
        for st, per in by_sql.items():
            vals = {s: (round(sum(v) / len(v), 6) if v else 0.0) for s, v in per.items()}
            present = {s for s, v in per.items() if v}
            out[p][st] = {'values': vals, 'total': round(sum(vals.values()), 6),
                          'present': present, 'rounds': max(len(v) for v in per.values())}
    return out


def load_ck_off_avg(path, prof):
    """读 CK/MyScale 某表分析 CSV 的指定 profile (off 档) 行, 多轮数据按 sql_type 平均。

    返回 {sql_type: {'values','total','present','rounds'}}; 只取 CK_OFF_STEPS 六列。
    """
    return load_ck_profiles_avg(path, CK_OFF_STEPS).get(prof, {})


def _pg_row(r):
    """把 pgvector 分析 CSV 的一行转成 {'values','total','present'}。

    vin 为按次数摊到每次执行的输入解析耗时: vector_in(vi_ms × vi# / exec#) +
    halfvec_in(hv_ms × hv# / exec#)。文本/数组路径只有 vi 计数, halfvec_literal
    只有 hv 计数, 两者互斥、直接相加。
    """
    en = steady_exec_n(r['exec#']) or 0.0
    raw = {s: steady(r['%s_ms' % s]) for s in STEPS7 if s != 'vin'}

    def per_exec(ms_col, n_col):
        ms, n = steady(r[ms_col]), steady(r[n_col])
        return ms * n / en if (ms is not None and en and n) else 0.0

    vin = round(per_exec('vi_ms', 'vi#') + per_exec('hv_ms', 'hv#'), 6)
    raw['vin'] = vin if vin > 0 else None
    present = {s for s, v in raw.items() if v is not None}
    vals = {s: (raw[s] if raw[s] is not None else 0.0) for s in STEPS7}
    return {'values': vals, 'total': round(sum(vals.values()), 6), 'present': present}


def load_pg_off(path):
    """读 pgvector 某数据集 CSV 的 cache=0 & res_cache=0（simple 协议）行。

    返回 {sql_type: {'values','total','present'}}。
    """
    out = {}
    with open(path, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            st = r['sql_type'].strip()
            if not st or int(r['cache']) != 0 or int(r['res_cache']) != 0:
                continue
            out[st] = _pg_row(r)
    return out


def load_pg_caches(path):
    """读 pgvector 某数据集 CSV 的全部计划缓存档(cache=0/1/2/3, res_cache=0)行。

    返回 {cache(字符串): {sql_type: row}}; row 字段同 load_pg_off。用于第二组
    "4 种 cache 情况"占比图。
    """
    out = {}
    with open(path, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            st = r['sql_type'].strip()
            if not st or int(r['res_cache']) != 0:
                continue
            out.setdefault(str(int(r['cache'])), {})[st] = _pg_row(r)
    return out


def types_in_all(types, by_series):
    """只保留在所有 series(各 cache 档)里都有数据的 sql_type, 避免绘图 KeyError。"""
    out = [st for st in types if all(st in data for data in by_series.values())]
    dropped = [st for st in types if st not in out]
    if dropped:
        print('  (info) 部分 cache 档缺数据、已跳过: %s' % dropped, file=sys.stderr)
    return out


def restrict_steps(data, steps):
    """把每行 total 改为仅所选阶段之和。

    draw_share 用 row['total'] 作 100% 堆叠的分母; 只画部分阶段时必须同步重算
    分母, 否则柱内占比合计不到 100%, 柱顶的 ms/query 也会把未画的阶段算进去。
    """
    for row in data.values():
        row['total'] = round(sum(row['values'].get(s, 0.0) for s in steps), 6)
    return data


# ---------------- 绘图 ----------------
INK, MUTED, RULE = '#182230', '#5f6f84', '#dfe6f0'   # 正文/轴标签文字色、弱化文字色、网格线/轴线色


def fmt_val(v):
    """按数值量级选择小数位：越大越粗（>=1 两位，>=0.1 三位，其余四位）。"""
    if v >= 1:
        return '%.2f' % v
    if v >= 0.1:
        return '%.3f' % v
    return '%.4f' % v


def draw_share(by_dim, sql_types, steps, step_colors, light_set, dim_labels,
               title, out_path, figw=13.0, dims=None, dim_colors=None):
    """阶段耗时占比图（100% 堆叠）：横轴=sql_type 分组，组内每根柱=一个维度。

    柱内按阶段堆叠并归一化到 100%，段内标注占比；柱顶标注 ms/query 总耗时；
    柱底短横条标出该柱对应的维度颜色（维度图例在图下方）。
    dims/dim_colors: 组内柱的清单与颜色; 默认取 by_dim 的键 / DIM_COLOR(未知键用灰色)。
    """
    dims = dims if dims is not None else list(by_dim)
    dim_colors = dim_colors if dim_colors is not None else {d: DIM_COLOR.get(d, '#9aa5b1') for d in dims}
    ndim, nsql = len(dims), len(sql_types)
    fig, ax = plt.subplots(figsize=(figw, 6.8), dpi=200)
    step_handles = [Patch(facecolor=step_colors[s], label=s) for s in steps]
    dim_handles = [Patch(facecolor=dim_colors[d], label=dim_labels[d]) for d in dims]
    w = 0.84 / ndim                     # 柱宽：组内 5 根
    for j, d in enumerate(dims):
        xj = [i + (j - (ndim - 1) / 2.0) * (w * 1.04) for i in range(nsql)]   # *1.04=组内柱间留缝
        bottom = [0.0] * nsql
        for s in steps:
            shares, bottoms = [], list(bottom)
            for xi, st in zip(xj, sql_types):
                row = by_dim.get(d, {}).get(st)
                share = row['values'][s] / row['total'] * 100.0 if row and row['total'] > 0 else 0.0
                if share > 0:
                    ax.bar(xi, share, bottom=bottoms[sql_types.index(st)], width=w,
                           color=step_colors[s], edgecolor='white', linewidth=0.4, zorder=3)
                shares.append(share)
            for xi, share, b in zip(xj, shares, bottoms):
                # 段内占比标注：>=4% 才画，避免过密；浅色段用深色字
                if share >= 4.0:
                    ax.text(xi, b + share / 2.0, '%d%%' % round(share), ha='center', va='center',
                            fontsize=7.0, color=INK if s in light_set else 'white', zorder=4)
            bottom = [b + sh for b, sh in zip(bottom, shares)]
        for xi, st in zip(xj, sql_types):
            row = by_dim.get(d, {}).get(st)
            if not row:      # 该维度缺这个 sql_type(分析 CSV 无数据), 不画柱顶总耗时
                continue
            tot = row['total']
            ax.text(xi, 101.5, fmt_val(tot), ha='center', va='bottom',   # 柱顶 ms/query 总耗时
                    fontsize=6.6, color=INK, zorder=5)
            # 柱底维度色条（画在轴下方，clip_on=False），与下方维度图例对应；
            # y=-5.5 让色条落在 sql_type 刻度文字下方，避免与刻度名重叠
            ax.plot([xi - w * 0.38, xi + w * 0.38], [-5.5, -5.5], color=dim_colors[d],
                    lw=3.0, solid_capstyle='butt', clip_on=False, zorder=5)
    for i in range(0, nsql, 2):
        ax.axvspan(i - 0.5, i + 0.5, color='#2f6fed', alpha=0.045, zorder=0)   # 隔列底色条
    ax.set_xlim(-0.5, nsql - 0.5)
    ax.set_ylim(0, 108)   # 顶部留出总耗时标注空间
    ax.set_yticks([0, 20, 40, 60, 80, 100])
    ax.set_xticks(range(nsql))
    ax.set_xticklabels(sql_types, fontsize=9.5, color=INK)
    ax.set_ylabel('share of per-query time (%)', fontsize=10, color=MUTED)
    ax.tick_params(axis='y', labelsize=8.5, colors=MUTED)
    ax.grid(axis='y', color=RULE, linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    for sp in ('left', 'bottom'):
        ax.spines[sp].set_color(RULE)
    ax.legend(handles=step_handles, loc='lower center', bbox_to_anchor=(0.5, 1.002),
              ncol=min(len(steps), 12), fontsize=8, frameon=False, columnspacing=1.2,
              handlelength=1.2, handleheight=0.9)   # 阶段图例：贴顶
    fig.legend(handles=dim_handles, loc='lower center', bbox_to_anchor=(0.5, -0.075), ncol=len(dims),
               fontsize=8, frameon=False, columnspacing=1.4, handlelength=1.2,
               handleheight=0.9)                   # 维度图例：贴底（与柱底色条对应，留出刻度文字空间）
    ax.set_title(title, loc='left', fontsize=11.5, color=INK, pad=30)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches='tight', pad_inches=0.35, facecolor='white')
    plt.close(fig)


# ---------------- main ----------------
def pick_steps(frames, all_steps):
    """跨维度取并集：只保留"无计划缓存"形态下至少在一个维度不为空的步骤。"""
    present = set().union(*(row['present'] for f in frames.values() for row in f.values()))
    return [s for s in all_steps if s in present]


# ---------------- 解析模式: PG 日志 -> 汇总 CSV(表名+抽样数量命名) -> 占比图 ----------------
def parse_args():
    ap = argparse.ArgumentParser(
        description="pgvector 耗时分析: 无参数=用 run-benchmark-loop.sh 产出的 CSV 画默认图; "
                    "-n 指定抽样数量后=解析 PG 日志模式(支持循环多轮累计日志)")
    ap.add_argument("-t", "--table",
                    help="被测数据表名(如 benchmark_sift_128), 用于输出文件名与 analyze 引擎推断;"
                         "配合 --engine clickhouse/myscale 时为 CK/MS 的数据表名")
    ap.add_argument("-n", "--row-count", type=int,
                    help="随机抽样向量条数(数值输入); 给出即进入日志解析/图表模式")
    ap.add_argument("--engine", choices=["pgvector", "clickhouse", "myscale"],
                    default="pgvector",
                    help="-t/-n 模式下的数据库类型: pgvector=解析 PG 日志(默认); "
                         "clickhouse/myscale=读既有分析 CSV 按多轮平均画图")
    ap.add_argument("--pglog", default="/usr/local/pgsql/data/log/postgresql.log",
                    help="PostgreSQL 服务日志路径(默认 /usr/local/pgsql/data/log/postgresql.log)")
    ap.add_argument("--runs", default="all",
                    help="合并日志中的运行批次数: all(默认) 或正整数 N(传给 analyze --batches)")
    ap.add_argument("--no-chart", action="store_true", help="只写解析汇总 CSV, 不画图")
    ap.add_argument("--cache-charts", action="store_true",
                    help="额外生成第二组图(各计划缓存档位); 默认只输出无查询计划缓存的默认图")
    ap.add_argument("--sql-types", default="",
                    help="绘图包含的 sql_type 清单, 空格/逗号分隔(如 \"normal cast\"), "
                         "顺序即图中横轴顺序; 默认空=脚本内置默认清单。CSV 中不存在的形态自动跳过")
    args = ap.parse_args()
    args.sql_types = split_sql_types(args.sql_types)
    return args


def parse_log_mode(args):
    """解析 PG 日志(可含循环多轮运行), 按 <表名>-<抽样数量> 命名输出汇总 CSV 并画占比图。

    复用 analyze_pgvector_log.py: --batches all --aggregate 把日志里的每一轮
    (时间连续批次)独立分析后按格子平均合并, 解决"只能解析一次执行"的问题。
    """
    n = args.row_count
    name = "%s-%d" % (args.table or "unknown", n)
    out_csv = os.path.join(RAW, "pgvector_log_analysis-%s.csv" % name)
    analyzer = os.path.join(BASE, "analyze_pgvector_log.py")
    cmd = [sys.executable, analyzer, "--log", args.pglog, "--batches", str(args.runs),
           "--aggregate", "--out", out_csv]
    if args.table:
        cmd += ["--engine", args.table]
    print("[cmd]", " ".join(cmd), flush=True)
    rc = subprocess.call(cmd)
    if rc != 0:
        raise SystemExit("[ERR] 日志解析失败(analyze_pgvector_log.py 退出码 %d)" % rc)
    print("[OK] 日志解析汇总: %s" % out_csv)
    if args.no_chart:
        return
    if not load_mpl():
        return
    pg = load_pg_off(out_csv)
    types = ordered_sql_types(pg.keys(), PG_SQL_TYPES, args.sql_types)
    if not types:
        print("[warn] %s 无 cache=0 数据, 跳过占比图" % os.path.basename(out_csv))
        return
    dim = args.table or "unknown"
    steps = PG_CHART_STEPS
    restrict_steps(pg, steps)
    chart = os.path.join(CHART_DIR, "pgvector_share_off_%s.png" % name)
    draw_share({dim: pg}, types, steps, STEP_COLOR7, LIGHT7,
               {dim: "%s (n=%d)" % (dim, n)},
               "pgvector %s (n=%d): stage share per query, no plan cache (cache=0); "
               "top label = ms/query" % (dim, n),
               chart, dims=[dim], dim_colors={dim: DIM_COLOR['128']})
    print("[OK] 占比图: %s" % chart)

    # ---- 第二组(可选, --cache-charts): 4 种计划缓存档(cache=0/1/2/3), 含 eval/gcp ----
    if not args.cache_charts:
        return
    caches = load_pg_caches(out_csv)
    series = {c: caches[c] for c in PG_CACHE_ORDER if c in caches}
    if not series:
        return
    for data in series.values():
        restrict_steps(data, PG_CACHE_STEPS)
    cache_types = types_in_all(union_sql_types(series.values(), PG_SQL_TYPES,
                                              args.sql_types), series)
    if not cache_types:
        return
    chart2 = os.path.join(CHART_DIR, "pgvector_share_cache_%s.png" % name)
    draw_share(series, cache_types, PG_CACHE_STEPS, STEP_COLOR7, LIGHT7,
               {c: 'cache=%s' % c for c in series},
               "pgvector %s (n=%d): stage share per query, plan-cache modes "
               "(cache=0/1/2/3); top label = ms/query" % (dim, n),
               chart2, dims=list(series), dim_colors=CACHE_COLORS)
    print("[OK] 占比图: %s" % chart2)


def ck_ms_chart_mode(args):
    """CK/MyScale 占比图模式: 读 <engine>-<表名>-log-analysis-<n>.csv, 多轮取平均画图。

    循环脚本每个组合跑 R 轮基准后用 analyze_myscale_log.py 产出一份分析 CSV, 其中
    同一 (profile, sql_type) 有 R 行 (每轮一组); 本函数对无计划缓存档 (clickhouse=
    ck_off / myscale=off, 经 MS_PMAP 统一映射) 的各行按阶段取算术平均后画占比图。
    """
    eng, tbl, n = args.engine, args.table or "unknown", args.row_count
    name = "%s-%d" % (tbl, n)
    src = os.path.join(RAW, "%s-%s-log-analysis-%d.csv" % (eng, tbl, n))
    if not os.path.exists(src):
        bak = os.path.join(BAK, os.path.basename(src))
        if os.path.exists(bak):
            src = bak
        else:
            raise SystemExit("[ERR] 找不到日志分析 CSV: %s "
                             "(请先用 run-benchmark-loop.sh 跑完该组合)" % src)
    data = load_ck_off_avg(src, OFF_PROF)
    types = ordered_sql_types(data.keys(), CK_SQL_TYPES, args.sql_types)
    if not types:
        raise SystemExit("[ERR] %s 无 %s 数据" % (src, OFF_PROF))
    steps = pick_steps({tbl: data}, CK_OFF_STEPS)
    print("[%s] 各 sql_type 平均耗时 (ms/query, %s 轮平均):"
          % (os.path.basename(src), max(d['rounds'] for d in data.values())))
    for st in types:
        row = data[st]
        det = ", ".join("%s=%s" % (s, fmt_val(row['values'][s]))
                        for s in steps if row['values'][s] > 0)
        print("  %-12s 总计 %-8s (%d 轮): %s" % (st, fmt_val(row['total']), row['rounds'], det))
    if not load_mpl():
        return
    os.makedirs(CHART_DIR, exist_ok=True)
    chart = os.path.join(CHART_DIR, "%s_share_off_%s.png" % (eng, name))
    draw_share({tbl: data}, types, steps, STEP_COLOR12, LIGHT12,
               {tbl: "%s (n=%d)" % (tbl, n)},
               "%s %s (n=%d): stage share per query, no plan cache (%s); "
               "top label = ms/query" % (eng, tbl, n,
                                         'ck_off' if eng == 'clickhouse' else 'off'),
               chart, dims=[tbl], dim_colors={tbl: DIM_COLOR['128']})
    print("[OK] 占比图: %s" % chart)

    # ---- 第二组(可选, --cache-charts): 3 种计划缓存档(含复用阶段) ----
    if not args.cache_charts:
        return
    profs = load_ck_profiles_avg(src, CK_CACHE_STEPS)
    series = {p: profs[p] for p in CK_CACHE_ORDER if p in profs}
    if not series:
        return
    cache_types = types_in_all(union_sql_types(series.values(), CK_SQL_TYPES,
                                              args.sql_types), series)
    if not cache_types:
        return
    chart2 = os.path.join(CHART_DIR, "%s_share_cache_%s.png" % (eng, name))
    draw_share(series, cache_types, CK_CACHE_STEPS, STEP_COLOR12, LIGHT12,
               {k: k for k in series},
               "%s %s (n=%d): stage share per query, plan-cache modes "
               "(ck_off/only_plan_cache/plan_cache); top label = ms/query" % (eng, tbl, n),
               chart2, dims=list(series), dim_colors=CACHE_COLORS)
    print("[OK] 占比图: %s" % chart2)


def main():
    args = parse_args()
    os.makedirs(CHART_DIR, exist_ok=True)
    if args.row_count is not None:
        if args.engine == "pgvector":
            parse_log_mode(args)
        else:
            ck_ms_chart_mode(args)
        return
    if not load_mpl():
        raise SystemExit('[ERR] 生成图表需要 matplotlib, 或改用日志解析模式: '
                         '%s -t <表名> -n <条数> --no-chart' % os.path.basename(__file__))

    # ---- 默认图: 不使用查询计划缓存（数据文件取自 run-benchmark-loop.sh 的产出） ----
    # ClickHouse: ck_off
    ck_paths = {t: resolve_optional(loop_csv('clickhouse', t, CK_ROW_COUNT)) for t in CK_TABLES}
    ck_paths = {t: p for t, p in ck_paths.items() if p}
    ck = {t: load_ck_off(p, OFF_PROF) for t, p in ck_paths.items()}
    if ck:
        ck_steps = pick_steps(ck, CK_OFF_STEPS)
        ck_types = union_sql_types(ck.values(), CK_SQL_TYPES, args.sql_types)
        draw_share(ck, ck_types, ck_steps, STEP_COLOR12, LIGHT12, {t: t for t in ck},
                   'ClickHouse: stage share per query, no plan cache (ck_off); top label = ms/query',
                   os.path.join(CHART_DIR, 'clickhouse_share_off.png'),
                   dims=list(ck), dim_colors={t: table_color(t, '#9aa5b1') for t in ck})
    else:
        print('[warn] 未找到 ClickHouse 分析 CSV(clickhouse-*-log-analysis-1000.csv), 跳过',
              file=sys.stderr)

    # MyScale: off（映射到 ck_off 口径）
    ms_paths = {t: resolve_optional(loop_csv('myscale', t, CK_ROW_COUNT)) for t in CK_TABLES}
    ms_paths = {t: p for t, p in ms_paths.items() if p}
    ms = {t: load_ck_off(p, OFF_PROF) for t, p in ms_paths.items()}
    if ms:
        ms_steps = pick_steps(ms, CK_OFF_STEPS)
        ms_types = union_sql_types(ms.values(), CK_SQL_TYPES, args.sql_types)
        draw_share(ms, ms_types, ms_steps, STEP_COLOR12, LIGHT12, {t: t for t in ms},
                   'MyScale: stage share per query, no plan cache (off); top label = ms/query',
                   os.path.join(CHART_DIR, 'myscale_share_off.png'),
                   dims=list(ms), dim_colors={t: table_color(t, '#9aa5b1') for t in ms})
    else:
        print('[warn] 未找到 MyScale 分析 CSV(myscale-*-log-analysis-1000.csv), 跳过',
              file=sys.stderr)

    # pgvector: 1000 / 1，cache=0
    pg_paths_by_n = {}
    for suffix in ('1000', '1'):
        paths = {t: resolve_optional(loop_csv('pgvector', t, suffix)) for t in PG_TABLES}
        paths = {t: p for t, p in paths.items() if p}
        pg_paths_by_n[suffix] = paths
        if not paths:
            print('[warn] 未找到 pgvector_log_analysis-*-%s.csv, 跳过' % suffix, file=sys.stderr)
            continue
        pg = {t: load_pg_off(p) for t, p in paths.items()}
        for t in pg:
            restrict_steps(pg[t], PG_CHART_STEPS)
        pg_types = union_sql_types(pg.values(), PG_SQL_TYPES, args.sql_types)
        draw_share(pg, pg_types, PG_CHART_STEPS, STEP_COLOR7, LIGHT7,
                   {t: t for t in pg},
                   'pgvector (n=%s): stage share per query, no plan cache (cache=0); '
                   'top label = ms/query' % suffix,
                   os.path.join(CHART_DIR, 'pgvector_%s_share_off.png' % suffix),
                   dims=list(pg), dim_colors={t: table_color(t, '#9aa5b1') for t in pg})
        print('pgvector(n=%s) steps: %s' % (suffix, PG_CHART_STEPS))

    if not args.cache_charts:
        print('charts saved to', CHART_DIR)
        return

    # ---- 第二组(可选): 各计划缓存档位（每个档位一张图，柱=各表） ----
    ck_prof = {t: load_ck_profiles_avg(p, CK_CACHE_STEPS) for t, p in ck_paths.items()}
    for prof in CK_CACHE_ORDER:
        by_tbl = {t: ck_prof[t][prof] for t in ck_prof if prof in ck_prof[t]}
        if not by_tbl:
            continue
        types2 = types_in_all(union_sql_types(by_tbl.values(), CK_SQL_TYPES,
                                              args.sql_types), by_tbl)
        if not types2:
            continue
        draw_share(by_tbl, types2, CK_CACHE_STEPS, STEP_COLOR12, LIGHT12,
                   {t: t for t in by_tbl},
                   'ClickHouse: stage share per query, plan-cache mode %s; '
                   'top label = ms/query' % prof,
                   os.path.join(CHART_DIR, 'clickhouse_share_cache_%s.png' % prof),
                   dims=list(by_tbl), dim_colors={t: table_color(t, '#9aa5b1') for t in by_tbl})

    ms_prof = {t: load_ck_profiles_avg(p, CK_CACHE_STEPS) for t, p in ms_paths.items()}
    for prof in CK_CACHE_ORDER:
        by_tbl = {t: ms_prof[t][prof] for t in ms_prof if prof in ms_prof[t]}
        if not by_tbl:
            continue
        types2 = types_in_all(union_sql_types(by_tbl.values(), CK_SQL_TYPES,
                                              args.sql_types), by_tbl)
        if not types2:
            continue
        draw_share(by_tbl, types2, CK_CACHE_STEPS, STEP_COLOR12, LIGHT12,
                   {t: t for t in by_tbl},
                   'MyScale: stage share per query, plan-cache mode %s; '
                   'top label = ms/query' % prof,
                   os.path.join(CHART_DIR, 'myscale_share_cache_%s.png' % prof),
                   dims=list(by_tbl), dim_colors={t: table_color(t, '#9aa5b1') for t in by_tbl})

    for suffix, paths in pg_paths_by_n.items():
        if not paths:
            continue
        pg_cache = {t: load_pg_caches(p) for t, p in paths.items()}
        for c in PG_CACHE_ORDER:
            by_tbl = {t: pg_cache[t][c] for t in pg_cache if c in pg_cache[t]}
            if not by_tbl:
                continue
            for data in by_tbl.values():
                restrict_steps(data, PG_CACHE_STEPS)
            types2 = types_in_all(union_sql_types(by_tbl.values(), PG_SQL_TYPES,
                                                  args.sql_types), by_tbl)
            if not types2:
                continue
            draw_share(by_tbl, types2, PG_CACHE_STEPS, STEP_COLOR7, LIGHT7,
                       {t: t for t in by_tbl},
                       'pgvector (n=%s): stage share per query, plan-cache mode cache=%s; '
                       'top label = ms/query' % (suffix, c),
                       os.path.join(CHART_DIR, 'pgvector_%s_share_cache_%s.png' % (suffix, c)),
                       dims=list(by_tbl), dim_colors={t: table_color(t, '#9aa5b1') for t in by_tbl})

    print('charts saved to', CHART_DIR)


if __name__ == '__main__':
    main()
