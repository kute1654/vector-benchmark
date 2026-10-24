# -*- coding: utf-8 -*-
"""
向量数据库耗时分析：读 CSV -> 按口径折算 -> 生成各数据库的耗时图 PNG（仅图，无文字报告）。

每个数据库只输出三张图：
  ClickHouse   ：不使用结果缓存融合图 / 使用结果缓存融合图 / 总耗时对比图
  MyScale      ：不使用结果缓存融合图 / 使用结果缓存融合图 / 总耗时对比图
  pgvector     ：不使用结果缓存融合图 / 使用结果缓存融合图 / 总耗时对比图
pgvector 因需要在一张图上放置全部 11 类 SQL 语句类型，故采用横向柱状图
（纵轴为 sql_type，横轴为耗时；每条 sql_type 行内再按 profile 分组、按步骤堆叠）。

数据源（results/）：
  clickhouse-Benchmark_256_290k-log-analysis-1000.csv   （CK，6 profile × 5 sql_type）
  myscale-Benchmark_256_290k-log-analysis-1000.csv      （MyScale，profile 名与 CK 不同，口径相同）
  pgvector_log_analysis-nytimes-256-angular-1000.csv    （pgvector 1000 条 SQL，11 类 sql_type）
  pgvector_log_analysis-nytimes-256-angular-1.csv       （pgvector 1 条 SQL，11 类 sql_type）

用法：
  python gen_step_time_report.py                 # 生成全部图表

可配置项（按需修改后重跑）：
  PV_PARSE_MODE   pgvector 的向量文本解析器：strtof / inline / fast_float
  PV_SQL_TYPES    参与绘图的 sql_type 列表（CSV 共 11 种）
"""
import csv
import math
import os

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

BASE = os.path.dirname(os.path.abspath(__file__))                            # 脚本目录
RAW = os.path.join(BASE, 'results')                                          # 数据目录
CHART_DIR = os.path.join(RAW, 'charts')                                      # 图输出目录

# ---------------- 可配置项 ----------------
PV_PARSE_MODE = 'strtof'                                                  # strtof / inline / fast_float
PV_SQL_TYPES = ['text_literal', 'text_cast', 'vector_fn',
                'array_int_cast', 'array_real_cast',
                'array_cast', 'array_to_vector_fn', 'halfvec_literal',
                'with_text_literal', 'with_array_cast', 'with_vector_fn']  # CSV 共 11 种

# ---------------- CK / MyScale 口径 ----------------
# CK/MyScale 12 个耗时步骤列；增删步骤需同步 FORMULA12 / STEP_COLOR12 / LIGHT12
STEPS12 = ['build_ast', 'check_qp', 'prep_plan', 'build_plan', 'pipe', 'after_pipe',
           'reuse_ast', 'reuse_plan', 'reuse_result', 'set_ast', 'set_plan', 'normalized_ast']
# CK/MyScale sql_type 的读取/显示顺序（不在列表中的类型自动追加到末尾）
SQL_ORDER = ['cast', 'cast_array', 'normal', 'raw_bytes', 'raw_bytes_x']

# MyScale profile 名 -> CK 口径名
MS_PMAP = {'off': 'ck_off', 'only_plan_cache': 'ck_only_plan_cache', 'plan_cache': 'ck_plan_cache',
           'query_off': 'result_off', 'query_only_plan_cache': 'result_only_plan_cache',
           'query_plan_cache': 'result_plan_cache'}
# 6 个 profile：前 3 个=无结果缓存(off/only_plan_cache/plan_cache)，后 3 个=有结果缓存
CK_PROFILES = ['ck_off', 'ck_only_plan_cache', 'ck_plan_cache',
               'result_off', 'result_only_plan_cache', 'result_plan_cache']

# 折算公式矩阵（与 CK 报告一致）：
#   one=每次执行均发生; inv=按 1/select_n 均摊; planhit=按 (select_n-plan_miss_n)/select_n;
#   miss=仅结果未命中时发生，按 result_miss_n/select_n; hit=按 result_hit_n/select_n
FORMULA12 = {
    'ck_off': {'build_ast': 'one', 'check_qp': 'one', 'prep_plan': 'one', 'build_plan': 'one',
               'pipe': 'one', 'after_pipe': 'one'},
    'ck_only_plan_cache': {'build_ast': 'one', 'pipe': 'one', 'after_pipe': 'one',
                           'normalized_ast': 'one', 'check_qp': 'inv', 'prep_plan': 'inv',
                           'build_plan': 'inv', 'set_plan': 'inv', 'reuse_plan': 'planhit'},
    'ck_plan_cache': {'pipe': 'one', 'after_pipe': 'one', 'build_ast': 'inv', 'check_qp': 'inv',
                      'prep_plan': 'inv', 'build_plan': 'inv', 'set_ast': 'inv', 'set_plan': 'inv',
                      'reuse_ast': 'planhit', 'reuse_plan': 'planhit'},
    'result_off': {'build_ast': 'one', 'check_qp': 'one', 'after_pipe': 'one', 'prep_plan': 'miss',
                   'build_plan': 'miss', 'pipe': 'miss', 'reuse_result': 'hit'},
    'result_only_plan_cache': {'build_ast': 'one', 'check_qp': 'one', 'after_pipe': 'one',
                               'prep_plan': 'inv', 'build_plan': 'inv', 'pipe': 'miss',
                               'reuse_plan': 'miss', 'set_plan': 'miss', 'normalized_ast': 'miss',
                               'reuse_result': 'hit'},
    'result_plan_cache': {'after_pipe': 'one', 'reuse_ast': 'one', 'build_ast': 'inv',
                          'check_qp': 'inv', 'prep_plan': 'inv', 'build_plan': 'inv',
                          'set_ast': 'inv', 'set_plan': 'inv', 'pipe': 'miss',
                          'reuse_plan': 'miss', 'reuse_result': 'hit'},
}

STEP_COLOR12 = {'build_ast': '#4e79a7', 'check_qp': '#f28e2b', 'prep_plan': '#e15759',
                'build_plan': '#76b7b2', 'pipe': '#2c5fc4', 'after_pipe': '#59a14f',
                'reuse_ast': '#edc948', 'reuse_plan': '#b07aa1', 'reuse_result': '#ff9da7',
                'set_ast': '#9c755f', 'set_plan': '#bab0ac', 'normalized_ast': '#86bcb6'}   # 12 步骤柱颜色(key=STEPS12)
LIGHT12 = {'build_plan', 'reuse_ast', 'reuse_result', 'set_plan', 'normalized_ast', 'after_pipe'}   # 这些步骤的字用深色(否则白字)
PROF_COLOR6 = {'ck_off': '#9aa5b1', 'ck_only_plan_cache': '#8fb3e8', 'ck_plan_cache': '#2c5fc4',
               'result_off': '#f2a65e', 'result_only_plan_cache': '#7fd0cb',
               'result_plan_cache': '#0ea5a4'}   # 6 个 profile 的柱颜色

# ---------------- pgvector 口径 ----------------
STEPS7 = ['parse', 'analyze', 'plan', 'eval', 'gcp', 'exec', 'vin']   # pgvector 7 个耗时步骤列
# cache: 0=不使用查询计划缓存 1=查询计划缓存+auto 2=查询计划缓存+force_generic_plan+文本类型参数
#        （一条 SQL 产生 k+1=11 条 vector_in，k=10 为 top-k） 3=查询计划缓存+force_generic_plan+vector 类型参数
PROF8 = ['off', 'pc_auto', 'pc_gen_t', 'pc_gen_v', 'r_off', 'r_auto', 'r_gen_t', 'r_gen_v']   # 8 个 profile：前4=无结果缓存，后4=有结果缓存
PROF8_LABEL = {'off': 'off', 'pc_auto': 'pc·auto', 'pc_gen_t': 'pc·gen_t', 'pc_gen_v': 'pc·gen_v',
               'r_off': 'rc·off', 'r_auto': 'rc·auto', 'r_gen_t': 'rc·gen_t', 'r_gen_v': 'rc·gen_v'}   # 图例中显示名
PROF8_COLOR = {'off': '#9aa5b1', 'pc_auto': '#8fb3e8', 'pc_gen_t': '#f2a65e', 'pc_gen_v': '#2c5fc4',
               'r_off': '#d97706', 'r_auto': '#7fd0cb', 'r_gen_t': '#0ea5a4', 'r_gen_v': '#0b7285'}   # 8 个 profile 柱颜色
STEP_COLOR7 = {'parse': '#b07aa1', 'analyze': '#f28e2b', 'plan': '#e15759', 'eval': '#76b7b2',
               'gcp': '#9c755f', 'exec': '#2c5fc4', 'vin': '#edc948'}   # 7 步骤柱颜色(key=STEPS7)
LIGHT7 = {'vin', 'eval'}   # 这些步骤的字用深色(否则白字)
PV_SHORT4 = {'off': 'off', 'pc_auto': 'auto', 'pc_gen_t': 'gen_t', 'pc_gen_v': 'gen_v',
             'r_off': 'off', 'r_auto': 'auto', 'r_gen_t': 'gen_t', 'r_gen_v': 'gen_v'}   # 柱尾短名（横向图用）


def to_float(x):
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
    x = (x or '').strip()
    if '/ s=' in x:
        x = x.split('/ s=')[-1].strip()
    return None if x in ('', '-') else float(x)


def load_cklike(path, pmap):
    """读 CK/MyScale CSV 并按公式矩阵折算。返回 (sqlTypes, rows{ (profile,sql)->dict }, raws)"""
    rows, raws, seen = {}, {}, []
    with open(path, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            st = r['sql_type'].strip()
            prof = pmap.get(r['profile'].strip(), r['profile'].strip())
            if st not in seen:
                seen.append(st)
            sel = to_float(r['select_n']) or 0.0
            rh = to_float(r['result_hit_n']) or 0.0
            rm = to_float(r['result_miss_n']) or 0.0
            pm = to_float(r['plan_miss_n']) or 0.0
            raw = {}
            for s in STEPS12:
                raw[s] = to_float(r[s])
            raws[(prof, st)] = {'raw': raw, 'select_n': sel, 'result_hit_n': rh,
                                'result_miss_n': rm, 'plan_miss_n': pm}
    sql_types = [s for s in SQL_ORDER if s in seen] + [s for s in seen if s not in SQL_ORDER]
    for (prof, st), d in raws.items():
        sel, rh, rm, pm = d['select_n'], d['result_hit_n'], d['result_miss_n'], d['plan_miss_n']
        fac = {'one': 1.0, 'inv': 1.0 / sel if sel else 0.0,
               'planhit': (sel - pm) / sel if sel else 0.0,
               'miss': rm / sel if sel else 0.0, 'hit': rh / sel if sel else 0.0}
        vals = {}
        for s in STEPS12:
            f_mode = FORMULA12[prof].get(s)
            vals[s] = round(d['raw'][s] * fac[f_mode], 6) if (f_mode and d['raw'][s] is not None) else 0.0
        rows[(prof, st)] = {'values': vals, 'total': round(sum(vals.values()), 6)}
    return sql_types, rows, raws


def load_pgvector(path, parse_mode, sql_types):
    """读 pgvector CSV（取 steady 值），vin = vi_ms × vi# / exec#。返回 rows{ (prof,sql)->dict }"""
    rows = {}
    with open(path, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            if r['parse_mode'].strip() != parse_mode:
                continue
            st = r['sql_type'].strip()
            cache = int(r['cache'])
            rc = int(r['res_cache'])
            prof = ({0: 'r_off', 1: 'r_auto', 2: 'r_gen_t', 3: 'r_gen_v'} if rc else
                    {0: 'off', 1: 'pc_auto', 2: 'pc_gen_t', 3: 'pc_gen_v'})[cache]
            en = steady_exec_n(r['exec#(w/s)']) or 0.0
            vn = steady(r['vi#(w/s)']) or 0.0
            vi_ms = steady(r['vi_ms(w/s)'])
            vals = {
                'parse': steady(r['parse_ms(w/s)']) or 0.0,
                'analyze': steady(r['analyze_ms(w/s)']) or 0.0,
                'plan': steady(r['plan_ms(w/s)']) or 0.0,
                'eval': steady(r['eval_ms(w/s)']) or 0.0,
                'gcp': steady(r['gcp_ms(w/s)']) or 0.0,
                'exec': steady(r['exec_ms(w/s)']) or 0.0,
                'vin': round(vi_ms * vn / en, 6) if (vi_ms is not None and en and vn) else 0.0,
            }
            vals = {k: round(v, 6) for k, v in vals.items()}
            rows[(prof, st)] = {'values': vals, 'total': round(sum(vals.values()), 6),
                                'exec_n': en, 'vi_n': vn}
    missing = [(p, s) for p in PROF8 for s in sql_types if (p, s) not in rows]
    if missing:
        raise RuntimeError('pgvector 数据缺失: %s' % missing)
    return rows


# ---------------- 绘图 ----------------
INK, MUTED, RULE = '#182230', '#5f6f84', '#dfe6f0'   # 正文/轴标签文字色、弱化文字(数值等)色、网格线/轴线色


def nice_max(x):
    # x 轴刻度档位表：向上取最接近档位；需更细/更粗刻度可增删这里的数值
    for s in [0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.8, 1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 9, 10, 12, 15]:
        if x <= s:
            return s
    return math.ceil(x)


def fmt_val(v):
    if v >= 1:
        return '%.2f' % v
    if v >= 0.1:
        return '%.3f' % v
    return '%.4f' % v


# CK/MyScale 竖版融合图（每步耗时柱+占比标注），绘制后递减输出：ck_fusion_*.png / myscale_fusion_*.png
def draw_fusion(rows, prof_ids, shorts, prof_colors, sql_types, steps, step_colors, light_set,
                title, out_path, figw=13.0):
    nprof, nsql = len(prof_ids), len(sql_types)
    ymax = nice_max(max(rows[(p, s)]['total'] for p in prof_ids for s in sql_types))
    w = 0.86 / nprof                    # 柱宽：profile 越多单根越窄；0.86 可整体放大/缩小柱宽
    fig, ax = plt.subplots(figsize=(figw, 6.6), dpi=200)   # figw 由调用方传(默认 13.0)，6.6=图高
    from matplotlib.patches import Patch
    handles = [Patch(facecolor=step_colors[s], label=s) for s in steps]
    for j, p in enumerate(prof_ids):
        xj = [i + (j - (nprof - 1) / 2.0) * (w * 1.04) for i in range(nsql)]   # *1.04=profile 组间留缝，改大组间隔更宽
        bottom = [0.0] * nsql
        for s in steps:
            vals = [rows[(p, st)]['values'][s] for st in sql_types]
            ax.bar(xj, vals, bottom=bottom, width=w, color=step_colors[s],
                   edgecolor='white', linewidth=0.4, zorder=3)
            for xi, v, b, st in zip(xj, vals, bottom, sql_types):
                tot = rows[(p, st)]['total']
                # 只在占比>=4.5% 且 占y轴>=2.8% 时标注百分比，避免过密；调大则标注更少
                if v > 0 and tot > 0 and v / tot >= 0.045 and v / ymax >= 0.028:
                    ax.text(xi, b + v / 2.0, '%d%%' % round(v / tot * 100), ha='center', va='center',
                            fontsize=7.2, color=INK if s in light_set else 'white', zorder=4)
            bottom = [b + v for b, v in zip(bottom, vals)]
        for xi, st in zip(xj, sql_types):
            tot = rows[(p, st)]['total']
            ax.text(xi, tot + ymax * 0.012, fmt_val(tot), ha='center', va='bottom',   # 柱顶耗时数值，0.012=与柱顶间距
                    fontsize=7.3, color=INK, zorder=5)
            ax.text(xi, tot + ymax * 0.062, shorts[p], ha='center', va='bottom',   # profile 名，0.062=与数值间距
                    fontsize=8, fontweight='bold', color=prof_colors[p], zorder=5)
    for i in range(0, nsql, 2):
        ax.axvspan(i - 0.5, i + 0.5, color='#2f6fed', alpha=0.045, zorder=0)   # 隔列底色条，alpha 加深/减淡
    ax.set_xlim(-0.5, nsql - 0.5)
    ax.set_ylim(0, ymax * 1.17)   # y 轴上限：峰值×1.17，改大顶部更空
    ax.set_xticks(range(nsql))
    ax.set_xticklabels(sql_types, fontsize=9.5, color=INK)
    ax.set_ylabel('ms / query', fontsize=10, color=MUTED)
    ax.tick_params(axis='y', labelsize=8.5, colors=MUTED)
    ax.grid(axis='y', color=RULE, linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    for sp in ('left', 'bottom'):
        ax.spines[sp].set_color(RULE)
    ax.legend(handles=handles, loc='lower center', bbox_to_anchor=(0.5, 1.002),
              ncol=min(len(steps), 12), fontsize=8, frameon=False, columnspacing=1.2,
              handlelength=1.2, handleheight=0.9)   # 步骤图例：ncol 控制每行个数，1.002 使图例贴顶
    ax.set_title(title, loc='left', fontsize=11.5, color=INK, pad=30)   # pad=标题距图顶部距离，改大标题下移
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches='tight', facecolor='white')
    plt.close(fig)


# CK/MyScale 竖版总耗时图（仅 total 总柱+数值标注），绘制后递减输出：ck_total.png / myscale_total.png
def draw_total(rows, prof_ids, legend_labels, prof_colors, sql_types, out_path, title, figw=13.0):
    nprof, nsql = len(prof_ids), len(sql_types)
    ymax = nice_max(max(rows[(p, s)]['total'] for p in prof_ids for s in sql_types))
    w = 0.82 / nprof                    # 柱宽：profile 越多单根越窄；0.82 可整体放大/缩小
    fig, ax = plt.subplots(figsize=(figw, 6.0), dpi=200)   # figw 由调用方传(默认 13.0)，6.0=图高
    for j, p in enumerate(prof_ids):
        xj = [i + (j - (nprof - 1) / 2.0) * (w * 1.05) for i in range(nsql)]   # *1.05=profile 组间留缝
        vals = [rows[(p, st)]['total'] for st in sql_types]
        ax.bar(xj, vals, width=w, color=prof_colors[p], edgecolor='white', linewidth=0.4,
               label=legend_labels[p], zorder=3)
        for xi, v in zip(xj, vals):
            ax.text(xi, v + ymax * 0.012, fmt_val(v), ha='center', va='bottom',   # 柱顶数值，0.012=间距，rotation=90 竖排
                    fontsize=6.6, color=MUTED, rotation=90, zorder=4)
    for i in range(0, nsql, 2):
        ax.axvspan(i - 0.5, i + 0.5, color='#2f6fed', alpha=0.045, zorder=0)   # 隔列底色条
    ax.set_xlim(-0.5, nsql - 0.5)
    ax.set_ylim(0, ymax * 1.15)   # y 轴上限：峰值×1.15
    ax.set_xticks(range(nsql))
    ax.set_xticklabels(sql_types, fontsize=9.5, color=INK)
    ax.set_ylabel('ms / query', fontsize=10, color=MUTED)
    ax.tick_params(axis='y', labelsize=8.5, colors=MUTED)
    ax.grid(axis='y', color=RULE, linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    for sp in ('left', 'bottom'):
        ax.spines[sp].set_color(RULE)
    ax.legend(loc='lower center', bbox_to_anchor=(0.5, 1.002), ncol=nprof,
              fontsize=8 if nprof <= 6 else 7.3, frameon=False, columnspacing=1.0,
              handlelength=1.1, handleheight=0.9)   # profile 图例：ncol 每行个数，profile 多时字号自动变小
    ax.set_title(title, loc='left', fontsize=11.5, color=INK, pad=30)   # pad=标题距顶部距离
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches='tight', facecolor='white')
    plt.close(fig)


# pg 横向融合图（每行一 SQL 类型、行内步骤堆叠+占 j 标注），绘制后递减输出：pgvector*_fusion_*.png
def draw_fusion_h(rows, prof_ids, shorts, prof_colors, sql_types, steps, step_colors, light_set,
                  title, out_path, figw=13.5):
    """横向融合图：纵轴=sql_type（可容纳 11 类），每行内按 profile 分组、按步骤堆叠。"""
    from matplotlib.patches import Patch
    nprof, nsql = len(prof_ids), len(sql_types)
    ymax = nice_max(max(rows[(p, s)]['total'] for p in prof_ids for s in sql_types))
    bar_h, os_ = 0.5, 0.53              # 单根横柱高度 0.5；os_=组内相邻 profile 的纵向间距，改大行内更空
    group_span = (nprof - 1) * os_
    slot = max(1.0, group_span + 1.0)   # 相邻 sql_type 行中心距
    fig_h = nsql * slot * 0.5 + 1.6     # 画布高度：随行数自动；1.6 为标题/图例预留高度
    fig, ax = plt.subplots(figsize=(figw, fig_h), dpi=200)
    step_handles = [Patch(facecolor=step_colors[s], label=s) for s in steps]

    labels_y, labels_txt = [], []
    for i, st in enumerate(sql_types):
        center = i * slot
        for j, p in enumerate(prof_ids):
            yj = center + (j - (nprof - 1) / 2.0) * os_
            bottom = 0.0
            for s in steps:
                v = rows[(p, st)]['values'][s]
                tot = rows[(p, st)]['total']
                if v > 0:
                    ax.barh(yj, v, left=bottom, height=bar_h, color=step_colors[s],
                            edgecolor='white', linewidth=0.3, zorder=3)
                    # 只在占比>=5% 且 占x轴>=3% 时标注百分比，调大则标注更少
                    if tot > 0 and v / tot >= 0.05 and v / ymax >= 0.03:
                        ax.text(bottom + v / 2.0, yj, '%d%%' % round(v / tot * 100),
                                ha='center', va='center', fontsize=6.0,
                                color=INK if s in light_set else 'white', zorder=4)
                bottom += v
            ax.text(bottom + ymax * 0.02, yj, '%s %s' % (shorts[p], fmt_val(tot)),
                    ha='left', va='center', fontsize=6.6, color=prof_colors[p],
                    fontweight='bold', zorder=4)   # 行尾合计：'profile 数值'，0.02=距柱尾留白
        labels_y.append(center)
        labels_txt.append(st)

    ax.set_yticks(labels_y)
    ax.set_yticklabels(labels_txt, fontsize=8.5, color=INK)
    pady = group_span * 0.5 + 0.2
    ax.set_ylim(labels_y[0] - pady, labels_y[-1] + pady + 0.5)
    ax.set_xlim(0, ymax * 1.22)
    ax.set_xlabel('ms / query', fontsize=10, color=MUTED)
    ax.tick_params(axis='y', labelsize=8.5)
    ax.tick_params(axis='x', labelsize=8.5, colors=MUTED)
    ax.grid(axis='x', color=RULE, linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    for sp in ('left', 'bottom'):
        ax.spines[sp].set_color(RULE)
    leg_steps = ax.legend(handles=step_handles, loc='upper center', bbox_to_anchor=(0.5, 1.008),
                          ncol=min(len(steps), 14), fontsize=8, frameon=False,
                          columnspacing=1.1, handlelength=1.2, handleheight=0.9)   # 步骤图例：ncol 每行个数
    ax.set_title(title, loc='left', fontsize=11.5, color=INK, pad=16)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches='tight', pad_inches=0.5, facecolor='white')   # pad_inches=四周留白，改大则顶部更空(用于显示图例)
    plt.close(fig)


# pg 横向总耗时图（仅 total 总柱+数值标注），绘制后递减输出：pgvector*_total.png
def draw_total_h(rows, prof_ids, legend_labels, prof_colors, sql_types, out_path, title, figw=13.5):
    """横向总耗时对比图：纵轴=sql_type（可容纳 11 类），每行内按 profile 分组。"""
    nprof, nsql = len(prof_ids), len(sql_types)
    ymax = nice_max(max(rows[(p, s)]['total'] for p in prof_ids for s in sql_types))
    bar_h, os_ = 0.5, 0.53          # 单根横柱高度 0.5；os_=组内相邻 profile 纵向间距
    group_span = (nprof - 1) * os_
    slot = max(1.0, group_span + 1.0)
    fig_h = nsql * slot * 0.5 + 1.6   # 画布高度：随行数自动；1.6 标题/图例预留
    fig, ax = plt.subplots(figsize=(figw, fig_h), dpi=200)

    labels_y, labels_txt = [], []
    for i, st in enumerate(sql_types):
        center = i * slot
        for j, p in enumerate(prof_ids):
            yj = center + (j - (nprof - 1) / 2.0) * os_
            v = rows[(p, st)]['total']
            ax.barh(yj, v, height=bar_h, color=prof_colors[p], edgecolor='white',
                    linewidth=0.3, zorder=3)
            ax.text(v + ymax * 0.02, yj, '%s %s' % (legend_labels[p], fmt_val(v)),
                    ha='left', va='center', fontsize=5.6, color=prof_colors[p],
                    fontweight='bold', zorder=4)   # 行内 profile 合计，0.02=距柱尾留白
        labels_y.append(center)
        labels_txt.append(st)

    ax.set_yticks(labels_y)
    ax.set_yticklabels(labels_txt, fontsize=8.5, color=INK)
    pady = group_span * 0.5 + 0.2   # 顶部/底部首末行外留白
    ax.set_ylim(labels_y[0] - pady, labels_y[-1] + pady)
    ax.set_xlim(0, ymax * 1.3)   # x 轴上限：峰值×1.3，右侧为行内 profile 合计留空间
    ax.set_xlabel('ms / query', fontsize=10, color=MUTED)
    ax.tick_params(axis='y', labelsize=8.5)
    ax.tick_params(axis='x', labelsize=8.5, colors=MUTED)
    ax.grid(axis='x', color=RULE, linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    for sp in ('left', 'bottom'):
        ax.spines[sp].set_color(RULE)
    ax.set_title(title, loc='left', fontsize=11.5, color=INK, pad=16)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches='tight', pad_inches=0.5, facecolor='white')   # pad_inches=四周留白，改大则顶部更空(用于显示标题)
    plt.close(fig)


# ---------------- main ----------------
def prepare(charts=True):
    """加载数据并绘制图表（仅图表，不生成文字报告）。返回 G（各图表路径）。
    输出图：ck/myscale_fusion_norc·rc.png、ck/myscale_total.png、pgvector*_fusion_norc·rc.png、pgvector*_total.png"""
    os.makedirs(CHART_DIR, exist_ok=True)

    G = {
        'ck_norc': os.path.join(CHART_DIR, 'ck_fusion_norc.png'),
        'ck_rc': os.path.join(CHART_DIR, 'ck_fusion_rc.png'),
        'ck_total': os.path.join(CHART_DIR, 'ck_total.png'),
        'ms_norc': os.path.join(CHART_DIR, 'myscale_fusion_norc.png'),
        'ms_rc': os.path.join(CHART_DIR, 'myscale_fusion_rc.png'),
        'ms_total': os.path.join(CHART_DIR, 'myscale_total.png'),
        'pg_norc': os.path.join(CHART_DIR, 'pgvector_fusion_norc.png'),
        'pg_rc': os.path.join(CHART_DIR, 'pgvector_fusion_rc.png'),
        'pg_total': os.path.join(CHART_DIR, 'pgvector_total.png'),
        'pg1_norc': os.path.join(CHART_DIR, 'pgvector1_fusion_norc.png'),
        'pg1_rc': os.path.join(CHART_DIR, 'pgvector1_fusion_rc.png'),
        'pg1_total': os.path.join(CHART_DIR, 'pgvector1_total.png'),
    }

    if charts:
        sql_ck, rows_ck, _ = load_cklike(os.path.join(RAW, 'clickhouse-Benchmark_256_290k-log-analysis-1000.csv'), {})
        sql_ms, rows_ms, _ = load_cklike(os.path.join(RAW, 'myscale-Benchmark_256_290k-log-analysis-1000.csv'), MS_PMAP)
        sql_pg = PV_SQL_TYPES
        rows_pg = load_pgvector(os.path.join(RAW, 'pgvector_log_analysis-nytimes-256-angular-1000.csv'),
                                PV_PARSE_MODE, PV_SQL_TYPES)
        sql_pg1 = PV_SQL_TYPES
        rows_pg1 = load_pgvector(os.path.join(RAW, 'pgvector_log_analysis-nytimes-256-angular-1.csv'),
                                 PV_PARSE_MODE, PV_SQL_TYPES)

        # ---- ClickHouse：3 张图 ----
        draw_fusion(rows_ck, CK_PROFILES[:3],
                    {'ck_off': 'off', 'ck_only_plan_cache': 'only_plan', 'ck_plan_cache': 'plan'},
                    PROF_COLOR6, sql_ck, STEPS12, STEP_COLOR12, LIGHT12,
                    'ClickHouse: no result cache (off / only_plan_cache / plan_cache)', G['ck_norc'])
        draw_fusion(rows_ck, CK_PROFILES[3:],
                    {'result_off': 'off', 'result_only_plan_cache': 'only_plan',
                     'result_plan_cache': 'plan'},
                    PROF_COLOR6, sql_ck, STEPS12, STEP_COLOR12, LIGHT12,
                    'ClickHouse: with result cache (result_off / result_only_plan_cache / result_plan_cache)', G['ck_rc'])
        draw_total(rows_ck, CK_PROFILES, {p: p for p in CK_PROFILES}, PROF_COLOR6, sql_ck, G['ck_total'],
                   'ClickHouse: total time per query (6 profiles x 5 sql types)')

        # ---- MyScale：3 张图 ----
        draw_fusion(rows_ms, CK_PROFILES[:3],
                    {'ck_off': 'off', 'ck_only_plan_cache': 'only_plan', 'ck_plan_cache': 'plan'},
                    PROF_COLOR6, sql_ms, STEPS12, STEP_COLOR12, LIGHT12,
                    'MyScale: no result cache (off / only_plan_cache / plan_cache)', G['ms_norc'])
        draw_fusion(rows_ms, CK_PROFILES[3:],
                    {'result_off': 'off', 'result_only_plan_cache': 'only_plan',
                     'result_plan_cache': 'plan'},
                    PROF_COLOR6, sql_ms, STEPS12, STEP_COLOR12, LIGHT12,
                    'MyScale: with result cache (result_off / result_only_plan_cache / result_plan_cache)', G['ms_rc'])
        draw_total(rows_ms, CK_PROFILES, {p: p for p in CK_PROFILES}, PROF_COLOR6, sql_ms, G['ms_total'],
                   'MyScale: total time per query (6 profiles x 5 sql types)')

        # ---- pgvector (-1000)：3 张图（横向，含 11 类 sql_type） ----
        draw_fusion_h(rows_pg, PROF8[:4], PV_SHORT4, PROF8_COLOR, sql_pg, STEPS7, STEP_COLOR7, LIGHT7,
                      'pgvector (1000): no result cache (cache 0-3)', G['pg_norc'], figw=14.0)
        draw_fusion_h(rows_pg, PROF8[4:], PV_SHORT4, PROF8_COLOR, sql_pg, STEPS7, STEP_COLOR7, LIGHT7,
                      'pgvector (1000): with result cache (cache 0-3)', G['pg_rc'], figw=14.0)
        draw_total_h(rows_pg, PROF8, PROF8_LABEL, PROF8_COLOR, sql_pg, G['pg_total'],
                     'pgvector (1000): total time per query (8 profiles x 11 sql types)', figw=14.0)

        # ---- pgvector (-1)：3 张图（横向，含 11 类 sql_type） ----
        draw_fusion_h(rows_pg1, PROF8[:4], PV_SHORT4, PROF8_COLOR, sql_pg1, STEPS7, STEP_COLOR7, LIGHT7,
                      'pgvector (1): no result cache (cache 0-3)', G['pg1_norc'], figw=14.0)
        draw_fusion_h(rows_pg1, PROF8[4:], PV_SHORT4, PROF8_COLOR, sql_pg1, STEPS7, STEP_COLOR7, LIGHT7,
                      'pgvector (1): with result cache (cache 0-3)', G['pg1_rc'], figw=14.0)
        draw_total_h(rows_pg1, PROF8, PROF8_LABEL, PROF8_COLOR, sql_pg1, G['pg1_total'],
                     'pgvector (1): total time per query (8 profiles x 11 sql types)', figw=14.0)
        print('charts saved to', CHART_DIR)
    return G


def main():
    prepare(charts=True)


if __name__ == '__main__':
    main()