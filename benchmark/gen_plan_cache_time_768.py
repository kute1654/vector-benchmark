# -*- coding: utf-8 -*-
"""768 维查询计划缓存耗时对比图：四个数据库放在同一张图中。

每个数据库一个子图，横轴为 4 种 sql_type，组内每根柱对应一种缓存档位
（CK/MyScale 3 档，pgvector/PolarDB 4 档）。柱内按阶段堆叠并归一化为 100%
（段内标注占比），柱顶标注该档位每次查询的总耗时（ms/query），柱底短横条
用缓存档位颜色标识；既体现耗时占比，也体现总耗时，风格与
gen_step_time_report.py 的占比图一致。

数据源（results/csv/，缺失时回退 results/bak/）：
  clickhouse-*768*-log-analysis-1000.csv        ClickHouse（3 档 profile）
  myscale-*768*-log-analysis-1000.csv           MyScale（3 档 profile）
  pgvector_log_analysis-*768*-1000.csv          pgvector（cache=0/1/2/3）
  polardb_log_analysis-*768*.csv                PolarDB（cache=0/1/2/3）
缺少某个数据库的分析 CSV 时，该子图显示占位说明而不影响其它库。

用法：
  python gen_plan_cache_time_768.py
  python gen_plan_cache_time_768.py --sql-types "normal cast raw_bytes"
  python gen_plan_cache_time_768.py --out results/charts/xxx.png
"""

import argparse
import csv
import glob
import os
import sys

try:
    import gen_step_time_report as gst
except ImportError:          # 支持 python -m benchmark.gen_plan_cache_time_768
    from benchmark import gen_step_time_report as gst

DIM_KEY = '768'
OUT_NAME = 'plan_cache_time_768_4db.png'

# PG 计划缓存档阶段：按"自底向上"语义顺序定义。
# 原始列里 parse/analyze/plan/eval/gcp/exec 六段，但 analyze 与 eval、plan 与 gcp
# 是互斥的（命中缓存时走 eval+gcp、未命中时走 analyze+plan）。为了让堆叠顺序
# 自底向上总是 parse → (analyze | eval) → (plan | gcp) → exec，这里按 cache
# 档位各自指定一份白名单顺序（类似于 CK_PROFILE_STEPS）。
PG_PROFILE_STEPS = {
    '0': ['parse', 'analyze', 'plan', 'exec'],
    '1': ['parse', 'eval', 'gcp', 'exec'],
    '2': ['parse', 'eval', 'gcp', 'exec'],
    '3': ['parse', 'eval', 'gcp', 'exec'],
}
# 兼容 fallback（profile 不在表里时）
PG_CACHE_STEPS = ['parse', 'analyze', 'plan', 'eval', 'gcp', 'exec']

# 缓存档位显示名
CK_LABELS = {
    'ck_off': 'no plan cache',
    'ck_only_plan_cache': 'plan cache (only-vector)',
    'ck_plan_cache': 'plan cache',
}
PG_LABELS = {
    '0': 'no plan cache',
    '1': 'plan cache (auto)',
    '2': 'plan cache (custom)',
    '3': 'plan cache (generic)',
}


def pick_file(patterns):
    """在 results/csv 优先、results/bak 兜底地按 glob 模式查找分析 CSV。"""
    for base in (gst.RAW, gst.BAK):
        for pattern in patterns:
            hits = sorted(glob.glob(os.path.join(base, pattern)))
            if hits:
                return hits[0]
    return None


def load_ck_profiles(path):
    """读 CK/MyScale 分析 CSV 的全部计划缓存档，返回 {profile: {sql_type: row}}。"""
    profs = gst.load_ck_profiles_avg(path, gst.CK_CACHE_STEPS)
    return {p: profs[p] for p in gst.CK_CACHE_ORDER if p in profs}


def load_pg_caches(path):
    """读 pgvector 分析 CSV 的 cache=0/1/2/3 档，返回 {cache: {sql_type: row}}。

    每个 cache 档位用它自己的阶段白名单（PG_PROFILE_STEPS）过滤不可见阶段、
    并按该白名单重算每行的 total（保证堆叠顺序与 total 同口径）。
    """
    caches = gst.load_pg_caches(path)
    series = {c: caches[c] for c in gst.PG_CACHE_ORDER if c in caches}
    for cache, by_sql in series.items():
        allowed = PG_PROFILE_STEPS.get(cache, set(PG_CACHE_STEPS))
        for st, row in by_sql.items():
            vals = row['values']
            for s in PG_CACHE_STEPS:
                if s not in allowed:
                    vals[s] = 0.0
            row['total'] = round(sum(vals.get(s, 0.0) for s in PG_CACHE_STEPS), 6)
    return series


# CK/MyScale 各计划缓存档位要体现的阶段白名单（不可见阶段清零，total 按可见段重算）。
# ck_off          : 完全不命中缓存 -> build_ast/check_qp/prep_plan/build_plan/pipe/after_pipe 六段。
# ck_only_plan_cache: 只命中计划缓存、AST 仍要重新 parse -> build_ast/check_qp/build_plan/reuse_plan/pipe/after_pipe。
# ck_plan_cache   : AST 与计划都命中缓存 -> reuse_ast/build_plan/reuse_plan/pipe/after_pipe。
# build_plan 计入总耗时：ClickHouse 三档都算；MyScale 只有不命中计划缓存的档位算（见 MS_PROFILE_STEPS）。
CK_PROFILE_STEPS = {
    'ck_off':            ['build_ast', 'check_qp', 'prep_plan', 'build_plan', 'pipe', 'after_pipe'],
    'ck_only_plan_cache': ['build_ast', 'check_qp', 'build_plan', 'reuse_plan', 'pipe', 'after_pipe'],
    'ck_plan_cache':     ['reuse_ast', 'build_plan', 'reuse_plan', 'pipe', 'after_pipe'],
}

# MyScale 只有"不使用查询计划缓存"的档位算 build_plan；两个命中计划缓存的档位不计入。
MS_PROFILE_STEPS = {
    'ck_off':            ['build_ast', 'check_qp', 'prep_plan', 'build_plan', 'pipe', 'after_pipe'],
    'ck_only_plan_cache': ['build_ast', 'check_qp', 'reuse_plan', 'pipe', 'after_pipe'],
    'ck_plan_cache':     ['reuse_ast', 'reuse_plan', 'pipe', 'after_pipe'],
}


def filter_ck_profiles(series, profile_steps=None):
    """按档位白名单（默认 ``CK_PROFILE_STEPS``，MyScale 传 ``MS_PROFILE_STEPS``）把各缓存档
    不可见阶段清零，并按可见段重算 total。

    这样既保证堆叠柱 100% 的分母与分子完全同源，柱顶总耗时也等于柱内所有可见段之和。
    ck_off 不命中任何缓存，阶段清单与原始六段一致，total 不变。
    """
    steps = profile_steps or CK_PROFILE_STEPS
    for cache, by_sql in series.items():
        allowed = set(steps.get(cache, gst.CK_CACHE_STEPS))
        for st, row in by_sql.items():
            vals = row['values']
            for s in gst.CK_CACHE_STEPS:
                if s not in allowed:
                    vals[s] = 0.0
            row['total'] = round(sum(vals.get(s, 0.0) for s in gst.CK_CACHE_STEPS), 6)


def _norm_header(key):
    """去掉 PolarDB 表头里的 ``(w/s)`` / ``(warm/s)`` 后缀，与 pgvector 列名对齐。"""
    for suffix in ('(w/s)', '(warm/s)'):
        key = key.replace(suffix, '')
    return key.strip()


def load_polardb(path):
    """读 PolarDB 分析 CSV（表头带 w/s 后缀），返回 {cache: {sql_type: row}}。"""
    out = {}
    with open(path, encoding='utf-8-sig') as f:
        for r in csv.DictReader(f):
            r = {_norm_header(k): v for k, v in r.items()}
            st = (r.get('sql_type') or '').strip()
            try:
                cache = str(int(r['cache']))
                res_cache = int(r['res_cache'])
            except (KeyError, TypeError, ValueError):
                continue
            if not st or res_cache != 0:
                continue
            out.setdefault(cache, {})[st] = gst._pg_row(r)
    for data in out.values():
        gst.restrict_steps(data, gst.PG_CACHE_STEPS)
    return {c: out[c] for c in gst.PG_CACHE_ORDER if c in out}


def choose_types(series, preferred, wanted):
    """按各缓存档都存在的 sql_type 取交集，再套用默认清单/命令行清单。"""
    if not series:
        return []
    found = []
    for data in series.values():
        for st in data:
            if st not in found:
                found.append(st)
    types = gst.ordered_sql_types(found, preferred, wanted)
    return gst.types_in_all(types, series)


def draw_panel(ax, panel, show_ylabel):
    """画一个数据库面板（无面板级标题、无面板级图例，统一在 figure 级放）。"""
    series = panel['series']
    types = panel['types']
    if not series or not types:
        ax.set_axis_off()
        ax.text(0.5, 0.55, panel['name'], transform=ax.transAxes, ha='center',
                va='center', fontsize=12, color=gst.INK, fontweight='bold')
        ax.text(0.5, 0.44, panel['note'], transform=ax.transAxes, ha='center',
                va='center', fontsize=9, color=gst.MUTED, wrap=True)
        return

    step_colors = panel['step_colors']
    light = panel['light']
    cache_colors = gst.CACHE_COLORS
    caches = list(series)
    nc = len(caches)
    w = 0.82 / nc

    def order_for(cache):
        """返回该缓存档要按什么顺序堆叠阶段（自底向上）。"""
        if panel.get('per_profile_steps') and cache in panel['per_profile_steps']:
            return panel['per_profile_steps'][cache]
        if panel.get('pg_per_profile_steps') and cache in panel['pg_per_profile_steps']:
            return panel['pg_per_profile_steps'][cache]
        return panel['steps']

    for i, st in enumerate(types):
        for j, cache in enumerate(caches):
            row = series[cache].get(st)
            if not row or row['total'] <= 0:
                continue
            order = order_for(cache)
            xi = i + (j - (nc - 1) / 2.0) * w
            bottom = 0.0
            for step in order:
                share = row['values'].get(step, 0.0) / row['total'] * 100.0
                if share <= 0:
                    continue
                ax.bar(xi, share, bottom=bottom, width=w * 0.92,
                       color=step_colors[step], edgecolor='white', linewidth=0.35, zorder=3)
                if share >= 5.0:
                    ax.text(xi, bottom + share / 2.0, '%d%%' % round(share),
                            ha='center', va='center', fontsize=5.8,
                            color=gst.INK if step in light else 'white', zorder=4)
                bottom += share
            ax.text(xi, 102.0, gst.fmt_val(row['total']), ha='center', va='bottom',
                    fontsize=6.0, color='#c0392b', fontweight='bold',
                    rotation=90, zorder=5)
            ax.plot([xi - w * 0.40, xi + w * 0.40], [-8.0, -8.0],
                    color=cache_colors.get(cache, '#9aa5b1'), lw=2.6,
                    solid_capstyle='butt', clip_on=False, zorder=5)

    for i in range(0, len(types), 2):
        ax.axvspan(i - 0.5, i + 0.5, color='#2f6fed', alpha=0.045, zorder=0)
    ax.set_xlim(-0.5, len(types) - 0.5)
    ax.set_ylim(0, 110)
    ax.set_yticks([0, 20, 40, 60, 80, 100])
    ax.set_xticks(range(len(types)))
    ax.set_xticklabels(types, fontsize=8.5, color=gst.INK)
    if show_ylabel:
        ax.set_ylabel('share of per-query time (%)', fontsize=9, color=gst.MUTED)
    ax.tick_params(axis='y', labelsize=8, colors=gst.MUTED)
    ax.grid(axis='y', color=gst.RULE, linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    for sp in ('left', 'bottom'):
        ax.spines[sp].set_color(gst.RULE)

    # 面板名与阶段图例都放到 axes 外上方（紧贴），避免与柱顶耗时标注重叠
    ax.text(0.0, 1.01, panel['name'], transform=ax.transAxes,
            ha='left', va='bottom', fontsize=10.5, color=gst.INK,
            fontweight='bold')

    def step_is_used(s):
        for c in caches:
            for st2, row2 in series[c].items():
                if row2.get('values', {}).get(s, 0) > 0:
                    return True
        return False

    stage_handles = [gst.Patch(facecolor=step_colors[s], label=s)
                     for s in panel['steps']
                     if s != 'vin' and step_is_used(s)]
    # 阶段图例（紧贴轴外右上方，整行排列）——用 add_artist 保留，避免被后面的 cache legend 覆盖
    if stage_handles:
        leg_stage = ax.legend(handles=stage_handles, loc='lower right', bbox_to_anchor=(1.0, 1.01),
                              ncol=len(stage_handles), fontsize=6.2, frameon=False,
                              columnspacing=0.8, handlelength=1.0, handleheight=0.7)
        ax.add_artist(leg_stage)
    # 面板自己的缓存图例（放在轴外下方，下移一点但不过头）
    cache_handles = [gst.Patch(facecolor=cache_colors.get(c, '#9aa5b1'),
                               label=panel['labels'].get(c, c))
                     for c in caches]
    if cache_handles:
        ax.legend(handles=cache_handles, loc='upper center', bbox_to_anchor=(0.5, -0.16),
                  ncol=nc, fontsize=6.5, frameon=False, columnspacing=1.2,
                  handlelength=1.0, handleheight=0.7)


def build_panels(wanted):
    """查找四个数据库的 768 维分析 CSV 并组装子图数据。

    4 个子图：ClickHouse / MyScale / pgvector n=1000 / pgvector n=1。PolarDB 的 768
    维日志分析尚未产出，暂不占位子图。
    """
    # PG: 按 cache profile 给每个档位一份阶段堆叠顺序
    PG_PANEL_STEPS = ['parse', 'analyze', 'eval', 'plan', 'gcp', 'exec']
    specs = [
        ('ClickHouse',
         ['clickhouse-*%s*-log-analysis-1000.csv' % DIM_KEY],
         load_ck_profiles, gst.CK_SQL_TYPES, gst.CK_CACHE_STEPS,
         gst.STEP_COLOR12, gst.LIGHT12, CK_LABELS, filter_ck_profiles),
        ('MyScale',
         ['myscale-*%s*-log-analysis-1000.csv' % DIM_KEY],
         load_ck_profiles, gst.CK_SQL_TYPES, gst.CK_CACHE_STEPS,
         gst.STEP_COLOR12, gst.LIGHT12, CK_LABELS,
         lambda s: filter_ck_profiles(s, MS_PROFILE_STEPS)),
        ('pgvector (n=1000)',
         ['pgvector_log_analysis-*%s*-1000.csv' % DIM_KEY],
         load_pg_caches, gst.PG_SQL_TYPES, PG_PANEL_STEPS,
         gst.STEP_COLOR7, gst.LIGHT7, PG_LABELS, None),
        ('pgvector (n=1)',
         ['pgvector_log_analysis-*%s*-1.csv' % DIM_KEY],
         load_pg_caches, gst.PG_SQL_TYPES, PG_PANEL_STEPS,
         gst.STEP_COLOR7, gst.LIGHT7, PG_LABELS, None),
    ]

    panels = []
    for name, patterns, loader, preferred, steps, colors, light, labels, post in specs:
        path = pick_file(patterns)
        if path is None:
            panels.append({'name': name, 'series': {}, 'types': [],
                           'note': 'missing 768-dim analysis CSV (%s)' % patterns[0]})
            print('[warn] %s: 未找到 %s，子图留空' % (name, patterns[0]), file=sys.stderr)
            continue
        series = loader(path)
        if post is not None:
            post(series)
        types = choose_types(series, preferred, wanted)
        panel = {'name': name, 'series': series, 'types': types,
                 'steps': steps, 'step_colors': colors, 'light': light,
                 'labels': labels, 'note': '', 'path': path}
        # CK/MyScale: 每个 profile 有自己的阶段顺序（堆叠自底向上）；MyScale 单独一套
        if panel.get('series') and any(k in panel['series'] for k in CK_PROFILE_STEPS):
            panel['per_profile_steps'] = (MS_PROFILE_STEPS if name == 'MyScale'
                                          else CK_PROFILE_STEPS)
        # pgvector: 每个 cache 档位也有自己的阶段顺序
        if panel.get('series') and any(k in panel['series'] for k in PG_PROFILE_STEPS):
            panel['pg_per_profile_steps'] = PG_PROFILE_STEPS
        panels.append(panel)
        print('[OK] %s <- %s (%d sql_type × %d cache modes)'
              % (name, os.path.basename(path), len(types), len(series)))
    return panels


def parse_args():
    ap = argparse.ArgumentParser(
        description='768 维：查询计划缓存开/关的阶段耗时占比与总耗时对比（四库合一图）')
    ap.add_argument('--sql-types', default='',
                    help='要绘制的 sql_type 清单，空格/逗号分隔；默认各引擎内置 4 种形态')
    ap.add_argument('--out', default=os.path.join(gst.CHART_DIR, OUT_NAME),
                    help='输出 PNG 路径（默认 results/charts/%s）' % OUT_NAME)
    args = ap.parse_args()
    args.sql_types = gst.split_sql_types(args.sql_types)
    return args


def main():
    args = parse_args()
    panels = build_panels(args.sql_types)
    if not any(p['series'] for p in panels):
        raise SystemExit('[ERR] 没有任何数据库的 768 维分析 CSV，无法画图')
    if not gst.load_mpl():
        raise SystemExit('[ERR] 生成图表需要 matplotlib')

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    fig, axes = gst.plt.subplots(2, 2, figsize=(19.0, 10.5), dpi=170, sharex=False, sharey=True)
    for k, (ax, panel) in enumerate(zip(axes.flat, panels)):
        draw_panel(ax, panel, show_ylabel=(k % 2 == 0))
    fig.suptitle('768-dim per-query time: query plan cache OFF vs ON '
                 '(four databases × four SQL types)', fontsize=13.5, color=gst.INK,
                 y=0.97)

    fig.text(0.5, 0.018,
             'stacked bars = stage share of per-query time (100%); '
             'top label = total ms/query',
             ha='center', fontsize=9, color=gst.MUTED)
    fig.subplots_adjust(left=0.06, right=0.985, top=0.88, bottom=0.12,
                        hspace=0.28, wspace=0.10)
    fig.savefig(args.out, facecolor='white', bbox_inches='tight', pad_inches=0.3)
    gst.plt.close(fig)
    print('[OK] 图已保存: %s' % args.out)


if __name__ == '__main__':
    main()
