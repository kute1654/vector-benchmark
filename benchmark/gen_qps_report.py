# -*- coding: utf-8 -*-
"""QPS 分析（全数据驱动，仅固定 CSV 源文件名与表头字段）：

数据源（results/）：
    clickhouse-benchmark-results.csv
    myscale-benchmark-results.csv
    pgvector_benchmark_results.csv

图表输出到 results/charts/。

除源文件名与 CSV 表头（读取/分组依据）外，所有内容均从数据推导：
SQL 类型、表名、维度表、profile（按表头布尔列划分缓存口径）、主口径表、
基线/最优配置、英文标签、图例颜色。未写死任何实验数据。
"""
import os, re, math
import numpy as np

BASE = os.path.dirname(os.path.abspath(__file__))
RAW = os.path.join(BASE, 'results')
CHART_DIR = os.path.join(RAW, 'charts')

# ---------- 固定：源文件名 + CSV 表头字段（读取/分组依据） ----------
CK_SRC = 'clickhouse-benchmark-results.csv'
MS_SRC = 'myscale-benchmark-results.csv'
PG_SRC = 'pgvector_benchmark_results.csv'

# plan-cache 相关布尔列（表头固定；engine 值即数据库名）
ENG_COL = {
    'ClickHouse': {'plan': 'vector_query_plan_cache', 'only': 'vector_only_cache_query_plan',
                   'cast': 'vector_use_cast', 'ovec': 'vector_query_plan_cache_only_vector'},
    'MyScale': {'plan': 'enable_query_plan_cache', 'only': 'only_cache_query_plan',
                'cast': 'enable_cast_vector', 'ovec': 'query_plan_cache_only_vector'},
}
# pgvector 主测量口径（固定语义：并行 8、时长 60 秒、1000 条 SQL）
PG_PAR = '8'
PG_DUR = '60'
# ---------- /固定 ----------


def _on(v):
    return v in ('1', '1.0', 'true', 'True')


def dim_label(tname):
    """由表名推导维度标签，如 Benchmark_128_10m -> '128\\n10M rows'。"""
    m = re.match(r'^[^_]+_(\d+)_([\d.]+)([mk]?)', tname)
    if not m:
        return tname
    dim, val, unit = m.group(1), float(m.group(2)), m.group(3)
    if unit == 'm':
        rows = val * 1000000
    elif unit == 'k':
        rows = val * 1000
    else:
        rows = val
    if rows >= 1000000:
        num = '%gM' % round(rows / 1000000, 1)
    elif rows >= 1000:
        num = '%gK' % round(rows / 1000, 1)
    else:
        num = '%d' % rows
    return '%s\n%s rows' % (dim, num)


def _dim_num(tname):
    """由表名取维度数值用于排序，如 Benchmark_1536_1m -> 1536。"""
    m = re.match(r'^[^_]+_(\d+)_', tname)
    return int(m.group(1)) if m else 1 << 30


def dim_short(tname):
    """单行维度标签：Benchmark_1536_1m -> '1536 @ 1M'。"""
    return dim_label(tname).replace('\n rows', '').replace('\n', ' @ ')


def fmt_rows(n):
    """数据量缩写：1000000->'1m', 10000000->'10m', 290000->'290k'。"""
    n = int(n)
    if n >= 1000000:
        return ('%g' % (n / 1000000.0)) + 'm'
    if n >= 1000:
        return ('%g' % (n / 1000.0)) + 'k'
    return str(n)


def load_ckms(fname):
    """读 ClickHouse/MyScale CSV，推导 SQL 类型、表、profile 及其缓存口径。
    数据供 build_ckms(ck_qps_*.png)/build_dim(ck_dim_qps.png)/build_corr(*_corr_256.png) 使用。
    注意：不按 index_status 过滤（该列不可靠），直接取全部行，仅剔除 QPS 为空的行。"""
    rows = [r for r in csv_reader(fname) if r['qps_avg'] not in ('', None, 'None', 'none')]
    sql_types, tables, order = [], [], []
    recs, pflag = {}, {}
    col = ENG_COL['ClickHouse'] if fname == CK_SRC else ENG_COL['MyScale']
    for r in rows:
        t, p, st = r['table_name'], r['settings_profile'], r['sql_type']
        for seq, v in ((sql_types, st), (tables, t), (order, p)):
            if v not in seq:
                seq.append(v)
        recs[(t, p, st)] = float(r['qps_avg'])
        pflag[p] = {'rc': _on(r['use_query_cache']), 'plan': _on(r[col['plan']]),
                    'only': _on(r[col['only']]), 'cast': _on(r[col['cast']]),
                    'ovec': _on(r[col['ovec']])}
    return {'types': sql_types, 'tables': tables, 'order': order,
            'recs': recs, 'pflag': pflag}


def csv_reader(fname):
    import csv
    return list(csv.DictReader(open(os.path.join(RAW, fname), encoding='utf-8-sig')))


def cache_level(f):
    """缓存档位：仅计划(only列=1) < 完整计划缓存(plan列=1) < 无缓存。"""
    return 1 if f['only'] else (2 if f['plan'] else 0)


def core_axes(meta):
    """按缓存口径把 profile 分成 norc/rc 两轴，各按缓存档位升序。
    供 build_ckms/build_corr 分块图分别产出无/有结果缓存两张图。"""
    norc, rc = [], []
    for p in meta['order']:
        f = meta['pflag'][p]
        if f['cast'] or f['ovec']:
            continue
        (rc if f['rc'] else norc).append(p)
    norc.sort(key=lambda p: cache_level(meta['pflag'][p]))
    rc.sort(key=lambda p: cache_level(meta['pflag'][p]))
    return norc, rc


def cn_of(f):
    if f['rc'] and f['only']:
        return 'RC + plan-only'
    if f['rc'] and f['plan']:
        return 'RC + plan cache'
    if f['rc']:
        return 'RC only'
    if f['only']:
        return 'plan-only cache'
    if f['plan']:
        return 'plan cache'
    return 'no cache'


def short_of(f):
    lvl = cache_level(f)
    pre = 'r_' if f['rc'] else ''
    return pre + ('plan' if lvl == 2 else ('only' if lvl == 1 else 'off'))


# 统一配色：(是否用结果缓存[0/1], 缓存档位[0=无,1=缓存only,2=完整计划缓存,3=扩展]) -> 颜色
LEVEL_COLOR = {(0, 0): '#9aa5b1', (0, 1): '#8fb3e8', (0, 2): '#2c5fc4',
               (1, 0): '#f2a65e', (1, 1): '#7fd0cb', (1, 2): '#0ea5a4',
               (0, 3): '#b07aa1', (1, 3): '#d37295'}


def color_of(f):
    return LEVEL_COLOR.get((1 if f['rc'] else 0, cache_level(f) if 'only' in f else f.get('lvl', 0)),
                           '#9aa5b1')


def main_table(meta, profs):
    """主口径表：核心 profile × SQL 类型覆盖最全的表。"""
    best, bn = None, -1
    for t in meta['tables']:
        n = sum(1 for p in profs for st in meta['types'] if (t, p, st) in meta['recs'])
        if n > bn:
            best, bn = t, n
    return best


def _annotate(ax, bars, vals, base_vals, maxv, top_off=0.03, mult_off=0.02,
              qps_color='white', qps_size=5.0, mult_color='#c0392b', mult_size=7.2):
    """统一柱状图标注规则（用于 QPS 所有图）：
      - 只有基准柱显示 QPS：默认纵向写在柱体内部(白字)，柱太短则改到柱顶。
      - 非基准柱不体现 QPS，只在其柱状图顶部标相对基准的倍数。
      - 倍数保留 2 位小数。
      - 一张图统一：基准柱都够高才写柱内，否则全部基准柱改柱顶写 QPS。
      参数含义：base_vals[i]=None 代表该柱为基准柱(显示 QPS)；否则为非基准柱(柱顶只标倍数)。
                hmin 阈值=maxv*0.05，可调；maxv 为该图的 y 轴上限(决定各柱高度)。"""
    hmin = maxv * 0.05                      # 柱高阈值：基准柱低于此高度视为"太短"，改小则矮基准柱也标 QPS
    base_heights = [v for (v, bv) in zip(vals, base_vals) if bv is None and v > 0]
    inside = bool(base_heights) and all(v >= hmin for v in base_heights)   # 基准柱都够高才把 QPS 写柱内(全图统一)
    for (b, v, bv) in zip(bars, vals, base_vals):
        cx = b.get_x() + b.get_width() / 2
        if v <= 0:
            continue
        if bv is None:                      # 基准柱：显示 QPS（柱内或柱顶，均纵向）
            if inside:
                ax.text(cx, v * 0.5, '%.0f' % v, ha='center', va='center',
                        fontsize=qps_size, color=qps_color, rotation=90)
            else:
                ax.text(cx, v + maxv * top_off, '%.0f' % v, ha='center', va='bottom',
                        fontsize=qps_size + 0.8, color=mult_color, fontweight='bold', rotation=90)
        elif bv > 0:                        # 非基准柱：只在柱顶纵向显示相对基准的倍数(保留 2 位)，不显示 QPS
            ax.text(cx, v + maxv * mult_off, 'x%.2f' % (v / bv), ha='center', va='bottom',
                    fontsize=mult_size, color=mult_color, fontweight='bold', rotation=90)


def grouped_chart(ax, data, profs, T, title, meta, base, speed_flag, color, cn, off_key,
                  xlabel_fn=None, legend_loc='upper left', legend_anchor=None,
                  top_margin=None):
    """单个 SQL 类型组内 profile 柱对比图：被 build_corr(256 印证图) 与 pg 256 印证图复用，
    对应输出 *_corr_256.png / pg_corr_256_*.png。"""
    n = len(profs)
    x = np.arange(len(T))
    w = 0.92 / n                              # 柱宽：组内柱子总数越少则单根越宽；0.92 可整体放大/缩小柱宽
    top = max(data.get((p, st), 0.0) for p in profs for st in T) * 1.2   # y 轴上限 = 图中最大 QPS 的 1.2 倍
    for i, p in enumerate(profs):
        f = meta['pflag'].get(p, {})
        vals = [data.get((p, st), 0.0) for st in T]
        bars = ax.bar(x + (i - (n - 1) / 2.0) * w, vals, w * 0.92,   # 柱子之间留 8% 缝宽(0.92)；(i-...)*w 控制组内偏移
                      color=color(p), label=cn(p), zorder=3)
        # 统一标注：基准 profile(第 0 个)显示 QPS，其余 profile 柱顶只标相对基准的倍数(2 位)，均纵向显示
        bv = ([None] * len(T) if i == 0
              else [data.get((base, st), 0.0) for st in T])   # 基准柱(QPS) / 非基准柱(倍数)参照
        _annotate(ax, bars, vals, bv, top)
    ax.set_ylim(0, top * (1 + (top_margin or 0.0)))   # top_margin 为顶部额外留白比例(0~1)，用于放置顶部图例
    ax.margins(x=0.02)                              # 自动按柱边距留 2% 最小左右空白，删除图中两边大空白
    ax.set_xticks(x)
    labels = [xlabel_fn(s) if xlabel_fn else s for s in T]
    ax.set_xticklabels(labels, fontsize=10)
    ax.set_ylabel('QPS (queries/sec)', fontsize=10)
    ax.set_title(title, fontsize=12)
    ax.grid(axis='y', color='#dfe6f0', lw=0.8, zorder=0)
    for s in ('top', 'right'):
        ax.spines[s].set_visible(False)
    ax.legend(fontsize=8, ncol=n, frameon=False, loc='upper right',
              reverse=True, columnspacing=1.0,
              handlelength=1.2, handletextpad=0.5)   # 图例单行靠右、从右到左(reverse=True)展示，各 profile 一次排开


def nice_ceil(x):
    # y 轴刻度档位表：向上取最接近的档位。如需更细/更粗的坐标刻度，可增删这里的数值。
    for s in (100, 200, 250, 500, 1000, 2000, 2500, 5000, 10000, 20000, 50000,
              100000, 200000, 500000, 1000000):
        if x <= s:
            return s
    return int(math.ceil(x / 1000000.0) * 1000000)


# 中文字体候选：按优先级依次探测系统已装字体；改顺序可调整标题/标签实际使用的字体
_FONT_CAND = ['Microsoft YaHei', 'SimHei', 'Noto Sans CJK SC', 'Noto Sans SC',
              'Source Han Sans SC', 'WenQuanYi Zen Hei', 'WenQuanYi Micro Hei',
              'PingFang SC', 'Hiragino Sans GB', 'Droid Sans Fallback']


def _pick_cjk_font():
    from matplotlib import font_manager
    have = {f.name for f in font_manager.fontManager.ttflist}
    for name in _FONT_CAND:
        if name in have:
            return name
    return None


_FONT_SET = [False]   # 字体只初始化一次的开关（勿改）


def _plt():
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    if not _FONT_SET[0]:
        _FONT_SET[0] = True
        font = _pick_cjk_font()
        if font:
            plt.rcParams['font.sans-serif'] = [font]
            plt.rcParams['axes.unicode_minus'] = False
    return plt


# ---------------- ClickHouse / MyScale ----------------
_SQL_SHADE = ['#f4f7fb', '#ffffff']   # 相邻维度块底色(交替使用)：可改颜色或透明度美化分块背景


def _block_chart(ax, blocks, T, profs, pflag, getval, block_label, db, use_rc,
                 colr=color_of, lbl=cn_of, barw=None, xlabel_fn=None,
                 legend_loc='upper left', legend_anchor=None):
    """通用分块图：维度块 × SQL 类型(子簇) × 核心 profile(柱组)。块按输入顺序。
    供 CK/MyScale 与 pg 的分块图使用，对应输出 *_qps_norc/rc.png（由 build_ckms/build_pg 调用并保存）。"""
    pf = pflag
    nd, ns = len(blocks), len(T)
    # ---- 布局参数（效果微调，改后重跑即可） ----
    SPD = 1.7     # SQL 类型/子簇中心距：需 > CW 避免柱重叠；改小则子簇更紧凑
    GAP = 0.12    # 维度块间距：相邻维度块之间的空白(横轴分组间隔)，改小各分组更紧凑
    CW = 1.58     # 柱组占用宽度：决定每组柱子的横占宽度，增大单根柱子整体变宽(须 < SPD 留缝)
    w = (barw if barw else CW / 3.0 * 1)   # 单根柱子实际宽度 = 基准宽/柱数；barw 由调用方传入可整体覆盖
    vals = [getval(b, p, st) for b in blocks for p in profs for st in T]
    data_top = max(vals) * 1.2               # y 轴上限 = 图中最大 QPS 的 1.2 倍
    head = data_top * 0.12                   # 维度标题(块标题)占用的纵向高度(data_top 的比例)
    for d, tab in enumerate(blocks):
        start = d * (ns * SPD + GAP)
        if nd > 1:
            ax.axvspan(start - GAP / 2, start + ns * SPD + GAP / 2,
                       color=_SQL_SHADE[d % 2], zorder=0)
        for s, st in enumerate(T):
            cx = start + s * SPD + SPD / 2
            for i, p in enumerate(profs):
                v = getval(tab, p, st)
                ax.bar(cx + (i - 1.0) * w, v, w, color=colr(pf[p]),
                       label=lbl(pf[p]) if s == 0 and d == 0 else None, zorder=3,
                       edgecolor='none')
        if nd > 1:
            ax.text(start + ns * SPD / 2, data_top + head / 2,
                    block_label(tab), ha='center', va='center',
                    fontsize=9, fontweight='bold', color='#2b3f5c')
        else:
            ax.set_title('%s · %s' % (block_label(tab),
                                      'with result cache' if use_rc else 'no result cache'),
                         fontsize=11, pad=10)
    ax.set_xticks([d * (ns * SPD + GAP) + s * SPD + SPD / 2
                   for d in range(nd) for s in range(ns)])
    labels = [xlabel_fn(st) if xlabel_fn else st for st in T]
    ax.set_xticklabels(labels * nd, fontsize=7.8)   # 横轴(SQL 类型两行名)字号：值越大标签越粗
    for s in ('top', 'right'):
        ax.spines[s].set_visible(False)
    ax.set_ylim(0, data_top + head)
    ax.set_xlim(-0.02, nd * (ns * SPD + GAP) - GAP + 0.02)   # 收紧左右空白：改小则柱更贴边
    ax.set_title('%s · QPS by SQL type across vector dimensions · %s' %
                 (db, 'with result cache' if use_rc else 'no result cache'), fontsize=12)   # 图总标题字号
    ax.set_xlabel('SQL statement type (each block = one vector dimension)',
                  fontsize=9, labelpad=4)
    ax.grid(axis='y', color='#dfe6f0', lw=0.8, zorder=1)
    ax.legend(fontsize=7.5, ncol=len(profs), frameon=False, loc='upper right',
              bbox_to_anchor=(1.0, 0.94), reverse=True)   # 图例单行靠右、从右到左(reverse=True)；0.94 使图例下移避开维度标题
    if nd * ns * len(profs) <= 60:
        # 统一标注：基准柱(本轴最低缓存档位)显示 QPS(纵向写柱内，柱太短改柱顶)，其它柱顶只标相对基准的倍数(2 位)
        hmin = data_top * 0.05                    # 柱高阈值：低于此高度视为"太短"
        basep = profs[0]                      # 基准 profile = 排序后第一档(无缓存/最低缓存档位)
        for d, tab in enumerate(blocks):
            start = d * (ns * SPD + GAP)
            for s, st in enumerate(T):
                cx = start + s * SPD + SPD / 2
                bo = getval(tab, basep, st)   # 基准 QPS：非基准柱的倍数参照
                for i, p in enumerate(profs):
                    v = getval(tab, p, st)
                    ox = cx + (i - 1.0) * w
                    if p == basep and v > 0:  # 基准柱：显示 QPS(纵向)
                        if v >= hmin:
                            ax.text(ox, v * 0.5, '%.0f' % v, ha='center', va='center',
                                    fontsize=4.8, color='white', rotation=90)   # 纵向写柱内
                        else:
                            ax.text(ox, v + data_top * 0.03, '%.0f' % v, ha='center', va='bottom',
                                    fontsize=5.2, color='#c0392b', fontweight='bold', rotation=90)
                    elif p != basep and bo > 0 and v > 0:   # 非基准柱：只在柱顶纵向显示相对基准的倍数(2 位小数)
                        ax.text(ox, v + data_top * 0.02, 'x%.2f' % (v / bo), ha='center', va='bottom',
                                fontsize=5.0, color='#c0392b', fontweight='bold', rotation=90)


def ckms_chart(ax, meta, profs, db, use_rc):
    """单张轴：维度(块) × SQL 类型(子簇) × 核心 profile(3 组柱)，维度按大小升序。
    供 build_ckms 调用，对应输出 *_qps_norc/rc.png。"""
    tabs = sorted(meta['tables'], key=_dim_num)
    return _block_chart(ax, tabs, meta['types'], profs, meta['pflag'],
                        lambda tb, p, st: meta['recs'].get((tb, p, st), 0.0),
                        dim_short, db, use_rc)


def build_ckms(meta, db):
    """每个库 2 张图：no/with result cache，各自在单轴中呈现 维度×SQL类型×3 profile。
    输出：ck_qps_norc.png / ck_qps_rc.png（或 myscale_*），并转调 build_corr 输出 *_corr_256.png。"""
    plt = _plt()
    norc, rc = core_axes(meta)
    nd, ns = len(meta['tables']), len(meta['types'])
    extent = nd * (ns * 1.3 + 0.2)   # 估算整图横向跨度：ns*1.3(与上方 SPD 同步)+0.2(与 GAP 同步)，仅用于定画布宽度
    for tag, profs, use_rc in (('norc', norc, False), ('rc', rc, True)):
        fig, ax = plt.subplots(figsize=(max(9.0, extent * 0.62), 5.0), dpi=200)   # 0.62=横向缩放系数，5.0=图高
        ckms_chart(ax, meta, profs, db, use_rc)
        fname = ('ck' if db == 'ClickHouse' else 'myscale') + '_qps_%s.png' % tag
        fig.savefig(os.path.join(CHART_DIR, fname), bbox_inches='tight')
        plt.close(fig)
    corr = build_corr(meta, db)
    return ['%s_qps_%s.png' % ('ck' if db == 'ClickHouse' else 'myscale', t) for t in ('norc', 'rc')] + [corr]


def build_corr(meta, db):
    """256 维度数据集单块图：用于印证耗时(step-time)分析，采用无结果缓存核心 profile。
    输出：ck_corr_256.png（或 myscale_corr_256.png）。"""
    plt = _plt()
    norc, _ = core_axes(meta)
    tabs = sorted(meta['tables'], key=_dim_num)
    tab = next((t for t in tabs if _dim_num(t) == 256), tabs[0])
    T = meta['types']
    data = {(p, st): meta['recs'].get((tab, p, st), 0.0) for p in norc for st in T}
    base = norc[0]
    fig, ax = plt.subplots(figsize=(max(8.0, 1.35 * len(T)), 4.6), dpi=200)
    grouped_chart(ax, data, norc, T,
                  '%s · %s dimension QPS (no result cache) · vs step-time' % (db, _dim_num(tab)),
                  meta, base, False,
                  lambda c: color_of(meta['pflag'][c]),
                  lambda c: cn_of(meta['pflag'][c]), 'off')
    fig.tight_layout()
    fname = ('ck' if db == 'ClickHouse' else 'myscale') + '_corr_256.png'
    fig.savefig(os.path.join(CHART_DIR, fname), bbox_inches='tight')
    plt.close(fig)
    return fname


def build_dim(meta, db):
    """每个库单独的维度图：该库实际存在的全部表，off vs 最优结果缓存配置。
    输出：ck_dim_qps.png（或 myscale_dim_qps.png）。"""
    plt = _plt()
    norc, rc = core_axes(meta)
    off = norc[0]   # 基准状态 = 不用结果缓存 + 最低缓存档位（一般是无缓存）
    tabs = sorted(meta['tables'], key=_dim_num)   # 维度按大小升序，从左(小)到右(大)排列
    labels = [dim_label(t) for t in tabs]
    pf = meta['pflag']
    # 统一顺序：先用 norc(不用结果缓存，按缓存档位升序，首位即无缓存=基准)，再 rc；
    # 使基准柱固定在最左，且 ck / myscale 的 profile 排列逻辑一致。
    profs = norc + rc
    n = len(profs)
    x = np.arange(len(tabs))
    bw = 0.9 / n   # 单柱槽宽：n 柱一组，改大柱子更宽
    allv = {p: [sum(meta['recs'][(t, p, st)] for st in meta['types'] if (t, p, st) in meta['recs']) / len(meta['types']) for t in tabs]
            for p in profs}
    dtop = max(max(v) for v in allv.values()) * 1.2   # y 轴上限 = 图中最大 QPS 的 1.2 倍
    fig, ax = plt.subplots(figsize=(max(7.0, 2.3 * len(tabs)), 4.8), dpi=200)   # 2.3=每表横向占宽，4.8=图高
    for i, p in enumerate(profs):
        vals = allv[p]
        bs = ax.bar(x + (i - (n - 1) / 2) * bw, vals, bw * 0.9,
                    color=color_of(pf[p]), label=cn_of(pf[p]), zorder=3)
        # 统一标注：基准(off)柱显示 QPS，其余 profile 柱顶只标相对基准的倍数(2 位)，均纵向
        _annotate(ax, bs, vals, [None] * len(tabs) if p == off else allv[off], dtop)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=8.2)
    ax.set_title('%s vector dimensions · %d profiles · QPS' % (db, n), fontsize=12)
    ax.grid(axis='y', color='#dfe6f0', lw=0.8, zorder=0)
    for s in ('top', 'right'):
        ax.spines[s].set_visible(False)
    ax.set_ylabel('QPS (queries/sec)', fontsize=10)
    ax.legend(fontsize=7.0, ncol=n, frameon=False, loc='upper right',
              reverse=True)   # 图例单行靠右、从右到左(reverse=True)
    ax.set_ylim(0, dtop)
    ax.margins(x=0.02)   # 删除图中两边大空白：留 2% 最小左右留白
    fig.tight_layout()
    fname = ('ck' if db == 'ClickHouse' else 'myscale') + '_dim_qps.png'
    fig.savefig(os.path.join(CHART_DIR, fname), bbox_inches='tight')
    plt.close(fig)
    return fname


# ---------------- pgvector ----------------
PG5 = ['text_literal', 'array_int_cast', 'array_real_cast', 'halfvec_literal',
       'with_array_cast']   # pg 分块图/印证图限用的 5 类 SQL 类型（增删即改参与绘图的类型）


def two_line(name):
    """SQL 类型名两行显示：array_int_cast -> 'array int'/'cast'。"""
    parts = name.split('_')
    mid = (len(parts) + 1) // 2
    return '\n'.join([' '.join(parts[:mid]), ' '.join(parts[mid:])])


def _pg_group(flat, pdata=None, pmodes=None):
    """由 (dataset,缓存组合,sql_type)->rps 推导 pg 某一召回率组的全部结构。
    pdata/pmodes 额外携带 vector_in parse 维度，用于 parse 收益平均。"""
    types, ds, codes = [], [], []
    def app(seq, v):
        if v not in seq:
            seq.append(v)
    for (d, c, st) in flat:
        app(ds, d); app(codes, c); app(types, st)
    def key(c):
        return (0 if c[0] != '1' else 1, int(c[1]))
    codes.sort(key=key)
    norc = [c for c in codes if c[0] != '1']
    rc = [c for c in codes if c[0] == '1']
    pf = {c: {'rc': c[0] == '1', 'plan': c[1] not in ('0',), 'only': False,
              'lvl': int(c[1])} for c in codes}
    dim = {(d, st, c): v for (d, c, st), v in flat.items()}
    main_ds = max(ds, key=lambda d: sum(1 for k in flat if k[0] == d))
    # 三种 vector_in parse 模式的收益平均：对每种 parse，先把每个 (dataset, 非 use_cache=2 缓存组合)
    # 内全部 SQL 类型 QPS 平均得到一个"状态"，共 5 数据集×6 组合≈30 个有效状态（原 5×8=40 去掉 cache=2 后），再取平均。
    pstat = {}
    if pdata:
        combos_no2 = [c for c in codes if c[1] != '2']   # 去掉 vector_in 执行 11 次、权重失衡的 use_cache=2 状态
        for pm in (pmodes or []):
            states = []
            for d in ds:
                for c in combos_no2:
                    vals = [v for (k, v) in pdata.items()
                            if k[0] == d and k[2] == c and k[3] == pm and k[1] in types and v > 0]
                    if len(vals) >= 2:   # 至少 2 类 SQL 才纳入平均，避免单点噪声
                        states.append(sum(vals) / len(vals))
            pstat[pm] = (sum(states) / len(states)) if states else 0.0
    return {'types': types, 'ds': ds, 'codes': codes, 'norc': norc, 'rc': rc,
            'pf': pf, 'dim': dim, 'main_ds': main_ds,
            'off': ('0', '0'),
            'best': max((c for c in codes if c[0] == '1'), key=lambda c: key(c), default=None),
            'pstat': pstat, 'pmodes': pmodes or [], 'pdata': pdata or {}}


def load_pg():
    """读 pgvector CSV，按召回率拆两组（recall=1 单条 SQL / recall!=1 多语句），
    每组保留各 key 最后一条稳定记录。数据供 build_pg（pg_qps_*.png 等）使用。
    每组按缓存组合推导 profile，按主口径(par=8/dur=60)取最后连续数据。"""
    rows = [r for r in csv_reader(PG_SRC) if r['experiment_name'].startswith('pgvector-')]
    groups = {True: {}, False: {}}
    pdata = {True: {}, False: {}}   # 按 (dataset, sql_type, 缓存组合, parse模式) 收集，用于三种 vector_in parse 收益平均
    pmodes = {True: [], False: []}  # 每组出现的 parse 模式（strtof/inline/fast_float）
    vdim = {}
    for r in rows:
        if r['parallel'] == PG_PAR and r['test_duration'] == PG_DUR and r['rps'] not in ('', None):
            try:
                recall1 = abs(float(r['mean_precisions']) - 1.0) < 1e-9
            except (ValueError, KeyError):
                continue
            c = (r['use_query_cache'], r['use_cache'])
            pm = r.get('vector_in_parse_mode', '')
            try:
                rps = float(r['rps'])
            except ValueError:
                continue
            groups[recall1][((r['dataset'], r['rows']), c, r['sql_type'])] = rps
            if pm:
                pdata[recall1][((r['dataset'], r['rows']), r['sql_type'], c, pm)] = rps
                if pm not in pmodes[recall1]:
                    pmodes[recall1].append(pm)
            try:
                vdim[(r['dataset'], r['rows'])] = int(r['vector_size'])
            except (ValueError, KeyError):
                pass
    # 仅保留含结果缓存(use_query_cache=1)剖面的组，缺 rc 的组（如 1000 条 QPS 未跑完）跳过不生成
    return {'groups': {rl: _pg_group(flat, pdata[rl], pmodes[rl]) for rl, flat in groups.items()
                       if flat and any(k[1][0] == '1' for k in flat)},
            'vdim': vdim}


def build_pg(pg):
    """pg 全量图：按召回率两组(recall=1 / recall<1)分别输出。
    每组输出：pg_qps_norc/r1.png·pg_qps_rc/r1.png（分块）、pg_dim_qps/r1.png（维度）、
              pg_corr_256/r1.png（256 印证）；recall<1 组文件名后缀为 rn1。"""
    plt = _plt()
    vdim = pg['vdim']
    for rl, g in pg['groups'].items():
        rtag = 'r1' if rl else 'rn1'
        rname = '1 SQL case' if rl else '1000 SQL case'
        pf = g['pf']
        T = [st for st in PG5 if st in g['types']]
        if not T:
            T = g['types']
        blocks = sorted(g['ds'], key=lambda d: vdim.get(d, 0))
        def getval(d, p, st):
            return g['dim'].get((d, st, p), 0.0)
        def blabel(d):
            name = d[0].replace('pgvector-', '').replace('-', ' ')
            return '%s  (%s)' % (name, fmt_rows(d[1]))
        def col(f):
            return LEVEL_COLOR.get((1 if f['rc'] else 0, f['lvl']), '#9aa5b1')
        cc = {'pflag': pf}
        # 分块图：数据集(块，低维→高维) × SQL 类型(子簇，两行) × 3 profile（柱宽为默认 0.75 倍）
        nd, ns = len(blocks), len(T)
        extent = nd * (ns * 1.3 + 0.2)   # 与 _block_chart 的 SPD(1.3)/GAP(0.2) 同步，仅用于定画布宽度
        for tag, profs, use_rc in (('norc', g['norc'], False), ('rc', g['rc'], True)):
            fig, ax = plt.subplots(figsize=(max(9.0, extent * 0.62), 5.0), dpi=200)   # 0.62=横向缩放系数，5.0=图高
            _block_chart(ax, blocks, T, profs, pf, getval, blabel,
                         'pgvector · %s' % rname, use_rc,
                         col, cn_of_pg,
                         barw=0.3,            # 柱宽系数：覆盖默认 CW/3=0.4；0.3≈原 0.2 × 1.5 倍，改大柱子更宽
                         xlabel_fn=two_line,
                         legend_loc='upper left',
                         legend_anchor=(0.0, 0.93))   # 图例纵向位置：0.93≈从顶部下移一点(1.0=紧贴顶)，改小则更靠下
            fig.savefig(os.path.join(CHART_DIR, 'pg_qps_%s_%s.png' % (tag, rtag)),
                        bbox_inches='tight')
            plt.close(fig)
        # 维度图：基于 text_literal 绘制 8 种缓存状态的 QPS（每数据集 8 柱），并标注相对基准的倍数
        off = g['off']   # 基准状态 = (结果缓存=0, 计划缓存=0)
        PP = [('0', '0'), ('0', '1'), ('0', '2'), ('0', '3'),
              ('1', '0'), ('1', '1'), ('1', '2'), ('1', '3')]   # 8 种状态=(use_query_cache结果缓存, use_cache计划缓存档位)
        x = np.arange(len(blocks))
        nst = len(PP)
        bw = 0.88 / nst                                    # 单柱宽：8 柱一组，改大柱子更宽
        fig, ax = plt.subplots(figsize=(max(8.0, 2.3 * len(blocks)), 4.8), dpi=200)   # 2.3=每数据集横向占宽，4.8=图高
        ov = [g['dim'].get((d, 'text_literal', off), 0.0) or 0.0 for d in blocks]     # 基准(0,0)各数据集 QPS
        dtop = max(max([g['dim'].get((d, 'text_literal', c), 0.0) for d in blocks]
                       for c in PP), default=0.0) * 1.2   # y 轴上限 = 图中最大 QPS 的 1.2 倍
        for i, c in enumerate(PP):
            vals = [g['dim'].get((d, 'text_literal', c), 0.0) for d in blocks]
            bs = ax.bar(x + (i - (nst - 1) / 2) * bw, vals, bw * 0.9,
                        color=LEVEL_COLOR.get((int(c[0]), int(c[1])), '#9aa5b1'),
                        label=cn_of_pg(pf[c]), zorder=3)   # 8 个状态柱(s)色=LEVEL_COLOR 统一配色
            # 统一标注：QPS 纵向写在柱内；基准(0,0)柱柱偏矮时柱顶写 QPS，其它状态柱顶标相对基准的倍数(3 位小数)
            _annotate(ax, bs, vals, [None] * len(blocks) if c == off else ov, dtop)
        ax.set_ylim(0, dtop)
        ax.margins(x=0.02)   # 删除图中两边大空白：留 2% 最小左右留白
        ax.set_xticks(x)
        ax.set_xticklabels(['%s (%s)' % (d[0].replace('pgvector-', '').replace('-', ' '),
                                         fmt_rows(d[1])) for d in blocks],
                           fontsize=7)
        ax.set_ylabel('QPS (queries/sec)', fontsize=10)
        ax.set_title('pgvector · text_literal · 8 cache states · %s' % rname, fontsize=12)
        ax.grid(axis='y', color='#dfe6f0', lw=0.8, zorder=0)
        for s in ('top', 'right'):
            ax.spines[s].set_visible(False)
        ax.legend(fontsize=6.2, frameon=False, loc='upper right', ncol=nst,
                  reverse=True)   # 图例单行靠右、从右到左(reverse=True)，8 个缓存状态一次排开
        fig.tight_layout()
        fig.savefig(os.path.join(CHART_DIR, 'pg_dim_qps_%s.png' % rtag), bbox_inches='tight')
        plt.close(fig)
        # 256 维度（nytimes）印证耗时图，仅 5 类 SQL 类型；含计划缓存与结果缓存全部剖面
        d256 = next((d for d in blocks if '256' in d[0]), blocks[0])
        profs = g['codes']
        data = {(p, st): g['dim'].get((d256, st, p), 0.0) for p in profs for st in T}
        fig, ax = plt.subplots(figsize=(max(8.0, 1.35 * len(T)), 4.6), dpi=200)
        grouped_chart(ax, data, profs, T,
                      'pgvector %s · %s · QPS · vs step-time' % (d256[0], rname),
                      cc, off, False, lambda c: col(pf[c]), lambda c: cn_of_pg(pf[c]), 'off',
                      xlabel_fn=two_line, legend_loc='upper left', legend_anchor=(0.0, 1))
        fig.tight_layout()
        fig.savefig(os.path.join(CHART_DIR, 'pg_corr_256_%s.png' % rtag), bbox_inches='tight')
        plt.close(fig)
    build_pg_parse(pg)   # 额外输出三种 vector_in parse 模式的收益对比图


def _parse_avg(pdata, ds, sql_types, useqc, usec_set, pms):
    """对某结果缓存口径(useqc)下、指定计划缓存档位(usec_set)、按 (dataset, parse) 在全部 SQL 类型上取平均。
    usec_set 传 {'0','1','3'} 为剔除 cache=2 的正常状态；传 {'2'} 专用于 cache=2 单独图。"""
    res = {}
    for d in ds:
        for pm in pms:
            vals = [v for (k, v) in pdata.items()
                    if k[0] == d and k[1] in sql_types
                    and (useqc is None or k[2][0] == useqc)   # None=不区分结果缓存(合并 norc/rc)，如 cache=2 单独图
                    and k[2][1] in usec_set and k[3] == pm and v > 0]
            res[(d, pm)] = (sum(vals) / len(vals)) if vals else 0.0
    return res


def build_pg_parse(pg):
    """三种 vector_in parse(strtof/inline/fast_float)的维度收益对比，以 strtof 为基准：
      - pg_parse_qps_norc_*.png ：不使用查询结果缓存(use_query_cache=0)，剔除 cache=2
      - pg_parse_qps_rc_*.png   ：使用查询结果缓存(use_query_cache=1)，剔除 cache=2
      - pg_parse_qps_cache2_*.png：use_cache=2 单独一张，对比无结果缓存 vs 有结果缓存
    * 为 r1/rn1。X 轴均为 5 个维度数据集。"""
    plt = _plt()
    palette = {'strtof': '#9aa5b1', 'inline': '#7fd0cb', 'fast_float': '#0ea5a4'}   # 三种 parse 配色
    order_def = ('strtof', 'inline', 'fast_float')
    for rl, g in pg['groups'].items():
        rtag = 'r1' if rl else 'rn1'
        kind = '1 SQL case' if rl else '1000 SQL case'
        pdata = g.get('pdata') or {}
        pms = [p for p in order_def if any(k[3] == p for k in pdata)]   # 实际出现的 parse 模式
        if len(pms) < 2:
            continue
        dims = sorted(g['ds'], key=lambda d: pg['vdim'].get(d, 0))   # 按向量维度升序
        sql_types = g['types']
        slabs = ['%s (%s)' % (d[0].replace('pgvector-', '').replace('-', ' '),
                              fmt_rows(d[1])) for d in dims]
        xs = np.arange(len(dims))
        def _style(ax):
            ax.grid(axis='y', color='#dfe6f0', lw=0.8, zorder=0)
            for s in ('top', 'right'):
                ax.spines[s].set_visible(False)
        def _draw_dims(res, title, fname):
            # 每维度 3 柱(parse)，strtof 为基准；统一标注规则：QPS 纵写柱内，其它 parse 柱顶标相对 strtof 的倍数(3 位)
            fig, ax = plt.subplots(figsize=(max(8.0, 2.0 * len(dims)), 4.6), dpi=200)
            width = 0.227   # 组内每根柱的槽宽：前版为 0.34，现缩到其 2/3≈0.227，3 柱更细更紧凑
            maxv = max((res[(d, pm)] for d in dims for pm in pms), default=0.0) or 1.0
            strtof_vals = [res[(d, 'strtof')] for d in dims]   # 基准(strtof)各维度 QPS，供倍数参照
            for j, pm in enumerate(pms):
                vals = [res[(d, pm)] for d in dims]
                bars = ax.bar(xs + (j - 1) * width, vals, width * 0.9,
                              color=palette.get(pm, '#9aa5b1'), label=pm, zorder=3)
                bv = [None] * len(dims) if pm == 'strtof' else strtof_vals   # 基准柱不标倍数，其余柱顶标倍数
                _annotate(ax, bars, vals, bv, maxv)
            ax.set_ylim(0, maxv * 1.2)   # y 轴上限 = 图中最大 QPS 的 1.2 倍
            ax.margins(x=0.02)   # 删除图中两边大空白：留 2% 最小左右留白
            ax.set_xticks(xs); ax.set_xticklabels(slabs, fontsize=7)
            ax.set_ylabel('avg QPS over sql types', fontsize=10)
            ax.set_title(title, fontsize=12)
            ax.legend(fontsize=8, ncol=3, frameon=False, loc='upper right',
                      reverse=True)   # 图例单行靠右、从右到左(reverse=True)，各 parse 方式一次排开
            _style(ax); fig.tight_layout()
            fig.savefig(os.path.join(CHART_DIR, fname), bbox_inches='tight'); plt.close(fig)
        # 图 A：不使用查询结果缓存（剔除 cache=2）
        r0 = _parse_avg(pdata, dims, sql_types, '0', {'0', '1', '3'}, pms)
        _draw_dims(r0, 'pgvector · parse mode · no result cache · %s' % kind,
                   'pg_parse_qps_norc_%s.png' % rtag)
        # 图 B：使用查询结果缓存（剔除 cache=2）
        r1 = _parse_avg(pdata, dims, sql_types, '1', {'0', '1', '3'}, pms)
        _draw_dims(r1, 'pgvector · parse mode · with result cache · %s' % kind,
                   'pg_parse_qps_rc_%s.png' % rtag)
        # 图 C：cache=2 单独处理（因三种 parse 转换方式均执行了 11 次、权重特殊故单独成图），
        # 且按「不使用/使用查询结果缓存」分开成两张图（处理方式与图 A/B 相同：5 维度 × 3 parse，
        # 对 5 种 SQL 平均、以 strtof 为基准）。r1/rn1 各 2 张，共 4 张。
        r2_norc = _parse_avg(pdata, dims, sql_types, '0', {'2'}, pms)
        _draw_dims(r2_norc, 'pgvector · parse mode · use_cache=2 · no result cache · %s' % kind,
                   'pg_parse_qps_cache2_norc_%s.png' % rtag)
        r2_rc = _parse_avg(pdata, dims, sql_types, '1', {'2'}, pms)
        _draw_dims(r2_rc, 'pgvector · parse mode · use_cache=2 · with result cache · %s' % kind,
                   'pg_parse_qps_cache2_rc_%s.png' % rtag)


def cn_of_pg(f):
    """pg 的 profile 中文标签：无缓存/计划缓存/结果缓存 档位（供 pg 各图图例显示）。"""
    if f['rc'] and f['plan']:
        return 'RC · lv%s' % f['lvl']
    if f['rc']:
        return 'RC only'
    if f['plan']:
        return 'plan cache lv%s' % f['lvl']
    return 'no cache'


# ---------------- main ----------------
def main():
    """总入口：读取三类 CSV 并生成全部 QPS 图"""
    os.makedirs(CHART_DIR, exist_ok=True)
    print('load clickhouse...'); ck = load_ckms(CK_SRC)
    print('load myscale...');  ms = load_ckms(MS_SRC)
    print('load pgvector...'); pg = load_pg()

    build_ckms(ck, 'ClickHouse')
    build_dim(ck, 'ClickHouse')
    build_ckms(ms, 'MyScale')
    build_dim(ms, 'MyScale')
    build_pg(pg)
    print('charts saved to', CHART_DIR)


if __name__ == '__main__':
    main()