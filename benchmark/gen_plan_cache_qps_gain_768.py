# -*- coding: utf-8 -*-
"""768 维查询计划缓存 QPS 收益图：四个数据库放在同一张图中。

数据来源与 gen_qps_report.py / gen_qps_4db.py 完全一致——基准结果 CSV（并发 8）：
  clickhouse-benchmark-results.csv      ClickHouse（settings_profile 三档）
  myscale-benchmark-results.csv         MyScale（settings_profile 三档）
  pgvector-benchmark-results.csv        pgvector（plan_mode 四档，n=1 / n=1000 各一子图）

聚合口径与 gen_qps_report.py 相同（直接复用其加载函数）：一行 CSV 视为一轮
运行，先对 run_1..run_N 求均值，再对各轮均值取算术平均。

每个数据库一个子图，横轴为 sql_type，组内每根柱对应一种缓存档位：
  * 柱高 = QPS(该档位) / QPS(无计划缓存基准)，基准柱恒为 ×1.00；
  * 柱顶竖排标注收益倍数，柱体中竖排标注该档位的平均 QPS 绝对值；
  * 柱底短横条用缓存档位颜色标识，图例在面板下方。

用法：
  python gen_plan_cache_qps_gain_768.py
  python gen_plan_cache_qps_gain_768.py --out results/charts/xxx.png
"""

import argparse
import os
import sys

try:
    import gen_qps_report as gqr
    import gen_step_time_report as gst
except ImportError:          # 支持 python -m benchmark.gen_plan_cache_qps_gain_768
    from benchmark import gen_qps_report as gqr
    from benchmark import gen_step_time_report as gst

OUT_NAME = 'plan_cache_qps_gain_768_4db.png'
DIM_KEY = '768'
CONC = gqr.DEFAULT_CONCURRENCY


def pick_dim(tables):
    """取结果 CSV 里的 768 维表名；没有则返回 None。"""
    for t in tables:
        if DIM_KEY in t:
            return t
    return None


def build_ckms_panel(name, src):
    """CK/MyScale 面板：settings_profile 档位（排除结果缓存/CAST/only-vector 等），
    基准 = 无计划缓存档（cache_level=0）。返回 None 表示无 768 维数据。"""
    meta = gqr.load_ckms(src, CONC)
    dim = pick_dim(meta['tables'])
    if dim is None:
        return None
    pflag = meta['pflag']
    modes = [p for p in meta['order']
             if not pflag[p]['rc'] and not pflag[p]['cast'] and not pflag[p]['ovec']]
    modes.sort(key=lambda p: gqr.cache_level(pflag[p]))
    base = next((p for p in modes if gqr.cache_level(pflag[p]) == 0), modes[0])
    types = [st for st in meta['types'] if (dim, base, st) in meta['recs']]
    return {'name': name, 'dim': dim, 'modes': modes, 'base': base, 'types': types,
            'recs': meta['recs'], 'rounds': meta.get('rounds'),
            'colf': lambda m: gqr.color_of(pflag[m]),
            'labf': lambda m: gqr.cn_of(pflag[m]),
            'base_name': 'no plan cache'}


def build_pg_panel(name, row_count):
    """pgvector 面板：plan_mode 档位（raw/prepare_auto/prepare_generic/prepare_custom），
    基准 = raw。row_count 为 '1' / '1000'。返回 None 表示无该规格数据。"""
    try:
        pg = gqr.load_pg_loop(CONC, row_count)
    except SystemExit as e:
        print('[warn] %s' % e, file=sys.stderr)
        return None
    dim = pick_dim(pg['tables'])
    if dim is None:
        return None
    modes = [m for m in gqr.PG_CACHE_MODES if m in pg['modes']] + \
            [m for m in pg['modes'] if m not in gqr.PG_CACHE_MODES]
    base = 'raw' if 'raw' in modes else modes[0]
    types = [st for st in pg['types'] if (dim, base, st) in pg['recs']]
    return {'name': name, 'dim': dim, 'modes': modes, 'base': base, 'types': types,
            'recs': pg['recs'], 'rounds': pg.get('rounds'),
            'colf': lambda m: gqr.PG_MODE_COLOR.get(m, '#9aa5b1'),
            'labf': lambda m: gqr.PG_MODE_LABEL.get(m, m),
            'base_name': 'no plan cache'}


def draw_gain_panel(ax, panel, show_ylabel):
    """画一个数据库的 QPS 收益面板：柱高 = 相对基准的倍数，柱体内标注平均 QPS。"""
    if not panel or not panel['types'] or not panel['modes']:
        ax.set_axis_off()
        ax.text(0.5, 0.55, panel['name'] if panel else '', transform=ax.transAxes,
                ha='center', va='center', fontsize=12, color=gst.INK, fontweight='bold')
        ax.text(0.5, 0.44, panel.get('note', 'no 768-dim data') if panel else 'no 768-dim data',
                transform=ax.transAxes, ha='center', va='center',
                fontsize=9, color=gst.MUTED, wrap=True)
        return

    recs, dim, modes, base = panel['recs'], panel['dim'], panel['modes'], panel['base']
    types = panel['types']
    nc = len(modes)
    w = 0.82 / nc
    maxg = 1.0

    for i, st in enumerate(types):
        qb = recs.get((dim, base, st), 0.0)
        if qb <= 0:
            continue
        for j, m in enumerate(modes):
            q = recs.get((dim, m, st), 0.0)
            if q <= 0:
                continue
            gain = q / qb
            maxg = max(maxg, gain)
            xi = i + (j - (nc - 1) / 2.0) * w
            color = panel['colf'](m)
            ax.bar(xi, gain, width=w * 0.92, color=color,
                   edgecolor='white', linewidth=0.35, zorder=3)
            # 柱体中标注平均 QPS 绝对值（竖排白字）
            ax.text(xi, gain / 2.0, '%.0f' % q, ha='center', va='center',
                    fontsize=5.2, color='white', rotation=90, zorder=5)
            # 柱顶标注收益倍数（竖排红粗）
            ax.text(xi, gain + 0.035, '×%.2f' % gain, ha='center', va='bottom',
                    fontsize=6.0, color='#c0392b', fontweight='bold',
                    rotation=90, zorder=5)
            ax.plot([xi - w * 0.40, xi + w * 0.40], [-8.0, -8.0],
                    color=color, lw=2.6, solid_capstyle='butt',
                    clip_on=False, zorder=5)

    for i in range(0, len(types), 2):
        ax.axvspan(i - 0.5, i + 0.5, color='#2f6fed', alpha=0.045, zorder=0)
    ax.axhline(1.0, color=gst.MUTED, lw=0.9, ls=(0, (4, 3)), zorder=2)
    ax.set_xlim(-0.5, len(types) - 0.5)
    ax.set_ylim(0, maxg * 1.30)
    ax.set_xticks(range(len(types)))
    ax.set_xticklabels(types, fontsize=8.5, color=gst.INK)
    if show_ylabel:
        ax.set_ylabel('QPS gain vs no plan cache (×)', fontsize=9, color=gst.MUTED)
    ax.tick_params(axis='y', labelsize=8, colors=gst.MUTED)
    ax.grid(axis='y', color=gst.RULE, linewidth=0.7, zorder=0)
    ax.set_axisbelow(True)
    for sp in ('top', 'right'):
        ax.spines[sp].set_visible(False)
    for sp in ('left', 'bottom'):
        ax.spines[sp].set_color(gst.RULE)

    # 面板名放 axes 外上方（紧贴）
    ax.text(0.0, 1.01, '%s · %s' % (panel['name'], panel['dim']),
            transform=ax.transAxes, ha='left', va='bottom', fontsize=10.5,
            color=gst.INK, fontweight='bold')
    # 面板自己的缓存档位图例（轴外下方）
    handles = [gst.Patch(facecolor=panel['colf'](m), label=panel['labf'](m))
               for m in modes]
    if handles:
        ax.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, -0.16),
                  ncol=nc, fontsize=6.5, frameon=False, columnspacing=1.2,
                  handlelength=1.0, handleheight=0.7)


def parse_args():
    ap = argparse.ArgumentParser(
        description='768 维：查询计划缓存开/关的 QPS 收益倍数（四库合一图，基准结果 CSV）')
    ap.add_argument('--out', default=os.path.join(gst.CHART_DIR, OUT_NAME),
                    help='输出 PNG 路径（默认 results/charts/%s）' % OUT_NAME)
    return ap.parse_args()


def main():
    args = parse_args()
    panels = [
        build_ckms_panel('ClickHouse', gqr.CK_SRC),
        build_ckms_panel('MyScale', gqr.MS_SRC),
        build_pg_panel('pgvector (n=1000)', '1000'),
        build_pg_panel('pgvector (n=1)', '1'),
    ]
    if not any(panels):
        raise SystemExit('[ERR] 结果 CSV 中没有 768 维数据，无法画图')
    if not gst.load_mpl():
        raise SystemExit('[ERR] 生成图表需要 matplotlib')

    for p in panels:
        if not p or not p['types']:
            continue
        for st in p['types']:
            qb = p['recs'].get((p['dim'], p['base'], st), 0.0)
            if qb <= 0:
                continue
            items = []
            for m in p['modes']:
                key = (p['dim'], m, st)
                q = p['recs'].get(key, 0.0)
                if q > 0:
                    items.append('%s=%.0f(×%.2f)' % (m, q, q / qb))
            print('(info) %s %s: %s' % (p['name'], st, ', '.join(items)))

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    fig, axes = gst.plt.subplots(2, 2, figsize=(19.0, 10.0), dpi=170,
                                 sharex=False, sharey=False)
    for k, (ax, panel) in enumerate(zip(axes.flat, panels)):
        draw_gain_panel(ax, panel, show_ylabel=(k % 2 == 0))
    fig.suptitle('768-dim QPS gain: query plan cache ON vs OFF '
                 '(concurrency=%s, benchmark results)' % CONC,
                 fontsize=13.5, color=gst.INK, y=0.97)
    fig.text(0.5, 0.018,
             'bar height = QPS(profile) / QPS(no plan cache), baseline = ×1.00; '
             'number inside bar = mean QPS (avg over runs)',
             ha='center', fontsize=9, color=gst.MUTED)
    fig.subplots_adjust(left=0.06, right=0.985, top=0.88, bottom=0.12,
                        hspace=0.28, wspace=0.10)
    fig.savefig(args.out, facecolor='white', bbox_inches='tight', pad_inches=0.3)
    gst.plt.close(fig)
    print('[OK] 图已保存: %s' % args.out)


if __name__ == '__main__':
    main()
