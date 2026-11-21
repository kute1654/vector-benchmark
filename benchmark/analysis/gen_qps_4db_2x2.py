"""把四张 QPS 对比 EPS 合成一张 2×2 总图。

左列：ClickHouse（上）/ MyScale（下）；右列：PG (n=1000)（上）/ PG (n=1)（下）。
输入（都在 results/charts/ 下，需用 gen_qps_report.py --no-title 生成）:
  ck_qps_c8.eps
  myscale_qps_c8.eps
  pg_qps_c8_n1000.eps
  pg_qps_c8_n1.eps

输出:
  results/charts/qps_4db_2x2.eps

本脚本只做图像拼接（读取 EPS 面板的 PNG 缓存），不重跑任何数据生成逻辑。
"""
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from PIL import Image

try:
    from chart_export import panel_png, save_chart, trim_white
except ImportError:  # support `python -m benchmark.<script>`
    from benchmark.chart_export import panel_png, save_chart, trim_white

CHART_DIR = os.path.join(os.path.dirname(__file__), '../results', 'charts')
OUT_PATH = os.path.join(CHART_DIR, 'qps_4db_2x2.eps')

# 行优先：上排 CK | PG(n=1000)，下排 MyScale | PG(n=1)
PANELS = [
    ('ClickHouse',  os.path.join(CHART_DIR, 'ck_qps_c8.eps')),
    ('PG (n=1000)', os.path.join(CHART_DIR, 'pg_qps_c8_n1000.eps')),
    ('MyScale',     os.path.join(CHART_DIR, 'myscale_qps_c8.eps')),
    ('PG (n=1)',    os.path.join(CHART_DIR, 'pg_qps_c8_n1.eps')),
]


def _load_panel(path):
    """Read a panel through its PNG cache at full resolution."""
    png = panel_png(path)
    if not png:
        return None
    im = Image.open(png).convert("RGB")
    # 面板左侧竖排 y 轴标题占 16–50 px，56 px 起是刻度数字；随后再统一裁掉白边
    if im.width > 1000:
        im = im.crop((56, 0, im.width, im.height))
    im = trim_white(im)
    # 不做降采样：保留面板原始像素，避免 EPS 嵌入低分辨率位图后发虚
    return np.asarray(im) / 255.0


def main():
    dpi = 170
    imgs = [(name, path, _load_panel(path)) for name, path in PANELS]
    if all(im is None for _, _, im in imgs):
        raise SystemExit('[ERR] 无可用输入图')

    # 两列等宽；每列逻辑宽度 600 px @ 170 dpi，标题字号不随位图分辨率变化
    col_w = 600.0 / dpi
    gap_w = 0.18
    row_h = []
    for r in range(2):
        hs = [col_w * imgs[r * 2 + c][2].shape[0] / imgs[r * 2 + c][2].shape[1]
              for c in range(2) if imgs[r * 2 + c][2] is not None]
        row_h.append(max(hs) if hs else col_w * 0.5)

    gap_h = 0.34
    top_pad, bottom_pad = 0.78, 0.10
    fig_w = 2 * col_w + gap_w
    fig_h = sum(row_h) + gap_h + top_pad + bottom_pad
    # 画布以 2×dpi 输出，保证位图粘贴后在 \textwidth 下仍约 400 dpi
    fig = plt.figure(figsize=(fig_w, fig_h), dpi=2 * dpi)
    gs = fig.add_gridspec(2, 2, width_ratios=[1, 1], height_ratios=row_h,
                          left=0.012, right=0.988,
                          top=1 - top_pad / fig_h, bottom=bottom_pad / fig_h,
                          wspace=gap_w / col_w, hspace=gap_h / (sum(row_h) / 2))

    for idx, (name, path, im) in enumerate(imgs):
        ax = fig.add_subplot(gs[idx // 2, idx % 2])
        if im is None:
            ax.axis('off')
            ax.text(0.5, 0.5, 'missing: %s' % os.path.basename(path),
                    transform=ax.transAxes, ha='center', va='center',
                    fontsize=15, color='#c0392b')
            continue
        ax.imshow(im, aspect='auto')
        ax.axis('off')
        ax.set_title('%s' % name, loc='left', fontsize=10.5,
                     color='#2c3e50', pad=1)

    fig.suptitle('QPS comparison (concurrency=8) — four databases\n'
                 'QPS (queries/sec)',
                 fontsize=13, color='#2c3e50', y=0.992, linespacing=1.2)
    save_chart(fig, OUT_PATH, raster_cache=False, compress=False,
               facecolor='white', pad_inches=0.02)
    plt.close(fig)
    print('[OK] %s' % OUT_PATH)


if __name__ == '__main__':
    main()
