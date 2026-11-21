"""把 gen_qps_report.py 输出的四张 QPS 对比 EPS 合成一张 4×1 总图。

输入（都在 results/charts/ 下，需用 gen_qps_report.py --no-title 生成，
不带左上角长标题；组合图只在顶部保留一条总标题，每个子图左上角一行库名标签）:
  ck_qps_c8.eps
  myscale_qps_c8.eps
  pg_qps_c8_n1000.eps
  pg_qps_c8_n1.eps

输出:
  results/charts/qps_4db.eps

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
OUT_PATH = os.path.join(CHART_DIR, 'qps_4db.eps')

PANELS = [
    ('ClickHouse',        os.path.join(CHART_DIR, 'ck_qps_c8.eps')),
    ('MyScale',           os.path.join(CHART_DIR, 'myscale_qps_c8.eps')),
    ('PG (n=1000)', os.path.join(CHART_DIR, 'pg_qps_c8_n1000.eps')),
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
    # 读入全部子图；每行高度 = 行宽 × 该图实际高宽比，保证子图不被拉伸失真
    imgs = [(name, path, _load_panel(path))
            for name, path in PANELS]
    ref = next((im for _, _, im in imgs if im is not None), None)
    if ref is None:
        raise SystemExit('[ERR] 无可用输入图')
    # 画布逻辑宽度固定为 7.05 in（1200 px @ 170 dpi），标题字号不随位图像素变化；
    # 子图按原始像素贴入，从而在 \textwidth 下获得约 400 dpi 的清晰度
    cell_w = 1200.0 / dpi
    heights = [cell_w * im.shape[0] / im.shape[1] if im is not None else cell_w
               for _, _, im in imgs]

    ROW_GAP = 0.17                      # 仅够放一行库名标签，不再留大块空白
    fig_h = sum(heights) + ROW_GAP * (len(imgs) - 1) + 0.80
    fig = plt.figure(figsize=(cell_w, fig_h), dpi=dpi)
    gs = fig.add_gridspec(len(imgs), 1, height_ratios=heights,
                          left=0.020, right=0.992, top=0.905, bottom=0.006,
                          hspace=ROW_GAP / (sum(heights) / len(imgs)))

    for idx, (name, path, im) in enumerate(imgs):
        ax = fig.add_subplot(gs[idx, 0])
        if im is None:
            ax.axis('off')
            ax.text(0.5, 0.5, 'missing: %s' % os.path.basename(path),
                    transform=ax.transAxes, ha='center', va='center',
                    fontsize=22, color='#c0392b')
            continue
        # 行的宽高比与图片一致，aspect='auto' 只是贴满格子，不会产生拉伸
        ax.imshow(im, aspect='auto')
        ax.axis('off')                   # 子图不画长标题, 库名放在紧贴面板的标签行
        ax.set_title('%s' % name, loc='left', fontsize=12.5,
                     color='#2c3e50', pad=1)

    fig.suptitle('QPS comparison (concurrency=8) — four databases\n'
                 'QPS (queries/sec)',
                 fontsize=15, color='#2c3e50', y=0.988, linespacing=1.25)
    save_chart(fig, OUT_PATH, raster_cache=False, compress=False, facecolor='white', pad_inches=0.02)
    plt.close(fig)
    print('[OK] %s' % OUT_PATH)


if __name__ == '__main__':
    main()
