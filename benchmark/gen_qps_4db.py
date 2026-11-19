"""把 gen_qps_report.py 输出的四张 QPS 对比 PNG 合成一张 4×1 总图。

输入（都在 results/charts/ 下，需用 gen_qps_report.py --no-title 生成，
不带左上角长标题；组合图只在顶部保留一条总标题，每个子图左上角一行库名标签）:
  ck_qps_c8.png
  myscale_qps_c8.png
  pg_qps_c8_n1000.png
  pg_qps_c8_n1.png

输出:
  results/charts/qps_4db.png

本脚本只做图像拼接，不重跑任何数据生成逻辑。
"""
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

CHART_DIR = os.path.join(os.path.dirname(__file__), 'results', 'charts')
OUT_PATH = os.path.join(CHART_DIR, 'qps_4db.png')

PANELS = [
    ('ClickHouse',        os.path.join(CHART_DIR, 'ck_qps_c8.png')),
    ('MyScale',           os.path.join(CHART_DIR, 'myscale_qps_c8.png')),
    ('PG (n=1000)', os.path.join(CHART_DIR, 'pg_qps_c8_n1000.png')),
    ('PG (n=1)',    os.path.join(CHART_DIR, 'pg_qps_c8_n1.png')),
]


def main():
    dpi = 170
    # 读入全部子图；每行高度 = 行宽 × 该图实际高宽比，保证子图不被拉伸失真
    imgs = [(name, path, plt.imread(path) if os.path.exists(path) else None)
            for name, path in PANELS]
    ref = next((im for _, _, im in imgs if im is not None), None)
    if ref is None:
        raise SystemExit('[ERR] 无可用输入图')
    cell_w = ref.shape[1] / dpi
    heights = [cell_w * im.shape[0] / im.shape[1] if im is not None else cell_w
               for _, _, im in imgs]

    ROW_GAP = 0.5                       # 行间距(英寸)：给库名标签留位（源图已不带长标题）
    # 画布总高 = 各行实际高 + 行间距 + suptitle 高度；行间距换算成 hspace(平均行高的比例)
    fig_h = sum(heights) + ROW_GAP * (len(imgs) - 1) + 0.45
    fig = plt.figure(figsize=(cell_w, fig_h), dpi=dpi)
    gs = fig.add_gridspec(len(imgs), 1, height_ratios=heights,
                          left=0.005, right=0.995, top=0.985, bottom=0.005,
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
        ax.axis('off')                   # 子图不画长标题, 只保留一行库名标签 + figure 级总标题
        ax.set_title('%s' % name, loc='left', fontsize=24,
                     color='#2c3e50', pad=2)

    fig.suptitle('QPS comparison (concurrency=8) — four databases',
                 fontsize=26, color='#2c3e50', y=0.995)
    fig.savefig(OUT_PATH, facecolor='white', pad_inches=0.02)
    plt.close(fig)
    print('[OK] %s' % OUT_PATH)


if __name__ == '__main__':
    main()
