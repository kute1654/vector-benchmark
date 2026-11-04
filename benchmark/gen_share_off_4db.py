"""把四张 share-off 阶段占比 PNG 合成一张 2×2 总图。

输入（都在 results/charts/ 下）:
  clickhouse_share_off.png
  myscale_share_off.png
  pgvector_1000_share_off.png
  pgvector_1_share_off.png

输出:
  results/charts/share_off_4db.png

本脚本只做图像拼接，不重跑任何数据生成逻辑。
"""
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

CHART_DIR = os.path.join(os.path.dirname(__file__), 'results', 'charts')
OUT_PATH = os.path.join(CHART_DIR, 'share_off_4db.png')

PANELS = [
    ('ClickHouse',        os.path.join(CHART_DIR, 'clickhouse_share_off.png')),
    ('MyScale',           os.path.join(CHART_DIR, 'myscale_share_off.png')),
    ('pgvector (n=1000)', os.path.join(CHART_DIR, 'pgvector_1000_share_off.png')),
    ('pgvector (n=1)',    os.path.join(CHART_DIR, 'pgvector_1_share_off.png')),
]


def main():
    # 用第一张图的比例决定 figsize，保持子图无挤压
    ref = plt.imread(PANELS[0][1])
    h, w = ref.shape[:2]
    aspect = w / h

    # 按单图实际像素比例 × 2 列 / 2 行，几乎不留 padding
    dpi = 170
    cell_w = w / dpi
    cell_h = h / dpi
    fig_w = cell_w * 2
    fig_h = cell_h * 2 + 0.3  # 给 suptitle 一点高度

    fig, axes = plt.subplots(2, 2, figsize=(fig_w, fig_h), dpi=dpi)
    axes = axes.flat

    for idx, (name, path) in enumerate(PANELS):
        ax = axes[idx]
        if not os.path.exists(path):
            ax.axis('off')
            ax.text(0.5, 0.5, 'missing: %s' % os.path.basename(path),
                    transform=ax.transAxes, ha='center', va='center',
                    fontsize=10, color='#c0392b')
            continue
        img = plt.imread(path)
        # aspect='auto' 让图片拉伸填满整个 axes，消掉左右空白
        ax.imshow(img, aspect='auto')
        ax.axis('off')
        ax.set_title('%s' % name, loc='left', fontsize=11,
                     color='#2c3e50', pad=3)

    fig.suptitle('Stage time share (plan cache OFF) — four databases',
                 fontsize=14, color='#2c3e50')
    fig.subplots_adjust(left=0.005, right=0.995, top=0.93, bottom=0.005,
                        hspace=0.02, wspace=0.01)
    fig.savefig(OUT_PATH, facecolor='white', pad_inches=0.02)
    plt.close(fig)
    print('[OK] %s' % OUT_PATH)


if __name__ == '__main__':
    main()
