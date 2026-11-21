#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate an EPS version of the vector-query comparison figure.

The layout follows ``纯向量查询.png``: two columns comparing the SQL form used
by relational databases with the API form used by dedicated vector databases.

By default the figure is redrawn with vector text, which stays sharp in LaTeX.
Pass ``--raster`` to wrap the original PNG bitmap into an EPS instead.

Usage:
  python3 draw_vector_query_modes.py
  python3 draw_vector_query_modes.py -o vector_query_modes.eps
  python3 draw_vector_query_modes.py --raster
"""

from __future__ import annotations

import argparse
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE_PNG = os.path.join(HERE, "纯向量查询.png")

HEADER_FONT = dict(family=["DejaVu Serif", "Liberation Serif", "serif"],
                   fontsize=13.5, fontweight="bold", color="#111111")
CODE_FONT = dict(family=["DejaVu Sans Mono", "Liberation Mono", "monospace"],
                 fontsize=12.2, color="#111111")
SQL_FONT = dict(CODE_FONT, fontweight="bold")
LABEL_FONT = dict(family=["DejaVu Serif", "Liberation Serif", "serif"],
                  fontsize=12.5, color="#111111")
BG_COLOR = "none"   # soft warm off-white

LEFT_HEADER = "Relational Databases: Vector Queries via SQL"
LEFT_CODE = [
    "# pgvector",
    "SELECT id, (embedding <=> '[...]'::vector)",
    "AS dis FROM items",
    "ORDER BY dis ASC LIMIT 10;",
    "# ClickHouse",
    "SELECT id, cosineDistance(embedding, [...])",
    "AS dist FROM items",
    "ORDER BY dist ASC LIMIT 10;",
    "# MyScaleDB",
    "SELECT id, distance(embedding, [...]) AS",
    "dist FROM items",
    "ORDER BY dist ASC LIMIT 10;",
]

RIGHT_HEADER = "Vector Databases: Vector Queries via API"
RIGHT_CODE = [
    "# Milvus",
    "client.search(",
    "    collection_name=\"items\",",
    "    data=[query_vec],",
    "    anns_field=\"embedding\",",
    "    search_params={\"metric_type\":",
    "\"COSINE\", \"params\": {\"ef\": 100}},",
    "    limit=10,",
    "    output_fields=[\"id\", \"category\"],",
    ")",
]


def draw_vector_figure(out_path: str, png_path: str | None = None) -> None:
    matplotlib.rcParams["ps.fonttype"] = 42
    matplotlib.rcParams["ps.useafm"] = False
    matplotlib.rcParams["pdf.fonttype"] = 42

    fig = plt.figure(figsize=(10.34, 5.15), dpi=150)
    fig.patch.set_facecolor(BG_COLOR)

    def column(x, header, code, label, label_x, bold_code=False):
        fig.text(x, 0.965, header, ha="left", va="top", **HEADER_FONT)
        y = 0.902
        for line in code:
            style = SQL_FONT if (bold_code and not line.startswith("#")) else CODE_FONT
            fig.text(x, y, line, ha="left", va="top", **style)
            y -= 0.0525
        fig.text(label_x, 0.259, label, ha="center", va="top", **LABEL_FONT)

    column(0.012, LEFT_HEADER, LEFT_CODE, "(a)", 0.245, bold_code=True)
    column(0.518, RIGHT_HEADER, RIGHT_CODE, "(b)", 0.755, bold_code=True)

    fig.savefig(out_path, format="eps", facecolor=BG_COLOR,
                bbox_inches="tight", pad_inches=0.02)
    if png_path:
        fig.savefig(png_path, dpi=200, facecolor=BG_COLOR,
                    bbox_inches="tight", pad_inches=0.02)
        print(f"Wrote {png_path}")
    plt.close(fig)
    print(f"Wrote {out_path}")


def wrap_raster(source: str, out_path: str) -> None:
    """Embed the source PNG bitmap into an EPS (exact pixel reproduction)."""
    import matplotlib.image as mpimg

    if not os.path.exists(source):
        raise SystemExit(f"[ERR] source image not found: {source}")
    img = mpimg.imread(source)
    h, w = img.shape[:2]
    fig = plt.figure(figsize=(w / 100.0, h / 100.0), dpi=150)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.imshow(img)
    ax.axis("off")
    fig.savefig(out_path, format="eps", facecolor="white")
    plt.close(fig)
    print(f"Wrote {out_path}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate an EPS of the vector-query comparison figure")
    ap.add_argument("-o", "--out", default=os.path.join(HERE, "vector_query_modes.eps"),
                    help="EPS output path (default: vector_query_modes.eps)")
    ap.add_argument("--raster", action="store_true",
                    help="wrap 纯向量查询.png as a bitmap EPS instead of redrawing")
    ap.add_argument("--png", metavar="PATH",
                    help="also export a PNG preview of the redrawn figure")
    args = ap.parse_args()

    if args.raster:
        wrap_raster(SOURCE_PNG, args.out)
    else:
        draw_vector_figure(args.out, args.png)


if __name__ == "__main__":
    main()
