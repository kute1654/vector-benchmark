#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate a portrait (vertical) EPS of the vector-query comparison figure.

The layout follows ``纯向量查询.png``: it compares the SQL form used by
relational databases with the API form used by dedicated vector databases.

To make the figure convenient for two-column typesetting, the two panels are
stacked vertically (one below the other) instead of side by side:

    (a) Relational Databases: Vector Queries via SQL      (top)
    (b) Vector Databases: Vector Queries via API         (bottom)

Formatting rules:
  - All text is left-aligned.
  - The database headers (relational / vector databases) are NOT bold.
  - The SQL and API statements (code lines) are bold; comment lines are normal.

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

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCE_PNG = os.path.join(HERE, "纯向量查询.png")

HEADER_FONT = dict(family=["DejaVu Serif", "Liberation Serif", "serif"],
                   fontsize=13.5, fontweight="normal", color="#111111")
CODE_FONT = dict(family=["DejaVu Sans Mono", "Liberation Mono", "monospace"],
                 fontsize=12.2, fontweight="normal", color="#111111")
SQL_FONT = dict(CODE_FONT, fontweight="bold")
LABEL_FONT = dict(family=["DejaVu Serif", "Liberation Serif", "serif"],
                  fontsize=12.5, color="#111111")
BG_COLOR = "#F7F4ED"   # soft warm off-white

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

# Canvas in data units: portrait, sized so a column-width rendering fits a section.
W_UNITS = 560
H_UNITS = 920

# Vertical rhythm in data units.
HEADER_TO_CODE = 52
LINE_STEP = 30
LABEL_GAP = 55
TOP_MARGIN = 32

# Left margin for all text (left-aligned layout).
X_LEFT = 35


def draw_vector_figure(out_path: str, png_path: str | None = None) -> None:
    matplotlib.rcParams["ps.fonttype"] = 42
    matplotlib.rcParams["ps.useafm"] = False
    matplotlib.rcParams["pdf.fonttype"] = 42

    fig = plt.figure(figsize=(W_UNITS / 100.0, H_UNITS / 100.0), dpi=150)
    fig.patch.set_facecolor(BG_COLOR)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, W_UNITS)
    ax.set_ylim(0, H_UNITS)
    ax.axis("off")

    def place_section(header, code, label, y):
        """Draw one panel left-aligned; returns the y position below its label."""
        ax.text(X_LEFT, y, header, ha="left", va="top", **HEADER_FONT)
        y -= HEADER_TO_CODE
        for line in code:
            style = CODE_FONT if line.startswith("#") else SQL_FONT
            ax.text(X_LEFT, y, line, ha="left", va="top", **style)
            y -= LINE_STEP
        ax.text(270, y, label, ha="center", va="top", **LABEL_FONT)
        return y - LABEL_GAP

    y = H_UNITS - TOP_MARGIN
    y = place_section(LEFT_HEADER, LEFT_CODE, "(a)", y)
    y = place_section(RIGHT_HEADER, RIGHT_CODE, "(b)", y)

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
        description="Generate a portrait EPS of the vector-query comparison figure")
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
