#!/usr/bin/env python3
"""Flow chart of the two-level AST / query-plan cache (English version).

Assumption: both cache levels are enabled by default; the figure contains no
cache on/off branches.

Layout:
  - Left column: SQL text -> lightweight parsing / AST key -> AST hit check
    -> full parsing -> plan key -> plan hit check.
  - Right column: build plan -> record and match constant positions ->
    write cache -> execute and finish.
  - A polyline connects the two columns. Arrow heads are explicit triangles
    because some SVG renderers do not support marker-end.

Output:
  - EPS by default (vector output for LaTeX).
  - Optional SVG via --svg, and optional PNG via --png.
  - Font can be selected with --font.

Usage:
  python3 draw_plan_cache_flow_en.py
  python3 draw_plan_cache_flow_en.py -o flow_en.eps
  python3 draw_plan_cache_flow_en.py --svg flow_en.svg --png flow_en.png
"""

from __future__ import annotations

import argparse
import html
import math
import os
import sys
from dataclasses import dataclass, field

# ----------------------------------------------------------------------------- canvas and style
W, H = 2010, 1476
FONT_STACK = ("'Times New Roman','Liberation Serif','DejaVu Serif',"
              "'Noto Serif','serif'")
FONT_SIZE = 32
LINE_H = 54
DECISION_SIZE = 32
EDGE_LABEL_SIZE = 30
ARROW_LEN = 20
ARROW_HALF = 10

STYLE = {
    "process":  dict(fill="#EAF2FB", stroke="#3B6EA5"),
    "decision": dict(fill="#FFF4E0", stroke="#B7791F"),
    "start":    dict(fill="#E8F5E9", stroke="#2F855A"),
    "end":      dict(fill="#E8F5E9", stroke="#2F855A"),
}
EDGE_COLOR = "#2D3748"
TEXT_COLOR = "#1A202C"
MUTED = "#4A5568"


# ----------------------------------------------------------------------------- data model
@dataclass
class Node:
    nid: str
    kind: str            # start | process | decision | end
    cx: float
    cy: float
    w: float
    h: float
    lines: list[str] = field(default_factory=list)


@dataclass
class Edge:
    src: str
    dst: str
    points: list[tuple[float, float]]
    label: str = ""
    label_xy: tuple[float, float] | None = None


NODES: list[Node] = [
    # Left column: level-1 AST cache（加宽、压低）
    Node("start", "start", 560, 45, 340, 70, ["SQL text"]),

    Node("norm", "process", 560, 238, 1100, 220, [
        "Tokenize; skip comments",
        "Extract constants; normalize",
        "Key: SQL+DB+settings+user/role/tags",
    ]),
    Node("d_ast", "decision", 560, 471, 800, 150, ["AST cache key hit?"]),
    Node("end_ast", "end", 560, 677, 1080, 166, [
        "Copy cached AST/plan, Backfill constants;",
        "execute, Skip parsing/planning",
    ]),

    Node("parse", "process", 560, 868, 900, 120, [
        "Full SQL parsing",
        "AST + syntax check",
    ]),

    Node("plan_key", "process", 560, 1036, 1050, 120, [
        "Extract constants; replace",
        "Build plan-cache key",
    ]),
    Node("d_plan", "decision", 560, 1219, 800, 150, ["Plan cache key hit?"]),
    Node("end_plan", "end", 560, 1402, 900, 120, [
        "Copy cached plan, Backfill; execute",
    ]),

    # Right column: level-2 plan cache（除判断节点外全部收窄）
    Node("build", "process", 1600, 55, 520, 90, ["Build query plan"]),
    Node("record", "process", 1600, 382, 760, 205, [
        "Extract constant slots in AST",
        "and plan Match bidirectionally",
    ]),
    Node("d_match", "decision", 1600, 748, 560, 170, [
        "AST/plan match?",
    ]),
    Node("write", "process", 1600, 1087, 550, 150, [
        "Cache on match",
        "Store plan + bindings",
    ]),
    Node("end_ok", "end", 1600, 1401, 520, 120, [
        "Execute plan", "Query finished",
    ]),
]


EDGES: list[Edge] = [
    # Left column
    Edge("start", "norm", [(560, 80), (560, 128)]),
    Edge("norm", "d_ast", [(560, 348), (560, 396)]),

    Edge("d_ast", "end_ast", [(560, 546), (560, 594)], "Yes", (660, 565)),
    Edge("d_ast", "parse", [(960, 471), (1140, 471), (1140, 868), (1010, 868)],
         "No", (1000, 440)),

    Edge("parse", "plan_key", [(560, 928), (560, 976)]),
    Edge("plan_key", "d_plan", [(560, 1096), (560, 1144)]),

    Edge("d_plan", "end_plan", [(560, 1294), (560, 1342)], "Yes", (660, 1312)),
    Edge("d_plan", "build", [(960, 1219), (1170, 1219), (1170, 55), (1340, 55)],
         "No", (1030, 1190)),

    # Right column
    Edge("build", "record", [(1600, 100), (1600, 279)]),
    Edge("record", "d_match", [(1600, 484), (1600, 663)]),

    Edge("d_match", "write", [(1600, 833), (1600, 1012)], "Yes", (1650, 922)),
    Edge("write", "end_ok", [(1600, 1162), (1600, 1341)]),
    Edge("d_match", "end_ok", [(1880, 748), (1985, 748), (1985, 1401), (1860, 1401)],
         "No", (1930, 715)),
]


# ----------------------------------------------------------------------------- SVG drawing
def esc(s: str) -> str:
    return html.escape(s, quote=True)


def text_width_units(s: str) -> float:
    """Rough text width in font-size units: CJK/full-width = 1, ASCII = 0.56."""
    u = 0.0
    for ch in s:
        u += 1.0 if ord(ch) > 0x2E80 else 0.56
    return u


def svg_text_lines(node: Node, font_size: float, line_h: float) -> str:
    n = len(node.lines)
    if n == 0:
        return ""
    total = (n - 1) * line_h
    x, y = node.cx, node.cy - total / 2
    out = []
    for line in node.lines:
        out.append(
            f'<text x="{x:.1f}" y="{y + font_size * 0.35:.1f}" text-anchor="middle" '
            f'font-size="{font_size}" fill="{TEXT_COLOR}">{esc(line)}</text>'
        )
        y += line_h
    return "\n".join(out)


def svg_shape(node: Node) -> str:
    st = STYLE[node.kind]
    x, y, w, h = node.cx - node.w / 2, node.cy - node.h / 2, node.w, node.h
    common = f'fill="{st["fill"]}" stroke="{st["stroke"]}" stroke-width="2"'
    if node.kind == "decision":
        pts = f"{node.cx},{y} {x + w},{node.cy} {node.cx},{y + h} {x},{node.cy}"
        return f'<polygon points="{pts}" {common} />'
    if node.kind in ("start", "end"):
        r = h / 2
        return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{r}" ry="{r}" {common} />'
    return f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="8" ry="8" {common} />'


def arrow_head(points: list[tuple[float, float]]) -> str:
    """Draw an explicit triangle instead of marker-end."""
    (x1, y1), (x2, y2) = points[-2], points[-1]
    dx, dy = x2 - x1, y2 - y1
    length = math.hypot(dx, dy) or 1.0
    ux, uy = dx / length, dy / length
    bx, by = x2 - ux * ARROW_LEN, y2 - uy * ARROW_LEN
    px, py = -uy, ux
    pts = (f"{x2:.1f},{y2:.1f} "
           f"{bx + px * ARROW_HALF:.1f},{by + py * ARROW_HALF:.1f} "
           f"{bx - px * ARROW_HALF:.1f},{by - py * ARROW_HALF:.1f}")
    return f'<polygon points="{pts}" fill="{EDGE_COLOR}" />'


def svg_edge(e: Edge) -> str:
    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in e.points)
    out = (f'<polyline points="{pts}" fill="none" stroke="{EDGE_COLOR}" '
           f'stroke-width="2.2" stroke-linejoin="round" stroke-linecap="butt" />')
    out += "\n" + arrow_head(e.points)
    if e.label and e.label_xy:
        lx, ly = e.label_xy
        tw = text_width_units(e.label) * EDGE_LABEL_SIZE + 10
        out += (f'\n<rect x="{lx - tw / 2:.1f}" y="{ly - 14:.1f}" width="{tw:.1f}" '
                f'height="26" rx="4" fill="#FFFFFF" opacity="0.95" />')
        out += (f'\n<text x="{lx:.1f}" y="{ly + 7:.1f}" text-anchor="middle" '
                f'font-size="{EDGE_LABEL_SIZE}" fill="{MUTED}">{esc(e.label)}</text>')
    return out


def build_svg() -> str:
    body = [f'<rect width="{W}" height="{H}" fill="#FFFFFF" />']

    # Nodes first, edges second: arrow heads stay above node fills.
    for n in NODES:
        body.append(svg_shape(n))
        fs = DECISION_SIZE if n.kind == "decision" else FONT_SIZE
        body.append(svg_text_lines(n, fs, LINE_H))
    for e in EDGES:
        body.append(svg_edge(e))

    return (
        f'<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}" font-family="{FONT_STACK}">\n'
        + "\n".join(body) + "\n</svg>\n"
    )


# ----------------------------------------------------------------------------- matplotlib output (EPS/PNG)
def find_font(explicit: str | None):
    """Pick a TrueType serif with full ASCII coverage.

    Newer matplotlib lists variant faces such as ``DejaVu Serif Display`` that
    contain no Latin glyphs; substring matching would pick them and all text
    would vanish from the EPS.  Match exact family names and verify glyphs.
    """
    if explicit:
        return explicit
    try:
        from matplotlib import font_manager
        from matplotlib.ft2font import FT2Font
    except Exception:
        return None

    preferred = ["times new roman", "times", "liberation serif",
                 "tex gyre termes", "nimbus roman", "freeserif",
                 "dejavu serif"]
    def is_regular(weight) -> bool:
        return str(weight).lower() in ("normal", "regular", "400")

    def has_ascii(fname: str) -> bool:
        try:
            ft = FT2Font(fname)
        except Exception:
            return False
        return all(ft.get_char_index(ord(c)) for c in "Aa0?&")

    candidates = []
    for f in font_manager.fontManager.ttflist:
        if f.style != "normal" or not is_regular(f.weight):
            continue
        if not str(f.fname).lower().endswith((".ttf", ".ttc")):
            continue
        name = f.name.lower()
        if name in preferred:
            candidates.append((preferred.index(name), f.fname))
    for _, fname in sorted(candidates):
        if has_ascii(fname):
            return fname
    # 兜底：任意带完整 ASCII 的常规 TrueType，避免选中“Display”等残缺变体
    for f in font_manager.fontManager.ttflist:
        if f.style == "normal" and is_regular(f.weight) and \
                str(f.fname).lower().endswith((".ttf", ".ttc")) and has_ascii(f.fname):
            return f.fname
    return None


def build_raster(path: str, font_path: str | None) -> None:
    """Render the flow chart to EPS/PNG/PDF, chosen from the file suffix."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyBboxPatch, Polygon
    from matplotlib.font_manager import FontProperties
    from matplotlib.textpath import TextPath, TextToPath
    from matplotlib.ft2font import FT2Font

    # TrueType embedding keeps EPS editable and avoids Type-3 font warnings.
    matplotlib.rcParams["ps.fonttype"] = 42
    matplotlib.rcParams["ps.useafm"] = False
    matplotlib.rcParams["pdf.fonttype"] = 42

    fp = None
    if font_path and os.path.exists(font_path):
        fp = FontProperties(fname=font_path)
    else:
        sys.stderr.write("Warning: no serif font found; pass --font for EPS/PNG output.\n")

    def draw_text(x, y, s, size):
        ax.text(x, H - y, s, ha="center", va="center", fontsize=size,
                fontproperties=fp, color=TEXT_COLOR)

    text_to_path = TextToPath()
    ft_font = None
    if font_path and os.path.exists(font_path):
        try:
            ft_font = FT2Font(font_path)
        except Exception:
            ft_font = None

    def _line_width_units(line, fs):
        """Width of *line* at font size *fs* in canvas units (1 unit = 0.72 pt)."""
        if ft_font is not None:
            # FT2Font.get_width_height() 返回 1/64 pt，跨 matplotlib 版本稳定
            ft_font.set_size(fs, 72)
            ft_font.set_text(line, 0)
            w, _ = ft_font.get_width_height()
            return (w / 64.0) / 72.0 * 100.0
        prop = fp.copy() if fp is not None else FontProperties()
        prop.set_size(fs)
        try:
            w, _, _ = text_to_path.get_text_width_height_descent(line, prop, False)
        except Exception:
            w = TextPath((0, 0), line, size=fs, prop=fp).get_extents().width
        return w / 72.0 * 100.0

    def fit_font_size(lines, box_w, base):
        """Largest font size (<= base) whose widest line fits inside the box."""
        for fs in range(base, 7, -1):
            widest = max(_line_width_units(line, fs) for line in lines)
            if widest <= box_w - 40:
                return fs
        return 8

    fig = plt.figure(figsize=(W / 100, H / 100), dpi=150)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, W)
    ax.set_ylim(0, H)
    ax.axis("off")

    for n in NODES:
        st = STYLE[n.kind]
        x, y = n.cx - n.w / 2, H - (n.cy + n.h / 2)
        if n.kind == "decision":
            pts = [(n.cx, H - (n.cy - n.h / 2)), (x + n.w, H - n.cy),
                   (n.cx, H - (n.cy + n.h / 2)), (x, H - n.cy)]
            ax.add_patch(Polygon(pts, closed=True, facecolor=st["fill"],
                                 edgecolor=st["stroke"], linewidth=1.6))
        else:
            r = n.h / 2 if n.kind in ("start", "end") else 8
            ax.add_patch(FancyBboxPatch((x, y), n.w, n.h,
                                        boxstyle=f"round,pad=0,rounding_size={r}",
                                        facecolor=st["fill"], edgecolor=st["stroke"],
                                        linewidth=1.6))
        total = (len(n.lines) - 1) * LINE_H
        base_fs = DECISION_SIZE if n.kind == "decision" else FONT_SIZE
        fs = fit_font_size(n.lines, n.w, base_fs)
        for i, line in enumerate(n.lines):
            draw_text(n.cx, n.cy - total / 2 + i * LINE_H, line, fs)

    for e in EDGES:
        xs = [p[0] for p in e.points]
        ys = [H - p[1] for p in e.points]
        ax.plot(xs, ys, color=EDGE_COLOR, linewidth=1.8, solid_capstyle="butt")
        (x1, y1), (x2, y2) = e.points[-2], e.points[-1]
        dx, dy = x2 - x1, y2 - y1
        length = math.hypot(dx, dy) or 1.0
        ux, uy = dx / length, dy / length
        bx, by = x2 - ux * ARROW_LEN, y2 - uy * ARROW_LEN
        px, py = -uy, ux
        ax.add_patch(Polygon([(x2, H - y2),
                              (bx + px * ARROW_HALF, H - (by + py * ARROW_HALF)),
                              (bx - px * ARROW_HALF, H - (by - py * ARROW_HALF))],
                             closed=True, facecolor=EDGE_COLOR, edgecolor=EDGE_COLOR))
        if e.label and e.label_xy:
            ax.text(e.label_xy[0], H - e.label_xy[1], e.label, ha="center", va="center",
                    fontsize=EDGE_LABEL_SIZE, fontproperties=fp, color=MUTED,
                    bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none"))

    suffix = os.path.splitext(path)[1].lower()
    if suffix == ".eps":
        fig.savefig(path, format="eps", facecolor="white")
    elif suffix == ".pdf":
        fig.savefig(path, format="pdf", facecolor="white")
    else:
        fig.savefig(path, dpi=150, facecolor="white")
    plt.close(fig)
    print(f"Wrote {path}")


# ----------------------------------------------------------------------------- entry point
def main() -> None:
    ap = argparse.ArgumentParser(description="Draw the two-level AST/plan cache flow (English)")
    ap.add_argument("-o", "--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                        "plan_cache_flow_en.eps"),
                    help="output path; .eps (default), .png, .pdf, or .svg")
    ap.add_argument("--svg", metavar="PATH", help="also write an SVG")
    ap.add_argument("--png", metavar="PATH", help="also write a PNG")
    ap.add_argument("--font", metavar="TTF", help="font file for EPS/PNG/PDF output")
    args = ap.parse_args()

    suffix = os.path.splitext(args.out)[1].lower()
    if suffix == ".svg":
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(build_svg())
        print(f"Wrote {args.out}")
    else:
        build_raster(args.out, find_font(args.font))

    if args.svg:
        with open(args.svg, "w", encoding="utf-8") as fh:
            fh.write(build_svg())
        print(f"Wrote {args.svg}")

    if args.png:
        build_raster(args.png, find_font(args.font))


if __name__ == "__main__":
    main()
