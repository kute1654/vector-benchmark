#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""绘制 AST / 查询计划两级缓存的构建与命中流程图（两栏版）。

前提：两级缓存默认全部开启（图中不再出现缓存开关判断）。

版式：
  - 左栏：SQL 文本 → 轻量化解析/构造 AST 键 → AST 命中判断 → 完整解析 → 计划键 → 计划命中判断
  - 右栏：构建计划 → 记录并匹配常量位置 → 记录/写缓存 → 执行并结束
  - 两栏之间用一条折线连接；所有箭头用显式三角形绘制（不依赖 SVG marker，
    Word 的 SVG 渲染器不支持 marker-end，导入后会丢失箭头）

输出：
  - 默认生成 SVG（文字使用系统中文字体）
  - 可选 --png 生成 PNG（需要中文字体，可用 --font 指定 .ttf/.otf）

用法：
  python3 draw_plan_cache_flow.py
  python3 draw_plan_cache_flow.py -o flow.svg
  python3 draw_plan_cache_flow.py --png flow.png --font /path/to/NotoSansCJK.ttc
"""

from __future__ import annotations

import argparse
import html
import math
import os
import sys
from dataclasses import dataclass, field

# ----------------------------------------------------------------------------- 画布与样式
W, H = 1500, 1170
FONT_STACK = ("'Microsoft YaHei','微软雅黑','SimHei','黑体','PingFang SC',"
              "'Noto Sans CJK SC','Source Han Sans SC',sans-serif")
FONT_SIZE = 24
LINE_H = 30
DECISION_SIZE = 25
EDGE_LABEL_SIZE = 19
ARROW_LEN = 14
ARROW_HALF = 7

STYLE = {
    "process":  dict(fill="#EAF2FB", stroke="#3B6EA5"),
    "decision": dict(fill="#FFF4E0", stroke="#B7791F"),
    "start":    dict(fill="#E8F5E9", stroke="#2F855A"),
    "end":      dict(fill="#E8F5E9", stroke="#2F855A"),
}
EDGE_COLOR = "#2D3748"
TEXT_COLOR = "#1A202C"
MUTED = "#4A5568"


# ----------------------------------------------------------------------------- 数据模型
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
    # 左栏：第一级 AST 缓存
    Node("start", "start", 360, 52, 260, 58, ["SQL 文本"]),

    Node("norm", "process", 360, 176, 660, 146, [
        "轻量化解析：词法扫描，跳过空白与注释",
        "按括号、逗号、引号边界提取常量",
        "常量替换为统一占位符，拼出规范化 SQL",
        "规范化 SQL + 库 + 设置 + 用户/角色 + 标签构建 AST 缓存键",
    ]),
    Node("d_ast", "decision", 360, 342, 360, 142, ["AST 缓存键", "是否命中？"]),
    Node("end_ast", "end", 360, 506, 500, 142, [
        "深拷贝缓存的 AST 与查询计划",
        "按常量出现顺序回填本次常量",
        "常量顺序紊乱的特例逐一枚举处理",
        "直接执行（不再解析、不再规划）",
    ]),

    Node("parse", "process", 360, 651, 560, 104, [
        "完整解析 SQL",
        "（文法归约、构造全节点 AST、语法检查）",
    ]),

    Node("plan_key", "process", 360, 777, 560, 104, [
        "从 AST 提取常量并替换为占位符构建规范化 AST", "基于规范化 AST 构造计划缓存键",
    ]),
    Node("d_plan", "decision", 360, 922, 360, 142, ["计划缓存键", "是否命中？"]),
    Node("end_plan", "end", 360, 1077, 500, 124, [
        "深拷贝查询计划",
        "按计划中的位置信息回填本次常量",
        "执行更新后的计划",
    ]),

    # 右栏：第二级查询计划缓存（未命中路径）
    Node("build", "process", 1100, 180, 460, 80, ["构建并优化查询计划"]),
    Node("record", "process", 1100, 322, 600, 112, [
        "提取 AST 常量位置与计划常量位置",
        "逐一匹配（双向核对）",
    ]),
    Node("d_match", "decision", 1100, 482, 500, 146, [
        "AST 常量与计划常量", "是否匹配成功？",
    ]),
    Node("write", "process", 1100, 642, 620, 112, [
        "匹配成功才缓存查询计划",
        "记录计划与常量绑定",
        "写入 AST、计划与常量位置",
    ]),
    Node("end_ok", "end", 1100, 762, 400, 84, [
        "执行优化后的计划", "查询结束",
    ]),
]


EDGES: list[Edge] = [
    # 左栏
    Edge("start", "norm", [(360, 81), (360, 103)]),
    Edge("norm", "d_ast", [(360, 249), (360, 271)]),

    Edge("d_ast", "end_ast", [(360, 413), (360, 435)], "是", (380, 424)),
    Edge("d_ast", "parse", [(540, 342), (680, 342), (680, 651), (640, 651)], "否", (606, 326)),

    Edge("parse", "plan_key", [(360, 703), (360, 725)]),
    Edge("plan_key", "d_plan", [(360, 829), (360, 851)]),

    Edge("d_plan", "end_plan", [(360, 993), (360, 1015)], "是", (380, 1004)),
    Edge("d_plan", "build", [(540, 922), (760, 922), (760, 180), (870, 180)], "否", (620, 906)),

    # 右栏
    Edge("build", "record", [(1100, 220), (1100, 266)]),
    Edge("record", "d_match", [(1100, 378), (1100, 409)]),

    Edge("d_match", "write", [(1100, 555), (1100, 586)], "是", (1120, 570)),
    Edge("write", "end_ok", [(1100, 698), (1100, 720)]),
    Edge("d_match", "end_ok", [(850, 482), (780, 482), (780, 762), (900, 762)], "否（不缓存计划）", (815, 466)),
]


# ----------------------------------------------------------------------------- SVG 绘制
def esc(s: str) -> str:
    return html.escape(s, quote=True)


def text_width_units(s: str) -> float:
    """粗略估算文本宽度（以字号为单位）：CJK/全角按 1，ASCII 按 0.56。"""
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
    """用显式三角形代替 marker-end：Word 的 SVG 导入不支持 marker。"""
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

    # 先画节点、再画连线：箭头在最上层，避免被节点填充遮挡
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


# ----------------------------------------------------------------------------- 可选 PNG 输出
def find_cjk_font(explicit: str | None):
    if explicit:
        return explicit
    try:
        from matplotlib import font_manager
    except Exception:
        return None
    for f in font_manager.fontManager.ttflist:
        name = f.name.lower()
        if any(k in name for k in ("cjk", "wenquanyi", "yahei", "pingfang",
                                   "source han", "noto sans sc", "simhei", "simsun")):
            return f.fname
    return None


def build_png(path: str, font_path: str | None) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import FancyBboxPatch, Polygon
    from matplotlib.font_manager import FontProperties

    fp = None
    if font_path and os.path.exists(font_path):
        fp = FontProperties(fname=font_path)
    else:
        sys.stderr.write(
            "警告：未找到中文字体，PNG 中的中文会显示为方框。"
            "请用 --font 指定一个 .ttf/.otf（如 NotoSansCJK-Regular.ttc）。\n")

    def draw_text(x, y, s, size):
        ax.text(x, H - y, s, ha="center", va="center", fontsize=size,
                fontproperties=fp, color=TEXT_COLOR)

    fig = plt.figure(figsize=(W / 100, H / 100), dpi=150)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, W)
    ax.set_ylim(0, H)
    ax.axis("off")

    # 节点先画，连线后画（箭头在最上层）
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
        fs = 19 if n.kind == "decision" else 18
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
                    fontsize=14, fontproperties=fp, color=MUTED,
                    bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="none", alpha=0.95))

    fig.savefig(path, dpi=150, facecolor="white")
    plt.close(fig)
    print(f"已生成 {path}")


# ----------------------------------------------------------------------------- 入口
def main() -> None:
    ap = argparse.ArgumentParser(description="绘制 AST/查询计划两级缓存流程图（两栏）")
    ap.add_argument("-o", "--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                        "plan_cache_flow.svg"),
                    help="SVG 输出路径")
    ap.add_argument("--png", metavar="PATH", help="同时输出 PNG（需要中文字体）")
    ap.add_argument("--font", metavar="TTF", help="PNG 使用的中文字体文件")
    args = ap.parse_args()

    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(build_svg())
    print(f"已生成 {args.out}")

    if args.png:
        build_png(args.png, find_cjk_font(args.font))


if __name__ == "__main__":
    main()
