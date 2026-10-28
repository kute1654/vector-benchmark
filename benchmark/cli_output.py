"""命令行输出辅助函数集合。

统一 ``run.py`` / 各客户端在上传、搜索阶段打印到 stdout 的格式，例如
``== 标题 ==``、``- 步骤``、``- key: value``。上传进度条使用 ``\\r`` 原地
刷新方式（不换行），因此引入模块级全局状态 ``_live_line_active``：
当上一行是未换行的“实时行”时，任何后续普通输出都要先补一个换行，
否则日志会与进度条粘在同一行。

全局状态：``_live_line_active`` 是唯一可变状态，由 ``set_live_line()``
与内部 ``_println()`` 维护，非线程安全（仅在主进程/主线程的进度打印路径使用）。
"""

import sys
from typing import Optional


# 是否有一条通过 "\r" 刷新、尚未换行的实时输出行（通常为上传进度）
_live_line_active = False


def set_live_line(active: bool) -> None:
    """标记/取消“当前存在未换行的实时行”。

    进度条写完后调用 ``set_live_line(True)``，需要正常换行输出时再置回
    ``False``。参数会被强制转换为 ``bool``。
    """
    global _live_line_active
    _live_line_active = bool(active)


def _println(line: str = "") -> None:
    """输出一行并立即 flush；若存在实时行则先补一个换行。

    这保证 ``\\r`` 刷新的进度条不会被后续日志覆盖或粘连。
    """
    global _live_line_active
    if _live_line_active:
        sys.stdout.write("\n")
        _live_line_active = False
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def header(title: str) -> None:
    """打印分节标题：先空一行，再输出 ``== title ==``。"""
    _println()
    _println(f"== {title} ==")


def stage(name: str) -> None:
    """打印阶段标题，等价于 ``header("STAGE: <name>")``。"""
    header(f"STAGE: {name}")


def step(message: str) -> None:
    """打印一条以 ``- `` 开头的步骤/信息行。"""
    _println(f"- {message}")


def warn(message: str) -> None:
    """打印红色告警行（ANSI 31 前景色），格式仍为 ``- message``。"""
    _println(f"\033[31m- {message}\033[0m")


def kv(key: str, value) -> None:
    """以 ``key: value`` 形式打印一条步骤行。"""
    step(f"{key}: {value}")


def compact_kv(title: str, **kwargs) -> None:
    """把多个键值压缩成一行打印，跳过值为 ``None`` 的项。

    例：``compact_kv("upload", parallel=16, batch_size=256)`` 输出
    ``- upload: parallel=16 batch_size=256``；若无剩余键值，只输出 ``- title``。
    """
    parts = []
    for k, v in kwargs.items():
        if v is None:
            continue
        parts.append(f"{k}={v}")
    if parts:
        step(f"{title}: " + " ".join(parts))
    else:
        step(title)


def sql(statement: str) -> None:
    """打印一条 ``SQL: <statement>`` 步骤行（不执行 SQL）。"""
    step(f"SQL: {statement}")


def status(title: str, value: str, detail: Optional[str] = None) -> None:
    """打印状态行；``detail`` 非 None 时追加为括号说明。

    输出形如 ``- title: value (detail)`` 或 ``- title: value``。
    """
    if detail is not None:
        step(f"{title}: {value} ({detail})")
    else:
        step(f"{title}: {value}")