import sys
from typing import Optional


_live_line_active = False


def set_live_line(active: bool) -> None:
    global _live_line_active
    _live_line_active = bool(active)


def _println(line: str = "") -> None:
    global _live_line_active
    if _live_line_active:
        sys.stdout.write("\n")
        _live_line_active = False
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def header(title: str) -> None:
    _println()
    _println(f"== {title} ==")


def stage(name: str) -> None:
    header(f"STAGE: {name}")


def step(message: str) -> None:
    _println(f"- {message}")


def warn(message: str) -> None:
    _println(f"\033[31m- {message}\033[0m")


def kv(key: str, value) -> None:
    step(f"{key}: {value}")


def compact_kv(title: str, **kwargs) -> None:
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
    step(f"SQL: {statement}")


def status(title: str, value: str, detail: Optional[str] = None) -> None:
    if detail is not None:
        step(f"{title}: {value} ({detail})")
    else:
        step(f"{title}: {value}")
