"""Vector Benchmark - A unified benchmark framework for vector databases."""

import os
import sys
import shutil
from pathlib import Path

from .engine import (
    BaseEngine,
    PgvectorEngine,
    PolarDBEngine,
    ClickHouseEngine,
    MyScaleEngine,
    ENGINE_REGISTRY,
    get_engine,
)
from .dataset_reader import AnnH5Reader


def get_root_dir():
    def _looks_like_root(candidate: Path) -> bool:
        return (
            candidate.is_dir()
            and (candidate / "datasets").is_dir()
            and (candidate / "configurations").is_dir()
            and (candidate / "results").is_dir()
        )

    onefile_parent = os.environ.get("NUITKA_ONEFILE_PARENT")
    if onefile_parent:
        candidate = Path(onefile_parent).resolve()
        candidate = candidate.parent if candidate.is_file() else candidate
        if _looks_like_root(candidate):
            return candidate

    argv0 = sys.argv[0] if sys.argv else ""
    if argv0:
        argv0_path = Path(argv0)
        if not argv0_path.is_absolute() and os.path.sep not in argv0:
            resolved = shutil.which(argv0)
            if resolved:
                argv0_path = Path(resolved)
        try:
            argv0_path = argv0_path.resolve()
        except FileNotFoundError:
            pass

        candidate = argv0_path.parent if argv0_path.is_file() else argv0_path
        if _looks_like_root(candidate):
            return candidate

    cwd = Path.cwd().resolve()
    if _looks_like_root(cwd):
        return cwd

    if getattr(sys, "frozen", False) or "__compiled__" in globals():
        return Path(sys.executable).resolve().parent

    return Path(__file__).resolve().parent.parent


ROOT_DIR = get_root_dir()
DATASETS_DIR = ROOT_DIR / "datasets"
CONFIGURATIONS_DIR = ROOT_DIR / "configurations"
RESULTS_DIR = ROOT_DIR / "results"

__all__ = [
    "BaseEngine",
    "PgvectorEngine",
    "PolarDBEngine",
    "ClickHouseEngine",
    "MyScaleEngine",
    "ENGINE_REGISTRY",
    "get_engine",
    "AnnH5Reader",
    "ROOT_DIR",
    "DATASETS_DIR",
    "CONFIGURATIONS_DIR",
    "RESULTS_DIR",
]