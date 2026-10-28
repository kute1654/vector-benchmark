"""Vector Benchmark - A unified benchmark framework for vector databases.

本包是整个 benchmark 工程的顶层 Python 包，导入时完成两件事：

1. 确定工程根目录 ``ROOT_DIR``，并派生 ``DATASETS_DIR``、
   ``CONFIGURATIONS_DIR``、``RESULTS_DIR`` 三个约定目录（模块级全局状态）。
2. 对外导出脚本式引擎（``BaseEngine`` 及其子类）、引擎注册表
   ``ENGINE_REGISTRY`` / 工厂 ``get_engine``，以及 ANN H5 数据集读取器。

重要副作用（导入即执行）：``get_root_dir()`` 会探测环境变量与文件系统
（``sys.argv[0]``、当前工作目录、可执行文件路径等）；本模块本身不读写
数据集、不连接数据库。

注意区分两套抽象：``benchmark.engine.base.BaseEngine`` 是“脚本式”基类，
适合快速测量 QPS / 召回；``benchmark/engine/base_client/``（在 ``run.py``
中按顶层 ``engine.*`` 命名空间导入）是正式实验使用的“驱动式”客户端基类。
"""

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
    """定位并返回 benchmark 工程根目录。

    探测顺序（命中即返回）：
    1. Nuitka onefile 模式下，环境变量 ``NUITKA_ONEFILE_PARENT`` 指向的目录；
    2. ``sys.argv[0]`` 所在目录（裸命令名时用 ``shutil.which`` 解析为绝对路径）；
    3. 当前工作目录 ``cwd``；
    4. 冻结 / Nuitka 编译产物模式下，``sys.executable`` 所在目录；
    5. 源码仓库：本包目录（要求含 ``datasets`` 与 ``configurations``），
       否则退回其父目录。

    返回：``pathlib.Path``，工程根目录。
    副作用：仅做文件系统探测，不创建或修改任何文件；结果在模块导入时
    被固化为 ``ROOT_DIR`` 等全局常量。
    """

    def _looks_like_root(candidate: Path) -> bool:
        """判断目录是否具备工程根的三件套子目录（datasets/configurations/results）。

        只做存在性判断，不读取内容；用于在多个候选路径中挑出真正的工程根。
        """
        # 同时具备 datasets/、configurations/、results/ 三个子目录才算工程根。
        # 注意：results/ 被 gitignore，全新 clone 时可能缺失，故源码分支另有兜底。
        return (
            candidate.is_dir()
            and (candidate / "datasets").is_dir()
            and (candidate / "configurations").is_dir()
            and (candidate / "results").is_dir()
        )

    # Nuitka onefile 解包运行：父进程路径可能指向打包后的可执行文件本身
    onefile_parent = os.environ.get("NUITKA_ONEFILE_PARENT")
    if onefile_parent:
        candidate = Path(onefile_parent).resolve()
        candidate = candidate.parent if candidate.is_file() else candidate
        if _looks_like_root(candidate):
            return candidate

    # 其次尝试脚本自身位置；argv[0] 可能只是命令名（如 `vector-bench`），需在 PATH 中查找
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

    # 再次尝试当前工作目录（支持 `cd benchmark && python run.py` 的用法）
    cwd = Path.cwd().resolve()
    if _looks_like_root(cwd):
        return cwd

    # 冻结（PyInstaller）或 Nuitka 编译产物：以可执行文件所在目录为准
    if getattr(sys, "frozen", False) or "__compiled__" in globals():
        return Path(sys.executable).resolve().parent

    # Source checkout: the package directory itself is the benchmark root
    # (`benchmark/datasets`, `benchmark/configurations`, `benchmark/results`).
    # `results/` is gitignored, so it may be absent on a fresh clone — in that
    # case `_looks_like_root` cannot verify it, hence the direct check here.
    # 源码仓库下包目录即根目录；因 results/ 可能不存在，这里只校验前两个目录。
    pkg_dir = Path(__file__).resolve().parent
    if (pkg_dir / "datasets").is_dir() and (pkg_dir / "configurations").is_dir():
        return pkg_dir
    return pkg_dir.parent


# ---- 模块级路径常量：导入时求值一次，全工程复用 ----
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