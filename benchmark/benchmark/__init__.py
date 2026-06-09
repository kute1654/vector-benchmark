import os
import sys
import shutil
from pathlib import Path

def get_root_dir():
    """
    获取项目的根目录。
    在打包模式下，返回可执行文件所在的物理目录；
    在开发模式下，返回源代码根目录。
    """
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
    
    # 开发模式：指向当前文件所在位置的上两级（请根据你的目录层级调整 .parent 数量）
    return Path(__file__).resolve().parent.parent

# 全局根目录变量
ROOT_DIR = get_root_dir()

# 业务相关的子目录定义
DATASETS_DIR = ROOT_DIR / "datasets"
CONFIGURATIONS_DIR = ROOT_DIR / "configurations"
RESULTS_DIR = ROOT_DIR / "results"
