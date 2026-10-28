#!/usr/bin/env bash
# 用 Nuitka 把 benchmark/run.py 打包成单文件可执行程序（x86_64 / manylinux 版本）。
#
# 目标环境：quay.io/pypa/manylinux_2_28_x86_64 容器（自带 /opt/python/cp312
# 与静态 libpython）。产物布局与 PyInstaller 版本保持一致：
#   dist/myscale-bench/myscale-bench              可直接执行的封装脚本/二进制
#   dist/myscale-bench/datasets|configurations|results|docs  运行所需资源
#   dist/myscale-bench-linux-x86_64.tar.gz        + .sha256 校验文件
#
# 注意：Nuitka 的产物名会随版本/模式变化（onefile 出 <name>.bin，standalone
# 出 <name>.dist），因此下面用一串存在性检查来兼容不同命名，而不是硬编码。
set -euo pipefail

# ---- 基础环境 ----
# quay.io/pypa/manylinux_2_28_x86_64:latest
PYTHON_BIN="/opt/python/cp312-cp312/bin/python"
# 使用清华镜像加速容器内的 pip 安装。
PIP_INDEX_URL="https://pypi.tuna.tsinghua.edu.cn/simple"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$ROOT_DIR/benchmark"
BUILD_DIR="$ROOT_DIR/dist_nuitka"          # 中间产物（Nuitka 输出）
DIST_DIR="$ROOT_DIR/dist/myscale-bench"    # 最终分发包目录
TAR_NAME="myscale-bench-linux-x86_64.tar.gz"

# ---- 环境校验 ----
# 打包结果依赖 manylinux 的静态 libpython 与 yum，其他架构/发行版直接拒绝。
MACHINE="$(uname -m)"
case "$MACHINE" in
  x86_64|amd64) ;;
  *)
    echo "This script is intended to run in manylinux_2_28_x86_64 (uname -m: $MACHINE)" >&2
    exit 1
    ;;
esac

if [ ! -x "$PYTHON_BIN" ]; then
  echo "python not found: $PYTHON_BIN" >&2
  exit 1
fi

if ! command -v yum >/dev/null 2>&1; then
  echo "yum not found; this script expects manylinux_2_28_x86_64 base image" >&2
  exit 1
fi

# ---- pip 缓存与构建依赖 ----
export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_CACHE_DIR="${PIP_CACHE_DIR:-$ROOT_DIR/.cache/pip}"
mkdir -p "$PIP_CACHE_DIR"

# patchelf 是 Nuitka standalone/onefile 改写 RPATH 所必需的。
if ! command -v gcc >/dev/null 2>&1 || ! command -v g++ >/dev/null 2>&1; then
  yum install -y gcc gcc-c++ || true
  hash -r
fi
if ! command -v patchelf >/dev/null 2>&1; then
  yum install -y patchelf || true
  hash -r
fi

# 新版 pip 才支持 --root-user-action；容器里通常以 root 运行，需要该参数消除告警。
PIP_ROOT_ARGS=()
if "$PYTHON_BIN" -m pip install --help 2>/dev/null | grep -q -- '--root-user-action'; then
  PIP_ROOT_ARGS=(--root-user-action=ignore)
fi

# h5py 必须装预编译 wheel：容器内缺 HDF5 头文件与 fortran，源码编译会失败。
$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i "$PIP_INDEX_URL" -U pip setuptools wheel
$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i "$PIP_INDEX_URL" --only-binary=:all: h5py
$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i "$PIP_INDEX_URL" --no-build-isolation -U nuitka
if [ -f "$ROOT_DIR/requirements.txt" ]; then
  $PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i "$PIP_INDEX_URL" -r "$ROOT_DIR/requirements.txt"
fi

# 每次从干净的中间目录开始，避免旧的 .build/缓存影响 --follow-imports 的结果。
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# ---- 静态 libpython ----
# manylinux 镜像把静态库放在 /opt/_internal/cpython-<ver>/lib，若尚未解包则从
# 预置的 tar.xz 解出来（--static-libpython=yes 需要它）。
PY_PREFIX="$($PYTHON_BIN -c 'import sys; print(sys.prefix)')"
PY_VER="$($PYTHON_BIN -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"
PY_LIBDIR="$PY_PREFIX/lib"

INTERNAL_CPYTHON_DIR="/opt/_internal/cpython-$PY_VER"
INTERNAL_LIBDIR="$INTERNAL_CPYTHON_DIR/lib"
if [ ! -f "$INTERNAL_LIBDIR/libpython3.12.a" ] && [ -f /opt/_internal/static-libs-for-embedding-only.tar.xz ]; then
  tar -C /opt/_internal -xf /opt/_internal/static-libs-for-embedding-only.tar.xz
fi

# LIBRARY_PATH 供链接期找 .a，LD_LIBRARY_PATH 供运行期找 .so；ldconfig 刷新缓存。
export LIBRARY_PATH="$INTERNAL_LIBDIR:$PY_LIBDIR:${LIBRARY_PATH:-}"
export LD_LIBRARY_PATH="$PY_LIBDIR:${LD_LIBRARY_PATH:-}"
ldconfig

JOBS="${JOBS:-4}"   # 并行编译任务数
LTO="${LTO:-yes}"   # 链接时优化；开启后体积更小、启动更快，但编译明显变慢

# ---- 执行打包 ----
# 关键参数说明：
#   --standalone/--onefile   把依赖收进单个可执行文件，首次运行自解包到临时目录；
#   --follow-imports         递归收集 import 到的模块（benchmark 是包结构，必须开）；
#   --enable-plugin=multiprocessing  不启用时 worker 子进程在打包环境中找不到入口；
#   --no-deployment-flag=self-execution  允许程序在运行时重新 exec 自身；
#   --include-module=h5py._npystrings   h5py 的动态扩展无法被静态分析发现；
#   --assume-yes-for-downloads        容器内非交互，自动确认依赖下载；
#   --static-libpython=yes   静态链接 libpython，避免目标机缺少 libpython3.12.so。
cd "$SRC_DIR"
$PYTHON_BIN -m nuitka run.py \
  --standalone \
  --onefile \
  --follow-imports \
  --jobs="$JOBS" \
  --lto="$LTO" \
  --enable-plugin=multiprocessing \
  --no-deployment-flag=self-execution \
  --include-module=h5py._npystrings \
  --assume-yes-for-downloads \
  --static-libpython=yes \
  --output-dir="$BUILD_DIR" \
  --output-filename=myscale-bench

cd "$ROOT_DIR"

# ---- 兼容不同 Nuitka 版本的产物命名 ----
# onefile 通常产出 myscale-bench.bin（或直接是 myscale-bench）；
# standalone 产出 myscale-bench.dist/ 目录。按优先级逐个探测。
BIN="$BUILD_DIR/myscale-bench.bin"
if [ ! -f "$BIN" ]; then
  BIN="$BUILD_DIR/myscale-bench"
fi
DIST_TREE="$BUILD_DIR/myscale-bench.dist"
if [ ! -d "$DIST_TREE" ]; then
  DIST_TREE="$BUILD_DIR/run.dist"
fi
if [ ! -f "$BIN" ]; then
  BIN="$BUILD_DIR/run.bin"
fi
if [ ! -f "$BIN" ]; then
  BIN="$BUILD_DIR/run"
fi
if [ ! -f "$BIN" ] && [ ! -d "$DIST_TREE" ]; then
  echo "Nuitka output binary not found in $BUILD_DIR" >&2
  exit 1
fi

rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

if [ -f "$BIN" ]; then
  # 单文件模式：直接拷贝可执行文件并改名。
  cp -a "$BIN" "$DIST_DIR/myscale-bench"
elif [ -d "$DIST_TREE" ]; then
  # 目录模式：整体拷贝 .dist 目录，再生成一个转发脚本，使用户始终以
  # `./myscale-bench ...` 的方式调用，而不必关心内部二进制名字。
  cp -a "$DIST_TREE" "$DIST_DIR/myscale-bench.dist"
  if [ -f "$DIST_DIR/myscale-bench.dist/myscale-bench.bin" ]; then
    printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' 'exec "$DIR/myscale-bench.dist/myscale-bench.bin" "$@"' > "$DIST_DIR/myscale-bench"
    chmod +x "$DIST_DIR/myscale-bench"
  elif [ -f "$DIST_DIR/myscale-bench.dist/myscale-bench" ]; then
    printf '%s\n' '#!/usr/bin/env bash' 'set -euo pipefail' 'DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' 'exec "$DIR/myscale-bench.dist/myscale-bench" "$@"' > "$DIST_DIR/myscale-bench"
    chmod +x "$DIST_DIR/myscale-bench"
  else
    echo "Standalone dist produced, but main binary not found in $DIST_DIR/myscale-bench.dist" >&2
    exit 1
  fi
fi

# ---- 附带运行期资源 ----
# 这些文件不参与编译，但运行时需要：说明文档、数据集清单、配置文件、
# 结果目录与文档。路径相对可执行文件所在目录解析。
if [ -f "$ROOT_DIR/README.md" ]; then
  cp -a "$ROOT_DIR/README.md" "$DIST_DIR/"
fi
if [ -f "$ROOT_DIR/README.zh-CN.md" ]; then
  cp -a "$ROOT_DIR/README.zh-CN.md" "$DIST_DIR/"
fi

# 空的 downloaded 目录 + datasets.json：用户首次运行时由程序自行下载数据集。
mkdir -p "$DIST_DIR/datasets/downloaded"
cp -a "$SRC_DIR/datasets/datasets.json" "$DIST_DIR/datasets/"
if [ -f "$SRC_DIR/datasets/.gitignore" ]; then
  cp -a "$SRC_DIR/datasets/.gitignore" "$DIST_DIR/datasets/"
fi

mkdir -p "$DIST_DIR/configurations"
if [ -d "$ROOT_DIR/configurations" ]; then
  cp -a "$ROOT_DIR/configurations/." "$DIST_DIR/configurations/"
fi

mkdir -p "$DIST_DIR/results"
if [ -d "$SRC_DIR/results" ]; then
  cp -a "$SRC_DIR/results/." "$DIST_DIR/results/"
fi

if [ -d "$ROOT_DIR/docs" ]; then
  mkdir -p "$DIST_DIR/docs"
  cp -a "$ROOT_DIR/docs/." "$DIST_DIR/docs/"
fi

echo "$DIST_DIR"

# ---- 打包 ----
# 在 dist/ 目录内以相对名打 tar，解压后即为 dist/myscale-bench/ 结构。
tar -C "$ROOT_DIR/dist" -czf "$ROOT_DIR/dist/$TAR_NAME" myscale-bench
# 生成 sha256 校验文件，供发布时校验完整性。
( cd "$ROOT_DIR/dist" && sha256sum "$TAR_NAME" > "$TAR_NAME.sha256" )
