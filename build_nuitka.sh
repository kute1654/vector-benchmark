#!/usr/bin/env bash
set -euo pipefail

# quay.io/pypa/manylinux_2_28_x86_64:latest
PYTHON_BIN="/opt/python/cp312-cp312/bin/python"
PIP_INDEX_URL="https://pypi.tuna.tsinghua.edu.cn/simple"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$ROOT_DIR/myscale-bench"
BUILD_DIR="$ROOT_DIR/dist_nuitka"
DIST_DIR="$ROOT_DIR/dist/myscale-bench"
TAR_NAME="myscale-bench-linux-x86_64.tar.gz"

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

export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_CACHE_DIR="${PIP_CACHE_DIR:-$ROOT_DIR/.cache/pip}"
mkdir -p "$PIP_CACHE_DIR"

if ! command -v gcc >/dev/null 2>&1 || ! command -v g++ >/dev/null 2>&1; then
  yum install -y gcc gcc-c++ || true
  hash -r
fi
if ! command -v patchelf >/dev/null 2>&1; then
  yum install -y patchelf || true
  hash -r
fi

PIP_ROOT_ARGS=()
if "$PYTHON_BIN" -m pip install --help 2>/dev/null | grep -q -- '--root-user-action'; then
  PIP_ROOT_ARGS=(--root-user-action=ignore)
fi

$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i "$PIP_INDEX_URL" -U pip setuptools wheel
$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i "$PIP_INDEX_URL" --only-binary=:all: h5py
$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i "$PIP_INDEX_URL" --no-build-isolation -U nuitka
if [ -f "$SRC_DIR/requirements.txt" ]; then
  $PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i "$PIP_INDEX_URL" -r "$SRC_DIR/requirements.txt"
fi

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

PY_PREFIX="$($PYTHON_BIN -c 'import sys; print(sys.prefix)')"
PY_VER="$($PYTHON_BIN -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"
PY_LIBDIR="$PY_PREFIX/lib"

INTERNAL_CPYTHON_DIR="/opt/_internal/cpython-$PY_VER"
INTERNAL_LIBDIR="$INTERNAL_CPYTHON_DIR/lib"
if [ ! -f "$INTERNAL_LIBDIR/libpython3.12.a" ] && [ -f /opt/_internal/static-libs-for-embedding-only.tar.xz ]; then
  tar -C /opt/_internal -xf /opt/_internal/static-libs-for-embedding-only.tar.xz
fi

export LIBRARY_PATH="$INTERNAL_LIBDIR:$PY_LIBDIR:${LIBRARY_PATH:-}"
export LD_LIBRARY_PATH="$PY_LIBDIR:${LD_LIBRARY_PATH:-}"
ldconfig

JOBS="${JOBS:-4}"
LTO="${LTO:-yes}"

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
  cp -a "$BIN" "$DIST_DIR/myscale-bench"
elif [ -d "$DIST_TREE" ]; then
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

if [ -f "$ROOT_DIR/README.md" ]; then
  cp -a "$ROOT_DIR/README.md" "$DIST_DIR/"
fi
if [ -f "$ROOT_DIR/README.zh-CN.md" ]; then
  cp -a "$ROOT_DIR/README.zh-CN.md" "$DIST_DIR/"
fi

mkdir -p "$DIST_DIR/datasets/downloaded"
cp -a "$SRC_DIR/datasets/datasets.json" "$DIST_DIR/datasets/"
if [ -f "$SRC_DIR/datasets/.gitignore" ]; then
  cp -a "$SRC_DIR/datasets/.gitignore" "$DIST_DIR/datasets/"
fi

mkdir -p "$DIST_DIR/configurations"
if [ -d "$SRC_DIR/configurations" ]; then
  cp -a "$SRC_DIR/configurations/." "$DIST_DIR/configurations/"
fi

mkdir -p "$DIST_DIR/results"
if [ -d "$SRC_DIR/results" ]; then
  cp -a "$SRC_DIR/results/." "$DIST_DIR/results/"
fi

if [ -d "$SRC_DIR/docs" ]; then
  mkdir -p "$DIST_DIR/docs"
  cp -a "$SRC_DIR/docs/." "$DIST_DIR/docs/"
fi

echo "$DIST_DIR"

tar -C "$ROOT_DIR/dist" -czf "$ROOT_DIR/dist/$TAR_NAME" myscale-bench
( cd "$ROOT_DIR/dist" && sha256sum "$TAR_NAME" > "$TAR_NAME.sha256" )
