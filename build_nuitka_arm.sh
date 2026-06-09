#!/usr/bin/env bash
# docker pull --platform linux/arm64 docker.m.daocloud.io/library/ubuntu:20.04

set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-python3}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$ROOT_DIR/myscale-bench"
BUILD_DIR="$ROOT_DIR/dist_nuitka"
OUT_DIR="$ROOT_DIR/dist-arm"
DIST_DIR="$OUT_DIR/myscale-bench"
TAR_NAME="myscale-bench-linux-aarch64.tar.gz"
PIP_ROOT_ARGS=()

MACHINE="$(uname -m)"
case "$MACHINE" in
  aarch64|arm64) ;;
  *)
    echo "This script is intended to run in an ARM64 container (uname -m: $MACHINE)" >&2
    exit 1
    ;;
esac

export PIP_DISABLE_PIP_VERSION_CHECK=1
export PIP_CACHE_DIR="${PIP_CACHE_DIR:-$ROOT_DIR/.cache/pip}"
mkdir -p "$PIP_CACHE_DIR"

if ! command -v apt-get >/dev/null 2>&1; then
  echo "apt-get not found; this script expects an Ubuntu/Debian base image" >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive
if [ -r /etc/os-release ] && . /etc/os-release && [ "${ID:-}" = "ubuntu" ]; then
  CODENAME="${VERSION_CODENAME:-}"
  if [ -z "$CODENAME" ] && command -v lsb_release >/dev/null 2>&1; then
    CODENAME="$(lsb_release -cs 2>/dev/null || true)"
  fi
  if [ -z "$CODENAME" ]; then
    CODENAME="focal"
  fi

  APT_MIRROR_DEFAULT="http://mirrors.tuna.tsinghua.edu.cn/ubuntu/"
  if command -v dpkg >/dev/null 2>&1; then
    APT_ARCH="$(dpkg --print-architecture 2>/dev/null || true)"
  else
    APT_ARCH=""
  fi
  if [ "$APT_ARCH" = "arm64" ] || [ "$MACHINE" = "aarch64" ] || [ "$MACHINE" = "arm64" ]; then
    APT_MIRROR_DEFAULT="http://mirrors.tuna.tsinghua.edu.cn/ubuntu-ports/"
  fi
  APT_MIRROR="${APT_MIRROR:-$APT_MIRROR_DEFAULT}"
  cat > /etc/apt/sources.list <<EOF
deb ${APT_MIRROR} ${CODENAME} main restricted universe multiverse
deb ${APT_MIRROR} ${CODENAME}-updates main restricted universe multiverse
deb ${APT_MIRROR} ${CODENAME}-backports main restricted universe multiverse
deb ${APT_MIRROR} ${CODENAME}-security main restricted universe multiverse
EOF
fi

apt-get update
apt-get install -y --no-install-recommends \
  ca-certificates \
  patchelf \
  build-essential \
  python3 \
  python3-pip \
  python3-venv \
  python3-dev

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "python not found: PYTHON_BIN='$PYTHON_BIN'" >&2
  exit 1
fi

PY_VER="$("$PYTHON_BIN" -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"
if ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)'; then
  echo "python>=3.10 required, got $PY_VER (set PYTHON_BIN to a newer python3)" >&2
  exit 1
fi

if "$PYTHON_BIN" -m pip install --help 2>/dev/null | grep -q -- '--root-user-action'; then
  PIP_ROOT_ARGS=(--root-user-action=ignore)
fi

$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i https://pypi.tuna.tsinghua.edu.cn/simple -U pip setuptools wheel
$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i https://pypi.tuna.tsinghua.edu.cn/simple --only-binary=:all: h5py
$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i https://pypi.tuna.tsinghua.edu.cn/simple --no-build-isolation -U nuitka
if [ -f "$SRC_DIR/requirements.txt" ]; then
  $PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i https://pypi.tuna.tsinghua.edu.cn/simple -r "$SRC_DIR/requirements.txt"
fi

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

cd "$SRC_DIR"
$PYTHON_BIN -m nuitka run.py \
  --standalone \
  --onefile \
  --follow-imports \
  --jobs=8 \
  --lto=no \
  --enable-plugin=multiprocessing \
  --no-deployment-flag=self-execution \
  --include-module=h5py._npystrings \
  --assume-yes-for-downloads \
  --output-dir="$BUILD_DIR" \
  --output-filename=myscale-bench

cd "$ROOT_DIR"

BIN=""
for c in \
  "$BUILD_DIR/myscale-bench.bin" \
  "$BUILD_DIR/myscale-bench" \
  "$BUILD_DIR/run.bin" \
  "$BUILD_DIR/run"; do
  if [ -f "$c" ]; then
    BIN="$c"
    break
  fi
done

DIST_TREE=""
for d in "$BUILD_DIR/myscale-bench.dist" "$BUILD_DIR/run.dist"; do
  if [ -d "$d" ]; then
    DIST_TREE="$d"
    break
  fi
done

if [ -z "$BIN" ] && [ -z "$DIST_TREE" ]; then
  echo "Nuitka output binary not found in $BUILD_DIR" >&2
  exit 1
fi

rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

if [ -n "$BIN" ] && [ -f "$BIN" ]; then
  cp -a "$BIN" "$DIST_DIR/myscale-bench"
else
  cp -a "$DIST_TREE" "$DIST_DIR/myscale-bench.dist"

  ENTRY=""
  for e in \
    "$DIST_DIR/myscale-bench.dist/myscale-bench.bin" \
    "$DIST_DIR/myscale-bench.dist/myscale-bench" \
    "$DIST_DIR/myscale-bench.dist/run.bin" \
    "$DIST_DIR/myscale-bench.dist/run"; do
    if [ -f "$e" ]; then
      ENTRY="$e"
      break
    fi
  done
  if [ -z "$ENTRY" ]; then
    echo "Standalone dist produced, but main binary not found" >&2
    exit 1
  fi
  ENTRY_REL="${ENTRY#"$DIST_DIR/"}"
  printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    'DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' \
    "exec \"\$DIR/$ENTRY_REL\" \"\$@\"" > "$DIST_DIR/myscale-bench"
  chmod +x "$DIST_DIR/myscale-bench"
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

if [ -f "$ROOT_DIR/README.md" ]; then
  cp -a "$ROOT_DIR/README.md" "$DIST_DIR/"
fi
if [ -f "$ROOT_DIR/README.zh-CN.md" ]; then
  cp -a "$ROOT_DIR/README.zh-CN.md" "$DIST_DIR/"
fi

echo "$DIST_DIR"

mkdir -p "$OUT_DIR"
tar -C "$OUT_DIR" -czf "$OUT_DIR/$TAR_NAME" myscale-bench
( cd "$OUT_DIR" && sha256sum "$TAR_NAME" > "$TAR_NAME.sha256" )
