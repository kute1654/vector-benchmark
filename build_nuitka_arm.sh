#!/usr/bin/env bash
# 用 Nuitka 把 benchmark/run.py 打包成 aarch64（ARM64）单文件可执行程序。
#
# 与 build_nuitka.sh 的区别：
#   * 基础镜像为 Ubuntu 22.04（arm64），用 apt 而非 yum 装依赖；
#   * python 来自发行版（默认 python3），并要求 >= 3.10；
#   * 不做静态 libpython（--static-libpython 未开启），因此产物体积略大、
#     但对目标机的 glibc 版本更敏感；
#   * 输出到 dist-arm/，方便与 x86_64 的 dist/ 并存。
#
# 目标环境：docker pull --platform linux/arm64 docker.m.daocloud.io/library/ubuntu:22.04
set -euo pipefail

# 允许通过环境变量覆盖解释器，便于在非默认路径安装新版本 Python。
PYTHON_BIN="${PYTHON_BIN:-python3}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$ROOT_DIR/benchmark"
BUILD_DIR="$ROOT_DIR/dist_nuitka"          # Nuitka 中间产物
OUT_DIR="$ROOT_DIR/dist-arm"               # ARM 专用输出根目录
DIST_DIR="$OUT_DIR/myscale-bench"          # 最终分发包目录
TAR_NAME="myscale-bench-linux-aarch64.tar.gz"
PIP_ROOT_ARGS=()

# ---- 架构校验：产物只能在与构建机同架构的机器上运行 ----
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

# ---- 配置 apt 镜像源 ----
# 国内访问 archive.ubuntu.com 很慢，默认切到清华镜像；arm64 走 ubuntu-ports
# （Ubuntu 的官方端口仓库），架构判断同时参考 dpkg 与 uname。
export DEBIAN_FRONTEND=noninteractive
if [ -r /etc/os-release ] && . /etc/os-release && [ "${ID:-}" = "ubuntu" ]; then
  CODENAME="${VERSION_CODENAME:-}"
  if [ -z "$CODENAME" ] && command -v lsb_release >/dev/null 2>&1; then
    CODENAME="$(lsb_release -cs 2>/dev/null || true)"
  fi
  if [ -z "$CODENAME" ]; then
    # 22.04 的代号；镜像里既无 os-release 字段也无 lsb_release 时兜底。
    CODENAME="jammy"
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
  # 直接重写 sources.list：镜像里的默认源可能失效或过慢，且我们只需这四个仓库。
  cat > /etc/apt/sources.list <<EOF
deb ${APT_MIRROR} ${CODENAME} main restricted universe multiverse
deb ${APT_MIRROR} ${CODENAME}-updates main restricted universe multiverse
deb ${APT_MIRROR} ${CODENAME}-backports main restricted universe multiverse
deb ${APT_MIRROR} ${CODENAME}-security main restricted universe multiverse
EOF
fi

# patchelf：Nuitka 改写 RPATH 必需；build-essential/python3-dev：编译扩展与静态链接。
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

# 项目使用了 3.10+ 的语法/标准库特性，低版本直接拒绝，避免打包到一半才报错。
PY_VER="$("$PYTHON_BIN" -c 'import sys; print(".".join(map(str, sys.version_info[:3])))')"
if ! "$PYTHON_BIN" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else 1)'; then
  echo "python>=3.10 required, got $PY_VER (set PYTHON_BIN to a newer python3)" >&2
  exit 1
fi

if "$PYTHON_BIN" -m pip install --help 2>/dev/null | grep -q -- '--root-user-action'; then
  PIP_ROOT_ARGS=(--root-user-action=ignore)
fi

# h5py 强制使用预编译 wheel：ARM 上源码编译 HDF5 耗时且易失败。
$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i https://pypi.tuna.tsinghua.edu.cn/simple -U pip setuptools wheel
$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i https://pypi.tuna.tsinghua.edu.cn/simple --only-binary=:all: h5py
$PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i https://pypi.tuna.tsinghua.edu.cn/simple --no-build-isolation -U nuitka
if [ -f "$ROOT_DIR/requirements.txt" ]; then
  $PYTHON_BIN -m pip install "${PIP_ROOT_ARGS[@]}" -i https://pypi.tuna.tsinghua.edu.cn/simple -r "$ROOT_DIR/requirements.txt"
fi

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

# ---- 执行打包 ----
# LTO 在 ARM 交叉/原生构建下非常慢且收益有限，这里固定 --lto=no、--jobs=8。
# 其余参数含义同 build_nuitka.sh：--enable-plugin=multiprocessing 保证子进程
# 可启动，--include-module=h5py._npystrings 补上静态分析看不见的 h5py 扩展；
# 本脚本不加 --static-libpython，因此目标机需自带兼容的 libpython。
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

# ---- 探测产物路径（兼容不同 Nuitka 版本的命名） ----
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
  # 单文件产物：直接改名拷贝。
  cp -a "$BIN" "$DIST_DIR/myscale-bench"
else
  # 目录产物：拷贝 .dist 并生成转发脚本；ENTRY_REL 是相对 DIST_DIR 的路径，
  # 写进脚本后即便整个目录被移动也能正确执行。
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

# ---- 附带运行期资源（与 x86 脚本保持一致） ----
if [ -f "$ROOT_DIR/README.md" ]; then
  cp -a "$ROOT_DIR/README.md" "$DIST_DIR/"
fi
if [ -f "$ROOT_DIR/README.zh-CN.md" ]; then
  cp -a "$ROOT_DIR/README.zh-CN.md" "$DIST_DIR/"
fi

# 空的 downloaded 目录让首运行时程序自行下载数据集。
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

# ---- 打包与校验和 ----
# 归档在 OUT_DIR 内以相对名创建，解压后得到 dist-arm/myscale-bench/。
mkdir -p "$OUT_DIR"
tar -C "$OUT_DIR" -czf "$OUT_DIR/$TAR_NAME" myscale-bench
( cd "$OUT_DIR" && sha256sum "$TAR_NAME" > "$TAR_NAME.sha256" )
