#!/usr/bin/env bash
# Run inside quay.io/pypa/manylinux_2_28_{x86_64,aarch64}. Native Zig target
# is the image glibc (2.28). Zig 0.16 cannot retarget an older glibc from Ubuntu.
set -euo pipefail

ZVER="${ZIG_VERSION:-0.16.0}"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64) ZIG_ARCH=x86_64; PLAT=manylinux_2_28_x86_64 ;;
  aarch64) ZIG_ARCH=aarch64; PLAT=manylinux_2_28_aarch64 ;;
  *) echo "unsupported arch: $ARCH" >&2; exit 1 ;;
esac

ZIG_DIR="zig-${ZIG_ARCH}-linux-${ZVER}"
if [[ ! -x "$ZIG_DIR/zig" ]]; then
  curl -fsSL "https://ziglang.org/download/${ZVER}/${ZIG_DIR}.tar.xz" | tar -xJ
fi
export PATH="$PWD/$ZIG_DIR:$PATH"
zig version

zig build -Doptimize=ReleaseFast -Dlib_only=true
zig build -Doptimize=ReleaseFast fixtures

LIB="$PWD/zig-out/lib/libglacier.so"
if [[ ! -f "$LIB" ]]; then
  echo "missing $LIB" >&2
  exit 1
fi
export GLACIER_LIB="$LIB"

PY="${PY:-/opt/python/cp312-cp312/bin/python}"
export PIP_ROOT_USER_ACTION=ignore
"$PY" -m pip install -q build wheel auditwheel
rm -rf /tmp/glacier-wheels
"$PY" -m build --wheel --outdir /tmp/glacier-wheels bindings/python
mkdir -p dist
auditwheel repair /tmp/glacier-wheels/*.whl -w dist --plat "$PLAT"
auditwheel show dist/*.whl
ls -lh dist/*.whl
