#!/usr/bin/env bash
# Local Linux wheel. Needs Zig. The resulting wheel does not.
# Portable manylinux_2_28 wheels are built in CI (bindings/python/ci_manylinux.sh).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ZIG="${ZIG:-$HOME/opt/zig-0.16/zig}"
cd "$ROOT"
"$ZIG" build -Doptimize=ReleaseFast -Dlib_only=true
export GLACIER_LIB="$ROOT/zig-out/lib/libglacier.so"
#!/usr/bin/env bash
# Local Linux wheel. Needs Zig. The resulting wheel does not.
# Portable manylinux_2_28 wheels are built in CI (bindings/python/ci_manylinux.sh).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ZIG="${ZIG:-$HOME/opt/zig-0.16/zig}"
cd "$ROOT"
"$ZIG" build -Doptimize=ReleaseFast -Dlib_only=true
export GLACIER_LIB="$ROOT/zig-out/lib/libglacier.so"
#!/usr/bin/env bash
# Local Linux wheel. Needs Zig. The resulting wheel does not.
# Portable manylinux_2_28 wheels are built in CI (bindings/python/ci_manylinux.sh).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ZIG="${ZIG:-$HOME/opt/zig-0.16/zig}"
cd "$ROOT"
"$ZIG" build -Doptimize=ReleaseFast -Dlib_only=true
export GLACIER_LIB="$ROOT/zig-out/lib/libglacier.so"
python3 -m pip install -q --upgrade --target "$ROOT/.pybuild" build wheel setuptools
export PYTHONPATH="$ROOT/.pybuild${PYTHONPATH:+:$PYTHONPATH}"
python3 -m build --wheel --no-isolation --outdir dist bindings/python
ls -lh dist/glacier_olap-*.whl
