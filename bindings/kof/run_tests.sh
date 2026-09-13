#!/usr/bin/env bash
# Compile Conn.kf, swap in the JNI Conn.class, run tests.
# Work files (parquet copy, kof --output, GLACIER_TEMP) stay on the HD.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ZIG="${ZIG:-$HOME/opt/zig-0.16/zig}"
KOF_HOME="${KOF_HOME:-/tmp/kof-dist/kof-0.3.23-beta-linux-x86_64}"
HD="${GLACIER_KOF_HD:-/run/media/honinbou/MeusArquivos/glacier-kof/work}"
export GLACIER_TEMP="${GLACIER_TEMP:-$HD/temp}"

if [[ ! -x "$KOF_HOME/bin/kof" ]]; then
  echo "set KOF_HOME to a Kof linux-x86_64 distro (bin/kof + jdk/)" >&2
  exit 1
fi
if [[ ! -d "$HD" ]]; then
  mkdir -p "$HD"
fi
mkdir -p "$GLACIER_TEMP" "$HD/out" "$HD/suite"
rm -f "$HD/suite"/*.kf

export PATH="$KOF_HOME/bin:$PATH"
JAVA="$KOF_HOME/jdk/bin/java"
JAVAC="$KOF_HOME/jdk/bin/javac"

"$ZIG" build -Doptimize=ReleaseFast -Dlib_only=true
if [[ ! -f "$ROOT/tests/formats/sales.parquet" ]]; then
  "$ZIG" build -Doptimize=ReleaseFast fixtures
fi

# JNI .so must sit on an executable filesystem (not the NTFS HD). /tmp is tmpfs.
JNI_SO="${GLACIER_KOF_JNI:-/tmp/libglacier_kof.so}"
"$ZIG" cc -shared -fPIC \
  "$ROOT/bindings/kof/jni/glacier_jni.c" \
  -I "$ROOT/include" \
  -I "$KOF_HOME/jdk/include" \
  -I "$KOF_HOME/jdk/include/linux" \
  -L "$ROOT/zig-out/lib" \
  -lglacier \
  -Wl,-z,origin \
  -Wl,-rpath,"\$ORIGIN" \
  -Wl,-rpath,"$ROOT/zig-out/lib" \
  -o "$JNI_SO"

cp -f "$ROOT/tests/formats/sales.parquet" "$HD/sales.parquet"
export GLACIER_KOF_PARQUET="$HD/sales.parquet"
export GLACIER_KOF_JNI="$JNI_SO"
export LD_LIBRARY_PATH="$ROOT/zig-out/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

cp -f "$ROOT/bindings/kof/Conn.kf" "$HD/suite/Conn.kf"
# Kof cannot read process env; bake the HD parquet path into the suite copy.
sed "s|__GLACIER_KOF_PARQUET__|$HD/sales.parquet|" \
  "$ROOT/bindings/kof/test_query.kf" > "$HD/suite/test_query.kf"

kof build "$HD/suite" --target jvm --output "$HD/out"
"$JAVAC" -d "$HD/out" "$ROOT/bindings/kof/java/Conn.java"

"$JAVA" -Djava.library.path="$(dirname "$JNI_SO"):$ROOT/zig-out/lib" \
  -cp "$HD/out" Default.Main
