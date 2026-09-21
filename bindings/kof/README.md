# Glacier for Kof (JVM)

`.kf` API plus a Java/JNI class on `glacier.h`. Tested on **Kof 0.4.9-beta**.
Kof `extern` is scalar FFI (ints, floats); Arrow C Data is not that, so this
binding stays JVM JNI on `libglacier.so`. `Conn.kf` is what the compiler sees.
At run time that class is the Java `Conn`. Target is JVM only.

```bash
export ZIG="$HOME/opt/zig-0.16/zig"
export KOF_HOME=/path/to/kof-0.4.9-beta-linux-x86_64   # official Kof distro (embedded JDK)
# optional: GLACIER_TEMP and test parquet on a disk that is not the workspace SSD
bindings/kof/run_tests.sh
```

```kof
var c = Conn.connect()
assert(c.firstInt("SELECT 1") == 1)
c.close()

var sales = Conn.connect("sales.parquet")
assert(sales.firstInt("SELECT COUNT(*)") == 10)
sales.close()
```

`connect()` / `connect(path)` are overloads (Kof 0.4). `connectEmpty` /
`connectPath` still exist. Do not use `kof run` / `kof test` against this tree:
they recompile `Conn.kf` and replace the JNI class. `run_tests.sh` builds, overwrites `Conn.class`, and runs `Default.Main` with Kof's
embedded JDK (`--enable-native-access` for JDK 25 `System.load`).
