# Glacier for Kof (JVM)

`.kf` API plus a Java/JNI class on `glacier.h`. Kof cannot load a `.so` by itself.
`Conn.kf` is what the compiler sees. At run time that class is the Java `Conn` that
calls `libglacier.so`. Target is JVM only.

```bash
export ZIG="$HOME/opt/zig-0.16/zig"
export KOF_HOME=/path/to/kof-*-linux-x86_64   # official Kof distro (embedded JDK)
# optional: GLACIER_TEMP and test parquet on a disk that is not the workspace SSD
bindings/kof/run_tests.sh
```

```kof
var c = Conn.connectEmpty()
assert(c.firstInt("SELECT 1") == 1)
c.close()

var sales = Conn.connectPath("sales.parquet")
assert(sales.firstInt("SELECT COUNT(*)") == 10)
sales.close()
```

`connectEmpty` / `connectPath` are separate names because Kof mis-resolves overloads.
Do not use `kof run` / `kof test` against this tree: they recompile `Conn.kf` and
replace the JNI class. `run_tests.sh` builds, overwrites `Conn.class`, and runs
`Default.Main` with Kof's embedded JDK.

`ArrayList.size()` from Kof is the wrong descriptor; use `rowCount` / `firstInt`.
