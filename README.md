![Logo](img/GlacierLogo.png)

# <div align="center">Glacier</div>

<div align="center">

[![Zig Version](https://img.shields.io/badge/zig-0.16.0-orange.svg)](https://ziglang.org/)
[![CI](https://github.com/M-Tesla/Glacier/actions/workflows/ci.yml/badge.svg)](https://github.com/M-Tesla/Glacier/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-0.1-blue.svg)]()
[![Version](https://img.shields.io/badge/version-0.1.0-blue.svg)]()

</div>

Glacier is an OLAP engine. You open an Iceberg table, a Parquet file, or an Avro file — locally, over `https://`, or on `s3://` — and run SQL. Results come back as a columnar batch (Arrow C Data on the public ABI).

The query engine is Zig 0.16. Parquet, Avro, and in-memory Arrow are small C libraries (carquet, libavro, nanoarrow), not a from-scratch codec stack.

**0.1.0** is the first version that matches this description. It was built and tested on **Linux**. The commands in this README are bash (`export`, `$ZIG`, `./zig-out/bin/glacier`, `libglacier.so`). They are not a Windows playbook; cmd/PowerShell, `.exe` / `.dll`, and paths will differ, and that path was not used here.

CI in this repo: `zig build test` on Linux and macOS; on Windows only `zig build` (compile, no test suite). The Python and WASM jobs run on Ubuntu.

---

## What 0.1 does

**SQL.** `SELECT` (including `SELECT 1` with no table), `DISTINCT`, `WHERE` (`AND` / `OR`, comparisons, `IS [NOT] NULL`), `GROUP BY` / `HAVING`, `ORDER BY` (several columns; nulls last on `ASC`), `LIMIT` / `OFFSET`. Aggregates: `COUNT` / `SUM` / `AVG` / `MIN` / `MAX` (`COUNT(*)` counts every row; the others skip nulls). `INNER JOIN … ON` (equalities, including `AND` of equalities). Window: `COUNT` / `SUM` / `AVG` / `MIN` / `MAX` / `ROW_NUMBER` / `RANK` / `DENSE_RANK` with `OVER ([PARTITION BY …] [ORDER BY …])` — no `ROWS` / `RANGE` frame. Scalars: `abs`, `round`, `cast`, `coalesce`. `COPY TO` writes a native `.glacier` file you can open again.

**Data.** Parquet (Snappy, GZIP, LZ4, ZSTD), Avro (null / deflate / snappy), Iceberg (`metadata.json` + Avro manifests, prune by column bounds). Optional Parquet columns are real SQL nulls. Nested lists/structs are rejected.

**Access.** Local path, `http(s)://` Range GET, `s3://` (SigV4; `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` or `~/.aws/credentials`; `AWS_ENDPOINT_URL` for path-style). Large scans stream; `ORDER BY` / `GROUP BY` / `DISTINCT` can spill under `GLACIER_MEM` (default 256 MiB) into `$GLACIER_TEMP`.

**How you call it.** CLI `glacier` (TUI on a Linux terminal, or `-c SQL`). Shared library `libglacier.so` on Linux + [`include/glacier.h`](include/glacier.h). Bindings on that header: Python (`bindings/python`), WASM (`$ZIG build wasm`), Node, Go, Rust.

---

## What 0.1 does not do

LEFT / RIGHT / FULL JOIN, `USING` / `NATURAL` / comma-join, subqueries, CTEs, window frames, `LAG` / `LEAD`, `SELECT NULL` as a bare literal. Azure Blob and GCS. ZSTD inside the WASM build (Snappy / GZIP / LZ4 work). A manylinux / PyPI wheel — `pip install bindings/python` after a local `$ZIG build` on Linux does. A tested Windows (or native cmd) flow.

---

## Build (Linux)

Zig **0.16.0**. These are Linux shell commands.

```bash
export ZIG="$HOME/opt/zig-0.16/zig"   # or wherever 0.16 lives
$ZIG build
$ZIG build test
$ZIG build fixtures                  # tests/formats + tests/iceberg_prune
$ZIG build wasm                      # zig-out/bin/glacier.wasm
```

Output on Linux: `zig-out/bin/glacier`, `zig-out/lib/libglacier.so`, `zig-out/bin/glacier.wasm`.

```bash
./zig-out/bin/glacier                              # TUI: pick a source, then SQL
./zig-out/bin/glacier tests/formats/sales.parquet  # SQL on that file
./zig-out/bin/glacier tests/formats/sales.parquet -c 'SELECT category, COUNT(*) GROUP BY category'
./zig-out/bin/glacier -c 'SELECT 1'
```

---

## Python (Linux)

```bash
$ZIG build -Doptimize=ReleaseFast
python3 -m pip install bindings/python
```

```python
import glacier

con = glacier.connect("tests/formats/sales.parquet")
con.execute("SELECT category, COUNT(*) AS n GROUP BY category").fetchall()

n = glacier.connect("tests/formats/nulls.parquet")  # $ZIG build fixtures
n.execute("SELECT id WHERE qty IS NULL").fetchall()
```

`connect()` with no path is an empty session (`SELECT 1`). `.arrow()` / `.df()` need pyarrow (and pandas for `.df()`).

---

## WASM, Node, Go, Rust (Linux)

Same `glacier.h`. Build the shared library first (`$ZIG build -Doptimize=ReleaseFast`). WASM takes parquet bytes in memory (`bindings/wasm/example.html`); there is no filesystem in that build. The commands below are Linux.

```bash
$ZIG build wasm && node bindings/wasm/smoke.mjs
cd bindings/node && node-gyp rebuild && node test.mjs
cd bindings/go && go test
cd bindings/rust && cargo test
```

---

## What to expect next

0.1 stays on this SQL surface until the next minor. The line after that is LEFT JOIN (nulls are already in the batch), then window frames, then a manylinux wheel so `pip install glacier` does not need Zig on the machine. Running the test suite on Windows (today CI only compiles there) and documenting those commands comes with that. Azure / GCS and richer SQL (subquery, CTE) come after that. Parallel scan and extra formats are not on the 0.1 board.

Issues and CI live in this repo. The engine version is `0.1.0`; `glacier.api_version()` is `1`.

---

## License

MIT
