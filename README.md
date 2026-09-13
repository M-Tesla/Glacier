![Logo](img/GlacierLogo.png)

# <div align="center">Glacier</div>

<div align="center">

[![Zig Version](https://img.shields.io/badge/zig-0.16.0-orange.svg)](https://ziglang.org/)
[![CI](https://github.com/M-Tesla/Glacier/actions/workflows/ci.yml/badge.svg)](https://github.com/M-Tesla/Glacier/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-0.2-blue.svg)]()
[![Version](https://img.shields.io/badge/version-0.2.0-blue.svg)]()

</div>

Glacier is an OLAP engine. You open an Iceberg table, a Parquet file or an Avro file. You can run locally, over `https://`, or on `s3://` and run SQL. Results come back as a columnar batch (Arrow C Data on the public ABI).

The query engine is Zig 0.16. Parquet, Avro, and in-memory Arrow are small C libraries (carquet, libavro, nanoarrow), not a from-scratch codec stack.

**0.2.0** is the version that matches this description: analysis SQL on an Iceberg table, a Parquet file, or an Avro file. It was built and tested on **Linux**. The commands in this README are bash (`export`, `$ZIG`, `./zig-out/bin/glacier`, `libglacier.so`). They are not a Windows playbook; cmd/PowerShell, `.exe` / `.dll`, and paths will differ, and that path was not used here.

CI in this repo: `zig build test` on Linux and macOS; on Windows only `zig build` (compile, no test suite). Python tests and manylinux wheels (`manylinux_2_28` x86_64 and aarch64) run on Ubuntu. WASM on Ubuntu.

---

## Update v0.2

What 0.2 adds on top of 0.1 (the SQL surface that 0.1 froze and refused):

- **JOIN.** `LEFT` / `RIGHT` / `FULL JOIN … ON` (equalities, including `AND` of equalities). Null keys do not match. LEFT keeps every left row.
- **Predicates and scalars.** `LIKE`, `IN` (literals or uncorrelated `SELECT`), `BETWEEN`, `CASE WHEN … THEN … ELSE … END`, `SELECT NULL`.
- **Set and subquery.** `UNION` / `UNION ALL` (same schema), `FROM (SELECT …)`, `WITH t AS (SELECT …)`, scalar subquery in `WHERE`.
- **Window.** `ROWS` / `RANGE` frames (`UNBOUNDED PRECEDING` / `FOLLOWING`, `CURRENT ROW`, `N PRECEDING` / `FOLLOWING`), `LAG` / `LEAD`, window after `GROUP BY`.
- **Iceberg.** Position and equality deletes on scan. Partition prune for `bucket[N]` / `year` / `month` / `day` / `hour` / `truncate[W]` / `void`, not only file bounds. Snapshot `schema-id` may differ from `current-schema-id` (promote int32→int64 / float32→float64; incompatible types error; new optional columns are null).
- **Parquet nested.** A LIST of primitives is utf8 (`[1, 2, 3]`). Flat STRUCT leaves are columns. Maps and nested lists stay rejected.

Linux is still the tested path. `glacier.api_version()` is still `1`.

---

## What 0.2 does

**SQL.** `SELECT` (including `SELECT 1` / `SELECT NULL` with no table), `DISTINCT`, `WHERE` (`AND` / `OR`, comparisons, `IS [NOT] NULL`, `LIKE`, `IN (literals or uncorrelated SELECT)`, `BETWEEN`, scalar subquery), `GROUP BY` / `HAVING`, `ORDER BY` (several columns; nulls last on `ASC`), `LIMIT` / `OFFSET`, `UNION` / `UNION ALL` (same schema), `FROM (SELECT …)`, `WITH t AS (SELECT …)`. Aggregates: `COUNT` / `SUM` / `AVG` / `MIN` / `MAX` (`COUNT(*)` counts every row; the others skip nulls). `INNER` / `LEFT` / `RIGHT` / `FULL JOIN … ON` (equalities, including `AND` of equalities; null keys do not match; LEFT keeps every left row). Window: `COUNT` / `SUM` / `AVG` / `MIN` / `MAX` / `ROW_NUMBER` / `RANK` / `DENSE_RANK` / `LAG` / `LEAD` with `OVER ([PARTITION BY …] [ORDER BY …] [ROWS|RANGE …])`. Frames: `UNBOUNDED PRECEDING` / `FOLLOWING`, `CURRENT ROW`, `N PRECEDING` / `FOLLOWING`; `RANGE` offsets need a single numeric `ORDER BY`. Window after `GROUP BY` is allowed. Scalars: `abs`, `round`, `cast`, `coalesce`, `CASE WHEN … THEN … ELSE … END`. `COPY TO` writes a native `.glacier` file you can open again.

**Data.** Parquet (Snappy, GZIP, LZ4, ZSTD), Avro (null / deflate / snappy), Iceberg (`metadata.json` + Avro manifests). Iceberg prune uses column bounds and partition values (`identity`, `bucket[N]`, `year` / `month` / `day` / `hour`, `truncate[W]`, `void`; unknown transform is still rejected). Position and equality deletes (`content` 1 / 2) are applied on scan. Schema evolution: a snapshot `schema-id` may differ from `current-schema-id`; int32 promotes to int64 and float32 to float64; incompatible types error; new optional columns are null. Optional Parquet columns are real SQL nulls. A LIST of primitives is utf8 (`[1, 2, 3]`, `["a"]`). Flat STRUCT leaves are ordinary columns. Maps and nested lists (`max_rep_level > 1`) are rejected.

**Access.** Local path, `http(s)://` Range GET, `s3://` (SigV4; `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` or `~/.aws/credentials`; `AWS_ENDPOINT_URL` for path-style). Large scans stream; `ORDER BY` / `GROUP BY` / `DISTINCT` can spill under `GLACIER_MEM` (default 256 MiB) into `$GLACIER_TEMP`.

**How you call it.** CLI `glacier` (TUI on a Linux terminal, or `-c SQL`). Shared library `libglacier.so` on Linux + [`include/glacier.h`](include/glacier.h). Bindings on that header: Python (`pip install glacier-olap`, or `bindings/python` after a local `$ZIG build`), Kof JVM (`bindings/kof`), WASM (`$ZIG build wasm`), Node, Go, Rust.

---

## What 0.2 does not do

`USING` / `NATURAL` / comma-join, correlated subqueries, recursive `WITH`, `EXISTS`, subquery in the select list, named `WINDOW` clause, `GROUPS` frames. Azure Blob and GCS. ZSTD inside the WASM build (Snappy / GZIP / LZ4 work). A tested Windows (or native cmd) flow.

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
pip install glacier-olap
```

The PyPI name is `glacier-olap` because `glacier` is taken. `import glacier` is unchanged. The wheel includes `libglacier.so`; you do not need Zig to run queries. CI builds `manylinux_2_28` wheels; a GitHub Release publishes them to PyPI. Until that upload exists, install a CI artifact or the source checkout below.

From a source checkout:

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

## WASM, Node, Go, Rust, Kof (Linux)

Same `glacier.h`. Build the shared library first (`$ZIG build -Doptimize=ReleaseFast`). WASM takes parquet bytes in memory (`bindings/wasm/example.html`); there is no filesystem in that build. The commands below are Linux.

```bash
$ZIG build wasm && node bindings/wasm/smoke.mjs
cd bindings/node && node-gyp rebuild && node test.mjs
cd bindings/go && go test
cd bindings/rust && cargo test
KOF_HOME=/path/to/kof-*-linux-x86_64 bindings/kof/run_tests.sh
```

---

## What to expect next

The Windows test suite. Azure / GCS and parallel scan are not on that board.

Issues and CI live in this repo. The engine version is `0.2.0`; `glacier.api_version()` is `1`.

---

## License

MIT
