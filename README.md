![Logo](img/GlacierLogo.png)

# <div align="center">Glacier — Pure Zig OLAP Query Engine</div>

<div align="center">

[![Zig Version](https://img.shields.io/badge/zig-0.15.2-orange.svg)](https://ziglang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)]()
[![Version](https://img.shields.io/badge/version-0.8.5--alpha-blue.svg)]()

</div>

Pure Zig OLAP query engine for Apache Iceberg, Parquet, and Avro.

**Status: alpha.** Local file queries work. Remote storage (S3, HTTP) is not implemented yet. Moving slowly. Contributions welcome.

---

## What works

- SQL: `SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`, `OFFSET`, aggregations (`COUNT`, `AVG`, `SUM`, `MIN`, `MAX`)
- Parquet: PLAIN, RLE_DICTIONARY, PLAIN_DICTIONARY encodings; Snappy compression; all numeric types (INT32, INT64, FLOAT, DOUBLE, BOOLEAN, BYTE_ARRAY)
- Iceberg V2: metadata parsing, manifest reading, predicate pushdown, multi-file aggregation
- Avro: schema parsing, SELECT and WHERE — aggregations not yet wired
- Interactive REPL with 4 connection modes

## What doesn't work yet

- Avro aggregations (COUNT/AVG/SUM/GROUP BY)
- Multi-column ORDER BY (only first key used)
- HAVING, DISTINCT, subqueries, JOINs, window functions
- Remote storage (S3, Azure Blob, GCS, HTTP)
- ZSTD/GZIP compression
- ORC, Delta Lake

---

## Build

Requires Zig 0.15.2. No other dependencies — SDL3/FreeType/etc are not needed, everything is compiled from source.

```bash
git clone <repo-url>
cd Glacier

zig build                          # debug
zig build -Doptimize=ReleaseFast   # optimized
```

Output: `zig-out/bin/glacier`

---

## REPL

```bash
zig build repl
```

Four connection modes:

**1 — Iceberg table (directory)**
```
Enter option (1-4): 1
Enter path: iceberg_test_json_random

glacier(iceberg_test_json_random)> SELECT category, COUNT(*), AVG(price) GROUP BY category;
glacier(iceberg_test_json_random)> SELECT * WHERE price > 100 ORDER BY price DESC LIMIT 10;
```

**2 — Parquet file**
```
Enter option (1-4): 2
Enter path: data/products.parquet

glacier(products)> SELECT * LIMIT 5;
glacier(products)> SELECT category, AVG(price) GROUP BY category;
```

**3 — Avro file**
```
Enter option (1-4): 3
Enter path: test_employees.avro

glacier(default)> SELECT name, department WHERE age > 30 LIMIT 10;
```

**4 — FROM clause (ad-hoc)**
```
Enter option (1-4): 4

glacier> SELECT * FROM employees.parquet WHERE age > 30;
glacier> SELECT COUNT(*) FROM sales.parquet;
```

---

## SQL coverage

| Feature | Parquet | Iceberg | Avro |
|---------|---------|---------|------|
| SELECT * / columns | ✓ | ✓ | ✓ |
| WHERE (=, !=, <, >, AND, OR) | ✓ | ✓ | ✓ |
| COUNT / AVG / SUM / MIN / MAX | ✓ | ✓ | — |
| GROUP BY | ✓ | ✓ | — |
| ORDER BY | ✓ | ✓ | ✓ |
| LIMIT / OFFSET | ✓ | ✓ | ✓ |

---

## Architecture

```
SQL parser  →  expression evaluator  →  aggregate engine
                       │
         ┌─────────────┼─────────────┐
      Iceberg        Parquet        Avro
    (metadata +    (Thrift +      (binary
     manifests)    RLE/dict/       schema)
                   Snappy)
```

All layers use arena allocation. No GC pauses. Static binary (~8 MB).

---

## Project layout

```
glacier_repl.zig           REPL entrypoint

src/
  core/
    memory.zig             arena allocators
    errors.zig             error types
  formats/
    parquet.zig            Parquet reader
    thrift.zig             Thrift Compact Protocol parser
    encoding.zig           RLE + bit-packing
    value_decoder.zig      type-specific decoders
    compression.zig        Snappy decompressor
  table/
    iceberg.zig            Iceberg V2 metadata
    avro.zig               Avro manifest parser
    avro_reader.zig        Avro data reader
    avro_types.zig
    avro_parser.zig
  sql/
    parser.zig             SQL parser
  execution/
    batch.zig              columnar batch
    expr.zig               expression evaluation
    projection.zig         column projection

examples/
  parquet_dump.zig         Parquet inspector

test_data/
  test_employees.avro
  iceberg_test_json_random/
  create_test_avro.py
```

---

## Roadmap

- [ ] Avro aggregations
- [ ] Multi-column ORDER BY
- [ ] HAVING, DISTINCT
- [ ] JOINs
- [ ] Subqueries
- [ ] Window functions
- [ ] ZSTD / GZIP compression
- [ ] S3 / HTTP remote storage
- [ ] ORC, Delta Lake
- [ ] Parallel execution
- [ ] SIMD vectorized filters

---

## License

MIT
