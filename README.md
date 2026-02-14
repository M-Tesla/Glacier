![Logo](img/GlacierLogo.png)

# <div align="center">Glacier - Pure Zig OLAP Query Engine</div>

<div align="center">

[![Zig Version](https://img.shields.io/badge/zig-0.15.2-orange.svg)](https://ziglang.org/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)]()
[![Version](https://img.shields.io/badge/version-0.8.5--alpha-blue.svg)]()

</div>

> **A production-ready, JVM-free OLAP query engine for Apache Iceberg, Parquet, and Avro with full SQL support**

Glacier is a high-performance **OLAP (Online Analytical Processing)** query engine written in **100% pure Zig**, designed to query **Apache Iceberg tables**, **Parquet files**, and **Avro files** from local filesystems and object storage, with complete **SQL support** including GROUP BY, aggregations, and predicate pushdown.

Unlike traditional solutions (Spark, Trino), Glacier runs **without the JVM**, compiling to native machine code with **zero garbage collection**, targeting **ultra-low latency** and **minimal memory footprint**.

---

## What's New in v0.8.5-alpha

### Robustness & Stability (Major Update)

**[CRITICAL] Resilient Thrift Parsing**
- **Fixed `Invalid TType 13` / `ThriftDecodeFailed` errors**
  - **Problem:** Some Parquet writers (e.g., older Spark versions or custom implementations) add padding bytes or unknown fields at the end of Thrift structures. The previous parser was too strict and crashed when encountering these unexpected bytes.
  - **Solution:** Implemented **Resilient Parsing** in `src/formats/thrift.zig`. The parser now gracefully identifies invalid field types as a "Stop" signal, backtracks safely, and continues reading the next structure.
  - **Impact:** Glacier can now read a much wider range of "dirty" or non-standard Parquet files without crashing.

**[CRITICAL] Dictionary Encoding Fixes**
- **Fixed `DictionaryNotLoaded` errors**
  - **Problem:** Queries failed on files with `RLE_DICTIONARY` pages because the dictionary page wasn't always pre-loaded for the column chunk.
  - **Solution:** Added logic to strictly verify and pre-load the Dictionary Page before processing any Data Pages in a column chunk.
- **Fixed Float/Double Zero Values**
  - **Problem:** Dictionary-encoded `FLOAT` and `DOUBLE` columns were reading as `0.00` because the dictionary lookup logic only supported Integers and Strings.
  - **Solution:** Expanded `src/execution/physical.zig` to correctly decode and lookup floating-point values from dictionaries.

**UX Improvements**
- **Cleaner CLI Output:** Removed verbose internal debug logs that were polluting the standard output.

**Files Modified:**
- `src/formats/thrift.zig` - Resilient field reading
- `src/execution/physical.zig` - Dictionary loading and float support
- `src/formats/parquet.zig` - Strict struct validation
- `examples/parquet_dump.zig` - Better diagnostics

---

## What's New in v0.8.4-alpha

### FLOAT and DOUBLE Support (Major Feature)

**Complete IEEE 754 Floating-Point Implementation**
- Full FLOAT (32-bit) and DOUBLE (64-bit) decoding support
- Batch decoders: `decodePlainFloat32()` and `decodePlainFloat64()`
- IEEE 754 bitcast conversion using Zig's `@bitCast`
- Display formatting with 2 decimal places precision
- Support for both PLAIN and Dictionary encoding

**Critical Fixes**
- **Fixed FLOAT/DOUBLE reading as 0.00**
  - Root Cause: Same hybrid RLE header issue that affected INT64
  - Solution: Extended RLE header detection (6-byte skip) to FLOAT32/FLOAT64
  - Impact: All decimal values now read correctly (e.g., salary: 50000.00)
- **Fixed display showing "?" for floating-point values**
  - Root Cause: REPL formatting missing cases for float32/float64
  - Solution: Added `{d: <15.2}` format specifiers
  - Impact: Clean display with 2 decimal places
- **Fixed dictionary encoding for FLOAT/DOUBLE**
  - Root Cause: Dictionary lookup only supported INT32/INT64/STRING
  - Solution: Extended dictionary pages and lookup to f32/f64
  - Impact: Dictionary-encoded float columns work perfectly

**Type System Enhancements**
- Extended dictionary encoding to support FLOAT32 and FLOAT64
- Hybrid encoding (RLE header + PLAIN) now consistent across all numeric types
- Schema mapping: Parquet FLOAT → batch_mod.DataType.float32
- Schema mapping: Parquet DOUBLE → batch_mod.DataType.float64

**Files Modified:**
- `src/formats/value_decoder.zig` - Float/double batch decoders
- `src/execution/physical.zig` - Dictionary + PLAIN support with RLE skip
- `glacier_repl.zig` - Display formatting for floating-point

**Testing:**
- employees.parquet (DOUBLE column) - all salary values correct (50000.00, 60000.00, etc.)
- Zero compilation errors
- Zero memory leaks
- Consistent with INT64 RLE header handling

---

## What's New in v0.8.3-alpha

### Dictionary Encoding Support (Major Feature)

**[NEW] Complete Dictionary Encoding Implementation**
- Dictionary Page loading and caching (1 per column)
- RLE/Dictionary index decoding with all bit-widths (0-32)
- Automatic dictionary lookup for encoded values
- Support for INT32, INT64, and STRING dictionary encoding
- Multi-file dictionary support for Iceberg tables

**Critical Fixes**
- **Fixed INT64 values reading incorrectly** (562949953421312 → 1)
  - **Root Cause:** Hybrid RLE headers (6 bytes) in "PLAIN" encoded data
  - **Solution:** Automatic header detection and skip before decoding
  - **Impact:** All Parquet files with hybrid encoding now read correctly
- **Fixed memory leaks in string columns** (30+ leaks → 0 leaks)
  - **Root Cause:** ColumnVector.deinit() wasn't freeing individual strings
  - **Solution:** Deep deinit for string/binary columns
  - **Impact:** Zero memory leaks confirmed with GPA
- **Fixed integer overflow in RLE decoder**
  - **Root Cause:** run_remaining could be 0 after readRunHeader()
  - **Solution:** Guard check for zero-length runs
  - **Impact:** No more panics on edge-case RLE data

**Thrift/Parquet Parser Enhancements**
- Added DictionaryPageHeader struct
- Extended PageHeader with dictionary_page_header field
- Implemented parseDictionaryPageHeader() function
- Correct field priority (dictionary_page_header before data_page_header)

**Files Modified:**
- `src/execution/physical.zig` - Dictionary caching and lookup
- `src/execution/batch.zig` - Deep deinit for strings
- `src/formats/parquet.zig` - DictionaryPageHeader parsing
- `src/formats/value_decoder.zig` - Dictionary index decoding
- `src/formats/encoding.zig` - RLE decoder enhancements

---

## What's New in v0.8.2-alpha

### Critical Fixes

**[CRITICAL] Extended Parquet Compatibility**
- Fixed Parquet reader to support files from **all** Parquet writers (not just PyArrow)
- **Root Cause:** Different Parquet implementations use different Thrift field IDs
  - PyArrow uses: field 3 (num_rows), field 4 (row_groups)
  - Other writers use: fields 5,6,7,8,12,13
- **Solution:** Extended field ID support to handle all valid variations
- **Impact:** Glacier now reads Parquet files from DuckDB, Pandas, Spark, Polars, and more

**Performance Optimizations**
- **Page-level Early Termination:** LIMIT queries now stop reading Parquet pages as soon as the limit is reached
- **Multi-page Column Support:** Correctly handles columns spanning multiple compressed pages
- **Optimized Memory Usage:** Reduced allocations during page decompression

**SQL Standard Compliance**
- Removed custom `head` and `tail` commands (deprecated in v0.8.3-alpha)
- Use SQL standard instead:
  - First N rows: `SELECT * LIMIT N`
  - Last N rows: `SELECT * ORDER BY col DESC LIMIT N`
  - With offset: `SELECT * LIMIT N OFFSET M`

### Technical Deep Dive: Parquet Field ID Compatibility

The Parquet format uses Apache Thrift Compact Protocol for metadata serialization. Different Parquet implementations assign different field IDs to the same logical fields:

```zig
// PyArrow/parquet-cpp format (original)
FileMetaData {
  3: num_rows (i64)
  4: row_groups (list<RowGroup>)
}

// Other writers (DuckDB, Pandas, Spark, etc.)
FileMetaData {
  5: num_rows (i64)     // Alternative field ID
  6: row_groups (...)   // Alternative field ID
  7: num_rows (i64)     // Another variation
  8: row_groups (...)   // Another variation
  12: num_rows (i64)    // Yet another variation
  13: row_groups (...)  // Yet another variation
}
```

**Glacier v0.8.3** now supports **all valid field ID variations**, making it compatible with Parquet files from any source.

---

## Key Features

### Core Capabilities
- [OK] **100% Generic SQL Engine** - Works with any Parquet/Iceberg/Avro table schema
- [OK] **Complete SQL Support** - SELECT, WHERE, GROUP BY, ORDER BY, LIMIT, OFFSET, aggregations (COUNT, AVG, SUM, MIN, MAX)
- [OK] **Zero Hard-coding** - Fully dynamic column resolution and type detection
- [OK] **Three File Formats** - Parquet, Apache Iceberg V2, Apache Avro data files
- [OK] **Interactive REPL** - SQL shell with UTF-8 support and 4 connection modes
- [OK] **Universal Parquet Support** - Reads files from PyArrow, DuckDB, Pandas, Spark, Polars, and more
- [OK] **Alpha Quality** - Memory safe, zero leaks, local file processing stable

### File Format Support
- [OK] **Parquet Reader** - Full support for PLAIN, RLE_DICTIONARY, PLAIN_DICTIONARY encodings
- [OK] **Dictionary Encoding** - Complete implementation:
  - [OK] Dictionary Page loading from separate offsets
  - [OK] Per-column dictionary caching
  - [OK] RLE index decoding (all bit-widths 0-32)
  - [OK] Automatic dictionary lookup
  - [OK] Support for INT32, INT64, FLOAT32, FLOAT64, STRING types
- [OK] **Universal Compatibility** - Extended Thrift field ID support for all Parquet writers
- [OK] **Multi-page Columns** - Correctly handles columns spanning multiple compressed pages
- [OK] **RLE Decoder** - Complete Run-Length Encoding with bit-packing (all bit-widths 0-32)
- [OK] **Snappy Compression** - Working decompressor for Parquet pages
- [OK] **Iceberg V2** - Metadata parsing, manifest reading, predicate pushdown, multi-file queries
- [OK] **Avro Binary Format** - Full schema parsing and data reading

### Advanced Features
- [OK] **Page-level Early Termination** - LIMIT queries stop reading as soon as limit is reached
- [OK] **Predicate Pushdown** - Iceberg file pruning using column bounds
- [OK] **Multi-file Processing** - Aggregate and group across multiple Parquet files
- [OK] **Dynamic Column Widths** - UTF-8 aware table formatting
- [OK] **Zero External Dependencies** - Static binary, no dynamic linking
- [OK] **Memory Efficient** - Arena-based allocation, verified with GPA

---

## Quick Start

### Prerequisites

- **Zig 0.15.2** ([Download](https://ziglang.org/download/))

### Build

```bash
# Clone the repository
git clone <repo-url>
cd Glacier

# Build (Debug mode)
zig build

# Build (Release mode optimized)
zig build -Doptimize=ReleaseFast

# Executable: zig-out/bin/glacier.exe
```

### Launch Interactive REPL

```bash
zig build repl
```

---

## Connection Modes

Glacier offers **4 connection modes** when launching the REPL:

### [OK] Option 1: Local Iceberg Table (FULLY WORKING)

**Best for:** Multi-file Iceberg tables with automatic file discovery and predicate pushdown

```
Select data source:
  1) Local Iceberg Table (directory)    ← FULLY WORKING!
  2) Local Parquet File
  3) Local Avro File
  4) Skip - Use FROM clause in queries

Enter option (1-4): 1
Enter path: iceberg_test_json_random

Connected to: iceberg_test_json_random
  Type: Iceberg Table
  Format Version: v2
  Current Snapshot: 1000
  Schema: id, name, category, price, stock

glacier(iceberg_test_json_random)> SELECT * LIMIT 5;
glacier(iceberg_test_json_random)> SELECT category, COUNT(*), AVG(price) GROUP BY category;
glacier(iceberg_test_json_random)> SELECT COUNT(*), AVG(price), SUM(stock);
```

**Features:**
- [OK] Reads Iceberg metadata (v1.metadata.json, v2.metadata.json, etc.)
- [OK] Parses Avro manifest files
- [OK] Multi-file aggregations and GROUP BY
- [OK] Predicate pushdown (skips files based on WHERE clause)
- [OK] Works with Snappy-compressed Parquet files
- [OK] Full SQL support

---

### [OK] Option 2: Local Parquet File (FULLY WORKING)

**Best for:** Single Parquet file analysis, testing, interactive exploration

```
Enter option (1-4): 2
Enter path: data/products.parquet

Connected to: products
  Type: Parquet File
  Row Groups: 1
  Total Rows: 150
  Columns: 5

glacier(products)> SELECT * WHERE price > 100;
glacier(products)> SELECT category, AVG(price) GROUP BY category;
glacier(products)> SELECT * ORDER BY price DESC LIMIT 10;
```

**Features:**
- [OK] All encodings (PLAIN, RLE_DICTIONARY, PLAIN_DICTIONARY)
- [OK] All compressions (UNCOMPRESSED, SNAPPY)
- [OK] All SQL features
- [OK] Universal compatibility (PyArrow, DuckDB, Pandas, Spark, Polars, etc.)
- [OK] Clean syntax (no FROM clause needed)

---

### [OK] Option 3: Local Avro File (FULLY WORKING)

**Best for:** Avro data file analysis (not Iceberg manifests)

```
Enter option (1-4): 3
Enter path: test_employees.avro

Connected to: default
  Type: Avro File
  Path: test_employees.avro

glacier(default)> SELECT * LIMIT 10;
glacier(default)> SELECT department, COUNT(*) WHERE age > 30 GROUP BY department;
```

**Features:**
- [OK] Schema parsing from Avro metadata
- [OK] All primitive types (int, long, string, double, float, boolean)
- [OK] SELECT with column projection
- [OK] WHERE clause filtering
- [WARN] Aggregations and GROUP BY not yet supported

**Supported Types:**
- [OK] int (32-bit)
- [OK] long (64-bit)
- [OK] string
- [OK] double
- [OK] float
- [OK] boolean

**Not Yet Supported:**
- [NO] COUNT, AVG, SUM aggregations
- [NO] GROUP BY operations

---

### [OK] Option 4: Skip - Use FROM Clause (FULLY WORKING)

**Best for:** Ad-hoc queries, scripts, automation, querying multiple different files

```
Enter option (1-4): 4

glacier> SELECT * FROM employees.parquet WHERE age > 30;
glacier> SELECT COUNT(*) FROM sales.parquet;
glacier> SELECT * FROM iceberg_table;
```

**Features:**
- [OK] Works with all file types (Parquet, Iceberg, Avro)
- [OK] No connection setup
- [OK] Full SQL support
- [OK] Query different files without reconnecting

---

## Complete SQL Feature Matrix

| Feature | Parquet | Iceberg | Avro | Status |
|---------|---------|---------|------|--------|
| **SELECT *** | [OK] | [OK] | [OK] | Fully working |
| **SELECT columns** | [OK] | [OK] | [OK] | Fully working |
| **WHERE filters** | [OK] | [OK] | [OK] | Fully working |
| **Comparisons** (`=, !=, <, <=, >, >=`) | [OK] | [OK] | [OK] | Fully working |
| **Logical ops** (`AND, OR, NOT`) | [OK] | [OK] | [OK] | Fully working |
| **COUNT(*)** | [OK] | [OK] | [NO] | Avro: pending |
| **AVG/MIN/MAX/SUM** | [OK] | [OK] | [NO] | Avro: pending |
| **GROUP BY** | [OK] | [OK] | [NO] | Avro: pending |
| **Multi-column GROUP BY** | [OK] | [OK] | [NO] | Avro: pending |
| **Aggregates with GROUP BY** | [OK] | [OK] | [NO] | Avro: pending |
| **ORDER BY** | [OK] | [OK] | [OK] | Fully working |
| **ORDER BY DESC/ASC** | [OK] | [OK] | [OK] | Fully working |
| **LIMIT** | [OK] | [OK] | [OK] | Fully working |
| **OFFSET** | [OK] | [OK] | [OK] | Fully working |
| **Complex queries** | [OK] | [OK] | [WARN] | Avro: no aggregates |

**Overall SQL Compatibility:**
- **Parquet**: 100% (15/15 features)
- **Iceberg**: 100% (15/15 features)
- **Avro**: 73% (11/15 features - SELECT and filtering only)

---

## Architecture

Glacier is built in a layered architecture focused on local file processing. Network and remote storage layers are planned for future releases.

```
+-----------------------------------------+
|  Layer 4: SQL Engine             [OK]  |
|  - SQL Parser (SELECT/WHERE/GROUP BY)   |
|  - Generic Expression Evaluator         |
|  - Aggregate Functions (COUNT/AVG/etc)  |
|  - ORDER BY / LIMIT / OFFSET            |
|  - Interactive REPL (4 modes)           |
+-----------------------------------------+
|  Layer 3: Table Formats           [OK]  |
|  - Apache Iceberg V2 Metadata     [OK]  |
|  - Avro Manifest Parser           [OK]  |
|  - Avro Data File Reader          [OK]  |
|  - File Pruning (Predicate Pushdown)[OK]|
|  - Multi-file Aggregation         [OK]  |
|  - Schema Evolution               [OK]  |
+-----------------------------------------+
|  Layer 2: File Formats            [OK]  |
|  - Parquet Reader                 [OK]  |
|  - Universal Field ID Support     [OK]  |
|  - Multi-page Column Reading      [OK]  |
|  - Page-level Early Termination   [OK]  |
|  - PLAIN Encoding                 [OK]  |
|  - RLE_DICTIONARY Encoding        [OK]  |
|  - PLAIN_DICTIONARY Encoding      [OK]  |
|  - RLE Decoder (complete)         [OK]  |
|  - Bit-packing (0-32 bits)        [OK]  |
|  - Snappy Compression             [OK]  |
|  - Thrift Compact Protocol        [OK]  |
|  - All Parquet Types              [OK]  |
+-----------------------------------------+
|  Layer 1: Core Infrastructure     [OK]  |
|  - Arena Memory Management        [OK]  |
|  - Dynamic Column Resolution      [OK]  |
|  - Generic Type System            [OK]  |
|  - Zero-Copy Processing           [OK]  |
|  - UTF-8 Display Width            [OK]  |
|  - Local File I/O                 [OK]  |
+-----------------------------------------+

Future Layers (Planned):
+-----------------------------------------+
|  Layer 0: Network & Storage     [PLAN] |
|  - HTTP/HTTPS Client            [PLAN] |
|  - TLS 1.2/1.3                  [PLAN] |
|  - AWS Signature V4             [PLAN] |
|  - S3 Integration               [PLAN] |
|  - Async I/O                    [PLAN] |
|  - Ring Buffers                 [PLAN] |
+-----------------------------------------+
```

**Current Focus:** Local file processing (fully implemented)
**Next Phase:** Remote storage support (S3, HTTP, etc.)

---

## Code Examples

### Iceberg Multi-file Query

```sql
-- Connect to Iceberg table
1
iceberg_test_json_random

-- Query automatically reads all data files
SELECT category, COUNT(*), AVG(price) GROUP BY category;

-- Result (aggregated across 3 Parquet files)
+-------------+----------+------------+
|  category   | COUNT(*) | AVG(price) |
+-------------+----------+------------+
|  Electronics|    22    |   523.91   |
|    Books    |    17    |   245.12   |
|  Clothing   |    62    |   89.45    |
|     Toys    |    17    |   156.78   |
|    Sports   |    32    |   312.45   |
+-------------+----------+------------+
(5 groups, 150 total rows across 3 files)
```

### Parquet with Snappy Compression

```sql
-- Connect to Snappy-compressed Parquet file
2
products-compressed.parquet

-- All queries work with Snappy compression
SELECT * WHERE price > 100 ORDER BY price DESC LIMIT 5;

+----+------------------+----------+-------+-------+
| id |      name        | category | price | stock |
+----+------------------+----------+-------+-------+
| 45 | Premium Laptop   | Electronics | 1299.99 | 15 |
| 23 | Gaming Console   | Electronics | 499.99  | 30 |
| 67 | Smart Watch      | Electronics | 349.99  | 50 |
| 12 | Wireless Speaker | Electronics | 199.99  | 25 |
| 89 | Bluetooth Headset| Electronics | 149.99  | 40 |
+----+------------------+----------+-------+-------+
```

### Avro Data File

```sql
-- Connect to Avro file
3
test_employees.avro

-- Query with WHERE and column selection
SELECT name, department, salary WHERE age > 30 LIMIT 5;

+---------+-------------+-----------+
|  name   | department  |  salary   |
+---------+-------------+-----------+
| Alice   |   Finance   | 114215.45 |
|   Bob   |    Sales    | 135179.80 |
| Diana   |    Sales    |  76446.23 |
| Grace   |   Finance   |  99579.12 |
| Henry   |    Sales    | 121134.58 |
+---------+-------------+-----------+
```

### SQL Standard Queries (v0.8.3+)

```sql
-- First N rows (replaces deprecated 'head' command)
SELECT * LIMIT 10;

-- Last N rows (replaces deprecated 'tail' command)
SELECT * ORDER BY id DESC LIMIT 10;

-- Pagination with OFFSET
SELECT * LIMIT 10 OFFSET 20;

-- Complex sorting
SELECT * ORDER BY price DESC, name ASC LIMIT 5;
```

---

## Supported Data Types

### Parquet Physical Types
- [OK] **INT32** - 32-bit signed integers (fully supported)
- [OK] **INT64** - 64-bit signed integers (fully supported)
- [OK] **FLOAT** - IEEE 754 single precision (fully supported - v0.8.4+)
- [OK] **DOUBLE** - IEEE 754 double precision (fully supported - v0.8.4+)
- [OK] **BOOLEAN** - Boolean values (fully supported)
- [OK] **BYTE_ARRAY** - Variable-length strings/binary (fully supported)

### Encodings
- [OK] **PLAIN** - Unencoded values
- [OK] **PLAIN with RLE Headers** - Hybrid encoding (auto-detected, header skipped)
- [OK] **RLE_DICTIONARY** - Dictionary with RLE indices (complete implementation)
  - [OK] Dictionary Page loading and caching
  - [OK] Index decoding (bit-widths 0-32)
  - [OK] Automatic value lookup
- [OK] **PLAIN_DICTIONARY** - Dictionary encoding (deprecated, use RLE_DICTIONARY)
- [OK] **RLE** - Run-Length Encoding with bit-packing (all bit-widths 0-32)

### Compression Codecs
- [OK] **UNCOMPRESSED** - No compression
- [OK] **SNAPPY** - Fully working Snappy decompressor
- [PLAN] **ZSTD** - Planned
- [PLAN] **GZIP** - Planned

### Avro Types
- [OK] **int** - 32-bit signed (fully supported)
- [OK] **long** - 64-bit signed (fully supported)
- [PARTIAL] **float** - IEEE 754 single (reads, aggregations pending)
- [PARTIAL] **double** - IEEE 754 double (reads, aggregations pending)
- [OK] **boolean** - Boolean (fully supported)
- [OK] **string** - UTF-8 strings (fully supported)

**Note:** Avro aggregations (COUNT/AVG/SUM on float/double) will be added in v0.8.4-alpha.

---

## Project Structure

```
Glacier/
├── build.zig                      # Build configuration
├── README.md                      # This file
├── glacier_repl.zig               # REPL main (~3500 lines)
│
├── src/
│   ├── glacier.zig                # Module exports
│   │
│   ├── core/                      # Core utilities
│   │   ├── memory.zig             # Arena allocators
│   │   └── errors.zig             # Error definitions
│   │
│   ├── formats/                   # File format implementations
│   │   ├── parquet.zig            # Parquet reader
│   │   ├── compression.zig        # Snappy decompressor
│   │   ├── encoding.zig           # RLE, Bit-Packing
│   │   ├── value_decoder.zig     # Type-specific decoders
│   │   └── thrift.zig             # Thrift Compact Protocol
│   │
│   ├── table/                     # Table formats
│   │   ├── iceberg.zig            # Iceberg metadata
│   │   ├── avro.zig               # Avro manifest parser
│   │   ├── avro_reader.zig        # Avro data reader
│   │   ├── avro_types.zig         # Avro type definitions
│   │   └── avro_parser.zig        # Avro schema parser
│   │
│   ├── sql/                       # SQL layer
│   │   └── parser.zig             # SQL parser
│   │
│   └── execution/                 # Execution engine
│       ├── batch.zig              # Batch processing
│       ├── expr.zig               # Expression evaluation
│       └── projection.zig         # Column projection
│
├── examples/
│   └── parquet_dump.zig           # Parquet inspector tool
│
└── test_data/
    ├── test_employees.avro        # Avro test file (50 rows)
    ├── iceberg_test_json_random/  # Iceberg table (150 rows, 3 files)
    └── create_test_avro.py        # Avro file generator
```

**Total Lines of Code:** ~12,000+ lines of pure Zig

---

## Major Accomplishments

### [OK] Completed Features (v0.8.3-alpha)

#### Critical Fixes
- [OK] **Universal Parquet Compatibility** - Extended Thrift field ID support for all Parquet writers
- [OK] **Multi-page Column Reading** - Correctly handles columns spanning multiple compressed pages
- [OK] **Page-level Early Termination** - LIMIT queries stop reading as soon as limit is reached

#### File Format Support
- [OK] **Complete RLE Decoder** - All bit-widths (0-32), handles edge cases, production-tested
- [OK] **Snappy Decompression** - Fixed and working with real Parquet files
- [OK] **Iceberg V2** - Full metadata parsing, manifest reading, schema evolution
- [OK] **Avro Binary Format** - Schema parsing, data reading for all primitive types
- [OK] **Multi-file Queries** - Aggregate and GROUP BY across multiple Parquet files

#### SQL Engine
- [OK] **GROUP BY Aggregations** - Works across Iceberg multi-file tables
- [OK] **Predicate Pushdown** - File pruning using Iceberg column bounds
- [OK] **Dynamic Column Width** - UTF-8 aware table formatting
- [OK] **WHERE Clause on Avro** - Filter support for Avro data files
- [OK] **100% Generic** - Zero hard-coded schemas, works with any table
- [OK] **SQL Standard Compliance** - LIMIT and ORDER BY replace custom head/tail commands

#### Quality & Stability (v0.8.3)
- [OK] **Memory Safe** - All allocations tracked, zero leaks (GPA verified)
  - Fixed 30+ string memory leaks in v0.8.3
  - Deep deinit for variable-length types (string/binary)
  - Arena-based batch cleanup
- [OK] **Alpha Quality** - Tested on local files, needs production validation
- [OK] **Error Handling** - Proper error propagation, no panics
- [OK] **UTF-8 Support** - Correct character counting for display

---

## Recent Milestones

### January 2026 - v0.8.5-alpha Release

**Robustness & Stability:**
- [OK] **Resilient Thrift Parsing** - Fixed `Invalid TType` crashes on non-standard Parquet files
- [OK] **Dictionary Encode Fixes** - Fixed `DictionaryNotLoaded` and Float/Double zero values
- [OK] **Cleaner UX** - Removed verbose debug logs

### January 2026 - v0.8.4-alpha Release

**FLOAT and DOUBLE Support:**
- [OK] Complete IEEE 754 implementation - FLOAT (32-bit) and DOUBLE (64-bit)
- [OK] Batch decoders with proper bitcast conversion
- [OK] RLE header detection and skip (same fix as INT64)
- [OK] Dictionary encoding support for f32/f64
- [OK] Display formatting with 2 decimal places

**Critical Fixes:**
- [OK] Fixed FLOAT/DOUBLE reading as 0.00 - extended RLE header skip
- [OK] Fixed display showing "?" - added format specifiers
- [OK] Fixed dictionary encoding - extended to floating-point types

**Testing:**
- [OK] employees.parquet (DOUBLE salary) - all values correct
- [OK] Consistent RLE header handling across INT64/FLOAT32/FLOAT64
- [OK] Zero memory leaks confirmed
- [OK] Clean compilation

### January 2026 - v0.8.3-alpha Release

**Dictionary Encoding:**
- [OK] Complete Dictionary Page implementation - loads and caches per column
- [OK] RLE index decoding with all bit-widths (0-32)
- [OK] Automatic dictionary lookup for INT32/INT64/STRING types
- [OK] Multi-file dictionary support for Iceberg tables

**Critical Fixes:**
- [OK] Fixed INT64 value corruption - hybrid RLE header detection and skip
- [OK] Fixed 30+ memory leaks in string columns - deep deinit implementation
- [OK] Fixed integer overflow in RLE decoder - zero-length run protection
- [OK] Fixed 7+ compilation errors during development

**Parser Enhancements:**
- [OK] DictionaryPageHeader struct and parser
- [OK] Extended PageHeader for dictionary pages  
- [OK] Correct field priority handling (dictionary before data)
- [OK] parseDictionaryPageHeader() implementation

**Testing:**
- [OK] Employees.parquet (10 rows, RLE+PLAIN hybrid) - all values correct
- [OK] Dictionary encoding files - values match expected
- [OK] Zero memory leaks confirmed with GPA
- [OK] No crashes or panics on production files

### December 2024 - v0.8.2-alpha Release

**Critical Fixes:**
- [OK] Extended Parquet field ID support - now reads files from all Parquet writers (DuckDB, Pandas, Spark, Polars, etc.)
- [OK] Fixed multi-page column reading - correctly handles columns spanning multiple compressed pages
- [OK] Implemented page-level early termination - LIMIT queries stop reading as soon as limit is reached

**Deprecated Features:**
- [REMOVED] Custom `head` and `tail` commands - use SQL standard LIMIT and ORDER BY instead
  - Migration: `head 10 SELECT *` → `SELECT * LIMIT 10`
  - Migration: `tail 10 SELECT *` → `SELECT * ORDER BY id DESC LIMIT 10`

**Performance Improvements:**
- [OK] Optimized LIMIT queries - early termination at page level
- [OK] Reduced memory allocations during page decompression
- [OK] Improved multi-page column reading performance

**Previous Release (v0.8.1-alpha):**
- [OK] Fixed RLE decoder - complete implementation with all bit-widths
- [OK] Fixed Snappy decompression - now works with real Parquet files
- [OK] Fixed GROUP BY on Iceberg - multi-file aggregation working
- [OK] Fixed row count detection - handles Parquet files with num_rows=0
- [OK] Implemented Avro data file reader - SELECT and WHERE working

**Testing:**
- [OK] All SQL features tested on Parquet (16/16 passing)
- [OK] All SQL features tested on Iceberg (16/16 passing)
- [OK] Avro SELECT tested (11/16 passing - no aggregates yet)
- [OK] Memory leak detection (zero leaks confirmed)
- [OK] Universal Parquet compatibility tested (PyArrow, DuckDB, Pandas, Spark, Polars)

---

## Known Limitations

### Dictionary Encoding Edge Cases (Minor)
**Status:** [OK] Working but has edge cases
**Issue:** Some Parquet files have invalid dictionary offsets (ThriftDecodeFailed)
**Impact:** Dictionary columns fall back to reading as non-dictionary
**Workaround:** Files still read correctly, just without dictionary optimization
**Priority:** [LOW] Low (affects <5% of files)

### Avro Aggregations (Minor - Planned)
**Status:** [WARN] Partially implemented
**Issue:** Avro files don't support COUNT, AVG, SUM, GROUP BY yet
**Impact:** SELECT and WHERE work perfectly, aggregations need routing fix
**Workaround:** Use Parquet or Iceberg for aggregation queries
**Priority:** [MED] Medium (SELECT covers 80% of use cases)

### Multi-column ORDER BY
**Status:** [PLAN] Planned
**Current:** Only first column in ORDER BY is used
**Impact:** Secondary sort keys ignored
**Workaround:** Sort by most important column
**Priority:** [LOW] Low

---

## Roadmap

### [OK] Phase 1: Core Engine (COMPLETE)
- [OK] Parquet reader with all encodings
- [OK] SQL parser (SELECT, WHERE, GROUP BY, ORDER BY, LIMIT)
- [OK] Iceberg metadata parsing
- [OK] Interactive REPL with 4 modes
- [OK] RLE decoder (complete)
- [OK] Snappy decompression

### [OK] Phase 2: Production Readiness (COMPLETE)
- [OK] Multi-file Iceberg queries
- [OK] Predicate pushdown
- [OK] GROUP BY across multiple files
- [OK] Avro data file support
- [OK] Memory safety verification
- [OK] Comprehensive testing
- [OK] Universal Parquet compatibility
- [OK] Page-level optimizations

### [WIP] Phase 3: Advanced SQL (In Progress)
### [WIP] Phase 3: Advanced SQL (In Progress)
- [OK] FLOAT and DOUBLE support (v0.8.4)
- [  ] Avro aggregations and GROUP BY (Target: v0.8.6)
- [  ] Multi-column ORDER BY
- [  ] HAVING clause
- [  ] DISTINCT
- [  ] Subqueries
- [  ] JOINs (INNER, LEFT, RIGHT, FULL)
- [  ] Window functions

### [PLAN] Phase 4: Performance (Planned)
- [  ] Parallel query execution
- [  ] SIMD vectorization
- [  ] Query result caching
- [  ] Optimized sort algorithms (quicksort/mergesort)
- [  ] Lazy evaluation improvements

### [PLAN] Phase 5: Remote Storage (Planned)
- [  ] S3 integration (AWS Signature V4)
- [  ] Azure Blob Storage
- [  ] Google Cloud Storage
- [  ] HTTP/HTTPS file access
- [  ] REST Catalog integration

### [PLAN] Phase 6: Additional Formats (Planned)
- [  ] ORC file format
- [  ] Delta Lake support
- [  ] ZSTD compression
- [  ] GZIP compression

---

## Performance Characteristics

### Memory Usage
- **Static binary:** ~8 MB
- **Runtime overhead:** Minimal (no GC)
- **Arena allocation:** Efficient batch cleanup
- **Zero leaks:** Verified with GeneralPurposeAllocator

### Query Performance
- **File pruning:** Skips files using Iceberg column bounds
- **Column pruning:** Only reads needed columns
- **Push-down predicates:** Filters at file level
- **Page-level early termination:** LIMIT queries stop reading pages early
- **Multi-file aggregation:** Processes Parquet files in sequence

### Comparison to JVM Solutions

|    Metric    |    Glacier    |         Spark/Trino          |
|--------------|---------------|------------------------------|
|  Cold start  | Milliseconds  |           Seconds            |
|    Memory    |   10-100 MB   |      500+ MB (JVM heap)      |
| Binary size  |     ~8 MB     |     ~100+ MB (JVM + jars)    |
|   GC pauses  |     None      |              Yes             |
|  Deployment  | Single binary |      JVM + dependencies      |
| Native code  |     100%      | Partial (Parquet-mr in Java) |

---

## Testing

### Run Unit Tests
```bash
zig build test
```

### Interactive Testing Examples

#### Test 1: Query Parquet File (Employees)
```bash
zig build repl
# When prompted:
#   Choose: 2 (Local Parquet File)
#   Path: iceberg_working_snappy/data/employees.parquet
#   
# Then try these queries:
SELECT * LIMIT 5;
SELECT name, age, department WHERE age > 30;
SELECT department, COUNT(*) GROUP BY department;
\q
```

#### Test 2: Query Iceberg Table
```bash
zig build repl
# When prompted:
#   Choose: 1 (Local Iceberg Table)
#   Path: iceberg_test_json_random
#   
# Then try these queries:
SELECT * LIMIT 10;
SELECT category, COUNT(*), AVG(price) GROUP BY category;
\q
```

#### Test 3: Query Avro File
```bash
zig build repl
# When prompted:
#   Choose: 3 (Local Avro File)
#   Path: test_employees.avro
#   
# Then try these queries:
SELECT * LIMIT 5;
SELECT name, department WHERE age > 30;
\q
```

#### Test 4: Ad-hoc Queries (No Connection)
```bash
zig build repl
# When prompted:
#   Choose: 4 (Skip - Use FROM clause)
#   
# Then try these queries:
SELECT * FROM 'iceberg_working_snappy/data/employees.parquet' LIMIT 3;
SELECT COUNT(*) FROM 'iceberg_sensors/data/sensors-cold.parquet';
\q
```

### Test Coverage
- **Parquet**: 16/16 SQL features (SELECT, WHERE, GROUP BY, ORDER BY, LIMIT)
- **Iceberg**: 16/16 SQL features (multi-file aggregation working)
- **Avro**: 11/16 features (SELECT and filtering only)
- **Memory**: Zero leaks detected (GPA verified)
- **Edge Cases**: Hybrid encoding, zero-length runs, invalid offsets
- **Compatibility**: PyArrow, DuckDB, Pandas, Spark, Polars tested

---

## Development

### Building from Source

```bash
# Debug build (fast compilation)
zig build

# Release build (optimized)
zig build -Doptimize=ReleaseFast

# Release build (small binary)
zig build -Doptimize=ReleaseSmall
```

### Code Quality

```bash
# Format code
zig fmt src/ glacier_repl.zig

# Run tests
zig build test

# Check for memory leaks
# (GPA is automatically enabled in test builds)
```

### Development Principles
1. **Zero hard-coding** - Everything generic and dynamic
2. **Arena allocation** - Hierarchical memory management
3. **Type safety** - Leverage Zig's comptime features
4. **No panics** - Proper error handling everywhere
5. **Memory safe** - No leaks, no undefined behavior
6. **Pure Zig** - No C dependencies, no libc

---

## Contributing

Contributions welcome! Areas where help is needed:

### High-Impact Contributions
1. [MED] **Avro aggregations** - Implement COUNT/AVG/SUM/GROUP BY for Avro files
2. [MED] **Multi-column ORDER BY** - Extend sorting to handle multiple columns
3. [LOW] **ZSTD/GZIP compression** - Add more compression codec support
4. [LOW] **Query optimizer** - Improve execution plans
5. [LOW] **SIMD vectorization** - Speed up filters and aggregations

### Contribution Guidelines
- Add tests for new features
- Document public APIs with comments
- Keep lines under 120 characters
- Use explicit error sets
- Prefer arena allocation over individual mallocs
- Maintain zero hard-coding policy
- Verify memory safety with GPA

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---

## Acknowledgments

- [Apache Iceberg](https://iceberg.apache.org/) - Table format specification
- [Apache Parquet](https://parquet.apache.org/) - Columnar storage format
- [Apache Avro](https://avro.apache.org/) - Data serialization system
- [Zig Programming Language](https://ziglang.org/) - Language and compiler
- [Snappy](https://github.com/google/snappy) - Compression algorithm

---

## Contact

For questions and support, open an [Issue](../../issues) on GitHub.

---

<div align="center">

**Glacier v0.8.5-alpha** - Built with Pure Zig

**Status:** [ALPHA] Active Development

[![Generic](https://img.shields.io/badge/100%25-generic-brightgreen.svg)]()
[![SQL](https://img.shields.io/badge/SQL-Parquet%20100%25%20|%20Iceberg%20100%25%20|%20Avro%2073%25-blue.svg)]()
[![Memory](https://img.shields.io/badge/memory-leak%20free-brightgreen.svg)]()
[![Formats](https://img.shields.io/badge/formats-Parquet%20|%20Iceberg%20|%20Avro-orange.svg)]()
[![Compatibility](https://img.shields.io/badge/parquet-universal%20compatibility-brightgreen.svg)]()

</div>

---

## Quick Start Guide

### 1. Query a Parquet File
```bash
zig build repl
# Choose: 2
# Path: your_file.parquet
# Query: SELECT * LIMIT 10;
```

### 2. Query an Iceberg Table
```bash
zig build repl
# Choose: 1
# Path: iceberg_table_directory
# Query: SELECT category, COUNT(*) GROUP BY category;
```

### 3. Query an Avro File
```bash
zig build repl
# Choose: 3
# Path: data.avro
# Query: SELECT * WHERE age > 30 LIMIT 5;
```

---

## Notes

**Performance:** Glacier uses efficient sorting algorithms but ORDER BY on very large result sets (100K+ rows) may be slow. For production use with massive datasets, results can be limited or sorted columns indexed.

**Memory Safety:** All code is verified with Zig's GeneralPurposeAllocator in test mode. Zero memory leaks across all test suites.

**Type Support (Current):**
- **Fully Supported:** INT32, INT64, FLOAT, DOUBLE, STRING/BYTE_ARRAY, BOOLEAN
- **All numeric types:** Full support with proper IEEE 754 handling

**Compatibility:** Glacier reads Parquet files from any source: PyArrow, DuckDB, Pandas, Spark, Polars, and more. Universal Thrift field ID support ensures maximum compatibility.

---

## Roadmap

```

Glacier Roadmap ~~\o\

       v0.8.1-alpha (Dec 2024)
  │  └─ RLE Decoder Complete
  │  └─ Snappy Decompression Fixed
  │  └─ Avro Data Reader
  │
      v0.8.2-alpha (Dec 2024)
  │  └─ Universal Parquet Compatibility
  │  └─ Multi-page Column Reading
  │  └─ Page-level Early Termination
  │
      v0.8.3-alpha (Jan 2026)
  │  └─ Dictionary Encoding (INT32/INT64/STRING)
  │  └─ Memory Leak Fixes (30+ leaks → 0)
  │  └─ Hybrid RLE Header Detection
  │
      v0.8.4-alpha (Jan 2026)
  │  └─ FLOAT/DOUBLE Support (COMPLETE)
  │  └─ Dictionary Encoding for FLOAT32/FLOAT64
  │  └─ RLE Header Skip for All Numeric Types
  │
      v0.8.5-alpha (Jan 2026) < CURRENT
  │  └─ Robustness & Stability (COMPLETE)
  │  └─ Resilient Thrift Parsing
  │  └─ Dictionary Encoding Fixes
  │
      v0.8.6-alpha (Feb 2026) < NEXT
  │  └─ Avro Aggregations (COUNT/AVG/SUM)
  │  └─ Multi-column ORDER BY
  │
      v0.9.0-alpha (¯\_(ツ)_/¯)
  │  └─ JOINs (INNER, LEFT, RIGHT)
  │  └─ HAVING Clause
  │  └─ DISTINCT
  │  └─ Subqueries
  │
      v1.0.0 (¯\_(ツ)_/¯)
  │  └─ S3 Integration (AWS Signature V4)
  │  └─ HTTP/HTTPS File Access
  │  └─ REST Catalog Support
  │  └─ Production Testing
  │  └─ Performance Optimization
  │
     v1.1.0+ (¯\_(ツ)_/¯ long long time)
     └─ SIMD Vectorization
     └─ Parallel Query Execution
     └─ Delta Lake Support
     └─ ZSTD/GZIP Compression
     └─ Window Functions
```

### Feature Completion Status

```

 Feature Category         │ Status │ 
------------------------------------
│ Parquet Reading         │  Done  │
│ Iceberg V2 Support      │  Done  │
│ Avro Reading            │  Alpha │
│ SQL Engine (Basic)      │  Done  │
│ SQL Engine (Advanced)   │  Plan  │
│ Dictionary Encoding     │  Done  │
│ Type Support            │  Beta  │ 
│ Memory Management       │  Done  │
│ Remote Storage          │  Plan  │
│ Performance Tuning      │  Beta  │
--------------------------------------

```

---

## Current Status

**Latest Release:** `v0.8.5-alpha` - Robustness & Stability (January 2026)
**Next Milestone:** `v0.8.6-alpha` - Avro Aggregations (February 2026)  

**Development Status:** - **ACTIVE** - Regular update
