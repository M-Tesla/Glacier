#!/usr/bin/env python3
"""Optional pyarrow corpus for Glacier 2.2.

Unit tests write the same shapes with carquet and do not require pyarrow.
Run this when pyarrow is available to drop files under tests/formats/:

    python3 tools/gen_pyarrow_corpus.py
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    sys.stderr.write("pyarrow is not installed; Glacier tests use carquet writers instead.\n")
    sys.exit(0)

ROOT = Path(__file__).resolve().parents[1] / "tests" / "formats"
ROOT.mkdir(parents=True, exist_ok=True)


def write_types() -> None:
    table = pa.table(
        {
            "flag": pa.array([True, False, True], type=pa.bool_()),
            "i32": pa.array([1, 2, 3], type=pa.int32()),
            "i64": pa.array([10, 20, 30], type=pa.int64()),
            "f32": pa.array([1.5, 2.5, 3.5], type=pa.float32()),
            "f64": pa.array([1.25, 2.25, 3.25], type=pa.float64()),
            "name": pa.array(["a", "b", "c"], type=pa.string()),
        }
    )
    pq.write_table(table, ROOT / "pyarrow_types.parquet", compression=None, use_dictionary=True)


def write_codecs() -> None:
    table = pa.table({"id": pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    for codec in ("snappy", "gzip", "lz4", "zstd"):
        pq.write_table(table, ROOT / f"pyarrow_i64_{codec}.parquet", compression=codec)


def write_row_groups() -> None:
    table = pa.table({"id": pa.array([1, 2, 3, 4, 5, 6], type=pa.int64())})
    pq.write_table(table, ROOT / "pyarrow_row_groups.parquet", row_group_size=3, compression=None)


def write_nested() -> None:
    table = pa.table({"nums": pa.array([[1, 2, 3]], type=pa.list_(pa.int32()))})
    pq.write_table(table, ROOT / "pyarrow_nested.parquet", compression=None)


def main() -> None:
    write_types()
    write_codecs()
    write_row_groups()
    write_nested()
    print(f"wrote pyarrow corpus under {ROOT}")


if __name__ == "__main__":
    main()
