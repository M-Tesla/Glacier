"""Glacier Python API. DuckDB-shaped: connect / execute / fetchall / arrow / df."""

from __future__ import annotations

import ctypes
from pathlib import Path
from typing import Any, Optional, Union

from . import _native

__version__ = "0.2.1"


class GlacierError(Exception):
    pass


def version() -> str:
    lib = _native.load()
    return lib.glacier_version().decode("utf-8")


def api_version() -> int:
    return int(_native.load().glacier_api_version())


def connect(path: Union[str, Path, None] = None) -> "Connection":
    """Open a catalog session.

    ``None`` is empty (``SELECT 1``, then ``ATTACH``). A parquet / avro /
    ``.glacier`` / Iceberg dir / Hadoop warehouse is the default catalog.
    An ``http(s)://`` URI that is not a data file is an Iceberg REST Catalog
    (bearer from ``ICEBERG_TOKEN``). Token and warehouse without env:
    :func:`connect_catalog`.
    """
    lib = _native.load()
    err = ctypes.c_char_p()
    if path is None:
        db = lib.glacier_open(None, ctypes.byref(err))
    else:
        db = lib.glacier_open(str(path).encode("utf-8"), ctypes.byref(err))
    return _finish_open(lib, db, err)


def connect_catalog(
    uri: str,
    warehouse: str = "",
    token: Optional[str] = None,
) -> "Connection":
    """Open an Iceberg REST Catalog (URI, optional warehouse, optional bearer)."""
    lib = _native.load()
    err = ctypes.c_char_p()
    db = lib.glacier_open_catalog(
        uri.encode("utf-8"),
        warehouse.encode("utf-8") if warehouse else None,
        token.encode("utf-8") if token else None,
        ctypes.byref(err),
    )
    return _finish_open(lib, db, err)


def _finish_open(lib, db, err: ctypes.c_char_p) -> "Connection":
    if not db:
        msg = err.value.decode("utf-8") if err.value else "open failed"
        if err:
            lib.glacier_free(err)
        raise GlacierError(msg)
    conn = lib.glacier_connect(db, ctypes.byref(err))
    if not conn:
        msg = err.value.decode("utf-8") if err.value else "connect failed"
        if err:
            lib.glacier_free(err)
        lib.glacier_close(db)
        raise GlacierError(msg)
    return Connection(lib, db, conn)


class Connection:
    def __init__(self, lib, db, conn) -> None:
        self._lib = lib
        self._db = db
        self._conn = conn

    def execute(self, sql: str) -> "Result":
        self._check()
        err = ctypes.c_char_p()
        handle = self._lib.glacier_query(
            self._conn, sql.encode("utf-8"), ctypes.byref(err)
        )
        if not handle:
            msg = err.value.decode("utf-8") if err.value else "query failed"
            if err:
                self._lib.glacier_free(err)
            raise GlacierError(msg)
        result = Result(self._lib, handle)
        if result.error:
            msg = result.error
            result.close()
            raise GlacierError(msg)
        return result

    def read_parquet(self, buf: Union[bytes, bytearray, memoryview, str, Path]) -> "Result":
        """Attach a parquet (bytes or path) and `SELECT *`."""
        self._check()
        if isinstance(buf, (str, Path)):
            replacement = connect(buf)
            self.close()
            self._lib = replacement._lib
            self._db = replacement._db
            self._conn = replacement._conn
            replacement._db = None
            replacement._conn = None
            return self.execute("SELECT *")

        data = bytes(buf)
        err = ctypes.c_char_p()
        raw = ctypes.create_string_buffer(data)
        db = self._lib.glacier_open_buffer(raw, len(data), ctypes.byref(err))
        if not db:
            msg = err.value.decode("utf-8") if err.value else "open buffer failed"
            if err:
                self._lib.glacier_free(err)
            raise GlacierError(msg)
        conn = self._lib.glacier_connect(db, ctypes.byref(err))
        if not conn:
            msg = err.value.decode("utf-8") if err.value else "connect failed"
            if err:
                self._lib.glacier_free(err)
            self._lib.glacier_close(db)
            raise GlacierError(msg)
        self.close()
        self._db = db
        self._conn = conn
        return self.execute("SELECT *")

    def close(self) -> None:
        if self._db:
            self._lib.glacier_disconnect(self._conn)
            self._lib.glacier_close(self._db)
            self._db = None
            self._conn = None

    def _check(self) -> None:
        if not self._db:
            raise GlacierError("connection is closed")

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass


class Result:
    def __init__(self, lib, handle) -> None:
        self._lib = lib
        self._handle = handle
        self._array: Optional[_native.ArrowArray] = None
        self._schema: Optional[_native.ArrowSchema] = None
        self._rows: Optional[list[tuple]] = None
        self._names: Optional[list[str]] = None
        self._cursor = 0
        self._pyarrow_table = None
        err = lib.glacier_result_error(handle)
        self.error = err.decode("utf-8") if err else None

    def _ensure_arrow(self) -> None:
        if self._array is not None:
            return
        self._check()
        array = _native.ArrowArray()
        schema = _native.ArrowSchema()
        rc = self._lib.glacier_result_arrow(
            self._handle, ctypes.byref(array), ctypes.byref(schema)
        )
        if rc != 0:
            raise GlacierError("failed to export Arrow")
        self._array = array
        self._schema = schema

    def _materialize(self) -> None:
        if self._rows is not None:
            return
        if self._pyarrow_table is not None:
            self._names = list(self._pyarrow_table.column_names)
            self._rows = [tuple(row.values()) for row in self._pyarrow_table.to_pylist()]
            return
        self._ensure_arrow()
        self._names, self._rows = _native.rows_from_arrow(self._schema, self._array)

    def fetchall(self) -> list[tuple]:
        self._materialize()
        rows = self._rows[self._cursor :]
        self._cursor = len(self._rows)
        return rows

    def fetchone(self) -> Optional[tuple]:
        self._materialize()
        if self._cursor >= len(self._rows):
            return None
        row = self._rows[self._cursor]
        self._cursor += 1
        return row

    def arrow(self):
        try:
            import pyarrow as pa
        except ImportError as e:
            raise ImportError("pyarrow is required for .arrow()") from e
        if self._pyarrow_table is not None:
            return self._pyarrow_table
        self._ensure_arrow()
        batch = pa.RecordBatch._import_from_c(
            ctypes.addressof(self._array),
            ctypes.addressof(self._schema),
        )
        self._pyarrow_table = pa.Table.from_batches([batch])
        self._array = None
        self._schema = None
        return self._pyarrow_table

    def df(self):
        return self.arrow().to_pandas()

    def close(self) -> None:
        if self._array is not None:
            _native._release_array(self._array)
            self._array = None
        if self._schema is not None:
            _native._release_schema(self._schema)
            self._schema = None
        if self._handle:
            self._lib.glacier_result_destroy(self._handle)
            self._handle = None

    def _check(self) -> None:
        if not self._handle:
            raise GlacierError("result is closed")

    def __enter__(self) -> "Result":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
