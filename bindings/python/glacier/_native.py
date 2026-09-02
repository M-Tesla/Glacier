"""ctypes loader for libglacier. No pyo3, no zig-py."""

from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path

_LIB = None


class ArrowSchema(ctypes.Structure):
    pass


ArrowSchema._fields_ = [
    ("format", ctypes.c_char_p),
    ("name", ctypes.c_char_p),
    ("metadata", ctypes.c_char_p),
    ("flags", ctypes.c_int64),
    ("n_children", ctypes.c_int64),
    ("children", ctypes.POINTER(ctypes.POINTER(ArrowSchema))),
    ("dictionary", ctypes.POINTER(ArrowSchema)),
    ("release", ctypes.c_void_p),
    ("private_data", ctypes.c_void_p),
]


class ArrowArray(ctypes.Structure):
    pass


ArrowArray._fields_ = [
    ("length", ctypes.c_int64),
    ("null_count", ctypes.c_int64),
    ("offset", ctypes.c_int64),
    ("n_buffers", ctypes.c_int64),
    ("n_children", ctypes.c_int64),
    ("buffers", ctypes.POINTER(ctypes.c_void_p)),
    ("children", ctypes.POINTER(ctypes.POINTER(ArrowArray))),
    ("dictionary", ctypes.POINTER(ArrowArray)),
    ("release", ctypes.c_void_p),
    ("private_data", ctypes.c_void_p),
]


def _candidates() -> list[Path]:
    here = Path(__file__).resolve().parent
    names = []
    if sys.platform == "darwin":
        names = ["libglacier.dylib"]
    elif sys.platform == "win32":
        names = ["glacier.dll", "libglacier.dll"]
    else:
        names = ["libglacier.so"]

    out: list[Path] = []
    env = os.environ.get("GLACIER_LIB")
    if env:
        out.append(Path(env))
    for name in names:
        out.append(here / name)
        out.append(here.parent.parent.parent / "zig-out" / "lib" / name)
        out.append(Path.cwd() / "zig-out" / "lib" / name)
    return out


def load():
    global _LIB
    if _LIB is not None:
        return _LIB
    last = None
    for path in _candidates():
        if not path.is_file():
            continue
        try:
            lib = ctypes.CDLL(str(path))
        except OSError as e:
            last = e
            continue
        _bind(lib)
        _LIB = lib
        return lib
    raise FileNotFoundError(
        "libglacier not found. Build with `$ZIG build` or set GLACIER_LIB. "
        f"Last error: {last}"
    )


def _bind(lib) -> None:
    char_pp = ctypes.POINTER(ctypes.c_char_p)

    lib.glacier_open.argtypes = [ctypes.c_char_p, char_pp]
    lib.glacier_open.restype = ctypes.c_void_p

    lib.glacier_open_buffer.argtypes = [ctypes.c_void_p, ctypes.c_size_t, char_pp]
    lib.glacier_open_buffer.restype = ctypes.c_void_p

    lib.glacier_close.argtypes = [ctypes.c_void_p]
    lib.glacier_close.restype = None

    lib.glacier_connect.argtypes = [ctypes.c_void_p, char_pp]
    lib.glacier_connect.restype = ctypes.c_void_p

    lib.glacier_disconnect.argtypes = [ctypes.c_void_p]
    lib.glacier_disconnect.restype = None

    lib.glacier_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, char_pp]
    lib.glacier_query.restype = ctypes.c_void_p

    lib.glacier_result_destroy.argtypes = [ctypes.c_void_p]
    lib.glacier_result_destroy.restype = None

    lib.glacier_result_error.argtypes = [ctypes.c_void_p]
    lib.glacier_result_error.restype = ctypes.c_char_p

    lib.glacier_result_arrow.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ArrowArray),
        ctypes.POINTER(ArrowSchema),
    ]
    lib.glacier_result_arrow.restype = ctypes.c_int

    lib.glacier_free.argtypes = [ctypes.c_void_p]
    lib.glacier_free.restype = None

    lib.glacier_version.argtypes = []
    lib.glacier_version.restype = ctypes.c_char_p

    lib.glacier_api_version.argtypes = []
    lib.glacier_api_version.restype = ctypes.c_int


def _release_schema(schema: ArrowSchema) -> None:
    if schema.release:
        ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowSchema))(schema.release)(
            ctypes.byref(schema)
        )


def _release_array(array: ArrowArray) -> None:
    if array.release:
        ctypes.CFUNCTYPE(None, ctypes.POINTER(ArrowArray))(array.release)(
            ctypes.byref(array)
        )


def _c_str(p) -> str | None:
    if not p:
        return None
    if isinstance(p, bytes):
        return p.decode("utf-8")
    return ctypes.cast(p, ctypes.c_char_p).value.decode("utf-8")


def _bit_at(buf, i: int) -> int:
    return (buf[i >> 3] >> (i & 7)) & 1


def _valid(array: ArrowArray, i: int) -> bool:
    if array.null_count == 0 or array.n_buffers < 1 or not array.buffers[0]:
        return True
    validity = ctypes.cast(array.buffers[0], ctypes.POINTER(ctypes.c_uint8))
    return _bit_at(validity, i) != 0


def _col_values(schema: ArrowSchema, array: ArrowArray) -> list:
    fmt = _c_str(schema.format) or ""
    n = int(array.length)
    out: list = []
    if fmt == "b":
        bits = ctypes.cast(array.buffers[1], ctypes.POINTER(ctypes.c_uint8))
        for i in range(n):
            out.append(None if not _valid(array, i) else bool(_bit_at(bits, i)))
        return out
    if fmt == "i":
        data = ctypes.cast(array.buffers[1], ctypes.POINTER(ctypes.c_int32))
        for i in range(n):
            out.append(None if not _valid(array, i) else int(data[i]))
        return out
    if fmt == "l":
        data = ctypes.cast(array.buffers[1], ctypes.POINTER(ctypes.c_int64))
        for i in range(n):
            out.append(None if not _valid(array, i) else int(data[i]))
        return out
    if fmt == "f":
        data = ctypes.cast(array.buffers[1], ctypes.POINTER(ctypes.c_float))
        for i in range(n):
            out.append(None if not _valid(array, i) else float(data[i]))
        return out
    if fmt == "g":
        data = ctypes.cast(array.buffers[1], ctypes.POINTER(ctypes.c_double))
        for i in range(n):
            out.append(None if not _valid(array, i) else float(data[i]))
        return out
    if fmt == "u":
        offs = ctypes.cast(array.buffers[1], ctypes.POINTER(ctypes.c_int32))
        raw = ctypes.cast(array.buffers[2], ctypes.POINTER(ctypes.c_uint8))
        for i in range(n):
            if not _valid(array, i):
                out.append(None)
                continue
            start, end = int(offs[i]), int(offs[i + 1])
            out.append(bytes(raw[j] for j in range(start, end)).decode("utf-8"))
        return out
    if fmt.startswith(("tsu:", "tsm:", "tsn:")):
        data = ctypes.cast(array.buffers[1], ctypes.POINTER(ctypes.c_int64))
        for i in range(n):
            out.append(None if not _valid(array, i) else int(data[i]))
        return out
    if fmt.startswith("d:"):
        import decimal as _decimal

        parts = fmt[2:].split(",")
        scale = int(parts[1]) if len(parts) > 1 else 0
        data = ctypes.cast(array.buffers[1], ctypes.POINTER(ctypes.c_uint8))
        for i in range(n):
            if not _valid(array, i):
                out.append(None)
                continue
            raw = bytes(data[j] for j in range(i * 16, (i + 1) * 16))
            unscaled = int.from_bytes(raw, "little", signed=True)
            out.append(_decimal.Decimal(unscaled).scaleb(-scale))
        return out
    if fmt.startswith("w:"):
        import uuid as _uuid

        width = int(fmt[2:])
        data = ctypes.cast(array.buffers[1], ctypes.POINTER(ctypes.c_uint8))
        for i in range(n):
            if not _valid(array, i):
                out.append(None)
                continue
            raw = bytes(data[j] for j in range(i * width, (i + 1) * width))
            if width == 16:
                out.append(_uuid.UUID(bytes=raw))
            else:
                out.append(raw)
        return out
    raise TypeError(f"unsupported Arrow format {fmt!r}")


def rows_from_arrow(schema: ArrowSchema, array: ArrowArray) -> tuple[list[str], list[tuple]]:
    fmt = _c_str(schema.format) or ""
    if fmt != "+s":
        raise TypeError(f"expected struct Arrow batch, got {fmt!r}")
    n_cols = int(schema.n_children)
    names: list[str] = []
    cols: list[list] = []
    for i in range(n_cols):
        child_s = schema.children[i].contents
        child_a = array.children[i].contents
        names.append(_c_str(child_s.name) or f"_{i}")
        cols.append(_col_values(child_s, child_a))
    n = int(array.length)
    rows = [tuple(cols[c][r] for c in range(n_cols)) for r in range(n)]
    return names, rows
