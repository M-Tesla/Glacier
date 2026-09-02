# glacier (Python)

Same C ABI as `include/glacier.h`. Build the shared library with Zig 0.16, then install this package.

```bash
export ZIG="$HOME/opt/zig-0.16/zig"
$ZIG build -Doptimize=ReleaseFast
pip install -e bindings/python
python -c "import glacier; print(glacier.connect().execute('select 1').fetchall())"
```

`pip install` of a local wheel does **not** require Zig — the `.so` is inside the package. Building that wheel on this machine does.

```python
import glacier

con = glacier.connect()
con.execute("select 1").fetchall()  # [(1,)]

con = glacier.connect("tests/formats/sales.parquet")
con.execute("SELECT category, COUNT(*) AS n GROUP BY category").fetchall()

con.read_parquet(Path("tests/formats/sales.parquet").read_bytes()).fetchall()
table = con.execute("SELECT *").arrow()  # pyarrow
df = con.execute("SELECT *").df()        # pandas via pyarrow
```

JOIN is INNER only (`ON` column equality). LEFT / USING / NATURAL stay unsupported.
