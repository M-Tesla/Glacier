# glacier (Python)

`import glacier`. On Linux the manylinux wheel ships `libglacier.so`; you do not need Zig to use it. The PyPI name is `glacier-olap` (`glacier` is already taken).

```bash
pip install glacier-olap
python -c "import glacier; print(glacier.connect().execute('select 1').fetchall())"
```

From a source checkout, build the shared library first:

```bash
export ZIG="$HOME/opt/zig-0.16/zig"
$ZIG build -Doptimize=ReleaseFast -Dlib_only=true
pip install bindings/python
```

`tools/build_wheel.sh` writes `dist/glacier_olap-*.whl` on this machine (host glibc). CI runs `bindings/python/ci_manylinux.sh` for `manylinux_2_28`.

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
