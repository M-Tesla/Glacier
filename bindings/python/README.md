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
con.execute("SELECT * FROM glacier.catalogs").fetchall()
# glacier.connect("http://127.0.0.1:8181")  # Iceberg REST; CREATE/INSERT (commitTable; PUT to s3:// or gs://)
# glacier.connect_catalog(uri, warehouse="lake", token="…")
# ATTACH an empty dir, then CREATE TABLE / INSERT / COPY FROM / DELETE / UPDATE / MERGE / ALTER (0.4 Iceberg commit)
# CLI 0.5: glacier serve /warehouse --listen 127.0.0.1:8181 --flight 127.0.0.1:8815
# Iceberg REST catalog + Arrow Flight SQL query port; not in the wheel

con.read_parquet(Path("tests/formats/sales.parquet").read_bytes()).fetchall()
table = con.execute("SELECT *").arrow()  # pyarrow
df = con.execute("SELECT *").df()        # pandas via pyarrow
```
