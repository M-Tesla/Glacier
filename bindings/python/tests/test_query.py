import unittest
from pathlib import Path

import glacier

ROOT = Path(__file__).resolve().parents[3]
SALES = ROOT / "tests" / "formats" / "sales.parquet"
NULLS = ROOT / "tests" / "formats" / "nulls.parquet"


class TestQuery(unittest.TestCase):
    def test_select_1(self):
        con = glacier.connect()
        try:
            self.assertEqual(con.execute("select 1").fetchall(), [(1,)])
            self.assertEqual(con.execute("SELECT 1 AS x").fetchone(), (1,))
        finally:
            con.close()

    def test_version(self):
        self.assertEqual(glacier.version(), "0.1.0")
        self.assertEqual(glacier.api_version(), 1)

    def test_query_parquet(self):
        if not SALES.is_file():
            self.skipTest("run `$ZIG build fixtures`")
        with glacier.connect(SALES) as con:
            self.assertEqual(con.execute("SELECT COUNT(*)").fetchall(), [(10,)])
            self.assertEqual(con.execute("SELECT id ORDER BY id LIMIT 1").fetchone(), (1,))
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM sales a JOIN sales b ON a.id = b.id").fetchall(),
                [(10,)],
            )
            self.assertEqual(
                con.execute("SELECT COUNT(*) OVER ()").fetchall()[0],
                (10,),
            )

    def test_nulls(self):
        if not NULLS.is_file():
            self.skipTest("run `$ZIG build fixtures`")
        with glacier.connect(NULLS) as con:
            self.assertEqual(con.execute("SELECT COUNT(*), COUNT(qty)").fetchall(), [(4, 2)])
            self.assertEqual(
                con.execute("SELECT qty ORDER BY id").fetchall(),
                [(10,), (None,), (30,), (None,)],
            )
            self.assertEqual(
                con.execute("SELECT coalesce(qty, 0) ORDER BY id").fetchall(),
                [(10,), (0,), (30,), (0,)],
            )
            self.assertEqual(
                con.execute("SELECT id FROM nulls WHERE qty IS NULL ORDER BY id").fetchall(),
                [(2,), (4,)],
            )

    def test_read_parquet_bytes(self):
        if not SALES.is_file():
            self.skipTest("run `$ZIG build fixtures`")
        buf = SALES.read_bytes()
        with glacier.connect() as con:
            rows = con.read_parquet(buf).fetchall()
            self.assertEqual(len(rows), 10)

    def test_join_error(self):
        with glacier.connect() as con:
            with self.assertRaises(glacier.GlacierError) as ctx:
                con.execute("SELECT * FROM a JOIN b")
            self.assertIn("JOIN is not supported", str(ctx.exception))
            with self.assertRaises(glacier.GlacierError) as ctx:
                con.execute("SELECT * FROM a LEFT JOIN b ON a.id = b.id")
            self.assertIn("JOIN is not supported", str(ctx.exception))

    def test_logical_types(self):
        import decimal
        import uuid

        path = ROOT / "tests" / "formats" / "logical.parquet"
        if not path.is_file():
            self.skipTest("run `$ZIG build fixtures`")
        with glacier.connect(path) as con:
            rows = con.execute("SELECT * ORDER BY amount").fetchall()
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0][0], decimal.Decimal("0.50"))
            self.assertEqual(rows[1][0], decimal.Decimal("10.50"))
            self.assertEqual(rows[2][0], decimal.Decimal("20.00"))
            self.assertEqual(rows[1][1], uuid.UUID("550e8400-e29b-41d4-a716-446655440000"))
            self.assertEqual(rows[0][2], 1700000002000000)
            hit = con.execute(
                "SELECT * WHERE id = '550e8400-e29b-41d4-a716-446655440000'"
            ).fetchall()
            self.assertEqual(len(hit), 1)
            self.assertEqual(hit[0][0], decimal.Decimal("10.50"))
            over = con.execute("SELECT * WHERE amount > 10").fetchall()
            self.assertEqual(len(over), 2)

    def test_arrow_optional(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")
        with glacier.connect() as con:
            table = con.execute("select 1").arrow()
            self.assertEqual(table.num_rows, 1)
            self.assertEqual(table.to_pydict()["1"], [1])


if __name__ == "__main__":
    unittest.main()
