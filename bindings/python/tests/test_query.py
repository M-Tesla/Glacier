import unittest
from pathlib import Path

import glacier

ROOT = Path(__file__).resolve().parents[3]
SALES = ROOT / "tests" / "formats" / "sales.parquet"
NULLS = ROOT / "tests" / "formats" / "nulls.parquet"
NESTED = ROOT / "tests" / "formats" / "nested.parquet"


class TestQuery(unittest.TestCase):
    def test_select_1(self):
        con = glacier.connect()
        try:
            self.assertEqual(con.execute("select 1").fetchall(), [(1,)])
            self.assertEqual(con.execute("SELECT 1 AS x").fetchone(), (1,))
        finally:
            con.close()

    def test_version(self):
        self.assertEqual(glacier.version(), "0.2.0")
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
            self.assertEqual(
                con.execute(
                    "SELECT SUM(price) OVER ("
                    "PARTITION BY category ORDER BY id "
                    "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) "
                    "FROM sales WHERE category = 'fruit' ORDER BY id"
                ).fetchall(),
                [(50,), (130,), (230,), (240,), (165,)],
            )
            rows = con.execute("SELECT LAG(price) OVER (ORDER BY id) FROM sales ORDER BY id").fetchall()
            self.assertIsNone(rows[0][0])
            self.assertEqual(rows[1][0], 50)

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

    def test_case_like_in_between_null_union(self):
        with glacier.connect() as con:
            self.assertEqual(con.execute("SELECT NULL").fetchall(), [(None,)])
            self.assertEqual(con.execute("SELECT 1 UNION SELECT 1").fetchall(), [(1,)])
            self.assertEqual(con.execute("SELECT 1 UNION ALL SELECT 1").fetchall(), [(1,), (1,)])
            with self.assertRaises(glacier.GlacierError):
                con.execute("SELECT 1 UNION SELECT 'x'")
            self.assertEqual(con.execute("SELECT * FROM (SELECT 1 AS x)").fetchall(), [(1,)])
            self.assertEqual(con.execute("WITH t AS (SELECT 1 AS x) SELECT x FROM t").fetchall(), [(1,)])
            with self.assertRaises(glacier.GlacierError) as ctx:
                con.execute("SELECT (SELECT 1)")
            self.assertIn("not supported", str(ctx.exception))
        if not SALES.is_file():
            self.skipTest("run `$ZIG build fixtures`")
        with glacier.connect(SALES) as con:
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM sales WHERE category LIKE 'f%'").fetchall(),
                [(5,)],
            )
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM sales WHERE price IN (50, 80, 90)").fetchall(),
                [(3,)],
            )
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM sales WHERE price BETWEEN 100 AND 150").fetchall(),
                [(4,)],
            )
            self.assertEqual(
                con.execute(
                    "SELECT CASE WHEN price > 200 THEN 'high' ELSE 'low' END FROM sales WHERE id = 9"
                ).fetchall(),
                [("high",)],
            )

    def test_subquery_cte_left_join(self):
        if not SALES.is_file():
            self.skipTest("run `$ZIG build fixtures`")
        with glacier.connect(SALES) as con:
            self.assertEqual(
                con.execute(
                    "SELECT COUNT(*) FROM (SELECT id FROM sales WHERE price > 100) t"
                ).fetchall(),
                [(5,)],
            )
            self.assertEqual(
                con.execute(
                    "SELECT COUNT(*) FROM sales WHERE id IN (SELECT id FROM sales WHERE price > 100)"
                ).fetchall(),
                [(5,)],
            )
            self.assertEqual(
                con.execute(
                    "WITH cheap AS (SELECT id FROM sales WHERE price < 100) "
                    "SELECT COUNT(*) FROM sales a LEFT JOIN cheap b ON a.id = b.id"
                ).fetchall(),
                [(10,)],
            )
            with self.assertRaises(glacier.GlacierError) as ctx:
                con.execute(
                    "SELECT * FROM sales a WHERE a.id IN (SELECT b.id FROM sales b WHERE a.id > 0)"
                )
            self.assertIn("not supported", str(ctx.exception))

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
                con.execute("SELECT * FROM a NATURAL JOIN b")
            self.assertIn("JOIN is not supported", str(ctx.exception))

    def test_left_join(self):
        if not SALES.is_file() or not NULLS.is_file():
            self.skipTest("run `$ZIG build fixtures`")
        with glacier.connect(SALES) as con:
            inner = con.execute(
                f"SELECT COUNT(*) FROM sales a JOIN '{NULLS}' b ON a.id = b.id"
            ).fetchall()
            left = con.execute(
                f"SELECT COUNT(*) FROM sales a LEFT JOIN '{NULLS}' b ON a.id = b.id"
            ).fetchall()
            dangling = con.execute(
                f"SELECT COUNT(*) FROM sales a LEFT JOIN '{NULLS}' b ON a.id = b.id WHERE b.id IS NULL"
            ).fetchall()
            self.assertEqual(inner, [(4,)])
            self.assertEqual(left, [(10,)])
            self.assertEqual(dangling, [(6,)])

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

    def test_nested_list(self):
        if not NESTED.is_file():
            self.skipTest("run `$ZIG build fixtures`")
        with glacier.connect(NESTED) as con:
            self.assertEqual(con.execute("SELECT *").fetchall(), [("[1, 2, 3]",)])

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
