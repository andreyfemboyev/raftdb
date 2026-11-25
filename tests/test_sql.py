import unittest

from raftdb.sql import Database, SQLError


class SQLTests(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        self.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT NOT NULL, active BOOLEAN)")
        self.db.execute("INSERT INTO items VALUES (1, 'O''Brien', TRUE), (2, 'beta', NULL)")

    def test_projection_filter_order_and_limit(self):
        result = self.db.execute(
            "select label, active from ITEMS where id >= 1 and active is not null order by id desc limit 1;"
        )
        self.assertEqual(result, {"columns": ["label", "active"], "rows": [["O'Brien", True]]})
        self.assertEqual(self.db.execute("SELECT id FROM items ORDER BY active")["rows"], [[2], [1]])
        self.assertEqual(self.db.execute("SELECT id FROM items ORDER BY active DESC")["rows"], [[1], [2]])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM items WHERE active = NULL")["rows"], [[0]])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM items WHERE active IS NULL")["rows"], [[1]])

    def test_mutation_errors_are_atomic(self):
        for sql in (
            "INSERT INTO items VALUES (3, 'valid', TRUE), (1, 'duplicate', NULL)",
            "INSERT INTO items (id) VALUES (3)",
            "UPDATE items SET id = 4",
            "UPDATE items SET active = 1",
            "DELETE FROM items WHERE nonexistent = 1",
            "DELETE FROM items; SELECT * FROM items",
        ):
            with self.subTest(sql=sql), self.assertRaises(SQLError):
                self.db.execute(sql)
        self.assertEqual(self.db.execute("SELECT id FROM items ORDER BY id")["rows"], [[1], [2]])

    def test_mutations_and_integer_bounds(self):
        self.assertEqual(self.db.execute("UPDATE items SET id = 3 WHERE id = 1"), {"affected": 1})
        self.assertEqual(self.db.execute("DELETE FROM items WHERE id = 2"), {"affected": 1})
        with self.assertRaises(SQLError):
            self.db.execute("INSERT INTO items VALUES (9223372036854775808, 'overflow', NULL)")
        self.db.execute("INSERT INTO items (label, id) VALUES ('min', -9223372036854775808)")
        self.assertEqual(
            self.db.execute("SELECT * FROM items WHERE id = -9223372036854775808")["rows"],
            [[-9223372036854775808, "min", None]],
        )


    def test_aggregates_offset_drop_and_contextual_identifiers(self):
        self.db.execute("CREATE TABLE sums (id INTEGER PRIMARY KEY, sum INTEGER)")
        self.db.execute("INSERT INTO sums VALUES (1, 4), (2, NULL), (3, 8), (4, -3)")
        for query, expected in (
            ("COUNT(*)", 4), ("COUNT(sum)", 3), ("SUM(sum)", 9),
            ("AVG(sum)", 3.0), ("MIN(sum)", -3), ("MAX(sum)", 8),
        ):
            self.assertEqual(self.db.execute(f"SELECT {query} FROM sums")["rows"], [[expected]])
        self.assertEqual(self.db.execute("SELECT SUM(sum) FROM sums WHERE id = 2")["rows"], [[None]])
        self.assertEqual(self.db.execute("SELECT SUM(sum) FROM sums OFFSET 1")["rows"], [])
        self.assertEqual(self.db.execute("SELECT sum FROM sums ORDER BY id LIMIT 2 OFFSET 1")["rows"], [[None], [8]])
        self.assertEqual(self.db.execute("SELECT id FROM sums ORDER BY id OFFSET 3")["rows"], [[4]])
        self.assertEqual(self.db.execute("DROP TABLE sums"), {"affected": 0})
        with self.assertRaises(SQLError):
            self.db.execute("SELECT * FROM sums")


if __name__ == "__main__":
    unittest.main()
