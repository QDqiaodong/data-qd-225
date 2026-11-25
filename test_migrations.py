from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

from migrations import HistoryMismatch, Migration, apply_plan

INITIAL = Migration(1, ("CREATE TABLE deployments(id INTEGER PRIMARY KEY)",))


class MigrationTest(unittest.TestCase):
    def test_failure_rolls_back_all_new_schema_and_records(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "service.db")
            self.assertEqual(apply_plan(path, [INITIAL]), [1])
            stage = Migration(2, ("ALTER TABLE deployments ADD COLUMN region TEXT",))
            broken = Migration(3, ("INSERT INTO absent VALUES (1)",))
            with self.assertRaises(sqlite3.OperationalError):
                apply_plan(path, [INITIAL, stage, broken])
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual([row[1] for row in db.execute("PRAGMA table_info(deployments)")], ["id"])
                self.assertEqual(db.execute("SELECT version FROM _migration_ledger").fetchall(), [(1,)])
            self.assertEqual(apply_plan(path, [INITIAL, stage]), [2])
            self.assertEqual(apply_plan(path, [INITIAL, stage]), [])

    def test_drift_shorter_plan_and_transaction_escape(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp, "service.db")
            apply_plan(path, [INITIAL])
            for plan in ([], [Migration(1, ("SELECT 1",))]):
                with self.assertRaises(HistoryMismatch):
                    apply_plan(path, plan)
            with self.assertRaises(sqlite3.DatabaseError):
                apply_plan(path, [INITIAL, Migration(2, ("COMMIT",))])
            with self.assertRaises(sqlite3.DatabaseError):
                apply_plan(path, [INITIAL, Migration(2, ("DELETE FROM _migration_ledger",))])
            self.assertEqual(apply_plan(path, [INITIAL]), [])
