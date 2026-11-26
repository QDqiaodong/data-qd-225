from contextlib import closing
from pathlib import Path
import shutil
import sqlite3
import tempfile
import tracemalloc
import unittest

from migrations import (
    CheckedMigration,
    HistoryMismatch,
    Migration,
    OnlineMigrationRequired,
    OnlineUnique,
    QueryParity,
    QueryParityMismatch,
    QueryParityShape,
    apply_plan,
    prepare_online_unique,
    preview_plan,
    publish_online_unique,
    step_online_unique,
)
from replica_waves import ReadSlot, Replica, open_replica_wave


def connect(path):
    return closing(sqlite3.connect(path))


def table_names(path):
    with connect(path) as db:
        return {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            )
        }


def ledger_versions(path):
    with connect(path) as db:
        exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' "
            "AND name='_migration_ledger'"
        ).fetchone()
        if exists is None:
            return []
        return [row[0] for row in db.execute(
            "SELECT version FROM _migration_ledger ORDER BY version"
        )]


def parity_or_raise(before_rows, after_rows, columns=1, path=None):
    """Run a preview for supplied one-table multisets."""
    own_dir = None
    if path is None:
        own_dir = tempfile.TemporaryDirectory()
        path = Path(own_dir.name, "parity.db")
    try:
        with connect(path) as db:
            marks = ",".join("?" for _ in range(columns))
            db.execute("CREATE TABLE before_side (%s)" % ",".join(
                "c%d" % i for i in range(columns)
            ))
            db.execute("CREATE TABLE after_side (%s)" % ",".join(
                "c%d" % i for i in range(columns)
            ))
            db.executemany(
                "INSERT INTO before_side VALUES (%s)" % marks, before_rows
            )
            db.executemany(
                "INSERT INTO after_side VALUES (%s)" % marks, after_rows
            )
            db.commit()

        check = QueryParity(
            "exact",
            columns,
            "SELECT %s FROM before_side" % ",".join("c%d" % i for i in range(columns)),
            "SELECT %s FROM after_side" % ",".join("c%d" % i for i in range(columns)),
        )
        plan = [CheckedMigration(
            1,
            ("CREATE TABLE IF NOT EXISTS parity_marker(id INTEGER)",),
            [check],
        )]
        return preview_plan(path, plan)
    finally:
        if own_dir is not None:
            own_dir.cleanup()


class QueryParityValidationTest(unittest.TestCase):
    def test_name_columns_and_check_collection_shape(self):
        QueryParity("A-z_09", 8, "SELECT 1", "SELECT 2")

        bad_names = ["", "x" * 33, "has space", "dot.name", "café"]
        for name in bad_names:
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    QueryParity(name, 1, "SELECT 1", "SELECT 2")

        for columns in (0, 9, True, False, 1.0, "2", None):
            with self.subTest(columns=columns):
                with self.assertRaises(ValueError):
                    QueryParity("q", columns, "SELECT 1", "SELECT 2")

        with self.assertRaises(ValueError):
            CheckedMigration(1, (), [QueryParity("q", 1, "SELECT 1", "SELECT 2")])
        with self.assertRaises(ValueError):
            CheckedMigration(1, ("SELECT 1",), [])
        with self.assertRaises(ValueError):
            CheckedMigration(1, ("SELECT 1",), [
                QueryParity("q", 1, "SELECT 1", "SELECT 2")
                for _ in range(9)
            ])
        with self.assertRaises(ValueError):
            CheckedMigration(1, ("SELECT 1",), [
                QueryParity("same", 1, "SELECT 1", "SELECT 2"),
                QueryParity("same", 1, "SELECT 3", "SELECT 4"),
            ])
        # Case is significant, so these are distinct names.
        CheckedMigration(1, ("SELECT 1",), [
            QueryParity("Case", 1, "SELECT 1", "SELECT 2"),
            QueryParity("case", 1, "SELECT 3", "SELECT 4"),
        ])

    def test_sql_must_be_nonempty_parameter_free_single_select(self):
        valid = [
            "SELECT 1",
            " select 1; ",
            "WITH x(v) AS (SELECT 1) SELECT v FROM x",
            "SELECT '?; -- okay'",
            'SELECT "?; okay"',
            "/* ? */ SELECT 1",
        ]
        for sql in valid:
            QueryParity("q", 1, sql, "SELECT 1")

        invalid = [
            "",
            "   ",
            "DELETE FROM t",
            "INSERT INTO t VALUES (1)",
            "DROP TABLE t",
            "SELECT ?",
            "SELECT * FROM t WHERE id = :id",
            "SELECT 1; SELECT 2",
        ]
        for sql in invalid:
            with self.subTest(sql=sql):
                with self.assertRaises(ValueError):
                    QueryParity("q", 1, sql, "SELECT 1")

    def test_checksum_binds_every_declared_identity_part(self):
        base_checks = [QueryParity("q", 1, "SELECT 1", "SELECT 2")]
        base = CheckedMigration(2, ("CREATE TABLE x(x)",), base_checks)

        changed = [
            CheckedMigration(3, ("CREATE TABLE x(x)",), base_checks),
            CheckedMigration(2, ("CREATE TABLE y(x)",), base_checks),
            CheckedMigration(2, (
                "CREATE TABLE x(x)",
                "INSERT INTO x VALUES (1)",
            ), base_checks),
            CheckedMigration(2, ("CREATE TABLE x(x)",), [
                QueryParity("r", 1, "SELECT 1", "SELECT 2")
            ]),
            CheckedMigration(2, ("CREATE TABLE x(x)",), [
                QueryParity("q", 2, "SELECT 1,2", "SELECT 2")
            ]),
            CheckedMigration(2, ("CREATE TABLE x(x)",), [
                QueryParity("q", 1, "SELECT 1 ", "SELECT 2")
            ]),
            CheckedMigration(2, ("CREATE TABLE x(x)",), [
                QueryParity("q", 1, "SELECT 1", "SELECT 2 ")
            ]),
        ]
        for variant in changed:
            with self.subTest(checksum=variant.checksum()):
                self.assertNotEqual(base.checksum(), variant.checksum())

        ordinary = Migration(2, ("CREATE TABLE x(x)",))
        self.assertNotEqual(base.checksum(), ordinary.checksum())
        self.assertEqual(base.checksum(), CheckedMigration(
            2, ["CREATE TABLE x(x)"], [QueryParity("q", 1, "SELECT 1", "SELECT 2")]
        ).checksum())


class QueryParitySemanticsTest(unittest.TestCase):
    def test_order_and_exact_storage_types(self):
        mixed = [
            (None,), (1,), (1.0,), ("text",), (b"blob",), (-0.0,), (0.0,)
        ]
        # Reverse order is irrelevant; -0.0 and 0.0 have equal REAL value.
        self.assertEqual(parity_or_raise(mixed, list(reversed(mixed))), [1])

        equivalent_cases = [
            ([], [], 1),
            (
                [(1, 2, 3), (4, 5, 6)],
                [(4, 5, 6), (1, 2, 3)],
                3,
            ),
            (
                [(None, "x", b"y", 1.5), (1, "z", b"w", 2.5)],
                [(1, "z", b"w", 2.5), (None, "x", b"y", 1.5)],
                4,
            ),
            ([("A",), ("a",)], [("a",), ("A",)], 1),
        ]
        for before, after, columns in equivalent_cases:
            with self.subTest(before=before, after=after):
                self.assertEqual(
                    parity_or_raise(before, after, columns=columns),
                    [1],
                )

        incompatible_cases = [
            ([(1,)], [(1.0,)]),
            ([("x",)], [(b"x",)]),
            ([(b"x",)], [("x",)]),
            ([("A",)], [("a",)]),
            ([(1,), (1,), (2,)], [(1,), (2,), (2,)]),
        ]
        for before, after in incompatible_cases:
            with self.subTest(before=before, after=after):
                with self.assertRaises(QueryParityMismatch) as caught:
                    parity_or_raise(before, after)
                error = caught.exception
                self.assertEqual(error.version, 1)
                self.assertEqual(error.check_name, "exact")
                self.assertIsInstance(error.row, tuple)
                self.assertNotEqual(error.before_count, error.after_count)

    def test_mismatch_witness_and_counts_are_real_and_accurate(self):
        before = [(1, "a"), (1, "a"), (2, "b"), (2, "b"), (2, "b")]
        after = [(1, "a"), (2, "b"), (2, "b"), (2, "b"), (3, "c")]
        with self.assertRaises(QueryParityMismatch) as caught:
            parity_or_raise(before, after, columns=2)
        error = caught.exception

        # The discrepancy here is exactly row (1, 'a'): 2 versus 1.
        self.assertEqual(error.row, (1, "a"))
        self.assertEqual((error.before_count, error.after_count), (2, 1))

    def test_empty_result_checks_column_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "shape.db")
            with connect(path) as db:
                db.execute("CREATE TABLE empty_two(a, b)")
                db.commit()

            good = QueryParity(
                "good", 2, "SELECT a,b FROM empty_two",
                "SELECT 1,2 WHERE 0",
            )
            preview_plan(path, [CheckedMigration(
                1, ("CREATE TABLE marker(x)",), [good]
            )])

            wrong_before = QueryParity(
                "wrong-before", 2, "SELECT a FROM empty_two", "SELECT 1,2"
            )
            with self.assertRaises(QueryParityShape) as caught:
                preview_plan(path, [CheckedMigration(
                    1, ("CREATE TABLE marker2(x)",), [wrong_before]
                )])
            self.assertEqual(caught.exception.side, "before")
            self.assertEqual(caught.exception.expected_columns, 2)
            self.assertEqual(caught.exception.actual_columns, 1)
            self.assertNotIn("marker2", table_names(path))

            wrong_after = QueryParity(
                "wrong-after", 1, "SELECT a FROM empty_two",
                "SELECT a,b FROM empty_two",
            )
            with self.assertRaises(QueryParityShape) as caught:
                preview_plan(path, [CheckedMigration(
                    1, ("CREATE TABLE marker3(x)",), [wrong_after]
                )])
            self.assertEqual(caught.exception.side, "after")
            self.assertEqual(caught.exception.actual_columns, 2)
            self.assertNotIn("marker3", table_names(path))

    def test_sql_error_is_not_reported_as_parity_failure(self):
        check = QueryParity("missing", 1, "SELECT no_such_table.x", "SELECT 1")
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "errors.db")
            with self.assertRaises(sqlite3.OperationalError):
                preview_plan(path, [CheckedMigration(
                    1, ("CREATE TABLE marker(x)",), [check]
                )])
            self.assertEqual(table_names(path), set())

    def test_internal_ledger_cannot_be_read_by_parity_sql(self):
        check = QueryParity(
            "ledger", 1,
            "SELECT version FROM _migration_ledger",
            "SELECT 1",
        )
        plan = [
            Migration(1, ("CREATE TABLE ordinary(x)",)),
            CheckedMigration(2, ("CREATE TABLE gated(x)",), [check]),
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "internal.db")
            with self.assertRaises(sqlite3.DatabaseError):
                preview_plan(path, plan)
            self.assertEqual(ledger_versions(path), [])


class QueryParityPlanTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name, "events.db")
        with connect(self.path) as db:
            db.execute(
                "CREATE TABLE old_events("
                "name TEXT NOT NULL, eligible INTEGER NOT NULL)"
            )
            db.executemany(
                "INSERT INTO old_events(name, eligible) VALUES (?, 1)",
                [("red",), ("blue",)],
            )
            db.commit()

        self.check = QueryParity(
            "event-names",
            1,
            "SELECT name FROM old_events ORDER BY rowid",
            "SELECT name FROM copied_events ORDER BY name DESC",
        )
        self.plan = [
            Migration(1, ("CREATE TABLE batch_marker(id INTEGER)",)),
            CheckedMigration(2, (
                "CREATE TABLE copied_events(name TEXT NOT NULL)",
                "INSERT INTO copied_events "
                "SELECT name FROM old_events WHERE eligible = 1",
            ), [self.check]),
            Migration(3, (
                "CREATE VIEW event_consumer AS SELECT name FROM copied_events",
            )),
        ]

    def tearDown(self):
        self.tmp.cleanup()

    def test_preview_then_changed_database_is_rejudged_by_apply(self):
        self.assertEqual(preview_plan(self.path, self.plan), [1, 2, 3])
        self.assertEqual(
            table_names(self.path), {"old_events"}
        )
        self.assertEqual(ledger_versions(self.path), [])

        with connect(self.path) as db:
            db.execute(
                "INSERT INTO old_events(name, eligible) VALUES ('red', 0)"
            )
            db.commit()

        with self.assertRaises(QueryParityMismatch) as caught:
            apply_plan(self.path, self.plan)
        error = caught.exception
        self.assertEqual(error.version, 2)
        self.assertEqual(error.check_name, "event-names")
        self.assertEqual(error.row, ("red",))
        self.assertEqual((error.before_count, error.after_count), (2, 1))

        names = table_names(self.path)
        self.assertNotIn("batch_marker", names)
        self.assertNotIn("copied_events", names)
        self.assertNotIn("event_consumer", names)
        self.assertNotIn("_migration_ledger", names)

        with connect(self.path) as db:
            db.execute("UPDATE old_events SET eligible=1 WHERE eligible=0")
            db.commit()

        self.assertEqual(apply_plan(self.path, self.plan), [1, 2, 3])
        with connect(self.path) as db:
            counts = dict(db.execute(
                "SELECT name, COUNT(*) FROM copied_events GROUP BY name"
            ).fetchall())
            consumer_count = db.execute(
                "SELECT COUNT(*) FROM event_consumer"
            ).fetchone()[0]
        self.assertEqual(counts, {"red": 2, "blue": 1})
        self.assertEqual(consumer_count, 3)
        self.assertEqual(apply_plan(self.path, self.plan), [])

    def test_preview_failure_leaves_no_changes(self):
        with connect(self.path) as db:
            db.execute(
                "INSERT INTO old_events(name, eligible) VALUES ('red', 0)"
            )
            db.commit()
        with self.assertRaises(QueryParityMismatch):
            preview_plan(self.path, self.plan)
        self.assertEqual(table_names(self.path), {"old_events"})
        self.assertEqual(ledger_versions(self.path), [])

    def test_changed_check_sql_on_replay_is_history_drift(self):
        self.assertEqual(apply_plan(self.path, self.plan), [1, 2, 3])
        changed_check = QueryParity(
            "event-names",
            1,
            self.check.before_sql,
            self.check.after_sql + " WHERE name IS NOT NULL",
        )
        drifted = self.plan[:1] + [CheckedMigration(
            2, self.plan[1].statements, [changed_check]
        )] + self.plan[2:]
        with self.assertRaises(HistoryMismatch):
            apply_plan(self.path, drifted)
        with self.assertRaises(HistoryMismatch):
            preview_plan(self.path, drifted)

    def test_registered_phase_is_not_rechecked_on_replay(self):
        self.assertEqual(apply_plan(self.path, self.plan), [1, 2, 3])
        # Deliberately make the old/new multisets unequal.  Exact replay must
        # trust the ledger prefix and neither migrate nor rerun the gate.
        with connect(self.path) as db:
            db.execute("INSERT INTO old_events(name, eligible) VALUES ('blue', 1)")
            db.commit()
        self.assertEqual(apply_plan(self.path, self.plan), [])

    def test_earlier_batch_writes_are_visible_to_before_queries(self):
        plan = [
            Migration(1, (
                "CREATE TABLE source(value INTEGER NOT NULL)",
                "INSERT INTO source VALUES (7)",
            )),
            CheckedMigration(2, (
                "CREATE TABLE copied(value INTEGER NOT NULL)",
                "INSERT INTO copied SELECT value FROM source",
            ), [QueryParity(
                "same-source", 1,
                "SELECT value FROM source",
                "SELECT value FROM copied",
            )]),
        ]
        self.assertEqual(preview_plan(self.path, plan), [1, 2])
        self.assertEqual(apply_plan(self.path, plan), [1, 2])

    def test_later_phase_cannot_mask_or_commit_failed_gate(self):
        # Add a later phase that would, if reached, create a repair object.
        repair = Migration(3, (
            "INSERT INTO copied_events(name) VALUES ('red')",
        ))
        plan = self.plan[:2] + [repair]
        with connect(self.path) as db:
            db.execute(
                "INSERT INTO old_events(name, eligible) VALUES ('red', 0)"
            )
            db.commit()
        with self.assertRaises(QueryParityMismatch):
            apply_plan(self.path, plan)
        self.assertEqual(table_names(self.path), {"old_events"})

    def test_successful_check_then_later_sql_failure_rolls_back(self):
        plan = [
            CheckedMigration(1, (
                "CREATE TABLE gated_ok(x INTEGER)",
            ), [QueryParity("one", 1, "SELECT 1", "SELECT 1")]),
            Migration(2, ("INSERT INTO absent_table VALUES (1)",)),
        ]
        with self.assertRaises(sqlite3.OperationalError):
            apply_plan(self.path, plan)
        names = table_names(self.path)
        self.assertNotIn("gated_ok", names)
        self.assertEqual(ledger_versions(self.path), [])

    def test_checks_run_in_order_and_stop_on_first_failure(self):
        checks = [
            QueryParity("pass", 1, "SELECT 1", "SELECT 1"),
            QueryParity("fail", 1, "SELECT 1", "SELECT 2"),
            QueryParity(
                "later", 1, "SELECT 1",
                "SELECT definitely_invalid_column FROM nowhere",
            ),
        ]
        plan = [CheckedMigration(1, ("CREATE TABLE ordered_gate(x)",), checks)]
        with self.assertRaises(QueryParityMismatch) as caught:
            preview_plan(self.path, plan)
        self.assertEqual(caught.exception.check_name, "fail")
        self.assertNotIn("ordered_gate", table_names(self.path))


class OnlineAndReplicaBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name, "online.db")
        with connect(self.path) as db:
            db.execute(
                "CREATE TABLE routes(id INTEGER PRIMARY KEY, route_key TEXT)"
            )
            db.execute("INSERT INTO routes VALUES (1, 'a')")
            db.commit()
        self.bootstrap = Migration(1, (
            "CREATE TABLE IF NOT EXISTS routes("
            "id INTEGER PRIMARY KEY, route_key TEXT)",
        ))
        self.online = OnlineUnique(2, "routes")
        self.after = Migration(3, ("CREATE TABLE after_online(id INTEGER)",))
        self.plan = [self.bootstrap, self.online, self.after]

    def tearDown(self):
        self.tmp.cleanup()

    def test_preview_cannot_bypass_unpublished_online(self):
        apply_plan(self.path, [self.bootstrap])
        with self.assertRaises(OnlineMigrationRequired):
            preview_plan(self.path, self.plan)
        with self.assertRaises(OnlineMigrationRequired):
            apply_plan(self.path, self.plan)

        self.assertTrue(prepare_online_unique(self.path, self.plan[:2]))
        while step_online_unique(self.path, self.plan[:2], 256):
            pass
        self.assertEqual(
            publish_online_unique(self.path, self.plan[:2])[0], "published"
        )
        self.assertEqual(preview_plan(self.path, self.plan), [3])
        self.assertNotIn("after_online", table_names(self.path))
        self.assertEqual(apply_plan(self.path, self.plan), [3])
        self.assertEqual(preview_plan(self.path, self.plan), [])

    def test_replica_wave_accepts_checked_prefix_but_not_remaining_gate(self):
        a = Path(self.tmp.name, "a.db")
        b = Path(self.tmp.name, "b.db")
        shutil.copy(self.path, a)
        shutil.copy(self.path, b)

        gate = CheckedMigration(2, (
            "CREATE TABLE checked_marker(x INTEGER)",
        ), [QueryParity("one", 1, "SELECT 1", "SELECT 1")])
        final = Migration(3, ("CREATE TABLE wave_final(x INTEGER)",))
        plan = [self.bootstrap, gate, final]

        apply_plan(a, plan[:2])
        apply_plan(b, plan[:1])

        replicas = [Replica("a", a, "z"), Replica("b", b, "z")]
        slots = [ReadSlot("S", {"a", "b"}, 1, 3, "SELECT 1")]
        wave = open_replica_wave(
            replicas, plan, slots, ["a", "b"], 1, {"z": 1}
        )
        with wave:
            # a has the checked phase in its registered prefix and only v3
            # ordinary SQL remaining.  b still has a CheckedMigration in tail.
            self.assertEqual(wave.selected, ("a",))
            self.assertEqual(wave.advance(), "a")
        self.assertEqual(ledger_versions(a), [1, 2, 3])
        self.assertEqual(ledger_versions(b), [1])


class LargeParityMemoryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.base = Path(cls.tmp.name, "large-base.db")
        cls.row_count = 120_000
        cls.distinct_count = 80_000

        with connect(cls.base) as db:
            db.execute("CREATE TABLE large_before(k INTEGER, payload TEXT)")
            db.execute("CREATE TABLE large_after(k INTEGER, payload TEXT)")
            rows = []
            for key in range(cls.distinct_count):
                payload = ("k=%08d-" % key) + ("x" * 392)
                rows.append((key, payload))
                if key % 2 == 0:
                    rows.append((key, payload))
            assert len(rows) == cls.row_count
            db.executemany("INSERT INTO large_before VALUES (?,?)", rows)
            db.executemany(
                "INSERT INTO large_after VALUES (?,?)", list(reversed(rows))
            )
            db.commit()

        cls.check = QueryParity(
            "large",
            2,
            "SELECT k,payload FROM large_before",
            "SELECT k,payload FROM large_after",
        )
        cls.plan = [CheckedMigration(
            1, ("CREATE TABLE large_gate_marker(x INTEGER)",), [cls.check]
        )]

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_120k_rows_compare_completely_under_memory_bound(self):
        success = Path(self.tmp.name, "success.db")
        shutil.copy(self.base, success)

        was_tracing = tracemalloc.is_tracing()
        if not was_tracing:
            tracemalloc.start()
        try:
            baseline_current, baseline_peak = tracemalloc.get_traced_memory()
            self.assertEqual(apply_plan(success, self.plan), [1])
            current, peak = tracemalloc.get_traced_memory()
            self.assertLess(peak - baseline_peak, 24 * 1024 * 1024)
            self.assertLess(current - baseline_current, 24 * 1024 * 1024)
        finally:
            if not was_tracing:
                tracemalloc.stop()
        self.assertEqual(ledger_versions(success), [1])
        self.assertEqual(ledger_versions(success), [1])

        with connect(success) as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM large_gate_marker").fetchone()[0],
                0,
            )
        for suffix in ("", "-wal", "-shm"):
            artifact = Path(str(success) + suffix)
            artifact.unlink(missing_ok=True)

    def test_large_duplicate_distribution_change_is_rejected_without_ledger(self):
        bad = Path(self.tmp.name, "bad.db")
        shutil.copy(self.base, bad)
        with connect(bad) as db:
            duplicate_key = db.execute(
                "SELECT k FROM large_before GROUP BY k "
                "HAVING COUNT(*) = 2 ORDER BY MAX(rowid) DESC LIMIT 1"
            ).fetchone()[0]
            existing_key = 1
            existing_payload = db.execute(
                "SELECT payload FROM large_after WHERE k=? LIMIT 1",
                (existing_key,),
            ).fetchone()[0]
            db.execute(
                "UPDATE large_after SET k=?, payload=? WHERE rowid=("
                "SELECT rowid FROM large_after WHERE k=? LIMIT 1)",
                (existing_key, existing_payload, duplicate_key),
            )
            db.commit()

        with self.assertRaises(QueryParityMismatch) as caught:
            apply_plan(bad, self.plan)
        error = caught.exception
        self.assertEqual(error.version, 1)
        self.assertIsInstance(error.row, tuple)
        self.assertEqual(len(error.row), 2)

        with connect(bad) as db:
            key, payload = error.row
            before_count = db.execute(
                "SELECT COUNT(*) FROM large_before "
                "WHERE k IS ? AND payload IS ?",
                (key, payload),
            ).fetchone()[0]
            after_count = db.execute(
                "SELECT COUNT(*) FROM large_after "
                "WHERE k IS ? AND payload IS ?",
                (key, payload),
            ).fetchone()[0]
        self.assertEqual(error.before_count, before_count)
        self.assertEqual(error.after_count, after_count)
        self.assertNotEqual(before_count, after_count)
        self.assertEqual(ledger_versions(bad), [])
        self.assertNotIn("large_gate_marker", table_names(bad))


if __name__ == "__main__":
    unittest.main()
