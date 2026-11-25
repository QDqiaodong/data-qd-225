from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
import unittest

import migrations
from migrations import (
    HistoryMismatch,
    Migration,
    OnlineMigrationRequired,
    OnlineUnique,
    apply_plan,
    prepare_online_unique,
    publish_online_unique,
    step_online_unique,
)

BOOTSTRAP = Migration(1, ("CREATE TABLE deployments(id INTEGER PRIMARY KEY)",))


def make_db(path, rows=()):
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "CREATE TABLE routes(id INTEGER PRIMARY KEY, route_key TEXT COLLATE BINARY, note TEXT)"
        )
        db.executemany("INSERT INTO routes(id,route_key) VALUES (?,?)", rows)


def ledger(path):
    with closing(sqlite3.connect(path)) as db:
        return db.execute("SELECT version,checksum FROM _migration_ledger ORDER BY version").fetchall()


def rows(path):
    with closing(sqlite3.connect(path)) as db:
        return db.execute("SELECT id,route_key FROM routes ORDER BY id").fetchall()


def plan_for(table="routes"):
    return [BOOTSTRAP, OnlineUnique(2, table)]


class OnlineUniqueTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.path = Path(self.tmp.name, "service.db")

    def tearDown(self):
        self.tmp.cleanup()

    # -- preparation -----------------------------------------------------
    def test_prepare_is_idempotent_and_does_not_record_version(self):
        make_db(self.path, [(1, "a")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        self.assertIs(prepare_online_unique(self.path, plan), True)
        self.assertIs(prepare_online_unique(self.path, plan), False)
        self.assertEqual([v for v, _ in ledger(self.path)], [1])
        # Still fully writable with duplicates before publication.
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO routes(id,route_key) VALUES (2,'a')")
        self.assertEqual(rows(self.path), [(1, "a"), (2, "a")])

    def test_prepare_requires_registered_ordinary_prefix(self):
        make_db(self.path, [(1, "a")])
        with self.assertRaises(HistoryMismatch):
            prepare_online_unique(self.path, plan_for())

    def test_second_online_plan_is_rejected(self):
        make_db(self.path, [(1, "a")])
        apply_plan(self.path, [BOOTSTRAP])
        prepare_online_unique(self.path, plan_for("routes"))
        with self.assertRaises(HistoryMismatch):
            prepare_online_unique(self.path, [BOOTSTRAP, OnlineUnique(2, "other")])

    def test_online_entry_points_require_prepare(self):
        make_db(self.path, [(1, "a")])
        apply_plan(self.path, [BOOTSTRAP])
        with self.assertRaises(OnlineMigrationRequired):
            step_online_unique(self.path, plan_for(), 10)
        with self.assertRaises(OnlineMigrationRequired):
            publish_online_unique(self.path, plan_for())

    def test_plan_shape_validation(self):
        make_db(self.path, [])
        apply_plan(self.path, [BOOTSTRAP])
        bad_plans = [
            [BOOTSTRAP, OnlineUnique(3, "routes")],                    # not consecutive
            [BOOTSTRAP, OnlineUnique(2, "routes"), OnlineUnique(3, "t")],
            [BOOTSTRAP, OnlineUnique(2, "routes"), BOOTSTRAP],        # online not last
            [BOOTSTRAP] + [Migration(i, ("SELECT 1",)) for i in range(2, 130)]
            + [OnlineUnique(130, "routes")],
        ]
        for plan in bad_plans:
            with self.assertRaises(ValueError):
                prepare_online_unique(self.path, plan)
        for name in ("bad name", "1table", "", "routes; DROP TABLE routes"):
            with self.assertRaises(ValueError):
                OnlineUnique(2, name)
        with self.assertRaises(ValueError):
            prepare_online_unique(self.path, [BOOTSTRAP])
        with self.assertRaises(ValueError):
            step_online_unique(self.path, plan_for(), 0)
        with self.assertRaises(ValueError):
            step_online_unique(self.path, plan_for(), 257)
        with self.assertRaises(ValueError):
            step_online_unique(self.path, plan_for(), True)

    # -- stepping --------------------------------------------------------
    def test_step_respects_limit_and_resumes_after_reopen(self):
        make_db(self.path, [(i, "k%d" % i) for i in range(1, 11)])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        self.assertEqual(step_online_unique(self.path, plan, 3), 3)
        # "Reopen": a brand new connection is used by each call.
        self.assertEqual(step_online_unique(self.path, plan, 3), 3)
        self.assertEqual(step_online_unique(self.path, plan, 3), 3)
        self.assertEqual(step_online_unique(self.path, plan, 3), 1)
        self.assertEqual(step_online_unique(self.path, plan, 3), 0)
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))

    def test_conflict_is_withheld_until_backfill_complete(self):
        make_db(self.path, [(1, "a"), (2, "a"), (3, "b")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        self.assertEqual(step_online_unique(self.path, plan, 1), 1)
        self.assertEqual(publish_online_unique(self.path, plan), ("not_ready", None))
        self.assertEqual(step_online_unique(self.path, plan, 10), 2)
        self.assertEqual(publish_online_unique(self.path, plan), ("conflict", ("a", 1, 2)))

    def test_smaller_id_inserted_after_cursor_is_merged(self):
        make_db(self.path, [(5, "x"), (6, "y"), (7, "z")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        self.assertEqual(step_online_unique(self.path, plan, 2), 2)  # cursor past 5,6
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO routes(id,route_key) VALUES (2,'x')")  # duplicate under cursor
            db.execute("INSERT INTO routes(id,route_key) VALUES (3,'w')")  # unique under cursor
        self.assertEqual(step_online_unique(self.path, plan, 10), 1)
        self.assertEqual(step_online_unique(self.path, plan, 10), 0)
        self.assertEqual(publish_online_unique(self.path, plan), ("conflict", ("x", 2, 5)))

    def test_old_node_changes_after_backfill_are_visible(self):
        make_db(self.path, [(1, "a"), (2, "b"), (3, "c")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 2):
            pass
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE routes SET route_key='a' WHERE id=2")   # new conflict
            db.execute("DELETE FROM routes WHERE id=3")
        self.assertEqual(publish_online_unique(self.path, plan), ("conflict", ("a", 1, 2)))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE routes SET route_key='b' WHERE id=2")   # repaired
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))

    def test_rolled_back_old_node_writes_do_not_count(self):
        make_db(self.path, [(1, "a"), (2, "b")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        with closing(sqlite3.connect(self.path)) as db:
            db.isolation_level = None
            db.execute("BEGIN")
            db.execute("INSERT INTO routes(id,route_key) VALUES (3,'a')")
            db.execute("UPDATE routes SET route_key='b' WHERE id=1")
            db.execute("ROLLBACK")
        while step_online_unique(self.path, plan, 5):
            pass
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))

    def test_witness_is_binary_smallest_key_then_two_smallest_ids(self):
        make_db(self.path, [
            (1, "b"), (2, "b"), (3, "a"), (7, "a"), (9, "a"), (4, "Z"), (5, "Z"),
        ])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 4):
            pass
        # 'Z' (0x5a) sorts before lowercase under BINARY.
        self.assertEqual(publish_online_unique(self.path, plan), ("conflict", ("Z", 4, 5)))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE routes SET route_key='z' WHERE id=5")
        self.assertEqual(publish_online_unique(self.path, plan), ("conflict", ("a", 3, 7)))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE routes SET route_key='a2' WHERE id=7")
            db.execute("UPDATE routes SET route_key='a3' WHERE id=9")
        self.assertEqual(publish_online_unique(self.path, plan), ("conflict", ("b", 1, 2)))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE routes SET route_key='b3' WHERE id=1")
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))

    # -- publication -----------------------------------------------------
    def test_ledger_failure_rolls_back_and_is_retryable(self):
        make_db(self.path, [(1, "a")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 10):
            pass
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute(
                "CREATE TRIGGER fail_ledger BEFORE INSERT ON _migration_ledger "
                "BEGIN SELECT RAISE(ABORT,'injected ledger failure'); END"
            )
        with self.assertRaises(sqlite3.DatabaseError):
            publish_online_unique(self.path, plan)
        self.assertEqual([v for v, _ in ledger(self.path)], [1])
        # Old connections can still write duplicates afterwards.
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO routes(id,route_key) VALUES (2,'a')")
            db.execute("DELETE FROM routes WHERE id=2")
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("DROP TRIGGER fail_ledger")
            db.execute("DELETE FROM routes WHERE id=1")
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO routes(id,route_key) VALUES (1,'a')")
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))
        self.assertEqual(publish_online_unique(self.path, plan), ("already_published", None))

    def test_published_constraints_on_preexisting_connections(self):
        make_db(self.path, [(1, "a"), (2, None)])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 10):
            pass
        old = sqlite3.connect(self.path)  # opened before publication
        try:
            self.assertEqual(publish_online_unique(self.path, plan), ("published", None))

            def attempt(sql):
                try:
                    old.execute(sql)
                except sqlite3.IntegrityError:
                    old.rollback()
                    return "conflict"
                old.commit()
                return "ok"

            self.assertEqual(attempt("INSERT INTO routes(id,route_key) VALUES (3,'a')"), "conflict")
            self.assertEqual([r[0] for r in old.execute("SELECT id FROM routes ORDER BY id")], [1, 2])
            self.assertEqual(attempt("UPDATE routes SET route_key='a' WHERE id=2"), "conflict")
            self.assertEqual(attempt("UPDATE routes SET route_key='a' WHERE id=1"), "ok")
            self.assertEqual(attempt("UPDATE routes SET route_key='a' WHERE id=1"), "ok")
            self.assertEqual(attempt("DELETE FROM routes WHERE id=1"), "ok")
            self.assertEqual(attempt("INSERT INTO routes(id,route_key) VALUES (4,'a')"), "ok")
            self.assertEqual(attempt("INSERT INTO routes(id,route_key) VALUES (5,NULL)"), "ok")
            self.assertEqual(attempt("INSERT INTO routes(id,route_key) VALUES (6,NULL)"), "ok")
            self.assertEqual(attempt("UPDATE routes SET route_key=NULL WHERE id=4"), "ok")
            self.assertEqual(attempt("UPDATE routes SET route_key='a' WHERE id=5"), "ok")
        finally:
            old.close()

    def test_checksum_ledger_replay_and_later_migrations(self):
        make_db(self.path, [(1, "a")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 10):
            pass
        publish_online_unique(self.path, plan)
        self.assertEqual(ledger(self.path), [(1, plan[0].checksum()), (2, plan[1].checksum())])
        self.assertNotEqual(plan[0].checksum(), plan[1].checksum())
        self.assertEqual(apply_plan(self.path, plan), [])
        later = Migration(3, ("CREATE TABLE later(id INTEGER PRIMARY KEY)",))
        self.assertEqual(apply_plan(self.path, plan + [later]), [3])
        # Later ordinary versions do not block repeating the online entry points.
        self.assertEqual(step_online_unique(self.path, plan, 5), 0)
        self.assertEqual(publish_online_unique(self.path, plan), ("already_published", None))
        self.assertIs(prepare_online_unique(self.path, plan), False)

    # -- ordinary executor interactions ----------------------------------
    def test_apply_plan_cannot_advance_past_unpublished_online(self):
        make_db(self.path, [(1, "a")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        other = Migration(2, ("CREATE TABLE sneaky(id INTEGER PRIMARY KEY)",))
        with self.assertRaises(OnlineMigrationRequired):
            apply_plan(self.path, [BOOTSTRAP, other])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertIsNone(
                db.execute("SELECT 1 FROM sqlite_master WHERE name='sneaky'").fetchone()
            )
        # Replaying the registered prefix stays a no-op.
        self.assertEqual(apply_plan(self.path, [BOOTSTRAP]), [])
        # The online version itself cannot be executed by apply_plan.
        with self.assertRaises(OnlineMigrationRequired):
            apply_plan(self.path, plan)

    def test_apply_plan_cannot_skip_unpublished_online_version(self):
        make_db(self.path, [(1, "a")])
        apply_plan(self.path, [BOOTSTRAP])
        with self.assertRaises(OnlineMigrationRequired):
            apply_plan(self.path, plan_for() + [Migration(3, ("SELECT 1",))])

    def test_status_after_publication_and_reopen(self):
        make_db(self.path, [(1, "a")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 10):
            pass
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))
        # A fresh connection sees the published state.
        self.assertEqual(step_online_unique(self.path, plan, 10), 0)
        self.assertEqual(publish_online_unique(self.path, plan), ("already_published", None))

    def test_empty_table_is_ready_immediately(self):
        make_db(self.path, [])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        self.assertEqual(step_online_unique(self.path, plan, 10), 0)
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))

    def test_updates_through_published_connection_track_conflicts(self):
        make_db(self.path, [(1, "a"), (2, "b")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 10):
            pass
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO routes(id,route_key) VALUES (3,'a')")
        self.assertEqual(publish_online_unique(self.path, plan), ("conflict", ("a", 1, 3)))
        # Deleting the *captured* row of the duplicate pair moves the witness.
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE routes SET route_key='c' WHERE id=1")
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))

    def test_extra_columns_and_multirow_transactions(self):
        make_db(self.path, [(1, "a"), (2, "b")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("UPDATE routes SET note='first' WHERE id=1")
            # A multi-statement transaction that fails midway must leave no
            # partial trace in the capture data.
            try:
                db.execute("INSERT INTO routes(id,route_key) VALUES (3,'c')")
                db.execute("INSERT INTO routes(id,route_key,missing) VALUES (4,'d')")
            except sqlite3.OperationalError:
                db.rollback()
        while step_online_unique(self.path, plan, 10):
            pass
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute("SELECT id,route_key FROM routes ORDER BY id").fetchall(),
                [(1, "a"), (2, "b")],
            )
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO routes(id,route_key,note) VALUES (5,'e','n')")
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute("SELECT note FROM routes WHERE id=5").fetchone(), ("n",)
            )

    def test_failed_statement_leaves_no_change(self):
        make_db(self.path, [(1, "a"), (2, "b")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 10):
            pass
        publish_online_unique(self.path, plan)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("BEGIN")
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE routes SET route_key='a' WHERE id=2")
            # The statement itself made no change; the transaction can continue.
            db.execute("UPDATE routes SET route_key='c' WHERE id=2")
            db.commit()
            self.assertEqual(
                db.execute("SELECT id,route_key FROM routes ORDER BY id").fetchall(),
                [(1, "a"), (2, "c")],
            )

    def test_publish_workload_is_bounded(self):
        make_db(self.path, [(i, "k%d" % i) for i in range(1, 30)])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 7):
            pass

        statements = []
        real_open = migrations._open

        def traced_open(path):
            db = real_open(path)
            db.set_trace_callback(statements.append)
            return db

        migrations._open = traced_open
        try:
            with closing(sqlite3.connect(self.path)) as db, db:
                db.execute("INSERT INTO routes(id,route_key) VALUES (0,'k5')")
            status = publish_online_unique(self.path, plan)
        finally:
            migrations._open = real_open
        self.assertEqual(status, ("conflict", ("k5", 0, 5)))
        # The readiness path never reads the target table, and its duplicate
        # probes are index lookups with LIMIT.
        data_queries = [
            sql for sql in statements
            if sql.lstrip().upper().startswith("SELECT")
            and "_STATE" not in sql.upper()
            and "SQLITE_MASTER" not in sql.upper()
            and "_MIGRATION_LEDGER" not in sql.upper()
        ]
        self.assertTrue(data_queries)
        for sql in statements:
            self.assertNotIn("FROM \"routes\"", sql)
        for sql in data_queries:
            self.assertIn("LIMIT", sql.upper())

    def test_step_returns_zero_only_when_work_is_drained(self):
        make_db(self.path, [(i, "k%d" % i) for i in range(1, 6)])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        self.assertEqual(step_online_unique(self.path, plan, 1), 1)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO routes(id,route_key) VALUES (0,'k3')")
        self.assertNotEqual(step_online_unique(self.path, plan, 256), 0)
        seen_zero = False
        for _ in range(5):
            if step_online_unique(self.path, plan, 256) == 0:
                seen_zero = True
                break
        self.assertTrue(seen_zero)
        self.assertEqual(publish_online_unique(self.path, plan), ("conflict", ("k3", 0, 3)))

    def test_delete_resolves_conflict_and_frees_key(self):
        make_db(self.path, [(1, "a"), (2, "a"), (3, "b")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 10):
            pass
        self.assertEqual(publish_online_unique(self.path, plan), ("conflict", ("a", 1, 2)))
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("DELETE FROM routes WHERE id=2")
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))

    def test_two_connections_do_not_double_count(self):
        make_db(self.path, [(1, "a"), (2, "b")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        viewer = sqlite3.connect(self.path)
        writer = sqlite3.connect(self.path)
        try:
            self.assertEqual(step_online_unique(self.path, plan, 10), 2)
            with writer:
                writer.execute("INSERT INTO routes(id,route_key) VALUES (3,'a')")
            # The other connection's committed row is visible to publish.
            self.assertEqual(
                publish_online_unique(self.path, plan), ("conflict", ("a", 1, 3))
            )
            with writer:
                writer.execute("UPDATE routes SET route_key='c' WHERE id=3")
            self.assertEqual(publish_online_unique(self.path, plan), ("published", None))
            with self.assertRaises(sqlite3.IntegrityError):
                with viewer:
                    viewer.execute("INSERT INTO routes(id,route_key) VALUES (4,'a')")
        finally:
            viewer.close()
            writer.close()

    def test_conflict_only_counts_non_null_keys(self):
        make_db(self.path, [(1, None), (2, None), (3, "a")])
        apply_plan(self.path, [BOOTSTRAP])
        plan = plan_for()
        prepare_online_unique(self.path, plan)
        while step_online_unique(self.path, plan, 10):
            pass
        self.assertEqual(publish_online_unique(self.path, plan), ("published", None))
        # Published already; a conflicting update is now rejected outright.
        with closing(sqlite3.connect(self.path)) as db, db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE routes SET route_key='a' WHERE id=2")


if __name__ == "__main__":
    unittest.main()
