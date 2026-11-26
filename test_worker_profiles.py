from contextlib import closing
from pathlib import Path
import sqlite3
import tempfile
import tracemalloc
import unittest

from migrations import (
    HistoryMismatch,
    Migration,
    OnlineMigrationRequired,
    OnlineUnique,
    ProfileRestoreBlocked,
    ReleaseArtifactReclaim,
    RestoreWorkerProfiles,
    WorkerProfileRollout,
    apply_plan,
    prepare_online_unique,
    publish_online_unique,
    step_online_unique,
)


SCHEMA = Migration(
    1,
    (
        "CREATE TABLE worker_profiles("
        "id INTEGER PRIMARY KEY, config BLOB NOT NULL)",
        "CREATE TABLE job_profile_pins("
        "job_id INTEGER PRIMARY KEY, "
        "profile_id INTEGER NOT NULL REFERENCES worker_profiles(id) "
        "ON DELETE RESTRICT)",
        "CREATE INDEX idx_pins_profile ON job_profile_pins(profile_id, job_id)",
        "INSERT INTO worker_profiles(id, config) VALUES (10, x'aa')",
        "INSERT INTO worker_profiles(id, config) VALUES (20, x'bb')",
        "INSERT INTO worker_profiles(id, config) VALUES (30, x'cc')",
    ),
)


def profiles(path):
    with closing(sqlite3.connect(path)) as db:
        db.text_factory = bytes
        return [
            (row[0], bytes(row[1]))
            for row in db.execute(
                "SELECT id, config FROM worker_profiles ORDER BY id"
            )
        ]


def delta(path):
    with closing(sqlite3.connect(path)) as db:
        db.text_factory = bytes
        return [
            (
                row[0],
                row[1],
                None if row[2] is None else bytes(row[2]),
                None if row[3] is None else bytes(row[3]),
            )
            for row in db.execute(
                "SELECT rollout_version, profile_id, before_config, after_config "
                "FROM _worker_profile_rollout_delta ORDER BY profile_id"
            )
        ]


def ledger(path):
    with closing(sqlite3.connect(path)) as db:
        return [row[0] for row in db.execute(
            "SELECT version FROM _migration_ledger ORDER BY version"
        )]


def run_direct(path, *sql):
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("PRAGMA foreign_keys=ON")
        for statement in sql:
            db.execute(statement)


ROLLOUT = WorkerProfileRollout(
    2,
    (
        "UPDATE worker_profiles SET config=x'11' WHERE id=10",
        "DELETE FROM worker_profiles WHERE id=20",
        "INSERT INTO worker_profiles(id, config) VALUES (40, x'dd')",
        "INSERT INTO worker_profiles(id, config) VALUES (50, x'ee')",
        "UPDATE worker_profiles SET config=x'33' WHERE id=30",
        "UPDATE worker_profiles SET config=x'cc' WHERE id=30",
    ),
)
RESTORE = RestoreWorkerProfiles(3, 2)


class RolloutAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name, "service.db")
        self.assertEqual(apply_plan(self.path, [SCHEMA]), [1])

    def tearDown(self):
        self.tmp.cleanup()

    def test_full_acceptance_block_then_restore(self):
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT]), [2])

        # Reopening the database preserves exactly four net-change rows.
        self.assertEqual(
            profiles(self.path),
            [(10, b"\x11"), (30, b"\xcc"), (40, b"\xdd"), (50, b"\xee")],
        )
        self.assertEqual(
            delta(self.path),
            [
                (2, 10, b"\xaa", b"\x11"),
                (2, 20, b"\xbb", None),
                (2, 40, None, b"\xdd"),
                (2, 50, None, b"\xee"),
            ],
        )
        self.assertEqual(ledger(self.path), [1, 2])

        # The service changes 10 and pins the newly created 40.
        run_direct(
            self.path,
            "UPDATE worker_profiles SET config=x'99' WHERE id=10",
            "INSERT INTO job_profile_pins(job_id, profile_id) VALUES (7, 40)",
        )

        with self.assertRaises(ProfileRestoreBlocked) as caught:
            apply_plan(self.path, [SCHEMA, ROLLOUT, RESTORE])
        self.assertEqual((caught.exception.profile_id, caught.exception.job_id), (40, 7))

        # Whole restore rejected: 20 not back, 50 not deleted, no ledger row.
        self.assertEqual(
            profiles(self.path),
            [(10, b"\x99"), (30, b"\xcc"), (40, b"\xdd"), (50, b"\xee")],
        )
        self.assertEqual(ledger(self.path), [1, 2])
        self.assertEqual(delta(self.path)[1], (2, 20, b"\xbb", None))

        # Removing the pin lets the very same plan through.
        run_direct(self.path, "DELETE FROM job_profile_pins")
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT, RESTORE]), [3])
        self.assertEqual(
            profiles(self.path),
            [(10, b"\x99"), (20, b"\xbb"), (30, b"\xcc")],
        )
        self.assertEqual(ledger(self.path), [1, 2, 3])
        # Evidence is untouched by a successful restore.
        self.assertEqual(len(delta(self.path)), 4)

    def test_modified_new_profile_with_pin_does_not_block(self):
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT]), [2])
        run_direct(
            self.path,
            "UPDATE worker_profiles SET config=x'ff' WHERE id=40",
            "INSERT INTO job_profile_pins(job_id, profile_id) VALUES (1, 40)",
        )
        # 40 differs from its rollout after-state, so it is kept, not deleted;
        # the pin on a retained profile never blocks.
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT, RESTORE]), [3])
        self.assertEqual(
            profiles(self.path),
            [(10, b"\xaa"), (20, b"\xbb"), (30, b"\xcc"), (40, b"\xff")],
        )

    def test_blocking_witness_is_lexicographically_least_profile_then_job(self):
        rollout = WorkerProfileRollout(
            2,
            (
                "INSERT INTO worker_profiles(id, config) VALUES (40, x'40')",
                "INSERT INTO worker_profiles(id, config) VALUES (50, x'50')",
            ),
        )
        self.assertEqual(apply_plan(self.path, [SCHEMA, rollout]), [2])
        run_direct(
            self.path,
            "INSERT INTO job_profile_pins VALUES (9, 50)",
            "INSERT INTO job_profile_pins VALUES (8, 40)",
            "INSERT INTO job_profile_pins VALUES (3, 40)",
        )
        with self.assertRaises(ProfileRestoreBlocked) as caught:
            apply_plan(
                self.path, [SCHEMA, rollout, RestoreWorkerProfiles(3, 2)]
            )
        self.assertEqual((caught.exception.profile_id, caught.exception.job_id), (40, 3))

    def test_service_changing_existing_profile_back_to_after_is_restored(self):
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT]), [2])
        run_direct(
            self.path,
            "UPDATE worker_profiles SET config=x'42' WHERE id=10",
            "UPDATE worker_profiles SET config=x'11' WHERE id=10",
        )
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT, RESTORE]), [3])
        by_id = dict(profiles(self.path))
        self.assertEqual(by_id[10], b"\xaa")

    def test_profiles_untouched_by_rollout_are_never_restored(self):
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT]), [2])
        # 30 was changed and changed back by the rollout, so it is not in the
        # delta; the service deleting it afterwards must survive the restore.
        run_direct(self.path, "DELETE FROM worker_profiles WHERE id=30")
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT, RESTORE]), [3])
        by_id = dict(profiles(self.path))
        self.assertNotIn(30, by_id)
        self.assertEqual(by_id[10], b"\xaa")
        self.assertEqual(by_id[20], b"\xbb")
        self.assertNotIn(40, by_id)
        self.assertNotIn(50, by_id)

    def test_statements_see_earlier_statements_in_the_same_rollout(self):
        rollout = WorkerProfileRollout(
            2,
            (
                "INSERT INTO worker_profiles(id, config) VALUES (60, x'01')",
                "UPDATE worker_profiles SET config=x'02' WHERE id=60",
                "DELETE FROM worker_profiles WHERE id=60",
                "INSERT INTO worker_profiles(id, config) VALUES (60, x'03')",
            ),
        )
        self.assertEqual(apply_plan(self.path, [SCHEMA, rollout]), [2])
        self.assertEqual(delta(self.path), [(2, 60, None, b"\x03")])
        self.assertEqual(dict(profiles(self.path))[60], b"\x03")

    def test_empty_config_is_distinct_from_absence(self):
        rollout = WorkerProfileRollout(
            2,
            (
                "UPDATE worker_profiles SET config=x'' WHERE id=10",
                "DELETE FROM worker_profiles WHERE id=20",
            ),
        )
        self.assertEqual(apply_plan(self.path, [SCHEMA, rollout]), [2])
        self.assertEqual(
            delta(self.path),
            [
                (2, 10, b"\xaa", b""),
                (2, 20, b"\xbb", None),
            ],
        )
        self.assertEqual(apply_plan(self.path, [SCHEMA, rollout, RESTORE]), [3])
        by_id = dict(profiles(self.path))
        self.assertEqual(by_id[10], b"\xaa")
        self.assertEqual(by_id[20], b"\xbb")

    def test_configs_are_compared_and_stored_as_bytes(self):
        # A non-BLOB storage class with the same bytes is not a net change; a
        # BLOB with the same bytes as a TEXT value is the same configuration.
        run_direct(
            self.path,
            "UPDATE worker_profiles SET config=CAST(x'aa' AS TEXT) WHERE id=10",
        )
        rollout = WorkerProfileRollout(
            2,
            (
                "UPDATE worker_profiles SET config=CAST(x'11' AS TEXT) WHERE id=20",
                "UPDATE worker_profiles SET config=x'aa' WHERE id=30",
            ),
        )
        self.assertEqual(apply_plan(self.path, [SCHEMA, rollout]), [2])
        rows = delta(self.path)
        self.assertEqual([row[1] for row in rows], [20, 30])
        self.assertTrue(all(row[0] == 2 for row in rows))
        with closing(sqlite3.connect(self.path)) as db:
            types = db.execute(
                "SELECT typeof(before_config), typeof(after_config) "
                "FROM _worker_profile_rollout_delta"
            ).fetchall()
        self.assertEqual(set(types), {("blob", "blob")})
        self.assertEqual(apply_plan(self.path, [SCHEMA, rollout, RESTORE]), [3])
        by_id = dict(profiles(self.path))
        self.assertEqual(by_id[20], b"\xbb")
        self.assertEqual(by_id[30], b"\xcc")
        # 10 was TEXT with the same bytes as its BLOB before-state; restoring
        # always writes the canonical BLOB form.
        self.assertEqual(by_id[10], b"\xaa")

    def test_recreated_deleted_profile_with_new_config_is_kept(self):
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT]), [2])
        run_direct(
            self.path,
            "INSERT INTO worker_profiles(id, config) VALUES (20, x'fe')",
        )
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT, RESTORE]), [3])
        by_id = dict(profiles(self.path))
        self.assertEqual(by_id[20], b"\xfe")  # site differs from after (absent)
        self.assertEqual(by_id[10], b"\xaa")  # unchanged by rollout
        self.assertIn(30, by_id)
        self.assertNotIn(40, by_id)
        self.assertNotIn(50, by_id)


class RolloutSemanticsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name, "service.db")
        self.assertEqual(apply_plan(self.path, [SCHEMA]), [1])

    def tearDown(self):
        self.tmp.cleanup()

    def test_earlier_sql_in_same_batch_is_the_baseline(self):
        seed = Migration(
            2,
            (
                "INSERT INTO worker_profiles(id, config) VALUES (60, x'6a')",
                "UPDATE worker_profiles SET config=x'cc' WHERE id=20",
            ),
        )
        rollout = WorkerProfileRollout(
            3, ("UPDATE worker_profiles SET config=x'dd' WHERE id=20",)
        )
        self.assertEqual(apply_plan(self.path, [SCHEMA, seed, rollout]), [2, 3])
        self.assertEqual(
            delta(self.path),
            [(3, 20, b"\xcc", b"\xdd")],
        )

    def test_rollout_rollback_on_later_statement_failure(self):
        broken = Migration(3, ("INSERT INTO absent VALUES (1)",))
        with self.assertRaises(sqlite3.OperationalError):
            apply_plan(self.path, [SCHEMA, ROLLOUT, broken])
        self.assertEqual(ledger(self.path), [1])
        self.assertEqual(
            profiles(self.path),
            [(10, b"\xaa"), (20, b"\xbb"), (30, b"\xcc")],
        )
        with closing(sqlite3.connect(self.path)) as db:
            tables = {
                row[0] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertNotIn("_worker_profile_rollout_delta", tables)

    def test_restore_rollback_on_later_statement_failure(self):
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT]), [2])
        broken = Migration(4, ("INSERT INTO absent VALUES (1)",))
        with self.assertRaises(sqlite3.OperationalError):
            apply_plan(self.path, [SCHEMA, ROLLOUT, RESTORE, broken])
        self.assertEqual(ledger(self.path), [1, 2])
        self.assertEqual(
            profiles(self.path),
            [(10, b"\x11"), (30, b"\xcc"), (40, b"\xdd"), (50, b"\xee")],
        )
        # The blocked-free plan still works afterwards.
        self.assertEqual(apply_plan(self.path, [SCHEMA, ROLLOUT, RESTORE]), [3])

    def test_empty_net_change_still_registers_version(self):
        rollout = WorkerProfileRollout(
            2,
            (
                "UPDATE worker_profiles SET config=x'aa' WHERE id=10",
                "INSERT INTO worker_profiles(id, config) VALUES (77, x'77')",
                "DELETE FROM worker_profiles WHERE id=77",
            ),
        )
        self.assertEqual(apply_plan(self.path, [SCHEMA, rollout]), [2])
        self.assertEqual(delta(self.path), [])
        self.assertEqual(ledger(self.path), [1, 2])
        # Restoring an empty net change changes nothing but records its version.
        self.assertEqual(
            apply_plan(self.path, [SCHEMA, rollout, RestoreWorkerProfiles(3, 2)]),
            [3],
        )
        self.assertEqual(ledger(self.path), [1, 2, 3])

    def test_intermediate_values_are_not_persisted(self):
        rollout = WorkerProfileRollout(
            2,
            (
                "UPDATE worker_profiles SET config=x'01' WHERE id=10",
                "UPDATE worker_profiles SET config=x'02' WHERE id=10",
                "UPDATE worker_profiles SET config=x'03' WHERE id=10",
            ),
        )
        apply_plan(self.path, [SCHEMA, rollout])
        self.assertEqual(delta(self.path), [(2, 10, b"\xaa", b"\x03")])
        self.assertEqual(apply_plan(self.path, [SCHEMA, rollout, RESTORE]), [3])
        self.assertEqual(dict(profiles(self.path))[10], b"\xaa")

    def test_exact_replay_is_noop_and_does_not_recollect(self):
        plan = [SCHEMA, ROLLOUT]
        self.assertEqual(apply_plan(self.path, plan), [2])
        before = delta(self.path)
        self.assertEqual(apply_plan(self.path, plan), [])
        self.assertEqual(delta(self.path), before)

        restored = plan + [RESTORE]
        self.assertEqual(apply_plan(self.path, restored), [3])
        evidence = delta(self.path)
        self.assertEqual(apply_plan(self.path, restored), [])
        self.assertEqual(delta(self.path), evidence)

    def test_statement_order_drift_is_history_mismatch(self):
        apply_plan(self.path, [SCHEMA, ROLLOUT])
        reordered = WorkerProfileRollout(
            2,
            tuple(reversed(ROLLOUT.statements)),
        )
        with self.assertRaises(HistoryMismatch):
            apply_plan(self.path, [SCHEMA, reordered])
        altered = WorkerProfileRollout(
            2,
            tuple(
                s.replace("x'11'", "x'12'") for s in ROLLOUT.statements
            ),
        )
        with self.assertRaises(HistoryMismatch):
            apply_plan(self.path, [SCHEMA, altered])

    def test_restore_checksum_drift_is_history_mismatch(self):
        apply_plan(self.path, [SCHEMA, ROLLOUT])
        # The rollout checksum in the plan no longer matches the ledger.
        drifted_rollout = WorkerProfileRollout(
            2,
            ROLLOUT.statements[:-1],
        )
        with self.assertRaises(HistoryMismatch):
            apply_plan(
                self.path,
                [SCHEMA, drifted_rollout, RestoreWorkerProfiles(3, 2)],
            )

    def test_restore_cannot_share_batch_with_its_rollout(self):
        with self.assertRaises(ValueError):
            apply_plan(self.path, [SCHEMA, ROLLOUT, RESTORE])
        self.assertEqual(ledger(self.path), [1])
        self.assertEqual(
            profiles(self.path),
            [(10, b"\xaa"), (20, b"\xbb"), (30, b"\xcc")],
        )

    def test_structure_validation(self):
        invalid_rollouts = [
            lambda: WorkerProfileRollout(0, ("UPDATE worker_profiles SET config=x'00'",)),
            lambda: WorkerProfileRollout(True, ("UPDATE worker_profiles SET config=x'00'",)),
            lambda: WorkerProfileRollout(1.5, ("UPDATE worker_profiles SET config=x'00'",)),
            lambda: WorkerProfileRollout(1, ()),
            lambda: WorkerProfileRollout(1, tuple(
                "UPDATE worker_profiles SET config=x'%02x' WHERE id=%d" % (i, i)
                for i in range(65)
            )),
            lambda: WorkerProfileRollout(1, ("SELECT 1",)),
            lambda: WorkerProfileRollout(1, ("UPDATE other SET config=x'00'",)),
            lambda: WorkerProfileRollout(1, (
                "INSERT OR REPLACE INTO worker_profiles VALUES (1, x'00')",
            )),
            lambda: WorkerProfileRollout(1, (
                "UPDATE worker_profiles SET config=x'00' WHERE id=1; "
                "DELETE FROM worker_profiles",
            )),
            lambda: WorkerProfileRollout(1, (
                "UPDATE worker_profiles SET config=x'00' "
                "WHERE id=1 ON CONFLICT DO NOTHING",
            )),
            lambda: WorkerProfileRollout(1, (
                "UPDATE worker_profiles SET config=x'00' " + "#" * 4100,
            )),
        ]
        for factory in invalid_rollouts:
            with self.assertRaises(ValueError):
                factory()

        # A semicolon inside a string literal is not a second statement.
        WorkerProfileRollout(
            1, ("UPDATE worker_profiles SET config=x'00' WHERE id=1",)
        )

        for version, rollout_version in (
            (3, 1),
            (3, 3),
            (True, 1),
            (3, True),
            (3.5, 2),
        ):
            with self.assertRaises(ValueError):
                RestoreWorkerProfiles(version, rollout_version)

        # A plan whose restore is not preceded by a rollout phase is invalid.
        stray = RestoreWorkerProfiles(2, 1)
        with self.assertRaises(ValueError):
            apply_plan(self.path, [SCHEMA, stray])

    def test_checksums_are_stable_and_typed(self):
        first = WorkerProfileRollout(
            2, ("UPDATE worker_profiles SET config=x'11' WHERE id=10",)
        )
        second = WorkerProfileRollout(
            2, ("UPDATE worker_profiles SET config=x'11' WHERE id=10",)
        )
        other = WorkerProfileRollout(
            2, ("UPDATE worker_profiles SET config=x'12' WHERE id=10",)
        )
        self.assertEqual(first.checksum(), second.checksum())
        self.assertNotEqual(first.checksum(), other.checksum())
        self.assertNotEqual(first.checksum(), Migration(2, first.statements).checksum())

        restore = RestoreWorkerProfiles(3, 2)
        self.assertEqual(restore.checksum(), RestoreWorkerProfiles(3, 2).checksum())
        self.assertNotEqual(restore.checksum(), first.checksum())
        self.assertNotEqual(restore.checksum(), Migration(3, ()).checksum())
        self.assertNotEqual(
            restore.checksum(), ReleaseArtifactReclaim(3, [2], 1).checksum()
        )

    def test_after_published_online_and_reclaim(self):
        path = Path(self.tmp.name, "online.db")
        schema = Migration(
            1,
            SCHEMA.statements
            + (
                "CREATE TABLE routes("
                "id INTEGER PRIMARY KEY, route_key TEXT COLLATE BINARY)",
                "CREATE TABLE releases(id INTEGER PRIMARY KEY, retired INTEGER NOT NULL)",
                "CREATE TABLE blobs(id INTEGER PRIMARY KEY, size_bytes INTEGER NOT NULL)",
                "CREATE TABLE release_blobs("
                "release_id INTEGER NOT NULL, blob_id INTEGER NOT NULL, "
                "PRIMARY KEY(release_id, blob_id), "
                "FOREIGN KEY(release_id) REFERENCES releases(id), "
                "FOREIGN KEY(blob_id) REFERENCES blobs(id))",
            ),
        )
        online = OnlineUnique(2, "routes")
        reclaim = ReleaseArtifactReclaim(3, [5], 1)
        rollout = WorkerProfileRollout(
            4, ("UPDATE worker_profiles SET config=x'11' WHERE id=10",)
        )
        restore = RestoreWorkerProfiles(5, 4)
        plan = [schema, online, reclaim, rollout, restore]

        self.assertEqual(apply_plan(path, [schema]), [1])
        online_plan = [schema, online]
        self.assertIs(prepare_online_unique(path, online_plan), True)
        self.assertEqual(step_online_unique(path, online_plan, 10), 0)
        self.assertEqual(
            publish_online_unique(path, online_plan), ("published", None)
        )
        run_direct(path, "INSERT INTO releases VALUES (5,1)")

        # The rollout and its restore are separate commits.
        self.assertEqual(apply_plan(path, plan[:4]), [3, 4])
        self.assertEqual(dict(profiles(path))[10], b"\x11")
        self.assertEqual(apply_plan(path, plan), [5])
        self.assertEqual(dict(profiles(path))[10], b"\xaa")
        self.assertEqual(ledger(path), [1, 2, 3, 4, 5])
        self.assertEqual(apply_plan(path, plan), [])

    def test_unpublished_online_blocks_rollouts(self):
        path2 = Path(self.tmp.name, "unpublished.db")
        schema = Migration(
            1,
            SCHEMA.statements
            + (
                "CREATE TABLE routes("
                "id INTEGER PRIMARY KEY, route_key TEXT COLLATE BINARY)",
            ),
        )
        online = OnlineUnique(2, "routes")
        rollout = WorkerProfileRollout(
            3, ("UPDATE worker_profiles SET config=x'11' WHERE id=10",)
        )
        plan = [schema, online, rollout]

        self.assertEqual(apply_plan(path2, [schema]), [1])
        prepare_online_unique(path2, [schema, online])
        with self.assertRaises(OnlineMigrationRequired):
            apply_plan(path2, plan)
        self.assertEqual(ledger(path2), [1])
        self.assertEqual(dict(profiles(path2))[10], b"\xaa")


class LargeRolloutTest(unittest.TestCase):
    """80,000 profiles of 512 bytes each, checked under tracemalloc."""

    PROFILE_COUNT = 80000

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name, "large.db")
        schema = Migration(
            1,
            (
                "CREATE TABLE worker_profiles("
                "id INTEGER PRIMARY KEY, config BLOB NOT NULL)",
                "CREATE TABLE job_profile_pins("
                "job_id INTEGER PRIMARY KEY, "
                "profile_id INTEGER NOT NULL REFERENCES worker_profiles(id) "
                "ON DELETE RESTRICT)",
                "CREATE INDEX idx_large_pins ON job_profile_pins(profile_id, job_id)",
            ),
        )
        apply_plan(self.path, [schema])
        # Fixture generation is outside the measured calls.
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute(
                "WITH RECURSIVE counter(x) AS ("
                "SELECT 1 UNION ALL SELECT x + 1 FROM counter WHERE x < ?) "
                "INSERT INTO worker_profiles(id, config) "
                "SELECT x, CAST(x'00' || randomblob(511) AS BLOB) FROM counter",
                (self.PROFILE_COUNT,),
            )

    def tearDown(self):
        self.tmp.cleanup()

    @staticmethod
    def measure(path, plan):
        started_here = not tracemalloc.is_tracing()
        if started_here:
            tracemalloc.start()
        if hasattr(tracemalloc, "reset_peak"):
            tracemalloc.reset_peak()
        applied = apply_plan(path, plan)
        _, peak = tracemalloc.get_traced_memory()
        if started_here:
            tracemalloc.stop()
        return applied, peak

    def _check_first_byte_distribution(self, expected_other, expected_one_ids):
        with closing(sqlite3.connect(self.path)) as db:
            count = db.execute("SELECT COUNT(*) FROM worker_profiles").fetchone()[0]
            wrong_other = db.execute(
                "SELECT COUNT(*) FROM worker_profiles "
                "WHERE substr(config,1,1) IS NOT ? AND id NOT IN ("
                + ",".join("?" for _ in expected_one_ids) + ")",
                (expected_other,) + tuple(expected_one_ids),
            ).fetchone()[0]
            ones = db.execute(
                "SELECT COUNT(*) FROM worker_profiles WHERE substr(config,1,1)=?",
                (b"\x01",),
            ).fetchone()[0]
            sizes = db.execute(
                "SELECT MIN(length(config)), MAX(length(config)) FROM worker_profiles"
            ).fetchone()
        self.assertEqual(count, self.PROFILE_COUNT)
        self.assertEqual(wrong_other, 0)
        self.assertEqual(ones, len(expected_one_ids))
        self.assertEqual(sizes, (512, 512))

    def test_large_change_then_revert_leaves_one_net_row(self):
        rollout = WorkerProfileRollout(
            2,
            (
                "UPDATE worker_profiles SET config=CAST(x'01' || substr(config,2) AS BLOB)",
                "UPDATE worker_profiles SET config=CAST(x'00' || substr(config,2) AS BLOB) "
                "WHERE id<>40000",
            ),
        )
        restore = RestoreWorkerProfiles(3, 2)
        _, peak = self.measure(self.path, [*self._schema_plan(), rollout])
        self.assertLessEqual(peak, 20 * 1024 * 1024)
        self.assertEqual(len(delta(self.path)), 1)
        self.assertEqual(delta(self.path)[0][1], 40000)
        self.assertEqual(delta(self.path)[0][2][:1], b"\x00")
        self.assertEqual(delta(self.path)[0][3][:1], b"\x01")
        self._check_first_byte_distribution(b"\x00", [40000])

        _, peak = self.measure(self.path, [*self._schema_plan(), rollout, restore])
        self.assertLessEqual(peak, 20 * 1024 * 1024)
        self._check_first_byte_distribution(b"\x00", [])
        self.assertEqual(len(delta(self.path)), 1)

    def test_large_dense_change_persisted_then_restored(self):
        rollout = WorkerProfileRollout(
            2,
            ("UPDATE worker_profiles SET config=CAST(x'02' || substr(config,2) AS BLOB)",),
        )
        restore = RestoreWorkerProfiles(3, 2)
        _, peak = self.measure(self.path, [*self._schema_plan(), rollout])
        self.assertLessEqual(peak, 20 * 1024 * 1024)

        rows = delta(self.path)
        self.assertEqual(len(rows), self.PROFILE_COUNT)
        self.assertEqual([row[1] for row in rows][:3], [1, 2, 3])
        self.assertTrue(all(row[0] == 2 for row in rows))
        self.assertTrue(all(row[2][:1] == b"\x00" for row in rows))
        self.assertTrue(all(row[3][:1] == b"\x02" for row in rows))
        with closing(sqlite3.connect(self.path)) as db:
            after_bytes = db.execute(
                "SELECT COUNT(*) FROM worker_profiles "
                "WHERE substr(config,1,1)=x'02'"
            ).fetchone()[0]
        self.assertEqual(after_bytes, self.PROFILE_COUNT)

        _, peak = self.measure(self.path, [*self._schema_plan(), rollout, restore])
        self.assertLessEqual(peak, 20 * 1024 * 1024)
        with closing(sqlite3.connect(self.path)) as db:
            restored = db.execute(
                "SELECT COUNT(*) FROM worker_profiles "
                "WHERE substr(config,1,1)=x'00'"
            ).fetchone()[0]
        self.assertEqual(restored, self.PROFILE_COUNT)
        self.assertEqual(len(delta(self.path)), self.PROFILE_COUNT)

    def _schema_plan(self):
        return [
            Migration(
                1,
                (
                    "CREATE TABLE worker_profiles("
                    "id INTEGER PRIMARY KEY, config BLOB NOT NULL)",
                    "CREATE TABLE job_profile_pins("
                    "job_id INTEGER PRIMARY KEY, "
                    "profile_id INTEGER NOT NULL REFERENCES worker_profiles(id) "
                    "ON DELETE RESTRICT)",
                    "CREATE INDEX idx_large_pins ON job_profile_pins(profile_id, job_id)",
                ),
            )
        ]


if __name__ == "__main__":
    unittest.main()
