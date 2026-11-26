from contextlib import closing
from itertools import combinations
from pathlib import Path
import random
import sqlite3
import tempfile
import tracemalloc
import unittest

import migrations
from migrations import (
    HistoryMismatch,
    Migration,
    OnlineUnique,
    OnlineMigrationRequired,
    ReleaseArtifactReclaim,
    apply_plan,
    prepare_online_unique,
    publish_online_unique,
    step_online_unique,
)


SCHEMA = Migration(
    1,
    (
        "CREATE TABLE releases("
        "id INTEGER PRIMARY KEY, retired INTEGER NOT NULL)",
        "CREATE TABLE blobs("
        "id INTEGER PRIMARY KEY, size_bytes INTEGER NOT NULL)",
        "CREATE TABLE release_blobs("
        "release_id INTEGER NOT NULL, blob_id INTEGER NOT NULL, "
        "PRIMARY KEY(release_id, blob_id), "
        "FOREIGN KEY(release_id) REFERENCES releases(id), "
        "FOREIGN KEY(blob_id) REFERENCES blobs(id))",
        "CREATE INDEX idx_release_blobs_blob_release "
        "ON release_blobs(blob_id, release_id)",
    ),
)


def ledger_versions(path):
    with closing(sqlite3.connect(path)) as db:
        return [row[0] for row in db.execute(
            "SELECT version FROM _migration_ledger ORDER BY version"
        )]


def catalog_rows(path):
    with closing(sqlite3.connect(path)) as db:
        releases = db.execute(
            "SELECT id,retired FROM releases ORDER BY id"
        ).fetchall()
        blobs = db.execute(
            "SELECT id,size_bytes FROM blobs ORDER BY id"
        ).fetchall()
        links = db.execute(
            "SELECT release_id,blob_id FROM release_blobs "
            "ORDER BY release_id,blob_id"
        ).fetchall()
    return releases, blobs, links


def seed_acceptance(path, extra_active_holder=False):
    with closing(sqlite3.connect(path)) as db, db:
        db.executemany(
            "INSERT INTO releases(id,retired) VALUES (?,?)",
            [(1, 0), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1)],
        )
        db.executemany(
            "INSERT INTO blobs(id,size_bytes) VALUES (?,?)",
            [
                (12, 1000),  # shared by active release 1 and candidate 2
                (20, 60),    # exclusive to candidate 2
                (30, 50),    # exclusive to candidate 3
                (45, 150),   # shared exclusively by candidates 4 and 5
                (46, 70),    # shared by candidate 4 and unlisted release 6
                (99, 5),     # historical orphan
            ],
        )
        links = [
            (1, 12), (2, 12),
            (2, 20),
            (3, 30),
            (4, 45), (5, 45),
            (4, 46), (6, 46),
        ]
        if extra_active_holder:
            links.append((1, 45))
        db.executemany("INSERT INTO release_blobs VALUES (?,?)", links)


class ReleaseReclaimTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name, "service.db")
        self.assertEqual(apply_plan(self.path, [SCHEMA]), [1])

    def tearDown(self):
        self.tmp.cleanup()

    def test_acceptance_batch_deletes_four_five_and_only_free_blob(self):
        seed_acceptance(self.path)
        phase = ReleaseArtifactReclaim(2, [5, 2, 4, 3], 2)

        self.assertEqual(apply_plan(self.path, [SCHEMA, phase]), [2])

        releases, blobs, links = catalog_rows(self.path)
        self.assertEqual(
            releases, [(1, 0), (2, 1), (3, 1), (6, 1)]
        )
        self.assertEqual(
            blobs,
            [(12, 1000), (20, 60), (30, 50), (46, 70), (99, 5)],
        )
        # Links belonging to retained holders are preserved.  The links from
        # deleted release 4 to the still-blocked shared blob are necessarily
        # removed with that release.
        self.assertEqual(
            links,
            [
                (1, 12),
                (2, 12), (2, 20),
                (3, 30),
                (6, 46),
            ],
        )
        self.assertEqual(ledger_versions(self.path), [1, 2])

    def test_active_holder_changes_optimal_batch_and_preserved_links(self):
        seed_acceptance(self.path, extra_active_holder=True)
        phase = ReleaseArtifactReclaim(2, [2, 3, 4, 5], 2)

        self.assertEqual(apply_plan(self.path, [SCHEMA, phase]), [2])

        releases, blobs, links = catalog_rows(self.path)
        self.assertEqual(
            releases, [(1, 0), (4, 1), (5, 1), (6, 1)]
        )
        self.assertEqual(
            blobs,
            [(12, 1000), (45, 150), (46, 70), (99, 5)],
        )
        self.assertEqual(
            links,
            [
                (1, 12), (1, 45),
                (4, 45), (4, 46),
                (5, 45),
                (6, 46),
            ],
        )

    def test_tie_breaks_by_count_then_lexicographic_ids(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executemany(
                "INSERT INTO releases(id,retired) VALUES (?,?)",
                [(2, 1), (3, 1), (4, 1), (5, 1)],
            )
            db.executemany(
                "INSERT INTO blobs(id,size_bytes) VALUES (?,?)",
                [(10, 100), (20, 100), (30, 100)],
            )
            db.executemany(
                "INSERT INTO release_blobs VALUES (?,?)",
                [(2, 10), (3, 20), (4, 20), (3, 30), (5, 30)],
            )

        # Benefit 100 is available from one release (2) or the pair (3,4);
        # fewer releases wins.
        phase = ReleaseArtifactReclaim(2, [2, 3, 4, 5], 2)
        apply_plan(self.path, [SCHEMA, phase])
        releases, blobs, _ = catalog_rows(self.path)
        self.assertEqual([row[0] for row in releases], [3, 4, 5])
        self.assertEqual([row[0] for row in blobs], [20, 30])

    def test_lexicographic_tie_uses_sorted_numeric_ids(self):
        other = Path(self.tmp.name, "lex.db")
        self.assertEqual(apply_plan(other, [SCHEMA]), [1])
        with closing(sqlite3.connect(other)) as db, db:
            db.executemany(
                "INSERT INTO releases(id,retired) VALUES (?,?)",
                [(2, 1), (3, 1), (4, 1), (5, 1)],
            )
            db.executemany(
                "INSERT INTO blobs(id,size_bytes) VALUES (?,?)",
                [(25, 100), (34, 100)],
            )
            db.executemany(
                "INSERT INTO release_blobs VALUES (?,?)",
                [(2, 25), (5, 25), (3, 34), (4, 34)],
            )

        phase = ReleaseArtifactReclaim(2, [5, 4, 3, 2], 2)
        apply_plan(other, [SCHEMA, phase])
        releases, blobs, _ = catalog_rows(other)
        self.assertEqual([row[0] for row in releases], [3, 4])
        self.assertEqual([row[0] for row in blobs], [34])

    def test_zero_benefit_deletes_nothing_but_records_phase(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executemany(
                "INSERT INTO releases(id,retired) VALUES (?,?)",
                [(1, 0), (2, 1), (3, 1)],
            )
            db.executemany(
                "INSERT INTO blobs(id,size_bytes) VALUES (?,?)",
                [(10, 7), (11, 9)],
            )
            db.executemany(
                "INSERT INTO release_blobs VALUES (?,?)",
                [(1, 10), (2, 10)],
            )

        before = catalog_rows(self.path)
        phase = ReleaseArtifactReclaim(2, [2, 3, 999], 2)
        self.assertEqual(apply_plan(self.path, [SCHEMA, phase]), [2])
        after = catalog_rows(self.path)
        self.assertEqual(before, after)
        self.assertEqual(ledger_versions(self.path), [1, 2])

    def test_ineligible_and_missing_candidates_do_not_use_quota(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.executemany(
                "INSERT INTO releases(id,retired) VALUES (?,?)",
                [(1, 0), (2, 1), (3, 1)],
            )
            db.executemany(
                "INSERT INTO blobs(id,size_bytes) VALUES (?,?)",
                [(20, 50), (30, 60)],
            )
            db.executemany(
                "INSERT INTO release_blobs VALUES (?,?)",
                [(2, 20), (3, 30)],
            )

        phase = ReleaseArtifactReclaim(2, [1, 999, 2, 3], 1)
        apply_plan(self.path, [SCHEMA, phase])
        releases, blobs, _ = catalog_rows(self.path)
        self.assertEqual([row[0] for row in releases], [1, 2])
        self.assertEqual([row[0] for row in blobs], [20])

    def test_quota_can_exceed_candidate_count(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("INSERT INTO releases VALUES (2,1)")
            db.execute("INSERT INTO blobs VALUES (20,60)")
            db.execute("INSERT INTO release_blobs VALUES (2,20)")

        phase = ReleaseArtifactReclaim(2, [2], 8)
        apply_plan(self.path, [SCHEMA, phase])
        releases, blobs, _ = catalog_rows(self.path)
        self.assertEqual(releases, [])
        self.assertEqual(blobs, [])

    def test_validation_and_order_insensitive_checksum(self):
        valid = ReleaseArtifactReclaim(3, [3, 1, 2], 8)
        self.assertEqual(valid.candidate_ids, (1, 2, 3))
        self.assertEqual(
            valid.checksum(),
            ReleaseArtifactReclaim(3, (2, 3, 1), 8).checksum(),
        )
        self.assertNotEqual(
            valid.checksum(),
            ReleaseArtifactReclaim(3, [1, 2, 4], 8).checksum(),
        )
        self.assertNotEqual(
            valid.checksum(),
            ReleaseArtifactReclaim(3, [1, 2, 3], 7).checksum(),
        )
        self.assertNotEqual(
            valid.checksum(),
            Migration(3, ()).checksum(),
        )

        invalid = [
            lambda: ReleaseArtifactReclaim(0, [1], 1),
            lambda: ReleaseArtifactReclaim(True, [1], 1),
            lambda: ReleaseArtifactReclaim(1.5, [1], 1),
            lambda: ReleaseArtifactReclaim(1, [], 1),
            lambda: ReleaseArtifactReclaim(1, list(range(21)), 1),
            lambda: ReleaseArtifactReclaim(1, [1, 1], 1),
            lambda: ReleaseArtifactReclaim(1, [True], 1),
            lambda: ReleaseArtifactReclaim(1, [0], 1),
            lambda: ReleaseArtifactReclaim(1, [-2], 1),
            lambda: ReleaseArtifactReclaim(1, [2 ** 63], 1),
            lambda: ReleaseArtifactReclaim(1, "12", 1),
            lambda: ReleaseArtifactReclaim(1, [1], 0),
            lambda: ReleaseArtifactReclaim(1, [1], 9),
            lambda: ReleaseArtifactReclaim(1, [1], True),
            lambda: ReleaseArtifactReclaim(1, [1], 1.0),
        ]
        for factory in invalid:
            with self.assertRaises(ValueError):
                factory()

        with self.assertRaises(ValueError):
            apply_plan(self.path, [ReleaseArtifactReclaim(2, [1], 1)])

    def test_replay_checksum_drift_and_reordered_list(self):
        seed_acceptance(self.path)
        phase = ReleaseArtifactReclaim(2, [2, 3, 4, 5], 2)
        plan = [SCHEMA, phase]
        self.assertEqual(apply_plan(self.path, plan), [2])
        self.assertEqual(apply_plan(self.path, plan), [])

        reordered = ReleaseArtifactReclaim(2, [5, 4, 3, 2], 2)
        self.assertEqual(apply_plan(self.path, [SCHEMA, reordered]), [])

        changed_candidates = ReleaseArtifactReclaim(2, [2, 3, 4], 2)
        with self.assertRaises(HistoryMismatch):
            apply_plan(self.path, [SCHEMA, changed_candidates])
        changed_quota = ReleaseArtifactReclaim(2, [2, 3, 4, 5], 1)
        with self.assertRaises(HistoryMismatch):
            apply_plan(self.path, [SCHEMA, changed_quota])

    def test_previous_sql_in_same_batch_is_used(self):
        seed = Migration(
            2,
            (
                "INSERT INTO releases(id,retired) VALUES (2,0)",
                "INSERT INTO blobs(id,size_bytes) VALUES (20,60)",
                "INSERT INTO release_blobs VALUES (2,20)",
            ),
        )
        retire = Migration(
            3, ("UPDATE releases SET retired=1 WHERE id=2",)
        )
        reclaim = ReleaseArtifactReclaim(4, [2], 1)
        self.assertEqual(
            apply_plan(self.path, [SCHEMA, seed, retire, reclaim]),
            [2, 3, 4],
        )
        releases, blobs, links = catalog_rows(self.path)
        self.assertEqual(releases, [])
        self.assertEqual(blobs, [])
        self.assertEqual(links, [])

    def test_later_failure_rolls_back_catalog_deletes_and_ledger(self):
        seed = Migration(
            2,
            (
                "INSERT INTO releases(id,retired) VALUES (2,1)",
                "INSERT INTO blobs(id,size_bytes) VALUES (20,60)",
                "INSERT INTO release_blobs VALUES (2,20)",
            ),
        )
        self.assertEqual(apply_plan(self.path, [SCHEMA, seed]), [2])

        reclaim = ReleaseArtifactReclaim(3, [2], 1)
        broken = Migration(4, ("INSERT INTO definitely_missing VALUES (1)",))
        with self.assertRaises(sqlite3.OperationalError):
            apply_plan(self.path, [SCHEMA, seed, reclaim, broken])

        self.assertEqual(ledger_versions(self.path), [1, 2])
        releases, blobs, links = catalog_rows(self.path)
        self.assertEqual(releases, [(2, 1)])
        self.assertEqual(blobs, [(20, 60)])
        self.assertEqual(links, [(2, 20)])

        fixed = Migration(4, ("CREATE TABLE repaired_after_reclaim(id INTEGER PRIMARY KEY)",))
        self.assertEqual(
            apply_plan(self.path, [SCHEMA, seed, reclaim, fixed]), [3, 4]
        )
        self.assertEqual(
            apply_plan(self.path, [SCHEMA, seed, reclaim, fixed]), []
        )
        releases, blobs, links = catalog_rows(self.path)
        self.assertEqual(releases, [])
        self.assertEqual(blobs, [])
        self.assertEqual(links, [])
        self.assertEqual(ledger_versions(self.path), [1, 2, 3, 4])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertIsNotNone(
                db.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='repaired_after_reclaim'"
                ).fetchone()
            )

    def test_runs_after_published_online_unique_and_unpublished_blocks(self):
        online_path = Path(self.tmp.name, "online.db")
        online_schema = Migration(
            1,
            SCHEMA.statements
            + (
                "CREATE TABLE routes("
                "id INTEGER PRIMARY KEY, route_key TEXT COLLATE BINARY)",
            ),
        )
        online = OnlineUnique(2, "routes")
        reclaim = ReleaseArtifactReclaim(3, [2], 1)
        online_plan = [online_schema, online]
        full_plan = [online_schema, online, reclaim]

        self.assertEqual(apply_plan(online_path, [online_schema]), [1])
        self.assertIs(prepare_online_unique(online_path, online_plan), True)
        self.assertEqual(step_online_unique(online_path, online_plan, 10), 0)
        self.assertEqual(publish_online_unique(online_path, online_plan),
                         ("published", None))

        with closing(sqlite3.connect(online_path)) as db, db:
            db.execute("INSERT INTO releases VALUES (2,1)")
            db.execute("INSERT INTO blobs VALUES (20,60)")
            db.execute("INSERT INTO release_blobs VALUES (2,20)")

        self.assertEqual(apply_plan(online_path, full_plan), [3])
        _, blobs, links = catalog_rows(online_path)
        self.assertEqual(blobs, [])
        self.assertEqual(links, [])

        blocked_path = Path(self.tmp.name, "blocked-online.db")
        self.assertEqual(apply_plan(blocked_path, [online_schema]), [1])
        self.assertIs(prepare_online_unique(blocked_path, online_plan), True)
        with closing(sqlite3.connect(blocked_path)) as db, db:
            db.execute("INSERT INTO releases VALUES (2,1)")
            db.execute("INSERT INTO blobs VALUES (20,60)")
            db.execute("INSERT INTO release_blobs VALUES (2,20)")
        with self.assertRaises(OnlineMigrationRequired):
            apply_plan(blocked_path, full_plan)
        _, blobs, links = catalog_rows(blocked_path)
        self.assertEqual(blobs, [(20, 60)])
        self.assertEqual(links, [(2, 20)])
        self.assertEqual(ledger_versions(blocked_path), [1])

    def test_random_small_catalogs_match_brute_force_optimizer(self):
        rng = random.Random(225225)
        for trial in range(60):
            count = rng.randint(1, 10)
            quota = rng.randint(1, min(8, count))
            candidate_ids = sorted(rng.sample(range(1, 100), count))
            external_id = 1000 + trial
            db = sqlite3.connect(":memory:")
            try:
                db.executescript(
                    """
                    CREATE TABLE releases(
                        id INTEGER PRIMARY KEY, retired INTEGER NOT NULL);
                    CREATE TABLE blobs(
                        id INTEGER PRIMARY KEY, size_bytes INTEGER NOT NULL);
                    CREATE TABLE release_blobs(
                        release_id INTEGER NOT NULL,
                        blob_id INTEGER NOT NULL,
                        PRIMARY KEY(release_id, blob_id));
                    """
                )
                db.executemany(
                    "INSERT INTO releases VALUES (?,1)",
                    [(release_id,) for release_id in candidate_ids]
                    + [(external_id,)],
                )

                weights = {}
                blob_id = 1
                for _ in range(rng.randint(1, 120)):
                    holder_count = rng.randint(1, count)
                    holders = set(rng.sample(candidate_ids, holder_count))
                    blocked = rng.random() < 0.35
                    if blocked:
                        holders.add(external_id)
                    size = rng.randint(1, 10000)
                    db.execute(
                        "INSERT INTO blobs VALUES (?,?)", (blob_id, size)
                    )
                    db.executemany(
                        "INSERT INTO release_blobs VALUES (?,?)",
                        [(holder, blob_id) for holder in holders],
                    )
                    if not blocked:
                        mask = 0
                        for index, release_id in enumerate(candidate_ids):
                            if release_id in holders:
                                mask |= 1 << index
                        weights[mask] = weights.get(mask, 0) + size
                    blob_id += 1

                expected_benefit = 0
                expected_count = 0
                expected_ids = ()
                for chosen_count in range(quota + 1):
                    for indices in combinations(range(count), chosen_count):
                        mask = 0
                        for index in indices:
                            mask |= 1 << index
                        benefit = sum(
                            size
                            for holder_mask, size in weights.items()
                            if (holder_mask & mask) == holder_mask
                        )
                        chosen_ids = tuple(candidate_ids[i] for i in indices)
                        if (
                            benefit > expected_benefit
                            or (
                                benefit == expected_benefit
                                and (
                                    chosen_count < expected_count
                                    or (
                                        chosen_count == expected_count
                                        and chosen_ids < expected_ids
                                    )
                                )
                            )
                        ):
                            expected_benefit = benefit
                            expected_count = chosen_count
                            expected_ids = chosen_ids

                phase = ReleaseArtifactReclaim(
                    1, list(reversed(candidate_ids)), quota
                )
                selected_ids, selected_mask = (
                    migrations._select_release_reclaim(db, phase)
                )
                self.assertEqual(selected_ids, expected_ids)
                self.assertEqual(
                    selected_mask,
                    sum(1 << candidate_ids.index(rid) for rid in expected_ids),
                )
            finally:
                db.close()

    def test_large_synthetic_batch_is_correct_and_memory_bounded(self):
        large_path = Path(self.tmp.name, "large.db")
        with closing(sqlite3.connect(large_path)) as db:
            db.execute("PRAGMA foreign_keys=ON")
            db.executescript(
                """
                CREATE TABLE releases(
                    id INTEGER PRIMARY KEY, retired INTEGER NOT NULL);
                CREATE TABLE blobs(
                    id INTEGER PRIMARY KEY, size_bytes INTEGER NOT NULL);
                CREATE TABLE release_blobs(
                    release_id INTEGER NOT NULL,
                    blob_id INTEGER NOT NULL,
                    PRIMARY KEY(release_id, blob_id),
                    FOREIGN KEY(release_id) REFERENCES releases(id),
                    FOREIGN KEY(blob_id) REFERENCES blobs(id));
                CREATE INDEX idx_large_blob_release
                    ON release_blobs(blob_id, release_id);
                """
            )
            db.executemany(
                "INSERT INTO releases(id,retired) VALUES (?,?)",
                [(release_id, 1) for release_id in range(1, 21)]
                + [(21, 0)],
            )
            # 100,000 background blobs, each with eight candidate holders:
            # exactly 800,000 candidate links.  Every one also has noncandidate
            # release 21 as a holder, so all of these bytes are blocked.
            db.execute(
                """
                WITH RECURSIVE counter(x) AS (
                    SELECT 1
                    UNION ALL
                    SELECT x + 1 FROM counter WHERE x < 199999
                )
                INSERT INTO blobs(id, size_bytes)
                SELECT x, CASE WHEN x <= 100000 THEN 1 + (x % 997) ELSE 1 END
                FROM counter
                """
            )
            db.execute(
                """
                INSERT INTO release_blobs(release_id, blob_id)
                SELECT 1 + ((b.id + offsets.n) % 20), b.id
                FROM blobs b
                JOIN (
                    SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2
                    UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
                    UNION ALL SELECT 6 UNION ALL SELECT 7
                ) offsets
                WHERE b.id <= 100000
                """
            )
            db.execute(
                "INSERT INTO release_blobs(release_id, blob_id) "
                "SELECT 21, id FROM blobs WHERE id <= 100000"
            )
            # Many unblocked singleton blobs, all held by one of the three
            # intended jackpot releases.  They add score-array/cursor volume
            # without changing the minimal winning set.
            db.execute(
                """
                INSERT INTO release_blobs(release_id, blob_id)
                SELECT CASE WHEN id % 3 = 0 THEN 3
                            WHEN id % 3 = 1 THEN 7
                            ELSE 12 END,
                       id
                FROM blobs
                WHERE id BETWEEN 100001 AND 199999
                """
            )
            db.execute(
                "INSERT INTO blobs(id,size_bytes) VALUES (200000,1000000000)"
            )
            db.executemany(
                "INSERT INTO release_blobs(release_id,blob_id) VALUES (?,?)",
                [(3, 200000), (7, 200000), (12, 200000)],
            )
            db.commit()

        phase = ReleaseArtifactReclaim(1, list(range(1, 21)), 8)

        # Start/reset only after synthetic data generation so the bound
        # concerns new phase allocations, not fixture construction.
        started_here = not tracemalloc.is_tracing()
        if started_here:
            tracemalloc.start()
        if hasattr(tracemalloc, "reset_peak"):  # Python 3.9+
            tracemalloc.reset_peak()
        applied = apply_plan(large_path, [phase])
        _, peak = tracemalloc.get_traced_memory()
        if started_here:
            tracemalloc.stop()

        self.assertEqual(applied, [1])
        self.assertLessEqual(peak, 32 * 1024 * 1024,
                             "tracemalloc peak was %d bytes" % peak)

        with closing(sqlite3.connect(large_path)) as db:
            remaining_releases = [
                row[0] for row in db.execute(
                    "SELECT id FROM releases ORDER BY id"
                )
            ]
            blob_count = db.execute("SELECT COUNT(*) FROM blobs").fetchone()[0]
            blocked_holder_links = db.execute(
                "SELECT COUNT(*) FROM release_blobs WHERE release_id=21"
            ).fetchone()[0]
            jackpot = db.execute(
                "SELECT COUNT(*) FROM blobs WHERE id=200000"
            ).fetchone()[0]
            total_links = db.execute(
                "SELECT COUNT(*) FROM release_blobs"
            ).fetchone()[0]

        self.assertNotIn(3, remaining_releases)
        self.assertNotIn(7, remaining_releases)
        self.assertNotIn(12, remaining_releases)
        self.assertIn(21, remaining_releases)
        self.assertEqual(len(remaining_releases), 18)
        self.assertEqual(blob_count, 100000)
        self.assertEqual(blocked_holder_links, 100000)
        self.assertEqual(jackpot, 0)
        # Each candidate has 40,000 blocked-background links, so deleting
        # releases 3, 7 and 12 removes 120,000 of the 800,000 candidate links.
        # All 99,999 singleton links and the three jackpot links are removed
        # too.  What remains is 680,000 background candidate links plus
        # 100,000 noncandidate holder links.
        self.assertEqual(total_links, 780000)


if __name__ == "__main__":
    unittest.main()
