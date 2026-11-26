"""Tests for the resumable route_catalog change feed."""
from contextlib import closing
from pathlib import Path
import multiprocessing
import sqlite3
import tracemalloc
from tempfile import TemporaryDirectory
import unittest

from migrations import (
    Migration,
    RouteFeedFull,
    RouteFeedState,
    ack_route_changes,
    apply_plan,
    finish_route_snapshot,
    open_route_feed,
    preview_plan,
    read_route_changes,
    read_route_snapshot,
)

# Tracemalloc ceiling for feed calls: the contract allows 12 MiB of new
# Python allocations per call; keep a safety margin below it.
MEMORY_LIMIT = 12 * 1024 * 1024


def make_catalog(path, rows):
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "CREATE TABLE route_catalog("
            "id INTEGER PRIMARY KEY, payload BLOB NOT NULL)"
        )
        db.executemany(
            "INSERT INTO route_catalog(id,payload) VALUES (?,?)", rows
        )
        db.execute(
            "CREATE TABLE deployment_markers(id INTEGER PRIMARY KEY)"
        )


def live_catalog(path):
    with closing(sqlite3.connect(path)) as db:
        return dict(
            (row_id, bytes(payload))
            for row_id, payload in db.execute(
                "SELECT id,payload FROM route_catalog"
            )
        )


class RouteFeedTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "routes.db")
        make_catalog(
            self.path,
            [(10, b"ten"), (30, b"thirty"), (50, b"fifty")],
        )
        self.v1 = Migration(
            1, ("CREATE TABLE ledger_anchor(id INTEGER PRIMARY KEY)",)
        )
        self.assertEqual(apply_plan(self.path, [self.v1]), [1])

    def tearDown(self):
        self._tmp.cleanup()

    def _read_all_snapshot(self, page_size=256):
        pages = []
        after = 0
        while True:
            rows, next_after = read_route_snapshot(self.path, after, page_size)
            pages.append(rows)
            if next_after is None:
                self.assertEqual(rows, [])
                break
            after = next_after
        return [row for page in pages for row in page]

    def test_one_cut_point_per_load_across_process_reopen(self):
        # Acceptance 1: paging never mixes states; progress survives reopen.
        base = open_route_feed(self.path, 3)
        self.assertEqual(base, 1)
        # Reopening with the same quota returns the same cut point and keeps
        # all progress.
        self.assertEqual(open_route_feed(self.path, 3), 1)

        rows, next_after = read_route_snapshot(self.path, 0, 1)
        self.assertEqual(rows, [(10, b"ten")])
        self.assertEqual(next_after, 10)
        # Re-reading the same cursor yields the same immutable page.
        self.assertEqual(
            read_route_snapshot(self.path, 0, 1), ([(10, b"ten")], 10)
        )

        # One real commit: rewrite 10, delete 30, insert 20, then change 50
        # back in a later phase of the same plan call.
        plan = [
            self.v1,
            Migration(2, (
                "UPDATE route_catalog SET payload=x'74656e2d78' WHERE id=10",
                "DELETE FROM route_catalog WHERE id=30",
                "INSERT INTO route_catalog(id,payload) "
                "VALUES (20, x'7477656e7479')",
            )),
            Migration(3, (
                "UPDATE route_catalog SET payload=x'6669667479' WHERE id=50",
            )),
        ]
        self.assertEqual(apply_plan(self.path, plan), [2, 3])

        # An independent reader already sees the post-commit live table.
        live = live_catalog(self.path)
        self.assertEqual(set(live), {10, 20, 50})
        self.assertEqual(live[10], b"ten-x")
        self.assertEqual(live[20], b"twenty")

        # "Restart the process": every call opens its own connection anyway.
        rows, next_after = read_route_snapshot(self.path, 10, 256)
        self.assertEqual(rows, [(30, b"thirty"), (50, b"fifty")])
        self.assertEqual(next_after, 50)
        rows, next_after = read_route_snapshot(self.path, 50, 256)
        self.assertEqual(rows, [])
        self.assertIsNone(next_after)

        # No changes before the snapshot is confirmed.
        with self.assertRaises(RouteFeedState):
            read_route_changes(self.path)

        self.assertTrue(finish_route_snapshot(self.path, 1))
        self.assertFalse(finish_route_snapshot(self.path, 1))
        # The retained snapshot rows are released.
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM _route_feed_snapshot"
                ).fetchone()[0],
                0,
            )
        with self.assertRaises(RouteFeedState):
            read_route_snapshot(self.path, 0, 10)

        # One complete batch at the highest registered version.
        version, entries = read_route_changes(self.path)
        self.assertEqual(version, 3)
        self.assertEqual(
            entries, [(10, b"ten-x"), (20, b"twenty"), (30, None)]
        )

        # Reopen before ack: the same unacked batch is delivered again.
        self.assertEqual(
            read_route_changes(self.path), (3, entries)
        )

        # Replay snapshot + batch against an independent cache, then compare
        # byte-for-byte with the live table.
        cache = {10: b"ten", 30: b"thirty", 50: b"fifty"}
        for row_id, payload in entries:
            if payload is None:
                del cache[row_id]
            else:
                cache[row_id] = payload
        self.assertEqual(cache, live_catalog(self.path))

    def test_quota_gates_real_commits_and_only_ack_releases_it(self):
        # Acceptance 2, quota 3: the first batch's three net changes fill it.
        open_route_feed(self.path, 3)
        plan = [
            self.v1,
            Migration(2, (
                "UPDATE route_catalog SET payload=x'74656e2d78' WHERE id=10",
                "DELETE FROM route_catalog WHERE id=30",
                "INSERT INTO route_catalog(id,payload) "
                "VALUES (20, x'7477656e7479')",
            )),
        ]
        self.assertEqual(apply_plan(self.path, plan), [2])
        self.assertTrue(finish_route_snapshot(self.path, 1))
        version, entries = read_route_changes(self.path)
        self.assertEqual((version, len(entries)), (2, 3))

        # A plan that exceeds the quota is rejected identically by the preview
        # and by the real application: both judge quota on the same state and
        # raise RouteFeedFull; the preview leaves nothing behind.
        next_plan = plan + [
            Migration(3, (
                "INSERT INTO deployment_markers(id) VALUES (7)",
                "UPDATE route_catalog SET payload=x'66696674792d32' "
                "WHERE id=50",
            ))
        ]
        with self.assertRaises(RouteFeedFull) as preview_caught:
            preview_plan(self.path, next_plan)
        self.assertEqual(preview_caught.exception.needed, 1)
        self.assertEqual(preview_caught.exception.available, 0)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT MAX(version) FROM _migration_ledger"
                ).fetchone()[0],
                2,
            )
        with self.assertRaises(RouteFeedFull) as caught:
            apply_plan(self.path, next_plan)
        self.assertEqual(caught.exception.needed, 1)
        self.assertEqual(caught.exception.available, 0)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT MAX(version) FROM _migration_ledger"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM deployment_markers"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                db.execute(
                    "SELECT payload FROM route_catalog WHERE id=50"
                ).fetchone()[0],
                b"fifty",
            )

        # Skipping the earliest batch cannot advance anything.
        with self.assertRaises(RouteFeedState):
            ack_route_changes(self.path, 3)
        with self.assertRaises(RouteFeedState):
            ack_route_changes(self.path, 99)

        # Ack only, then the unchanged plan really commits.
        self.assertTrue(ack_route_changes(self.path, 2))
        self.assertFalse(ack_route_changes(self.path, 2))
        self.assertEqual(apply_plan(self.path, next_plan), [3])
        version, entries = read_route_changes(self.path)
        self.assertEqual(version, 3)
        self.assertEqual(entries, [(50, b"fifty-2")])
        self.assertTrue(ack_route_changes(self.path, 3))

        # Exact replay registers no version and publishes no batch.
        self.assertEqual(apply_plan(self.path, next_plan), [])
        self.assertIsNone(read_route_changes(self.path))

    def test_net_zero_preview_and_failure_semantics(self):
        open_route_feed(self.path, 1)  # quota of one, quickly exhausted
        plan = [
            self.v1,
            Migration(2, ("UPDATE route_catalog SET payload=x'01' WHERE id=10",)),
        ]
        self.assertEqual(apply_plan(self.path, plan), [2])
        self.assertTrue(finish_route_snapshot(self.path, 1))
        version, entries = read_route_changes(self.path)
        self.assertEqual(entries, [(10, b"\x01")])
        self.assertTrue(ack_route_changes(self.path, version))

        # Fill the quota again so available == 0.
        plan2 = plan + [
            Migration(3, ("UPDATE route_catalog SET payload=x'02' WHERE id=10",)),
        ]
        self.assertEqual(apply_plan(self.path, plan2), [3])
        version, _ = read_route_changes(self.path)
        self.assertEqual(version, 3)
        # Acking releases the slot; later checks pin available back to zero
        # with a fresh unacked batch before the net-zero commit.
        self.assertTrue(ack_route_changes(self.path, 3))
        plan2b = plan2 + [
            Migration(4, ("UPDATE route_catalog SET payload=x'02' WHERE id=10",)),
        ]
        # Exact same value: net zero, registers the version but no batch.
        self.assertEqual(apply_plan(self.path, plan2b), [4])
        self.assertIsNone(read_route_changes(self.path))

        # Re-fill the quota to zero available with a real change.
        plan2c = plan2b + [
            Migration(5, ("UPDATE route_catalog SET payload=x'05' WHERE id=10",)),
        ]
        self.assertEqual(apply_plan(self.path, plan2c), [5])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT used_pending FROM _route_feed_meta"
                ).fetchone()[0],
                1,
            )

        # A net-zero plan still commits at zero available quota.
        net_zero = plan2c + [
            Migration(6, (
                "UPDATE route_catalog SET payload=x'03' WHERE id=10",
                "UPDATE route_catalog SET payload=x'05' WHERE id=10",
            )),
        ]
        self.assertEqual(apply_plan(self.path, net_zero), [6])
        # The earlier unacked batch is untouched and no new batch appeared.
        self.assertEqual(read_route_changes(self.path)[0], 5)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT used_pending FROM _route_feed_meta"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                db.execute(
                    "SELECT payload FROM route_catalog WHERE id=10"
                ).fetchone()[0],
                b"\x05",
            )

        # Ack the held batch; a successful preview of a fitting change then
        # consumes no quota and leaves nothing behind.
        self.assertTrue(ack_route_changes(self.path, 5))
        preview = net_zero + [
            Migration(7, ("UPDATE route_catalog SET payload=x'04' WHERE id=10",)),
        ]
        self.assertEqual(preview_plan(self.path, preview), [7])
        self.assertIsNone(read_route_changes(self.path))
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT used_pending FROM _route_feed_meta"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM _migration_ledger"
                ).fetchone()[0],
                6,
            )

        # Re-fill to full, then a failing statement rolls back SQL, ledger and
        # channel together.
        self.assertEqual(apply_plan(self.path, preview), [7])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT used_pending FROM _route_feed_meta"
                ).fetchone()[0],
                1,
            )
        broken = preview + [
            Migration(8, (
                "UPDATE route_catalog SET payload=x'08' WHERE id=10",
                "INSERT INTO no_such_table VALUES (1)",
            )),
        ]
        with self.assertRaises(sqlite3.OperationalError):
            apply_plan(self.path, broken)
        self.assertEqual(
            read_route_changes(self.path)[0], 7
        )
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM _migration_ledger"
                ).fetchone()[0],
                7,
            )
            self.assertEqual(
                db.execute(
                    "SELECT payload FROM route_catalog WHERE id=10"
                ).fetchone()[0],
                b"\x04",
            )
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM _route_feed_batches"
                ).fetchone()[0],
                1,
            )

    def test_batches_track_successive_commits_and_blob_semantics(self):
        open_route_feed(self.path, 16384)
        self.assertTrue(finish_route_snapshot(self.path, 1))

        plan = [
            self.v1,
            Migration(2, (
                "INSERT INTO route_catalog(id,payload) VALUES (5, x'00')",
                "UPDATE route_catalog SET payload=x'74656e2d32' WHERE id=10",
            )),
        ]
        self.assertEqual(apply_plan(self.path, plan), [2])
        version, entries = read_route_changes(self.path)
        self.assertEqual(version, 2)
        self.assertEqual(entries, [(5, b"\x00"), (10, b"ten-2")])
        self.assertTrue(ack_route_changes(self.path, 2))

        # One plan call spanning two phases still yields exactly one batch:
        # the net change between the states before and after the whole call,
        # labelled with the highest registered version.
        plan2 = plan + [
            Migration(3, (
                "DELETE FROM route_catalog WHERE id=5",
                "UPDATE route_catalog SET payload=x'' WHERE id=30",
            )),
            Migration(4, (
                "INSERT INTO route_catalog(id,payload) VALUES (5, x'')",
                "UPDATE route_catalog SET payload=x'' WHERE id=10",
            )),
        ]
        self.assertEqual(apply_plan(self.path, plan2), [3, 4])
        version, entries = read_route_changes(self.path)
        self.assertEqual(version, 4)
        # id 5 was deleted then recreated within the call: only its final
        # value is delivered, alongside the other net changes.
        self.assertEqual(entries, [(5, b""), (10, b""), (30, b"")])
        self.assertTrue(ack_route_changes(self.path, 4))
        self.assertIsNone(read_route_changes(self.path))

        # Intermediate states are never exposed: rewrite, then restore within
        # one call publishes nothing for that id.
        plan3 = plan2 + [
            Migration(5, (
                "UPDATE route_catalog SET payload=x'55' WHERE id=30",
                "UPDATE route_catalog SET payload=x'' WHERE id=30",
            )),
        ]
        self.assertEqual(apply_plan(self.path, plan3), [5])
        self.assertIsNone(read_route_changes(self.path))

    def test_parameter_validation_and_lifecycle_states(self):
        # Booleans are never accepted as integer parameters.
        with self.assertRaises(ValueError):
            open_route_feed(self.path, True)
        with self.assertRaises(ValueError):
            open_route_feed(self.path, 0)
        with self.assertRaises(ValueError):
            open_route_feed(self.path, 16385)

        # No feed, no service.
        with self.assertRaises(RouteFeedState):
            read_route_snapshot(self.path, 0, 10)
        with self.assertRaises(RouteFeedState):
            finish_route_snapshot(self.path, 1)
        with self.assertRaises(RouteFeedState):
            read_route_changes(self.path)
        with self.assertRaises(RouteFeedState):
            ack_route_changes(self.path, 1)

        open_route_feed(self.path, 4)
        with self.assertRaises(ValueError):
            read_route_snapshot(self.path, True, 10)
        with self.assertRaises(ValueError):
            read_route_snapshot(self.path, -1, 10)
        with self.assertRaises(ValueError):
            read_route_snapshot(self.path, 0, 0)
        with self.assertRaises(ValueError):
            read_route_snapshot(self.path, 0, 257)
        with self.assertRaises(ValueError):
            finish_route_snapshot(self.path, True)
        with self.assertRaises(ValueError):
            ack_route_changes(self.path, 0)
        with self.assertRaises(ValueError):
            ack_route_changes(self.path, False)

        # Wrong cut point and unknown feed operations.
        with self.assertRaises(RouteFeedState):
            finish_route_snapshot(self.path, 2)
        with self.assertRaises(RouteFeedState):
            ack_route_changes(self.path, 1)

    def test_large_catalog_paging_boundaries_and_memory(self):
        # Acceptance 3: >= 60k rows, >= 320-byte payloads.
        with TemporaryDirectory() as tmp:
            big_path = Path(tmp, "big.db")
            payload = bytes((i % 251 for i in range(320)))
            size = 60000
            with closing(sqlite3.connect(big_path)) as db, db:
                db.execute(
                    "CREATE TABLE route_catalog("
                    "id INTEGER PRIMARY KEY, payload BLOB NOT NULL)"
                )
                db.executemany(
                    "INSERT INTO route_catalog(id,payload) VALUES (?,?)",
                    ((i, payload) for i in range(1, size + 1)),
                )
            anchor = Migration(1, ("SELECT 1",))
            anchor = Migration(
                1, ("CREATE TABLE ledger_anchor(id INTEGER PRIMARY KEY)",)
            )
            self.assertEqual(apply_plan(big_path, [anchor]), [1])

            started_here = not tracemalloc.is_tracing()
            if started_here:
                tracemalloc.start()

            def new_allocation_bytes(call):
                # The 12 MiB ceiling covers allocations made by one call.
                # Compare two snapshots: allocations that predate the call
                # (e.g. the accumulated paging list) are not charged to it.
                before = tracemalloc.take_snapshot()
                result = call()
                after = tracemalloc.take_snapshot()
                growth = sum(
                    stat.size_diff
                    for stat in after.compare_to(before, "lineno")
                    if stat.size_diff > 0
                )
                return result, growth

            base, open_growth = new_allocation_bytes(
                lambda: open_route_feed(big_path, 16384)
            )
            self.assertEqual(base, 1)
            self.assertLess(open_growth, MEMORY_LIMIT)

            # Actual paging with a small page size over the full cut point.
            # Memory is measured per call on a bounded sample of pages at the
            # start, middle and end; the remaining pages verify correctness
            # without the snapshot overhead.
            seen = []
            after = 0
            page_size = 7
            measured_pages = 0
            worst_page_growth = 0
            page_index = 0
            measure_at = {0, size // page_size // 2,
                          (size + page_size - 1) // page_size - 1}
            while True:
                if page_index in measure_at:
                    page, page_growth = new_allocation_bytes(
                        lambda after=after: read_route_snapshot(
                            big_path, after, page_size
                        )
                    )
                    worst_page_growth = max(worst_page_growth, page_growth)
                    measured_pages += 1
                else:
                    page = read_route_snapshot(big_path, after, page_size)
                rows, next_after = page
                self.assertLessEqual(len(rows), page_size)
                seen.extend(rows)
                if next_after is None:
                    break
                after = next_after
                page_index += 1
            self.assertEqual(measured_pages, len(measure_at))
            self.assertEqual(len(seen), size)
            self.assertEqual([row[0] for row in seen], list(range(1, size + 1)))
            self.assertTrue(all(len(row[1]) == 320 for row in seen))
            self.assertLess(worst_page_growth, MEMORY_LIMIT)

            # Boundary churn: delete/rewrite around page edges and insert a
            # new id at the trailing boundary; snapshot pages must stay
            # exactly at the cut point.
            plan = [
                anchor,
                Migration(2, (
                    "DELETE FROM route_catalog WHERE id IN (7,8,14)",
                    "UPDATE route_catalog SET payload=x'aa' WHERE id=15",
                    "INSERT INTO route_catalog(id,payload) "
                    "VALUES (60001, x'bb')",
                )),
            ]
            applied, publish_growth = new_allocation_bytes(
                lambda: apply_plan(big_path, plan)
            )
            self.assertEqual(applied, [2])
            self.assertLess(publish_growth, MEMORY_LIMIT)

            # Cut point still shows the deleted rows and not the new insert.
            cut_rows, _ = read_route_snapshot(big_path, 0, 256)
            cut_ids = [row[0] for row in cut_rows]
            self.assertIn(7, cut_ids)
            self.assertIn(8, cut_ids)
            self.assertIn(14, cut_ids)
            self.assertNotIn(60001, cut_ids)
            self.assertEqual(dict(cut_rows)[15], payload)

            preview_plan_arg = plan + [
                Migration(3, (
                    "UPDATE route_catalog SET payload=x'cc' WHERE id=1",
                )),
            ]
            previewed, preview_growth = new_allocation_bytes(
                lambda: preview_plan(big_path, preview_plan_arg)
            )
            self.assertEqual(previewed, [3])
            self.assertLess(preview_growth, MEMORY_LIMIT)

            if started_here:
                tracemalloc.stop()

            # Finish and verify the real net-change batch content.
            self.assertTrue(finish_route_snapshot(big_path, 1))
            version, entries = read_route_changes(big_path)
            self.assertEqual(version, 2)
            self.assertEqual(
                entries,
                [(7, None), (8, None), (14, None), (15, b"\xaa"),
                 (60001, b"\xbb")],
            )

    def test_internal_objects_are_protected_and_state_guards_hold(self):
        open_route_feed(self.path, 4)
        # Trusted migration SQL must never touch feed internals.
        for statement in (
            "INSERT INTO _route_feed_meta(slot) VALUES (2)",
            "DELETE FROM _route_feed_batches",
            "UPDATE _route_feed_snapshot SET payload=x'00' WHERE id=10",
        ):
            with self.assertRaises(sqlite3.DatabaseError):
                apply_plan(
                    self.path,
                    [self.v1, Migration(2, (statement,))],
                )
        # The failed attempts left no ledger and no channel damage.
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT MAX(version) FROM _migration_ledger"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                db.execute("SELECT COUNT(*) FROM _route_feed_snapshot")
                .fetchone()[0],
                3,
            )

        # Snapshot-only operations reject change-phase calls until confirm.
        with self.assertRaises(RouteFeedState):
            finish_route_snapshot(self.path, 2)
        with self.assertRaises(RouteFeedState):
            read_route_changes(self.path)
        with self.assertRaises(RouteFeedState):
            ack_route_changes(self.path, 1)

    def test_open_requires_catalog_and_registered_version(self):
        with TemporaryDirectory() as tmp:
            fresh = Path(tmp, "fresh.db")
            with closing(sqlite3.connect(fresh)) as db, db:
                db.execute(
                    "CREATE TABLE route_catalog("
                    "id INTEGER PRIMARY KEY, payload BLOB NOT NULL)"
                )
            with self.assertRaises(RouteFeedState):
                open_route_feed(fresh, 4)

            no_catalog = Path(tmp, "no_catalog.db")
            apply_plan(
                no_catalog,
                [Migration(1, ("CREATE TABLE other(id INTEGER PRIMARY KEY)",))],
            )
            with self.assertRaises(RouteFeedState):
                open_route_feed(no_catalog, 4)

    def test_channel_without_feed_keeps_legacy_behavior(self):
        # Plans on a database that never opened the feed are untouched.
        plan = [
            self.v1,
            Migration(2, (
                "UPDATE route_catalog SET payload=x'01' WHERE id=10",
                "DELETE FROM route_catalog WHERE id=30",
            )),
        ]
        self.assertEqual(apply_plan(self.path, plan), [2])
        self.assertEqual(preview_plan(self.path, plan), [])
        with closing(sqlite3.connect(self.path)) as db:
            self.assertFalse(
                db.execute(
                    "SELECT 1 FROM sqlite_master WHERE name LIKE '_route_feed_%'"
                ).fetchall()
            )
        with self.assertRaises(RouteFeedState):
            read_route_changes(self.path)


class RouteFeedConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.path = Path(self._tmp.name, "routes.db")
        make_catalog(
            self.path,
            [(1, b"a"), (2, b"b"), (3, b"c"), (4, b"d")],
        )
        self.v1 = Migration(
            1, ("CREATE TABLE ledger_anchor(id INTEGER PRIMARY KEY)",)
        )
        self.assertEqual(apply_plan(self.path, [self.v1]), [1])

    def tearDown(self):
        self._tmp.cleanup()

    @staticmethod
    def _publish(path, plan, queue):
        try:
            queue.put(("ok", apply_plan(path, plan)))
        except Exception as error:  # delivered by class name to the parent
            queue.put((type(error).__name__,
                       (getattr(error, "needed", None),
                        getattr(error, "available", None))))

    def test_writers_serialize_and_quota_releases_after_ack_commit(self):
        if "fork" not in multiprocessing.get_all_start_methods():
            self.skipTest("fork-based process test needs a POSIX start method")
        open_route_feed(self.path, 2)
        self.assertTrue(finish_route_snapshot(self.path, 1))

        # Two identical plans race for version 2.  BEGIN IMMEDIATE serializes
        # them: one registers the batch, the other replays as an exact no-op.
        plan2 = [
            self.v1,
            Migration(2, (
                "UPDATE route_catalog SET payload=x'10' WHERE id IN (1,2)",
            )),
        ]
        ctx = multiprocessing.get_context("fork")
        queue = ctx.Queue()
        procs = [
            ctx.Process(
                target=self._publish, args=(str(self.path), plan2, queue)
            )
            for _ in range(2)
        ]
        for proc in procs:
            proc.start()
        results = sorted(queue.get() for _ in procs)
        for proc in procs:
            proc.join()
            self.assertEqual(proc.exitcode, 0)
        self.assertEqual(results, [("ok", []), ("ok", [2])])

        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(
                db.execute(
                    "SELECT COUNT(*) FROM _route_feed_batches"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                db.execute(
                    "SELECT used_pending FROM _route_feed_meta"
                ).fetchone()[0],
                2,
            )

        # The quota is full: another change is rejected with exact figures.
        plan3 = plan2 + [
            Migration(3, (
                "UPDATE route_catalog SET payload=x'20' WHERE id=3",
            )),
        ]
        with self.assertRaises(RouteFeedFull) as caught:
            apply_plan(self.path, plan3)
        self.assertEqual(
            (caught.exception.needed, caught.exception.available), (1, 0)
        )

        # Only the ack commit returns the slot; then the blocked plan fits.
        version, entries = read_route_changes(self.path)
        self.assertEqual(version, 2)
        self.assertEqual(len(entries), 2)
        self.assertTrue(ack_route_changes(self.path, version))
        self.assertEqual(apply_plan(self.path, plan3), [3])
        self.assertEqual(
            read_route_changes(self.path), (3, [(3, b"\x20")])
        )


if __name__ == "__main__":
    unittest.main()
