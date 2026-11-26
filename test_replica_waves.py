"""Tests for maintenance-wave scheduling over SQLite replicas."""
from contextlib import closing
import os
from pathlib import Path
import random
import sqlite3
from tempfile import TemporaryDirectory
import tracemalloc
import unittest

from migrations import (
    HistoryMismatch,
    Migration,
    OnlineUnique,
    apply_plan,
)
from replica_waves import (
    ReadSlot,
    Replica,
    WaveUnavailable,
    open_replica_wave,
)

PLAN = [
    Migration(1, ("CREATE TABLE data(k INTEGER PRIMARY KEY, v TEXT)",)),
    Migration(2, ("ALTER TABLE data ADD COLUMN note TEXT DEFAULT 'v2'",)),
    Migration(3, ("ALTER TABLE data ADD COLUMN tag TEXT DEFAULT 'v3'",)),
]
SELECT_V = "SELECT v FROM data WHERE k=1"


def build_fleet(root, spec, zones):
    """Create one SQLite file per (name, version) pair and seed a marker row."""
    paths = {}
    for name, version in spec.items():
        path = Path(root, name + ".db")
        apply_plan(path, PLAN[:version])
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("INSERT INTO data(k, v) VALUES (1, ?)", (name,))
        paths[name] = path
    return [Replica(name, paths[name], zones[name]) for name in sorted(spec)]


def ledger_versions(path):
    with closing(sqlite3.connect(path)) as db:
        return [row[0] for row in db.execute(
            "SELECT version FROM _migration_ledger ORDER BY version")]


def columns(path, table="data"):
    with closing(sqlite3.connect(path)) as db:
        return [row[1] for row in db.execute("PRAGMA table_info(%s)" % table)]


class AcceptanceScenarioOneTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.spec = {"a": 1, "b": 1, "c": 1, "d": 1, "e": 2}
        self.zones = {"a": "east", "b": "east", "c": "west",
                      "d": "west", "e": "west"}
        self.replicas = build_fleet(self.tmp.name, self.spec, self.zones)
        self.candidates = ["a", "b", "c"]
        self.zone_limits = {"east": 1, "west": 1}

    def tearDown(self):
        self.tmp.cleanup()

    def _open(self, x_max):
        slots = [
            ReadSlot("X", {"a", "c", "e"}, 1, x_max, SELECT_V),
            ReadSlot("Y", {"c", "d"}, 1, 1, SELECT_V),
        ]
        wave = open_replica_wave(
            self.replicas, PLAN, slots, self.candidates, 2, self.zone_limits
        )
        return wave.__enter__(), wave

    def test_first_wave_picks_b_c_and_serves_from_a_d(self):
        wave, closer = self._open(1)
        try:
            self.assertEqual(wave.selected, ("b", "c"))
            # Every served slot sits on a replica that stays online.
            self.assertNotIn("a", wave.selected)
            self.assertNotIn("d", wave.selected)
            self.assertEqual(wave.read("X"), ("a", "a"))
            self.assertEqual(wave.read("Y"), ("d", "d"))

            advanced = []
            while True:
                name = wave.advance()
                if name is None:
                    break
                advanced.append(name)
                # Reads keep using the originally assigned replicas, and they
                # observe data committed after the wave opened.
                if name == "b":
                    with closing(sqlite3.connect(
                            self.replicas[0].path)) as db, db:
                        db.execute("UPDATE data SET v='fresh-a' WHERE k=1")
                    self.assertEqual(wave.read("X"), ("a", "fresh-a"))
                    self.assertEqual(wave.read("Y"), ("d", "d"))
            self.assertEqual(advanced, ["b", "c"])
        finally:
            closer.__exit__(None, None, None)

        # Only the selected replicas were migrated.
        for name, version in self.spec.items():
            path = next(r.path for r in self.replicas if r.name == name)
            expected = 3 if name in ("b", "c") else version
            self.assertEqual(ledger_versions(path), list(range(1, expected + 1)))

    def test_widening_slot_x_switches_optimal_wave_to_a_c(self):
        wave, closer = self._open(2)
        try:
            self.assertEqual(wave.selected, ("a", "c"))
            self.assertEqual(wave.read("X"), ("e", "e"))
            self.assertEqual(wave.read("Y"), ("d", "d"))
            self.assertEqual(wave.advance(), "a")
            self.assertEqual(wave.advance(), "c")
            self.assertIsNone(wave.advance())
        finally:
            closer.__exit__(None, None, None)

    def test_input_order_does_not_change_selection(self):
        shuffled = list(self.replicas)
        shuffled.reverse()
        slots = [
            ReadSlot("X", {"a", "c", "e"}, 1, 1, SELECT_V),
            ReadSlot("Y", {"c", "d"}, 1, 1, SELECT_V),
        ]
        wave = open_replica_wave(
            shuffled, PLAN, slots, ["c", "a", "b"], 2,
            {"west": 1, "east": 1},
        )
        with wave:
            self.assertEqual(wave.selected, ("b", "c"))

    def test_slots_without_exclusive_nodes_cannot_open(self):
        # Both slots can only ever reach replica c: one of them must lose.
        slots = [
            ReadSlot("P", {"c"}, 1, 3, SELECT_V),
            ReadSlot("Q", {"c"}, 1, 3, SELECT_V),
        ]
        with self.assertRaises(WaveUnavailable):
            open_replica_wave(
                self.replicas, PLAN, slots, [], 0,
                {"east": 0, "west": 0},
            )
        # Nothing was modified by the failed open.
        for name, version in self.spec.items():
            path = next(r.path for r in self.replicas if r.name == name)
            self.assertEqual(len(ledger_versions(path)), version)

    def test_slot_pins_its_only_node_online(self):
        # Without the slot the optimum is {b, c}; with a slot that only b can
        # serve, b must stay online and the optimum moves to {a, c}.
        slots = [ReadSlot("Z", {"b"}, 1, 1, SELECT_V)]
        with open_replica_wave(
            self.replicas, PLAN, slots, self.candidates, 2, self.zone_limits
        ) as wave:
            self.assertEqual(wave.selected, ("a", "c"))
            self.assertEqual(wave.read("Z"), ("b", "b"))

    def test_slot_with_empty_eligible_set_is_unservable(self):
        slots = [ReadSlot("Z", set(), 1, 3, SELECT_V)]
        with self.assertRaises(WaveUnavailable):
            open_replica_wave(
                self.replicas, PLAN, slots, [], 0,
                {"east": 0, "west": 0},
            )

    def test_empty_wave_opens_and_still_serves_reads(self):
        slots = [
            ReadSlot("X", {"a", "c", "e"}, 1, 2, SELECT_V),
            ReadSlot("Y", {"c", "d"}, 1, 1, SELECT_V),
        ]
        with open_replica_wave(
            self.replicas, PLAN, slots, [], 0,
            {"east": 0, "west": 0},
        ) as wave:
            self.assertEqual(wave.selected, ())
            # Any distinct, eligible, compatible online node is acceptable.
            served = {}
            for slot in slots:
                node, value = wave.read(slot.name)
                self.assertEqual(value, node)
                self.assertIn(node, slot.eligible_names)
                self.assertTrue(
                    slot.min_version
                    <= self.spec[node] <= slot.max_version)
                self.assertNotIn(node, served)
                served[node] = slot.name
            self.assertEqual(len(served), 2)
            self.assertIsNone(wave.advance())


class FailureAndReopenTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        # v3 changes the schema and then writes into a table that may be absent;
        # the first statement proves intermediate schema changes roll back.
        self.plan = [
            Migration(1, ("CREATE TABLE items(id INTEGER PRIMARY KEY, label TEXT)",)),
            Migration(2, ("ALTER TABLE items ADD COLUMN stage TEXT DEFAULT 'two'",)),
            Migration(3, (
                "ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'x'",
                "INSERT INTO audit_log(who) VALUES ('migrator')",
            )),
        ]
        self.names = ["n1", "n2", "n3", "s1"]
        self.paths = {}
        for name in self.names:
            path = Path(self.tmp.name, name + ".db")
            apply_plan(path, self.plan[:1])
            with closing(sqlite3.connect(path)) as db, db:
                db.execute("INSERT INTO items(id, label) VALUES (1, ?)", (name,))
            self.paths[name] = path
        for name in ("n1", "n2", "n3"):
            apply_plan(self.paths[name], self.plan[:2])
        # Only the first migrating node already has the table the v3 migration
        # needs: n1 commits, n2 fails mid-transaction and n3 never starts.
        with closing(sqlite3.connect(self.paths["n1"])) as db, db:
            db.execute("CREATE TABLE audit_log(who TEXT)")
        self.replicas = [
            Replica("n1", self.paths["n1"], "z"),
            Replica("n2", self.paths["n2"], "z"),
            Replica("n3", self.paths["n3"], "z"),
            Replica("s1", self.paths["s1"], "z"),
        ]
        self.slots = [ReadSlot("S", {"s1"}, 1, 3,
                               "SELECT label FROM items WHERE id=1")]

    def tearDown(self):
        self.tmp.cleanup()

    def test_failure_keeps_commit_rolls_back_node_and_blocks_later_nodes(self):
        with open_replica_wave(
            self.replicas, self.plan, self.slots,
            ["n1", "n2", "n3"], 3, {"z": 3},
        ) as wave:
            self.assertEqual(wave.selected, ("n1", "n2", "n3"))
            self.assertEqual(wave.advance(), "n1")
            with self.assertRaises(sqlite3.OperationalError):
                wave.advance()

            # Earlier node committed at target; failed node rolled back schema
            # and ledger entry together; later node never started.
            self.assertEqual(ledger_versions(self.paths["n1"]), [1, 2, 3])
            self.assertEqual(columns(self.paths["n2"], "items"),
                             ["id", "label", "stage"])
            self.assertEqual(ledger_versions(self.paths["n2"]), [1, 2])
            self.assertEqual(ledger_versions(self.paths["n3"]), [1, 2])

            # The same wave stays readable but refuses to advance again.
            self.assertEqual(wave.read("S"), ("s1", "s1"))
            with self.assertRaises(RuntimeError):
                wave.advance()

        # Fix the failing precondition without touching any ledger, then reopen.
        for name in ("n2", "n3"):
            with closing(sqlite3.connect(self.paths[name])) as db, db:
                db.execute("CREATE TABLE audit_log(who TEXT)")

        with open_replica_wave(
            self.replicas, self.plan, self.slots,
            ["n1", "n2", "n3"], 3, {"z": 3},
        ) as wave:
            # n1 already reached the target and is not selected again.
            self.assertEqual(wave.selected, ("n2", "n3"))
            self.assertEqual(wave.advance(), "n2")
            self.assertEqual(wave.advance(), "n3")
            self.assertIsNone(wave.advance())
            for name in self.names[:3]:
                self.assertEqual(ledger_versions(self.paths[name]), [1, 2, 3])

        with open_replica_wave(
            self.replicas, self.plan, self.slots,
            ["n1", "n2", "n3"], 3, {"z": 3},
        ) as wave:
            self.assertEqual(wave.selected, ())
            self.assertEqual(wave.read("S"), ("s1", "s1"))

    def test_non_candidate_drift_is_rejected_before_opening(self):
        with closing(sqlite3.connect(self.paths["s1"])) as db, db:
            db.execute("UPDATE _migration_ledger SET checksum='drifted'")
        with self.assertRaises(HistoryMismatch):
            open_replica_wave(
                self.replicas, self.plan, self.slots,
                ["n1", "n2", "n3"], 3, {"z": 3},
            )
        # The rejected open migrated nothing.
        for name in self.names:
            self.assertEqual(ledger_versions(self.paths[name]),
                             [1, 2] if name != "s1" else [1])

    def test_missing_and_empty_ledgers_raise_history_mismatch(self):
        fresh = Path(self.tmp.name, "blank.db")
        fresh.touch()
        replicas = self.replicas + [Replica("blank", fresh, "z")]
        with self.assertRaises(HistoryMismatch):
            open_replica_wave(
                replicas, self.plan, self.slots,
                ["n1", "n2", "n3"], 3, {"z": 3},
            )


class ClosedWaveAndSpecialPhaseTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.paths = {}
        for name, version in (("r1", 1), ("r2", 1)):
            path = Path(self.tmp.name, name + ".db")
            apply_plan(path, PLAN[:version])
            with closing(sqlite3.connect(path)) as db, db:
                db.execute("INSERT INTO data(k, v) VALUES (1, ?)", (name,))
            self.paths[name] = path
        self.replicas = [
            Replica("r1", str(self.paths["r1"]), "z"),
            Replica("r2", self.paths["r2"], "z"),
        ]

    def tearDown(self):
        self.tmp.cleanup()

    def test_closed_wave_rejects_reads_and_advances(self):
        slots = [ReadSlot("S", {"r1"}, 1, 3, SELECT_V)]
        wave = open_replica_wave(
            self.replicas, PLAN, slots, ["r2"], 1, {"z": 1})
        with wave:
            self.assertEqual(wave.selected, ("r2",))
        with self.assertRaises(RuntimeError):
            wave.read("S")
        with self.assertRaises(RuntimeError):
            wave.advance()

    def test_unknown_slot_is_value_error(self):
        slots = [ReadSlot("S", {"r1"}, 1, 3, SELECT_V)]
        with open_replica_wave(
            self.replicas, PLAN, slots, ["r2"], 1, {"z": 1}) as wave:
            with self.assertRaises(ValueError):
                wave.read("nope")

    def test_remaining_special_phases_exclude_node_from_wave(self):
        online_plan = PLAN + [OnlineUnique(4, "data")]
        # r1 stays on v1; r2 reaches v3 through ordinary phases.  The target
        # is v4, whose phase is the online publication, so neither node's
        # remaining tail consists only of ordinary Migration objects.
        apply_plan(self.paths["r2"], online_plan[:3])
        slots = [ReadSlot("S", {"r1"}, 1, 3, SELECT_V)]
        with open_replica_wave(
            self.replicas, online_plan, slots, ["r1", "r2"], 2, {"z": 2}
        ) as wave:
            self.assertEqual(wave.selected, ())
            self.assertEqual(wave.read("S"), ("r1", "r1"))


# --------------------------------------------------------------------------
# Exhaustive small-scale oracle and large-scale synthetic fleet
# --------------------------------------------------------------------------


def _brute_force_optimal(names, zones, versions, slots, candidates,
                         limit, zone_limits, target):
    """Enumerate every subset (small instances only) to obtain the optimum."""
    migratable = [n for n in names
                  if n in candidates and versions[n] < target]
    zones_of = dict(zip(names, zones))

    def quotas_ok(subset):
        if len(subset) > limit:
            return False
        used = {}
        for name in subset:
            zone = zones_of[name]
            used[zone] = used.get(zone, 0) + 1
            if used[zone] > zone_limits[zone]:
                return False
        return True

    def slots_serve(subset):
        offline = set(subset)
        options = {}
        for slot in slots:
            options[slot.name] = sorted(
                name for name in slot.eligible_names
                if name not in offline
                and slot.min_version <= versions[name] <= slot.max_version
            )
        ordered = list(options)
        chosen = {}

        def match(index):
            if index == len(ordered):
                return True
            for name in options[ordered[index]]:
                if name not in chosen:
                    chosen[name] = ordered[index]
                    if match(index + 1):
                        return True
                    del chosen[name]
            return False

        return match(0)

    best = None
    for mask in range(1 << len(migratable)):
        subset = tuple(n for i, n in enumerate(migratable) if mask & (1 << i))
        if not quotas_ok(subset) or not slots_serve(subset):
            continue
        if best is None or len(subset) > len(best) or (
                len(subset) == len(best) and subset < best):
            best = subset
    return best


class ExhaustiveOracleTest(unittest.TestCase):
    def test_random_small_fleets_match_brute_force(self):
        rng = random.Random(20261126)
        names = ["n%d" % i for i in range(9)]
        target = 3
        for trial in range(60):
            versions = {name: rng.randint(1, target) for name in names}
            zones = {name: ("za" if rng.random() < 0.5 else "zb")
                     for name in names}
            zone_limits = {"za": rng.randint(0, 5), "zb": rng.randint(0, 5)}
            limit = rng.randint(0, 9)
            candidates = [name for name in names if rng.random() < 0.7]
            slots = []
            for slot_index in range(rng.randint(0, 3)):
                pool = rng.sample(names, rng.randint(1, 5))
                lo = rng.randint(1, target)
                hi = rng.randint(lo, target)
                slots.append(ReadSlot(
                    "s%d" % slot_index, set(pool), lo, hi, SELECT_V))

            with TemporaryDirectory() as tmp:
                paths = {}
                for name in names:
                    path = Path(tmp, name + ".db")
                    apply_plan(path, PLAN[:versions[name]])
                    with closing(sqlite3.connect(path)) as db, db:
                        db.execute("INSERT INTO data(k, v) VALUES (1, ?)",
                                   (name,))
                    paths[name] = path
                replicas = [Replica(name, paths[name], zones[name])
                            for name in names]
                expected = _brute_force_optimal(
                    names, [zones[n] for n in names], versions, slots,
                    set(candidates), limit, zone_limits, target)
                if expected is None:
                    with self.assertRaises(WaveUnavailable):
                        open_replica_wave(
                            replicas, PLAN, slots, candidates, limit,
                            zone_limits)
                    continue
                with open_replica_wave(
                    replicas, PLAN, slots, candidates, limit, zone_limits
                ) as wave:
                    self.assertEqual(wave.selected, expected, trial)
                    # The produced assignment must itself be a valid matching.
                    used = set()
                    for slot in slots:
                        node, _ = wave.read(slot.name)
                        self.assertIn(node, slot.eligible_names)
                        self.assertTrue(
                            slot.min_version
                            <= versions[node] <= slot.max_version)
                        self.assertNotIn(node, wave.selected)
                        self.assertNotIn(node, used)
                        used.add(node)


class LargeFleetTest(unittest.TestCase):
    def test_deterministic_optimum_and_memory_bound(self):
        with TemporaryDirectory() as tmp:
            # 90 replicas across 6 zones: 80 migratable candidates (spread
            # round-robin so every zone holds candidates) plus 10 dedicated
            # serving nodes already at the target version.
            names = ["r%02d" % i for i in range(90)]
            zones = {name: "z%d" % (i % 6) for i, name in enumerate(names)}
            versions = {name: (1 if i < 60 else 2 if i < 80 else 3)
                        for i, name in enumerate(names)}
            paths = {}
            for name in names:
                path = Path(tmp, name + ".db")
                apply_plan(path, PLAN[:versions[name]])
                with closing(sqlite3.connect(path)) as db, db:
                    db.execute("INSERT INTO data(k, v) VALUES (1, ?)", (name,))
                paths[name] = path
            replicas = [Replica(name, paths[name], zones[name])
                        for name in names]
            candidates = names[:80]
            slots = [
                ReadSlot("slot%d" % i,
                         set(names[80:90]), 3, 3, SELECT_V)
                for i in range(8)
            ]

            def independent_expected(limit, zone_limits):
                # Slots are fully decoupled from the candidates, so the optimum
                # is the lexicographically first quota-respecting subset.
                picked = []
                zone_used = {}
                for name in candidates:
                    if len(picked) >= limit:
                        break
                    zone = zones[name]
                    if zone_used.get(zone, 0) < zone_limits[zone]:
                        picked.append(name)
                        zone_used[zone] = zone_used.get(zone, 0) + 1
                return tuple(picked)

            # Global cap binds, zone caps stay loose.
            expected = independent_expected(45, {z: 15 for z in
                                                  sorted(set(zones.values()))})
            self.assertGreater(len(expected), 40)

            tracemalloc.start()
            try:
                baseline_current, baseline_peak = tracemalloc.get_traced_memory()
                with open_replica_wave(
                    replicas, PLAN, slots, candidates, 45,
                    {z: 15 for z in sorted(set(zones.values()))},
                ) as wave:
                    self.assertEqual(wave.selected, expected)
                    for slot in slots:
                        node, value = wave.read(slot.name)
                        self.assertEqual(node, value)
                        self.assertNotIn(node, wave.selected)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
            self.assertLess(peak - baseline_peak, 24 * 1024 * 1024)

            # Binding per-zone caps plus a loose global cap.
            zone_limits = {z: 7 for z in sorted(set(zones.values()))}
            expected_capped = independent_expected(256, zone_limits)
            with open_replica_wave(
                replicas, PLAN, slots, candidates, 256, zone_limits
            ) as wave:
                self.assertEqual(wave.selected, expected_capped)
                self.assertEqual(len(expected_capped), 6 * 7)
                self.assertGreater(len(expected_capped), 40)

            # Reopening on the same ledgers is deterministic and idempotent.
            with open_replica_wave(
                replicas, PLAN, slots, candidates, 45,
                {z: 15 for z in sorted(set(zones.values()))},
            ) as wave:
                self.assertEqual(wave.selected, expected)


class ValidationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.path = Path(self.tmp.name, "r.db")
        apply_plan(self.path, PLAN[:1])

    def tearDown(self):
        self.tmp.cleanup()

    def _replicas(self):
        return [Replica("r", self.path, "z")]

    def test_names_and_counts(self):
        bad_names = ["", "x" * 33, "bad name", "slash/x", "dot.x"]
        for bad in bad_names:
            with self.assertRaises(ValueError):
                Replica(bad, self.path, "z")
        with self.assertRaises(ValueError):
            Replica("r", self.path, "")
        with self.assertRaises(ValueError):
            open_replica_wave(
                [], PLAN, [], ["r"], 1, {"z": 1})
        with self.assertRaises(ValueError):
            open_replica_wave(
                [Replica("r", self.path, "z"),
                 Replica("r", self.path, "z2")],
                PLAN, [], ["r"], 1, {"z": 1, "z2": 1})

    def test_paths_must_exist_and_differ(self):
        missing = Path(self.tmp.name, "missing.db")
        with self.assertRaises(ValueError):
            open_replica_wave(
                [Replica("m", missing, "z")], PLAN, [], ["m"], 1, {"z": 1})
        other = Path(self.tmp.name, "o.db")
        apply_plan(other, PLAN[:1])
        with self.assertRaises(ValueError):
            open_replica_wave(
                [Replica("r", self.path, "z"),
                 Replica("o", self.path, "z")],
                PLAN, [], ["r", "o"], 2, {"z": 2})

    def test_plan_shape(self):
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), [Migration(2, ("SELECT 1",))],
                [], ["r"], 1, {"z": 1})
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), "not-a-plan", [], ["r"], 1, {"z": 1})

    def test_slots(self):
        kwargs = dict(eligible_names=set(), min_version=1, max_version=1,
                      sql=SELECT_V)
        with self.assertRaises(ValueError):
            ReadSlot("bad name", **kwargs)
        with self.assertRaises(ValueError):
            ReadSlot("s", set(), 0, 1, SELECT_V)
        with self.assertRaises(ValueError):
            ReadSlot("s", set(), 2, 1, SELECT_V)
        with self.assertRaises(ValueError):
            ReadSlot("s", set(), True, 1, SELECT_V)
        with self.assertRaises(ValueError):
            ReadSlot("s", set(), 1, 1, "UPDATE data SET v='x'")
        with self.assertRaises(ValueError):
            ReadSlot("s", set(), 1, 1, "SELECT 1; SELECT 2")
        with self.assertRaises(ValueError):
            ReadSlot("s", set(), 1, 1, "SELECT ?")
        with self.assertRaises(ValueError):
            ReadSlot("s", "r", 1, 1, SELECT_V)
        # Version bounds exceeding the plan target can only be rejected when
        # the plan (and therefore the target) is known.
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), PLAN,
                [ReadSlot("s", set(), 1, 4, SELECT_V)],
                ["r"], 1, {"z": 1})
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), PLAN,
                [ReadSlot("s", {"ghost"}, 1, 1, SELECT_V)],
                ["r"], 1, {"z": 1})
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), PLAN,
                [ReadSlot("s", ["r", "r"], 1, 1, SELECT_V)],
                ["r"], 1, {"z": 1})

    def test_limits_and_candidates(self):
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), PLAN, [], ["r"], True, {"z": 1})
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), PLAN, [], ["r"], -1, {"z": 1})
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), PLAN, [], ["r"], 1, {"z": 1, "extra": 1})
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), PLAN, [], ["r"], 1, {})
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), PLAN, [], ["r", "r"], 1, {"z": 1})
        with self.assertRaises(ValueError):
            open_replica_wave(
                self._replicas(), PLAN, [], ["ghost"], 1, {"z": 1})
        # An empty candidate list is legal.
        with open_replica_wave(
            self._replicas(), PLAN, [], [], 0, {"z": 0}) as wave:
            self.assertEqual(wave.selected, ())

    def test_relation_and_slot_caps(self):
        many_files = [Path(self.tmp.name, "m%d.db" % i) for i in range(249)]
        for path in many_files:
            path.touch()
        replicas = [Replica("m%03d" % i, many_files[i], "z")
                    for i in range(249)]
        slots = [
            ReadSlot("s%02d" % i, {"m%03d" % j for j in range(249)},
                     1, 1, SELECT_V)
            for i in range(33)
        ]
        with self.assertRaises(ValueError):
            open_replica_wave(
                replicas, PLAN, slots, [], 0, {"z": 0})
        too_many_slots = [
            ReadSlot("s%03d" % i, set(), 1, 1, SELECT_V)
            for i in range(129)
        ]
        with self.assertRaises(ValueError):
            open_replica_wave(self._replicas(), PLAN, too_many_slots,
                              ["r"], 1, {"z": 1})


if __name__ == "__main__":
    unittest.main()
