"""Schedule maintenance waves over a fleet of SQLite replicas.

Every replica keeps its own SQLite database and its own migration ledger
(see :mod:`migrations`).  A maintenance wave takes a mutually agreed set of
replicas offline for the whole window: a replica that finishes its migration
early is not fed read traffic again until the wave closes.  Read slots are
served from replicas that stay online, and each online replica hosts at most
one slot for the window.

The wave is chosen exactly, without enumerating candidate subsets: the joint
problem (slot-to-replica matching plus global and per-zone offline quotas) is
solved once as a minimum-cost maximum-flow on an in-memory network.  Opening a
wave only reads each replica's ledger once; it never writes to any database.

Only the standard library is used and the existing migration entry points and
special-phase contracts are untouched.
"""
from collections import deque
from collections.abc import Mapping
from contextlib import closing
from dataclasses import dataclass
import os
from pathlib import Path
import re
import sqlite3

from migrations import (
    HistoryMismatch,
    Migration,
    _history,
    _parse_plan,
    _plan_keys,
    apply_plan,
)

__all__ = [
    "Replica",
    "ReadSlot",
    "WaveUnavailable",
    "open_replica_wave",
]

_NAME_RE = re.compile(r"[A-Za-z0-9_-]{1,32}\Z")
MAX_REPLICAS = 256
MAX_SLOTS = 128
MAX_ELIGIBILITY_RELATIONS = 8192
MAX_PLAN_LENGTH = 128
MAX_LIMIT = 256


class WaveUnavailable(Exception):
    """No wave can be opened while keeping every read slot served."""


@dataclass(frozen=True)
class Replica:
    """An independently migratable SQLite node."""

    name: str
    path: object
    zone: str

    def __post_init__(self):
        _require_name(self.name, "replica name")
        _require_name(self.zone, "zone name")
        if not isinstance(self.path, (str, os.PathLike)):
            raise ValueError("replica path must be a str or Path")


@dataclass(frozen=True)
class ReadSlot:
    """A read seat that exclusively occupies one replica for the whole wave."""

    name: str
    eligible_names: object
    min_version: int
    max_version: int
    sql: str

    def __post_init__(self):
        _require_name(self.name, "slot name")
        if type(self.min_version) is not int or type(self.max_version) is not int:
            raise ValueError("slot version bounds must be non-boolean integers")
        if self.min_version < 1 or self.max_version < 1:
            raise ValueError("slot version bounds must be positive integers")
        if self.min_version > self.max_version:
            raise ValueError("slot min_version must not exceed max_version")
        if not isinstance(self.sql, str) or not self.sql.strip():
            raise ValueError("slot sql must be a non-empty SELECT string")
        text = self.sql.strip()
        if not text.lower().startswith("select"):
            raise ValueError("slot sql must be a single read-only SELECT")
        body = text[:-1] if text.endswith(";") else text
        if ";" in body or "?" in body:
            raise ValueError(
                "slot sql must be one statement without bound parameters"
            )
        if isinstance(self.eligible_names, (str, bytes, bytearray)):
            raise ValueError("eligible_names must be a collection of names")
        try:
            raw_names = tuple(self.eligible_names)
        except TypeError:
            raise ValueError("eligible_names must be a collection of names") from None
        if any(not isinstance(name, str) for name in raw_names):
            raise ValueError("eligible_names must contain replica name strings")
        eligible = frozenset(raw_names)
        if len(eligible) != len(raw_names):
            raise ValueError("eligible_names must not list a replica twice")
        object.__setattr__(self, "eligible_names", eligible)
        object.__setattr__(self, "sql", text)


# --------------------------------------------------------------------------
# Validation helpers
# --------------------------------------------------------------------------


def _require_name(value, what):
    if not isinstance(value, str) or _NAME_RE.fullmatch(value) is None:
        raise ValueError(
            "%s must be 1-32 ASCII letters, digits, underscores or hyphens" % what
        )
    return value


def _require_nonbool_int(value, low, high, what):
    if type(value) is not int or not low <= value <= high:
        raise ValueError("%s must be an integer between %d and %d" % (what, low, high))
    return value


def _require_tuple(value, what):
    if isinstance(value, (str, bytes, bytearray)):
        raise ValueError("%s must be a collection, not a string" % what)
    try:
        return tuple(value)
    except TypeError:
        raise ValueError("%s must be an iterable collection" % what) from None


def _validate_inputs(replicas, plan, slots, candidates, limit, zone_limits):
    raw_replicas = _require_tuple(replicas, "replicas")
    if not 1 <= len(raw_replicas) <= MAX_REPLICAS:
        raise ValueError("between 1 and %d replicas are required" % MAX_REPLICAS)

    replica_by_name = {}
    resolved_paths = set()
    file_identities = set()
    zones = set()
    for replica in raw_replicas:
        if not isinstance(replica, Replica):
            raise ValueError("replicas must contain Replica instances")
        _require_name(replica.name, "replica name")
        _require_name(replica.zone, "zone name")
        if replica.name in replica_by_name:
            raise ValueError("replica names must be distinct: %r" % replica.name)
        if isinstance(replica.path, (str, os.PathLike)):
            path = Path(replica.path)
        else:
            raise ValueError("replica path must be a str or Path")
        if not path.is_file():
            raise ValueError("replica %r database does not exist: %s" %
                             (replica.name, path))
        resolved = str(path.resolve())
        if resolved in resolved_paths:
            raise ValueError("replicas must not share a database file: %s" % path)
        stat = os.stat(path)
        identity = (stat.st_dev, stat.st_ino)
        if identity in file_identities:
            raise ValueError("replicas must not point at the same file: %s" % path)
        resolved_paths.add(resolved)
        file_identities.add(identity)
        replica_by_name[replica.name] = Replica(replica.name, path, replica.zone)
        zones.add(replica.zone)

    # Plan shape: reuse the repository's existing plan parser so the accepted
    # migration types and numbering rules stay identical.
    plan_tuple, _ = _parse_plan(plan)
    if not 1 <= len(plan_tuple) <= MAX_PLAN_LENGTH:
        raise ValueError("plans must contain between 1 and %d versions" % MAX_PLAN_LENGTH)
    target = len(plan_tuple)

    raw_slots = _require_tuple(slots, "slots")
    if len(raw_slots) > MAX_SLOTS:
        raise ValueError("at most %d read slots are supported" % MAX_SLOTS)
    slot_by_name = {}
    relation_count = 0
    for slot in raw_slots:
        if not isinstance(slot, ReadSlot):
            raise ValueError("slots must contain ReadSlot instances")
        _require_name(slot.name, "slot name")
        if slot.name in slot_by_name:
            raise ValueError("slot names must be distinct: %r" % slot.name)
        _require_nonbool_int(slot.min_version, 1, target, "slot min_version")
        _require_nonbool_int(slot.max_version, 1, target, "slot max_version")
        if slot.min_version > slot.max_version:
            raise ValueError("slot min_version must not exceed max_version")
        if not isinstance(slot.sql, str) or not slot.sql.strip():
            raise ValueError("slot sql must be a non-empty SELECT string")
        text = slot.sql.strip()
        if not text.lower().startswith("select"):
            raise ValueError("slot sql must be a single read-only SELECT")
        body = text[:-1] if text.endswith(";") else text
        if ";" in body:
            raise ValueError("slot sql must contain exactly one statement")
        eligible = _require_tuple(slot.eligible_names, "eligible_names")
        eligible_set = set()
        for name in eligible:
            if not isinstance(name, str) or name not in replica_by_name:
                raise ValueError("slot %r references unknown replica %r" %
                                 (slot.name, name))
            if name in eligible_set:
                raise ValueError("slot %r lists duplicate replica %r" %
                                 (slot.name, name))
            eligible_set.add(name)
        relation_count += len(eligible_set)
        slot_by_name[slot.name] = ReadSlot(
            slot.name,
            frozenset(eligible_set),
            slot.min_version,
            slot.max_version,
            text,
        )
    if relation_count > MAX_ELIGIBILITY_RELATIONS:
        raise ValueError("at most %d eligibility relations are supported"
                         % MAX_ELIGIBILITY_RELATIONS)

    _require_nonbool_int(limit, 0, MAX_LIMIT, "limit")

    if not isinstance(zone_limits, Mapping):
        raise ValueError("zone_limits must be a mapping of zone to offline cap")
    if set(zone_limits.keys()) != zones:
        raise ValueError("zone_limits must cover exactly the declared zones")
    for zone, cap in zone_limits.items():
        _require_nonbool_int(cap, 0, MAX_LIMIT, "zone limit for %r" % zone)

    candidate_tuple = _require_tuple(candidates, "candidates")
    candidate_set = set()
    for name in candidate_tuple:
        if not isinstance(name, str) or name not in replica_by_name:
            raise ValueError("candidate list references unknown replica %r" % name)
        if name in candidate_set:
            raise ValueError("candidate list must not repeat %r" % name)
        candidate_set.add(name)

    return (
        tuple(replica_by_name[name] for name in sorted(replica_by_name)),
        replica_by_name,
        plan_tuple,
        tuple(slot_by_name[name] for name in sorted(slot_by_name)),
        slot_by_name,
        candidate_set,
    )


# --------------------------------------------------------------------------
# Minimum-cost flow (successive shortest paths, SPFA, big-integer costs)
# --------------------------------------------------------------------------


class _FlowEdge:
    __slots__ = ("to", "rev", "cap", "cost")

    def __init__(self, to, rev, cap, cost):
        self.to = to
        self.rev = rev
        self.cap = cap
        self.cost = cost


def _add_edge(graph, u, v, cap, cost):
    forward = _FlowEdge(v, len(graph[v]), cap, cost)
    backward = _FlowEdge(u, len(graph[u]), 0, -cost)
    graph[u].append(forward)
    graph[v].append(backward)
    return forward


def _augment_while_beneficial(graph, source, sink):
    """Run SSP until no beneficial (negative-cost) augmenting path remains."""
    vertex_count = len(graph)
    infinite = float("inf")
    while True:
        dist = [infinite] * vertex_count
        parent_vertex = [-1] * vertex_count
        parent_edge = [-1] * vertex_count
        in_queue = [False] * vertex_count
        dist[source] = 0
        queue = deque([source])
        in_queue[source] = True
        while queue:
            u = queue.popleft()
            in_queue[u] = False
            here = dist[u]
            for edge_index, edge in enumerate(graph[u]):
                if edge.cap <= 0:
                    continue
                new_dist = here + edge.cost
                if new_dist < dist[edge.to]:
                    dist[edge.to] = new_dist
                    parent_vertex[edge.to] = u
                    parent_edge[edge.to] = edge_index
                    if not in_queue[edge.to]:
                        queue.append(edge.to)
                        in_queue[edge.to] = True
        if parent_vertex[sink] == -1 or dist[sink] >= 0:
            return
        node = sink
        while node != source:
            edge = graph[parent_vertex[node]][parent_edge[node]]
            edge.cap -= 1
            graph[node][edge.rev].cap += 1
            node = parent_vertex[node]


def _solve_wave(replicas, slot_by_name, versions, candidate_set, limit,
                zone_limits, plan_tuple):
    """Return (selected_names, slot_to_replica) for the exact optimal wave."""
    target = len(plan_tuple)
    ordered_names = [replica.name for replica in replicas]
    by_name = {replica.name: replica for replica in replicas}

    # Only not-yet-target replicas whose remaining phases are all ordinary
    # Migration objects may be taken offline by this wave.
    migratable = [
        name for name in ordered_names
        if versions[name] < target
        and all(type(item) is Migration for item in plan_tuple[versions[name]:])
    ]
    candidate_migratable = [
        name for name in migratable if name in candidate_set
    ]
    m = len(candidate_migratable)

    # Three benefit tiers, each strictly dominating every lower tier:
    #   W0 per saturated read slot,
    #   W1 per offline replica,
    #   a superincreasing weight per offline replica (smaller name -> larger
    #   weight), encoding the lexicographic tie break exactly.
    offline_weight = 1 << m
    slot_weight = (m + 1) << m

    zones = sorted({replica.zone for replica in replicas})
    slots = [slot_by_name[name] for name in sorted(slot_by_name)]

    source = 0
    global_node = 1
    cursor = 2
    zone_vertex = {zone: cursor + i for i, zone in enumerate(zones)}
    cursor += len(zones)
    slot_vertex = {slot.name: cursor + i for i, slot in enumerate(slots)}
    cursor += len(slots)
    node_in = {name: cursor + i for i, name in enumerate(ordered_names)}
    cursor += len(ordered_names)
    node_out = {name: cursor + i for i, name in enumerate(ordered_names)}
    cursor += len(ordered_names)
    sink = cursor
    graph = [[] for _ in range(sink + 1)]

    # Offline path: source -> global cap -> zone cap -> replica inlet.
    if limit > 0:
        _add_edge(graph, source, global_node, limit, 0)
    for zone in zones:
        cap = zone_limits[zone]
        if cap > 0:
            _add_edge(graph, global_node, zone_vertex[zone], cap, 0)
    offline_edges = {}
    for rank, name in enumerate(candidate_migratable):
        replica = by_name[name]
        benefit = offline_weight + (1 << (m - 1 - rank))
        offline_edges[name] = _add_edge(
            graph, zone_vertex[replica.zone], node_in[name], 1, -benefit
        )

    # Serving path: source -> slot -> eligible compatible replica inlet.
    slot_edges = {}
    for slot in slots:
        slot_edges[slot.name] = _add_edge(
            graph, source, slot_vertex[slot.name], 1, -slot_weight
        )
        for name in ordered_names:
            if (name in slot.eligible_names
                    and slot.min_version <= versions[name] <= slot.max_version):
                _add_edge(graph, slot_vertex[slot.name], node_in[name], 1, 0)

    # The capacity-one splitter enforces "at most one role per replica":
    # offline unit and slot unit cannot pass the same replica simultaneously.
    for name in ordered_names:
        _add_edge(graph, node_in[name], node_out[name], 1, 0)
        _add_edge(graph, node_out[name], sink, 1, 0)

    _augment_while_beneficial(graph, source, sink)

    saturated = sum(1 for slot in slots if slot_edges[slot.name].cap == 0)
    if saturated != len(slots):
        raise WaveUnavailable(
            "read slots cannot all be served by distinct online replicas"
        )

    selected = tuple(
        name for name in candidate_migratable if offline_edges[name].cap == 0
    )

    # The flow proves a serving matching exists, but its zero-cost routing is
    # arbitrary.  Recompute the slot assignment with a deterministic augmenting
    # algorithm so identical inputs always pick the same replica per slot
    # (slots and replicas considered in ascending name order).
    offline = set(selected)
    options = {}
    for slot in slots:
        options[slot.name] = [
            name for name in ordered_names
            if name not in offline
            and name in slot.eligible_names
            and slot.min_version <= versions[name] <= slot.max_version
        ]
    owner = {}

    def augment(slot_name, tried):
        for name in options[slot_name]:
            if name in tried:
                continue
            tried.add(name)
            if name not in owner or augment(owner[name], tried):
                owner[name] = slot_name
                return True
        return False

    assignment = {}
    for slot in reversed(slots):
        if not augment(slot.name, set()):  # pragma: no cover - flow proved feasibility
            raise WaveUnavailable(
                "read slots cannot all be served by distinct online replicas"
            )
    for name, slot_name in owner.items():
        assignment[slot_name] = name
    return selected, assignment


# --------------------------------------------------------------------------
# Wave object
# --------------------------------------------------------------------------


class _ReplicaWave:
    __slots__ = (
        "selected",
        "_paths",
        "_slots",
        "_assignment",
        "_plan",
        "_active",
        "_failed",
        "_cursor",
    )

    def __init__(self, paths, slots, assignment, selected, plan):
        self._paths = paths
        self._slots = slots
        self._assignment = assignment
        self.selected = selected
        self._plan = plan
        self._active = False
        self._failed = False
        self._cursor = 0

    def __enter__(self):
        self._active = True
        return self

    def __exit__(self, exc_type, exc, traceback):
        # Closing starts no work; a closed wave rejects every further action.
        self._active = False
        return False

    def read(self, slot_name):
        """Run the slot's SELECT on its assigned replica.

        A fresh connection is used per call so the query observes data
        committed after the wave opened.  Returns ``(replica_name, value)``.
        """
        if not self._active:
            raise RuntimeError("the maintenance wave is closed")
        slot = self._slots.get(slot_name)
        if slot is None:
            raise ValueError("unknown read slot: %r" % (slot_name,))
        node_name = self._assignment[slot_name]
        with closing(sqlite3.connect(os.fspath(self._paths[node_name]))) as db:
            row = db.execute(slot.sql).fetchone()
        return node_name, row[0]

    def advance(self):
        """Migrate the next selected replica to the plan target.

        Returns the replica name advanced, or ``None`` when every selected
        replica has been migrated.  A failure is reported as-is: already
        committed replicas stay committed, the failed replica rolls back per
        the ordinary transaction rules, and later replicas never start.
        """
        if not self._active:
            raise RuntimeError("the maintenance wave is closed")
        if self._failed:
            raise RuntimeError("the wave cannot advance after a failed migration")
        if self._cursor >= len(self.selected):
            return None
        name = self.selected[self._cursor]
        try:
            apply_plan(self._paths[name], self._plan)
        except Exception:
            self._failed = True
            raise
        self._cursor += 1
        return name


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def open_replica_wave(replicas, plan, slots, candidates, limit, zone_limits):
    """Validate inputs and ledgers, then select and open a maintenance wave.

    Raises :class:`ValueError` for malformed parameters or plan shape,
    :class:`migrations.HistoryMismatch` when any replica's ledger is missing,
    empty or not an exact checksum prefix of the plan, and
    :class:`WaveUnavailable` when even an empty batch cannot serve every slot.
    No replica is modified by opening a wave.
    """
    (
        replicas,
        replica_by_name,
        plan_tuple,
        normalized_slots,
        slot_by_name,
        candidate_set,
    ) = _validate_inputs(replicas, plan, slots, candidates, limit, zone_limits)

    # One connection and one ledger read per replica, before any selection:
    # drift on unselected and non-candidate replicas still rejects the wave.
    plan_keys = _plan_keys(plan_tuple)
    versions = {}
    for replica in replicas:
        with closing(sqlite3.connect(os.fspath(replica.path))) as db:
            history = _history(db)
        if not history:
            raise HistoryMismatch(
                "replica %r has no registered migration history" % replica.name
            )
        if history != plan_keys[: len(history)]:
            raise HistoryMismatch(
                "replica %r migration history is not an exact plan prefix"
                % replica.name
            )
        versions[replica.name] = len(history)

    selected, assignment = _solve_wave(
        replicas,
        slot_by_name,
        versions,
        candidate_set,
        limit,
        zone_limits,
        plan_tuple,
    )

    paths = {name: replica_by_name[name].path for name in replica_by_name}
    return _ReplicaWave(paths, slot_by_name, assignment, selected, plan_tuple)
