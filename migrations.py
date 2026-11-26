"""Apply numbered, checksummed SQLite migration plans as atomic batches.

Besides ordinary SQL batches a plan may contain one :class:`OnlineUnique`
version.  Such a version is released by a short, resumable sequence of
transactions so that old service nodes never have to stop writing:

* :func:`prepare_online_unique` installs bookkeeping triggers (no write-rule
  changes) and records online progress outside the migration ledger;
* :func:`step_online_unique` performs a bounded amount of backfill work;
* :func:`publish_online_unique` atomically records the version and switches
  the target to enforcing uniqueness.

Only the standard library is used and ordinary plans behave exactly as before.
"""
from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
import re
import sqlite3


@dataclass(frozen=True)
class Migration:
    version: int
    statements: tuple

    def checksum(self):
        return hashlib.sha256(json.dumps(self.statements, ensure_ascii=False).encode()).hexdigest()


_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


@dataclass(frozen=True)
class OnlineUnique:
    """An online uniqueness release for one ``route_key`` style column.

    The checksum lives in a separate hashing domain from :class:`Migration`
    so the two kinds of ledger entries can never be confused.  It binds both
    the version number and the target table name.
    """

    version: int
    table: str

    def __post_init__(self):
        if type(self.version) is not int or self.version < 1:
            raise ValueError("online migration versions must be positive integers")
        if not isinstance(self.table, str) or not _IDENTIFIER.fullmatch(self.table):
            raise ValueError("table must be a simple ASCII SQL identifier")

    def checksum(self):
        payload = json.dumps(
            {"kind": "online-unique", "version": self.version, "table": self.table},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(("online-unique\x00" + payload).encode()).hexdigest()


_SQLITE_MAX_INTEGER = (1 << 63) - 1


@dataclass(frozen=True)
class ReleaseArtifactReclaim:
    """Atomically retire a bounded, optimal subset of candidate releases.

    Candidate order is intentionally not identity: checksums bind the sorted
    candidate set, version and release quota.
    """

    version: int
    candidate_ids: tuple
    max_releases: int

    def __post_init__(self):
        if type(self.version) is not int or not 1 <= self.version <= _SQLITE_MAX_INTEGER:
            raise ValueError("reclaim version must be a positive SQLite integer")

        if isinstance(self.candidate_ids, (str, bytes, bytearray)):
            raise ValueError("candidate_ids must be a collection of integer ids")
        try:
            raw_ids = tuple(self.candidate_ids)
        except TypeError:
            raise ValueError("candidate_ids must be a collection of integer ids") from None

        if not 1 <= len(raw_ids) <= 20:
            raise ValueError("between 1 and 20 candidate ids are required")

        seen = set()
        for candidate_id in raw_ids:
            # ``type(...) is int`` deliberately rejects bool.
            if type(candidate_id) is not int:
                raise ValueError("candidate ids must be integers")
            if not 1 <= candidate_id <= _SQLITE_MAX_INTEGER:
                raise ValueError("candidate ids must be positive SQLite integer ids")
            if candidate_id in seen:
                raise ValueError("candidate ids must be distinct")
            seen.add(candidate_id)

        if type(self.max_releases) is not int or not 1 <= self.max_releases <= 8:
            raise ValueError("max_releases must be an integer between 1 and 8")

        # Canonicalize only after validating; input ordering has no historical
        # meaning for this phase.
        object.__setattr__(self, "candidate_ids", tuple(sorted(raw_ids)))

    def checksum(self):
        payload = json.dumps(
            {
                "kind": "release-artifact-reclaim",
                "version": self.version,
                "candidate_ids": list(self.candidate_ids),
                "max_releases": self.max_releases,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(
            ("release-artifact-reclaim\x00" + payload).encode()
        ).hexdigest()


_MAX_ROLLOUT_STATEMENTS = 64
_MAX_ROLLOUT_STATEMENT_BYTES = 4096
_PROFILE_TABLE = "worker_profiles"
_DELTA_TABLE = "_worker_profile_rollout_delta"
_ROLLOUT_SNAPSHOT = "_worker_profile_rollout_before"
_RESTORE_PLAN = "_worker_profile_restore_plan"


def _strip_sql_literals(sql):
    """Blank out single-quoted strings and comments for structural checks."""
    out = []
    index = 0
    length = len(sql)
    while index < length:
        char = sql[index]
        if char == "'":
            index += 1
            while index < length:
                if sql[index] == "'":
                    if index + 1 < length and sql[index + 1] == "'":
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            out.append(" ")
        elif char == "-" and index + 1 < length and sql[index + 1] == "-":
            newline = sql.find("\n", index + 2)
            index = length if newline < 0 else newline + 1
            out.append(" ")
        elif char == "/" and index + 1 < length and sql[index + 1] == "*":
            end = sql.find("*/", index + 2)
            index = length if end < 0 else end + 2
            out.append(" ")
        else:
            out.append(char)
            index += 1
    return "".join(out)


_ROLLOUT_TARGET = re.compile(
    r"""\s*(?:
        INSERT\s+INTO\s+(?:main\s*\.\s*)?["`\[]?([A-Za-z_]\w*)
      | UPDATE\s+(?:main\s*\.\s*)?["`\[]?([A-Za-z_]\w*)
      | DELETE\s+FROM\s+(?:main\s*\.\s*)?["`\[]?([A-Za-z_]\w*)
    )""",
    re.IGNORECASE | re.VERBOSE,
)
_UPSERT_CLAUSE = re.compile(r"\bON\s+CONFLICT\b", re.IGNORECASE)
_CONFLICT_MODIFIER = re.compile(
    r"\s*(?:INSERT|UPDATE)\s+OR\b", re.IGNORECASE
)


def _validate_rollout_statements(statements):
    if isinstance(statements, (str, bytes, bytearray)):
        raise ValueError("statements must be an ordered collection of SQL strings")
    try:
        raw = tuple(statements)
    except TypeError:
        raise ValueError("statements must be an ordered collection of SQL strings") from None
    if not 1 <= len(raw) <= _MAX_ROLLOUT_STATEMENTS:
        raise ValueError("between 1 and 64 rollout statements are required")

    checked = []
    for statement in raw:
        if not isinstance(statement, str):
            raise ValueError("rollout statements must be strings")
        if len(statement.encode("utf-8")) > _MAX_ROLLOUT_STATEMENT_BYTES:
            raise ValueError("rollout statements must not exceed 4096 UTF-8 bytes")
        structural = _strip_sql_literals(statement)
        fragments = [part for part in structural.split(";") if part.strip()]
        if not fragments:
            raise ValueError("rollout statements must not be empty")
        if len(fragments) > 1:
            raise ValueError("each rollout statement must be a single SQL statement")
        match = _ROLLOUT_TARGET.match(structural)
        if match is None:
            raise ValueError(
                "rollout statements must be plain INSERT, UPDATE or DELETE on worker_profiles"
            )
        target = next(group for group in match.groups() if group is not None)
        if target.casefold() != _PROFILE_TABLE:
            raise ValueError("rollout statements may only write worker_profiles")
        if _CONFLICT_MODIFIER.match(structural):
            raise ValueError("rollout statements must not use REPLACE or conflict clauses")
        if _UPSERT_CLAUSE.search(structural):
            raise ValueError("rollout statements must not use conflict clauses")
        checked.append(statement)
    return tuple(checked)


@dataclass(frozen=True)
class WorkerProfileRollout:
    """Publish an ordered batch of profile writes and keep rollback evidence.

    The checksum lives in its own hashing domain and binds the version and the
    ordered statement text, so a reordered or edited batch is history drift.
    """

    version: int
    statements: tuple

    def __post_init__(self):
        if type(self.version) is not int or not 1 <= self.version <= _SQLITE_MAX_INTEGER:
            raise ValueError("rollout version must be a positive SQLite integer")
        object.__setattr__(
            self, "statements", _validate_rollout_statements(self.statements)
        )

    def checksum(self):
        payload = json.dumps(
            {
                "kind": "worker-profile-rollout",
                "version": self.version,
                "statements": list(self.statements),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(
            ("worker-profile-rollout\x00" + payload).encode()
        ).hexdigest()


@dataclass(frozen=True)
class RestoreWorkerProfiles:
    """A new migration version that restores an adjacent rollout's net changes.

    ``rollout_version`` must be exactly ``version - 1``: the restore refers to
    the immediately preceding rollout version in the plan.
    """

    version: int
    rollout_version: int

    def __post_init__(self):
        if type(self.version) is not int or not 1 <= self.version <= _SQLITE_MAX_INTEGER:
            raise ValueError("restore version must be a positive SQLite integer")
        if (
            type(self.rollout_version) is not int
            or not 1 <= self.rollout_version <= _SQLITE_MAX_INTEGER
        ):
            raise ValueError("rollout_version must be a positive SQLite integer")
        if self.rollout_version != self.version - 1:
            raise ValueError("a restore must refer to the immediately preceding rollout")

    def checksum(self):
        payload = json.dumps(
            {
                "kind": "worker-profile-restore",
                "version": self.version,
                "rollout_version": self.rollout_version,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(
            ("worker-profile-restore\x00" + payload).encode()
        ).hexdigest()


class HistoryMismatch(Exception):
    pass


class ProfileRestoreBlocked(Exception):
    """A restore cannot delete a profile that jobs still pin."""

    def __init__(self, profile_id, job_id):
        self.profile_id = profile_id
        self.job_id = job_id
        super().__init__(
            "worker profile %d is still pinned by job %d"
            % (profile_id, job_id)
        )


class OnlineMigrationRequired(Exception):
    """The online version must be prepared and published before this advance."""


_LEDGER = "_migration_ledger"
_STATE = "_online_unique_state"
_STATE_SLOT = 1


# --------------------------------------------------------------------------
# Small helpers


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _sql_literal(value):
    return "'" + value.replace("'", "''") + "'"


def _open(path):
    db = sqlite3.connect(path)
    db.execute("PRAGMA busy_timeout=5000")
    return db


def _has_table(db, name):
    return db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def _history(db):
    if not _has_table(db, _LEDGER):
        return []
    return db.execute(
        "SELECT version,checksum FROM " + _LEDGER + " ORDER BY version"
    ).fetchall()


def _load_state(db):
    """Return (version, table, checksum, cursor, done) or None."""
    if not _has_table(db, _STATE):
        return None
    return db.execute(
        "SELECT version,table_name,checksum,backfill_cursor,backfill_done "
        "FROM " + _STATE + " WHERE slot=?",
        (_STATE_SLOT,),
    ).fetchone()


def _state_identity(state):
    return state[0], state[1], state[2]


def _state_published(db, state):
    if not _has_table(db, _LEDGER):
        return False
    row = db.execute(
        "SELECT checksum FROM " + _LEDGER + " WHERE version=?", (state[0],)
    ).fetchone()
    return row is not None and row[0] == state[2]


def _names(online):
    base = "_online_u%d_%s" % (online.version, online.table)
    return {
        "keys": base + "_keys",
        "keys_key": base + "_keys_key",
        "dups": base + "_dups",
        "capture_insert": base + "_capture_insert",
        "capture_update": base + "_capture_update",
        "capture_delete": base + "_capture_delete",
        "enforce_insert": base + "_enforce_insert",
        "enforce_update": base + "_enforce_update",
    }


def _plan_keys(plan):
    return [(item.version, item.checksum()) for item in plan]


# --------------------------------------------------------------------------
# Plan validation


def _parse_plan(plan, require_online=False, online_last=False):
    """Validate plan shape and return ``(tuple_plan, online_or_None)``.

    Ordinary-only plans keep the historical behaviour: the shape check matches
    the original executor and the caller's prefix check reports short plans.
    """
    try:
        plan = tuple(plan)
    except TypeError:
        raise ValueError("plan must be an iterable of migrations") from None

    online = None
    for ordinal, item in enumerate(plan, 1):
        version = getattr(item, "version", None)
        if type(version) is not int or version != ordinal:
            raise ValueError("plans must be complete and numbered consecutively from 1")
        if isinstance(item, OnlineUnique):
            online = item
        elif isinstance(item, ReleaseArtifactReclaim):
            # The reclaim phase carries no SQL statements but is an ordinary,
            # atomic ledger phase handled by apply_plan itself.
            pass
        elif isinstance(item, (WorkerProfileRollout, RestoreWorkerProfiles)):
            # Both are ordinary atomic ledger phases executed by apply_plan.
            pass
        else:
            if not getattr(item, "statements", None) or not callable(
                getattr(item, "checksum", None)
            ):
                raise ValueError("plans must be complete and numbered consecutively from 1")

    by_version = {item.version: item for item in plan}
    for item in plan:
        if isinstance(item, RestoreWorkerProfiles):
            rollout = by_version.get(item.rollout_version)
            if (
                item.rollout_version != item.version - 1
                or not isinstance(rollout, WorkerProfileRollout)
            ):
                raise ValueError(
                    "a restore must refer to the immediately preceding rollout phase"
                )

    online_count = sum(isinstance(item, OnlineUnique) for item in plan)
    if online_count > 1:
        raise ValueError("a plan may contain at most one online migration")
    if online is not None:
        if len(plan) > 128:
            raise ValueError("plans containing an online migration are limited to 128 versions")
        if online_last and online is not plan[-1]:
            raise ValueError("an online plan must end with its online version")
    if require_online and online is None:
        raise ValueError("the plan must contain an online migration")
    return plan, online


# --------------------------------------------------------------------------
# Auxiliary schema


def _create_capture_objects(db, online):
    names = _names(online)
    keys = _quote(names["keys"])
    dups = _quote(names["dups"])
    table = _quote(online.table)
    route = _quote("route_key")
    key = _quote("key")
    ident = _quote("id")

    db.execute(
        "CREATE TABLE IF NOT EXISTS " + _STATE + " ("
        "slot INTEGER PRIMARY KEY CHECK (slot=" + str(_STATE_SLOT) + "),"
        "version INTEGER NOT NULL, table_name TEXT NOT NULL,"
        "checksum TEXT NOT NULL, backfill_cursor INTEGER NOT NULL,"
        "backfill_done INTEGER NOT NULL)"
    )

    # One row per committed non-NULL key, keyed by id so that single-row
    # writes only touch O(log n) index entries.  The secondary index supports
    # "all ids for this key" and the constant-time duplicate probes below.
    db.execute(
        "CREATE TABLE %s (%s INTEGER PRIMARY KEY, %s TEXT NOT NULL COLLATE BINARY) "
        "WITHOUT ROWID" % (keys, ident, key)
    )
    db.execute("CREATE INDEX %s ON %s (%s, %s)" % (_quote(names["keys_key"]), keys, key, ident))
    # Keys known to have at least two captured ids.  Maintained incrementally
    # with LIMIT 1 OFFSET 1 probes so no probe walks a large duplicate group.
    db.execute(
        "CREATE TABLE %s (%s TEXT NOT NULL COLLATE BINARY PRIMARY KEY) WITHOUT ROWID"
        % (dups, key)
    )

    has_second = "(SELECT 1 FROM %s WHERE %s=%%s LIMIT 1 OFFSET 1)" % (keys, key)
    db.execute(
        f"CREATE TRIGGER {_quote(names['capture_insert'])} AFTER INSERT ON {table} "
        f"WHEN NEW.{route} IS NOT NULL BEGIN "
        f"INSERT OR IGNORE INTO {keys} ({ident}, {key}) VALUES (NEW.{ident}, NEW.{route}); "
        f"INSERT OR IGNORE INTO {dups} ({key}) SELECT NEW.{route} "
        f"WHERE EXISTS {has_second % ('NEW.' + route)}; "
        f"END"
    )
    db.execute(
        f"CREATE TRIGGER {_quote(names['capture_update'])} AFTER UPDATE OF {route} ON {table} BEGIN "
        f"DELETE FROM {keys} WHERE {ident}=OLD.{ident}; "
        f"DELETE FROM {dups} WHERE {key}=OLD.{route} "
        f"AND NOT EXISTS {has_second % ('OLD.' + route)}; "
        f"INSERT OR IGNORE INTO {keys} ({ident}, {key}) "
        f"SELECT NEW.{ident}, NEW.{route} WHERE NEW.{route} IS NOT NULL; "
        f"INSERT OR IGNORE INTO {dups} ({key}) SELECT NEW.{route} "
        f"WHERE NEW.{route} IS NOT NULL AND EXISTS {has_second % ('NEW.' + route)}; "
        f"END"
    )
    db.execute(
        f"CREATE TRIGGER {_quote(names['capture_delete'])} AFTER DELETE ON {table} BEGIN "
        f"DELETE FROM {keys} WHERE {ident}=OLD.{ident}; "
        f"DELETE FROM {dups} WHERE {key}=OLD.{route} "
        f"AND NOT EXISTS {has_second % ('OLD.' + route)}; "
        f"END"
    )


def _create_enforcement_objects(db, online):
    names = _names(online)
    keys = _quote(names["keys"])
    table = _quote(online.table)
    route = _quote("route_key")
    key = _quote("key")
    ident = _quote("id")
    message = _sql_literal("online unique constraint: duplicate route_key")

    db.execute(
        f"CREATE TRIGGER {_quote(names['enforce_insert'])} BEFORE INSERT ON {table} "
        f"WHEN NEW.{route} IS NOT NULL BEGIN "
        f"SELECT RAISE(ABORT, {message}) WHERE EXISTS "
        f"(SELECT 1 FROM {keys} WHERE {key}=NEW.{route} AND {ident}<>NEW.{ident}); "
        f"END"
    )
    db.execute(
        f"CREATE TRIGGER {_quote(names['enforce_update'])} BEFORE UPDATE OF {route} ON {table} "
        f"WHEN NEW.{route} IS NOT NULL BEGIN "
        f"SELECT RAISE(ABORT, {message}) WHERE EXISTS "
        f"(SELECT 1 FROM {keys} WHERE {key}=NEW.{route} AND {ident}<>OLD.{ident}); "
        f"END"
    )


# --------------------------------------------------------------------------
# Online entry points


def prepare_online_unique(path, plan):
    """Install online bookkeeping; return True the first time, False afterwards."""
    plan, online = _parse_plan(plan, require_online=True, online_last=True)
    keys = _plan_keys(plan)
    prefix = keys[:-1]
    identity = (online.version, online.table, online.checksum())

    with closing(_open(path)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            "CREATE TABLE IF NOT EXISTS " + _LEDGER
            + " (version INTEGER PRIMARY KEY, checksum TEXT NOT NULL)"
        )
        history = _history(db)
        state = _load_state(db)

        if state is not None:
            if _state_identity(state) != identity:
                raise HistoryMismatch("another online publication occupies this database")
            if _state_published(db, state):
                if history[:online.version] != keys[:online.version]:
                    raise HistoryMismatch("applied plan is not an exact prefix")
                return False
            if history != prefix:
                raise HistoryMismatch("applied plan is not an exact prefix")
            # Already prepared: keep progress and released status untouched.
            return False

        # No state.  Repeating after publication (or restoring a backup) must
        # not recreate bookkeeping or reset anything.
        if len(history) >= online.version and history[:online.version] == keys[:online.version]:
            return False
        if history != prefix:
            raise HistoryMismatch("applied plan is not an exact prefix")

        _create_capture_objects(db, online)
        db.execute(
            "INSERT INTO " + _STATE + " "
            "(slot,version,table_name,checksum,backfill_cursor,backfill_done) "
            "VALUES (?,?,?,?,0,0)",
            (_STATE_SLOT,) + identity,
        )
        return True


def _load_for_online(db, keys, online):
    """Resolve ``(state, published)`` for an online entry point.

    Raises HistoryMismatch for a bad prefix or a foreign online publication,
    and OnlineMigrationRequired when the prefix matches but prepare has not run.
    """
    history = _history(db)
    state = _load_state(db)
    identity = (online.version, online.table, online.checksum())

    if state is not None:
        if _state_identity(state) != identity:
            raise HistoryMismatch("another online publication occupies this database")
        if _state_published(db, state):
            if history[:online.version] != keys[:online.version]:
                raise HistoryMismatch("applied plan is not an exact prefix")
            return state, True
        if history != keys[:-1]:
            raise HistoryMismatch("the required ordinary prefix is not registered exactly")
        return state, False

    if len(history) >= online.version and history[:online.version] == keys[:online.version]:
        return None, True
    if history != keys[:-1]:
        raise HistoryMismatch("the required ordinary prefix is not registered exactly")
    raise OnlineMigrationRequired("prepare_online_unique must run first")


def _capture_row(db, names, ident, key):
    """Make the capture entry authoritative for one target row (backfill)."""
    keys = _quote(names["keys"])
    dups = _quote(names["dups"])
    key_col = _quote("key")
    ident_col = _quote("id")
    # Replace rather than ignore: a trigger may already have recorded this id,
    # and the backfilled row is the authoritative committed value.  This also
    # re-runs the duplicate probe so a previously unseen partner is marked.
    db.execute(f"DELETE FROM {keys} WHERE {ident_col}=?", (ident,))
    db.execute(
        f"INSERT INTO {keys} ({ident_col}, {key_col}) VALUES (?,?)", (ident, key)
    )
    db.execute(
        f"INSERT OR IGNORE INTO {dups} ({key_col}) SELECT ? WHERE EXISTS "
        f"(SELECT 1 FROM {keys} WHERE {key_col}=? LIMIT 1 OFFSET 1)",
        (key, key),
    )


def step_online_unique(path, plan, limit):
    """Process at most ``limit`` pending ids in one short transaction."""
    plan, online = _parse_plan(plan, require_online=True, online_last=True)
    if type(limit) is not int or not 1 <= limit <= 256:
        raise ValueError("limit must be an integer between 1 and 256")
    keys = _plan_keys(plan)

    with closing(_open(path)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        state, published = _load_for_online(db, keys, online)
        if published:
            return 0
        if state[4]:
            return 0

        table = _quote(online.table)
        route = _quote("route_key")
        ident = _quote("id")
        rows = db.execute(
            f"SELECT {ident}, {route} FROM {table} WHERE {ident}>? "
            f"ORDER BY {ident} LIMIT ?",
            (state[3], limit),
        ).fetchall()

        names = _names(online)
        for row_id, key in rows:
            if key is not None:
                _capture_row(db, names, row_id, key)

        cursor = rows[-1][0] if rows else state[3]
        done = len(rows) < limit
        db.execute(
            "UPDATE " + _STATE + " SET backfill_cursor=?,backfill_done=? WHERE slot=?",
            (cursor, 1 if done else 0, _STATE_SLOT),
        )
        return len(rows)


def publish_online_unique(path, plan):
    """Atomically record the online version and enable uniqueness enforcement.

    Returns ``(status, witness)`` where status is ``not_ready``, ``conflict``,
    ``published`` or ``already_published``.
    """
    plan, online = _parse_plan(plan, require_online=True, online_last=True)
    keys = _plan_keys(plan)

    with closing(_open(path)) as db, db:
        db.execute("BEGIN IMMEDIATE")
        state, published = _load_for_online(db, keys, online)
        if published:
            return "already_published", None
        if not state[4]:
            return "not_ready", None

        names = _names(online)
        keys_table = _quote(names["keys"])
        dups = _quote(names["dups"])
        key_col = _quote("key")
        ident_col = _quote("id")

        # Readiness check: the duplicate index is maintained incrementally by
        # the capture triggers, so this is one indexed probe plus, at most, a
        # single witness lookup.
        row = db.execute(
            f"SELECT {key_col} FROM {dups} ORDER BY {key_col} LIMIT 1"
        ).fetchone()
        if row is not None:
            occupied = db.execute(
                f"SELECT {ident_col} FROM {keys_table} WHERE {key_col}=? "
                f"ORDER BY {ident_col} LIMIT 2",
                (row[0],),
            ).fetchall()
            if len(occupied) >= 2:
                return "conflict", (row[0], occupied[0][0], occupied[1][0])
            # Unreachable given the trigger invariants; heal a stale marker and
            # report pending instead of misreporting or crashing.
            db.execute(f"DELETE FROM {dups} WHERE {key_col}=?", (row[0],))
            return "not_ready", None

        # Same transaction: the ledger entry, the enforcing triggers and the
        # readiness decision become visible together or not at all.
        db.execute(
            "INSERT INTO " + _LEDGER + " VALUES (?,?)",
            (online.version, online.checksum()),
        )
        _create_enforcement_objects(db, online)
        return "published", None


# --------------------------------------------------------------------------
# Release artifact reclamation


_RECLAIM_TEMP_TABLE = "_migration_reclaim_blobs"
_RECLAIM_WEIGHT_TEMP_TABLE = "_migration_reclaim_weights"
_RECLAIM_SCORE_TEMP_TABLE = "_migration_reclaim_scores"


def _select_release_reclaim(db, reclaim):
    """Return ``(selected_release_ids, selected_mask)``.

    Catalog reads are set-based/index-assisted.  Scoring uses bounded SQLite
    temporary tables: one holder-mask/weight row per distinct mask and at most
    sum(C(20, s), s=0..max_releases) subset rows.
    """
    requested = reclaim.candidate_ids
    requested_marks = ",".join("?" for _ in requested)
    eligible = [
        row[0]
        for row in db.execute(
            "SELECT id FROM releases "
            "WHERE retired=1 AND id IN (" + requested_marks + ") ORDER BY id",
            requested,
        )
    ]
    count = len(eligible)
    if count == 0:
        return (), 0

    names = {f"e{i}": release_id for i, release_id in enumerate(eligible)}
    holder_terms = " + ".join(
        f"MAX(CASE WHEN rb.release_id=:e{i} THEN {1 << i} ELSE 0 END)"
        for i in range(count)
    )
    eligible_list = ",".join(f":e{i}" for i in range(count))
    temp_table = _quote(_RECLAIM_TEMP_TABLE)

    # The inner lookup starts from candidate releases via the release_id
    # leading column of the primary key.  The outer blob_id lookup then uses
    # the supplied (blob_id, release_id) index to inspect every holder.  The
    # HAVING clause retains only blobs whose holders are all eligible
    # candidates.  A later subset mask checks whether all of those holders are
    # in the chosen batch.
    db.execute("DROP TABLE IF EXISTS temp." + temp_table)
    db.execute(
        "CREATE TEMP TABLE " + temp_table + " ("
        "blob_id INTEGER PRIMARY KEY, "
        "holder_mask INTEGER NOT NULL, "
        "size_bytes INTEGER NOT NULL)"
    )
    db.execute(
        "INSERT INTO " + temp_table + " (blob_id, holder_mask, size_bytes) "
        "SELECT rb.blob_id, " + holder_terms + ", MAX(b.size_bytes) "
        "FROM release_blobs rb "
        "JOIN blobs b ON b.id = rb.blob_id "
        "WHERE rb.blob_id IN ("
        "SELECT blob_id FROM release_blobs WHERE release_id IN (" + eligible_list + ")"
        ") "
        "GROUP BY rb.blob_id "
        "HAVING SUM(CASE WHEN rb.release_id IN (" + eligible_list + ") "
        "THEN 0 ELSE 1 END) = 0",
        names,
    )

    quota = min(reclaim.max_releases, count)
    weight_table = _quote(_RECLAIM_WEIGHT_TEMP_TABLE)
    score_table = _quote(_RECLAIM_SCORE_TEMP_TABLE)
    db.execute("DROP TABLE IF EXISTS temp." + weight_table)
    db.execute("DROP TABLE IF EXISTS temp." + score_table)

    # Aggregate duplicate holder masks before scoring.  Both temporary tables
    # have bounded size: at most one weight row per catalog blob and one score
    # row per subset of at most max_releases candidates.
    db.execute(
        "CREATE TEMP TABLE " + weight_table + " ("
        "holder_mask INTEGER PRIMARY KEY, total_bytes INTEGER NOT NULL)"
    )
    db.execute(
        "INSERT INTO " + weight_table + " "
        "SELECT holder_mask, SUM(size_bytes) FROM " + temp_table + " "
        "GROUP BY holder_mask"
    )
    db.execute(
        "CREATE TEMP TABLE " + score_table + " ("
        "mask INTEGER PRIMARY KEY, release_count INTEGER NOT NULL, "
        "benefit INTEGER NOT NULL)"
    )
    db.execute(
        """
        WITH RECURSIVE
        bits(bit) AS (
            SELECT 0
            UNION ALL
            SELECT bit + 1 FROM bits WHERE bit < ?
        ),
        subsets(mask, release_count, last_bit) AS (
            SELECT 0, 0, -1
            UNION ALL
            SELECT subsets.mask | (1 << bits.bit),
                   subsets.release_count + 1,
                   bits.bit
            FROM subsets
            JOIN bits ON bits.bit > subsets.last_bit
            WHERE subsets.release_count < ?
        )
        INSERT INTO """ + score_table + """ (mask, release_count, benefit)
        SELECT subsets.mask,
               subsets.release_count,
               COALESCE(""" + weight_table + """.total_bytes, 0)
        FROM subsets
        LEFT JOIN """ + weight_table + """
          ON """ + weight_table + """.holder_mask = subsets.mask
        """,
        (count - 1, quota),
    )

    # Set-based subset zeta transform.  For each bit, add the score of the
    # mask without that bit.  Every required source mask has one fewer bit and
    # therefore exists in the bounded score table.  At completion,
    # benefit[mask] is the sum of all exact holder masks contained in mask.
    for bit_index in range(count):
        bit = 1 << bit_index
        db.execute(
            "UPDATE " + score_table + " "
            "SET benefit = benefit + COALESCE(("
            "SELECT source.benefit FROM " + score_table + " AS source "
            "WHERE source.mask = " + score_table + ".mask - ?), 0) "
            "WHERE (mask & ?) <> 0",
            (bit, bit),
        )

    best_mask = 0
    best_count = 0
    best_benefit = 0

    # Stream rows instead of fetching all scores into Python.  Tie comparison
    # uses the lowest differing bit: the mask containing that bit has the
    # lexicographically smaller ascending-id sequence.
    cursor = db.execute(
        "SELECT mask, benefit, release_count FROM " + score_table
    )
    for mask, benefit, release_count in cursor:
        replace = False
        if benefit > best_benefit:
            replace = True
        elif benefit == best_benefit:
            if release_count < best_count:
                replace = True
            elif release_count == best_count and mask != best_mask:
                differing_bit = (mask ^ best_mask) & -(mask ^ best_mask)
                if mask & differing_bit:
                    replace = True
        if replace:
            best_mask = mask
            best_count = release_count
            best_benefit = benefit

    selected_ids = [
        eligible[index]
        for index in range(count)
        if best_mask & (1 << index)
    ]

    db.execute("DROP TABLE temp." + weight_table)
    db.execute("DROP TABLE temp." + score_table)
    return tuple(selected_ids), best_mask


def _apply_release_artifact_reclaim(db, reclaim):
    selected_ids, selected_mask = _select_release_reclaim(db, reclaim)
    temp_table = _quote(_RECLAIM_TEMP_TABLE)

    if selected_ids:
        marks = ",".join("?" for _ in selected_ids)

        # Remove child rows first because foreign keys remain enforced and
        # there are no cascading deletes.
        db.execute(
            "DELETE FROM release_blobs WHERE release_id IN (" + marks + ")",
            selected_ids,
        )
        # The temp table recorded only externally unblocked blobs.  The mask
        # test additionally requires every eligible holder to be selected.
        db.execute(
            "DELETE FROM blobs WHERE id IN ("
            "SELECT blob_id FROM " + temp_table + " "
            "WHERE (holder_mask & ?) = holder_mask)",
            (selected_mask,),
        )
        db.execute(
            "DELETE FROM releases WHERE id IN (" + marks + ")",
            selected_ids,
        )

    db.execute("DROP TABLE IF EXISTS temp." + temp_table)


# --------------------------------------------------------------------------
# Worker profile rollout and restoration


def _ensure_delta_table(db):
    # SQL NULL means "no such profile row at that point"; BLOB 0 bytes is a
    # valid configuration distinct from absence.
    db.execute(
        "CREATE TABLE IF NOT EXISTS " + _DELTA_TABLE + " ("
        "rollout_version INTEGER NOT NULL, profile_id INTEGER NOT NULL, "
        "before_config BLOB, after_config BLOB, "
        "PRIMARY KEY (rollout_version, profile_id))"
    )


def _apply_worker_profile_rollout(db, rollout):
    """Run the batch, record one net-change row per id, and ledger the version.

    The baseline is the state just before this rollout's first statement; the
    final state is that after its last statement.  ``before`` is materialised
    in a bounded SQLite temporary table (the only touched ids), never in
    Python memory.  Everything commits with the surrounding apply_plan
    transaction, so a later statement failure rolls it all back.
    """
    count = db.execute(
        "SELECT COUNT(*) FROM " + _PROFILE_TABLE
    ).fetchone()[0]
    if count > 250000:
        raise ValueError("too many worker profiles to snapshot safely")

    _ensure_delta_table(db)
    db.execute("DROP TABLE IF EXISTS temp." + _quote(_ROLLOUT_SNAPSHOT))
    db.execute(
        "CREATE TEMP TABLE " + _quote(_ROLLOUT_SNAPSHOT) + " ("
        "profile_id INTEGER PRIMARY KEY, config BLOB NOT NULL)"
    )
    db.execute(
        "INSERT INTO temp." + _quote(_ROLLOUT_SNAPSHOT) + " (profile_id, config) "
        "SELECT id, CAST(config AS BLOB) FROM " + _PROFILE_TABLE
    )

    db.set_authorizer(_authorize)
    try:
        for statement in rollout.statements:
            db.execute(statement)
    finally:
        db.set_authorizer(lambda *_: sqlite3.SQLITE_OK)

    before = _quote(_ROLLOUT_SNAPSHOT)
    profiles = _quote(_PROFILE_TABLE)
    delta = _quote(_DELTA_TABLE)
    # One row per net-changed id whose before value was recorded above.  (A
    # pre-existing id can only be absent before if it never existed there.)
    db.execute(
        "INSERT INTO " + delta + " "
        "(rollout_version, profile_id, before_config, after_config) "
        "SELECT ?, b.profile_id, b.config, CAST(a.config AS BLOB) "
        "FROM temp." + before + " AS b LEFT JOIN " + profiles + " AS a "
        "ON a.id = b.profile_id "
        "WHERE a.config IS NULL OR CAST(a.config AS BLOB) IS NOT b.config",
        (rollout.version,),
    )
    # Ids absent before the rollout and created by it.
    db.execute(
        "INSERT INTO " + delta + " "
        "(rollout_version, profile_id, before_config, after_config) "
        "SELECT ?, a.id, NULL, CAST(a.config AS BLOB) "
        "FROM " + profiles + " AS a "
        "WHERE NOT EXISTS (SELECT 1 FROM temp." + before + " AS b "
        "WHERE b.profile_id = a.id)",
        (rollout.version,),
    )
    db.execute("DROP TABLE temp." + before)


def _apply_worker_profile_restore(db, restore):
    """Restore the adjacent rollout's net changes where the site is unchanged.

    The decision is made set-based in SQLite so no catalog is materialised in
    Python.  Deletion eligibility (after state absent), pin checks and the
    applied mutations all happen in the caller's transaction.
    """
    version = restore.rollout_version
    row = db.execute(
        "SELECT 1 FROM " + _LEDGER + " WHERE version=?", (version,)
    ).fetchone()
    if row is None:
        raise HistoryMismatch("the rollout to restore is not registered")
    if not _has_table(db, _DELTA_TABLE):
        raise HistoryMismatch("the rollout to restore has no recorded changes")

    profiles = _quote(_PROFILE_TABLE)
    delta = _quote(_DELTA_TABLE)
    pins = _quote("job_profile_pins")
    plan = _quote(_RESTORE_PLAN)

    # Decide every net-changed id at once.  "restore" means the site still
    # equals the recorded after state; then we move it to the before state.
    db.execute("DROP TABLE IF EXISTS temp." + plan)
    db.execute(
        "CREATE TEMP TABLE " + plan + " ("
        "profile_id INTEGER PRIMARY KEY, "
        "before_config BLOB, after_config BLOB, "
        "action TEXT NOT NULL)"
    )
    db.execute(
        "INSERT INTO temp." + plan + " "
        "(profile_id, before_config, after_config, action) "
        "SELECT d.profile_id, d.before_config, d.after_config, "
        "CASE "
        "WHEN d.after_config IS NULL THEN "
        "  CASE WHEN p.id IS NULL THEN 'restore' ELSE 'keep' END "
        "WHEN p.id IS NOT NULL "
        "  AND CAST(p.config AS BLOB) IS d.after_config THEN 'restore' "
        "ELSE 'keep' END "
        "FROM " + delta + " AS d "
        "LEFT JOIN " + profiles + " AS p ON p.id = d.profile_id "
        "WHERE d.rollout_version=?",
        (version,),
    )

    # A restore that would delete a profile (before state absent) is blocked
    # when any job still pins it.  Pick the numerically least (profile, job).
    blocked = db.execute(
        "SELECT r.profile_id, MIN(j.job_id) FROM temp." + plan + " AS r "
        "JOIN " + pins + " AS j ON j.profile_id = r.profile_id "
        "WHERE r.action='restore' AND r.before_config IS NULL "
        "GROUP BY r.profile_id ORDER BY r.profile_id LIMIT 1"
    ).fetchone()
    if blocked is not None:
        raise ProfileRestoreBlocked(blocked[0], blocked[1])

    # Updates: rows that exist and whose before state is a real config.
    db.execute(
        "UPDATE " + profiles + " SET config=("
        "SELECT r.before_config FROM temp." + plan + " AS r "
        "WHERE r.profile_id=" + profiles + ".id) "
        "WHERE id IN (SELECT profile_id FROM temp." + plan + " "
        "WHERE action='restore' AND before_config IS NOT NULL)"
    )
    # Deletions: rows that exist and whose before state was absent.
    db.execute(
        "DELETE FROM " + profiles + " WHERE id IN ("
        "SELECT r.profile_id FROM temp." + plan + " AS r "
        "WHERE r.action='restore' AND r.before_config IS NULL "
        "AND EXISTS (SELECT 1 FROM " + profiles + " AS p "
        "WHERE p.id = r.profile_id))"
    )
    # Re-insertions: the rollout deleted the profile and the site still lacks
    # it, so the recorded before state comes back.
    db.execute(
        "INSERT INTO " + profiles + " (id, config) "
        "SELECT r.profile_id, r.before_config FROM temp." + plan + " AS r "
        "WHERE r.action='restore' AND r.before_config IS NOT NULL "
        "AND r.after_config IS NULL"
    )
    db.execute("DROP TABLE temp." + plan)


# --------------------------------------------------------------------------
# Ordinary plans


def _authorize(action, first, second, *_):
    # The executor controls transactions and the ledger; a migration is trusted application SQL.
    if '_migration_ledger' in {str(first).casefold(), str(second).casefold()}:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_DENY if action in {
        sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT, sqlite3.SQLITE_ATTACH,
        sqlite3.SQLITE_DETACH, sqlite3.SQLITE_PRAGMA
    } else sqlite3.SQLITE_OK


def apply_plan(path, plan):
    plan, online = _parse_plan(plan)
    keys = _plan_keys(plan)

    with closing(_open(path)) as db, db:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("BEGIN IMMEDIATE")
        db.execute("""CREATE TABLE IF NOT EXISTS _migration_ledger (
            version INTEGER PRIMARY KEY, checksum TEXT NOT NULL)""")
        history = db.execute("SELECT version,checksum FROM _migration_ledger ORDER BY version").fetchall()
        if history != keys[:len(history)]:
            raise HistoryMismatch("applied plan is not an exact prefix")

        pending = plan[len(history):]
        if pending:
            # A restore may only undo a rollout that was already committed
            # before this call began: never run a rollout and its restore as
            # one uncommitted change.
            for item in pending:
                if (
                    isinstance(item, RestoreWorkerProfiles)
                    and item.rollout_version > len(history)
                ):
                    raise ValueError(
                        "the rollout must be registered before its restore can apply"
                    )
            state = _load_state(db)
            if state is not None:
                if online is not None and _state_identity(state) != (
                    online.version, online.table, online.checksum()
                ):
                    raise HistoryMismatch("another online publication occupies this database")
                if not _state_published(db, state):
                    raise OnlineMigrationRequired(
                        "the online migration must be published before the ledger advances"
                    )
            if online is not None and len(history) < online.version:
                raise OnlineMigrationRequired(
                    "the online migration must be published before it can be applied"
                )
            if any(isinstance(item, OnlineUnique) for item in pending):
                raise OnlineMigrationRequired(
                    "the online migration must be published before it can be applied"
                )

        applied = []
        for migration, (_, checksum) in zip(pending, keys[len(history):]):
            if isinstance(migration, WorkerProfileRollout):
                _apply_worker_profile_rollout(db, migration)
            elif isinstance(migration, RestoreWorkerProfiles):
                _apply_worker_profile_restore(db, migration)
            elif isinstance(migration, ReleaseArtifactReclaim):
                _apply_release_artifact_reclaim(db, migration)
            else:
                db.set_authorizer(_authorize)
                try:
                    for statement in migration.statements:
                        db.execute(statement)
                finally:
                    db.set_authorizer(lambda *_: sqlite3.SQLITE_OK)
            db.execute("INSERT INTO _migration_ledger VALUES (?,?)", (migration.version, checksum))
            applied.append(migration.version)
        return applied
