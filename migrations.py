"""Apply a numbered, checksummed SQLite migration plan as an atomic batch."""
from contextlib import closing
from dataclasses import dataclass
import hashlib
import json
import sqlite3


@dataclass(frozen=True)
class Migration:
    version: int
    statements: tuple

    def checksum(self):
        return hashlib.sha256(json.dumps(self.statements, ensure_ascii=False).encode()).hexdigest()


class HistoryMismatch(Exception):
    pass


def _authorize(action, first, second, *_):
    # The executor controls transactions and the ledger; a migration is trusted application SQL.
    if '_migration_ledger' in {str(first).casefold(), str(second).casefold()}:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_DENY if action in {
        sqlite3.SQLITE_TRANSACTION, sqlite3.SQLITE_SAVEPOINT, sqlite3.SQLITE_ATTACH,
        sqlite3.SQLITE_DETACH, sqlite3.SQLITE_PRAGMA
    } else sqlite3.SQLITE_OK


def apply_plan(path, plan):
    plan = tuple(plan)
    if any(type(item.version) is not int or item.version != ordinal or not item.statements
           for ordinal, item in enumerate(plan, 1)):
        raise ValueError("plans must be complete and numbered consecutively from 1")
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("BEGIN IMMEDIATE")
        db.execute("""CREATE TABLE IF NOT EXISTS _migration_ledger (
            version INTEGER PRIMARY KEY, checksum TEXT NOT NULL)""")
        history = db.execute("SELECT version,checksum FROM _migration_ledger ORDER BY version").fetchall()
        keys = [(migration.version, migration.checksum()) for migration in plan]
        if history != keys[:len(history)]:
            raise HistoryMismatch("applied plan is not an exact prefix")
        applied = []
        for migration, (_, checksum) in zip(plan[len(history):], keys[len(history):]):
            db.set_authorizer(_authorize)
            try:
                for statement in migration.statements:
                    db.execute(statement)
            finally:
                db.set_authorizer(lambda *_: sqlite3.SQLITE_OK)
            db.execute("INSERT INTO _migration_ledger VALUES (?,?)", (migration.version, checksum))
            applied.append(migration.version)
        return applied
