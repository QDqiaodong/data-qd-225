from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from migrations import (
    Migration,
    OnlineUnique,
    apply_plan,
    prepare_online_unique,
    publish_online_unique,
    step_online_unique,
)

with TemporaryDirectory() as root:
    path = Path(root, "service.db")
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "CREATE TABLE routes(id INTEGER PRIMARY KEY, route_key TEXT COLLATE BINARY)"
        )
        db.executemany(
            "INSERT INTO routes(id,route_key) VALUES (?,?)",
            [(1, "eu"), (2, "us"), (3, "eu"), (4, None)],
        )

    initial = Migration(1, ("CREATE TABLE deployment(id INTEGER PRIMARY KEY)",))
    online = OnlineUnique(2, "routes")
    plan = [initial, online]

    print("bootstrap:", apply_plan(path, [initial]))
    print("prepare:", prepare_online_unique(path, plan))
    print("prepare again:", prepare_online_unique(path, plan))
    print("step:", step_online_unique(path, plan, 2))
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("INSERT INTO routes(id,route_key) VALUES (0,'us')")  # smaller id, new dup
    while step_online_unique(path, plan, 2):
        pass
    print("publish:", publish_online_unique(path, plan))
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("UPDATE routes SET route_key='us-2' WHERE id=2")
        db.execute("UPDATE routes SET route_key='us-0' WHERE id=0")
        db.execute("UPDATE routes SET route_key='eu-3' WHERE id=3")
    print("publish after repair:", publish_online_unique(path, plan))
    print("publish again:", publish_online_unique(path, plan))
    with closing(sqlite3.connect(path)) as db:
        try:
            db.execute("INSERT INTO routes(id,route_key) VALUES (9,'eu')")
        except sqlite3.IntegrityError as error:
            print("rejected duplicate:", error)
        print("rows:", db.execute("SELECT id,route_key FROM routes ORDER BY id").fetchall())
