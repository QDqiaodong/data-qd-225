from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from migrations import (
    Migration,
    OnlineUnique,
    ProfileRestoreBlocked,
    RestoreWorkerProfiles,
    WorkerProfileRollout,
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

# -- worker profile rollout / restore ------------------------------------
with TemporaryDirectory() as root:
    path = Path(root, "profiles.db")
    setup = Migration(1, (
        "CREATE TABLE worker_profiles(id INTEGER PRIMARY KEY, config BLOB NOT NULL)",
        "CREATE TABLE job_profile_pins(job_id INTEGER PRIMARY KEY, "
        "profile_id INTEGER NOT NULL REFERENCES worker_profiles(id) ON DELETE RESTRICT)",
        "CREATE INDEX idx_pin_profile ON job_profile_pins(profile_id, job_id)",
        "INSERT INTO worker_profiles(id, config) VALUES (10, x'aa')",
        "INSERT INTO worker_profiles(id, config) VALUES (20, x'bb')",
    ))
    rollout = WorkerProfileRollout(2, (
        "UPDATE worker_profiles SET config=x'11' WHERE id=10",
        "DELETE FROM worker_profiles WHERE id=20",
        "INSERT INTO worker_profiles(id, config) VALUES (40, x'dd')",
    ))
    restore = RestoreWorkerProfiles(3, 2)

    print("rollout:", apply_plan(path, [setup, rollout]))
    with closing(sqlite3.connect(path)) as db:
        print("delta:", db.execute(
            "SELECT profile_id, hex(before_config), hex(after_config) "
            "FROM _worker_profile_rollout_delta ORDER BY profile_id"
        ).fetchall())
        db.execute("BEGIN")
        db.execute("INSERT INTO job_profile_pins VALUES (1, 40)")
        db.commit()
    try:
        apply_plan(path, [setup, rollout, restore])
    except ProfileRestoreBlocked as error:
        print("restore blocked by:", error.profile_id, error.job_id)
    with closing(sqlite3.connect(path)) as db:
        db.execute("DELETE FROM job_profile_pins")
        db.commit()
    print("restore:", apply_plan(path, [setup, rollout, restore]))
    with closing(sqlite3.connect(path)) as db:
        print("profiles:", db.execute(
            "SELECT id, hex(config) FROM worker_profiles ORDER BY id"
        ).fetchall())
