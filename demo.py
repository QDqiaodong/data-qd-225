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
    ack_route_changes,
    apply_plan,
    finish_route_snapshot,
    open_route_feed,
    prepare_online_unique,
    publish_online_unique,
    read_route_changes,
    read_route_snapshot,
    step_online_unique,
)

# -- route feed: consistent snapshot then recoverable change batches ------
with TemporaryDirectory() as root:
    path = Path(root, "feed.db")
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "CREATE TABLE route_catalog(id INTEGER PRIMARY KEY, payload BLOB NOT NULL)"
        )
        db.executemany(
            "INSERT INTO route_catalog(id,payload) VALUES (?,?)",
            [(10, b"ten"), (30, b"thirty"), (50, b"fifty")],
        )
    bootstrap = Migration(
        1, ("CREATE TABLE deployment_markers(id INTEGER PRIMARY KEY)",)
    )
    apply_plan(path, [bootstrap])

    base = open_route_feed(path, 3)
    print("feed cut point:", base)
    pages = []
    cursor = 0
    while True:
        page, cursor = read_route_snapshot(path, cursor, 2)
        pages.extend(page)
        if cursor is None:
            break
    print("snapshot pages:", pages)

    churn = [
        bootstrap,
        Migration(2, (
            "UPDATE route_catalog SET payload=x'74656e2d78' WHERE id=10",
            "DELETE FROM route_catalog WHERE id=30",
            "INSERT INTO route_catalog(id,payload) VALUES (20, x'7477656e7479')",
        )),
        Migration(3, (
            "UPDATE route_catalog SET payload=x'6669667479' WHERE id=50",
        )),
    ]
    print("churn applied:", apply_plan(path, churn))
    # Snapshot confirmation releases the retained cut point; batches survive.
    print("snapshot confirmed:", finish_route_snapshot(path, base),
          "retry:", finish_route_snapshot(path, base))
    print("first batch:", read_route_changes(path))
    version, _ = read_route_changes(path)
    print("ack:", ack_route_changes(path, version),
          "repeat ack:", ack_route_changes(path, version))

    one_more = churn + [
        Migration(4, (
            "INSERT INTO deployment_markers(id) VALUES (1)",
            "UPDATE route_catalog SET payload=x'66696674792d32' WHERE id=50",
        )),
    ]
    print("next apply:", apply_plan(path, one_more))
    print("next batch:", read_route_changes(path))

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
