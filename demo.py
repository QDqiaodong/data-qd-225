from contextlib import closing
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from migrations import Migration, apply_plan

with TemporaryDirectory() as root:
    path = Path(root, "service.db")
    initial = Migration(1, ("CREATE TABLE deployment(id INTEGER PRIMARY KEY)",))
    add = Migration(2, ("ALTER TABLE deployment ADD COLUMN region TEXT",))
    print("bootstrap:", apply_plan(path, [initial]))
    try:
        apply_plan(path, [initial, add, Migration(3, ("INSERT INTO absent VALUES (1)",))])
    except sqlite3.OperationalError as error:
        print("rejected:", error)
    with closing(sqlite3.connect(path)) as db:
        print("after rollback:", [row[1] for row in db.execute("PRAGMA table_info(deployment)")])
    print("upgrade:", apply_plan(path, [initial, add]))
    print("retry:", apply_plan(path, [initial, add]))
