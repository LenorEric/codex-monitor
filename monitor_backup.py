#!/usr/bin/env python3
"""Write-only recorder logs. SQLite owns pending delivery and authoritative data."""

import json
import os
import sys
import threading
from pathlib import Path


def append_backup(path, rows):
    """Never inspect, repair, truncate or replace an existing recorder log.

    A separator before each batch isolates a torn final line after a crash. A
    crash after fsync but before acknowledgement can duplicate a backup batch.
    Record identities allow duplicate deliveries to be recognized during recovery.
    """
    if not rows:
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as stream:
        stream.write(b"\n")
        for row in rows:
            stream.write((json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))
        stream.flush()
        os.fsync(stream.fileno())


class BackupRecorder:
    def __init__(self, index):
        self.index, self.lock, self.error = index, threading.RLock(), None

    @staticmethod
    def enqueue(db, path, rows):
        db.executemany("INSERT INTO processing_backup_queue(path,data) VALUES(?,?)", ((str(Path(path).resolve()), json.dumps(row, ensure_ascii=False, separators=(",", ":"))) for row in rows))

    def flush(self):
        with self.lock:
            with self.index.transaction() as db:
                paths = [row[0] for row in db.execute("SELECT DISTINCT path FROM processing_backup_queue ORDER BY path")]
            errors = []
            for path in paths:
                while True:
                    with self.index.transaction() as db:
                        pending = list(db.execute("SELECT id,data FROM processing_backup_queue WHERE path=? ORDER BY id LIMIT 256", (path,)))
                    if not pending:
                        break
                    try:
                        append_backup(path, [json.loads(row[1]) for row in pending])
                    except OSError as exc:
                        errors.append(f"{path}: {exc}")
                        break  # One unavailable log must not block the other logs.
                    with self.index.transaction() as db:
                        db.executemany("DELETE FROM processing_backup_queue WHERE id=?", ((row[0],) for row in pending))
            error = "; ".join(errors) or None
            if error and self.error != error:
                print(f"Backup log write failed; committed SQLite data is safe and backup delivery will retry: {error}", file=sys.stderr, flush=True)
            self.error = error
