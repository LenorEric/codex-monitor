"""Shared SQLite metadata and bounded space maintenance for persistent indexes."""

import json
import sqlite3
import time
from contextlib import closing

VACUUM_FREE_BYTES = 100 * 1024 * 1024
VACUUM_FREE_RATIO = .20
VACUUM_BATCH_PAGES = 256
VACUUM_BATCH_SECONDS = .05
VACUUM_CHECK_SECONDS = 60


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class IndexState:
    def maintain_disk_space(self):
        """Reclaim a bounded batch after cleanup; leave headroom for routine cache churn."""
        now = time.monotonic()
        if now < getattr(self, "_vacuum_check_at", 0) or not self.lock.acquire(blocking=False):
            return 0
        try:
            self._vacuum_check_at = now + VACUUM_CHECK_SECONDS
            with closing(sqlite3.connect(self.path, timeout=0, isolation_level=None)) as db:
                if db.execute("PRAGMA auto_vacuum").fetchone()[0] != 2:
                    return 0  # Existing NONE databases convert only during startup migration.
                page_size = db.execute("PRAGMA page_size").fetchone()[0]
                pages = db.execute("PRAGMA page_count").fetchone()[0]
                free = db.execute("PRAGMA freelist_count").fetchone()[0]
                if free * page_size <= VACUUM_FREE_BYTES or free / max(pages, 1) <= VACUUM_FREE_RATIO:
                    return 0
                reclaimed = 0
                while reclaimed < VACUUM_BATCH_PAGES and time.monotonic() - now < VACUUM_BATCH_SECONDS:
                    db.execute("PRAGMA incremental_vacuum(1)").fetchall()
                    reclaimed += 1
                # PASSIVE never waits for readers; another maintenance pass can finish checkpointing.
                db.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchall()
                return reclaimed
        except sqlite3.OperationalError as exc:
            if getattr(exc, "sqlite_errorcode", 0) & 255 not in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                raise
            return 0
        finally:
            self.lock.release()

    @staticmethod
    def get(db, name, default=None):
        row = db.execute("SELECT value FROM processing_meta WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else default

    @staticmethod
    def put(db, name, value):
        db.execute("INSERT INTO processing_meta VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value", (name, encode(value)))


