import shutil
import json
import sqlite3
import threading
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from unittest import mock

import monitor_processing
from monitor_auto_update import _migrate_history_to_v13
from monitor_processing import ProcessingIndex
from monitor_usage_sync import initialize_v4_cache


class SQLiteMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-vacuum-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.path = self.root / "cache.sqlite3"

    def fill(self, mode=0):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute(f"PRAGMA auto_vacuum={mode}")
            db.execute("CREATE TABLE preserved(id INTEGER PRIMARY KEY, body BLOB)")
            db.executemany("INSERT INTO preserved(body) VALUES(?)", [(b'x' * 4096,)] * 700)
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute("DELETE FROM preserved WHERE id>10")

    def test_new_database_uses_incremental_mode_from_either_initializer(self):
        for initialize in (initialize_v4_cache, ProcessingIndex):
            path = self.root / f"{initialize.__name__}.sqlite3"
            initialize(path)
            with closing(sqlite3.connect(path)) as db:
                self.assertEqual(db.execute("PRAGMA auto_vacuum").fetchone()[0], 2)

    def test_pending_migration_compacts_none_database_preserving_rows_and_is_retryable(self):
        self.fill()
        before = self.path.stat().st_size
        for _ in range(2):
            _migrate_history_to_v13({"processing_index": self.path, "dashboard_cache": self.root / "display.json"})
            with closing(sqlite3.connect(self.path)) as db:
                self.assertEqual(db.execute("PRAGMA auto_vacuum").fetchone()[0], 2)
                self.assertEqual(db.execute("SELECT count(*),sum(length(body)) FROM preserved").fetchone(), (10, 40960))
                self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        self.assertLess(self.path.stat().st_size, before // 2)

    def test_thresholds_bounded_reclamation_cooldown_and_busy_writer(self):
        self.fill(2)
        index = ProcessingIndex(self.path)
        before = self.path.stat().st_size
        with closing(sqlite3.connect(self.path)) as db:
            free = db.execute("PRAGMA freelist_count").fetchone()[0]
        self.assertEqual(index.maintain_disk_space(), 0)  # Below the default 100 MiB threshold.
        index._vacuum_check_at = 0
        with mock.patch.object(monitor_processing, "VACUUM_FREE_BYTES", 4096), mock.patch.object(monitor_processing, "VACUUM_BATCH_PAGES", 8):
            with closing(sqlite3.connect(self.path)) as writer:
                writer.execute("BEGIN IMMEDIATE")
                self.assertEqual(index.maintain_disk_space(), 0)
                writer.rollback()
            index._vacuum_check_at = 0
            reclaimed = index.maintain_disk_space()
            self.assertGreater(reclaimed, 0)
            self.assertLessEqual(reclaimed, 8)
            self.assertEqual(index.maintain_disk_space(), 0)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertLess(db.execute("PRAGMA freelist_count").fetchone()[0], free)
            self.assertEqual(db.execute("SELECT count(*) FROM preserved").fetchone()[0], 10)
            db.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchall()
        self.assertLess(self.path.stat().st_size, before)

    def test_ratio_threshold_and_index_lock_skip(self):
        self.fill(2)
        index = ProcessingIndex(self.path)
        with mock.patch.object(monitor_processing, "VACUUM_FREE_BYTES", 0), mock.patch.object(monitor_processing, "VACUUM_FREE_RATIO", 1):
            self.assertEqual(index.maintain_disk_space(), 0)
        entered, release = threading.Event(), threading.Event()
        def hold():
            with index.lock:
                entered.set()
                release.wait(5)
        worker = threading.Thread(target=hold)
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            index._vacuum_check_at = 0
            self.assertEqual(index.maintain_disk_space(), 0)
        finally:
            release.set()
            worker.join()

    def test_reader_snapshot_survives_incremental_reclamation(self):
        self.fill(2)
        index = ProcessingIndex(self.path)
        with closing(sqlite3.connect(self.path)) as reader:
            reader.execute("BEGIN")
            self.assertEqual(reader.execute("SELECT count(*) FROM preserved").fetchone()[0], 10)
            with mock.patch.object(monitor_processing, "VACUUM_FREE_BYTES", 0):
                self.assertGreater(index.maintain_disk_space(), 0)
            self.assertEqual(reader.execute("SELECT count(*) FROM preserved").fetchone()[0], 10)
            reader.rollback()

    def test_existing_none_database_is_not_rebuilt_during_live_maintenance(self):
        self.fill()
        index = ProcessingIndex(self.path)
        with mock.patch.object(monitor_processing, "VACUUM_FREE_BYTES", 0):
            self.assertEqual(index.maintain_disk_space(), 0)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("PRAGMA auto_vacuum").fetchone()[0], 0)

    def test_12_to_13_conversion_rolls_back_and_preserves_tables_on_retry(self):
        import monitor_auto_update
        self.fill()
        state = self.root / monitor_auto_update.DATA_MIGRATION_STATE_FILENAME
        state.write_text('{"dataContractVersion":12}', encoding="utf-8")
        # Use the configured sync database filename, including any committed WAL data.
        with closing(sqlite3.connect(self.path)) as source, closing(sqlite3.connect(self.root / "sync-v4.sqlite3")) as target:
            source.backup(target)
        def migrate():
            return monitor_auto_update.migrate_history_data(self.root, self.root / "history", self.root / "quota", self.root / "sessions",
                self.root / "ledger", self.root / "samples", usage_sync_cache_path=self.root / "sync.json")
        original = monitor_auto_update.DATA_MIGRATIONS[13]
        def interrupted(paths):
            original(paths)
            raise RuntimeError("interrupted conversion")
        with mock.patch.dict(monitor_auto_update.DATA_MIGRATIONS, {13: interrupted}), self.assertRaisesRegex(RuntimeError, "interrupted conversion"):
            migrate()
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 12)
        with closing(sqlite3.connect(self.root / "sync-v4.sqlite3")) as db:
            self.assertEqual(db.execute("PRAGMA auto_vacuum").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT count(*) FROM preserved").fetchone()[0], 10)
        self.assertEqual(migrate(), [13])
        self.assertEqual(migrate(), [])
        with closing(sqlite3.connect(self.root / "sync-v4.sqlite3")) as db:
            self.assertEqual(db.execute("PRAGMA auto_vacuum").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT count(*) FROM preserved").fetchone()[0], 10)

