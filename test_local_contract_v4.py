import json
import shutil
import unittest
import uuid
import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path

import monitor_auto_update
import monitor_history
import monitor_token_ledger


@contextmanager
def workspace_directory():
    path = Path.cwd() / f".test-local-contract-v4-{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


class LocalContractV4Tests(unittest.TestCase):
    def test_v8_preserves_canonical_files_and_durable_v4_database(self):
        from monitor_usage_sync import initialize_v4_cache
        with workspace_directory() as root:
            state = root / monitor_auto_update.DATA_MIGRATION_STATE_FILENAME
            state.write_text('{"dataContractVersion":7}\n', encoding="utf-8")
            quota, ledger, cache = root / "quota.jsonl", root / "ledger.jsonl", root / "cache.json"
            quota.write_bytes(b'{"raw":"quota"}\n')
            ledger.write_bytes(b'{"raw":"ledger"}\n')
            database = cache.with_name("cache-v4.sqlite3")
            initialize_v4_cache(database)
            with closing(sqlite3.connect(database)) as db, db:
                db.execute("INSERT INTO local_records VALUES('key','2030-01-01T00:00Z','digest','{}')")
            args = (root, root / "history.jsonl", quota, root / "sessions.jsonl", ledger, root / "samples.jsonl")
            self.assertEqual(monitor_auto_update.migrate_history_data(*args, usage_sync_cache_path=cache), [8])
            with closing(sqlite3.connect(database)) as db, db:
                self.assertEqual(db.execute("SELECT * FROM local_records").fetchall(), [('key', '2030-01-01T00:00Z', 'digest', '{}')])
                self.assertIsNotNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_scan_sources'").fetchone())
            self.assertEqual(quota.read_bytes(), b'{"raw":"quota"}\n')
            self.assertEqual(ledger.read_bytes(), b'{"raw":"ledger"}\n')
            self.assertEqual(monitor_auto_update.migrate_history_data(*args, usage_sync_cache_path=cache), [])

    def test_sqlite_migration_backup_and_rollback_include_wal(self):
        with workspace_directory() as root:
            path, state, journal = root / "cache.sqlite3", root / "state.json", root / "journal.json"
            db = sqlite3.connect(path)
            try:
                db.execute("PRAGMA journal_mode=WAL")
                db.execute("CREATE TABLE durable(value TEXT)")
                db.execute("INSERT INTO durable VALUES('original')")
                db.commit()
                def fail(paths):
                    db.execute("UPDATE durable SET value='modified'")
                    db.commit()
                    raise RuntimeError("interrupted")
                with self.assertRaisesRegex(RuntimeError, "interrupted"):
                    monitor_auto_update._run_data_migration(fail, {"processing_index": path}, 8, state, journal)
                self.assertEqual(db.execute("SELECT value FROM durable").fetchone()[0], "original")
                self.assertFalse(state.exists())
                self.assertFalse(journal.exists())
            finally:
                db.close()
    def test_v7_rebuilds_derived_dashboard_without_changing_raw_history(self):
        with workspace_directory() as root:
            (root / monitor_auto_update.DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":6}\n', encoding="utf-8")
            history = monitor_history.default_history_path(root)
            quota = monitor_history.default_quota_history_path(history)
            ledger = monitor_token_ledger.default_token_ledger_path(history)
            quota.write_text('{"raw":"quota"}\n', encoding="utf-8")
            ledger.write_text('{"raw":"tokens"}\n', encoding="utf-8")
            cache = root / "usage_monitor_dashboard_cache.json"
            cache.write_text('{"version":3}\n', encoding="utf-8")
            args = (root, history, quota, root / "usage_monitor_token_sessions.jsonl", ledger, monitor_history.default_sample_log_path(history))
            self.assertEqual(monitor_auto_update.migrate_history_data(*args, target_version=7), [7])
            self.assertFalse(cache.exists())
            self.assertEqual(quota.read_text(encoding="utf-8"), '{"raw":"quota"}\n')
            self.assertEqual(ledger.read_text(encoding="utf-8"), '{"raw":"tokens"}\n')
            self.assertEqual(monitor_auto_update.migrate_history_data(*args, target_version=7), [])

    def test_default_recorders_are_named_for_their_data(self):
        with workspace_directory() as root:
            history = monitor_history.default_history_path(root)

            self.assertEqual(monitor_history.default_quota_history_path(history), root / "usage_monitor_quota_readings.jsonl")
            self.assertEqual(monitor_token_ledger.default_token_ledger_path(history), root / "usage_monitor_token_events.jsonl")
            self.assertEqual(monitor_history.default_sample_log_path(history), root / "usage_monitor_diagnostic_samples.jsonl")

    def test_v4_renames_default_files_and_merges_existing_destinations(self):
        with workspace_directory() as root:
            state = root / monitor_auto_update.DATA_MIGRATION_STATE_FILENAME
            state.write_text('{"dataContractVersion":3}\n', encoding="utf-8")
            legacy = {
                "usage_monitor_quota_history.jsonl": [{"source": "old-quota"}],
                "usage_monitor_token_ledger.jsonl": [{"source": "old-token"}],
                "usage_monitor_samples.jsonl": [{"source": "old-sample"}],
            }
            for name, rows in legacy.items():
                (root / name).write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            (root / "usage_monitor_quota_readings.jsonl").write_text('{"source":"new-quota"}\n', encoding="utf-8")

            history = monitor_history.default_history_path(root)
            completed = monitor_auto_update.migrate_history_data(
                root, history, monitor_history.default_quota_history_path(history), root / "usage_monitor_token_sessions.jsonl",
                monitor_token_ledger.default_token_ledger_path(history), monitor_history.default_sample_log_path(history), target_version=4,
            )

            self.assertEqual(completed, [4])
            self.assertEqual(json.loads(state.read_text(encoding="utf-8")), {"dataContractVersion": 4})
            self.assertEqual([json.loads(line) for line in (root / "usage_monitor_quota_readings.jsonl").read_text(encoding="utf-8").splitlines()], [{"source": "old-quota"}, {"source": "new-quota"}])
            self.assertEqual(json.loads((root / "usage_monitor_token_events.jsonl").read_text(encoding="utf-8")), {"source": "old-token"})
            self.assertEqual(json.loads((root / "usage_monitor_diagnostic_samples.jsonl").read_text(encoding="utf-8")), {"source": "old-sample"})
            self.assertTrue(all(not (root / name).exists() for name in legacy))

    def test_upgrade_from_unversioned_data_applies_old_contracts_before_rename(self):
        with workspace_directory() as root:
            old_quota = root / "usage_monitor_quota_history.jsonl"
            old_quota.write_text('{"checkedAt":"2026-01-01T00:00:00Z","windows":{"5h":{"used_percent":4,"usedPercent":4}}}\n', encoding="utf-8")
            history = monitor_history.default_history_path(root)

            completed = monitor_auto_update.migrate_history_data(
                root, history, monitor_history.default_quota_history_path(history), root / "usage_monitor_token_sessions.jsonl",
                monitor_token_ledger.default_token_ledger_path(history), monitor_history.default_sample_log_path(history), target_version=4,
            )

            self.assertEqual(completed, [1, 2, 3, 4])
            self.assertFalse(old_quota.exists())
            migrated = json.loads((root / "usage_monitor_quota_readings.jsonl").read_text(encoding="utf-8"))
            self.assertEqual(migrated["accountSlotId"], "unknown")
            self.assertNotIn("used_percent", migrated["windows"]["5h"])

    def test_v4_failure_restores_every_old_and_new_file_for_retry(self):
        with workspace_directory() as root:
            state = root / monitor_auto_update.DATA_MIGRATION_STATE_FILENAME
            state.write_text('{"dataContractVersion":3}\n', encoding="utf-8")
            old_quota, new_quota = root / "usage_monitor_quota_history.jsonl", root / "usage_monitor_quota_readings.jsonl"
            old_token = root / "usage_monitor_token_ledger.jsonl"
            old_quota.write_text('{"old":true}\n', encoding="utf-8")
            new_quota.write_text('{"new":true}\n', encoding="utf-8")
            old_token.write_text('not-json\n', encoding="utf-8")
            originals = {path: path.read_bytes() for path in (state, old_quota, new_quota, old_token)}
            history = monitor_history.default_history_path(root)

            with self.assertRaises(json.JSONDecodeError):
                monitor_auto_update.migrate_history_data(
                    root, history, monitor_history.default_quota_history_path(history), root / "usage_monitor_token_sessions.jsonl",
                    monitor_token_ledger.default_token_ledger_path(history), monitor_history.default_sample_log_path(history), target_version=4,
                )

            self.assertEqual({path: path.read_bytes() for path in originals}, originals)
            self.assertFalse((root / "usage_monitor_token_events.jsonl").exists())
            self.assertFalse((root / monitor_auto_update.DATA_MIGRATION_JOURNAL_FILENAME).exists())

    def test_interrupted_old_contract_recovers_legacy_path_before_continuing(self):
        with workspace_directory() as root:
            history = monitor_history.default_history_path(root)
            quota = monitor_history.default_quota_history_path(history)
            ledger = monitor_token_ledger.default_token_ledger_path(history)
            samples = monitor_history.default_sample_log_path(history)
            old_quota = root / "usage_monitor_quota_history.jsonl"
            backup = root / ".usage_monitor_quota_history.jsonl.migration-backup"
            backup.write_text('{"checkedAt":"2026-01-01T00:00:00Z","windows":{"5h":{"usedPercent":1}}}\n', encoding="utf-8")
            old_quota.write_text('{"broken":', encoding="utf-8")
            targets = {
                "history": history, "quota_history": old_quota, "token_session_history": root / "usage_monitor_token_sessions.jsonl", "token_ledger": ledger,
                "sample_log": samples, "runtime_state": root / "usage_monitor_state.json", "dashboard_cache": root / "usage_monitor_dashboard_cache.json",
                "usage_sync_cache": root / "usage_monitor_sync_cache.json", "cloud_state": root / "cloud-state.json",
            }
            (root / monitor_auto_update.DATA_MIGRATION_JOURNAL_FILENAME).write_text(json.dumps({
                "version": 1, "existing": ["quota_history"], "backups": {"quota_history": str(backup.resolve())},
                "targets": {name: str(path.resolve()) for name, path in targets.items()},
            }), encoding="utf-8")

            completed = monitor_auto_update.migrate_history_data(root, history, quota, root / "usage_monitor_token_sessions.jsonl", ledger, samples, target_version=4)

            self.assertEqual(completed, [1, 2, 3, 4])
            self.assertFalse(old_quota.exists())
            self.assertEqual(json.loads(quota.read_text(encoding="utf-8"))["accountSlotId"], "unknown")

    def test_v4_leaves_custom_paths_and_legacy_defaults_independent(self):
        with workspace_directory() as root:
            (root / monitor_auto_update.DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":3}\n', encoding="utf-8")
            legacy = root / "usage_monitor_quota_history.jsonl"
            custom = root / "custom-quota.jsonl"
            legacy.write_text('{"legacy":true}\n', encoding="utf-8")
            custom.write_text('{"custom":true}\n', encoding="utf-8")

            monitor_auto_update.migrate_history_data(root, root / "custom-history.jsonl", custom, root / "custom-sessions.jsonl", root / "custom-token.jsonl", root / "custom-samples.jsonl", target_version=4)

            self.assertEqual(legacy.read_text(encoding="utf-8"), '{"legacy":true}\n')
            self.assertEqual(custom.read_text(encoding="utf-8"), '{"custom":true}\n')


if __name__ == "__main__":
    unittest.main()
