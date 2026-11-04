import json
import shutil
import threading
import unittest
import uuid
from pathlib import Path
from unittest import mock

import monitor_backup
import monitor_migrations
import monitor_sampling
from monitor_usage_sync import UsageDataStore


class SQLiteAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-sqlite-authority-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.store = self.open_store()

    def open_store(self):
        return UsageDataStore(self.root / "quota.jsonl", self.root / "ledger.jsonl", "machine", lambda _: "v3:a", threading.RLock())

    @staticmethod
    def quota(minute=0, value=10):
        return {"checkedAt": f"2030-01-01T00:{minute:02}:00Z", "accountSlotId": "slot", "accountLabel": "A", "windows": {"5h": {"usedPercent": value, "plan": "plus"}}}

    @staticmethod
    def ledger(event="event", cost=1):
        from monitor_common import empty_token_totals, empty_cost_totals
        return {"schemaVersion": 2, "recordType": "usage", "eventId": event, "sessionId": "session", "occurredAt": "2030-01-01T00:00:00Z",
            "rawModel": "gpt-5", "billingModel": "gpt-5", "serviceTier": "default", "accountSlotId": "slot", "accountLabel": "A",
            "tokens": empty_token_totals(), "cost": empty_cost_totals() | {"totalCostUsd": cost}}

    def test_normal_processing_restart_rename_and_repair_never_read_recorder_files(self):
        original_open = Path.open
        paths = {self.store.quota_path, self.store.token_ledger_path}
        def guarded(path, mode="r", *args, **kwargs):
            if path in paths and ("r" in mode or "+" in mode):
                raise AssertionError(f"Recorder read: {path}")
            return original_open(path, mode, *args, **kwargs)
        with mock.patch.object(Path, "open", guarded):
            self.store.record_quota(self.quota())
            self.store.record_ledger([self.ledger()])
            expected = self.store.datasets()
            self.store.snapshot()
            self.store.v4_local_snapshot(1893456600)
            self.store.rename_account("slot", "Renamed")
            self.store.index.repair()
            restarted = self.open_store()
            restarted.initialize_processing()
            restarted.initialize_v4_index(1893456600)
            self.assertEqual(restarted.datasets()[0][0]["accountLabel"], "Renamed")
            self.assertEqual(restarted.datasets()[1][0]["cost"], expected[1][0]["cost"])
            self.assertEqual(restarted.index.metrics["canonicalBytesRead"], 0)

    def test_log_deletion_corruption_and_arbitrary_edits_do_not_change_database(self):
        self.store.record_quota(self.quota())
        self.store.record_ledger([self.ledger()])
        expected = self.store.datasets()
        self.store.quota_path.write_bytes(b"invalid backup\n")
        self.store.token_ledger_path.unlink()
        restarted = self.open_store()
        self.assertEqual(restarted.datasets(), expected)
        restarted.record_quota(self.quota(1, 11))
        restarted.record_ledger([self.ledger("event-2")])
        self.assertEqual(len(restarted.datasets()[0]), 2)
        self.assertEqual(len(restarted.datasets()[1]), 2)
        self.assertEqual([json.loads(line) for line in self.store.quota_path.read_text().splitlines()], restarted.datasets()[0])

    def test_failed_database_commit_never_appends_backup_or_changes_sampling(self):
        self.store.record_quota(self.quota())
        before = self.store.quota_path.read_bytes()
        with mock.patch.object(monitor_sampling, "refresh", side_effect=RuntimeError("commit failure")), self.assertRaisesRegex(RuntimeError, "commit failure"):
            self.store.record_quota(self.quota(1, 20))
        self.assertEqual(self.store.quota_path.read_bytes(), before)
        self.assertEqual(len(self.store.datasets()[0]), 1)
        self.store.record_quota(self.quota(1, 20))
        self.assertEqual(len(self.store.datasets()[0]), 2)

    def test_log_failure_keeps_database_live_and_retries_from_queue_after_restart(self):
        with mock.patch.object(monitor_backup, "append_backup", side_effect=OSError("read-only archive")), mock.patch.object(monitor_backup, "replace_quota_backup", side_effect=OSError("read-only archive")):
            self.store.record_quota(self.quota())
            self.store.record_ledger([self.ledger()])
            self.assertEqual(tuple(map(len, self.store.datasets())), (1, 1))
            with self.store.index.transaction() as db:
                self.assertEqual(db.execute("SELECT count(*) FROM processing_backup_queue").fetchone()[0], 2)
        restarted = self.open_store()
        restarted.backup.flush()
        self.assertEqual(len([line for line in restarted.quota_path.read_bytes().splitlines() if line.strip()]), 1)
        self.assertEqual(len([line for line in restarted.token_ledger_path.read_bytes().splitlines() if line.strip()]), 1)
        with restarted.index.transaction() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM processing_backup_queue").fetchone()[0], 0)

    def test_crash_after_log_append_only_duplicates_backup_delivery(self):
        append = monitor_backup.append_backup
        def interrupted(path, rows):
            append(path, rows)
            raise RuntimeError("interrupted before backup acknowledgement")
        with mock.patch.object(monitor_backup, "append_backup", side_effect=interrupted), self.assertRaises(RuntimeError):
            self.store.record_ledger([self.ledger()])
        restarted = self.open_store()
        self.assertEqual(len(restarted.datasets()[1]), 1)
        restarted.backup.flush()
        lines = [line for line in restarted.token_ledger_path.read_bytes().splitlines() if line.strip()]
        self.assertEqual(len(lines), 2)
        self.assertTrue(all(json.loads(line) == restarted.datasets()[1][0] for line in lines))

    def test_one_unavailable_backup_does_not_block_other_backup_files(self):
        append = monitor_backup.append_backup
        def unavailable(path, rows):
            if Path(path) == self.store.quota_path:
                raise OSError("quota log unavailable")
            append(path, rows)
        with mock.patch.object(monitor_backup, "append_backup", side_effect=unavailable), mock.patch.object(monitor_backup, "replace_quota_backup", side_effect=OSError("quota snapshot unavailable")):
            self.store.record_quota(self.quota())
            self.store.record_ledger([self.ledger()])
            self.assertEqual([json.loads(line) for line in self.store.token_ledger_path.read_bytes().splitlines() if line.strip()], self.store.datasets()[1])
        self.store.backup.flush()
        self.assertEqual(len([line for line in self.store.quota_path.read_bytes().splitlines() if line.strip()]), 1)

    def test_database_removals_do_not_append_operations_to_backup_logs(self):
        self.store.record_ledger([self.ledger(), self.ledger("second")])
        prefix = self.store.token_ledger_path.read_bytes()
        self.store.record_ledger([self.ledger(cost=2)], replace=True)
        self.assertTrue(self.store.token_ledger_path.read_bytes().startswith(prefix))
        rows = [json.loads(line) for line in self.store.token_ledger_path.read_bytes().splitlines() if line.strip()]
        self.assertEqual(len(rows), 3)
        self.assertEqual({row["eventId"]: row["cost"]["totalCostUsd"] for row in rows}, {"event": 2, "second": 1})
        self.assertTrue(all(row["recordType"] == "usage" for row in rows))
        self.assertEqual([row["eventId"] for row in self.store.datasets()[1]], ["event"])
        self.assertEqual(self.store.index.ledger_cost()[0]["totalCostUsd"], 2)
        before_removal = self.store.token_ledger_path.read_bytes()
        self.store.record_ledger([], replace=True)
        self.assertEqual(self.store.datasets()[1], [])
        self.assertEqual(self.store.token_ledger_path.read_bytes(), before_removal)
        self.assertEqual(self.store.index.ledger_cost()[0]["totalCostUsd"], 0)

    def test_quota_snapshot_replaces_torn_tail_from_authoritative_database(self):
        self.store.quota_path.write_bytes(b'{"torn":')
        self.store.record_quota(self.quota())
        lines = self.store.quota_path.read_bytes().splitlines()
        self.assertEqual([json.loads(line) for line in lines if line.strip()], self.store.datasets()[0])

    def test_repeated_quota_does_not_duplicate_snapshot_or_database_record(self):
        self.store.record_quota(self.quota())
        self.store.record_quota(self.quota())
        self.assertEqual(len(self.store.datasets()[0]), 1)
        self.assertEqual(len([line for line in self.store.quota_path.read_bytes().splitlines() if line.strip()]), 1)

    def test_dataset_reads_do_not_attempt_backup_delivery(self):
        self.store.record_quota(self.quota())
        with mock.patch.object(self.store.backup, "flush", side_effect=AssertionError("read attempted backup delivery")):
            self.store.datasets()
            self.store.snapshot()
            self.store.v4_local_snapshot(1893456600)

    def test_v19_finishes_released_ledger_batch_without_runtime_recovery_logic(self):
        row = self.store.normalize_new_ledger(self.ledger())
        with self.store.index.transaction() as db:
            db.execute("INSERT INTO processing_scan_events VALUES(?,?,?,?,?,?,?)", ("event", str(self.root), "session", 1893456000, "{}", 1, 0))
            self.store.index.put(db, f"ledgerBatch:{self.root}", {"ids": ["event"], "additions": [row], "replacements": {}, "coverage": [], "order": {}})
        paths = {"processing_index": self.store.index.path, "quota_history": self.store.quota_path, "token_ledger": self.store.token_ledger_path}
        monitor_migrations._migrate_history_to_v19(paths)
        self.assertEqual(self.open_store().datasets()[1], [row])
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT ledger_done FROM processing_scan_events").fetchone()[0], 1)
            self.assertIsNone(db.execute("SELECT 1 FROM processing_meta WHERE name LIKE 'ledgerBatch:%'").fetchone())
            self.assertEqual(db.execute("SELECT count(*) FROM processing_backup_queue").fetchone()[0], 1)

    def test_backup_output_paths_do_not_identify_database_records(self):
        self.store.record_quota(self.quota())
        self.store.record_ledger([self.ledger()])
        expected = self.store.datasets()
        relocated = UsageDataStore(self.root / "other-quota.jsonl", self.root / "other-ledger.jsonl", "machine", lambda _: "v3:a", threading.RLock(), cache_path=self.store.cache_path)
        self.assertEqual(relocated.datasets(), expected)
        relocated.record_quota(self.quota(1, 11))
        self.assertEqual(len(self.store.datasets()[0]), 2)
        self.assertEqual(len([line for line in relocated.quota_path.read_bytes().splitlines() if line.strip()]), 2)

    def test_v19_sql_conversion_is_journaled_and_ignores_additional_backup_records(self):
        self.store.record_quota(self.quota())
        self.store.record_ledger([self.ledger()])
        self.store.initialize_v4_index(1893456600)
        with self.store.index.transaction() as db:
            ownership = list(db.execute("SELECT record_key,period FROM local_records ORDER BY record_key"))
        # Extra backup records must not supplement the complete SQLite database.
        monitor_backup.append_backup(self.store.quota_path, [self.store.normalize_new_quota(self.quota(1, 11))])
        originals = self.store.quota_path.read_bytes(), self.store.token_ledger_path.read_bytes()
        paths = {"processing_index": self.store.index.path, "quota_history": self.store.quota_path, "token_ledger": self.store.token_ledger_path}
        state, journal = self.root / "contract.json", self.root / "journal.json"
        state.write_text('{"dataContractVersion":18}')
        def interrupted(paths):
            monitor_migrations._migrate_history_to_v19(paths)
            raise RuntimeError("migration failure")
        with self.assertRaises(RuntimeError):
            monitor_migrations._run_data_migration(interrupted, paths, 19, state, journal)
        self.assertEqual(len(self.store.datasets()[0]), 1)
        monitor_migrations._run_data_migration(monitor_migrations._migrate_history_to_v19, paths, 19, state, journal)
        self.assertEqual(tuple(map(len, self.open_store().datasets())), (1, 1))
        self.assertEqual((self.store.quota_path.read_bytes(), self.store.token_ledger_path.read_bytes()), originals)
        with self.store.index.transaction() as db:
            self.assertEqual(list(db.execute("SELECT record_key,period FROM local_records ORDER BY record_key")), ownership)
            self.assertTrue(self.store.index.get(db, "sqliteAuthoritative"))

    def test_v19_missing_archives_preserve_database_only_records(self):
        self.store.record_quota(self.quota())
        self.store.record_ledger([self.ledger()])
        expected = self.store.datasets()
        self.store.quota_path.unlink()
        self.store.token_ledger_path.unlink()
        monitor_migrations._migrate_history_to_v19({"processing_index": self.store.index.path, "quota_history": self.store.quota_path, "token_ledger": self.store.token_ledger_path})
        self.assertEqual(self.open_store().datasets(), expected)

    def test_v19_rekeys_released_path_sources_without_losing_database_only_history(self):
        rows = self.store.normalize_new_quota(self.quota()), self.store.normalize_new_ledger(self.ledger())
        with self.store.index.transaction() as db:
            self.store.index.fact(db, str(self.store.quota_path.resolve()), 8192, "quota", rows[0])
            self.store.index.fact(db, str(self.store.token_ledger_path.resolve()), 4096, "tokenLedger", rows[1])
        monitor_migrations._migrate_history_to_v19({"processing_index": self.store.index.path, "quota_history": self.store.quota_path, "token_ledger": self.store.token_ledger_path})
        self.assertEqual(self.open_store().datasets(), ([rows[0]], [rows[1]]))
        self.assertEqual(self.store.index.ledger_cost()[0]["totalCostUsd"], 1)
        with self.store.index.transaction() as db:
            self.assertEqual({row[0] for row in db.execute("SELECT DISTINCT source FROM processing_facts")}, {"local:quota", "local:tokenLedger"})

    def test_v19_uses_sqlite_state_and_never_opens_recorder_or_legacy_state_files(self):
        self.store.record_quota(self.quota())
        self.store.record_ledger([self.ledger()])
        with self.store.index.transaction() as db:
            self.store.index.put(db, "runtimeState", {"activeAccountSlotId": "database-slot"})
        state = self.root / "state.json"
        state.write_text('{"activeAccountSlotId":"stale-slot"}')
        self.store.quota_path.write_bytes(b"corrupt quota backup")
        self.store.token_ledger_path.write_bytes(b"corrupt token backup")
        originals = self.store.quota_path.read_bytes(), self.store.token_ledger_path.read_bytes()
        paths = {"processing_index": self.store.index.path, "quota_history": self.store.quota_path, "token_ledger": self.store.token_ledger_path, "runtime_state": state}
        original_open = Path.open
        def guarded(path, *args, **kwargs):
            if path in {state, self.store.quota_path, self.store.token_ledger_path}:
                raise AssertionError(f"Migration opened non-database data: {path}")
            return original_open(path, *args, **kwargs)
        with mock.patch.object(Path, "open", guarded):
            monitor_migrations._run_data_migration(monitor_migrations._migrate_history_to_v19, paths, 19, self.root / "contract.json", self.root / "journal.json")
        with self.store.index.transaction() as db:
            self.assertEqual(self.store.index.get(db, "runtimeState")["activeAccountSlotId"], "database-slot")
        self.assertEqual((self.store.quota_path.read_bytes(), self.store.token_ledger_path.read_bytes()), originals)
