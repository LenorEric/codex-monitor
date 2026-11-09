import json
import sqlite3
import shutil
import threading
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from unittest import mock

from monitor_canonical import CanonicalDataStore
from monitor_canonical_store import CanonicalRepository as records
from monitor_delivery import DashboardDelivery
from monitor_migration_zero_quota import migrate_local
import monitor_migrations as migrations
import test_sqlite_authority as authority


class ZeroQuotaMergeTests(unittest.TestCase):
    def setUp(self):
        (Path.cwd() / "tmp").mkdir(exist_ok=True)
        self.root = Path.cwd() / "tmp" / ("codex-zero-quota-" + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.store = CanonicalDataStore(self.root / "quota.jsonl", self.root / "ledger.jsonl", "test", lambda _: "v3:test", threading.RLock())
        self.addCleanup(self.store.index.close)
        from test_support_delivery import released_delivery
        released_delivery(self.store)
        self.paths = {"processing_index": self.store.index.path}

    @staticmethod
    def row(first, reset, last=None, value=0, label="5h", plan="plus", multiplier=1):
        return {"checkedAt": first, "quotaRunVersion": 1, "accountSlotId": "slot", "accountLabel": "Test",
            "sync": {"version": 1, "originMachineId": "test", "accountId": "v3:test", "recordId": f"{label}:{first}"},
            "windows": {label: {"usedPercent": value, "multiplier": multiplier, "plan": plan, "resetAt": reset,
                "firstAt": first, "lastAt": first if last is None else last}}}

    def merge(self, row, dataset="local"):
        with self.store.index.transaction() as db:
            self.store._quota_merge(db, dataset, row)

    def windows(self, dataset="local", label="5h"):
        with self.store.index.transaction() as db:
            return [row["windows"][label] for row in records.rows(db, dataset, "quota", condition="window=?", parameters=(label,))]

    def test_append_prepend_bridge_and_covered_old_point(self):
        for dataset in ("local", "merged"):
            for label in ("5h", "7d"):
                self.merge(self.row(100, 1000, label=label), dataset)
                self.merge(self.row(200, 900, label=label), dataset)
                self.assertEqual([(w["firstAt"], w["lastAt"], w["resetAt"]) for w in self.windows(dataset, label)], [(100, 200, 900)])
                self.merge(self.row(50, 5000, label=label), dataset)
                self.merge(self.row(150, 6000, label=label), dataset)
                self.assertEqual([(w["firstAt"], w["lastAt"], w["resetAt"]) for w in self.windows(dataset, label)], [(50, 200, 900)])
                # Direct inserts model separate historical neighbors before a bridging import.
                with self.store.index.transaction() as db:
                    records.write(db, dataset, "quota", self.row(400, 800, label=label))
                self.merge(self.row(300, 7000, label=label), dataset)
                self.assertEqual([(w["firstAt"], w["lastAt"], w["resetAt"]) for w in self.windows(dataset, label)], [(50, 400, 800)])

    def test_nonzero_and_state_changes_remain_barriers(self):
        for field, value in (("usedPercent", 1), ("plan", "pro"), ("multiplier", 5)):
            with self.subTest(field=field), self.store.index.transaction() as db:
                db.execute("DELETE FROM canonical_quota")
                self.store._quota_merge(db, "local", self.row(100, 1000))
                middle = self.row(200, 2000)
                middle["windows"]["5h"][field] = value
                self.store._quota_merge(db, "local", middle)
                self.store._quota_merge(db, "local", self.row(300, 3000))
                self.assertEqual(len(records.rows(db, "local", "quota")), 3)
        with self.store.index.transaction() as db:
            db.execute("DELETE FROM canonical_quota")
        self.merge(self.row(100, 1000, value=5))
        self.merge(self.row(200, 1001, value=5))
        self.assertEqual(len(self.windows()), 2)

    def seed_history(self):
        self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=2)])
        with self.store.index.transaction() as db:
            for dataset in ("local", "merged"):
                for label in ("5h", "7d"):
                    for first, last, reset in ((100, 110, 1000), (200, 210, 3000), (300, 320, 900)):
                        records.write(db, dataset, "quota", self.row(first, reset, last, label=label))
            db.execute("INSERT INTO upload_buffer(record_key,arrived,data) VALUES('pending',1,'{}')")
            db.execute("INSERT INTO package_receipts VALUES('dest','peer','package')")
            db.execute("INSERT INTO source_checkpoints VALUES('file','{}')")
            self.store.index.put(db, "uploadAttempt", {"package": "uncertain"})
        DashboardDelivery(self.store.index).prepare(force=True)

    def test_migration_latest_reset_preserves_other_state_and_refreshes_delivery(self):
        self.seed_history()
        with self.store.index.transaction() as db:
            tables = [name for name, in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'delivery_%' AND name!='canonical_quota'")]
            before = {table: db.execute(f'SELECT * FROM "{table}"').fetchall() for table in tables}
        migrate_local(self.paths)
        for dataset in ("local", "merged"):
            for label in ("5h", "7d"):
                self.assertEqual([(w["firstAt"], w["lastAt"], w["resetAt"]) for w in self.windows(dataset, label)], [(100, 320, 900)])
        with self.store.index.transaction() as db:
            for table in tables:
                self.assertEqual(db.execute(f'SELECT * FROM "{table}"').fetchall(), before[table], table)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM delivery_dirty").fetchone()[0], 2)
            saved = db.execute("SELECT * FROM canonical_quota ORDER BY dataset,window").fetchall()
        migrate_local(self.paths)
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT * FROM canonical_quota ORDER BY dataset,window").fetchall(), saved)
        delivery = DashboardDelivery(self.store.index)
        delivery.prepare(force=True)
        packet = json.loads(delivery.bulk(False))
        self.assertEqual(packet["accounts"][0]["local"], packet["accounts"][0]["merged"])
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM delivery_run_refs").fetchone()[0], 4)

    def test_migration_failure_rolls_back_and_retries(self):
        self.seed_history()
        with self.store.index.transaction() as db:
            before = db.execute("SELECT * FROM canonical_quota").fetchall()
        original = records.write
        calls = []
        def fail(*args, **kwargs):
            original(*args, **kwargs)
            calls.append(True)
            if len(calls) == 2:
                raise RuntimeError("interrupted")
        with mock.patch.object(records, "write", side_effect=fail), self.assertRaisesRegex(RuntimeError, "interrupted"):
            migrate_local(self.paths)
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT * FROM canonical_quota").fetchall(), before)
        migrate_local(self.paths)
        self.assertEqual(len(self.windows()), 1)

    def test_contract_marker_crash_recovers_and_retries(self):
        self.seed_history()
        state, journal = self.root / "contract.json", self.root / "journal.json"
        state.write_text('{"dataContractVersion":24}', encoding="utf-8")
        atomic_write = migrations._atomic_write
        def crash(path, data):
            if path == state:
                raise SystemExit("crash")
            atomic_write(path, data)
        with mock.patch.object(migrations, "_atomic_write", side_effect=crash), self.assertRaises(SystemExit):
            migrations._run_data_migration(migrate_local, self.paths, 25, state, journal)
        migrations._recover_data_migration(journal, state, self.paths | {"config": self.root / "config.json"})
        self.assertEqual(len(self.windows()), 3)
        migrations._run_data_migration(migrate_local, self.paths, 25, state, journal)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 25)
        self.assertEqual(len(self.windows()), 1)

    def test_empty_and_missing_database(self):
        migrate_local({"processing_index": self.root / "missing.sqlite3"})
        migrate_local(self.paths)
        self.assertEqual(self.windows(), [])

    def test_historical_compaction_keeps_barriers_and_newest_reset_representation(self):
        with self.store.index.transaction() as db:
            for first, reset, value, plan, multiplier in ((100, 5000, 0, "plus", 1), (200, "2030-01-01T00:00:00Z", 0, "plus", 1),
                    (300, 6000, 1, "plus", 1), (400, 7000, 0, "plus", 1), (500, 8000, 0, "pro", 1),
                    (600, 9000, 0, "pro", 5), (700, None, 0, "pro", 5)):
                records.write(db, "local", "quota", self.row(first, reset, value=value, plan=plan, multiplier=multiplier))
        migrate_local(self.paths)
        self.assertEqual([(w["firstAt"], w["lastAt"], w["resetAt"]) for w in self.windows()],
            [(100, 200, "2030-01-01T00:00:00Z"), (300, 300, 6000), (400, 400, 7000), (500, 500, 8000), (600, 700, None)])

    def test_normal_startup_runs_contract25_once(self):
        self.seed_history()
        (self.root / migrations.DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":24}', encoding="utf-8")
        args = (self.root, self.root / "history", self.store.quota_path, self.root / "sessions", self.store.token_ledger_path, self.root / "diagnostics")
        self.assertEqual(migrations.migrate_history_data(*args, usage_sync_cache_path=self.store.cache_path), [25, 26])
        self.assertEqual(migrations.migrate_history_data(*args, usage_sync_cache_path=self.store.cache_path), [])
        self.assertEqual(len(self.windows()), 1)
        self.assertEqual(self.store.index.ledger_cost()[0]["totalCostUsd"], 2)

    def test_ingestion_and_late_cloud_import_keep_latest_reset(self):
        from monitor_usage_sync import add_record_provenance
        raw = {"checkedAt": "2030-01-01T10:00:00Z", "accountSlotId": "slot", "accountLabel": "Test",
            "windows": {"5h": {"usedPercent": 0, "resetAt": 1893500000, "plan": "plus"}}}
        self.store.record_quota(raw)
        self.store.record_quota(raw | {"checkedAt": "2030-01-01T11:00:00Z",
            "windows": {"5h": raw["windows"]["5h"] | {"resetAt": 1893499999}}})
        self.assertEqual(self.windows(), self.windows("merged"))
        self.assertEqual(len(self.windows()), 1)
        before = self.windows()
        older = add_record_provenance("quota", raw | {"checkedAt": "2030-01-01T09:00:00Z"}, "peer", "v3:test")
        self.store.import_package("dest", "peer", "older", [{"kind": "quota", "row": older}])
        self.assertEqual(self.windows(), before)
        self.assertEqual(self.windows("merged")[0]["resetAt"], 1893499999)
        self.assertLess(self.windows("merged")[0]["firstAt"], before[0]["firstAt"])


if __name__ == "__main__":
    unittest.main()
