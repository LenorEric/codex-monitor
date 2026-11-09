import copy
import json
import shutil
import sqlite3
import threading
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from unittest import mock

from monitor_canonical import CanonicalDataStore
from monitor_canonical_store import CanonicalRepository
from monitor_delivery import DashboardDelivery
from monitor_migration_canonical import remove_seen
from monitor_usage_sync import add_record_provenance
import test_canonical_pipeline as pipeline
import test_sqlite_authority as authority


class CanonicalIdentityTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / "tmp" / ("canonical-identity-" + uuid.uuid4().hex)
        self.root.mkdir(parents=True)
        self.addCleanup(shutil.rmtree, self.root)
        self.open_store()

    def open_store(self):
        self.store = CanonicalDataStore(self.root / "quota.jsonl", self.root / "tokens.jsonl", "a", lambda _: "v3:account", threading.RLock())
        self.addCleanup(self.store.index.close)

    def cloud_quota(self, package, at, value):
        row = add_record_provenance("quota", pipeline.CanonicalPipelineTests.quota(at, value), "peer", "v3:account")
        self.store.import_package("destination", "peer", package, [{"kind": "quota", "row": row}])

    def test_quota_ids_are_not_stored_and_repeated_points_and_ranges_are_ignored(self):
        for at in ("2030-01-01T10:00:00Z", "2030-01-01T10:01:00Z", "2030-01-01T10:02:00Z"):
            self.store.record_quota(pipeline.CanonicalPipelineTests.quota(at, seven=10))
        before = {view: self.store.datasets(view) for view in ("local", "merged")}
        revision = self.store.index.revision()
        for at in ("2030-01-01T10:00:00Z", "2030-01-01T10:01:00Z", "2030-01-01T10:02:00Z"):
            self.store.record_quota(pipeline.CanonicalPipelineTests.quota(at, seven=10))
            self.cloud_quota(at, at, 50)
        for view in before:
            self.assertEqual(self.store.datasets(view), before[view])
        self.assertEqual(self.store.index.revision(), revision)
        with self.store.index.transaction() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_seen'").fetchone())

    def test_conflicting_points_inside_range_are_discarded(self):
        for minute in range(3):
            self.store.record_quota(pipeline.CanonicalPipelineTests.quota(f"2030-01-01T10:0{minute}:00Z"))
        local = self.store.datasets("local")
        self.cloud_quota("new-value", "2030-01-01T10:01:00Z", 51)
        merged = self.store.datasets("merged")
        self.assertEqual([row["windows"]["5h"]["usedPercent"] for row in merged[0]], [50])
        self.cloud_quota("duplicate", "2030-01-01T10:01:00Z", 51)
        self.cloud_quota("point-conflict", "2030-01-01T10:01:00Z", 52)
        self.assertEqual(self.store.datasets("merged"), merged)
        self.assertEqual(self.store.datasets("local"), local)

    def test_cloud_points_extend_nearest_predecessor_or_successor(self):
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota())
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota("2030-01-01T11:00:00Z", 60))
        local = self.store.datasets("local")
        self.cloud_quota("successor", "2030-01-01T10:30:00Z", 60)
        self.cloud_quota("predecessor", "2030-01-01T10:10:00Z", 50)
        self.cloud_quota("before-first", "2030-01-01T09:00:00Z", 50)
        self.cloud_quota("after-last", "2030-01-01T12:00:00Z", 60)
        self.cloud_quota("different", "2030-01-01T10:20:00Z", 55)
        self.cloud_quota("barrier", "2030-01-01T10:25:00Z", 50)
        windows = [row["windows"]["5h"] for row in self.store.datasets("merged")[0]]
        self.assertEqual([(w["firstAt"], w["lastAt"], w["usedPercent"]) for w in windows], [
            (pipeline.moment("2030-01-01T09:00:00Z"), pipeline.moment("2030-01-01T10:10:00Z"), 50),
            (pipeline.moment("2030-01-01T10:20:00Z"), pipeline.moment("2030-01-01T10:20:00Z"), 55),
            (pipeline.moment("2030-01-01T10:25:00Z"), pipeline.moment("2030-01-01T10:25:00Z"), 50),
            (pipeline.moment("2030-01-01T10:30:00Z"), pipeline.moment("2030-01-01T12:00:00Z"), 60)])
        self.assertEqual(self.store.datasets("local"), local)
        revision = self.store.index.revision()
        for at in ("2030-01-01T09:00:00Z", "2030-01-01T09:30:00Z", "2030-01-01T10:10:00Z", "2030-01-01T10:20:00Z"):
            self.cloud_quota(at, at, 99)
        self.assertEqual(self.store.index.revision(), revision)

    def test_successor_with_different_reset_or_plan_remains_separate(self):
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota())
        for index, changes in enumerate(({"resetAt": 1893500001}, {"plan": "pro"})):
            with self.subTest(changes=changes):
                row = pipeline.CanonicalPipelineTests.quota(f"2030-01-01T09:0{index}:00Z")
                row["windows"]["5h"].update(changes)
                self.store.import_package("destination", "peer", str(index), [{"kind": "quota", "row": add_record_provenance("quota", row, "peer", "v3:account")}])
                self.assertEqual(len(self.store.datasets("merged")[0]), index + 2)

    def test_repeated_pending_point_cannot_confirm_itself(self):
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota(value=50))
        pending = pipeline.CanonicalPipelineTests.quota("2030-01-01T10:01:00Z", 40)
        self.store.record_quota(pending)
        self.store.record_quota(pending)
        for view in ("local", "merged"):
            self.assertEqual([row["windows"]["5h"]["usedPercent"] for row in self.store.datasets(view)[0]], [50])
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 1)
        self.store.record_quota(pipeline.CanonicalPipelineTests.quota("2030-01-01T10:02:00Z", 41))
        for view in ("local", "merged"):
            self.assertEqual([row["windows"]["5h"]["usedPercent"] for row in self.store.datasets(view)[0]], [50, 40, 41])

    def test_cost_identity_deduplication_survives_restart_and_cross_machine_import(self):
        row = authority.SQLiteAuthorityTests.ledger(cost=2)
        self.store.record_ledger([row])
        self.store.index.close()
        self.open_store()
        conflicting = copy.deepcopy(row)
        conflicting["cost"]["totalCostUsd"] = 99
        self.assertFalse(self.store.record_ledger([conflicting]))
        remote = add_record_provenance("tokenLedger", conflicting, "peer", "v3:account")
        self.store.import_package("destination", "peer", "cost-duplicate", [{"kind": "tokenLedger", "row": remote}])
        for view in ("local", "merged"):
            self.assertEqual(len(self.store.datasets(view)[1]), 1)
            self.assertEqual(self.store.datasets(view)[1][0]["cost"]["totalCostUsd"], 2)
        with self.store.index.transaction() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_seen'").fetchone())
            self.assertEqual(db.execute("SELECT COUNT(*) FROM canonical_tokens").fetchone()[0], 2)

    def test_duplicate_insert_does_not_update_caches_buffer_or_delivery(self):
        row = authority.SQLiteAuthorityTests.ledger(cost=2)
        self.store.record_ledger([row])
        self.store.index.ledger_cost()
        with self.store.index.transaction() as db:
            self.store.index.usage_totals(db)
        DashboardDelivery(self.store.index).prepare()
        with self.store.index.transaction() as db:
            before = {table: db.execute(f"SELECT * FROM {table}").fetchall() for table in
                ("canonical_tokens", "processing_meta", "upload_buffer", "delivery_pending", "delivery_items", "delivery_journal", "delivery_meta")}
        conflicting = copy.deepcopy(row)
        conflicting["cost"]["totalCostUsd"] = 99
        conflicting["tokens"]["inputTokens"] += 1000
        self.assertFalse(self.store.record_ledger([conflicting]))
        with self.store.index.transaction() as db:
            for table, values in before.items():
                self.assertEqual(db.execute(f"SELECT * FROM {table}").fetchall(), values, table)

    def test_independent_connections_commit_duplicate_only_once(self):
        other = CanonicalDataStore(self.root / "quota.jsonl", self.root / "tokens.jsonl", "a", lambda _: "v3:account", threading.RLock())
        self.addCleanup(other.index.close)
        barrier, results, errors = threading.Barrier(2), [], []
        def insert(store):
            try:
                barrier.wait(5)
                results.append(store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=2)]))
            except BaseException as exc:
                errors.append(exc)
        threads = [threading.Thread(target=insert, args=(store,)) for store in (self.store, other)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(15)
            self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(sorted(results), [False, True])
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 1)
        self.assertEqual(self.store.index.ledger_cost()[0]["totalCostUsd"], 2)

    def test_same_event_in_distinct_accounts_remains_distinct(self):
        row = authority.SQLiteAuthorityTests.ledger(cost=2)
        self.store.record_ledger([row])
        self.store.account_id_resolver = lambda _: "v3:other"
        self.assertTrue(self.store.record_ledger([row]))
        for view in ("local", "merged"):
            self.assertEqual(len(self.store.datasets(view)[1]), 2)

    def legacy_seen(self):
        with self.store.index.transaction() as db:
            db.execute("CREATE TABLE canonical_seen(view TEXT NOT NULL,record_key TEXT NOT NULL,PRIMARY KEY(view,record_key)) WITHOUT ROWID")
            db.execute("INSERT INTO canonical_seen SELECT dataset,record_key FROM canonical_tokens")

    def test_migration_removes_only_redundant_table_and_retries_safely(self):
        self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=2)])
        self.legacy_seen()
        DashboardDelivery(self.store.index).prepare()
        with self.store.index.transaction() as db:
            before = {table: db.execute(f'SELECT * FROM "{table}"').fetchall() for table, in
                db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name!='canonical_seen'").fetchall()}
        for _ in range(2):
            remove_seen({"processing_index": self.store.index.path})
        with self.store.index.transaction() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_seen'").fetchone())
            for table, values in before.items():
                self.assertEqual(db.execute(f'SELECT * FROM "{table}"').fetchall(), values, table)
        self.store.index.close()
        self.open_store()
        self.assertFalse(self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=99)]))

    def test_migration_preserves_missing_token_identity_on_failure(self):
        self.legacy_seen()
        with self.store.index.transaction() as db:
            db.execute("INSERT INTO canonical_seen VALUES('local','missing')")
        with self.assertRaisesRegex(ValueError, "no permanent token record"):
            remove_seen({"processing_index": self.store.index.path})
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT * FROM canonical_seen").fetchall(), [("local", "missing")])

    def test_migration_marker_interruption_recovers_and_retries(self):
        import monitor_migrations as migrations
        self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=2)])
        self.legacy_seen()
        self.store.index.close()
        state, journal = self.root / "contract.json", self.root / "journal.json"
        state.write_text('{"dataContractVersion":25}', encoding="utf-8")
        write = migrations._atomic_write
        def crash(target, data):
            if target == state:
                raise SystemExit("before marker")
            write(target, data)
        paths = {"processing_index": self.store.index.path, "config": self.root / "config.json"}
        with mock.patch.object(migrations, "_atomic_write", side_effect=crash), self.assertRaises(SystemExit):
            migrations._run_data_migration(remove_seen, paths, 26, state, journal)
        migrations._recover_data_migration(journal, state, paths)
        with closing(sqlite3.connect(self.store.index.path)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM canonical_seen").fetchone()[0], 2)
        migrations._run_data_migration(remove_seen, paths, 26, state, journal)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 26)
        self.assertFalse(journal.exists())
        self.open_store()
        self.assertFalse(self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=99)]))

    def test_insert_only_does_not_suppress_other_constraint_failures(self):
        row = self.store.normalize_new_ledger(authority.SQLiteAuthorityTests.ledger(cost=2))
        row["serviceTier"] = "invalid"
        with self.store.index.transaction() as db, self.assertRaises(sqlite3.IntegrityError):
            CanonicalRepository.write(db, "local", "tokenLedger", row, insert_only=True)

    def test_startup_contract25_upgrade_preserves_prepared_snapshot(self):
        import monitor_migrations as migrations
        self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=2)])
        self.legacy_seen()
        snapshot = DashboardDelivery(self.store.index)
        snapshot.prepare()
        before = snapshot.bulk(False)
        (self.root / migrations.DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":25}', encoding="utf-8")
        self.store.index.close()
        args = (self.root, self.root / "history", self.store.quota_path, self.root / "sessions", self.store.token_ledger_path, self.root / "diagnostics")
        self.assertEqual(migrations.migrate_history_data(*args, usage_sync_cache_path=self.store.cache_path), [26])
        self.assertEqual(migrations.migrate_history_data(*args, usage_sync_cache_path=self.store.cache_path), [])
        self.open_store()
        self.assertEqual(DashboardDelivery(self.store.index).bulk(False), before)
        self.assertFalse(self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=99)]))


if __name__ == "__main__":
    unittest.main()
