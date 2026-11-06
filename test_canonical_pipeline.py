import json
import shutil
import threading
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from monitor_canonical import CanonicalDataStore
from monitor_cloud import CloudError, CloudManager, CryptoBox
from monitor_delivery import DashboardDelivery
from monitor_migration_packages import migrate_local
from monitor_migration_canonical import migrate_local as migrate_contract23
from monitor_migration_quota_boundary import migrate_local as migrate_contract24
from monitor_packages import fetch, push
from monitor_usage_sync import UsageDataStore, add_record_provenance
import test_sqlite_authority as authority
from test_support_webdav import MemoryWebDav, moment


def migrate_typed(paths):
    migrate_contract23(paths)
    migrate_contract24(paths)


class PackageWebDav(MemoryWebDav):
    def list(self, path):
        prefix = path.rstrip("/") + "/"
        return sorted({name[len(prefix):].split("/")[0] for name in self.files if name.startswith(prefix)})


class CanonicalPipelineTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / (".canonical-test-" + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.client = PackageWebDav()
        self.box = CryptoBox("", CryptoBox.descriptor(""))
        self.store = self.open_store()
        self.manager = CloudManager(self.root / "private", SimpleNamespace(), None)
        self.addCleanup(self.manager.operation_queue.close)
        self.manager._usage_data = self.store
        self.manager._machine["machineId"] = "a"
        self.manager._config["webdav"]["enabled"] = True


    def open_store(self, name="a"):
        store = CanonicalDataStore(self.root / name / "quota.jsonl", self.root / name / "token.jsonl", name, lambda _: "v3:account", threading.RLock())
        self.addCleanup(store.index.close)
        return store

    @staticmethod
    def quota(at="2030-01-01T10:00:00Z", value=50, seven=None):
        row = {"checkedAt": at, "accountSlotId": "slot", "accountLabel": "A", "windows": {"5h": {"usedPercent": value, "plan": "plus", "resetAt": 1893500000}}}
        if seven is not None:
            row["windows"]["7d"] = {"usedPercent": seven, "plan": "plus", "resetAt": 1894000000}
        return row

    def windows(self, view="local", label="5h"):
        return [row["windows"][label] for row in self.store.datasets(view)[0] if label in row["windows"]]

    def remote(self, row):
        return {"kind": "quota", "row": add_record_provenance("quota", row, "peer", "v3:account")}

    def test_shared_schema_and_identical_ui_preparation(self):
        self.store.record_quota(self.quota(seven=10))
        self.store.record_ledger([authority.SQLiteAuthorityTests.ledger()])
        self.assertEqual(self.store.datasets("local"), self.store.datasets("merged"))
        delivery = DashboardDelivery(self.store.index)
        delivery.prepare()
        account = json.loads(delivery.bulk(False))["accounts"][0]
        self.assertEqual(account["local"], account["merged"])
        self.assertFalse(self.store.quota_path.exists())
        self.assertNotIsInstance(self.store, UsageDataStore)
        for name in ("backup", "ingest_local", "initialize_processing", "remove_origins_not_in"):
            self.assertFalse(hasattr(self.store, name), name)
        for name in ("fact", "write_records", "v4_publish"):
            self.assertFalse(hasattr(self.store.index, name), name)
        for name in ("_v4_publish", "_v4_fetch", "_v4_publish_retry"):
            self.assertFalse(hasattr(self.manager, name), name)
        self.assertFalse(self.store.token_ledger_path.exists())
        with self.store.index.transaction() as db:
            names = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertFalse(names & {"processing_scan_events", "processing_backup_queue", "remote_record_refs", "local_record_refs"})

    def test_late_point_inside_range_preserves_both_datasets(self):
        self.store.record_quota(self.quota())
        self.store.record_quota(self.quota("2030-01-01T11:00:00Z"))
        before = self.store.datasets("local")
        self.store.import_package("dest", "peer", "package", [self.remote(self.quota("2030-01-01T10:30:00Z", 51))])
        self.assertEqual(self.store.datasets("local"), before)
        windows = self.windows("merged")
        self.assertEqual([(w["firstAt"], w["lastAt"], w["usedPercent"]) for w in windows], [
            (moment("2030-01-01T10:00:00Z"), moment("2030-01-01T11:00:00Z"), 50)])
        self.store.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=2)])
        delivery = DashboardDelivery(self.store.index)
        delivery.prepare()
        account = json.loads(delivery.bulk(False))["accounts"][0]
        self.assertEqual(sum(row[2][0][-1] for row in account["local"]["tokens"]), sum(row[2][0][-1] for row in account["merged"]["tokens"]))

    def test_independent_windows_partial_update_and_compaction(self):
        self.store.record_quota(self.quota())
        row = self.quota(seven=10)
        self.store.record_quota(row)
        self.store.record_quota(self.quota("2030-01-01T10:01:00Z", seven=10))
        self.assertEqual(len(self.windows()), 1)
        self.assertEqual(len(self.windows(label="7d")), 1)
        self.assertEqual(self.windows()[0]["lastAt"], moment("2030-01-01T10:01:00Z"))
        with self.store.index.transaction() as db:
            records = [json.loads(data) for data, in db.execute("SELECT data FROM upload_buffer ORDER BY seq")]
        self.assertEqual({label for record in records for label in record["row"]["windows"]}, {"5h", "7d"})

    def test_arrival_deadline_manual_flush_and_restart(self):
        with mock.patch("time.time", return_value=1000):
            self.store.record_quota(self.quota())
        self.manager._usage_data = self.open_store()
        with mock.patch("time.time", return_value=2799):
            self.assertEqual(push(self.manager, self.client, self.box, False)["packagesUploaded"], 0)
        with mock.patch("time.time", return_value=2800):
            self.assertEqual(push(self.manager, self.client, self.box, False)["packagesUploaded"], 1)
        self.assertEqual(self.manager._usage_data.buffer_status()["pendingRecords"], 0)
        self.store.record_quota(self.quota("2030-01-01T10:01:00Z", 51))
        self.assertEqual(push(self.manager, self.client, self.box)["packagesUploaded"], 1)

    def test_failed_upload_keeps_one_growing_buffer(self):
        self.store.record_quota(self.quota())
        with mock.patch.object(self.client, "put", side_effect=CloudError("offline", 502)):
            with self.assertRaises(CloudError):
                push(self.manager, self.client, self.box)
        self.store.record_quota(self.quota("2030-01-01T10:01:00Z", 51))
        self.assertEqual(push(self.manager, self.client, self.box)["packagesUploaded"], 1)
        descriptors = [json.loads(data) for path, (data, _) in self.client.files.items() if "/packages/" in path]
        self.assertEqual([row["recordCount"] for row in descriptors], [2])

    def test_concurrent_arrivals_are_not_acknowledged(self):
        self.store.record_quota(self.quota())
        original = self.client.put
        def put(path, *args, **kwargs):
            if "/data/" in path:
                self.store.record_quota(self.quota("2030-01-01T10:01:00Z", 51))
            return original(path, *args, **kwargs)
        with mock.patch.object(self.client, "put", side_effect=put):
            push(self.manager, self.client, self.box)
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 1)

    def test_remote_success_before_local_ack_is_recovered(self):
        self.store.record_quota(self.quota())
        with mock.patch("monitor_packages._acknowledge", side_effect=RuntimeError("crash")):
            with self.assertRaises(RuntimeError):
                push(self.manager, self.client, self.box)
        count = len(self.client.files)
        self.manager._usage_data = self.open_store()
        push(self.manager, self.client, self.box)
        self.assertEqual(len(self.client.files), count)
        self.assertEqual(self.manager._usage_data.buffer_status()["pendingRecords"], 0)

    def test_descriptor_timeout_preserves_attempt_identity(self):
        self.store.record_quota(self.quota())
        original = self.client.put
        def put(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            if "/packages/" in path:
                raise CloudError("lost response", 502)
            return result
        with mock.patch.object(self.client, "put", side_effect=put):
            with self.assertRaises(CloudError):
                push(self.manager, self.client, self.box)
        push(self.manager, self.client, self.box)
        self.assertEqual(len(self.client.files), 2)

    def test_fetch_only_unseen_packages_and_never_prunes_history(self):
        self.store.record_quota(self.quota())
        push(self.manager, self.client, self.box)
        self.manager._usage_data = self.open_store("reader")
        self.assertEqual(fetch(self.manager, self.client, self.box)["packagesDownloaded"], 1)
        with mock.patch("tempfile.TemporaryFile", side_effect=AssertionError("cloud disk spool")):
            self.assertEqual(fetch(self.manager, self.client, self.box)["packagesDownloaded"], 0)
        before = self.manager._usage_data.datasets("merged")
        self.client.files.clear()
        fetch(self.manager, self.client, self.box)
        self.assertEqual(self.manager._usage_data.datasets("merged"), before)

    def test_fetch_skips_own_directory_without_receipts_even_during_bootstrap(self):
        self.store.record_quota(self.quota())
        push(self.manager, self.client, self.box)
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM package_receipts").fetchone()[0], 1)
            db.execute("DELETE FROM package_receipts")
        for bootstrap in (False, True):
            with self.subTest(bootstrap=bootstrap):
                with self.store.index.transaction() as db:
                    self.store.index.put(db, "legacyPublicationPending", bootstrap)
                with mock.patch.object(self.manager, "_optional_remote_list", return_value=["a"]) as listing, mock.patch.object(self.client, "get", side_effect=AssertionError("own package downloaded")):
                    self.assertEqual(fetch(self.manager, self.client, self.box)["packagesDownloaded"], 0)
                listing.assert_called_once_with(self.client, "usage/v5/packages")

    def test_import_failure_rolls_back_receipt_and_data(self):
        rows = [self.remote(self.quota()), {"kind": "quota", "row": {"windows": {"5h": {}}}}]
        with self.assertRaises(Exception):
            self.store.import_package("dest", "peer", "bad", rows)
        self.assertEqual(self.store.datasets("merged"), ([], []))
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM package_receipts").fetchone()[0], 0)

    def test_small_pack_limit_drains_buffer_by_record_boundaries(self):
        for minute in range(4):
            self.store.record_quota(self.quota(f"2030-01-01T10:0{minute}:00Z", 50 + minute))
        with mock.patch("monitor_packages.PACK_RAW_BYTES", 1):
            self.assertEqual(push(self.manager, self.client, self.box)["packagesUploaded"], 4)
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 0)

    def test_local_migration_preserves_saved_costs_and_quotas(self):
        root = self.root / "legacy"
        old = UsageDataStore(root / "quota.jsonl", root / "token.jsonl", "a", lambda _: "v3:account", threading.RLock())
        old.backup.enabled = lambda: False
        old.record_quota(self.quota())
        old.record_quota(self.quota("2030-01-01T11:00:00Z"))
        old.record_ledger([authority.SQLiteAuthorityTests.ledger(cost=3)])
        old.initialize_v4_index()
        migrate_local({"processing_index": old.index.path})
        migrate_typed({"processing_index": old.index.path})
        new = self.open_store("legacy")
        self.assertEqual(new.index.ledger_cost()[0]["totalCostUsd"], 3)
        self.assertEqual(new.datasets("local"), new.datasets("merged"))
        self.assertEqual(len(new.datasets()[0]), 1)
        with new.index.transaction() as db:
            self.assertTrue(new.index.get(db, "legacyPublicationPending"))
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='remote_records'").fetchone())

    def test_session_append_replacement_duplicate_and_missing_source(self):
        import test_processing as fixtures
        log = self.root / "sessions" / "log.jsonl"
        log.parent.mkdir()
        header = [{"timestamp": "2030-01-01T00:00:00Z", "type": "session_meta", "payload": {"id": "session"}},
            {"timestamp": "2030-01-01T00:00:01Z", "type": "turn_context", "payload": {"model": "gpt-5.5"}}]
        def write(rows, mode="w"):
            with log.open(mode, encoding="utf-8") as output:
                output.write("".join(json.dumps(row) + "\n" for row in rows))
        def sync():
            self.store.sources.scan(self.root)
            return self.store.sources.sync_ledger(self.store.token_ledger_path, "slot", "A", [],
                lambda row: add_record_provenance("tokenLedger", row, "a", "v3:account"), self.store.normalize_new_ledger, "a")
        write(header + [fixtures.ProcessingTests.event(1, 100)])
        notifications = []
        self.store.sources.on_new_cost = notifications.append
        sync()
        write([fixtures.ProcessingTests.event(2, 150)], "a")
        sync()
        self.assertEqual([row["tokens"]["inputTokens"] for row in self.store.datasets()[1]], [100, 50])
        self.assertEqual(len(notifications), 2)
        write(header + [fixtures.ProcessingTests.event(1, 200)])
        sync()
        self.assertEqual(sum(row["tokens"]["inputTokens"] for row in self.store.datasets()[1]), 350)
        shutil.copyfile(log, log.with_name("duplicate.jsonl"))
        sync()
        log.unlink()
        sync()
        self.assertEqual(len(self.store.datasets()[1]), 3)
        with mock.patch.object(Path, "open", side_effect=AssertionError("idle history read")):
            sync()

    def test_checkpoint_and_upload_buffer_roll_back_with_processing(self):
        import test_processing as fixtures
        log = self.root / "sessions" / "log.jsonl"
        log.parent.mkdir()
        log.write_text(json.dumps({"type": "session_meta", "payload": {"id": "session"}}) + "\n" + json.dumps(fixtures.ProcessingTests.event(1, 100)) + "\n", encoding="utf-8")
        self.store.sources.scan(self.root)
        original = self.store._record_ledger
        def fail(db, rows):
            original(db, rows)
            raise RuntimeError("crash")
        args = (self.store.token_ledger_path, "slot", "A", [], lambda row: add_record_provenance("tokenLedger", row, "a", "v3:account"), self.store.normalize_new_ledger, "a")
        with mock.patch.object(self.store, "_record_ledger", side_effect=fail), self.assertRaises(RuntimeError):
            self.store.sources.sync_ledger(*args)
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM source_checkpoints").fetchone()[0], 0)
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 0)
        self.store.sources.sync_ledger(*args)
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 1)

    def test_new_window_during_upload_is_not_lost(self):
        self.store.record_quota(self.quota())
        original = self.client.put
        def put(path, *args, **kwargs):
            if "/data/" in path:
                self.store.record_quota(self.quota(seven=10))
            return original(path, *args, **kwargs)
        with mock.patch.object(self.client, "put", side_effect=put):
            push(self.manager, self.client, self.box)
        self.assertEqual(self.store.buffer_status()["pendingRecords"], 1)

    def test_raw_quota_response_logging_before_validation_and_io_failure(self):
        import monitor_quota
        from monitor_dashboard import UsageDashboardState
        state = UsageDashboardState.__new__(UsageDashboardState)
        state.args = SimpleNamespace(save_raw_quota_responses=lambda: True, raw_quota_log=self.root / "raw.jsonl")
        state._raw_quota_log_lock = threading.Lock()
        response = {"unexpected": "raw body"}
        with mock.patch.object(monitor_quota, "refresh_access_token", return_value="secret"), mock.patch.object(monitor_quota, "request_json", return_value=(200, response)):
            with self.assertRaises(Exception):
                monitor_quota.fetch_usage({}, object(), None, 1, {"_response_callback": state._record_raw_quota_response})
        saved = json.loads(state.args.raw_quota_log.read_text(encoding="utf-8"))
        self.assertEqual(saved["response"], response)
        self.assertEqual(set(saved), {"capturedAt", "accountId", "status", "response"})
        with mock.patch.object(Path, "open", side_effect=OSError("unwritable")):
            state._record_raw_quota_response(response, 200, "account")

    def test_first_sync_preserves_local_migration_candidates_when_skipping_own_directory(self):
        root = self.root / "legacy"
        old = UsageDataStore(root / "quota.jsonl", root / "token.jsonl", "a", lambda _: "v3:account", threading.RLock())
        old.backup.enabled = lambda: False
        old.record_quota(self.quota())
        old.record_quota(self.quota("2030-01-01T11:00:00Z"))
        old.initialize_v4_index(moment("2030-01-02T00:00:00Z"))
        published = self.open_store("published")
        published.machine_id = "a"
        published.record_quota(self.quota())
        published.record_quota(self.quota("2030-01-01T11:00:00Z"))
        self.manager._usage_data = published
        push(self.manager, self.client, self.box)
        old.record_quota(self.quota("2030-01-02T00:01:00Z", 51))
        migrate_local({"processing_index": old.index.path})
        migrate_typed({"processing_index": old.index.path})
        new = CanonicalDataStore(root / "quota.jsonl", root / "token.jsonl", "a", lambda _: "v3:account", threading.RLock())
        self.addCleanup(new.index.close)
        self.manager._usage_data = new
        before = new.datasets("merged")
        fetch(self.manager, self.client, self.box)
        self.assertEqual(new.datasets("merged"), before)
        self.assertEqual(new.buffer_status()["pendingRecords"], 3)
        self.assertEqual(push(self.manager, self.client, self.box)["packagesUploaded"], 1)

    def test_startup_due_buffer_and_independent_fetch_schedule(self):
        with mock.patch("time.time", return_value=100):
            self.store.record_quota(self.quota())
        self.manager.configure_usage_sync(self.open_store())
        with mock.patch.object(self.manager, "sync_usage_data") as sync, mock.patch("time.time", return_value=1900):
            self.assertTrue(self.manager._auto_usage_sync_if_due(time.monotonic()))
            sync.assert_called_once_with(automatic=True)
        self.manager._next_usage_fetch_at = time.monotonic() - 1
        self.manager._next_usage_upload_retry_at = time.monotonic() + 900
        self.manager._connection = lambda *_: (self.client, self.box)
        with mock.patch("monitor_packages.push", side_effect=AssertionError("upload backoff")), mock.patch("monitor_packages.fetch", return_value={"packagesDownloaded": 0}) as fetch_mock:
            self.manager.sync_usage_data(automatic=True)
            fetch_mock.assert_called_once()

    def test_startup_connection_failure_obeys_retry_backoff(self):
        self.manager.configure_usage_sync(self.store)
        now = time.monotonic()
        with mock.patch.object(self.manager, "sync_usage_data", side_effect=CloudError("offline", 502)) as sync:
            self.assertFalse(self.manager._auto_usage_sync_if_due(now))
            self.assertFalse(self.manager._auto_usage_sync_if_due(now + 5))
            self.assertEqual(sync.call_count, 1)
            self.assertFalse(self.manager._auto_usage_sync_if_due(now + 30))
            self.assertEqual(sync.call_count, 2)

    def test_repair_preserves_canonical_buffer_and_receipts(self):
        self.store.record_quota(self.quota())
        self.store.import_package("dest", "peer", "package", [self.remote(self.quota("2030-01-01T10:30:00Z", 51))])
        before = self.store.datasets("local"), self.store.datasets("merged"), self.store.buffer_status()
        self.store.index.repair()
        self.assertEqual((self.store.datasets("local"), self.store.datasets("merged"), self.store.buffer_status()), before)
        delivery = DashboardDelivery(self.store.index)
        delivery.prepare()
        self.assertTrue(json.loads(delivery.bulk(False))["accounts"])
        with self.store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM package_receipts").fetchone()[0], 1)

    def test_migration_preserves_uncommitted_source_events_after_file_disappears(self):
        import test_processing as fixtures
        root = self.root / "pending"
        old = UsageDataStore(root / "quota.jsonl", root / "token.jsonl", "a", lambda _: "v3:account", threading.RLock())
        log = root / "sessions" / "log.jsonl"
        log.parent.mkdir()
        log.write_text(json.dumps({"type": "session_meta", "payload": {"id": "session"}}) + "\n" + json.dumps(fixtures.ProcessingTests.event(1, 100)) + "\n", encoding="utf-8")
        old.sources.scan(root)
        log.unlink()
        migrate_local({"processing_index": old.index.path})
        migrate_typed({"processing_index": old.index.path})
        new = self.open_store("pending")
        self.assertEqual(new.datasets()[1][0]["tokens"]["inputTokens"], 100)
        self.assertEqual(new.buffer_status()["pendingRecords"], 1)


    def test_binding_account_updates_both_view_labels_without_cloud_history(self):
        self.store.record_ledger([authority.SQLiteAuthorityTests.ledger()])
        delivery = DashboardDelivery(self.store.index)
        delivery.prepare()
        before = self.store.buffer_status()
        self.store.account_mapper = lambda *_: ("bound-slot", "Bound account")
        self.store.refresh_accounts()
        delivery.prepare(force=True)
        account = json.loads(delivery.bulk(False))["accounts"][0]
        self.assertEqual(account["label"], "Bound account")
        self.assertEqual(account["local"], account["merged"])
        self.assertEqual(self.store.buffer_status(), before)


def canonical_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


if __name__ == "__main__":
    unittest.main()
