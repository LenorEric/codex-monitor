import base64
import json
import shutil
import uuid
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest import mock

import monitor_migrations as updates
import monitor_migration_samples as old_samples
import monitor_sample_store as samples
from monitor_cloud import CryptoBox, valid_passphrase_hash
from monitor_common import empty_cost_totals, empty_token_totals
from monitor_migrations import FormatConverter, convert_verifier, stable_id, write_json
from monitor_processing import initialize_processing_index
from monitor_migrations import _load_v18_token_ledger as load_token_ledger
from monitor_usage_sync import UsageDataStore, content_hash, initialize_v4_cache, record_key, validate_sync_operation


class UnifiedContractTests(unittest.TestCase):
    def test_contract_metadata_requires_aligned_targets_and_contiguous_registries(self):
        import monitor_migrations
        metadata = {"dataContractVersion": updates.DATA_CONTRACT_VERSION, "cloudDataContractVersion": updates.CLOUD_DATA_CONTRACT_VERSION}
        monitor_migrations.validate_contract_metadata(metadata)
        for field in metadata:
            with self.subTest(field=field), self.assertRaises(monitor_migrations.MigrationError):
                monitor_migrations.validate_contract_metadata(metadata | {field: metadata[field] + 1})
        for name in ("DATA_MIGRATIONS",):
            registry = dict(getattr(monitor_migrations, name))
            del registry[1]
            with self.subTest(registry=name), mock.patch.object(monitor_migrations, name, registry), self.assertRaises(monitor_migrations.MigrationError):
                monitor_migrations.validate_contract_metadata(metadata)

    def setUp(self):
        self.root = Path.cwd() / f".test-unified-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.current = stable_id("account", "account-a")
        write_json(self.root / updates.DATA_MIGRATION_STATE_FILENAME, {"dataContractVersion": 14})
        write_json(self.root / "machine.json", {"version": 1, "machineId": "machine-a"})
        write_json(self.root / "accounts/accounts.json", {"version": 2, "accounts": [{"id": "slot-a", "label": "A", "identity": {"accountId": "account-a"}, "sessionRefreshEnabled": True, "configHeader": {"model_provider": "custom"}}]})
        write_json(self.root / "cloud-state.json", {"version": 1, "usage": {"accountAliases": {"old-id": self.current}}, "remote": {"accounts": {}, "skills": {}}})
        self.paths = [self.root / name for name in ("history.jsonl", "quota.jsonl", "sessions.jsonl", "ledger.jsonl", "samples.jsonl")]

    def migrate(self):
        return updates.migrate_history_data(self.root, *self.paths, target_version=15)

    def quota(self, account="old-id"):
        return {"checkedAt": "2026-07-15T11:55:40Z", "accountSlotId": "slot-a", "accountLabel": "A", "windows": {"7d": {"usedPercent": 2, "plan": "plus", "resetAt": None}},
            "sync": {"version": 1, "originMachineId": "machine-a", "accountId": account, "recordId": "old-key"}}

    def baseline(self):
        totals = empty_token_totals() | {"inputTokens": 12, "freshInputTokens": 12, "totalTokens": 12}
        return {"schemaVersion": 1, "recordType": "legacyBaseline", "pricingBasis": "recorded", "session": {"sessionId": "session-a", "accountSlotId": "slot-a", "accountLabel": "A",
            "startedAt": "2026-07-15T11:55:40Z", "updatedAt": "2026-07-15T11:55:40Z", "tokens": totals, "cost": empty_cost_totals() | {"totalCostUsd": 1.5}, "byModel": {}},
            "sourceTotals": {"gpt-5": {"default": totals | {"inputTokens": 20}}}}

    def test_local_migration_converts_raw_and_typed_records_and_invalidates_old_context(self):
        self.paths[1].write_text(json.dumps(self.quota()) + "\n", encoding="utf-8")
        self.paths[3].write_text(json.dumps(self.baseline()) + "\n", encoding="utf-8")
        dbpath = self.root / "usage_monitor_sync_cache-v4.sqlite3"
        initialize_v4_cache(dbpath)
        initialize_processing_index(dbpath)
        with closing(old_samples.connect(dbpath)) as db, db:
            record = {"kind": "tokenLedger", "row": self.baseline()}
            old_samples.put_local(db, "old-key", "2026-07-15T12:00Z", content_hash(record), record)
            db.execute("INSERT INTO processing_quota_points VALUES(?,?,?,?,?)", ("merged", "old-id", 1, "old", json.dumps({"usageAccountId": "old-id"})))
            db.execute("INSERT OR REPLACE INTO processing_meta VALUES('parserState','keep')")
        before = self.baseline()["session"]
        self.assertEqual(self.migrate(), [15])
        account = json.loads((self.root / "accounts/accounts.json").read_text())["accounts"][0]
        self.assertNotIn("configHeader", account)
        self.assertEqual(account["configHeaderToml"], 'model_provider = "custom"\n')
        quota = json.loads(self.paths[1].read_text())
        self.assertEqual(quota["sync"]["accountId"], self.current)
        self.assertNotEqual(quota["sync"]["recordId"], "old-key")
        ledger = load_token_ledger(self.paths[3])
        self.assertEqual(ledger[0]["recordType"], "sessionBaseline")
        self.assertEqual(ledger[0]["sourceTotals"], self.baseline()["sourceTotals"])
        self.assertEqual(ledger[0]["session"]["cost"], before["cost"])
        self.assertEqual(ledger[0]["session"]["tokens"], before["tokens"])
        with closing(samples.connect(dbpath)) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM processing_quota_points").fetchone()[0], 0)
            record = json.loads(db.execute("SELECT record_json FROM local_records").fetchone()[0])
            self.assertEqual(record["row"]["sync"]["accountId"], self.current)
            self.assertEqual(record["row"]["recordType"], "sessionBaseline")
            self.assertEqual(db.execute("SELECT period FROM local_records").fetchone()[0], "2026-07-15T12:00Z")
        self.assertEqual(self.migrate(), [])

    def test_failure_restores_all_originals_and_retry_is_safe(self):
        self.paths[1].write_text(json.dumps(self.quota()) + "\n", encoding="utf-8")
        originals = {path: path.read_bytes() for path in self.root.rglob("*") if path.is_file()}
        checkpoint = updates._atomic_write
        def fail(path, data):
            if Path(path).name == updates.DATA_MIGRATION_STATE_FILENAME:
                raise OSError("checkpoint failure")
            return checkpoint(path, data)
        with mock.patch.object(updates, "_atomic_write", side_effect=fail), self.assertRaisesRegex(OSError, "checkpoint failure"):
            self.migrate()
        self.assertEqual({path: path.read_bytes() for path in originals}, originals)
        self.assertFalse((self.root / "config.json").exists())
        self.assertEqual(self.migrate(), [15])

    def test_v15_does_not_open_retired_history_or_diagnostic_logs(self):
        self.paths[4] = self.root / "usage_monitor_diagnostic_samples.jsonl"
        retired = {self.paths[0], self.paths[4], self.root / "usage_monitor_samples.jsonl"}
        for path in retired:
            path.write_bytes(b'{"checkedAt":0}\n\x00broken diagnostic tail\xff')
        self.paths[1].write_text(json.dumps(self.quota()) + "\n", encoding="utf-8")
        open_path = Path.open

        def guard(path, *args, **kwargs):
            if path in retired:
                raise AssertionError(f"Migration opened retired log: {path}")
            return open_path(path, *args, **kwargs)

        with mock.patch.object(Path, "open", guard):
            self.assertEqual(self.migrate(), [15])
            self.assertEqual(self.migrate(), [])
        for path in retired:
            self.assertEqual(path.read_bytes(), b'{"checkedAt":0}\n\x00broken diagnostic tail\xff')
        self.assertEqual(json.loads(self.paths[1].read_text())["sync"]["accountId"], self.current)

    def test_runtime_rejects_old_ledger_and_identity(self):
        self.paths[3].write_text(json.dumps(self.baseline()) + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            load_token_ledger(self.paths[3])
        old = self.quota()
        with self.assertRaises(ValueError):
            validate_sync_operation({"action": "upsert", "key": record_key("quota", old), "record": {"kind": "quota", "row": old}})

    def test_missing_session_logs_preserve_cost_tokens_and_replay_coverage(self):
        row = {"schemaVersion": 1, "recordType": "usage", "eventId": "session-a:1", "sessionId": "session-a", "occurredAt": "2026-07-15T11:55:40Z",
            "rawModel": "gpt-5", "billingModel": "gpt-5", "serviceTier": "default", "accountSlotId": "slot-a", "accountLabel": "A", "sourceFile": "missing.jsonl",
            "tokens": empty_token_totals() | {"inputTokens": 20, "freshInputTokens": 20, "totalTokens": 20, "requests": 1}, "cost": empty_cost_totals() | {"totalCostUsd": 1.5}}
        self.paths[3].write_text(json.dumps(row) + "\n", encoding="utf-8")
        self.migrate()
        migrated = load_token_ledger(self.paths[3])
        self.assertEqual(migrated[0]["recordType"], "usage")
        self.assertNotEqual(migrated[0]["eventId"], "session-a:1")
        self.assertEqual(migrated[0]["occurredAt"], row["occurredAt"])
        self.assertEqual(migrated[0]["cost"], row["cost"])
        self.assertEqual(migrated[0]["tokens"], row["tokens"])
        self.assertIn("sourceCoverage", migrated[0])
        self.assertNotIn("sourceFile", migrated[0])
        from monitor_token_ledger import canonical_token_ledger_rows
        with self.assertRaises(ValueError):
            canonical_token_ledger_rows(migrated)

    def test_peer_usage_ids_are_converted_consistently_without_changing_recorded_cost(self):
        row = {"schemaVersion": 1, "recordType": "usage", "eventId": "session-a:1", "sessionId": "session-a", "occurredAt": "2026-07-15T11:55:40Z",
            "rawModel": "gpt-5", "billingModel": "gpt-5", "serviceTier": "default", "accountSlotId": "slot-a", "sourceFile": "old.jsonl",
            "tokens": empty_token_totals() | {"inputTokens": 20}, "cost": empty_cost_totals() | {"totalCostUsd": 1.5}}
        converter = FormatConverter({}, {}, {}, "machine-a")
        record = converter.record({"kind": "tokenLedger", "row": row}, "machine-b")
        self.assertRegex(record["row"]["eventId"], r"^session-a:[0-9a-f]{24}$")
        self.assertEqual(record["row"]["cost"], row["cost"])
        self.assertEqual(record["row"]["tokens"], row["tokens"])
        self.assertEqual(converter.record(record, "machine-b"), record)

    def test_flat_cache_and_foreign_raw_rows_move_to_current_partitions(self):
        remote = self.quota()
        remote["sync"]["originMachineId"] = "machine-b"
        self.paths[1].write_text(json.dumps(remote) + "\n", encoding="utf-8")
        record = {"kind": "quota", "row": remote}
        write_json(self.root / "usage_monitor_sync_cache.json", {"version": 1, "records": [{"sourceMachineId": "machine-b", "key": "old-key", "record": record}]})
        self.migrate()
        self.assertEqual(self.paths[1].read_text(), "")
        self.assertFalse((self.root / "usage_monitor_sync_cache.json").exists())
        store = UsageDataStore(self.paths[1], self.paths[3], "machine-a", lambda _: self.current, threading.RLock(), lambda *_: ("slot-a", "A"), cache_path=self.root / "usage_monitor_sync_cache.json")
        store.initialize_processing()
        self.assertEqual(len(store.datasets("merged")[0]), 1)
        with store.index.transaction() as db:
            row = json.loads(db.execute("SELECT record_json FROM remote_records").fetchone()[0])
            self.assertEqual(row["record"]["row"]["sync"]["accountId"], self.current)

    def test_sharded_cache_failure_restores_deleted_shard_and_retry(self):
        entries = [{"sourceMachineId": "machine-b", "key": "old", "sourcePackId": None, "record": {"kind": "quota", "row": self.quota()}}]
        entries[0]["record"]["row"]["sync"]["originMachineId"] = "machine-b"
        shard = self.root / "usage_monitor_sync_cache.json.d" / "old.json"
        write_json(shard, entries)
        write_json(self.root / "usage_monitor_sync_cache.json", {"version": 3, "layout": "origin-pack-shards-v1", "origins": {"machine-b": {"shards": {
            "loose": {"file": "old.json", "contentHash": content_hash(entries), "recordCount": 1}}}}})
        original = shard.read_bytes()
        checkpoint = updates._atomic_write
        def fail(path, data):
            if Path(path).name == updates.DATA_MIGRATION_STATE_FILENAME:
                raise OSError("checkpoint failure")
            return checkpoint(path, data)
        with mock.patch.object(updates, "_atomic_write", side_effect=fail), self.assertRaises(OSError):
            self.migrate()
        self.assertEqual(shard.read_bytes(), original)
        self.migrate()
        self.assertFalse(shard.exists())

    def test_all_released_migrations_remain_sequential_for_a_fresh_install(self):
        (self.root / updates.DATA_MIGRATION_STATE_FILENAME).unlink()
        self.assertEqual(updates.migrate_history_data(self.root, *self.paths, target_version=15, codex_home=self.root / "codex"), list(range(1, 16)))

    def test_old_verifier_conversion_preserves_encryption_key_and_endpoint_binding(self):
        salt, key = base64.b64encode(b"codex-switch-passphrase-v1").decode(), base64.b64encode(b"k" * 32).decode()
        old = f"scrypt-key-v1$32768$8$1${salt}${key}"
        converted = convert_verifier(old, {"baseUrl": "https://example.test", "username": "user"})
        self.assertFalse(valid_passphrase_hash(old))
        self.assertTrue(valid_passphrase_hash(converted))
        box = CryptoBox(converted, CryptoBox.descriptor(converted))
        self.assertEqual(box.key, b"k" * 32)
        encrypted = box.encrypt("test", b"retained data")
        self.assertEqual(box.decrypt("test", encrypted), b"retained data")


if __name__ == "__main__":
    unittest.main()
