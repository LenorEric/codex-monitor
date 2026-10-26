import base64
import hashlib
import json
import shutil
import uuid
import threading
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import monitor_migrations as updates
import monitor_migration_samples as old_samples
import monitor_sample_store as samples
from monitor_accounts import AccountManager
from monitor_cloud import CloudManager, CryptoBox, passphrase_hash, valid_passphrase_hash
from monitor_common import empty_cost_totals, empty_token_totals
from monitor_migrations import FormatConverter, convert_verifier, stable_id, write_json
from monitor_processing import initialize_processing_index
from monitor_token_ledger import load_token_ledger, token_sessions_from_ledger
from monitor_usage_sync import UsageDataStore, content_hash, initialize_v4_cache, record_key, validate_sync_operation
from test_usage_v4 import MemoryWebDav


class UnifiedContractTests(unittest.TestCase):
    def test_contract_metadata_requires_aligned_targets_and_contiguous_registries(self):
        import monitor_migrations
        metadata = {"dataContractVersion": 15, "cloudDataContractVersion": 3}
        monitor_migrations.validate_contract_metadata(metadata)
        for field in metadata:
            with self.subTest(field=field), self.assertRaises(monitor_migrations.MigrationError):
                monitor_migrations.validate_contract_metadata(metadata | {field: metadata[field] + 1})
        for name in ("DATA_MIGRATIONS", "CLOUD_DATA_MIGRATIONS"):
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
        self.assertEqual(token_sessions_from_ledger(ledger)[0]["cost"], before["cost"])
        self.assertEqual(token_sessions_from_ledger(ledger)[0]["tokens"], before["tokens"])
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

    def test_runtime_rejects_old_ledger_and_identity(self):
        self.paths[3].write_text(json.dumps(self.baseline()) + "\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            load_token_ledger(self.paths[3])
        old = self.quota()
        with self.assertRaises(ValueError):
            validate_sync_operation({"action": "upsert", "key": record_key("quota", old), "record": {"kind": "quota", "row": old}})

    def test_missing_session_logs_preserve_cost_tokens_and_replay_coverage(self):
        from monitor_token_ledger import _source_highwaters, _usage_records
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
        replay = {"eventId": "session-a:hash", "sessionId": "session-a", "checkedAt": row["occurredAt"], "model": "gpt-5", "tokens": {"input": 20, "cachedInput": 0, "cacheWriteInput": 0, "output": 0}}
        self.assertEqual(_usage_records([replay], _source_highwaters(migrated), "slot-a", "A", []), [])
        from monitor_token_ledger import sync_token_ledger
        self.assertNotIn("sourceFile", migrated[0])
        sync_token_ledger(self.paths[3], [replay], "slot-a", "A", source_files=["missing.jsonl"], own_machine_id="machine-a")
        self.assertEqual(load_token_ledger(self.paths[3]), migrated)

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

    def test_cloud_migration_converts_old_packs_to_v4_and_commits_after_verification(self):
        self.migrate()
        cloud = CloudManager(self.root, SimpleNamespace(), AccountManager(self.root / "auth.json", self.root / "accounts"))
        self.addCleanup(cloud.operation_queue.close)
        client = MemoryWebDav()
        client._migration_manager = cloud
        box = CryptoBox("", CryptoBox.descriptor(""))
        client.put("crypto.json", json.dumps(box.descriptor("")).encode())
        client.put("data-contract.json", b'{"dataContractVersion":2}')
        row = self.quota()
        row["sync"]["originMachineId"] = "machine-b"
        record = {"kind": "quota", "row": row}
        digest, _ = cloud._put_usage_payload(client, box, "packs", "machine-b", {"version": 1, "machineId": "machine-b", "packId": "pack", "records": [{"key": "old", "record": record}]})
        client.put("usage/machines/machine-b.enc", box.encrypt("usage-pointer:machine-b", json.dumps({"version": 2, "machineId": "machine-b", "packs": {"pack": digest}}).encode()))
        from monitor_migrations import ensure_cloud_contract
        ensure_cloud_contract(client)
        self.assertEqual(json.loads(client.get("data-contract.json")[0])["dataContractVersion"], 3)
        head, _ = cloud._v4_head(client, box, "machine-b")
        catalog = cloud._v4_catalog(client, box, "machine-b", head)
        self.assertEqual(set(catalog), {"2026-07-15"})
        reference = catalog["2026-07-15"]
        manifest = cloud._v4_day_manifest(client, box, "machine-b", "2026-07-15", reference)
        rows = cloud._v4_data(client, box, "machine-b", "2026-07-15", "bulk", manifest["parts"]["bulk"])
        self.assertTrue(rows[0]["record"]["row"]["sync"]["accountId"].startswith("v3:"))
        self.assertNotIn("usage/machines/machine-b.enc", client.files)

    def test_encrypted_cloud_failure_restores_roots_and_retries(self):
        from monitor_cloud import CloudError
        from monitor_migrations import ensure_cloud_contract
        from monitor_skills import SkillManager
        self.migrate()
        skills = SkillManager(self.root / "codex", self.root, self.root / "gemini")
        cloud = CloudManager(self.root, skills, None)
        self.addCleanup(cloud.operation_queue.close)
        key = passphrase_hash("test", "https://example.test", "user")
        cloud._config["webdav"]["encryptionPassphraseHash"] = key
        client = MemoryWebDav()
        client._migration_manager = cloud
        box = CryptoBox(key, CryptoBox.descriptor(key))
        descriptor = box.descriptor(key) | {"version": 2}
        client.put("crypto.json", json.dumps(descriptor).encode())
        client.put("data-contract.json", b'{"dataContractVersion":2}')
        demo = skills.skills_root / "demo"
        demo.mkdir(parents=True)
        (demo / "SKILL.md").write_text("Retain this skill", encoding="utf-8")
        snapshot = skills.snapshot()
        client.put("skills/snapshots/old.enc", box.encrypt("skills-snapshot:old", snapshot))
        pointer = box.encrypt("skills-pointer", b'{"version":1,"snapshotId":"old"}')
        client.put("skills/current.enc", pointer)
        original_put = client.put
        def fail(path, data, **kwargs):
            if path == "crypto.json":
                raise CloudError("simulated commit failure")
            return original_put(path, data, **kwargs)
        with mock.patch.object(client, "put", side_effect=fail), self.assertRaisesRegex(CloudError, "simulated"):
            ensure_cloud_contract(client)
        self.assertEqual(client.get("skills/current.enc")[0], pointer)
        self.assertEqual(json.loads(client.get("data-contract.json")[0])["dataContractVersion"], 2)
        ensure_cloud_contract(client)
        migrated = json.loads(box.decrypt("skills-pointer", client.get("skills/current.enc")[0]))
        self.assertEqual(migrated["version"], 2)
        package = migrated["packages"]["demo"]
        self.assertEqual(skills.inspect_snapshot(cloud._download_skill_package(client, box, "demo", package))[1]["demo"]["SKILL.md"], b"Retain this skill")
        self.assertEqual(json.loads(client.get("crypto.json")[0])["version"], 3)
        self.assertEqual(json.loads(client.get("data-contract.json")[0])["dataContractVersion"], 3)


if __name__ == "__main__":
    unittest.main()
