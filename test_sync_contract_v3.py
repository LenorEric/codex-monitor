import json
import shutil
import threading
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

from monitor_common import empty_cost_totals, empty_token_totals
from monitor_history import load_quota_history, write_quota_history
from monitor_token_ledger import _source_highwaters, load_token_ledger, write_token_ledger
from monitor_usage_sync import CACHE_LAYOUT, UsageDataStore, add_record_provenance, canonical_ledger_row, merge_token_ledger_rows, record_key, validate_sync_operation


def quota_row(machine="machine-b", account="account-b", record_id="quota-b", checked_at="2030-01-01T00:00:00Z"):
    return {
        "checkedAt": checked_at,
        "windows": {"5h": {"usedPercent": 12, "resetAt": "2030-01-01T05:00:00Z", "plan": "plus"}},
        "sync": {"version": 1, "originMachineId": machine, "accountId": account, "recordId": record_id},
    }


def usage_row(machine="machine-a", account="account-a", event_id="event-a", cost=1.25):
    return {
        "schemaVersion": 2, "recordType": "usage", "eventId": event_id, "sessionId": "session-a", "occurredAt": "2030-01-01T00:00:00Z",
        "rawModel": "gpt-5", "billingModel": "gpt-5", "serviceTier": "default", "tokens": empty_token_totals(),
        "cost": empty_cost_totals() | {"totalCostUsd": cost}, "sync": {"version": 1, "originMachineId": machine, "accountId": account},
    }


class SyncContractV3Tests(unittest.TestCase):
    @contextmanager
    def directory(self):
        path = Path(__file__).parent / f".sync-v3-test-{uuid.uuid4().hex}"
        path.mkdir()
        try:
            yield path
        finally:
            shutil.rmtree(path, ignore_errors=True)

    def store(self, directory, mapper=None):
        quota, ledger = directory / "quota.jsonl", directory / "ledger.jsonl"
        quota.write_text("", encoding="utf-8")
        ledger.write_text("", encoding="utf-8")
        return UsageDataStore(quota, ledger, "machine-a", lambda _slot: "stable-a", threading.Lock(), mapper)

    def operation(self, row):
        return {"action": "upsert", "key": record_key("quota", row), "record": {"kind": "quota", "row": row}}

    def test_semantic_validation_rejects_invalid_quota_and_ledger_values(self):
        row = quota_row()
        validate_sync_operation(self.operation(row))
        for invalid in (
            row | {"checkedAt": "not-a-time"},
            row | {"windows": {"5h": {"usedPercent": 101, "resetAt": None, "plan": "plus"}}},
            row | {"sync": row["sync"] | {"localOnly": True}},
        ):
            with self.assertRaises(ValueError):
                validate_sync_operation(self.operation(invalid))
        usage = usage_row()
        validate_sync_operation({"action": "upsert", "key": record_key("tokenLedger", usage), "record": {"kind": "tokenLedger", "row": usage}})
        with self.assertRaises(ValueError):
            validate_sync_operation({"action": "upsert", "key": record_key("tokenLedger", usage), "record": {"kind": "tokenLedger", "row": usage | {"cost": {"totalCostUsd": -1}}}})

    def test_canonical_ledger_key_and_content_drop_redundant_pricing_metadata(self):
        legacy = usage_row() | {"schemaVersion": 1, "pricingId": "old-price", "pricingBasis": "old", "sourceTotals": {"unused": True}}
        legacy["sync"] = legacy["sync"] | {"recordId": "redundant", "localOnly": False}
        canonical = canonical_ledger_row(legacy)
        merged, conflicts = merge_token_ledger_rows([legacy])
        self.assertEqual(canonical, merged[0])
        self.assertEqual(conflicts, [])
        self.assertEqual(canonical["cost"]["totalCostUsd"], 1.25)
        self.assertFalse({"pricingId", "pricingBasis", "sourceTotals"} & set(canonical))
        self.assertFalse({"recordId", "localOnly"} & set(canonical["sync"]))
        self.assertEqual(record_key("tokenLedger", canonical), "tokenLedger:usage:account-a:event-a")

    def test_local_quota_old_identity_is_rekeyed_with_new_record_id(self):
        with self.directory() as directory:
            store = self.store(directory)
            local = quota_row("machine-a", "local:machine-a:slot-a", "old-record") | {"accountSlotId": "slot-a", "accountLabel": "A"}
            write_quota_history(store.quota_path, [local])
            normalized, _ = store.normalize_local()
            self.assertEqual(normalized[0]["sync"]["accountId"], "stable-a")
            self.assertNotEqual(normalized[0]["sync"]["recordId"], "old-record")
            self.assertEqual(load_quota_history(store.quota_path), normalized)

    def test_ledger_normalization_persists_identity_and_preserves_stable_identity_after_slot_removal(self):
        with self.directory() as directory:
            mapper = lambda account, *_args: ("slot-a", "A") if account == "legacy-hmac" else None
            store = self.store(directory, mapper)
            untagged = {key: value for key, value in usage_row().items() if key != "sync"}
            legacy = usage_row(account="legacy-hmac", event_id="event-b")
            stable = usage_row(account="v3:" + "a" * 64, event_id="event-c")
            write_token_ledger(store.token_ledger_path, [untagged, legacy, stable])
            store.normalize_local()
            identities = {row["eventId"]: row["sync"]["accountId"] for row in load_token_ledger(store.token_ledger_path)}
            self.assertEqual(identities, {"event-a": "stable-a", "event-b": "stable-a", "event-c": "v3:" + "a" * 64})

    def test_ledger_normalization_preserves_non_derivable_legacy_highwaters(self):
        with self.directory() as directory:
            store = self.store(directory)
            baseline = {
                "schemaVersion": 1, "recordType": "legacyBaseline", "pricingBasis": "legacyRecordedCost", "session": {
                    "sessionId": "old", "accountSlotId": "slot-a", "accountLabel": "A", "startedAt": "2030-01-01T00:00:00Z", "updatedAt": "2030-01-01T00:01:00Z",
                    "tokens": empty_token_totals(), "cost": empty_cost_totals(), "byModel": {},
                },
                "sourceTotals": {"gpt-5": {"default": empty_token_totals() | {"inputTokens": 20}, "fast": empty_token_totals()}},
            }
            before = _source_highwaters([baseline])
            write_token_ledger(store.token_ledger_path, [baseline])
            store.normalize_local()
            persisted = load_token_ledger(store.token_ledger_path)
            self.assertEqual(_source_highwaters(persisted), before)
            self.assertIn("sourceTotals", persisted[0])

    def test_mapped_legacy_remote_identity_deduplicates_with_stable_local_event(self):
        with self.directory() as directory:
            stable_id = "v3:" + "a" * 64
            mapper = lambda account, *_args: ("slot-a", "A") if account in {"legacy-hmac", stable_id} else None
            store = UsageDataStore(directory / "quota.jsonl", directory / "ledger.jsonl", "machine-a", lambda _slot: stable_id, threading.Lock(), mapper)
            store.quota_path.write_text("", encoding="utf-8")
            local = usage_row(account=stable_id)
            write_token_ledger(store.token_ledger_path, [local])
            remote = usage_row(machine="machine-b", account="legacy-hmac")
            store.apply([{"action": "upsert", "key": record_key("tokenLedger", remote), "record": {"kind": "tokenLedger", "row": remote}}], operation_origin="machine-b")
            self.assertEqual(len(store.datasets("merged")[1]), 1)

    def test_pack_updates_write_immutable_per_pack_shards_and_detect_missing_shard(self):
        with self.directory() as directory:
            store = self.store(directory)
            first, second = quota_row(record_id="first"), quota_row(record_id="second", checked_at="2030-01-01T00:01:00Z")
            manifest = {"pack-1": "1" * 64}
            store.apply_pack_snapshot("machine-b", manifest, {"pack-1": [{"key": record_key("quota", first), "record": {"kind": "quota", "row": first}}]})
            root = json.loads(store.cache_path.read_text(encoding="utf-8"))
            self.assertEqual(root["layout"], CACHE_LAYOUT)
            first_file = root["origins"]["machine-b"]["shards"]["pack-1"]["file"]
            first_bytes = (store._cache_shard_path / first_file).read_bytes()
            manifest["pack-2"] = "2" * 64
            store.apply_pack_snapshot("machine-b", manifest, {"pack-2": [{"key": record_key("quota", second), "record": {"kind": "quota", "row": second}}]})
            self.assertEqual((store._cache_shard_path / first_file).read_bytes(), first_bytes)
            (store._cache_shard_path / first_file).unlink()
            recovered = UsageDataStore(store.quota_path, store.token_ledger_path, "machine-a", lambda _slot: "stable-a", threading.Lock(), cache_path=store.cache_path)
            self.assertEqual(recovered.pack_hashes("machine-b"), {})
            self.assertTrue(recovered.needs_remote_rebuild)

    def test_authoritative_origin_removal_is_atomic_and_preserves_unknown_accounts(self):
        with self.directory() as directory:
            store = self.store(directory, mapper=lambda *_args: None)
            for machine in ("machine-b", "machine-c"):
                row = quota_row(machine, f"unknown-{machine}", f"record-{machine}")
                store.apply([self.operation(row)], operation_origin=machine)
            self.assertEqual(store.datasets("merged")[0], [])
            self.assertEqual(store.remove_origins_not_in({"machine-c"}), 1)
            self.assertEqual(store.remove_origins_not_in({"machine-c"}), 0)
            payload = json.loads(store.cache_path.read_text(encoding="utf-8"))
            self.assertEqual(set(payload["origins"]), {"machine-c"})

    def test_remote_import_is_cached_before_local_file_cleanup(self):
        with self.directory() as directory:
            store = self.store(directory, mapper=lambda *_args: ("slot-a", "A"))
            remote = quota_row() | {"accountSlotId": "remote-slot", "accountLabel": "Remote"}
            write_quota_history(store.quota_path, [remote])
            original_write = store._atomic_write

            def fail_quota(path, data):
                if path == store.quota_path:
                    raise OSError("simulated crash boundary")
                return original_write(path, data)

            with mock.patch.object(store, "_atomic_write", side_effect=fail_quota), self.assertRaises(OSError):
                store.normalize_local()
            recovered = UsageDataStore(store.quota_path, store.token_ledger_path, "machine-a", lambda _slot: "stable-a", threading.Lock(), lambda *_args: ("slot-a", "A"), cache_path=store.cache_path)
            self.assertEqual(len(recovered.datasets("merged")[0]), 1)


if __name__ == "__main__":
    unittest.main()
