import json
import shutil
import threading
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

from monitor_migrations import FormatConverter
from monitor_common import empty_cost_totals, empty_token_totals
from monitor_history import load_quota_history
from monitor_migrations import _load_v18_token_ledger as load_token_ledger
from monitor_usage_sync import v4_record_envelope, v4_logical_hash, UsageDataStore, add_record_provenance, canonical_ledger_row, merge_token_ledger_rows, record_key, validate_sync_operation


def quota_row(machine="machine-b", account="account-b", record_id="quota-b", checked_at="2030-01-01T00:00:00Z"):
    return {
        "checkedAt": checked_at,
        "windows": {"5h": {"usedPercent": 12, "resetAt": "2030-01-01T05:00:00Z", "plan": "plus"}},
        "sync": {"version": 1, "originMachineId": machine, "accountId": account if account.startswith("v3:") or account.startswith("local:") or account == "legacy-hmac" else "v3:" + account, "recordId": record_id},
    }


def usage_row(machine="machine-a", account="v3:account-a", event_id="event-a", cost=1.25):
    return {
        "schemaVersion": 2, "recordType": "usage", "eventId": event_id, "sessionId": "session-a", "occurredAt": "2030-01-01T00:00:00Z",
        "rawModel": "gpt-5", "billingModel": "gpt-5", "serviceTier": "default", "tokens": empty_token_totals(),
        "cost": empty_cost_totals() | {"totalCostUsd": cost}, "sync": {"version": 1, "originMachineId": machine, "accountId": account if account.startswith("v3:") or account == "legacy-hmac" else "v3:" + account},
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
        return UsageDataStore(quota, ledger, "machine-a", lambda _slot: "v3:stable-a", threading.Lock(), mapper)

    def import_peer(self, store, machine, kind, row):
        key = record_key(kind, row)
        envelope = v4_record_envelope(machine, key, {"kind": kind, "row": row}, "2030-01-01T00:00Z")
        store.v4_apply_day(machine, {"day": "2030-01-01", "parts": {"bulk": "digest"}, "logicalHash": v4_logical_hash([envelope])}, {"bulk": [envelope]}, True)

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
        legacy = usage_row() | {"schemaVersion": 1, "pricingId": "old-price", "pricingBasis": "old"}
        legacy["sync"] = legacy["sync"] | {"recordId": "redundant", "localOnly": False}
        converter = FormatConverter({}, {}, {}, "machine-a")
        with self.assertRaises(ValueError):
            canonical_ledger_row(legacy)
        canonical = canonical_ledger_row(converter.row("tokenLedger", legacy))
        merged = merge_token_ledger_rows([canonical])
        self.assertEqual(canonical, merged[0])
        self.assertEqual(canonical["cost"]["totalCostUsd"], 1.25)
        self.assertFalse({"pricingId", "pricingBasis", "sourceTotals"} & set(canonical))
        self.assertFalse({"recordId", "localOnly"} & set(canonical["sync"]))
        self.assertEqual(record_key("tokenLedger", canonical), "tokenLedger:usage:v3:account-a:event-a")

    def test_local_quota_old_identity_is_rekeyed_with_new_record_id(self):
        with self.directory() as directory:
            store = self.store(directory)
            local = quota_row("machine-a", "local:machine-a:slot-a", "old-record") | {"accountSlotId": "slot-a", "accountLabel": "A"}
            converter = FormatConverter({}, {"usage": {"accountAliases": {local["sync"]["accountId"]: "v3:stable-a"}}}, {}, "machine-a")
            store.record_quota(converter.row("quota", local))
            normalized, _ = store.datasets()
            self.assertEqual(normalized[0]["sync"]["accountId"], "v3:stable-a")
            self.assertNotEqual(normalized[0]["sync"]["recordId"], "old-record")
            self.assertEqual(load_quota_history(store.quota_path), normalized)

    def test_ledger_normalization_persists_identity_and_preserves_stable_identity_after_slot_removal(self):
        with self.directory() as directory:
            mapper = lambda account, *_args: ("slot-a", "A") if account == "legacy-hmac" else None
            store = self.store(directory, mapper)
            untagged = {key: value for key, value in usage_row().items() if key != "sync"}
            legacy = usage_row(account="legacy-hmac", event_id="event-b")
            stable = usage_row(account="v3:" + "a" * 64, event_id="event-c")
            converter = FormatConverter({}, {"usage": {"accountAliases": {"legacy-hmac": "v3:stable-a"}}}, {}, "machine-a")
            store.record_ledger([untagged, converter.row("tokenLedger", legacy), stable])
            store.datasets()
            identities = {row["eventId"]: row["sync"]["accountId"] for row in load_token_ledger(store.token_ledger_path)}
            self.assertEqual(identities, {"event-a": "v3:stable-a", "event-b": "v3:stable-a", "event-c": "v3:" + "a" * 64})

    def test_ledger_normalization_rejects_non_derivable_legacy_highwaters(self):
        with self.directory() as directory:
            store = self.store(directory)
            baseline = {
                "schemaVersion": 1, "recordType": "legacyBaseline", "pricingBasis": "legacyRecordedCost", "session": {
                    "sessionId": "old", "accountSlotId": "slot-a", "accountLabel": "A", "startedAt": "2030-01-01T00:00:00Z", "updatedAt": "2030-01-01T00:01:00Z",
                    "tokens": empty_token_totals(), "cost": empty_cost_totals(), "byModel": {},
                },
                "sourceTotals": {"gpt-5": {"default": empty_token_totals() | {"inputTokens": 20}, "fast": empty_token_totals()}},
            }
            baseline = FormatConverter({}, {}, {}, "machine-a").row("tokenLedger", baseline)
            with self.assertRaises(ValueError):
                store.record_ledger([baseline])
            self.assertEqual(store.datasets()[1], [])
            self.assertEqual(load_token_ledger(store.token_ledger_path), [])

    def test_mapped_legacy_remote_identity_deduplicates_with_stable_local_event(self):
        with self.directory() as directory:
            stable_id = "v3:" + "a" * 64
            mapper = lambda account, *_args: ("slot-a", "A") if account in {"legacy-hmac", stable_id} else None
            store = UsageDataStore(directory / "quota.jsonl", directory / "ledger.jsonl", "machine-a", lambda _slot: stable_id, threading.Lock(), mapper)
            store.quota_path.write_text("", encoding="utf-8")
            local = usage_row(account=stable_id)
            store.record_ledger([local])
            remote = usage_row(machine="machine-b", account="legacy-hmac")
            remote = FormatConverter({}, {"usage": {"accountAliases": {"legacy-hmac": stable_id}}}, {}, "machine-a").row("tokenLedger", remote)
            self.import_peer(store, "machine-b", "tokenLedger", remote)
            self.assertEqual(len(store.datasets("merged")[1]), 1)

    def test_obsolete_cache_transport_apis_are_removed(self):
        for name in ("apply", "apply_pack_snapshot", "pack_hashes", "_load_cache"):
            self.assertFalse(hasattr(UsageDataStore, name))

    def test_authoritative_origin_removal_is_atomic_and_preserves_unknown_accounts(self):
        with self.directory() as directory:
            store = self.store(directory, mapper=lambda *_args: None)
            for machine in ("machine-b", "machine-c"):
                row = quota_row(machine, f"unknown-{machine}", f"record-{machine}")
                self.import_peer(store, machine, "quota", row)
            self.assertEqual(store.datasets("merged")[0], [])
            self.assertEqual(store.remove_origins_not_in({"machine-c"}), 1)
            self.assertEqual(store.remove_origins_not_in({"machine-c"}), 0)
            with store.index.transaction() as db:
                self.assertEqual({row[0] for row in db.execute("SELECT machine_id FROM remote_origins")}, {"machine-c"})

    def test_foreign_history_is_rejected_until_explicit_migration(self):
        with self.directory() as directory:
            store = self.store(directory, mapper=lambda *_args: ("slot-a", "A"))
            with self.assertRaisesRegex(ValueError, "require data migration"):
                store.record_quota(quota_row())
            self.assertEqual(store.datasets(), ([], []))


if __name__ == "__main__":
    unittest.main()
