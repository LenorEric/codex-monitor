import json
import shutil
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import monitor_auto_update
import monitor_history
import monitor_token_ledger
from monitor_common import empty_cost_totals, empty_token_totals


@contextmanager
def workspace_directory():
    path = Path.cwd() / f".test-local-contract-{uuid.uuid4().hex}"
    path.mkdir()
    try:
        yield str(path)
    finally:
        shutil.rmtree(path)


def quota_row(checked_at: str) -> dict:
    return {"checkedAt": checked_at, "accountSlotId": "account", "accountLabel": "Account", "windows": {"5h": {"usedPercent": 1, "resetAt": None, "plan": "plus", "planMultiplier": 1}}}


def usage_row(schema_version: int = 2) -> dict:
    return {
        "schemaVersion": schema_version, "recordType": "usage", "eventId": "event", "occurredAt": "2026-01-01T00:00:00Z", "sessionId": "session",
        "rawModel": "gpt-5.5", "billingModel": "gpt-5.5", "serviceTier": "default", "accountSlotId": "account", "accountLabel": "Account",
        "tokens": empty_token_totals() | {"inputTokens": 10, "totalTokens": 10}, "cost": empty_cost_totals() | {"inputCostUsd": 0.1, "totalCostUsd": 0.1},
    }


class LocalContractV3Tests(unittest.TestCase):
    def test_v2_rejects_partially_represented_session_and_preserves_originals(self):
        with workspace_directory() as directory:
            root = Path(directory)
            history, quota, sessions, ledger, samples = (root / name for name in ("history.jsonl", "quota.jsonl", "sessions.jsonl", "ledger.jsonl", "samples.jsonl"))
            state = root / monitor_auto_update.DATA_MIGRATION_STATE_FILENAME
            state.write_text('{"dataContractVersion":1}\n', encoding="utf-8")
            source_tokens = empty_token_totals() | {"inputTokens": 10, "freshInputTokens": 10, "totalTokens": 10, "requests": 1}
            source_cost = empty_cost_totals() | {"inputCostUsd": 0.1, "totalCostUsd": 0.1}
            session = {
                "sessionId": "session", "startedAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:01:00Z", "accountSlotId": "account", "accountLabel": "Account",
                "tokens": source_tokens, "cost": source_cost, "byModel": {"gpt-5.5": {"tokens": source_tokens, "cost": source_cost}},
            }
            partial = usage_row(1) | {
                "tokens": empty_token_totals() | {"inputTokens": 5, "freshInputTokens": 5, "totalTokens": 5, "requests": 1},
                "cost": empty_cost_totals() | {"inputCostUsd": 0.05, "totalCostUsd": 0.05},
            }
            sessions.write_text(json.dumps(session) + "\n", encoding="utf-8")
            ledger.write_text(json.dumps(partial) + "\n", encoding="utf-8")
            originals = {path: path.read_bytes() for path in (sessions, ledger, state)}

            with self.assertRaisesRegex(monitor_auto_update.AutoUpdateError, "does not cover all historical"):
                monitor_auto_update.migrate_history_data(root, history, quota, sessions, ledger, samples, 2)

            self.assertEqual({path: path.read_bytes() for path in originals}, originals)
            self.assertFalse((root / monitor_auto_update.DATA_MIGRATION_JOURNAL_FILENAME).exists())

    def test_checkpoint_failure_rolls_back_changed_data_and_retry_succeeds(self):
        with workspace_directory() as directory:
            root = Path(directory)
            history, quota, sessions, ledger, samples = (root / name for name in ("history.jsonl", "quota.jsonl", "sessions.jsonl", "ledger.jsonl", "samples.jsonl"))
            state = root / monitor_auto_update.DATA_MIGRATION_STATE_FILENAME
            state.write_text('{"dataContractVersion":2}\n', encoding="utf-8")
            history.write_text('{"derived":true}\n', encoding="utf-8")
            quota.write_text(json.dumps(quota_row("2026-01-01T00:00:00Z"), indent=2) + "\n", encoding="utf-8")
            ledger.write_text(json.dumps(usage_row(1) | {"pricingId": "old-price"}) + "\n", encoding="utf-8")
            originals = {path: path.read_bytes() for path in (history, quota, ledger, state)}
            atomic_write = monitor_auto_update._atomic_write

            def fail_checkpoint(path, data):
                if Path(path) == state:
                    raise OSError("checkpoint failed")
                return atomic_write(path, data)

            with mock.patch("monitor_auto_update._atomic_write", side_effect=fail_checkpoint), self.assertRaisesRegex(OSError, "checkpoint failed"):
                monitor_auto_update.migrate_history_data(root, history, quota, sessions, ledger, samples, 3)

            self.assertEqual({path: path.read_bytes() for path in originals}, originals)
            self.assertFalse((root / monitor_auto_update.DATA_MIGRATION_JOURNAL_FILENAME).exists())
            self.assertEqual(monitor_auto_update.migrate_history_data(root, history, quota, sessions, ledger, samples, 3), [3])
            self.assertFalse(history.exists())
            self.assertEqual(json.loads(state.read_text(encoding="utf-8"))["dataContractVersion"], 3)

    def test_derived_session_retains_usage_identity_after_slot_deletion(self):
        row = usage_row() | {"accountSlotId": "deleted-slot", "accountLabel": "Deleted", "sync": {"version": 1, "originMachineId": "machine", "accountId": "v3:stable-account"}}

        sessions = monitor_token_ledger.token_sessions_from_ledger([row])

        self.assertEqual(sessions[0]["accountSlotId"], "deleted-slot")
        self.assertEqual(sessions[0]["usageAccountId"], "v3:stable-account")

    def test_quota_append_recovers_only_torn_tail_and_fsyncs(self):
        with workspace_directory() as directory:
            path = Path(directory) / "quota.jsonl"
            path.write_bytes(json.dumps(quota_row("2026-01-01T00:00:00Z"), separators=(",", ":")).encode() + b"\n{\"checkedAt\":")
            sample = quota_row("2026-01-01T00:01:00Z")
            with mock.patch("monitor_history.os.fsync") as fsync:
                self.assertTrue(monitor_history.append_quota_history_sample(path, sample))
            self.assertEqual([row["checkedAt"] for row in monitor_history.load_quota_history(path)], ["2026-01-01T00:00:00Z", "2026-01-01T00:01:00Z"])
            self.assertTrue(path.read_bytes().endswith(b"\n"))
            fsync.assert_called_once()

    def test_ledger_load_and_append_recover_torn_tail(self):
        with workspace_directory() as directory:
            path = Path(directory) / "ledger.jsonl"
            first, second = usage_row(), usage_row() | {"eventId": "event-2", "occurredAt": "2026-01-01T00:01:00Z"}
            path.write_bytes(json.dumps(first, separators=(",", ":")).encode() + b"\n{\"schemaVersion\":2")
            self.assertEqual(monitor_token_ledger.load_token_ledger(path), [first])
            monitor_token_ledger.append_token_ledger(path, [second])
            self.assertEqual(monitor_token_ledger.load_token_ledger(path), [first, second])

    def test_new_ledger_rows_persist_provenance_at_creation(self):
        with workspace_directory() as directory:
            path = Path(directory) / "ledger.jsonl"
            event = {"eventId": "session:1", "sessionId": "session", "checkedAt": "2026-08-01T00:00:00Z", "model": "gpt-5.6-luna", "serviceTier": "default", "tokens": {"input": 10, "cachedInput": 0, "cacheWriteInput": 0, "output": 0}}
            monitor_token_ledger.sync_token_ledger(path, [], [event], "account", "Account", record_provenance=lambda row: row | {"sync": {"version": 1, "originMachineId": "machine", "accountId": "usage-account"}})

            rows = monitor_token_ledger.load_token_ledger(path)
            self.assertEqual(rows[0]["sync"], {"version": 1, "originMachineId": "machine", "accountId": "usage-account"})
            self.assertEqual(monitor_token_ledger.token_sessions_from_ledger(rows)[0]["usageAccountId"], "usage-account")

    def test_state_replace_failure_preserves_previous_state(self):
        with workspace_directory() as directory:
            path = Path(directory) / "state.json"
            path.write_text('{"previous":true}\n', encoding="utf-8")
            with mock.patch("monitor_history.os.replace", side_effect=OSError("replace failed")), self.assertRaises(OSError):
                monitor_history.write_state(path, {"updatedAt": "now"})
            self.assertEqual(path.read_text(encoding="utf-8"), '{"previous":true}\n')

    def test_v3_normalizes_ledger_and_rebuildable_derived_files(self):
        with workspace_directory() as directory:
            root = Path(directory)
            history, quota, sessions, ledger, samples = (root / name for name in ("old-history.jsonl", "custom-quota.jsonl", "sessions.jsonl", "custom-ledger.jsonl", "samples.jsonl"))
            dashboard_cache, sync_cache = root / "custom-dashboard.json", root / "custom-sync.json"
            (root / monitor_auto_update.DATA_MIGRATION_STATE_FILENAME).write_text('{"dataContractVersion":2}\n', encoding="utf-8")
            history.write_text('{"derived":true}\n', encoding="utf-8")
            dashboard_cache.write_text("{}", encoding="utf-8")
            sync_cache.write_text('{"keep":"sync migration owns this"}', encoding="utf-8")
            quota.write_text(json.dumps(quota_row("2026-01-01T00:00:00Z")) + "\n", encoding="utf-8")
            old_usage = usage_row(1) | {"pricingId": "old-price"}
            price = {"schemaVersion": 1, "recordType": "priceEpoch", "pricingId": "old-price", "rates": {}}
            ledger.write_text("\n".join(json.dumps(row) for row in (price, old_usage)) + "\n", encoding="utf-8")

            self.assertEqual(monitor_auto_update.migrate_history_data(root, history, quota, sessions, ledger, samples, 3, dashboard_cache_path=dashboard_cache, usage_sync_cache_path=sync_cache), [3])
            migrated = monitor_token_ledger.load_token_ledger(ledger)
            self.assertEqual(len(migrated), 1)
            self.assertEqual(migrated[0]["schemaVersion"], 2)
            self.assertNotIn("pricingId", migrated[0])
            self.assertFalse(history.exists())
            self.assertFalse(dashboard_cache.exists())
            self.assertTrue(sync_cache.exists())

    def test_interrupted_migration_restores_original_before_retry(self):
        with workspace_directory() as directory:
            root = Path(directory)
            history, quota, sessions, ledger, samples = (root / name for name in ("history.jsonl", "quota.jsonl", "sessions.jsonl", "ledger.jsonl", "samples.jsonl"))
            state = root / monitor_auto_update.DATA_MIGRATION_STATE_FILENAME
            state.write_text('{"dataContractVersion":2}\n', encoding="utf-8")
            quota.write_text(json.dumps(quota_row("2026-01-01T00:00:00Z")) + "\n", encoding="utf-8")
            backup = root / ".quota.jsonl.backup"
            backup.write_bytes(quota.read_bytes())
            quota.write_text('{"broken":', encoding="utf-8")
            paths = {
                "history": history, "quota_history": quota, "token_session_history": sessions, "token_ledger": ledger, "sample_log": samples,
                "runtime_state": root / "usage_monitor_state.json", "dashboard_cache": root / "usage_monitor_dashboard_cache.json",
                "usage_sync_cache": root / "usage_monitor_sync_cache.json", "cloud_state": root / "cloud-state.json",
            }
            (root / monitor_auto_update.DATA_MIGRATION_JOURNAL_FILENAME).write_text(json.dumps({
                "version": 3, "existing": ["quota_history"], "backups": {"quota_history": str(backup.resolve())},
                "targets": {name: str(path.resolve()) for name, path in paths.items()},
            }), encoding="utf-8")

            self.assertEqual(monitor_auto_update.migrate_history_data(root, history, quota, sessions, ledger, samples, 3), [3])
            self.assertEqual(monitor_history.load_quota_history(quota)[0]["checkedAt"], "2026-01-01T00:00:00Z")
            self.assertFalse((root / monitor_auto_update.DATA_MIGRATION_JOURNAL_FILENAME).exists())


if __name__ == "__main__":
    unittest.main()
