import json
import unittest
import threading
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from monitor_accounts import AccountManager
from monitor_dashboard import UsageDashboardState, dashboard_account_status


class UsageAccountPollingTests(unittest.TestCase):
    def state(self, active="a1"):
        state = UsageDashboardState.__new__(UsageDashboardState)
        slots = {"a1": "a", "a2": "a", "b1": "b", "b2": "b", "c1": "c"}
        state.args = SimpleNamespace(local_only=False)
        state.opener = object()
        state.accounts = SimpleNamespace(
            status=lambda: {"activeAccountId": active, "slotOrder": ["b1", "b2", "a1", "a2", "c1"]},
            attribution_timeline=lambda: [{"accountSlotId": slot} for slot in ("a1", "b2", "b1", "a2")],
            inactive_ready_credentials=lambda: [{"id": slot, "label": slot, "data": json.dumps({"tokens": {}}).encode()} for slot in slots if slot != active],
        )
        state.cloud = SimpleNamespace(usage_account_id=lambda slot: slots[slot])
        state.inactive_account_poll_started_at = {}
        return state

    def test_duplicates_are_polled_once_using_latest_surviving_slot(self):
        state = self.state()
        self.assertEqual([(r["usageAccountId"], r["id"]) for r in state._inactive_usage_credentials()], [("b", "b1"), ("c", "c1")])
        state.inactive_account_poll_started_at["b"] = 100
        state.inactive_account_poll_started_at["c"] = 100
        self.assertEqual(state.inactive_account_poll_wait_seconds(101), 599)
        with mock.patch.object(state, "_poll_inactive_account") as poll:
            self.assertEqual(state.poll_due_inactive_accounts(101), 0)
            poll.assert_not_called()

    def test_switching_duplicate_slots_preserves_account_timer(self):
        state = self.state("b2")
        state.inactive_account_poll_started_at["a"] = 100
        state.inactive_account_poll_started_at["c"] = 100
        self.assertEqual({r["usageAccountId"] for r in state._inactive_usage_credentials()}, {"a", "c"})
        self.assertEqual(state.poll_due_inactive_accounts(101), 0)

    def test_one_request_per_inactive_account_and_shared_failure_health(self):
        state = self.state()
        state.lock = threading.RLock()
        state.inactive_account_poll_errors = {}
        state.usage_data = SimpleNamespace(ingest_local=mock.Mock())
        with mock.patch.object(state, "_poll_inactive_account", side_effect=[RuntimeError("offline"), None]) as poll, \
                mock.patch.object(state, "_maintain_quota_retention"), mock.patch.object(state, "_signal_dashboard_cache"), mock.patch("builtins.print"):
            self.assertEqual(state.poll_due_inactive_accounts(100), 1)
            self.assertEqual([call.args[0]["id"] for call in poll.call_args_list], ["b1", "c1"])
            self.assertEqual(set(state.inactive_account_poll_errors), {"b"})
            self.assertEqual(state.poll_due_inactive_accounts(101), 0)

    def test_deletion_persists_identity_without_changing_usage_order(self):
        directory = Path.cwd() / f".test-account-order-{uuid.uuid4().hex}"
        directory.mkdir()
        self.addCleanup(shutil.rmtree, directory)
        manager = AccountManager(Path(directory) / "auth.json", Path(directory) / "accounts")
        manager.manifest.update({"activeAccountId": "a", "accounts": [{"id": slot, "label": slot.upper(), "ready": False} for slot in ("a", "b", "c")],
            "activationHistory": [{"accountSlotId": slot, "accountLabel": slot.upper(), "checkedAt": f"2030-01-01T00:00:0{i}Z"} for i, slot in enumerate(("b", "c", "a"))]})
        manager._save_manifest()
        manager.cloud = SimpleNamespace(usage_account_id=lambda slot: "u", ensure_no_pending_account_operation=lambda: None, account_transition_targets=lambda *args: [])
        manager.delete("c")
        restored = AccountManager(manager.auth_path, manager.root)
        self.assertEqual([row["accountSlotId"] for row in restored.attribution_timeline()], ["b", "c", "a"])
        self.assertEqual(restored.attribution_timeline()[1]["usageAccountId"], "u")

    def test_deleted_slots_do_not_reorder_remaining_names(self):
        history = [{"accountSlotId": slot, "usageAccountId": "u", "accountLabel": slot} for slot in ("a", "b", "c")]
        status = {"activeAccountId": "other", "items": [{"id": slot, "label": slot.upper()} for slot in ("a", "b")]}
        self.assertEqual(dashboard_account_status(status, lambda slot: "u", history, ["c", "b", "a"])["usageGroups"][0]["label"], "B")
        status["items"] = []
        self.assertEqual(dashboard_account_status(status, lambda slot: "u", history, ["c", "b", "a"])["usageGroups"][0]["label"], "a")
        manager = AccountManager.__new__(AccountManager)
        normalized = manager._normalized_activation_history({"accounts": [], "activationHistory": [dict(row, checkedAt=f"2030-01-01T00:00:0{i}Z") for i, row in enumerate(history)]})
        self.assertEqual([r["accountSlotId"] for r in normalized], ["a", "b", "c"])
        self.assertTrue(all(r["usageAccountId"] == "u" for r in normalized))

    def test_switching_or_adding_a_slot_does_not_promote_it(self):
        status = {"activeAccountId": "new", "items": [{"id": slot, "label": slot} for slot in ("old", "used", "new")]}
        history = [{"accountSlotId": slot} for slot in ("old", "used", "new")]
        self.assertEqual(dashboard_account_status(status, lambda slot: "u", history)["usageGroups"][0]["label"], "old")
        self.assertEqual(dashboard_account_status(status, lambda slot: "u", history, ["used", "old", "new"])["usageGroups"][0]["label"], "used")
        self.assertEqual(dashboard_account_status(status, lambda slot: "u", history, ["new", "used", "old"])["usageGroups"][0]["label"], "new")

    def test_local_order_persists_and_only_new_active_cost_promotes(self):
        directory = Path.cwd() / f".test-saved-order-{uuid.uuid4().hex}"
        directory.mkdir()
        self.addCleanup(shutil.rmtree, directory)
        manager = AccountManager(directory / "auth.json", directory / "accounts")
        manager.manifest.update({"activeAccountId": "b", "accounts": [{"id": slot, "label": slot.upper(), "ready": False} for slot in ("a", "b", "c")]})
        manager.cloud = SimpleNamespace(usage_account_id=lambda slot: "u", ensure_no_pending_account_operation=lambda: None, account_transition_targets=lambda *args: [])
        manager._save_manifest()
        self.assertEqual(manager.manifest["slotOrder"], ["a", "b", "c"])
        cost = {"recordType": "usage", "accountSlotId": "b", "cost": {"totalCostUsd": 1}}
        self.assertFalse(manager.promote_active_slot_on_cost(cost | {"accountSlotId": "a"}))
        self.assertFalse(manager.promote_active_slot_on_cost(cost | {"recordType": "legacyBaseline"}))
        self.assertTrue(manager.promote_active_slot_on_cost(cost))
        self.assertEqual(manager.manifest["slotOrder"], ["b", "a", "c"])
        with mock.patch.object(manager, "_save_manifest", side_effect=AssertionError("unnecessary save")):
            self.assertFalse(manager.promote_active_slot_on_cost(cost))
        restored = AccountManager(manager.auth_path, manager.root)
        self.assertEqual(restored.manifest["slotOrder"], ["b", "a", "c"])
        manager.manifest["activeAccountId"] = "c"
        manager._record_activation("c")
        manager._save_manifest()
        self.assertEqual(manager.manifest["slotOrder"], ["b", "a", "c"])
        with mock.patch.object(manager, "_save_manifest", side_effect=OSError("disk full")), self.assertRaises(OSError):
            manager.promote_active_slot_on_cost(cost | {"accountSlotId": "c"})
        self.assertEqual(manager.manifest["slotOrder"], ["b", "a", "c"])
        self.assertTrue(manager.promote_active_slot_on_cost(cost | {"accountSlotId": "c", "cost": {"totalCostUsd": 0}}))
        manager.delete("b")
        self.assertEqual(manager.manifest["slotOrder"], ["c", "a"])
        manager.manifest["accounts"].append({"id": "d", "label": "D", "ready": False})
        manager._save_manifest()
        self.assertEqual(manager.manifest["slotOrder"], ["c", "a", "d"])
        self.assertEqual(AccountManager(manager.auth_path, manager.root).manifest["slotOrder"], ["c", "a", "d"])
