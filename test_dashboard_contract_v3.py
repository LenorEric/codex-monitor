import unittest

from monitor_dashboard import _dashboard_display_data


class DashboardContractTests(unittest.TestCase):
    def test_account_rename_updates_rebuilt_sessions_without_mutating_ledger(self):
        ledger = [{"schemaVersion": 1, "recordType": "usage", "eventId": "s:1", "sessionId": "s", "occurredAt": "2030-01-01T00:00:00Z", "accountSlotId": "a", "accountLabel": "Original", "rawModel": "m", "tokens": {}, "cost": {"totalCostUsd": 2}}]
        accounts = {"activeAccountId": "a", "items": [{"id": "a", "label": "Renamed", "usageAccountId": "stable-a"}]}

        display = _dashboard_display_data([], ledger, accounts)

        self.assertEqual(display["tokenSessions"][0]["accountLabel"], "Renamed")
        self.assertEqual(display["tokenSessions"][0]["byModel"]["m"]["cost"]["totalCostUsd"], 2)
        self.assertEqual(ledger[0]["accountLabel"], "Original")


if __name__ == "__main__":
    unittest.main()
