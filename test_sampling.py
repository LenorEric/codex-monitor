import copy
import json
import shutil
import uuid
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from monitor_dashboard import UsageDashboardState
from monitor_canonical import CanonicalIndex as ProcessingIndex
from monitor_quota import compact_quota
from monitor_usage_sync import UsageDataStore, add_record_provenance, record_key, v4_logical_hash, v4_record_envelope


def reading(minute, percent, *, account="v3:a", reset=1893520800, seven=None, plan="plus"):
    row = {"checkedAt": datetime.fromtimestamp(1893510000 + minute * 60, timezone.utc).isoformat(), "accountSlotId": account,
        "accountLabel": account, "windows": {"5h": {"usedPercent": percent, "resetAt": reset, "plan": plan}}}
    if seven is not None:
        row["windows"]["7d"] = {"usedPercent": seven, "resetAt": 1894114800, "plan": plan}
    return add_record_provenance("quota", row, "device", account)


class SamplingTests(unittest.TestCase):









    def test_ignore_remaining_duration_and_isolate_malformed_window(self):
        response = {"plan_type": "plus", "rate_limit": {"primary_window": {"limit_window_seconds": 18000, "reset_at": 1893520800, "used_percent": 40},
            "secondary_window": {"limit_window_seconds": 604800, "reset_at": 1894114800, "used_percent": 11}}}
        for remaining in (None, "invalid", -10, {}, 999999999):
            response["rate_limit"]["primary_window"]["reset_after_seconds"] = remaining
            self.assertEqual(compact_quota(response)["windows"]["5h"]["values"]["reset_at"], 1893520800)
        response["rate_limit"]["primary_window"]["reset_at"] = "invalid"
        quota = compact_quota(response)
        self.assertTrue(quota["windows"]["5h"]["unavailable"])
        self.assertEqual(quota["windows"]["7d"]["values"]["used_percent"], 11)
        self.assertIn("reset_at", quota["errors"][0])



if __name__ == '__main__':
    unittest.main()
