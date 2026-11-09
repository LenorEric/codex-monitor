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
from monitor_sampling import sample_rows, stored_points
from monitor_usage_sync import UsageDataStore, add_record_provenance, record_key, v4_logical_hash, v4_record_envelope


def reading(minute, percent, *, account="v3:a", reset=1893520800, seven=None, plan="plus"):
    row = {"checkedAt": datetime.fromtimestamp(1893510000 + minute * 60, timezone.utc).isoformat(), "accountSlotId": account,
        "accountLabel": account, "windows": {"5h": {"usedPercent": percent, "resetAt": reset, "plan": plan}}}
    if seven is not None:
        row["windows"]["7d"] = {"usedPercent": seven, "resetAt": 1894114800, "plan": plan}
    return add_record_provenance("quota", row, "device", account)


class SamplingTests(unittest.TestCase):
    def test_same_time_saved_values_restore_in_ascending_order_without_reset_metadata(self):
        rows = []
        for value in (30, 10, 20):
            row = reading(0, value)
            row["quotaRunVersion"] = 1
            row["sync"]["recordId"] = str(100 - value)
            row["windows"]["5h"].update(continuous=value, multiplier=1, firstAt=1893510000, lastAt=1893510000)
            rows.append(row)
        for source in (rows, list(reversed(rows))):
            states = {}
            points = sample_rows(source, states)
            self.assertEqual([point["fiveHour"]["raw"] for point in points], [10, 20, 30])
            self.assertEqual(states["v3:a"]["fiveHour"]["raw"], 30)

    def values(self, values):
        return [point["fiveHour"]["continuous"] for point in sample_rows([reading(index, value) for index, value in enumerate(values)])]

    def test_decrease_protection_accepts_equal_or_increasing_followup(self):
        for values, expected in (([40, 20, 20], [40, None, 20]), ([40, 20, 21], [40, None, 21]),
                ([40, 20, 19, 19], [40, None, None, 19]), ([40, 20, 39], [40, None, 39]), ([40, 20, 45], [40, None, 45]),
                ([40, 39, 39], [40, None, 39]), ([40, 0, 0], [40, None, 0]), ([1, 99], [1, 99])):
            with self.subTest(values=values):
                self.assertEqual(self.values(values), expected)

    def test_reset_comparison_is_strict_for_both_periods(self):
        for label, output, period in (("5h", "fiveHour", 18000), ("7d", "sevenDay", 604800)):
            for offset, expected in ((-1, None), (0, None), (1, 20)):
                with self.subTest(label=label, offset=offset):
                    rows = [reading(0, 40), reading(1, 20)]
                    rows[0]["windows"] = {label: {"usedPercent": 40, "resetAt": 1893510000 + period - 7200, "plan": "plus"}}
                    rows[1]["windows"] = {label: {"usedPercent": 20, "resetAt": 1893510000 + period + offset, "plan": "plus"}}
                    self.assertEqual(sample_rows(rows)[-1][output]["continuous"], expected)

    def test_reset_boundary_for_each_window_preserves_raw_and_state(self):
        for label, name, period in (("5h", "fiveHour", 18000), ("7d", "sevenDay", 604800)):
            with self.subTest(label=label):
                rows = [reading(0, 90), reading(120, 10)]
                rows[0]["windows"] = {label: {"usedPercent": 90, "resetAt": 1893510000 + period, "plan": "pro"}}
                rows[1]["windows"] = {label: {"usedPercent": 10, "resetAt": 1893513600 + period, "plan": "pro"}}
                original, states = copy.deepcopy(rows), {}
                points = sample_rows(rows, states=states)
                self.assertEqual([point[name]["continuous"] for point in points], [90, 0, 10])
                self.assertEqual(points[1]["timestamp"], 1893513600)
                self.assertEqual(points[1]["resetWindow"], name)
                self.assertEqual(points[1][name]["plan"], "pro")
                self.assertEqual(states["v3:a"][name]["raw"], 10)
                self.assertEqual(rows, original)

    def test_reset_boundary_requires_nonzero_accepted_value_and_valid_time(self):
        for initial, reset, plan in ((0, 1893531600, "plus"), (90, None, "plus"), (90, 1893520800, "plus"),
                (90, 1893517200, "plus"), (90, 1893535201, "plus"), (90, 1893531600, "pro")):
            with self.subTest(initial=initial, reset=reset, plan=plan):
                self.assertFalse(any(point.get("resetWindow") for point in sample_rows([reading(0, initial), reading(120, 10, reset=reset, plan=plan)])))
        # Forward reset times also establish a zero baseline when usage has already overtaken the previous reading.
        self.assertEqual([point["fiveHour"]["continuous"] for point in sample_rows([reading(0, 10), reading(120, 20, reset=1893531600)])], [10, 0, 20])

    def test_supplied_forward_reset_example_and_plateau_timestamp(self):
        self.assertEqual(sample_rows([reading(0, 40), reading(1, 20, reset=1893531600)])[1]["fiveHour"]["continuous"], 20)
        rows = [reading(0, 40), reading(60, 40), reading(61, 20, reset=1893531600)]
        self.assertIsNone(sample_rows(rows)[2]["fiveHour"]["continuous"])

    def test_no_due_reset_or_backward_reset_exceptions(self):
        for reset in (1893520800, 1893517200):
            points = sample_rows([reading(0, 40), reading(240, 20, reset=reset), reading(241, 20, reset=reset)])
            self.assertEqual([point["fiveHour"]["continuous"] for point in points], [40, None, 20])

    def test_accounts_windows_plans_and_missing_are_independent(self):
        rows = [reading(0, 40, seven=10), reading(1, 20, seven=11), reading(2, None), reading(3, 20, seven=12)]
        points = sample_rows(rows)
        self.assertEqual([point["fiveHour"]["continuous"] for point in points], [40, None, None, 20])
        self.assertEqual([point["sevenDay"]["continuous"] for point in points], [10, 11, None, 12])
        self.assertEqual(sample_rows([reading(0, 40), reading(1, 20, plan="pro")])[1]["fiveHour"]["continuous"], 20)
        self.assertEqual(sample_rows([reading(0, 40), reading(1, 20, account="v3:b")])[1]["fiveHour"]["continuous"], 20)

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
