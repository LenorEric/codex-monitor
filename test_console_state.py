import copy
import shutil
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import monitor_dashboard
from monitor_events import delta_state_needs_hydration, process_sample_delta_events
from monitor_processing import ProcessingIndex
from test_monitor_codex_usage import event_sample


class ConsoleStateTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-console-state-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(self.cleanup)
        self.index = ProcessingIndex(self.root / "index.sqlite3")

    def cleanup(self):
        if self.root.resolve().parent != Path.cwd().resolve() or not self.root.name.startswith(".test-console-state-"):
            raise ValueError("Unexpected test directory")
        shutil.rmtree(self.root)

    @staticmethod
    def complete_state():
        return {"windows": {label: {"baselinePercent": 5, "baselineCostUsd": 1, "baselineResetAt": None, "baselinePlan": "unknown", "baselineMultiplier": 1,
            "previousCostUsd": 1, "cumulativePercent": 10, "cumulativeCostUsd": 20, "models": {"model-a": {"cumulativePercent": 10, "cumulativeCostUsd": 20}}} for label in ("5h", "7d")}}

    def startup(self, cached, history_forbidden=False, previous=True):
        with self.index.transaction() as db:
            self.index.put(db, "consoleState", cached)
        state = monitor_dashboard.UsageDashboardState.__new__(monitor_dashboard.UsageDashboardState)
        state.usage_data = SimpleNamespace(index=self.index, datasets=mock.Mock(side_effect=AssertionError("warm startup replayed history")) if history_forbidden else mock.Mock(return_value=([], [])))
        state.runtime_state = {"lastSample": event_sample("2030-01-01T01:00:00Z", five_hour=20, cost=20)} if previous else {}
        costs = {"fiveHour": [{"checkedAt": "2030-01-01T00:00:00Z", "deltaPercent": 7, "totalCostUsd": 14, "costByModelUsd": {"model-a": 14}}], "sevenDay": []}
        with mock.patch.object(monitor_dashboard, "dashboard_accounts", return_value={"items": []}), mock.patch.object(monitor_dashboard, "dashboard_cost_usage", return_value=costs):
            state.restore_console_event_state()
        return state

    def test_partial_saved_cache_rebuilds_totals_and_does_not_replay_last_delta(self):
        cached = self.complete_state()
        for name in ("cumulativePercent", "cumulativeCostUsd", "models"):
            cached["windows"]["5h"].pop(name)
        state = self.startup(cached)
        self.assertEqual(state.console_event_state["windows"]["5h"]["cumulativePercent"], 7)
        self.assertEqual(state.console_event_state["windows"]["5h"]["cumulativeCostUsd"], 14)
        self.assertEqual(state.console_event_state["windows"]["5h"]["models"]["model-a"]["cumulativePercent"], 7)
        self.assertEqual(state.console_event_state["windows"]["5h"]["baselinePercent"], 20)
        self.assertEqual(state.console_event_state["_pendingCostIntervals"], [])
        self.assertEqual(state.console_event_state["_specialEvents"], [])
        state.usage_data.datasets.assert_called_once_with("local")
        with self.index.transaction() as db:
            saved = self.index.get(db, "consoleState")
        self.assertFalse(delta_state_needs_hydration(saved))
        restarted = self.startup(saved, history_forbidden=True)
        self.assertEqual(restarted.console_event_state["windows"]["5h"]["cumulativePercent"], 7)

    def test_valid_cache_keeps_totals_without_history_replay_or_double_counting(self):
        state = self.startup(self.complete_state(), history_forbidden=True)
        self.assertEqual(state.console_event_state["windows"]["5h"]["cumulativePercent"], 10)
        self.assertEqual(state.console_event_state["windows"]["5h"]["cumulativeCostUsd"], 20)
        self.assertEqual(state.console_event_state["windows"]["5h"]["baselinePercent"], 20)
        self.assertEqual(state.console_event_state["_pendingCostIntervals"], [])

    def test_missing_or_malformed_cache_can_recover_without_a_previous_sample(self):
        for cached in (None, {}, {"windows": []}, {"windows": {"5h": None, "7d": {}}}):
            with self.subTest(cached=cached):
                state = self.startup(cached, previous=False)
                self.assertFalse(delta_state_needs_hydration(state.console_event_state))
                self.assertEqual(state.console_event_state["windows"]["5h"]["cumulativePercent"], 7)

    def test_incomplete_model_totals_also_trigger_recovery(self):
        cached = self.complete_state()
        cached["windows"]["5h"]["models"]["model-a"].pop("cumulativeCostUsd")
        self.assertTrue(delta_state_needs_hydration(cached))
        state = self.startup(cached)
        self.assertEqual(state.console_event_state["windows"]["5h"]["models"]["model-a"]["cumulativeCostUsd"], 14)

    def test_incremental_unavailable_window_retains_totals_and_rebases_on_return(self):
        state = {}
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=10), None)
        first = process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=6, cost=12), None)
        self.assertEqual(first[0]["cumulativePercent"], 1)
        models = copy.deepcopy(state["windows"]["5h"]["models"])
        unavailable = event_sample("2030-01-01T02:00:00Z", cost=15)
        unavailable["windows"]["5h"] = {"unavailable": True}
        self.assertEqual(process_sample_delta_events(state, unavailable, None), [])
        self.assertEqual(state["windows"]["5h"]["cumulativePercent"], 1)
        self.assertEqual(state["windows"]["5h"]["models"], models)
        self.assertNotIn("baselinePercent", state["windows"]["5h"])
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T03:00:00Z", five_hour=8, cost=18), None), [])
        last = process_sample_delta_events(state, event_sample("2030-01-01T04:00:00Z", five_hour=9, cost=20), None)
        self.assertEqual(last[0]["cumulativePercent"], 2)
        self.assertEqual(last[0]["cumulativeCostUsd"], 4)
        self.assertFalse(delta_state_needs_hydration(state))


if __name__ == "__main__":
    unittest.main()
