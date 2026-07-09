import http.server
import unittest
import urllib.error
from unittest import mock
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import monitor_common
import monitor_history
from monitor_codex_usage import (
    append_capped_jsonl,
    append_history,
    apply_runtime_cost_measurement,
    build_delta_event_series,
    calculate_token_costs,
    compact_delta_event,
    compact_history,
    compact_monitor_state,
    compact_quota,
    compact_quota_for_debug,
    collect_usage_sample,
    dashboard_html,
    dashboard_display,
    DashboardHTTPServer,
    derive_history_events,
    empty_token_totals,
    format_ratio_warning,
    format_special_event,
    format_valid_delta_event,
    is_client_disconnect,
    load_history,
    make_history_sample,
    new_valid_delta_events,
    poll_sleep_seconds,
    process_sample_delta_events,
    quota_history_windows,
    request_json,
    reset_runtime_baselines,
    sample_debug_log_row,
    UsageError,
    write_history,
)


class MonitorCodexUsageTests(unittest.TestCase):
    def test_poll_sleep_seconds_counts_from_acquire_start(self):
        self.assertEqual(poll_sleep_seconds(100, 60, now=112), 48)

    def test_poll_sleep_seconds_returns_zero_after_interval_overrun(self):
        self.assertEqual(poll_sleep_seconds(100, 60, now=175), 0)

    def test_poll_sleep_seconds_keeps_minimum_interval_of_one_second(self):
        self.assertEqual(poll_sleep_seconds(100, 0, now=100.25), 0.75)

    def test_quota_extraction_finds_5h_and_7d_windows(self):
        compact = compact_quota({
            "rate_limit": {
                "primary_window": {
                    "used_percent": 42.5,
                    "limit_window_seconds": 18000,
                    "reset_at": 1893456000,
                    "planType": "plus",
                },
                "secondary_window": {
                    "used_percent": 61,
                    "limit_window_seconds": 604800,
                    "reset_at": 1893888000,
                    "planType": "pro_lite",
                },
            },
        })

        windows = quota_history_windows({"usage": compact})

        self.assertTrue(compact["complete"])
        self.assertEqual(windows["5h"]["usedPercent"], 42.5)
        self.assertEqual(windows["7d"]["usedPercent"], 61)
        self.assertEqual(windows["5h"]["resetAt"], "2030-01-01T00:00:00Z")
        self.assertEqual(windows["5h"]["plan"], "plus")
        self.assertEqual(windows["7d"]["plan"], "pro_lite")
        self.assertEqual(windows["7d"]["planMultiplier"], 5.0)

    def test_quota_extraction_uses_parent_plan_as_window_fallback(self):
        compact = compact_quota({
            "planType": "pro",
            "rate_limit": {
                "primary_window": {
                    "used_percent": 3,
                    "limit_window_seconds": 18000,
                },
                "secondary_window": {
                    "used_percent": 4,
                    "limit_window_seconds": 604800,
                },
            },
        })

        windows = quota_history_windows({"usage": compact})

        self.assertEqual(windows["5h"]["plan"], "pro")
        self.assertEqual(windows["5h"]["planMultiplier"], 20.0)
        self.assertEqual(windows["7d"]["plan"], "pro")
        self.assertEqual(windows["7d"]["planMultiplier"], 20.0)

    def test_collect_usage_sample_loads_auth_for_remote_usage(self):
        original_load_json = monitor_history.load_json
        original_fetch_usage = monitor_history.fetch_usage
        try:
            monitor_history.load_json = lambda path: {"authPath": str(path)}
            monitor_history.fetch_usage = lambda auth, opener, auth_path, timeout, debug, retries=3: {
                "endpoint": "test",
                "status": 200,
                "usage": compact_quota({
                    "rate_limit": {
                        "primary_window": {"used_percent": 12, "limit_window_seconds": 18000},
                        "secondary_window": {"used_percent": 34, "limit_window_seconds": 604800},
                    },
                }),
            }

            sample = collect_usage_sample(SimpleNamespace(local_only=False, no_token_scan=True, auth=Path("auth.json"), timeout=10), object(), None)

            self.assertEqual(sample["windows"]["5h"]["usedPercent"], 12)
            self.assertEqual(sample["windows"]["7d"]["usedPercent"], 34)
            self.assertEqual(sample["errors"], {})
        finally:
            monitor_history.load_json = original_load_json
            monitor_history.fetch_usage = original_fetch_usage

    def test_collect_usage_sample_arbitrates_weird_percent_and_accepts_stable_retry(self):
        original_load_json = monitor_history.load_json
        original_fetch_usage = monitor_history.fetch_usage
        calls = []
        try:
            monitor_history.load_json = lambda path: {"authPath": str(path)}

            def fake_fetch_usage(auth, opener, auth_path, timeout, debug, retries=3):
                calls.append(1)
                percent = 70 if len(calls) == 1 else 71
                debug.update({"rawResponse": remote_identity("user-old") | {"sample": len(calls)}})
                return {
                    "endpoint": "test",
                    "status": 200,
                    "usage": compact_quota({
                        "rate_limit": {
                            "primary_window": {"used_percent": percent, "limit_window_seconds": 18000, "planType": "plus"},
                            "secondary_window": {"used_percent": 10, "limit_window_seconds": 604800, "planType": "plus"},
                        },
                    }),
                }

            monitor_history.fetch_usage = fake_fetch_usage

            sample = collect_usage_sample(
                SimpleNamespace(local_only=False, no_token_scan=True, auth=Path("auth.json"), timeout=10),
                object(),
                None,
                runtime_state={
                    "remoteUsageIdentity": remote_identity("user-old"),
                    "windows": {
                        "5h": {"baselinePercent": 10, "baselinePlan": "plus", "baselineMultiplier": 1.0},
                        "7d": {"baselinePercent": 10, "baselinePlan": "plus", "baselineMultiplier": 1.0},
                    },
                },
            )

            self.assertEqual(len(calls), 2)
            self.assertEqual(sample["windows"]["5h"]["usedPercent"], 71)
            self.assertEqual(sample["remoteUsage"]["rawResponse"]["sample"], 2)
            self.assertEqual(sample["remoteUsage"]["percentArbitration"]["acceptedResponse"], 2)
        finally:
            monitor_history.load_json = original_load_json
            monitor_history.fetch_usage = original_fetch_usage

    def test_collect_usage_sample_accepts_weird_percent_on_account_switch(self):
        original_load_json = monitor_history.load_json
        original_fetch_usage = monitor_history.fetch_usage
        calls = []
        try:
            monitor_history.load_json = lambda path: {"authPath": str(path)}

            def fake_fetch_usage(auth, opener, auth_path, timeout, debug, retries=3):
                calls.append(1)
                debug.update({"rawResponse": remote_identity("user-new")})
                return {
                    "endpoint": "test",
                    "status": 200,
                    "usage": compact_quota({
                        "rate_limit": {
                            "primary_window": {"used_percent": 80, "limit_window_seconds": 18000, "planType": "plus"},
                            "secondary_window": {"used_percent": 10, "limit_window_seconds": 604800, "planType": "plus"},
                        },
                    }),
                }

            monitor_history.fetch_usage = fake_fetch_usage

            sample = collect_usage_sample(
                SimpleNamespace(local_only=False, no_token_scan=True, auth=Path("auth.json"), timeout=10),
                object(),
                None,
                runtime_state={
                    "remoteUsageIdentity": remote_identity("user-old"),
                    "windows": {"5h": {"baselinePercent": 10, "baselinePlan": "plus", "baselineMultiplier": 1.0}},
                },
            )

            self.assertEqual(len(calls), 1)
            self.assertEqual(sample["windows"]["5h"]["usedPercent"], 80)
            self.assertNotIn("percentArbitration", sample["remoteUsage"])
        finally:
            monitor_history.load_json = original_load_json
            monitor_history.fetch_usage = original_fetch_usage

    def test_collect_usage_sample_raises_when_percent_arbitration_never_stabilizes(self):
        original_load_json = monitor_history.load_json
        original_fetch_usage = monitor_history.fetch_usage
        calls = []
        try:
            monitor_history.load_json = lambda path: {"authPath": str(path)}

            def fake_fetch_usage(auth, opener, auth_path, timeout, debug, retries=3):
                calls.append(1)
                debug.update({"rawResponse": remote_identity("user-old")})
                return {
                    "endpoint": "test",
                    "status": 200,
                    "usage": compact_quota({
                        "rate_limit": {
                            "primary_window": {"used_percent": 10 + len(calls) * 50, "limit_window_seconds": 18000, "planType": "plus"},
                            "secondary_window": {"used_percent": 10, "limit_window_seconds": 604800, "planType": "plus"},
                        },
                    }),
                }

            monitor_history.fetch_usage = fake_fetch_usage

            with self.assertRaises(UsageError):
                collect_usage_sample(
                    SimpleNamespace(local_only=False, no_token_scan=True, auth=Path("auth.json"), timeout=10),
                    object(),
                    None,
                    runtime_state={
                        "remoteUsageIdentity": remote_identity("user-old"),
                        "windows": {"5h": {"baselinePercent": 10, "baselinePlan": "plus", "baselineMultiplier": 1.0}},
                    },
                )

            self.assertEqual(len(calls), 5)
        finally:
            monitor_history.load_json = original_load_json
            monitor_history.fetch_usage = original_fetch_usage

    def test_request_json_retries_network_errors_without_limit(self):
        class SuccessfulResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def read(self):
                return b'{"ok": true}'

        class FailingOpener:
            def __init__(self):
                self.count = 0

            def open(self, request, timeout):
                self.count += 1
                if self.count <= 4:
                    raise urllib.error.URLError("temporary failure")
                return SuccessfulResponse()

        opener = FailingOpener()
        original_sleep = monitor_common.time.sleep
        try:
            monitor_common.time.sleep = lambda seconds: None
            status, data = request_json(opener, "GET", "https://example.invalid", {}, timeout=1, retries=2)

            self.assertEqual(status, 200)
            self.assertEqual(data, {"ok": True})
            self.assertEqual(opener.count, 5)
        finally:
            monitor_common.time.sleep = original_sleep

    def test_token_delta_uses_previous_persisted_sample(self):
        previous = {"totals": {"inputTokens": 10, "freshInputTokens": 9, "cachedInputTokens": 1, "outputTokens": 5, "totalTokens": 15, "requests": 1}}
        current = {"totals": {"inputTokens": 25, "freshInputTokens": 20, "cachedInputTokens": 5, "outputTokens": 15, "totalTokens": 40, "requests": 3}}

        sample = make_history_sample({"checkedAt": "2030-01-01T00:00:00Z", "tokenUsage": current}, previous)

        self.assertEqual(sample["tokenDelta"]["inputTokens"], 15)
        self.assertEqual(sample["tokenDelta"]["freshInputTokens"], 11)
        self.assertEqual(sample["tokenDelta"]["cachedInputTokens"], 4)
        self.assertEqual(sample["tokenDelta"]["outputTokens"], 10)
        self.assertEqual(sample["tokenDelta"]["totalTokens"], 25)
        self.assertEqual(sample["tokenDelta"]["requests"], 2)

    def test_cost_uses_uncached_cached_and_output_prices(self):
        costs = calculate_token_costs({
            "byModel": {
                "gpt-5.5": {
                    "freshInputTokens": 1_000_000,
                    "cachedInputTokens": 1_000_000,
                    "outputTokens": 1_000_000,
                },
            },
        })

        self.assertEqual(costs["inputCostUsd"], 5.0)
        self.assertEqual(costs["cachedInputCostUsd"], 0.5)
        self.assertEqual(costs["outputCostUsd"], 30.0)
        self.assertEqual(costs["totalCostUsd"], 35.5)

    def test_delta_event_series_records_only_percentage_increases(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12),
            event_sample("2030-01-01T01:00:00Z", five_hour=5, cost=13),
            event_sample("2030-01-01T02:00:00Z", five_hour=8, cost=15),
        ], "5h")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["cumulativePercent"], 0)
        self.assertEqual(events[0]["cumulativeCostUsd"], 0)
        self.assertEqual(events[1]["deltaPercent"], 3)
        self.assertEqual(events[1]["deltaCostUsd"], 3)
        self.assertEqual(events[1]["percentCostRatio"], 1)
        self.assertIsNone(events[1]["averagePercentCostRatio"])
        self.assertEqual(events[1]["cumulativePercent"], 3)
        self.assertEqual(events[1]["cumulativeCostUsd"], 3)

    def test_delta_event_series_counts_flat_cost_into_next_valid_pair(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12),
            event_sample("2030-01-01T01:00:00Z", five_hour=5, cost=13),
            event_sample("2030-01-01T02:00:00Z", five_hour=5, cost=14),
            event_sample("2030-01-01T03:00:00Z", five_hour=8, cost=16),
        ], "5h")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[1]["deltaPercent"], 3)
        self.assertEqual(events[1]["deltaCostUsd"], 4)
        self.assertEqual(events[1]["cumulativePercent"], 3)
        self.assertEqual(events[1]["cumulativeCostUsd"], 4)

    def test_delta_event_series_ignores_cost_while_percentage_is_at_100(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=99, cost=100),
            event_sample("2030-01-01T01:00:00Z", five_hour=100, cost=102),
            event_sample("2030-01-01T02:00:00Z", five_hour=100, cost=108),
            event_sample("2030-01-01T03:00:00Z", five_hour=101, cost=110),
        ], "5h")

        self.assertEqual([(event["deltaPercent"], event["deltaCostUsd"]) for event in events[1:]], [(1, 2), (1, 2)])
        self.assertEqual(events[-1]["cumulativeCostUsd"], 4)

    def test_delta_event_series_resets_percent_baseline_but_keeps_cost_baseline(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12),
            event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=15),
            event_sample("2030-01-01T02:00:00Z", five_hour=10, cost=18),
            event_sample("2030-01-01T03:00:00Z", five_hour=3, cost=21),
        ], "5h")

        self.assertEqual(events[-1]["deltaPercent"], 3)
        self.assertEqual(events[-1]["deltaCostUsd"], 3)
        self.assertEqual(events[-1]["cumulativePercent"], 8)
        self.assertEqual(events[-1]["cumulativeCostUsd"], 9)

    def test_delta_event_series_normalizes_pro_lite_percentage_to_plus_plan(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=3, cost=12, plan="pro_lite"),
            event_sample("2030-01-01T01:00:00Z", five_hour=4, cost=15, plan="pro_lite"),
        ], "5h")

        self.assertEqual(events[0]["normalizedPercent"], 15)
        self.assertEqual(events[1]["deltaPercent"], 5)
        self.assertEqual(events[1]["deltaCostUsd"], 3)
        self.assertEqual(events[1]["cumulativePercent"], 5)
        self.assertEqual(events[1]["cumulativeCostUsd"], 3)

    def test_delta_event_series_discards_account_type_switch_sample(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=8, plan="plus"),
            event_sample("2030-01-01T01:00:00Z", five_hour=30, cost=12, plan="pro_lite"),
            event_sample("2030-01-01T02:00:00Z", five_hour=33, cost=15, plan="pro_lite"),
        ], "5h")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["cumulativePercent"], 0)
        self.assertEqual(events[0]["cumulativeCostUsd"], 0)
        self.assertEqual(events[1]["deltaPercent"], 15)
        self.assertEqual(events[1]["deltaCostUsd"], 3)
        self.assertEqual(events[1]["cumulativePercent"], 15)
        self.assertEqual(events[1]["cumulativeCostUsd"], 3)

    def test_delta_event_series_does_not_count_account_switch_gap_cost(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=8, plan="plus"),
            event_sample("2030-01-01T01:00:00Z", five_hour=30, cost=12, plan="pro_lite"),
            event_sample("2030-01-01T02:00:00Z", five_hour=30, cost=14, plan="pro_lite"),
            event_sample("2030-01-01T03:00:00Z", five_hour=33, cost=17, plan="pro_lite"),
        ], "5h")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[1]["deltaPercent"], 15)
        self.assertEqual(events[1]["deltaCostUsd"], 5)
        self.assertEqual(events[1]["cumulativePercent"], 15)
        self.assertEqual(events[1]["cumulativeCostUsd"], 5)

    def test_delta_event_series_discards_low_cost_external_usage(self):
        events = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=10),
            event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=10),
            event_sample("2030-01-01T02:00:00Z", five_hour=9, cost=11),
        ], "5h")

        self.assertEqual(len(events), 2)
        self.assertEqual(events[1]["deltaPercent"], 1)
        self.assertEqual(events[1]["deltaCostUsd"], 1)

    def test_new_valid_delta_events_returns_only_new_pairs(self):
        before = [
            event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12),
            event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=15),
        ]
        after = [
            *before,
            event_sample("2030-01-01T02:00:00Z", five_hour=9, cost=18),
        ]

        events = new_valid_delta_events(before, after)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 3)

    def test_delta_event_console_format_includes_current_normalized_usage_and_plan(self):
        sample_row = event_sample("2030-01-01T01:00:00Z", five_hour=4, seven_day=2, cost=15, plan="pro_lite")
        event = build_delta_event_series([
            event_sample("2030-01-01T00:00:00Z", five_hour=3, cost=12, plan="pro_lite"),
            sample_row,
        ], "5h")[-1]

        text = format_valid_delta_event(event, sample_row)

        self.assertIn("2030-01-01T01:00:00Z (local " + datetime.fromisoformat("2030-01-01T01:00:00+00:00").astimezone().isoformat(timespec="seconds") + ")", text)
        self.assertIn("+5% / +$3", text)
        self.assertIn("ratio 1.66667%/$", text)
        self.assertIn("current 5h 20%", text)
        self.assertIn("7d 10%", text)
        self.assertIn("pro_lite 5x", text)

    def test_live_processing_records_only_compact_delta_pair(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=12), history), [])

        events = process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=15), history)
        history.extend(compact_delta_event(event) for event in events)

        self.assertEqual(history, [{"checkedAt": "2030-01-01T01:00:00Z", "window": "5h", "deltaPercent": 3.0, "deltaCostUsd": 3.0, "percentCostRatio": 1.0}])
        self.assertNotIn("windows", history[0])
        self.assertEqual(derive_history_events(history)["fiveHour"][0]["cumulativePercent"], 0)
        self.assertTrue(derive_history_events(history)["fiveHour"][0]["synthetic"])
        self.assertEqual(derive_history_events(history)["fiveHour"][1]["cumulativePercent"], 3)

    def test_live_processing_flags_percent_cost_ratio_deviation_for_both_windows(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, seven_day=5, cost=10), history)
        events = process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=8, seven_day=8, cost=13), history)
        history.extend(compact_delta_event(event) for event in events)

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=9, seven_day=9, cost=16), history)

        self.assertEqual([event["window"] for event in events], ["5h", "7d"])
        self.assertEqual([event["percentCostRatio"] for event in events], [0.33333333, 0.33333333])
        self.assertEqual([event["averagePercentCostRatio"] for event in events], [1.0, 1.0])
        self.assertTrue(all(event["ratioDeviationWarning"] for event in events))
        self.assertIn("percent/cost ratio 0.333333%/$ deviates from average 1%/$", format_ratio_warning(events[0]))

    def test_empty_compact_event_log_shows_zero_zero_baseline(self):
        events = derive_history_events([])

        self.assertEqual(events["fiveHour"][0]["cumulativePercent"], 0)
        self.assertEqual(events["fiveHour"][0]["cumulativeCostUsd"], 0)
        self.assertTrue(events["fiveHour"][0]["synthetic"])
        self.assertEqual(events["sevenDay"][0]["cumulativePercent"], 0)
        self.assertEqual(events["sevenDay"][0]["cumulativeCostUsd"], 0)
        self.assertTrue(events["sevenDay"][0]["synthetic"])

    def test_live_processing_discards_low_cost_delta_and_resets_baseline(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=10), history)
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=10), history), [])

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=9, cost=11), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 1)

    def test_live_processing_records_only_after_runtime_cost_baseline_is_ready(self):
        state = {}
        history = []
        first = event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=100)
        first["eventCostUsd"] = 0
        first["eventCostReady"] = False
        second = event_sample("2030-01-01T01:00:00Z", five_hour=6, cost=101)
        second["eventCostUsd"] = 1
        second["eventCostReady"] = False
        third = event_sample("2030-01-01T02:00:00Z", five_hour=7, cost=103)
        third["eventCostUsd"] = 3
        third["eventCostReady"] = True

        self.assertEqual(process_sample_delta_events(state, first, history), [])
        self.assertEqual(process_sample_delta_events(state, second, history), [])
        events = process_sample_delta_events(state, third, history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 2)

    def test_live_processing_keeps_independent_window_cost_baselines(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=10, seven_day=10, cost=100), history)
        events = process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=10, seven_day=11, cost=112), history)
        history.extend(compact_delta_event(event) for event in events)

        self.assertEqual([(event["window"], event["deltaCostUsd"]) for event in events], [("7d", 12)])
        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=11, seven_day=11, cost=115), history)

        self.assertEqual([(event["window"], event["deltaCostUsd"]) for event in events], [("5h", 15)])

    def test_live_processing_ignores_100_percent_overflow_per_window(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=99, seven_day=99, cost=100), history)
        events = process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=100, seven_day=99, cost=102), history)
        history.extend(compact_delta_event(event) for event in events)

        self.assertEqual([(event["window"], event["deltaPercent"], event["deltaCostUsd"]) for event in events], [("5h", 1, 2)])
        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=100, seven_day=100, cost=105), history)
        history.extend(compact_delta_event(event) for event in events)

        self.assertEqual([(event["window"], event["deltaPercent"], event["deltaCostUsd"]) for event in events], [("7d", 1, 5)])
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 105)
        events = process_sample_delta_events(state, event_sample("2030-01-01T03:00:00Z", five_hour=101, seven_day=100, cost=108), history)
        history.extend(compact_delta_event(event) for event in events)

        self.assertEqual([(event["window"], event["deltaPercent"], event["deltaCostUsd"]) for event in events], [("5h", 1, 3)])
        self.assertEqual(state["windows"]["7d"]["baselineCostUsd"], 108)
        events = process_sample_delta_events(state, event_sample("2030-01-01T04:00:00Z", five_hour=102, seven_day=101, cost=110), history)

        self.assertEqual([(event["window"], event["deltaPercent"], event["deltaCostUsd"]) for event in events], [("5h", 1, 2), ("7d", 1, 2)])

    def test_live_processing_recovers_from_transient_future_reset_rollback(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=24, cost=100, five_hour_reset="2030-01-01T06:00:00Z"), history), [])

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=6, cost=102, five_hour_reset="2030-01-01T08:00:00Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "bad-remote-usage-discarded")

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=37, cost=103, five_hour_reset="2030-01-01T06:00:00Z"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 13)
        self.assertEqual(events[0]["deltaCostUsd"], 3)

    def test_live_processing_recovers_from_transient_reset_rollback_with_jitter(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", seven_day=65, cost=100, seven_day_reset="2030-01-14T05:32:00Z"), history), [])
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", seven_day=3, cost=101, seven_day_reset="2030-01-15T03:18:06Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "bad-remote-usage-discarded")

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", seven_day=66, cost=101, seven_day_reset="2030-01-14T05:31:59Z"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 1)

    def test_live_processing_discards_stale_backward_reset_sample(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=6, cost=100, five_hour_reset="2030-01-01T11:00:00Z"), history), [])

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=16, cost=100.4, five_hour_reset="2030-01-01T08:00:00Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "reset-time-moved-backward-discarded")

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=8, cost=100.5, five_hour_reset="2030-01-01T11:00:00Z"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 2)
        self.assertEqual(events[0]["deltaCostUsd"], 0.5)

    def test_live_processing_rebases_after_consistent_backward_reset_samples(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", seven_day=3, cost=100, seven_day_reset="2030-01-15T03:18:06Z"), history), [])

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", seven_day=73, cost=107, seven_day_reset="2030-01-14T05:32:00Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "reset-time-moved-backward-discarded")

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", seven_day=73, cost=107, seven_day_reset="2030-01-14T05:31:59Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "consistent-backward-reset-rebased")
        self.assertEqual(state["windows"]["7d"]["baselinePercent"], 73)
        self.assertEqual(state["windows"]["7d"]["baselineCostUsd"], 107)
        self.assertEqual(state["windows"]["7d"]["baselineResetAt"], "2030-01-14T05:31:59Z")

        events = process_sample_delta_events(state, event_sample("2030-01-01T03:00:00Z", seven_day=74, cost=108, seven_day_reset="2030-01-14T05:32:00Z"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 1)

    def test_live_processing_requires_consecutive_backward_reset_samples_to_rebase(self):
        state = {}
        history = []
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", seven_day=3, cost=100, seven_day_reset="2030-01-15T03:18:06Z"), history), [])
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", seven_day=73, cost=107, seven_day_reset="2030-01-14T05:32:00Z"), history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "reset-time-moved-backward-discarded")

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", seven_day=3, cost=107, seven_day_reset="2030-01-15T03:18:06Z"), history), [])
        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T03:00:00Z", seven_day=73, cost=108, seven_day_reset="2030-01-14T05:31:59Z"), history), [])

        self.assertEqual(state["_specialEvents"][0]["reason"], "reset-time-moved-backward-discarded")
        self.assertEqual(state["windows"]["7d"]["baselinePercent"], 3)
        self.assertEqual(state["windows"]["7d"]["baselineResetAt"], "2030-01-15T03:18:06Z")

    def test_bad_remote_usage_does_not_consume_cost_before_next_good_sample(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, five_hour_reset="2030-01-01T10:00:00Z"), history)

        bad = event_sample("2030-01-01T01:00:00Z", five_hour=10, cost=105, five_hour_reset="2030-01-01T12:00:00Z")
        self.assertEqual(process_sample_delta_events(state, bad, history), [])
        self.assertEqual(state["_specialEvents"][0]["reason"], "bad-remote-usage-discarded")
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 40)
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 100)
        self.assertEqual(state["windows"]["5h"]["previousCostUsd"], 100)
        self.assertEqual(bad["windows"], {})
        self.assertIn("remoteUsageRejected", bad["errors"])

        events = process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=42, cost=110, five_hour_reset="2030-01-01T10:00:00Z"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 2)
        self.assertEqual(events[0]["deltaCostUsd"], 10)

    def test_repeated_bad_remote_usage_suppresses_duplicate_console_event(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, five_hour_reset="2030-01-01T10:00:00Z"), history)

        process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=10, cost=105, five_hour_reset="2030-01-01T12:00:00Z"), history)
        self.assertFalse(state["_specialEvents"][0]["extra"]["suppressConsole"])

        process_sample_delta_events(state, event_sample("2030-01-01T02:00:00Z", five_hour=10, cost=106, five_hour_reset="2030-01-01T12:00:00Z"), history)
        self.assertTrue(state["_specialEvents"][0]["extra"]["suppressConsole"])

    def test_live_processing_rebases_on_remote_identity_switch_with_usable_windows(self):
        state = {}
        history = []
        process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-old"), history)

        switched = with_remote_identity(event_sample("2030-01-01T01:00:00Z", five_hour=10, cost=105, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-new")
        self.assertEqual(process_sample_delta_events(state, switched, history), [])

        self.assertTrue(switched["remoteUsage"]["accepted"])
        self.assertEqual(state["remoteUsageIdentity"]["user_id"], "user-new")
        self.assertEqual(state["_specialEvents"][0]["reason"], "account-switch")
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 10)
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 105)

        events = process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T02:00:00Z", five_hour=12, cost=107, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-new"), history)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 2)
        self.assertEqual(events[0]["deltaCostUsd"], 2)
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 12)
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 107)

        events = process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T03:00:00Z", five_hour=13, cost=109, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-new"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 2)

    def test_remote_identity_switch_with_rollback_shape_is_real_account_switch(self):
        state = {}
        history = []
        process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "user-old"), history)

        switched = with_remote_identity(event_sample("2030-01-01T01:00:00Z", five_hour=1, cost=105, plan="plus", five_hour_reset="2030-01-01T12:00:00Z"), "user-new")
        self.assertEqual(process_sample_delta_events(state, switched, history), [])

        self.assertTrue(switched["remoteUsage"]["accepted"])
        self.assertNotIn("remoteUsageRejected", switched["errors"])
        self.assertEqual(state["_specialEvents"][0]["reason"], "account-switch")
        self.assertEqual(state["remoteUsageIdentity"]["user_id"], "user-new")
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 1)
        self.assertEqual(state["windows"]["5h"]["baselineResetAt"], "2030-01-01T12:00:00Z")
        self.assertNotIn("awaitingTrustedPercentBaseline", state["windows"]["5h"])

    def test_auth_identity_switch_with_rollback_shape_is_real_account_switch(self):
        state = {}
        history = []
        process_sample_delta_events(state, with_auth_identity(event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, plan="plus", five_hour_reset="2030-01-01T10:00:00Z"), "acct-old"), history)

        switched = with_auth_identity(event_sample("2030-01-01T01:00:00Z", five_hour=1, cost=105, plan="plus", five_hour_reset="2030-01-01T12:00:00Z"), "acct-new")
        self.assertEqual(process_sample_delta_events(state, switched, history), [])

        self.assertTrue(switched["remoteUsage"]["accepted"])
        self.assertNotIn("remoteUsageRejected", switched["errors"])
        self.assertEqual(state["_specialEvents"][0]["reason"], "account-switch")
        self.assertEqual(state["remoteUsageIdentity"]["account_id"], "acct-new")
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 1)
        self.assertNotIn("awaitingTrustedPercentBaseline", state["windows"]["5h"])

    def test_account_switch_counts_next_percent_from_switch_baseline(self):
        state = {}
        history = []
        process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T00:00:00Z", five_hour=40, cost=100, plan="plus"), "user-old"), history)
        self.assertEqual(process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T01:00:00Z", five_hour=4, cost=110, plan="plus"), "user-new"), history), [])

        events = process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T02:00:00Z", five_hour=5, cost=120, plan="plus"), "user-new"), history)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 10)
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 5)
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 120)
        self.assertNotIn("awaitingTrustedPercentBaseline", state["windows"]["5h"])

        events = process_sample_delta_events(state, with_remote_identity(event_sample("2030-01-01T03:00:00Z", five_hour=6, cost=123, plan="plus"), "user-new"), history)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["deltaPercent"], 1)
        self.assertEqual(events[0]["deltaCostUsd"], 3)

    def test_remote_identity_switch_without_quota_windows_is_discarded(self):
        state = {"remoteUsageIdentity": remote_identity("user-old")}
        sample_row = with_remote_identity(event_sample("2030-01-01T00:00:00Z", cost=100), "user-new")

        self.assertEqual(process_sample_delta_events(state, sample_row, []), [])

        self.assertFalse(sample_row["remoteUsage"]["accepted"])
        self.assertEqual(state["_specialEvents"][0]["reason"], "bad-remote-usage-discarded")
        self.assertIn("identity changed without usable quota windows", sample_row["errors"]["remoteUsageRejected"])
        self.assertEqual(state["remoteUsageIdentity"]["user_id"], "user-old")

    def test_live_processing_reports_account_switch_baseline_update(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=8, plan="plus"), history)

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=30, cost=12, plan="pro_lite"), history), [])

        self.assertEqual(len(state["_specialEvents"]), 1)
        self.assertEqual(state["_specialEvents"][0]["reason"], "account-switch")
        text = format_special_event(state["_specialEvents"][0])
        self.assertIn("account-switch", text)
        self.assertIn("2030-01-01T01:00:00Z (local " + datetime.fromisoformat("2030-01-01T01:00:00+00:00").astimezone().isoformat(timespec="seconds") + ")", text)
        self.assertIn("plan plus (1x) -> pro_lite (5x)", text)
        self.assertIn("baseline 5% -> 150%", text)

    def test_live_processing_reports_low_cost_delta_baseline_update(self):
        state = {}
        history = []
        process_sample_delta_events(state, event_sample("2030-01-01T00:00:00Z", five_hour=5, cost=10), history)

        self.assertEqual(process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=8, cost=10), history), [])

        self.assertEqual(len(state["_specialEvents"]), 1)
        self.assertEqual(state["_specialEvents"][0]["reason"], "low-cost-delta-discarded")
        text = format_special_event(state["_specialEvents"][0])
        self.assertIn("low-cost-delta-discarded", text)
        self.assertIn("baseline 5% -> 8%", text)
        self.assertIn("discarded delta +3% / +$0", text)

    def test_startup_reset_clears_percent_and_cost_baselines(self):
        state = reset_runtime_baselines(compact_monitor_state({
            "windows": {
                "5h": {
                    "cumulativePercent": 10.0,
                    "cumulativeCostUsd": 5.0,
                    "baselinePercent": 10.0,
                    "baselineCostUsd": 5.0,
                    "baselinePlan": "unknown",
                    "baselineMultiplier": 1.0,
                    "previousCostUsd": 5.0,
                },
            },
            "runCostUsd": 5.0,
            "measuredCostIntervals": 3,
            "hasRuntimeCostBaseline": True,
        }))
        sample_row = event_sample("2030-01-01T01:00:00Z", five_hour=11, cost=101)
        sample_row["costDelta"] = {"totalCostUsd": 1.0}

        apply_runtime_cost_measurement(sample_row, state)
        events = process_sample_delta_events(state, sample_row, [])

        self.assertEqual(events, [])
        self.assertEqual(state["runCostUsd"], 0.0)
        self.assertEqual(state["measuredCostIntervals"], 0)
        self.assertEqual(state["windows"]["5h"]["baselinePercent"], 11)
        self.assertEqual(state["windows"]["5h"]["baselineCostUsd"], 0.0)

    def test_sample_debug_log_row_uses_lean_state_without_last_sample_or_cost_totals(self):
        row = sample_debug_log_row(sample("2030-01-01T00:00:00Z", five_hour=1), [], {
            "windows": {"5h": {"baselinePercent": 1}},
            "tokenUsage": {"totals": {"requests": 1}},
            "cost": {"totalCostUsd": 2},
            "lastSample": {"checkedAt": "2030-01-01T00:00:00Z"},
            "updatedAt": "2030-01-01T00:00:00Z",
            "runCostUsd": 3,
            "measuredCostIntervals": 4,
            "hasRuntimeCostBaseline": True,
            "remoteUsageIdentity": {"plan_type": "plus"},
        })

        self.assertEqual(row["state"]["windows"]["5h"]["baselinePercent"], 1)
        self.assertEqual(row["state"]["runCostUsd"], 3)
        self.assertNotIn("lastSample", row["state"])
        self.assertNotIn("tokenUsage", row["state"])
        self.assertNotIn("cost", row["state"])

    def test_compact_quota_for_debug_removes_matches_but_keeps_windows(self):
        compact = {"complete": True, "missingWindows": [], "windows": {"5h": {}}, "matches": [{"path": "$"}]}

        debug = compact_quota_for_debug(compact)

        self.assertEqual(debug["windows"], {"5h": {}})
        self.assertNotIn("matches", debug)

    def test_compact_event_log_ignores_low_cost_delta_rows(self):
        events = derive_history_events([
            {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 3, "deltaCostUsd": 0},
            {"checkedAt": "2030-01-01T01:00:00Z", "window": "5h", "deltaPercent": 2, "deltaCostUsd": 1},
        ])

        self.assertEqual(len(events["fiveHour"]), 2)
        self.assertTrue(events["fiveHour"][0]["synthetic"])
        self.assertEqual(events["fiveHour"][1]["cumulativePercent"], 2)
        self.assertEqual(events["fiveHour"][1]["cumulativeCostUsd"], 1)

    def test_live_processing_rehydrates_stale_cumulative_totals_from_history(self):
        state = {"windows": {"5h": {"cumulativePercent": 999, "cumulativeCostUsd": 999, "baselinePercent": 5, "baselineCostUsd": 10, "baselineResetAt": None, "baselinePlan": "unknown", "baselineMultiplier": 1.0, "previousCostUsd": 10}}}
        process_sample_delta_events(state, event_sample("2030-01-01T01:00:00Z", five_hour=5, cost=10), [{"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 2, "deltaCostUsd": 3}])

        self.assertEqual(state["windows"]["5h"]["cumulativePercent"], 2)
        self.assertEqual(state["windows"]["5h"]["cumulativeCostUsd"], 3)

    def test_dashboard_rebases_filtered_delta_charts_from_zero(self):
        html = dashboard_html()

        self.assertIn("function rebaseDeltaEvents(list)", html)
        self.assertIn("const chartStates={}, NODE_RADIUS=5, CHART_MARGINS={l:48,r:18,t:16,b:34}", html)
        self.assertIn("function denseMergeMetrics(id,list)", html)
        self.assertIn("maxPercent:maxPercent*1.03||1,maxCost:maxCost*1.08||1,minDistance:NODE_RADIUS*4", html)
        self.assertIn("function denseDeltaDistance(a,b,metrics)", html)
        self.assertIn("Math.hypot(((b.cumulativePercent||0)-(a.cumulativePercent||0))/metrics.maxPercent*metrics.plotWidth", html)
        self.assertIn("function mergeDenseDeltaEvents(list,metrics)", html)
        self.assertIn("const mergeTrailing=()=>", html)
        self.assertIn("if(index<0)return flush()", html)
        self.assertIn(
            "if(denseDeltaDistance(result[result.length-1]||{cumulativePercent:0,cumulativeCostUsd:0},"
            "{cumulativePercent:percent+(pending.deltaPercent||0),cumulativeCostUsd:cost+(pending.deltaCostUsd||0)},metrics)>metrics.minDistance)flush()",
            html,
        )
        self.assertIn("function scaleFromZero(values", html)
        self.assertIn('button id="prevDate" aria-label="Previous day">&lt;</button>', html)
        self.assertIn('input class="date-range" id="rangeDate" type="date"', html)
        self.assertIn('button id="nextDate" aria-label="Next day">&gt;</button>', html)
        self.assertIn('<div class="quota"><span id="top5h">5h: -</span><span id="top7d">7d: -</span></div>', html)
        self.assertIn("function pointFromSample(sample)", html)
        self.assertIn('const window=(sample.windows||{})[label]||{}', html)
        self.assertIn('return {checkedAt:sample.checkedAt,timestamp:eventTimestamp(sample),fiveHour:windowPoint("5h"),sevenDay:windowPoint("7d"),cost:sample.cost||{}}', html)
        self.assertIn('points=payload.points||[], latest=points[points.length-1]||pointFromSample(payload.lastSample)', html)
        self.assertIn('document.getElementById("top5h").textContent=`5h: ${displayWindows["5h"]?.usageText??pct(latest?.fiveHour?.raw)}`', html)
        self.assertIn('document.getElementById("top7d").textContent=`7d: ${displayWindows["7d"]?.usageText??pct(latest?.sevenDay?.raw)}`', html)
        self.assertIn('<span class="progress-label">5h time</span>', html)
        self.assertIn('<span class="progress-label">7d time</span>', html)
        self.assertIn('<span class="progress-label">5h usage</span>', html)
        self.assertIn('<span class="progress-label">7d usage</span>', html)
        self.assertIn(".window-progress.weekly .time-fill,.window-progress.weekly .usage-fill{background:var(--green)}", html)
        self.assertIn("function updateWindowTime(id,display,resetAt,durationSeconds)", html)
        self.assertIn("if(Number.isFinite(display?.timePercent))", html)
        self.assertIn("Math.max(0,Math.min(100,(1-(resetMs-Date.now())/(durationSeconds*1000))*100))", html)
        self.assertIn('updateWindowTime("time5h",displayWindows["5h"],latest?.fiveHour?.resetAt,5*3600)', html)
        self.assertIn('updateWindowTime("time7d",displayWindows["7d"],latest?.sevenDay?.resetAt,7*24*3600)', html)
        self.assertIn("function updateUsageProgress(id,percent)", html)
        self.assertIn('updateUsageProgress("usage5h",latest?.fiveHour?.raw)', html)
        self.assertIn('updateUsageProgress("usage7d",latest?.sevenDay?.raw)', html)
        self.assertIn('?`Latest: ${new Date(latest.checkedAt).toLocaleString()} | raw cost ${usd(latest.cost.totalCostUsd)}`', html)
        self.assertIn('let selected="Date", previousRange="24h", selectedDate=localDateValue(new Date())', html)
        self.assertIn('const vscode=typeof acquireVsCodeApi==="function"?acquireVsCodeApi():null', html)
        self.assertIn('vscode.postMessage({type:"getCodexUsageSeries"})', html)
        self.assertNotIn('id="refresh"', html)
        self.assertIn("function selectedDateBounds()", html)
        self.assertIn("const [year,month,day]=parts, start=new Date(year,month-1,day), end=new Date(year,month-1,day+1)", html)
        self.assertIn("return {startMs:start.getTime(),endMs:end.getTime()}", html)
        self.assertIn("function localDateValue(date)", html)
        self.assertIn('return `${date.getFullYear()}-${String(date.getMonth()+1).padStart(2,"0")}-${String(date.getDate()).padStart(2,"0")}`', html)
        self.assertIn("function shiftSelectedDate(days)", html)
        self.assertIn("date.setDate(date.getDate()+days)", html)
        self.assertIn('selectedDate=localDateValue(date);selected="Date";setupControls();drawAll(true)', html)
        self.assertIn("function syncControls()", html)
        self.assertIn('document.querySelectorAll("[data-range]").forEach(b=>b.classList.toggle("active",b.dataset.range===selected))', html)
        self.assertIn('date.classList.toggle("active",selected==="Date")', html)
        self.assertIn("function activateDateInput()", html)
        self.assertIn("if(!selectedDate)selectedDate=localDateValue(new Date())", html)
        self.assertIn('if(selected==="Date"){syncControls();return}', html)
        self.assertIn('selected="Date";syncControls();drawAll(true)', html)
        self.assertIn('if(selected==="Date")', html)
        self.assertIn("p.synthetic||(eventTimestamp(p)!=null&&eventTimestamp(p)*1000>=bounds.startMs&&eventTimestamp(p)*1000<bounds.endMs)", html)
        self.assertIn("return hasRealEvents(filtered)?filtered:[]", html)
        self.assertIn("date.onclick=activateDateInput", html)
        self.assertIn("date.onfocus=activateDateInput", html)
        self.assertIn('date.onchange=()=>{selectedDate=date.value;if(selectedDate){selected="Date"}else{selected=previousRange||"24h"}setupControls();drawAll(true)}', html)
        self.assertIn('document.getElementById("prevDate").onclick=()=>shiftSelectedDate(-1)', html)
        self.assertIn('document.getElementById("nextDate").onclick=()=>shiftSelectedDate(1)', html)
        self.assertIn("rawKeys:[p.checkedAt]", html)
        self.assertIn("rawKeys:[...(pending.rawKeys||[pending.checkedAt]),...(p.rawKeys||[p.checkedAt])]", html)
        self.assertIn("const rebased=rebaseDeltaEvents(list), merged=mergeDenseDeltaEvents(rebased,denseMergeMetrics(id,rebased))", html)
        self.assertIn("xDomain:scaleFromZero(points.map(p=>p.x),.03)", html)
        self.assertIn("yDomain:scaleFromZero(points.map(p=>p.y),.08)", html)
        self.assertIn("const chartStates={}", html)
        self.assertIn("drawAll(true)", html)
        self.assertIn("const eventTime=p=>", html)
        self.assertIn("const pointKey=p=>", html)
        self.assertIn("const currentSeries=series.map", html)
        self.assertIn("const makeLineLayer=drawSeries=>", html)
        self.assertIn("const drawLayer=(layer,alpha)=>", html)
        self.assertIn("const previousPoints=previousSeries[seriesIndex]?.points||[], oldOwners=new Map(), newOwners=new Map(), moves=new Map()", html)
        self.assertIn("for(const p of previousPoints)for(const rawKey of p.rawKeys||[p.key])oldOwners.set(rawKey,p)", html)
        self.assertIn("for(const rawKey of new Set([...oldOwners.keys(),...newOwners.keys()]))", html)
        self.assertIn("const key=`${oldPoint.key}->${newPoint.key}`", html)
        self.assertIn("moves.get(key).rawKeys.push(rawKey)", html)
        self.assertIn("rawKeys:t.rawKeys", html)
        self.assertIn("const snapshotSeries=progress=>", html)
        self.assertIn("const updateLiveLineLayer=progress=>", html)
        self.assertIn("chartStates[id]={series:snapshotSeries(progress),lineLayer:liveLineLayer,frame:null}", html)
        self.assertIn("drawLayer(previousLineLayer,1-progress)", html)
        self.assertIn("drawLayer(currentLineLayer,progress)", html)
        self.assertIn("function eventTimestamp(p){", html)
        self.assertIn("const withFallback=filtered=>hasRealEvents(filtered)||!hasRealEvents(list)?filtered:list", html)
        self.assertIn("lineLayer:currentLineLayer", html)
        self.assertIn("requestAnimationFrame(step)", html)
        self.assertIn("chartStates[id]={series:currentSeries.map", html)
        self.assertNotIn("const percentLimit=maxPercent*.0075, costLimit=maxCost*.015", html)

    def test_dashboard_server_polls_once_before_serving(self):
        source = Path(__file__).with_name("monitor_dashboard.py").read_text(encoding="utf-8")

        self.assertIn("def serve_dashboard(args, opener: urllib.request.OpenerDirector | None) -> int:\n    state = UsageDashboardState(args, opener)\n    retry_operation(state.poll_once, getattr(args, \"retry_limit\", DEFAULT_RETRY_LIMIT))", source)
        self.assertIn("if self.last_acquire_started_at is not None:\n                time.sleep(poll_sleep_seconds(self.last_acquire_started_at, self.args.interval))", source)
        self.assertIn('DASHBOARD_PORT = 8765', source)
        self.assertIn('DashboardHTTPServer(("127.0.0.1", DASHBOARD_PORT), Handler)', source)

    def test_dashboard_display_formats_usage_and_remaining_time_for_extension(self):
        now = datetime.fromisoformat("2030-01-01T00:00:00+00:00").timestamp()
        display = dashboard_display({
            "checkedAt": "2030-01-01T00:00:00Z",
            "windows": {
                "5h": {"usedPercent": 42.25, "resetAt": "2030-01-01T01:00:00Z"},
                "7d": {"usedPercent": 61, "resetAt": "2030-01-02T00:00:00Z"},
            },
        }, now)

        self.assertEqual(display["statusBarText"], "5h 42.2% · 7d 61.0%")
        self.assertEqual(display["windows"]["5h"]["timeText"], "80.0%")
        self.assertTrue(display["windows"]["5h"]["resetText"].endswith("(1h 0m remaining)"))
        self.assertIn("5h: 42.2% used", display["tooltip"])

    def test_capped_jsonl_drops_oldest_rows_after_append(self):
        path = Path(__file__).with_name("test_samples_tmp.jsonl")
        try:
            if path.exists():
                path.unlink()
            for index in range(1, 5):
                append_capped_jsonl(path, {"index": index, "payload": "x" * 24}, 120)

            rows = load_history(path)

            self.assertLessEqual(path.stat().st_size, 120)
            self.assertEqual(rows[-1]["index"], 4)
            self.assertGreater(rows[0]["index"], 1)
        finally:
            if path.exists():
                path.unlink()

    def test_write_history_groups_delta_events_by_window(self):
        path = Path(__file__).with_name("test_history_tmp.jsonl")
        try:
            write_history(path, [
                {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 1, "deltaCostUsd": 2},
                {"checkedAt": "2030-01-01T01:00:00Z", "window": "7d", "deltaPercent": 3, "deltaCostUsd": 4},
            ])

            text = path.read_text(encoding="utf-8")

            self.assertIn('{\n  "window": "5h",\n  "events": [', text)
            self.assertIn('    {"checkedAt":"2030-01-01T00:00:00Z","deltaPercent":1,"deltaCostUsd":2}', text)
            self.assertIn('{\n  "window": "7d",\n  "events": [', text)
            self.assertIn('    {"checkedAt":"2030-01-01T01:00:00Z","deltaPercent":3,"deltaCostUsd":4}', text)
            self.assertEqual(load_history(path), [
                {"checkedAt": "2030-01-01T00:00:00Z", "deltaPercent": 1, "deltaCostUsd": 2, "window": "5h"},
                {"checkedAt": "2030-01-01T01:00:00Z", "deltaPercent": 3, "deltaCostUsd": 4, "window": "7d"},
            ])
        finally:
            if path.exists():
                path.unlink()

    def test_append_history_preserves_grouped_delta_event_history(self):
        path = Path(__file__).with_name("test_history_tmp.jsonl")
        try:
            if path.exists():
                path.unlink()

            append_history(path, {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 1, "deltaCostUsd": 2})
            append_history(path, {"checkedAt": "2030-01-01T01:00:00Z", "window": "7d", "deltaPercent": 3, "deltaCostUsd": 4})

            text = path.read_text(encoding="utf-8")

            self.assertIn('{\n  "window": "5h",\n  "events": [', text)
            self.assertIn('    {"checkedAt":"2030-01-01T00:00:00Z","deltaPercent":1,"deltaCostUsd":2}', text)
            self.assertIn('{\n  "window": "7d",\n  "events": [', text)
            self.assertIn('    {"checkedAt":"2030-01-01T01:00:00Z","deltaPercent":3,"deltaCostUsd":4}', text)
            self.assertEqual(load_history(path)[-1]["window"], "7d")
        finally:
            if path.exists():
                path.unlink()

    def test_compact_history_keeps_grouped_indented_delta_events(self):
        path = Path(__file__).with_name("test_history_tmp.jsonl")
        try:
            write_history(path, [
                {"checkedAt": "2030-01-01T00:00:00Z", "window": "5h", "deltaPercent": 1, "deltaCostUsd": 2},
                {"checkedAt": "2030-01-01T01:00:00Z", "window": "7d", "deltaPercent": 3, "deltaCostUsd": 4},
            ])

            compact_history(path, 999999)
            text = path.read_text(encoding="utf-8")
            events = derive_history_events(load_history(path))

            self.assertIn('{\n  "window": "5h",\n  "events": [', text)
            self.assertIn('    {"checkedAt":"2030-01-01T00:00:00Z","deltaPercent":1,"deltaCostUsd":2}', text)
            self.assertIn('{\n  "window": "7d",\n  "events": [', text)
            self.assertIn('    {"checkedAt":"2030-01-01T01:00:00Z","deltaPercent":3,"deltaCostUsd":4}', text)
            self.assertEqual(events["fiveHour"][1]["deltaPercent"], 1)
            self.assertEqual(events["sevenDay"][1]["deltaPercent"], 3)
        finally:
            if path.exists():
                path.unlink()

    def test_client_disconnect_detection_matches_browser_abort_errors(self):
        self.assertTrue(is_client_disconnect(ConnectionAbortedError(10053, "connection aborted")))
        self.assertTrue(is_client_disconnect(ConnectionResetError(10054, "connection reset")))
        self.assertTrue(is_client_disconnect(BrokenPipeError(32, "broken pipe")))
        self.assertFalse(is_client_disconnect(RuntimeError("server bug")))

    def test_dashboard_server_suppresses_client_disconnect_tracebacks(self):
        server = DashboardHTTPServer.__new__(DashboardHTTPServer)
        with mock.patch.object(http.server.ThreadingHTTPServer, "handle_error") as default_handler:
            try:
                raise ConnectionAbortedError(10053, "connection aborted")
            except ConnectionAbortedError:
                server.handle_error(None, ("127.0.0.1", 12345))
        default_handler.assert_not_called()

    def test_dashboard_server_reports_unexpected_exceptions(self):
        server = DashboardHTTPServer.__new__(DashboardHTTPServer)
        with mock.patch.object(http.server.ThreadingHTTPServer, "handle_error") as default_handler:
            try:
                raise RuntimeError("server bug")
            except RuntimeError:
                server.handle_error(None, ("127.0.0.1", 12345))
        default_handler.assert_called_once_with(None, ("127.0.0.1", 12345))


def sample(checked_at, five_hour=None, seven_day=None, five_hour_reset=None, seven_day_reset=None):
    token_usage = {"totals": empty_token_totals()}
    windows = {}
    if five_hour is not None:
        windows["5h"] = {"usedPercent": five_hour, "resetAt": five_hour_reset, "path": "$.rate_limit.primary_window"}
    if seven_day is not None:
        windows["7d"] = {"usedPercent": seven_day, "resetAt": seven_day_reset, "path": "$.rate_limit.secondary_window"}
    return {"checkedAt": checked_at, "windows": windows, "errors": {}, "tokenUsage": token_usage, "tokenDelta": empty_token_totals()}


def event_sample(checked_at, five_hour=None, seven_day=None, cost=0, plan="unknown", five_hour_reset=None, seven_day_reset=None):
    row = sample(checked_at, five_hour=five_hour, seven_day=seven_day, five_hour_reset=five_hour_reset, seven_day_reset=seven_day_reset)
    for window in row["windows"].values():
        window["plan"] = plan
        window["planMultiplier"] = {"plus": 1.0, "pro_lite": 5.0, "pro": 20.0, "unknown": 1.0}[plan]
    row["cost"] = {"inputCostUsd": 0, "cachedInputCostUsd": 0, "outputCostUsd": 0, "totalCostUsd": cost}
    return row


def remote_identity(user_id, account_id=None, email=None, plan_type="plus"):
    return {"user_id": user_id, "account_id": account_id or user_id, "email": email or f"{user_id}@example.test", "plan_type": plan_type}


def with_remote_identity(row, user_id, account_id=None, email=None, plan_type="plus"):
    row["remoteUsage"] = {"rawResponse": remote_identity(user_id, account_id, email, plan_type)}
    return row


def with_auth_identity(row, account_id):
    row["remoteUsage"] = {"authIdentity": {"account_id": account_id}}
    return row


if __name__ == "__main__":
    unittest.main()
