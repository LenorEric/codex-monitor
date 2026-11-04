import json
import unittest
from pathlib import Path

from monitor_tokens import _consume_codex_session_line, _new_codex_file_state


class SessionFallbackTests(unittest.TestCase):
    def setUp(self):
        self.path = Path("rollout.jsonl")
        self.state = _new_codex_file_state(self.path, (0, 0))
        self.consume("session_meta", {"id": "child", "session_id": "parent", "forked_from_id": "parent"})
        self.consume("turn_context", {"model": "gpt-5.5", "thread_settings": {"service_tier": "fast"}})

    def consume(self, kind, payload):
        _consume_codex_session_line(self.state, self.path, json.dumps({"type": kind, "timestamp": "2030-01-01T00:00:00Z", "payload": payload}).encode())

    def usage(self, total, last):
        self.consume("event_msg", {"type": "token_count", "info": {
            "total_token_usage": {"input_tokens": total, "output_tokens": 0},
            "last_token_usage": {"input_tokens": last, "output_tokens": 0},
        }})

    def test_first_observation_uses_increment_without_counting_inherited_total(self):
        self.usage(1000, 10)
        self.assertEqual(self.state["events"][0]["tokens"]["input"], 10)
        self.assertEqual(self.state["events"][0]["cumulative"]["input"], 1000)
        self.assertEqual(self.state["events"][0]["model"], "gpt-5.5")
        self.assertEqual(self.state["events"][0]["serviceTier"], "fast")

    def test_cumulative_progress_and_repeated_snapshot_do_not_recount_last_usage(self):
        self.usage(1000, 10)
        self.usage(1020, 5)
        self.usage(1020, 5)
        self.assertEqual([event["tokens"]["input"] for event in self.state["events"]], [10, 20])

    def test_counter_rollback_uses_increment(self):
        self.usage(1000, 10)
        self.usage(100, 5)
        self.assertEqual([event["tokens"]["input"] for event in self.state["events"]], [10, 5])

    def test_usage_requires_both_counters_instead_of_inventing_an_identity(self):
        for field in ("total_token_usage", "last_token_usage"):
            with self.subTest(field=field):
                self.setUp()
                self.consume("event_msg", {"type": "token_count", "info": {field: {"input_tokens": 10, "output_tokens": 1}}})
                self.assertTrue(self.state["invalidLines"])
                self.assertEqual(self.state["events"], [])
                self.assertIsNone(self.state["previousTotal"])

    def test_null_info_is_a_quota_update_without_token_usage(self):
        self.consume("event_msg", {"type": "token_count", "info": None, "rate_limits": {}})
        self.assertFalse(self.state["invalidLines"])
        self.assertEqual(self.state["events"], [])

    def test_session_id_never_falls_back_to_parent_or_filename(self):
        self.state = _new_codex_file_state(self.path, (0, 0))
        self.consume("session_meta", {"session_id": "parent", "thread_id": "alternate"})
        self.assertFalse(self.state["hasSessionMeta"])
        self.assertTrue(self.state["invalidLines"])
        self.assertEqual(self.state["sessionId"], "")

    def test_token_metadata_does_not_override_turn_context(self):
        self.consume("event_msg", {"type": "token_count", "model": "gpt-6", "service_tier": "default", "info": {
            "model_name": "gpt-6", "service_tier": "default",
            "total_token_usage": {"input_tokens": 100, "output_tokens": 0},
            "last_token_usage": {"input_tokens": 10, "output_tokens": 0},
        }})
        self.assertEqual(self.state["events"][0]["model"], "gpt-5.5")
        self.assertEqual(self.state["events"][0]["serviceTier"], "fast")
