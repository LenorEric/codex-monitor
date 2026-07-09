#!/usr/bin/env python3
import argparse
import json
import time
from pathlib import Path

from monitor_common import *
from monitor_dashboard import *
from monitor_events import *
from monitor_history import *
from monitor_quota import *
from monitor_tokens import *


def main() -> int:
    parser = argparse.ArgumentParser(description="Poll Codex ChatGPT-account usage/rate-limit data and local Codex session token usage.")
    parser.add_argument("--auth", type=Path, default=codex_home() / "auth.json")
    parser.add_argument("--codex-home", type=Path, default=codex_home(), help="Codex config directory containing sessions/. Defaults to CODEX_HOME or ~/.codex.")
    parser.add_argument("--interval", type=int, default=90)
    parser.add_argument("--timeout", type=int, default=10, help="Per-request timeout in seconds.")
    parser.add_argument("--history", type=Path, help="JSONL history path. Defaults to usage_monitor_history.jsonl beside this script.")
    parser.add_argument("--sample-log", type=Path, help="Detailed JSONL sample debug log path. Defaults to usage_monitor_samples.jsonl beside the history file.")
    parser.add_argument("--sample-log-max-bytes", type=int, default=DEFAULT_SAMPLE_LOG_MAX_BYTES, help="Maximum detailed sample debug log size before oldest rows are trimmed. Defaults to 50 MiB.")
    parser.add_argument("--dashboard", action="store_true", help="Start the local chart dashboard.")
    parser.add_argument("--process-history", action="store_true", help="Extract and print valid delta percent/cost pairs from the local JSONL history, then exit.")
    parser.add_argument("--no-open", action="store_true", help="Do not open the dashboard in the default browser.")
    parser.add_argument("--compact-history-days", type=int, help="Rewrite history to keep only samples newer than this many days.")
    parser.add_argument("--local-only", action="store_true", help="Only scan local Codex session logs; do not call ChatGPT usage endpoints.")
    parser.add_argument("--no-token-scan", action="store_true", help="Disable local Codex session token usage scanning.")
    parser.add_argument("--retry-limit", type=int, default=DEFAULT_RETRY_LIMIT, help="Retries for HTTP and dashboard polling failures before raising; network errors retry indefinitely. Defaults to 3.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    args.history = args.history or default_history_path(args.codex_home)
    args.sample_log = args.sample_log or default_sample_log_path(args.history)
    args.state = state_path_for(args.history)
    opener = None
    if not args.local_only:
        opener = opener_for(args.auth.parent)

    if args.dashboard:
        return serve_dashboard(args, opener)

    if args.process_history:
        history = load_history(args.history)
        print_valid_delta_events(valid_delta_events(derive_history_events(history)), (load_state(args.state) or {}).get("lastSample"))
        return 0

    runtime_state = reset_runtime_baselines(load_state(args.state))
    while True:
        history = load_history(args.history)
        acquire_started_at = time.monotonic()
        sample = collect_usage_sample(args, opener, runtime_state.get("tokenUsage"), runtime_state.get("cost"), runtime_state)
        apply_runtime_cost_measurement(sample, runtime_state)
        events = process_sample_delta_events(runtime_state, sample, history)
        append_capped_jsonl(args.sample_log, sample_debug_log_row(sample, events, runtime_state), args.sample_log_max_bytes)
        for event in events:
            append_history(args.history, compact_delta_event(event))
        write_state(args.state, runtime_state)
        compact_history(args.history, args.compact_history_days)
        print_special_events(runtime_state.get("_specialEvents") or [])
        print_valid_delta_events(events, sample)
        print_ratio_warnings(events)
        print(json.dumps(sample, indent=2 if args.pretty else None, ensure_ascii=False), flush=True)
        if args.once:
            return 0
        time.sleep(poll_sleep_seconds(acquire_started_at, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
