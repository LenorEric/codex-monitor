#!/usr/bin/env python3

import http.server
import json
import sys
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime
from pathlib import Path

from monitor_common import DEFAULT_RETRY_LIMIT, coerce_float, empty_cost_totals, is_client_disconnect, parse_timestamp, poll_sleep_seconds, retry_operation
from monitor_events import compact_delta_event, derive_history_events, print_ratio_warnings, print_special_events, print_valid_delta_events, process_sample_delta_events, sample_debug_log_row
from monitor_history import append_capped_jsonl, append_history, apply_runtime_cost_measurement, collect_usage_sample, compact_history, load_history, load_state, reset_runtime_baselines, write_state

DASHBOARD_HTML_PATH = Path(__file__).with_name("dashboard.html")
DASHBOARD_PORT = 8765

class DashboardHTTPServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        if is_client_disconnect(sys.exc_info()[1]):
            return
        super().handle_error(request, client_address)

def window_point(sample: dict, label: str) -> dict:
    window = (sample.get("windows") or {}).get(label) or {}
    raw = coerce_float(window.get("usedPercent"))
    return {"raw": raw, "continuous": raw, "resetAt": window.get("resetAt"), "path": window.get("path")}

def dashboard_points_from_state(state: dict) -> list[dict]:
    sample = state.get("lastSample")
    if not isinstance(sample, dict):
        return []
    checked_at = sample.get("checkedAt")
    return [{
        "checkedAt": checked_at,
        "timestamp": parse_timestamp(checked_at),
        "fiveHour": window_point(sample, "5h"),
        "sevenDay": window_point(sample, "7d"),
        "cost": sample.get("cost") or empty_cost_totals(),
        "errors": sample.get("errors") or {},
    }]

def format_remaining_time(seconds: float) -> str:
    seconds = max(0, int(seconds))
    days, seconds = divmod(seconds, 24 * 60 * 60)
    hours, seconds = divmod(seconds, 60 * 60)
    minutes = seconds // 60
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"

def dashboard_window_display(sample: dict, label: str, duration_seconds: int, now: float | None = None) -> dict:
    window = (sample.get("windows") or {}).get(label) or {}
    usage = coerce_float(window.get("usedPercent"))
    reset_at = window.get("resetAt")
    reset_timestamp = parse_timestamp(reset_at)
    now = time.time() if now is None else now
    time_percent = None if reset_timestamp is None else max(0.0, min(100.0, (1 - (reset_timestamp - now) / duration_seconds) * 100))
    return {
        "usagePercent": usage,
        "usageText": "-" if usage is None else f"{usage:.1f}%",
        "timePercent": time_percent,
        "timeText": "-" if time_percent is None else f"{time_percent:.1f}%",
        "resetAt": reset_at,
        "resetText": "-" if reset_timestamp is None else f"{datetime.fromtimestamp(reset_timestamp).astimezone().strftime('%Y-%m-%d %H:%M:%S')} ({format_remaining_time(reset_timestamp - now)} remaining)",
    }

def dashboard_display(sample: dict | None, now: float | None = None) -> dict:
    sample = sample if isinstance(sample, dict) else {}
    now = time.time() if now is None else now
    windows = {
        "5h": dashboard_window_display(sample, "5h", 5 * 60 * 60, now),
        "7d": dashboard_window_display(sample, "7d", 7 * 24 * 60 * 60, now),
    }
    return {
        "statusBarText": f"5h {windows['5h']['usageText']} · 7d {windows['7d']['usageText']}",
        "tooltip": "\n".join((
            "Codex Usage",
            f"5h: {windows['5h']['usageText']} used, resets {windows['5h']['resetText']}",
            f"7d: {windows['7d']['usageText']} used, resets {windows['7d']['resetText']}",
        )),
        "checkedAt": sample.get("checkedAt"),
        "windows": windows,
    }

class UsageDashboardState:
    def __init__(self, args, opener: urllib.request.OpenerDirector | None):
        self.args = args
        self.opener = opener
        self.lock = threading.Lock()
        self.running = True
        self.last_sample = None
        self.last_error = None
        self.last_acquire_started_at = None
        self.runtime_state = reset_runtime_baselines(load_state(args.state))

    def history(self) -> list[dict]:
        with self.lock:
            return load_history(self.args.history)

    def state(self) -> dict:
        with self.lock:
            return load_state(self.args.state)

    def poll_once(self) -> dict:
        with self.lock:
            history = load_history(self.args.history)
            self.last_acquire_started_at = time.monotonic()
            sample = collect_usage_sample(self.args, self.opener, self.runtime_state.get("tokenUsage"), self.runtime_state.get("cost"), self.runtime_state)
            apply_runtime_cost_measurement(sample, self.runtime_state)
            events = process_sample_delta_events(self.runtime_state, sample, history)
            append_capped_jsonl(self.args.sample_log, sample_debug_log_row(sample, events, self.runtime_state), self.args.sample_log_max_bytes)
            for event in events:
                append_history(self.args.history, compact_delta_event(event))
            write_state(self.args.state, self.runtime_state)
            compact_history(self.args.history, self.args.compact_history_days)
            print_special_events(self.runtime_state.get("_specialEvents") or [])
            print_valid_delta_events(events, sample)
            print_ratio_warnings(events)
            self.last_sample = sample
            self.last_error = None
            return sample

    def run(self) -> None:
        while self.running:
            if self.last_acquire_started_at is not None:
                time.sleep(poll_sleep_seconds(self.last_acquire_started_at, self.args.interval))
                if not self.running:
                    break
            retry_operation(self.poll_once, getattr(self.args, "retry_limit", DEFAULT_RETRY_LIMIT))

def dashboard_html() -> str:
    return DASHBOARD_HTML_PATH.read_text(encoding="utf-8")

def dashboard_series_payload(args, state: UsageDashboardState) -> dict:
    history = state.history()
    current_state = state.state()
    events = derive_history_events(history)
    last_sample = state.last_sample or current_state.get("lastSample")
    return {
        "historyPath": str(args.history),
        "statePath": str(args.state),
        "points": dashboard_points_from_state(current_state),
        "events": events,
        "historyStats": {
            "rows": len(history),
            "fiveHourEvents": len(events["fiveHour"]),
            "sevenDayEvents": len(events["sevenDay"]),
            "fiveHourRealEvents": sum(1 for event in events["fiveHour"] if not event.get("synthetic")),
            "sevenDayRealEvents": sum(1 for event in events["sevenDay"] if not event.get("synthetic")),
        },
        "lastError": state.last_error,
        "lastSample": last_sample,
        "display": dashboard_display(last_sample),
    }

def serve_dashboard(args, opener: urllib.request.OpenerDirector | None) -> int:
    state = UsageDashboardState(args, opener)
    retry_operation(state.poll_once, getattr(args, "retry_limit", DEFAULT_RETRY_LIMIT))
    thread = threading.Thread(target=state.run, daemon=True)
    thread.start()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.handle_get()

        def handle_get(self):
            path = urllib.parse.urlparse(self.path).path
            if path == "/":
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(dashboard_html().encode("utf-8"))
                return
            if path == "/api/series":
                body = json.dumps(dashboard_series_payload(args, state), ensure_ascii=False).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, fmt, *args):
            return

    server = DashboardHTTPServer(("127.0.0.1", DASHBOARD_PORT), Handler)
    url = f"http://127.0.0.1:{DASHBOARD_PORT}/"
    print(f"Dashboard: {url}", flush=True)
    if not args.no_open:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        state.running = False
        server.server_close()
    return 0
