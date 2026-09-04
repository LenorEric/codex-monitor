#!/usr/bin/env python3

import base64
import gzip
import hashlib
import hmac
import http.server
import ipaddress
import json
import math
import os
import secrets
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from collections import deque
from datetime import datetime
from http.cookies import SimpleCookie
from pathlib import Path

from monitor_accounts import AccountError, AccountManager, auth_fingerprint, is_api_auth, remove_directory
from monitor_cloud import CloudError, CloudManager, control_password_is_compromised, control_password_is_configured, control_password_matches, load_server_config
from monitor_common import DEFAULT_RETRY_LIMIT, UsageError, coerce_float, empty_cost_totals, is_client_disconnect, now_iso, parse_timestamp, poll_sleep_seconds, retry_operation
from monitor_events import collect_with_bad_remote_usage_retry, compact_delta_event, cost_percent_ratio, derive_history_events, print_ratio_warnings, print_special_events, print_valid_delta_events, process_sample_delta_events, ratio_deviation, ratio_deviation_warning, sample_debug_log_row
from monitor_history import (
    append_capped_jsonl, append_history, append_quota_history_sample, apply_runtime_cost_measurement, collect_usage_sample, compact_history, compact_quota_history,
    default_dashboard_cache_path, default_quota_history_path, default_token_session_history_path, load_history,
    fetch_usage_with_percent_arbitration, load_quota_history, load_state, load_token_session_history, make_history_sample, quota_history_row_from_sample, replace_account_label, reset_runtime_baselines,
    rewrite_account_labels, write_state, write_token_session_history,
)
from monitor_skills import SkillError, SkillManager
from monitor_session_refresh import refresh_session
from monitor_token_ledger import default_token_ledger_path, sync_token_ledger, token_cost_snapshot
from monitor_tokens import cost_progress, cost_progress_by_model
from monitor_usage_sync import UsageDataStore, add_record_provenance, default_usage_sync_cache_path

DASHBOARD_HTML_PATH = Path(__file__).with_name("dashboard.html")
MANAGEMENT_HTML_PATH = Path(__file__).with_name("management.html")
DASHBOARD_PORT = 8765
DASHBOARD_INSTANCE_NAME = f"CodexUsageMonitorDashboard-{DASHBOARD_PORT}"
INACTIVE_ACCOUNT_POLL_INTERVAL_SECONDS = 10 * 60
SESSION_REFRESH_MAX_ATTEMPTS = 10
SESSION_REFRESH_FAILURE_RETRY_SECONDS = 60 * 60
SESSION_REFRESH_RESET_LATENCY_SECONDS = 60
SESSION_REFRESH_SUCCESS_TOLERANCE_SECONDS = 10
SESSION_REFRESH_WINDOW_SECONDS = {"5h": 5 * 60 * 60, "7d": 7 * 24 * 60 * 60}
CONTROL_COOKIE_NAME = "codex_monitor_control"
CONTROL_COOKIE_MAX_AGE_SECONDS = 30 * 24 * 60 * 60
USAGE_TIME_WINDOWS = {"fiveHour": 5 * 60 * 60, "sevenDay": 7 * 24 * 60 * 60}
USAGE_TIME_RATE_FLOORS = {"fiveHour": 5.0, "sevenDay": 1.0}
USAGE_TIME_ROUNDING_ALLOWANCE = 2.0
USAGE_TIME_RESET_JITTER_SECONDS = 2 * 60
USAGE_TIME_NEW_RESET_TOLERANCE_SECONDS = 30 * 60
QUOTA_HISTORY_DISPLAY_FULL_SECONDS = 3 * 24 * 60 * 60
# The x4/x8/x16 tiers mean 4/8/16 source points per display point.
QUOTA_HISTORY_DISPLAY_X4_SECONDS = 7 * 24 * 60 * 60
QUOTA_HISTORY_DISPLAY_X8_SECONDS = 30 * 24 * 60 * 60
HOUR_MULTIPLIER = 60 * 60
DASHBOARD_DISPLAY_CACHE_VERSION = 2
DASHBOARD_DISPLAY_CACHE_GAP_SECONDS = 4 * 60 * 60
DASHBOARD_DISPLAY_CACHE_MAINTENANCE_SECONDS = 60
DASHBOARD_SERIES_PROTOCOL_VERSION = 2
DASHBOARD_SERIES_JOURNAL_MAX_AGE_SECONDS = 24 * 60 * 60
DASHBOARD_SERIES_JOURNAL_MAX_BATCHES = 512
DASHBOARD_SERIES_JOURNAL_MAX_BYTES = 16 * 1024 * 1024
DASHBOARD_GZIP_MIN_BYTES = 1024
SENSITIVE_DASHBOARD_FIELDS = {
    "access_token", "account_id", "accountId", "apiIdentityId", "_apiIdentityId", "authIdentity", "baseUrl", "boundMachineId", "configPath", "email", "encryptionPassphrase", "fingerprint", "id_token", "identity",
    "password", "privatePath", "rawResponse", "refresh_token", "remoteRoot", "revisionId", "statePath", "target", "tokens", "user_id", "username",
}


def session_refresh_cost_text(cost: float | None) -> str:
    return f"${cost:.8f}" if cost is not None else "unavailable (Codex CLI did not report token usage)"


def client_host_is_loopback(host) -> bool:
    try:
        return ipaddress.ip_address(host.split("%", 1)[0]).is_loopback
    except (AttributeError, ValueError):
        return False

class DashboardHTTPServer(http.server.ThreadingHTTPServer):
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        if is_client_disconnect(sys.exc_info()[1]):
            return
        super().handle_error(request, client_address)

class DashboardInstanceLock:
    def __init__(self):
        self.handle = None

    def acquire(self) -> bool:
        if os.name == "nt":
            import ctypes
            from ctypes import wintypes

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CreateMutexW.argtypes = (wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR)
            kernel32.CreateMutexW.restype = wintypes.HANDLE
            kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
            kernel32.CloseHandle.restype = wintypes.BOOL
            self.handle = kernel32.CreateMutexW(None, False, f"Local\\{DASHBOARD_INSTANCE_NAME}")
            if not self.handle:
                raise OSError(ctypes.get_last_error(), "Cannot create the dashboard instance mutex")
            if ctypes.get_last_error() == 183:
                kernel32.CloseHandle(self.handle)
                self.handle = None
                return False
            return True

        import fcntl

        self.handle = os.open(Path(tempfile.gettempdir()) / f"{DASHBOARD_INSTANCE_NAME}-{os.getuid()}.lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self.handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(self.handle)
            self.handle = None
            return False
        return True

    def release(self) -> None:
        if self.handle is None:
            return
        if os.name == "nt":
            import ctypes

            ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(self.handle)
        else:
            os.close(self.handle)
        self.handle = None

class ControlAuth:
    def __init__(self, control: dict):
        self.update(control)

    def update(self, control: dict) -> None:
        self.password_hash = control["passwordHash"]
        self.password_salt = control.get("passwordSalt", "")
        self.compromised = control_password_is_compromised(control)
        self.cookie_secret = hmac.new(control["cookieSecret"].encode("utf-8"), self.password_hash.encode("utf-8"), hashlib.sha256).digest()

    def password_matches(self, password) -> bool:
        return control_password_matches(password, self.password_hash, self.password_salt)

    def is_configured(self) -> bool:
        return bool(self.password_hash) and not self.compromised

    def is_compromised(self) -> bool:
        return self.compromised

    def create_token(self, now: int | None = None) -> str:
        payload = f"{int(time.time()) if now is None else int(now)}:{secrets.token_urlsafe(18)}".encode("ascii")
        return f"{base64.urlsafe_b64encode(payload).decode('ascii').rstrip('=')}.{hmac.new(self.cookie_secret, payload, hashlib.sha256).hexdigest()}"

    def token_is_valid(self, token: str | None, now: int | None = None) -> bool:
        try:
            encoded, signature = str(token or "").split(".", 1)
            payload = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
            issued_at = int(payload.split(b":", 1)[0])
        except (ValueError, TypeError):
            return False
        current = int(time.time()) if now is None else int(now)
        return 0 <= current - issued_at <= CONTROL_COOKIE_MAX_AGE_SECONDS and hmac.compare_digest(signature, hmac.new(self.cookie_secret, payload, hashlib.sha256).hexdigest())

def dashboard_safe_json(value):
    if isinstance(value, dict):
        return {key: dashboard_safe_json(item) for key, item in value.items() if key not in SENSITIVE_DASHBOARD_FIELDS or key in {"password", "encryptionPassphrase"} and isinstance(item, bool)}
    if isinstance(value, list):
        return [dashboard_safe_json(item) for item in value]
    return value

def window_point(sample: dict, label: str) -> dict:
    window = (sample.get("windows") or {}).get(label) or {}
    raw = coerce_float(window.get("usedPercent"))
    return {"raw": raw, "continuous": raw, "resetAt": window.get("resetAt"), "plan": window.get("plan") or "plus"}

def dashboard_sample(sample: dict | None) -> dict | None:
    if not isinstance(sample, dict):
        return None
    api_account = bool(sample.get("isApiAccount"))
    projected = {
        "checkedAt": sample.get("checkedAt"),
        "percentCheckedAt": sample.get("percentCheckedAt"),
        "windows": {} if api_account else {label: {"usedPercent": ((sample.get("windows") or {}).get(label) or {}).get("usedPercent"), "resetAt": ((sample.get("windows") or {}).get(label) or {}).get("resetAt"), "plan": ((sample.get("windows") or {}).get(label) or {}).get("plan") or "plus"} for label in ("5h", "7d")},
        "isApiAccount": api_account,
        "cost": sample.get("cost") or empty_cost_totals(),
    }
    if usage_account_id := sample.get("usageAccountId") or (sample.get("sync") or {}).get("accountId"):
        projected["usageAccountId"] = usage_account_id
    return projected

def dashboard_account_status(status: dict, usage_account_resolver=None, activation_history: list[dict] | None = None) -> dict:
    items = [{key: account.get(key) for key in ("id", "label", "ready", "active", "pollError", "pollErrorAt", "stale", "accountType", "isApiAccount", "sessionRefresh", "configHeader", "configHeaderToml")} for account in status.get("items", [])]
    label_counts = {}
    for account in items:
        label_counts[str(account.get("label") or "Unknown").casefold()] = label_counts.get(str(account.get("label") or "Unknown").casefold(), 0) + 1
    for account in items:
        account["usageAccountId"] = usage_account_resolver(account.get("id")) if usage_account_resolver is not None else account.get("id") or "unknown"
        account["displayLabel"] = f"{account.get('label') or 'Unknown'} · {str(account.get('id') or 'profile')[-6:]}" if label_counts[str(account.get("label") or "Unknown").casefold()] > 1 else account.get("label") or "Unknown"
    groups = {}
    for account in items:
        groups.setdefault(account["usageAccountId"], []).append(account)
    activation_ids = [str(row.get("accountSlotId")) for row in reversed(activation_history or []) if row.get("accountSlotId")]
    usage_groups = []
    for usage_account_id, profiles in groups.items():
        profile_ids = {profile["id"] for profile in profiles}
        primary_id = next((account_id for account_id in activation_ids if account_id in profile_ids), status.get("activeAccountId") if status.get("activeAccountId") in profile_ids else profiles[0]["id"])
        primary = next((profile for profile in profiles if profile["id"] == primary_id), profiles[0])
        usage_groups.append({
            "id": usage_account_id,
            "label": f"{primary.get('label') or 'Unknown'} ({len(profiles)})" if len(profiles) > 1 else primary.get("label") or "Unknown",
            "profileCount": len(profiles),
            "isApiAccount": bool(primary.get("isApiAccount")),
        })
    return {
        "activeAccountId": status.get("activeAccountId"),
        "awaitingLogin": bool(status.get("awaitingLogin")),
        "error": "Account operation failed" if status.get("error") else None,
        "message": status.get("message"),
        "items": items,
        "usageGroups": usage_groups,
    }

def dashboard_accounts(state) -> dict:
    return dashboard_account_status(
        state.accounts.status(),
        getattr(state.cloud, "usage_account_id", None) if hasattr(state, "cloud") else None,
        state.accounts.attribution_timeline() if hasattr(state.accounts, "attribution_timeline") else [],
    )

def dashboard_skill_status(status: dict) -> dict:
    return {
        "version": status.get("version"),
        "items": [{
            "name": item.get("name"),
            "assignments": item.get("assignments") or {},
            "projections": {app: {"state": projection.get("state")} for app, projection in (item.get("projections") or {}).items()},
            "errors": {app: "Projection error" for app in (item.get("errors") or {})},
        } for item in status.get("items", [])],
    }

def dashboard_cloud_status(status: dict) -> dict:
    webdav = status.get("webdav") or {}
    auto_sync = status.get("autoSync") or {}
    usage_sync = status.get("usageSync") or {}
    failure = auto_sync.get("failure")
    return {
        "webdav": {key: webdav.get(key) for key in ("enabled", "skillsAutoUpload", "usageDataAutoSync", "allowOptimisticWrites")},
        "secretsConfigured": status.get("secretsConfigured") or {},
        "conditionalWritesVerified": bool(status.get("conditionalWritesVerified")),
        "optimisticWritesActive": bool(status.get("optimisticWritesActive")),
        "error": "Cloud configuration error" if status.get("error") else None,
        "autoSync": {
            "pending": bool(auto_sync.get("pending")),
            "pendingSkills": auto_sync.get("pendingSkills") or [],
            "attempts": auto_sync.get("attempts") or 0,
            "failure": {"id": failure.get("id"), "message": "Automatic skill upload failed"} if isinstance(failure, dict) else None,
        },
        "usageSync": {
            "lastSuccessAt": usage_sync.get("lastSuccessAt"), "nextAttemptInSeconds": usage_sync.get("nextAttemptInSeconds"),
            "failure": {"message": "Automatic usage synchronization failed"} if usage_sync.get("failure") else None,
        },
    }

def dashboard_points_from_state(state: dict) -> list[dict]:
    sample = state.get("lastSample")
    if not isinstance(sample, dict):
        return []
    checked_at = sample.get("checkedAt")
    point = {
        "checkedAt": checked_at,
        "timestamp": parse_timestamp(checked_at),
        "fiveHour": window_point(sample, "5h"),
        "sevenDay": window_point(sample, "7d"),
        "cost": sample.get("cost") or empty_cost_totals(),
    }
    if usage_account_id := sample.get("usageAccountId") or (sample.get("sync") or {}).get("accountId"):
        point["usageAccountId"] = usage_account_id
    return [point]

def dashboard_usage_account_id(row: dict, slot_usage_accounts: dict[str, str] | None = None) -> str:
    return str(row.get("usageAccountId") or (row.get("sync") or {}).get("accountId") or (slot_usage_accounts or {}).get(str(row.get("accountSlotId") or "")) or row.get("accountSlotId") or row.get("accountLabel") or "unknown")

def dashboard_quota_point(row: dict, slot_usage_accounts: dict[str, str] | None = None) -> dict:
    compaction = row.get("compaction") or {}
    return {
        "checkedAt": row.get("checkedAt"),
        "timestamp": parse_timestamp(row.get("checkedAt")),
        "compactedFrom": parse_timestamp(compaction.get("continuousFrom")),
        "accountSlotId": row.get("accountSlotId"),
        "accountLabel": row.get("accountLabel"),
        "usageAccountId": dashboard_usage_account_id(row, slot_usage_accounts),
        "fiveHour": window_point(row, "5h"),
        "sevenDay": window_point(row, "7d"),
    }

def _usage_time_rate_limit(records: list[dict], label: str) -> float:
    rates = sorted(
        max(0.0, current["raw"] - previous["raw"] - USAGE_TIME_ROUNDING_ALLOWANCE) / ((current["timestamp"] - previous["timestamp"]) / 60)
        for previous, current in zip(records, records[1:])
        if current["timestamp"] > previous["timestamp"] and current["raw"] > previous["raw"]
    )
    floor = USAGE_TIME_RATE_FLOORS[label]
    if len(rates) < 10:
        return floor
    return max(floor, min(floor * 3, rates[int((len(rates) - 1) * .9)] * 2.5))

def _usage_time_new_reset_is_supported(records: list[dict], index: int, previous: dict, duration: int) -> bool:
    current = records[index]
    if current["resetAt"] is None or not previous["timestamp"] - USAGE_TIME_NEW_RESET_TOLERANCE_SECONDS <= current["resetAt"] - duration <= current["timestamp"] + USAGE_TIME_NEW_RESET_TOLERANCE_SECONDS:
        return False
    if previous["resetAt"] is not None and current["resetAt"] <= previous["resetAt"] + USAGE_TIME_RESET_JITTER_SECONDS:
        return False
    support = []
    for candidate in records[index:index + 64]:
        if candidate["timestamp"] - current["timestamp"] > 2 * 60 * 60:
            break
        if previous["resetAt"] is not None and candidate["raw"] >= previous["raw"] - USAGE_TIME_ROUNDING_ALLOWANCE and candidate["resetAt"] is not None and abs(candidate["resetAt"] - previous["resetAt"]) <= USAGE_TIME_RESET_JITTER_SECONDS:
            return False
        if (
            candidate["raw"] < previous["raw"] - USAGE_TIME_ROUNDING_ALLOWANCE and candidate["resetAt"] is not None
            and current["resetAt"] - duration - USAGE_TIME_NEW_RESET_TOLERANCE_SECONDS <= candidate["resetAt"] - duration <= candidate["timestamp"] + USAGE_TIME_NEW_RESET_TOLERANCE_SECONDS
        ):
            support.append(candidate)
    return len(support) >= 4

def _usage_time_reset_is_credible(records: list[dict], index: int, previous: dict, duration: int) -> bool:
    current = records[index]
    if previous["resetAt"] is not None and current["timestamp"] >= previous["resetAt"] - USAGE_TIME_RESET_JITTER_SECONDS:
        return True
    if current["timestamp"] - previous["timestamp"] >= duration:
        return True
    return _usage_time_new_reset_is_supported(records, index, previous, duration)

def _usage_time_continuous_values(records: list[dict], label: str) -> None:
    if not records:
        return
    rate_limit = _usage_time_rate_limit(records, label)
    previous, lower_anchor, lower_run = None, None, []
    for index, current in enumerate(records):
        if not 0 <= current["raw"] <= 100:
            current["window"]["continuous"] = None
            lower_anchor, lower_run = None, []
            continue
        if previous is None:
            previous = current
            continue
        if lower_anchor is not None:
            if current["raw"] >= lower_anchor["raw"]:
                for candidate in lower_run:
                    candidate["window"]["continuous"] = lower_anchor["raw"]
                previous, lower_anchor, lower_run = lower_anchor, None, []
            elif current["raw"] >= lower_anchor["raw"] - USAGE_TIME_ROUNDING_ALLOWANCE:
                current["window"]["continuous"] = lower_anchor["raw"]
                lower_run.append(current)
                if len(lower_run) >= 3:
                    for candidate in lower_run:
                        candidate["window"]["continuous"] = candidate["raw"]
                    previous = current
                continue
            else:
                if len(lower_run) < 3:
                    previous = lower_anchor
                lower_anchor, lower_run = None, []
        if current["raw"] < previous["raw"]:
            if _usage_time_reset_is_credible(records, index, previous, USAGE_TIME_WINDOWS[label]):
                previous = current
            elif current["raw"] >= previous["raw"] - USAGE_TIME_ROUNDING_ALLOWANCE:
                current["window"]["continuous"] = previous["raw"]
                lower_anchor, lower_run = previous, [current]
            else:
                current["window"]["continuous"] = None
            continue
        elapsed_minutes = (current["timestamp"] - previous["timestamp"]) / 60
        if elapsed_minutes < 0 or current["raw"] - previous["raw"] > USAGE_TIME_ROUNDING_ALLOWANCE + rate_limit * elapsed_minutes:
            current["window"]["continuous"] = None
            continue
        previous = current

def dashboard_quota_points(rows: list[dict], slot_usage_accounts: dict[str, str] | None = None) -> list[dict]:
    merged = {}
    for row in rows:
        point = dashboard_quota_point(row, slot_usage_accounts)
        key = point["usageAccountId"], point.get("checkedAt")
        if key in merged:
            previous = merged[key]
            for label in ("fiveHour", "sevenDay"):
                if point[label]["raw"] is None and previous[label]["raw"] is not None:
                    point[label] = previous[label]
            starts = [value for value in (previous.get("compactedFrom"), point.get("compactedFrom")) if value is not None]
            point["compactedFrom"] = min(starts) if starts else None
        merged[key] = point
    points = list(merged.values())
    for label in USAGE_TIME_WINDOWS:
        groups = {}
        for point in points:
            window = point[label]
            if point["timestamp"] is None or window["raw"] is None:
                window["continuous"] = None
                continue
            groups.setdefault(point["usageAccountId"], []).append({
                "timestamp": point["timestamp"], "raw": window["raw"], "resetAt": parse_timestamp(window.get("resetAt")), "window": window,
            })
        for records in groups.values():
            records.sort(key=lambda record: record["timestamp"])
            _usage_time_continuous_values(records, label)
    return points

def dashboard_token_session(row: dict, slot_usage_accounts: dict[str, str] | None = None) -> dict:
    return {
        "sessionId": row.get("sessionId"),
        "startedAt": row.get("startedAt"),
        "updatedAt": row.get("updatedAt") or row.get("startedAt"),
        "accountSlotId": row.get("accountSlotId"),
        "accountLabel": row.get("accountLabel"),
        "usageAccountId": dashboard_usage_account_id(row, slot_usage_accounts),
        "byModel": {
            model: {"usageTokens": value.get("tokens") or {}, "cost": value.get("cost") or empty_cost_totals()}
            for model, value in (row.get("byModel") or {}).items()
            if isinstance(value, dict)
        },
    }

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

def format_last_update(checked_at: str | None, now: float) -> str:
    timestamp = parse_timestamp(checked_at)
    return "-" if timestamp is None else f"{max(0, int(now - timestamp))}s ago"

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
    percent_checked_at = sample.get("percentCheckedAt") or sample.get("checkedAt")
    last_update_text = format_last_update(percent_checked_at, now)
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
            f"Last update {last_update_text}",
        )),
        "checkedAt": sample.get("checkedAt"),
        "percentCheckedAt": percent_checked_at,
        "lastUpdateText": last_update_text,
        "windows": windows,
    }

def _path_revision(path: Path) -> tuple:
    try:
        stat = path.stat()
    except OSError:
        return ()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, getattr(stat, "st_ctime_ns", 0)

def _dashboard_revision(value) -> str:
    return hashlib.sha256(json.dumps(dashboard_safe_json(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()[:20]

def dashboard_display_factor(timestamp: float | None, now: float) -> int:
    if timestamp is None or now - timestamp <= QUOTA_HISTORY_DISPLAY_FULL_SECONDS:
        return 1
    if now - timestamp <= QUOTA_HISTORY_DISPLAY_X4_SECONDS:
        return 4
    if now - timestamp <= QUOTA_HISTORY_DISPLAY_X8_SECONDS:
        return 8
    return 16

def _dashboard_display_timestamp(value: dict) -> float | None:
    if not isinstance(value, dict):
        return None
    timestamp = coerce_float(value.get("timestamp"))
    return timestamp if timestamp is not None else parse_timestamp(value.get("checkedAt"))

def _dashboard_display_sort_key(value: dict) -> tuple:
    timestamp = _dashboard_display_timestamp(value)
    return timestamp is None, timestamp or 0, str(value.get("usageAccountId") or value.get("accountSlotId") or value.get("accountLabel") or "")

def _dashboard_display_points_are_contiguous(previous: dict | None, current: dict) -> bool:
    previous_timestamp = _dashboard_display_timestamp(previous or {})
    current_timestamp = _dashboard_display_timestamp(current)
    return previous_timestamp is not None and current_timestamp is not None and 0 <= current_timestamp - previous_timestamp <= DASHBOARD_DISPLAY_CACHE_GAP_SECONDS

def _dashboard_display_point_has_discontinuity(point: dict) -> bool:
    return any(window.get("raw") is not None and window.get("continuous") is None for window in (point.get("fiveHour") or {}, point.get("sevenDay") or {}))

def _dashboard_compact_quota_group(group: list[dict]) -> dict:
    if len(group) == 1:
        return group[0]
    compacted = dict(group[-1])
    starts = []
    for point in group:
        compacted_from = coerce_float(point.get("compactedFrom"))
        starts.append(compacted_from if compacted_from is not None else _dashboard_display_timestamp(point))
    starts = [start for start in starts if start is not None]
    if starts:
        compacted["compactedFrom"] = min(starts)
    return compacted

def _dashboard_compact_quota_account_points(points: list[dict], now: float) -> list[dict]:
    compacted = []
    pending = []
    pending_factor = None
    previous = None
    for point in sorted(points, key=_dashboard_display_sort_key):
        timestamp = _dashboard_display_timestamp(point)
        factor = dashboard_display_factor(timestamp, now)
        if factor == 1 or timestamp is None or _dashboard_display_point_has_discontinuity(point):
            if pending:
                compacted.append(_dashboard_compact_quota_group(pending))
                pending = []
                pending_factor = None
            compacted.append(point)
        elif not pending or factor != pending_factor or not _dashboard_display_points_are_contiguous(previous, point):
            if pending:
                compacted.append(_dashboard_compact_quota_group(pending))
            pending = [point]
            pending_factor = factor
        else:
            pending.append(point)
        if pending and len(pending) >= pending_factor:
            compacted.append(_dashboard_compact_quota_group(pending))
            pending = []
            pending_factor = None
        previous = point
    if pending:
        compacted.append(_dashboard_compact_quota_group(pending))
    return compacted

def dashboard_compact_quota_points(points: list[dict], now: float | None = None) -> list[dict]:
    now = time.time() if now is None else now
    by_account = {}
    for point in points:
        key = point.get("usageAccountId") or point.get("accountSlotId") or point.get("accountLabel") or "unknown"
        by_account.setdefault(key, []).append(point)
    compacted = [point for points_for_account in by_account.values() for point in _dashboard_compact_quota_account_points(points_for_account, now)]
    return sorted(compacted, key=_dashboard_display_sort_key)

def _dashboard_compact_event_group(group: list[dict]) -> dict:
    if len(group) == 1:
        return group[0]
    first, last = group[0], group[-1]
    delta_percent = sum(coerce_float(event.get("deltaPercent")) or 0.0 for event in group)
    delta_cost = sum(coerce_float(event.get("deltaCostUsd")) or 0.0 for event in group)
    first_cumulative_percent = coerce_float(first.get("cumulativePercent")) or 0.0
    first_cumulative_cost = coerce_float(first.get("cumulativeCostUsd")) or 0.0
    ratio = cost_percent_ratio(delta_cost, delta_percent)
    average_ratio = cost_percent_ratio(first_cumulative_cost - (coerce_float(first.get("deltaCostUsd")) or 0.0), first_cumulative_percent - (coerce_float(first.get("deltaPercent")) or 0.0))
    merged_count = sum(int(coerce_float(event.get("mergedCount")) or 1) for event in group)
    compacted = dict(last)
    compacted.update({
        "deltaPercent": round(delta_percent, 8),
        "deltaCostUsd": round(delta_cost, 8),
        "costPercentRatio": None if ratio is None else round(ratio, 8),
        "averageCostPercentRatio": None if average_ratio is None else round(average_ratio, 8),
        "ratioDeviation": None if ratio_deviation(ratio, average_ratio) is None else round(ratio_deviation(ratio, average_ratio), 8),
        "ratioDeviationWarning": ratio_deviation_warning(ratio, average_ratio),
        "mergedCount": merged_count,
        "mergedFrom": first.get("mergedFrom") or first.get("checkedAt"),
        "mergedTo": last.get("mergedTo") or last.get("checkedAt"),
    })
    if coerce_float(last.get("cumulativePercent")) is None:
        compacted["cumulativePercent"] = round(first_cumulative_percent + delta_percent, 8)
    if coerce_float(last.get("cumulativeCostUsd")) is None:
        compacted["cumulativeCostUsd"] = round(first_cumulative_cost + delta_cost, 8)
    compacted.pop("rawKeys", None)
    return compacted

def _dashboard_event_sort_key(event: dict) -> tuple:
    timestamp = _dashboard_display_timestamp(event)
    return timestamp is None, timestamp or 0, str(event.get("usageAccountId") or event.get("accountSlotId") or ""), str(event.get("model") or "")

def dashboard_compact_event_series(events: list[dict], now: float | None = None) -> list[dict]:
    now = time.time() if now is None else now
    synthetic = [event for event in events if event.get("synthetic")]
    streams = {}
    standalone = []
    for event in events:
        if event.get("synthetic"):
            continue
        timestamp = _dashboard_display_timestamp(event)
        if timestamp is None:
            standalone.append(event)
            continue
        key = (event.get("usageAccountId") or event.get("accountSlotId") or "unknown", event.get("model") or "unknown")
        streams.setdefault(key, []).append(event)
    compacted = list(standalone)
    for stream in streams.values():
        pending = []
        pending_factor = None
        previous = None
        for event in sorted(stream, key=_dashboard_event_sort_key):
            factor = dashboard_display_factor(_dashboard_display_timestamp(event), now)
            if factor == 1 or not pending or factor != pending_factor or not _dashboard_display_points_are_contiguous(previous, event):
                if pending:
                    compacted.append(_dashboard_compact_event_group(pending))
                pending = [event]
                pending_factor = factor
            else:
                pending.append(event)
            if len(pending) >= pending_factor:
                compacted.append(_dashboard_compact_event_group(pending))
                pending = []
                pending_factor = None
            previous = event
        if pending:
            compacted.append(_dashboard_compact_event_group(pending))
    return synthetic + sorted(compacted, key=_dashboard_event_sort_key)

def dashboard_compact_events(events: dict[str, list[dict]], now: float | None = None) -> dict[str, list[dict]]:
    return {label: dashboard_compact_event_series(values, now) for label, values in events.items()}

def _dashboard_display_next_maintenance_at(quota_points: list[dict], events: dict[str, list[dict]], now: float) -> float | None:
    current_hour = math.floor(now / HOUR_MULTIPLIER)
    timestamps = [_dashboard_display_timestamp(point) for point in quota_points]
    timestamps.extend(_dashboard_display_timestamp(event) for values in events.values() for event in values)
    candidates = []
    for timestamp in (value for value in timestamps if value is not None):
        for threshold in (QUOTA_HISTORY_DISPLAY_FULL_SECONDS, QUOTA_HISTORY_DISPLAY_X4_SECONDS, QUOTA_HISTORY_DISPLAY_X8_SECONDS):
            # Round up so the hour-resolution deadline never falls in the past.
            candidate_hour = math.ceil((timestamp + threshold + 1) / HOUR_MULTIPLIER)
            if candidate_hour > current_hour:
                candidates.append(candidate_hour)
    return min(candidates, default=None) * HOUR_MULTIPLIER if candidates else None

def _dashboard_display_data(history: list[dict], quota_history: list[dict], token_sessions: list[dict], accounts: dict, now: float | None = None) -> dict:
    now = time.time() if now is None else now
    slot_usage_accounts = {str(account.get("id")): str(account.get("usageAccountId")) for account in accounts.get("items", []) if account.get("id") and account.get("usageAccountId")}
    events = derive_history_events(history)
    for values in events.values():
        for event in values:
            if not event.get("synthetic"):
                event["usageAccountId"] = dashboard_usage_account_id(event, slot_usage_accounts)
    active = next((item for item in accounts.get("items", []) if item.get("id") == accounts.get("activeAccountId")), None)
    if active and active.get("isApiAccount"):
        events, quota_points = {"fiveHour": [], "sevenDay": []}, []
    else:
        quota_points = dashboard_quota_points(quota_history, slot_usage_accounts)
    history_stats = {
        "rows": len(history),
        "fiveHourEvents": len(events["fiveHour"]),
        "sevenDayEvents": len(events["sevenDay"]),
        "fiveHourRealEvents": sum(1 for event in events["fiveHour"] if not event.get("synthetic")),
        "sevenDayRealEvents": sum(1 for event in events["sevenDay"] if not event.get("synthetic")),
    }
    compacted_events = dashboard_compact_events(events, now)
    return {
        "quotaPoints": _dashboard_key_rows("quota", dashboard_compact_quota_points(quota_points, now)),
        "tokenSessions": _dashboard_key_rows("token", [dashboard_token_session(row, slot_usage_accounts) for row in token_sessions]),
        "events": {name: _dashboard_key_rows("event", values) for name, values in compacted_events.items()},
        "historyStats": history_stats,
        "nextMaintenanceAt": _dashboard_display_next_maintenance_at(quota_points, events, now),
    }

def _dashboard_row_key(kind: str, identity) -> str:
    return f"{kind}:{_dashboard_revision(identity)}"

def _dashboard_key_rows(kind: str, rows: list[dict]) -> list[dict]:
    identities = []
    for row in rows:
        if kind == "quota":
            identity = (row.get("usageAccountId"), row.get("checkedAt"))
        elif kind == "token":
            identity = (row.get("usageAccountId"), row.get("sessionId"))
        elif row.get("synthetic"):
            identity = (row.get("window"), "baseline")
        else:
            identity = (
                row.get("window"), row.get("usageAccountId") or row.get("accountSlotId"), row.get("model"),
                row.get("mergedFrom") or row.get("checkedAt"), row.get("mergedTo") or row.get("checkedAt"),
                coerce_float(row.get("deltaPercent")), coerce_float(row.get("deltaCostUsd")),
            )
        identities.append(identity)
    occurrences = {}
    keyed = []
    for row, identity in zip(rows, identities):
        ordinal = occurrences.get(identity, 0)
        occurrences[identity] = ordinal + 1
        keyed.append(row | {"_key": _dashboard_row_key(kind, (identity, ordinal))})
    return keyed

def _dashboard_collection_changes(before: list[dict], after: list[dict]) -> dict | None:
    old = {row.get("_key"): row for row in before if row.get("_key")}
    new = {row.get("_key"): row for row in after if row.get("_key")}
    upsert = [row for row in after if row.get("_key") and old.get(row["_key"]) != row]
    deleted = [key for key in old if key not in new]
    return {"upsert": upsert, "delete": deleted} if upsert or deleted else None

def dashboard_view_changes(before: dict, after: dict) -> dict:
    changes = {}
    for name in ("quotaPoints", "tokenSessions"):
        if collection := _dashboard_collection_changes(before.get(name) or [], after.get(name) or []):
            changes[name] = collection
    event_changes = {}
    for name in ("fiveHour", "sevenDay"):
        if collection := _dashboard_collection_changes((before.get("events") or {}).get(name) or [], (after.get("events") or {}).get(name) or []):
            event_changes[name] = collection
    if event_changes:
        changes["events"] = event_changes
    if before.get("historyStats") != after.get("historyStats"):
        changes["historyStats"] = after.get("historyStats") or {}
    return changes

def dashboard_transfer_view(display: dict | None) -> dict:
    display = display or {}
    return {
        "quotaPoints": display.get("quotaPoints") or [],
        "tokenSessions": display.get("tokenSessions") or [],
        "events": {
            "fiveHour": (display.get("events") or {}).get("fiveHour") or [],
            "sevenDay": (display.get("events") or {}).get("sevenDay") or [],
        },
        "historyStats": display.get("historyStats") or {},
    }

class DashboardDisplayCache:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.payload = None
        self.load()

    def load(self) -> None:
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
            self.payload = None
            return
        self.payload = payload if isinstance(payload, dict) and payload.get("version") == DASHBOARD_DISPLAY_CACHE_VERSION and isinstance(payload.get("views"), dict) else None

    def entry(self, view: str, source_revision: str, now: float) -> dict | None:
        payload = self.payload
        if not payload or payload.get("sourceRevision") != source_revision:
            return None
        next_maintenance = coerce_float(payload.get("nextMaintenanceAt"))
        if next_maintenance is not None and now >= next_maintenance:
            return None
        entry = payload.get("views", {}).get(view)
        required = ("quotaPoints", "tokenSessions", "events", "historyStats")
        return entry if isinstance(entry, dict) and all(key in entry for key in required) else None

    def revision(self, source_revision: str, now: float) -> str:
        if self.entry("local", source_revision, now) is not None or self.entry("merged", source_revision, now) is not None:
            return str(self.payload.get("displayRevision") or "legacy")
        return "stale" if self.payload and self.payload.get("sourceRevision") == source_revision else "missing"

    def next_maintenance_at(self) -> float | None:
        return coerce_float((self.payload or {}).get("nextMaintenanceAt"))

    def replace(self, payload: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
                stream.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
                stream.flush()
                os.fsync(stream.fileno())
            try:
                os.chmod(temp_name, 0o600)
            except OSError:
                pass
            os.replace(temp_name, self.path)
        except Exception:
            try:
                os.unlink(temp_name)
            except OSError:
                pass
            raise
        self.payload = payload

class UsageDashboardState:
    def __init__(self, args, opener: urllib.request.OpenerDirector | None):
        self.args = args
        if not getattr(self.args, "quota_history", None):
            self.args.quota_history = default_quota_history_path(self.args.history)
        if not getattr(self.args, "token_session_history", None):
            self.args.token_session_history = default_token_session_history_path(self.args.history)
        if not getattr(self.args, "token_ledger", None):
            self.args.token_ledger = default_token_ledger_path(self.args.history)
        if not getattr(self.args, "dashboard_cache", None):
            self.args.dashboard_cache = default_dashboard_cache_path(self.args.history)
        self.dashboard_cache = DashboardDisplayCache(self.args.dashboard_cache)
        self.opener = opener
        self.lock = threading.RLock()
        self.accounts = AccountManager(args.auth, getattr(args, "account_root", None), getattr(args, "legacy_account_root", None))
        self.skills = SkillManager(args.codex_home, getattr(args, "data_home", None) or args.auth.parent / ".codex-switch", getattr(args, "gemini_skills", None))
        self.cloud = CloudManager(self.skills.private_root, self.skills, self.accounts)
        self.accounts.cloud = self.cloud
        self.usage_data = UsageDataStore(
            self.args.history, self.args.quota_history, self.args.token_session_history, self.cloud.machine_id, self.cloud.usage_account_id, self.lock, self.cloud.local_usage_account,
            getattr(self.args, "usage_sync_cache", None), self.cloud.usage_account_revision,
        )
        self.args.usage_sync_cache = self.usage_data.cache_path
        self.usage_data.normalize_local()
        if self.usage_data.needs_remote_rebuild:
            self.cloud.reset_usage_apply_cursors()
            self.usage_data.needs_remote_rebuild = False
        self.cloud.configure_usage_sync(self.usage_data)
        try:
            self.recovered_account_transition = self.cloud.recover_account_transition()
        except CloudError as exc:
            self.recovered_account_transition = None
            self.accounts.error = f"Cloud account recovery requires attention: {exc}"
        self.projection_errors = self.skills.reconcile()
        self.skills.status()
        self.skills.scan(refresh=True)
        self.args.auth_lock = self.accounts.lock
        self.args.auth_refreshed_callback = self.sync_active_account_from_live
        self.args.account_attribution_callback = self.accounts.attribution_for_auth
        self.wake_event = threading.Event()
        self.inactive_account_poll_event = threading.Event()
        self.cloud_maintenance_event = threading.Event()
        self.config_monitor_event = threading.Event()
        self.dashboard_cache_event = threading.Event()
        self.cloud_maintenance_connection_failed = False
        self.running = True
        self.last_sample = None
        self.last_error = None
        self.last_acquire_started_at = None
        self.inactive_account_poll_started_at = {}
        self.inactive_account_poll_errors = {}
        self.session_refresh_event = threading.Event()
        self.session_refresh_attempts = {}
        self.session_refresh_costs = {}
        self.session_refresh_reset_at = {}
        self.session_refresh_procedures = set()
        self.session_refresh_failed = {}
        self.session_refresh_suppressed_reset_at = {}
        self.external_auth_validation_threads = set()
        self.external_auth_validation_fingerprints = set()
        self._series_build_lock = threading.Lock()
        self.dashboard_cache_event.set()
        self.runtime_state = reset_runtime_baselines(load_state(args.state))
        account_status = self.accounts.status()
        self.account_statuses = {account["id"]: None for account in account_status["items"]}
        if not self.runtime_state.get("activeAccountSlotId"):
            active_account_id = account_status["activeAccountId"]
            self.runtime_state["activeAccountSlotId"] = active_account_id
            if isinstance(self.runtime_state.get("lastSample"), dict) and not self.runtime_state["lastSample"].get("activeAccountSlotId"):
                self.runtime_state["lastSample"]["activeAccountSlotId"] = active_account_id
            write_state(args.state, self.runtime_state)
        self.sync_active_account_from_live()

    def _update_account_status_locked(self, account_id: str | None, sample: dict) -> bool:
        if sample.get("rejectedWindows") or sample.get("usingPreviousWindows") or (sample.get("remoteUsage") or {}).get("accepted") is False:
            return False
        row = quota_history_row_from_sample(sample)
        if not account_id or row is None:
            return False
        previous = self.account_statuses.get(account_id)
        if isinstance(previous, dict) and (parse_timestamp(row["checkedAt"]) or 0) < (parse_timestamp(previous.get("percentCheckedAt") or previous.get("checkedAt")) or 0):
            return False
        label = sample.get("accountLabel") or row.get("accountLabel") or next((account["label"] for account in self.accounts.status()["items"] if account["id"] == account_id), "Unknown")
        self.account_statuses[account_id] = sample | {
            "checkedAt": sample.get("checkedAt") or row["checkedAt"],
            "percentCheckedAt": row["checkedAt"],
            "accountSlotId": account_id,
            "accountLabel": label,
            "activeAccountSlotId": account_id,
            "windows": ((previous or {}).get("windows") or {}) | row["windows"],
        }
        if event := getattr(self, "session_refresh_event", None):
            event.set()
        return True

    def _active_account_status_locked(self, accounts: dict) -> dict | None:
        if accounts["awaitingLogin"]:
            return None
        if not hasattr(self, "account_statuses"):
            self.account_statuses = {}
        for account in accounts["items"]:
            self.account_statuses.setdefault(account["id"], None)
        active_id = accounts["activeAccountId"]
        candidate = self.last_sample or getattr(self, "runtime_state", {}).get("lastSample")
        if self.account_statuses.get(active_id) is None and isinstance(candidate, dict) and candidate.get("activeAccountSlotId") == active_id:
            self._update_account_status_locked(active_id, candidate)
        active = next((account for account in accounts["items"] if account["id"] == active_id), None)
        shared = [
            self.account_statuses.get(account["id"])
            for account in accounts["items"]
            if active is not None and account.get("usageAccountId") == active.get("usageAccountId") and isinstance(self.account_statuses.get(account["id"]), dict)
        ]
        if not shared:
            return None
        latest = max(shared, key=lambda sample: parse_timestamp(sample.get("percentCheckedAt") or sample.get("checkedAt")) or 0)
        return latest | {"activeAccountSlotId": active_id, "accountSlotId": active_id, "accountLabel": active.get("label") or "Unknown"}

    def _dashboard_accounts_locked(self) -> dict:
        status = dashboard_accounts(self)
        for account in status["items"]:
            if error := getattr(self, "inactive_account_poll_errors", {}).get(account["id"]):
                account.update({"pollError": True, "pollErrorAt": error["at"], "stale": True})
        return status

    def _series_source_revision_locked(self, accounts: dict) -> str:
        token_ledger = getattr(self.args, "token_ledger", default_token_ledger_path(self.args.history))
        return _dashboard_revision({
            "files": [_path_revision(path) for path in (self.args.history, self.args.quota_history, self.args.token_session_history, token_ledger, self.args.state, getattr(self.args, "usage_sync_cache", default_usage_sync_cache_path(self.args.history)))],
            "accounts": accounts,
        })

    def _ensure_series_stream_state_locked(self) -> None:
        if hasattr(self, "_series_stream_id"):
            return
        self._series_stream_id = secrets.token_hex(10)
        self._series_index = 0
        self._series_journal = deque()
        self._series_journal_bytes = 0
        cached_views = ((getattr(self, "dashboard_cache", None).payload or {}).get("views") or {}) if getattr(self, "dashboard_cache", None) is not None else {}
        self._series_views = {name: dashboard_transfer_view(cached_views.get(name)) for name in ("local", "merged")} if all(isinstance(cached_views.get(name), dict) for name in ("local", "merged")) else None
        self._series_initialized = self._series_views is not None
        self._merged_revision = _dashboard_revision({"streamId": self._series_stream_id, "merged": (self._series_views or {}).get("merged")})
        self._dashboard_cache_reasons = set()

    def _append_series_batch_locked(self, local_changes: dict, merged_changes: dict, now: float) -> None:
        self._series_index += 1
        batch = {"index": self._series_index, "createdAt": now, "local": local_changes, "merged": merged_changes}
        batch["bytes"] = len(json.dumps(dashboard_safe_json(batch), ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        self._series_journal.append(batch)
        self._series_journal_bytes += batch["bytes"]
        self._prune_series_journal_locked(now)

    def _prune_series_journal_locked(self, now: float) -> None:
        cutoff = now - DASHBOARD_SERIES_JOURNAL_MAX_AGE_SECONDS
        while self._series_journal and (
            len(self._series_journal) > DASHBOARD_SERIES_JOURNAL_MAX_BATCHES
            or self._series_journal_bytes > DASHBOARD_SERIES_JOURNAL_MAX_BYTES
            or self._series_journal[0]["createdAt"] < cutoff
        ):
            self._series_journal_bytes -= self._series_journal.popleft()["bytes"]

    def _series_snapshot_locked(self, status: dict | None = None) -> dict:
        status = status or self.status_payload()
        return {
            "protocolVersion": DASHBOARD_SERIES_PROTOCOL_VERSION,
            "mode": "snapshot",
            "streamId": self._series_stream_id,
            "includedIndex": self._series_index,
            "mergedRevision": self._merged_revision,
            "status": status,
            "statusEtag": f'W/"status-{status["revision"]}"',
            "views": self._series_views or {"local": dashboard_transfer_view(None), "merged": dashboard_transfer_view(None)},
        }

    def status_payload(self) -> dict:
        with self.lock:
            self._ensure_series_stream_state_locked()
            self._prune_series_journal_locked(time.time())
            accounts = self._dashboard_accounts_locked()
            if not accounts["awaitingLogin"] and self.last_error and self.last_error.startswith("Waiting for Codex login"):
                self.wake_event.set()
            last_sample = self._active_account_status_locked(accounts)
            sample = dashboard_sample(last_sample)
            control_password_configured = control_password_is_configured(self.cloud.config()["control"]) if hasattr(self, "cloud") else True
            stream = {"streamId": self._series_stream_id, "newestIndex": self._series_index, "mergedRevision": self._merged_revision}
            return {
                "revision": _dashboard_revision({"sample": sample, "accounts": accounts, "error": self.last_error, "controlPasswordConfigured": control_password_configured, "stream": stream}),
                **stream,
                "controlPasswordConfigured": control_password_configured,
                "lastSample": sample,
                "display": dashboard_display(last_sample),
                "accounts": accounts,
            }

    def series_response(self, stream_id: str | None = None, included_index: int | None = None, merged_revision: str | None = None) -> dict:
        self.refresh_dashboard_cache()
        status = self.status_payload()
        with self.lock:
            self._ensure_series_stream_state_locked()
            if stream_id is None and included_index is None and merged_revision is None:
                return self._series_snapshot_locked(status)
            if stream_id != self._series_stream_id or included_index is None or included_index < 0 or included_index > self._series_index:
                return self._series_snapshot_locked(status)
            batches = [batch for batch in self._series_journal if batch["index"] > included_index]
            if included_index < self._series_index and (not batches or batches[0]["index"] != included_index + 1 or batches[-1]["index"] != self._series_index):
                return self._series_snapshot_locked(status)
            replace_merged = merged_revision != self._merged_revision
            return {
                "protocolVersion": DASHBOARD_SERIES_PROTOCOL_VERSION,
                "mode": "update",
                "streamId": self._series_stream_id,
                "fromIndex": included_index,
                "includedIndex": self._series_index,
                "mergedRevision": self._merged_revision,
                "batches": [{"index": batch["index"], "local": batch["local"], **({} if replace_merged else {"merged": batch["merged"]})} for batch in batches],
                **({"mergedSnapshot": self._series_views["merged"]} if replace_merged else {}),
            }

    def refresh_dashboard_cache(self, force: bool = False, now: float | None = None) -> bool:
        cache = getattr(self, "dashboard_cache", None)
        if cache is None:
            return False
        now = time.time() if now is None else now
        with self._series_build_lock:
            with self.lock:
                self._ensure_series_stream_state_locked()
                accounts = self._dashboard_accounts_locked()
                source_revision = self._series_source_revision_locked(accounts)
                if not force and cache.entry("local", source_revision, now) is not None and cache.entry("merged", source_revision, now) is not None:
                    self._dashboard_cache_reasons.clear()
                    return False
                reasons = set(self._dashboard_cache_reasons) or {"local"}
                display_data = {}
                for view in ("local", "merged"):
                    history, quota_history, token_sessions = self.usage_data.datasets(view) if hasattr(self, "usage_data") else (load_history(self.args.history), load_quota_history(self.args.quota_history), load_token_session_history(self.args.token_session_history))
                    display_data[view] = _dashboard_display_data(history, quota_history, token_sessions, accounts, now)
                source_revision = self._series_source_revision_locked(accounts)
                next_maintenance_values = [value.get("nextMaintenanceAt") for value in display_data.values() if value.get("nextMaintenanceAt") is not None]
                next_maintenance = min(next_maintenance_values, default=None)
                payload = {
                    "version": DASHBOARD_DISPLAY_CACHE_VERSION,
                    "sourceRevision": source_revision,
                    "builtAt": now,
                    "nextMaintenanceAt": next_maintenance,
                    "displayRevision": _dashboard_revision({"version": DASHBOARD_DISPLAY_CACHE_VERSION, "views": {name: dashboard_transfer_view(value) for name, value in display_data.items()}}),
                    "views": display_data,
                }
                views = {name: dashboard_transfer_view(value) for name, value in display_data.items()}
                previous = self._series_views
                cache.replace(payload)
                self._series_views = views
                self._dashboard_cache_reasons.difference_update(reasons)
                if not self._series_initialized:
                    self._series_initialized = True
                    self._merged_revision = _dashboard_revision({"streamId": self._series_stream_id, "merged": views["merged"]})
                else:
                    local_changes = dashboard_view_changes(previous["local"], views["local"])
                    merged_changes = dashboard_view_changes(previous["merged"], views["merged"])
                    if "cloud" in reasons and merged_changes:
                        self._merged_revision = _dashboard_revision({"streamId": self._series_stream_id, "previous": self._merged_revision, "merged": views["merged"]})
                    if local_changes or (merged_changes and "cloud" not in reasons):
                        self._append_series_batch_locked(local_changes, merged_changes, now)
                return True

    def dashboard_cache_wait_seconds(self, now: float | None = None) -> float:
        now = time.time() if now is None else now
        with self.lock:
            next_maintenance = getattr(self, "dashboard_cache", None)
            next_maintenance = next_maintenance.next_maintenance_at() if next_maintenance is not None else None
        return DASHBOARD_DISPLAY_CACHE_MAINTENANCE_SECONDS if next_maintenance is None or next_maintenance <= now else min(DASHBOARD_DISPLAY_CACHE_MAINTENANCE_SECONDS, max(1, next_maintenance - now))

    def run_dashboard_cache_maintenance(self) -> None:
        while self.running:
            self.dashboard_cache_event.wait(self.dashboard_cache_wait_seconds())
            self.dashboard_cache_event.clear()
            try:
                self.refresh_dashboard_cache()
            except Exception as exc:
                print(f"Dashboard display cache maintenance failed: {exc}", file=sys.stderr, flush=True)

    def _signal_dashboard_cache(self, reason: str = "local") -> None:
        with self.lock:
            self._ensure_series_stream_state_locked()
            self._dashboard_cache_reasons.add("cloud" if reason == "cloud" else "local")
        event = getattr(self, "dashboard_cache_event", None)
        if event is not None:
            event.set()

    def history(self) -> list[dict]:
        with self.lock:
            return load_history(self.args.history)

    def state(self) -> dict:
        with self.lock:
            return load_state(self.args.state)

    def quota_history(self) -> list[dict]:
        with self.lock:
            return load_quota_history(self.args.quota_history)

    def token_session_history(self) -> list[dict]:
        with self.lock:
            return load_token_session_history(self.args.token_session_history)

    def poll_once(self) -> dict:
        account_status = self.accounts.status()
        if not self.args.local_only and account_status["awaitingLogin"]:
            with self.lock:
                self.last_error = account_status["error"] or "Waiting for Codex login to create auth.json"
                self.last_acquire_started_at = time.monotonic()
            return {}
        self.sync_active_account_from_live()
        account_status = self.accounts.status()
        with self.lock:
            history = load_history(self.args.history)
            self.last_acquire_started_at = time.monotonic()
            previous_token_usage = self.runtime_state.get("tokenUsage")
            previous_cost = self.runtime_state.get("cost")
        sample = collect_with_bad_remote_usage_retry(
            lambda: collect_usage_sample(self.args, self.opener, previous_token_usage, previous_cost, self.runtime_state),
            self.runtime_state,
        )
        if self.accounts.status()["activeAccountId"] != account_status["activeAccountId"]:
            return {}
        sample["activeAccountSlotId"] = account_status["activeAccountId"]
        sample["accountSlotId"] = account_status["activeAccountId"]
        sample["accountLabel"] = next((account["label"] for account in account_status["items"] if account["id"] == account_status["activeAccountId"]), "Unknown")
        sample["isApiAccount"] = bool(next((account.get("isApiAccount") for account in account_status["items"] if account["id"] == account_status["activeAccountId"]), False))
        sample["originMachineId"] = self.cloud.machine_id
        sample["usageAccountId"] = self.cloud.usage_account_id(account_status["activeAccountId"])
        sample["sync"] = {"version": 1, "originMachineId": self.cloud.machine_id, "accountId": sample["usageAccountId"]}
        api_account = is_api_auth(json.loads(self.args.auth.read_text(encoding="utf-8")))
        with self.lock:
            token_usage = sample.get("tokenUsage") or {}
            if token_usage:
                token_sessions = sync_token_ledger(
                    self.args.token_ledger,
                    load_token_session_history(self.args.token_session_history),
                    token_usage.get("events") or [],
                    account_status["activeAccountId"],
                    sample["accountLabel"],
                    self.accounts.attribution_timeline(),
                )
                write_token_session_history(self.args.token_session_history, token_sessions)
                sample["cost"], sample["costByModel"] = token_cost_snapshot(token_sessions)
                sample["costDelta"] = cost_progress(sample["cost"], previous_cost)
                sample["costDeltaByModel"] = cost_progress_by_model(sample["costByModel"], self.runtime_state.get("costByModel"))
                token_usage.pop("sessions", None)
                token_usage.pop("events", None)
            self.runtime_state["activeAccountSlotId"] = account_status["activeAccountId"]
            apply_runtime_cost_measurement(sample, self.runtime_state)
            if not api_account:
                append_quota_history_sample(self.args.quota_history, sample)
            events = process_sample_delta_events(self.runtime_state, sample, history)
            if api_account:
                events = []
            append_capped_jsonl(self.args.sample_log, sample_debug_log_row(sample, events, self.runtime_state), self.args.sample_log_max_bytes)
            for interval in self.runtime_state.pop("_pendingCostIntervals", []):
                append_history(self.args.history, add_record_provenance("cost", interval, self.cloud.machine_id, sample["usageAccountId"]))
            write_state(self.args.state, self.runtime_state)
            compact_history(self.args.history, self.args.compact_history_days)
            compact_quota_history(self.args.quota_history, self.args.compact_history_days)
            self.usage_data.normalize_local()
            self._signal_dashboard_cache()
            print_special_events(self.runtime_state.get("_specialEvents") or [])
            print_valid_delta_events(events, sample)
            print_ratio_warnings(events)
            self.last_sample = sample
            self._update_account_status_locked(account_status["activeAccountId"], sample)
            self.last_error = None
            return sample

    def _poll_inactive_account(self, credential: dict) -> None:
        fd, temp_name = tempfile.mkstemp(prefix=".inactive-usage-", suffix=".json", dir=self.accounts.root)
        temp_path = Path(temp_name)
        expected_fingerprint = credential["fingerprint"]
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(credential["data"])
                stream.flush()
                os.fsync(stream.fileno())
            auth = json.loads(credential["data"].decode("utf-8"))
            if is_api_auth(auth):
                return

            def commit_refreshed_credentials():
                nonlocal expected_fingerprint
                data = temp_path.read_bytes()
                if not self.accounts.commit_polled_credentials(credential["id"], expected_fingerprint, data):
                    raise UsageError("recorded credentials changed during token refresh")
                expected_fingerprint = auth_fingerprint(data)

            debug = {}
            output = fetch_usage_with_percent_arbitration(
                auth, self.opener, temp_path, max(self.args.timeout, 1), debug, getattr(self.args, "retry_limit", DEFAULT_RETRY_LIMIT), refreshed_callback=commit_refreshed_credentials,
            )
            usage_account_id = self.cloud.usage_account_id(credential["id"])
            output.update({"checkedAt": now_iso(), "accountSlotId": credential["id"], "accountLabel": credential["label"], "remoteUsage": debug, "sync": {"version": 1, "originMachineId": self.cloud.machine_id, "accountId": usage_account_id}})
            sample = make_history_sample(output, None)
            with self.lock:
                append_quota_history_sample(self.args.quota_history, sample)
                self._update_account_status_locked(credential["id"], sample)
        finally:
            try:
                if temp_path.exists():
                    self.accounts.commit_polled_credentials(credential["id"], expected_fingerprint, temp_path.read_bytes())
            finally:
                temp_path.unlink(missing_ok=True)

    def poll_due_inactive_accounts(self, now: float | None = None) -> int:
        if self.args.local_only or self.opener is None:
            return 0
        now = time.monotonic() if now is None else now
        polled = 0
        for credential in self.accounts.inactive_ready_credentials():
            if now - self.inactive_account_poll_started_at.get(credential["id"], float("-inf")) < INACTIVE_ACCOUNT_POLL_INTERVAL_SECONDS:
                continue
            self.inactive_account_poll_started_at[credential["id"]] = now
            try:
                if is_api_auth(json.loads(credential["data"].decode("utf-8"))):
                    continue
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            try:
                self._poll_inactive_account(credential)
                with self.lock:
                    self.inactive_account_poll_errors.pop(credential["id"], None)
                polled += 1
            except Exception as exc:
                with self.lock:
                    self.inactive_account_poll_errors[credential["id"]] = {"at": now_iso(), "error": str(exc)}
                print(f"Inactive account usage polling failed for {credential['label']!r}: {exc}", file=sys.stderr, flush=True)
        if polled:
            with self.lock:
                compact_quota_history(self.args.quota_history, self.args.compact_history_days)
                self.usage_data.normalize_local()
                self._signal_dashboard_cache()
        return polled

    def inactive_account_poll_wait_seconds(self, now: float | None = None) -> float:
        now = time.monotonic() if now is None else now
        return min((max(INACTIVE_ACCOUNT_POLL_INTERVAL_SECONDS - (now - self.inactive_account_poll_started_at.get(credential["id"], float("-inf"))), 0) for credential in self.accounts.inactive_ready_credentials()), default=INACTIVE_ACCOUNT_POLL_INTERVAL_SECONDS)

    def run_inactive_account_polling(self) -> None:
        while self.running:
            self.poll_due_inactive_accounts()
            self.inactive_account_poll_event.wait(self.inactive_account_poll_wait_seconds())
            self.inactive_account_poll_event.clear()

    @staticmethod
    def _session_refresh_enabled(credential: dict, label: str) -> bool:
        config = credential.get("sessionRefresh")
        if not isinstance(config, dict):
            return True
        return bool(((config.get("fiveHour") if label == "5h" else config.get("sevenDay") if label == "7d" else {}) or {}).get("enabled"))

    @staticmethod
    def _session_refresh_window_allows(credential: dict, label: str, now: float) -> bool:
        if label != "5h":
            return True
        windows = (((credential.get("sessionRefresh") or {}).get("fiveHour") or {}).get("windowsUtc") or [])
        if not windows:
            return True
        minute = now % (24 * 60 * 60) / 60
        return any(int(window["start"][:2]) * 60 + int(window["start"][3:]) <= minute < (24 * 60 if window["end"] == "24:00" else int(window["end"][:2]) * 60 + int(window["end"][3:])) for window in windows)

    def _session_refresh_schedule_wait_seconds(self, now: float) -> float:
        waits = []
        second = now % (24 * 60 * 60)
        for credential in self.accounts.session_refresh_credentials():
            if not self._session_refresh_enabled(credential, "5h") or self._session_refresh_window_allows(credential, "5h", now):
                continue
            for window in (((credential.get("sessionRefresh") or {}).get("fiveHour") or {}).get("windowsUtc") or []):
                start = (int(window["start"][:2]) * 60 + int(window["start"][3:])) * 60
                waits.append((start - second) % (24 * 60 * 60) or 24 * 60 * 60)
        return min(waits, default=10 * 60)

    @staticmethod
    def _session_needs_refresh(label: str, window: dict, now: float) -> bool:
        used_percent = coerce_float(window.get("usedPercent"))
        reset_at = parse_timestamp(window.get("resetAt"))
        duration = SESSION_REFRESH_WINDOW_SECONDS.get(label)
        return used_percent == 0 and reset_at is not None and duration is not None and 0 <= now + duration - reset_at <= SESSION_REFRESH_RESET_LATENCY_SECONDS

    @staticmethod
    def _session_refresh_succeeded(previous_reset_at: float | None, current_reset_at: float | None) -> bool:
        return previous_reset_at is not None and current_reset_at is not None and abs(current_reset_at - previous_reset_at) < SESSION_REFRESH_SUCCESS_TOLERANCE_SECONDS

    def _retrieve_session_refresh_reset_at(self, credential: dict, label: str) -> float | None:
        auth_path = self.args.auth if credential["active"] else self.accounts._account_path(credential["id"])
        auth_data = auth_path.read_bytes()
        expected_fingerprint = auth_fingerprint(auth_data)
        temp_path = auth_path
        try:
            if not credential["active"]:
                fd, temp_name = tempfile.mkstemp(prefix=".session-refresh-usage-", suffix=".json", dir=self.accounts.root)
                temp_path = Path(temp_name)
                with os.fdopen(fd, "wb") as stream:
                    stream.write(auth_data)
                    stream.flush()
                    os.fsync(stream.fileno())

            def commit_refreshed_credentials():
                nonlocal expected_fingerprint
                if credential["active"]:
                    self.sync_active_account_from_live()
                    return
                data = temp_path.read_bytes()
                if not self.accounts.commit_polled_credentials(credential["id"], expected_fingerprint, data):
                    raise UsageError("recorded credentials changed during token refresh")
                expected_fingerprint = auth_fingerprint(data)

            output = fetch_usage_with_percent_arbitration(
                json.loads(auth_data.decode("utf-8")), self.opener, temp_path, max(self.args.timeout, 1), {}, getattr(self.args, "retry_limit", DEFAULT_RETRY_LIMIT),
                auth_lock=self.accounts.lock if credential["active"] else None, refreshed_callback=commit_refreshed_credentials,
            )
            output.update({"checkedAt": now_iso(), "accountSlotId": credential["id"], "accountLabel": credential["label"]})
            sample = make_history_sample(output, None)
            with self.lock:
                self._update_account_status_locked(credential["id"], sample)
            return parse_timestamp(((sample.get("windows") or {}).get(label) or {}).get("resetAt"))
        finally:
            if not credential["active"] and temp_path.exists():
                try:
                    self.accounts.commit_polled_credentials(credential["id"], expected_fingerprint, temp_path.read_bytes())
                finally:
                    temp_path.unlink(missing_ok=True)

    def _due_session_refreshes(self) -> list[tuple[dict, str]]:
        now = time.time()
        due = []
        eligible_keys = set()
        for credential in self.accounts.session_refresh_credentials():
            for label, window in (self.account_statuses.get(credential["id"]) or {}).get("windows", {}).items():
                key = credential["id"], label
                if not self._session_refresh_enabled(credential, label):
                    continue
                eligible_keys.add(key)
                used_percent = coerce_float((window or {}).get("usedPercent"))
                if used_percent is None or key in self.session_refresh_procedures and key not in self.session_refresh_attempts and used_percent != 0:
                    self.session_refresh_attempts.pop(key, None)
                    self.session_refresh_costs.pop(key, None)
                    self.session_refresh_reset_at.pop(key, None)
                    self.session_refresh_procedures.discard(key)
                    self.session_refresh_failed.pop(key, None)
                    self.session_refresh_suppressed_reset_at.pop(key, None)
                    continue
                if key in self.session_refresh_failed:
                    if now - self.session_refresh_failed[key] < SESSION_REFRESH_FAILURE_RETRY_SECONDS:
                        continue
                    self.session_refresh_attempts.pop(key, None)
                    self.session_refresh_costs.pop(key, None)
                    self.session_refresh_reset_at.pop(key, None)
                    self.session_refresh_procedures.discard(key)
                    self.session_refresh_failed.pop(key)
                    if coerce_float((window or {}).get("usedPercent")) == 0:
                        due.append((credential, label))
                        continue
                if key not in self.session_refresh_procedures and not self._session_needs_refresh(label, window or {}, now):
                    self.session_refresh_attempts.pop(key, None)
                    self.session_refresh_costs.pop(key, None)
                    self.session_refresh_reset_at.pop(key, None)
                    self.session_refresh_procedures.discard(key)
                    self.session_refresh_failed.pop(key, None)
                    self.session_refresh_suppressed_reset_at.pop(key, None)
                    continue
                if key not in self.session_refresh_procedures:
                    reset_at = parse_timestamp((window or {}).get("resetAt"))
                    if self.session_refresh_suppressed_reset_at.get(key) == reset_at:
                        continue
                    self.session_refresh_suppressed_reset_at.pop(key, None)
                if self.session_refresh_attempts.get(key, 0) >= SESSION_REFRESH_MAX_ATTEMPTS:
                    if key not in self.session_refresh_failed:
                        print(f"Manual refresh failed for {credential['label']!r} ({label}) after {SESSION_REFRESH_MAX_ATTEMPTS} attempts; token API-equivalent cost: {session_refresh_cost_text(self.session_refresh_costs.get(key))}.", flush=True)
                        self.session_refresh_failed[key] = now
                    continue
                if key not in self.session_refresh_procedures or self._session_refresh_window_allows(credential, label, now):
                    due.append((credential, label))
        tracked_keys = self.session_refresh_attempts.keys() | self.session_refresh_costs.keys() | self.session_refresh_reset_at.keys() | self.session_refresh_procedures | self.session_refresh_failed.keys()
        for key in tracked_keys - eligible_keys:
            self.session_refresh_attempts.pop(key, None)
            self.session_refresh_costs.pop(key, None)
            self.session_refresh_reset_at.pop(key, None)
            self.session_refresh_procedures.discard(key)
            self.session_refresh_failed.pop(key, None)
        for key in self.session_refresh_suppressed_reset_at.keys() - eligible_keys:
            self.session_refresh_suppressed_reset_at.pop(key)
        return due

    def _refresh_session_for_account(self, credential: dict) -> tuple[bool, bytes | None, float | None]:
        if credential["active"]:
            return refresh_session(self.args.codex_home)
        for _ in range(5):
            refresh_home = self.accounts.root / f".session-refresh-{secrets.token_hex(16)}"
            try:
                refresh_home.mkdir()
                if os.name != "nt":
                    os.chmod(refresh_home, 0o700)
                break
            except FileExistsError:
                continue
        else:
            raise OSError("Could not create a unique session refresh workspace")
        try:
            return refresh_session(refresh_home, credential["data"])
        finally:
            try:
                remove_directory(refresh_home)
            except OSError as exc:
                print(f"Session refresh workspace cleanup failed: {exc}", file=sys.stderr, flush=True)

    def run_session_refreshing(self) -> None:
        while self.running:
            due = self._due_session_refreshes()
            for credential, label in due:
                key = credential["id"], label
                if key not in self.session_refresh_procedures:
                    window = ((self.account_statuses.get(credential["id"]) or {}).get("windows", {}).get(label) or {})
                    previous_reset_at = parse_timestamp(window.get("resetAt"))
                    try:
                        reset_at = self._retrieve_session_refresh_reset_at(credential, label)
                    except Exception:
                        if previous_reset_at is not None:
                            self.session_refresh_suppressed_reset_at[key] = previous_reset_at
                        continue
                    if previous_reset_at is None or reset_at is None or reset_at == previous_reset_at:
                        if reset_at is not None:
                            self.session_refresh_suppressed_reset_at[key] = reset_at
                        continue
                    window = ((self.account_statuses.get(credential["id"]) or {}).get("windows", {}).get(label) or {})
                    if self._session_refresh_window_allows(credential, label, time.time()):
                        print(f"Manual refresh needed for {credential['label']!r} ({label}; next refresh at {window.get('resetAt') or 'unknown'}); starting manual refresh procedure.", flush=True)
                    else:
                        print(f"Manual refresh needed for {credential['label']!r} ({label}; next refresh at {window.get('resetAt') or 'unknown'}); queued until an allowed UTC window.", flush=True)
                    self.session_refresh_procedures.add(key)
                    self.session_refresh_costs[key] = 0.0
                    self.session_refresh_reset_at[key] = reset_at
                if not self._session_refresh_window_allows(credential, label, time.time()):
                    continue
                try:
                    refreshed, auth_data, refresh_cost = self._refresh_session_for_account(credential)
                    current_cost = self.session_refresh_costs.get(key, 0.0)
                    if refresh_cost is None or current_cost is None:
                        self.session_refresh_costs[key] = None
                    else:
                        self.session_refresh_costs[key] = current_cost + refresh_cost
                    if not refreshed:
                        continue
                    if credential["active"]:
                        self.sync_active_account_from_live()
                    elif auth_data is not None:
                        self.accounts.commit_polled_credentials(credential["id"], credential["fingerprint"], auth_data)
                    reset_at = self._retrieve_session_refresh_reset_at(credential, label)
                    if self._session_refresh_succeeded(self.session_refresh_reset_at.get(key), reset_at):
                        print(f"Manual refresh succeeded for {credential['label']!r} ({label}) after {self.session_refresh_attempts.get(key, 0) + 1} times; token API-equivalent cost: {session_refresh_cost_text(self.session_refresh_costs.get(key))}.", flush=True)
                        self.session_refresh_suppressed_reset_at[key] = reset_at
                        self.session_refresh_attempts.pop(key, None)
                        self.session_refresh_costs.pop(key, None)
                        self.session_refresh_reset_at.pop(key, None)
                        self.session_refresh_procedures.discard(key)
                        self.session_refresh_failed.pop(key, None)
                        continue
                    self.session_refresh_reset_at[key] = reset_at
                except Exception:
                    self.session_refresh_costs[key] = None
                    pass
                finally:
                    if key in self.session_refresh_procedures:
                        self.session_refresh_attempts[key] = self.session_refresh_attempts.get(key, 0) + 1
            now = time.time()
            self.session_refresh_event.wait(0 if due else min(self._session_refresh_schedule_wait_seconds(now), min((max(failed_at + SESSION_REFRESH_FAILURE_RETRY_SECONDS - now, 0) for failed_at in self.session_refresh_failed.values()), default=10 * 60)))
            self.session_refresh_event.clear()

    def run(self) -> None:
        while self.running:
            if self.last_acquire_started_at is not None:
                self.wake_event.wait(poll_sleep_seconds(self.last_acquire_started_at, self.args.interval))
                self.wake_event.clear()
                if not self.running:
                    break
            try:
                retry_operation(self.poll_once, getattr(self.args, "retry_limit", DEFAULT_RETRY_LIMIT))
            except Exception as exc:
                with self.lock:
                    self.last_error = str(exc)
                print(f"Dashboard polling failed: {exc}", file=sys.stderr, flush=True)

    def run_cloud_maintenance(self) -> None:
        while self.running:
            try:
                result = self.cloud.maintenance_tick()
            except Exception as exc:
                if not isinstance(exc, CloudError) or exc.category != "network" or not self.cloud_maintenance_connection_failed:
                    print(f"Cloud maintenance failed: {exc}", file=sys.stderr, flush=True)
                self.cloud_maintenance_connection_failed = isinstance(exc, CloudError) and exc.category == "network"
            else:
                if any(result.values()):
                    self.cloud_maintenance_connection_failed = False
                if result.get("usageSynced"):
                    self._signal_dashboard_cache("cloud")
            self.cloud_maintenance_event.wait(5)
            self.cloud_maintenance_event.clear()

    def run_config_monitor(self) -> None:
        while self.running:
            try:
                self.accounts.sync_config_from_disk()
            except AccountError as exc:
                self.accounts.error = f"config.toml update could not be recorded: {exc}"
            self.config_monitor_event.wait(5)
            self.config_monitor_event.clear()

    def _account_changed(self, refresh_usage_data: bool = False) -> None:
        with self.lock:
            self.last_sample = None
            self.last_error = None
            self.inactive_account_poll_errors.pop(self.accounts.status()["activeAccountId"], None)
            if refresh_usage_data:
                self.usage_data.refresh_accounts()
            self._signal_dashboard_cache()
        self.wake_event.set()
        self.inactive_account_poll_event.set()

    def sync_active_account_from_live(self) -> bool:
        changed, external_update = self.accounts.reconcile_active_from_live()
        if changed:
            self.usage_data.refresh_accounts()
            self._signal_dashboard_cache()
        if external_update is not None:
            self._start_external_auth_validation(external_update)
        return changed

    def _start_external_auth_validation(self, external_update: dict) -> None:
        fingerprint = auth_fingerprint(external_update["data"])
        with self.lock:
            if fingerprint in self.external_auth_validation_fingerprints:
                return
            self.external_auth_validation_fingerprints.add(fingerprint)

        def validate():
            fd, temp_name = tempfile.mkstemp(prefix=".external-auth-validation-", suffix=".json", dir=self.accounts.root)
            temp_path = Path(temp_name)
            expected_fingerprint = external_update["fingerprint"]
            try:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(external_update["data"])
                    stream.flush()
                    os.fsync(stream.fileno())

                def commit_refreshed_credentials():
                    nonlocal expected_fingerprint
                    data = temp_path.read_bytes()
                    if not self.accounts.commit_polled_credentials(external_update["id"], expected_fingerprint, data):
                        raise UsageError("recorded credentials changed during external auth validation")
                    expected_fingerprint = auth_fingerprint(data)

                auth = json.loads(external_update["data"].decode("utf-8"))
                output = fetch_usage_with_percent_arbitration(auth, self.opener, temp_path, max(self.args.timeout, 1), {}, getattr(self.args, "retry_limit", DEFAULT_RETRY_LIMIT), refreshed_callback=commit_refreshed_credentials)
                data = temp_path.read_bytes()
                if not self.accounts.commit_polled_credentials(external_update["id"], expected_fingerprint, data):
                    raise UsageError("recorded credentials changed during external auth validation")
                usage_account_id = self.cloud.usage_account_id(external_update["id"])
                output.update({"checkedAt": now_iso(), "accountSlotId": external_update["id"], "accountLabel": external_update["label"], "usageAccountId": usage_account_id, "sync": {"version": 1, "originMachineId": self.cloud.machine_id, "accountId": usage_account_id}})
                sample = make_history_sample(output, None)
                with self.lock:
                    append_quota_history_sample(self.args.quota_history, sample)
                    self._update_account_status_locked(external_update["id"], sample)
                    self._signal_dashboard_cache()
                print(f"External credentials for {external_update['label']!r} were validated and saved.", flush=True)
            except Exception as exc:
                print(f"External credentials for {external_update['label']!r} were not saved because background validation failed: {exc}", file=sys.stderr, flush=True)
            finally:
                temp_path.unlink(missing_ok=True)
                with self.lock:
                    self.external_auth_validation_fingerprints.discard(fingerprint)
                    self.external_auth_validation_threads.discard(threading.current_thread())

        thread = threading.Thread(target=validate, daemon=True)
        with self.lock:
            self.external_auth_validation_threads.add(thread)
        thread.start()

    def wait_for_external_auth_validations(self, timeout: float) -> None:
        deadline = time.monotonic() + max(timeout, 0)
        while True:
            with self.lock:
                threads = list(self.external_auth_validation_threads)
            if not threads:
                return
            threads[0].join(max(deadline - time.monotonic(), 0))
            if time.monotonic() >= deadline:
                return

    def create_account(self, label: str, account_type: str = "account", api_key: str | None = None) -> dict:
        previous = self.accounts.status()
        result = self.accounts.create_account(label, account_type, api_key, self._start_external_auth_validation)
        self._account_changed(refresh_usage_data=True)
        if previous["activeAccountId"] is None:
            print(f"Account event: prepared {str(label).strip()!r} for sign-in.", flush=True)
        else:
            print(f"Account event: saved {next(account['label'] for account in previous['items'] if account['id'] == previous['activeAccountId'])!r} and prepared {str(label).strip()!r} for sign-in.", flush=True)
        return result

    def switch_account(self, account_id: str) -> dict:
        previous_status = self.accounts.status()
        previous_id = previous_status["activeAccountId"]
        result = self.accounts.switch(account_id, self._start_external_auth_validation)
        if result["activeAccountId"] != previous_id:
            self._account_changed()
            print(
                f"Account event: switched from {next(account['label'] for account in previous_status['items'] if account['id'] == previous_id)!r} "
                f"to {next(account['label'] for account in result['items'] if account['id'] == result['activeAccountId'])!r}.", flush=True,
            )
        return result

    def rename_account(self, account_id: str, label: str) -> dict:
        account = next((account for account in self.accounts.status()["items"] if account["id"] == str(account_id or "")), None)
        with self.lock:
            result = self.accounts.rename(account_id, label, lambda renamed_id, renamed_label: rewrite_account_labels(
                (path for path in (self.args.history, getattr(self.args, "quota_history", None), getattr(self.args, "token_session_history", None), self.args.sample_log, self.args.state) if path is not None), renamed_id, renamed_label,
            ))
            renamed_label = next(item["label"] for item in result["items"] if item["id"] == str(account_id))
            replace_account_label(self.runtime_state, str(account_id), renamed_label)
            replace_account_label(self.last_sample, str(account_id), renamed_label)
            replace_account_label(self.account_statuses.get(str(account_id)), str(account_id), renamed_label)
            self.usage_data.refresh_accounts()
            self._signal_dashboard_cache()
        print(f"Account event: renamed {account['label'] if account else None!r} to {str(label).strip()!r}.", flush=True)
        return result

    def delete_account(self, account_id: str) -> dict:
        previous_status = self.accounts.status()
        deleted = next((account for account in previous_status["items"] if account["id"] == str(account_id or "")), None)
        result = self.accounts.delete(account_id)
        with self.lock:
            self.account_statuses.pop(str(account_id or ""), None)
            self.usage_data.refresh_accounts()
            self._signal_dashboard_cache()
        if result["activeAccountId"] != previous_status["activeAccountId"]:
            self._account_changed()
        print(f"Account event: deleted {deleted['label']!r}.", flush=True)
        return result

def dashboard_html() -> str:
    return DASHBOARD_HTML_PATH.read_text(encoding="utf-8")

def management_html() -> str:
    return MANAGEMENT_HTML_PATH.read_text(encoding="utf-8")

def dashboard_remote_accounts(accounts: list[dict], local_api_identity_ids: set[str] | None = None, local_cloud_keys: set[str] | None = None) -> list[dict]:
    local_api_identity_ids, local_cloud_keys = local_api_identity_ids or set(), local_cloud_keys or set()
    rows = [{
        "accountKey": item.get("accountKey"), "label": item.get("label"), "accountType": item.get("accountType", "account"),
        **({"canLink": (item.get("_apiIdentityId") or item.get("apiIdentityId")) not in local_api_identity_ids} if item.get("accountType") == "api" else {"canBind": item.get("accountKey") not in local_cloud_keys}),
    } for item in accounts]
    label_counts = {}
    for account in rows:
        label_counts[str(account.get("label") or "Cloud account").casefold()] = label_counts.get(str(account.get("label") or "Cloud account").casefold(), 0) + 1
    for account in rows:
        label = account.get("label") or "Cloud account"
        account["displayLabel"] = f"{label} · {str(account.get('accountKey') or 'profile')[:6]}" if label_counts[label.casefold()] > 1 else label
    return rows

def management_payload(state: UsageDashboardState, include_remote: bool = False, refresh_scan: bool = False) -> dict:
    remote_accounts = state.cloud.cached_remote_accounts()
    api_identity_ids = state.accounts.api_identity_ids() if hasattr(state.accounts, "api_identity_ids") else {}
    accounts = dashboard_accounts(state)
    remote_api_identity_ids = {item.get("_apiIdentityId") or item.get("apiIdentityId") for item in remote_accounts if item.get("accountType") == "api"}
    for account in accounts["items"]:
        if account.get("isApiAccount"):
            account["canShare"] = bool(account.get("ready")) and api_identity_ids.get(account["id"]) not in remote_api_identity_ids
    payload = {
        "server": state.cloud.config()["server"],
        "editableConfig": state.cloud.editable_config(),
        "skills": dashboard_skill_status(state.skills.status()),
        "scan": [{**{key: item.get(key) for key in ("name", "sources", "authoritativeSource", "defaultAssignments")}, **({"error": "Skill scan error"} if item.get("error") else {})} for item in state.skills.scan(refresh_scan)],
        "cloud": dashboard_cloud_status(state.cloud.redacted_status()),
        "accounts": accounts,
        "apiConfig": state.accounts.config_editor_payload() if hasattr(state.accounts, "config_editor_payload") else {},
    }
    if include_remote and payload["cloud"]["webdav"].get("enabled"):
        state.cloud.fetch(include_usage=False)
        remote_accounts = state.cloud.cached_remote_accounts()
        remote_api_identity_ids = {item.get("_apiIdentityId") or item.get("apiIdentityId") for item in remote_accounts if item.get("accountType") == "api"}
        for account in accounts["items"]:
            if account.get("isApiAccount"):
                account["canShare"] = bool(account.get("ready")) and api_identity_ids.get(account["id"]) not in remote_api_identity_ids
    payload["remoteAccounts"] = dashboard_remote_accounts(remote_accounts, set(api_identity_ids.values()), state.accounts.cloud_account_keys() if hasattr(state.accounts, "cloud_account_keys") else set())
    return payload

def dashboard_status_payload(state: UsageDashboardState) -> dict:
    if hasattr(state, "status_payload"):
        return state.status_payload()
    accounts = dashboard_accounts(state)
    last_sample = state.last_sample
    if accounts["awaitingLogin"] or not isinstance(last_sample, dict) or last_sample.get("activeAccountSlotId") != accounts["activeAccountId"]:
        last_sample = None
    sample = dashboard_sample(last_sample)
    return {
        "revision": _dashboard_revision({"sample": sample, "accounts": accounts}),
        "streamId": None,
        "newestIndex": 0,
        "mergedRevision": None,
        "controlPasswordConfigured": control_password_is_configured(state.cloud.config()["control"]) if hasattr(state, "cloud") else True,
        "lastSample": sample,
        "display": dashboard_display(last_sample),
        "accounts": accounts,
    }

def serve_dashboard(args, opener: urllib.request.OpenerDirector | None) -> int:
    try:
        server_config = load_server_config(Path(args.data_home) / "config.json")
    except CloudError as exc:
        print(f"Cannot start dashboard: {exc}.", file=sys.stderr, flush=True)
        return 1
    server_host = server_config["host"]
    instance_lock = DashboardInstanceLock()
    try:
        instance_acquired = instance_lock.acquire()
    except OSError as exc:
        print(f"Cannot start dashboard instance lock: {exc}.", file=sys.stderr, flush=True)
        return 1
    if not instance_acquired:
        print("Cannot start dashboard: another monitor instance is already running.", file=sys.stderr, flush=True)
        return 1

    serialized_body_cache = {}
    compressed_body_cache = {}
    response_body_cache_lock = threading.Lock()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.handle_get()

        def do_POST(self):
            self.handle_post()

        def send_json_body(self, status: int, body: bytes, headers: dict | None = None):
            response_headers = dict(headers or {})
            if len(body) >= DASHBOARD_GZIP_MIN_BYTES and "gzip" in (self.headers.get("Accept-Encoding") or "").lower():
                cache_key = hashlib.sha256(body).digest()
                with response_body_cache_lock:
                    compressed = compressed_body_cache.get(cache_key)
                if compressed is None:
                    compressed = gzip.compress(body)
                    with response_body_cache_lock:
                        if len(compressed_body_cache) >= 32:
                            compressed_body_cache.pop(next(iter(compressed_body_cache)))
                        compressed_body_cache[cache_key] = compressed
                body = compressed
                response_headers["Content-Encoding"] = "gzip"
            response_headers["Vary"] = "Accept-Encoding"
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            if "Cache-Control" not in response_headers:
                self.send_header("Cache-Control", "no-store")
            for key, value in response_headers.items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, status: int, payload: dict, headers: dict | None = None, *, sanitize: bool = True, cache_key: str | None = None):
            body = None
            if cache_key:
                with response_body_cache_lock:
                    body = serialized_body_cache.get(cache_key)
            if body is None:
                body = json.dumps(dashboard_safe_json(payload) if sanitize else payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                if cache_key:
                    with response_body_cache_lock:
                        if len(serialized_body_cache) >= 32:
                            serialized_body_cache.pop(next(iter(serialized_body_cache)))
                        serialized_body_cache[cache_key] = body
            self.send_json_body(status, body, headers)

        def send_not_modified(self, etag: str):
            self.send_response(304)
            self.send_header("Cache-Control", "no-cache")
            self.send_header("ETag", etag)
            self.send_header("Vary", "Accept-Encoding")
            self.end_headers()

        def control_token(self) -> str | None:
            try:
                cookie = SimpleCookie(self.headers.get("Cookie") or "")
            except Exception:
                return None
            return cookie[CONTROL_COOKIE_NAME].value if CONTROL_COOKIE_NAME in cookie else None

        def control_is_authenticated(self) -> bool:
            return control_auth.token_is_valid(self.control_token())

        def require_control_auth(self) -> bool:
            if control_auth.is_compromised():
                self.send_json(409, {"error": "Control password compromised. Remove passwordHash from config.json, restart the monitor, and then create a new control password.", "controlPasswordCompromised": True})
                return False
            if not control_auth.is_configured():
                self.send_json(428, {"error": "Create a control password to continue", "setupRequired": True})
                return False
            if self.control_is_authenticated():
                return True
            self.send_json(401, {"error": "Control password required"})
            return False

        def read_json_body(self) -> dict:
            if not (self.headers.get("Content-Type") or "").lower().startswith("application/json"):
                raise AccountError("Content-Type must be application/json")
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0 or length > 16 * 1024:
                raise AccountError("Invalid request body")
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(body, dict):
                raise AccountError("Request body must be a JSON object")
            return body

        def handle_get(self):
            path = urllib.parse.urlparse(self.path).path
            if path == "/":
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(dashboard_html().encode("utf-8"))
                return
            if path == "/manage":
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(management_html().encode("utf-8"))
                return
            if path == "/api/series":
                query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                try:
                    if not query:
                        payload = state.series_response()
                    elif set(query) == {"streamId", "includedIndex", "mergedRevision"} and all(len(query[key]) == 1 for key in query):
                        payload = state.series_response(query["streamId"][0], int(query["includedIndex"][0]), query["mergedRevision"][0])
                    else:
                        raise ValueError
                except ValueError:
                    self.send_json(400, {"error": "Expected streamId, includedIndex, and mergedRevision together"})
                    return
                etag = f'W/"series-{payload["streamId"]}-{payload.get("fromIndex", "snapshot")}-{payload["includedIndex"]}-{payload["mergedRevision"]}-{payload["mode"]}"'
                self.send_json(200, payload, {"Cache-Control": "no-cache", "ETag": etag}, cache_key=etag)
                return
            if path == "/api/status":
                payload = dashboard_status_payload(state)
                etag = f'W/"status-{payload["revision"]}"'
                if self.headers.get("If-None-Match") == etag:
                    self.send_not_modified(etag)
                else:
                    self.send_json(200, payload, {"Cache-Control": "no-cache", "ETag": etag}, cache_key=etag)
                return
            if path == "/api/manage/status":
                if not self.require_control_auth():
                    return
                try:
                    query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                    self.send_json(200, management_payload(state, query.get("remote") == ["1"], query.get("scan") == ["1"]), sanitize=False)
                except (AccountError, SkillError, CloudError) as exc:
                    self.send_json(exc.status, {"error": str(exc)})
                return
            self.send_response(404)
            self.end_headers()

        def handle_post(self):
            path = urllib.parse.urlparse(self.path).path
            allowed = {
                "/api/control/login", "/api/control/setup",
                "/api/accounts", "/api/accounts/switch", "/api/accounts/rename", "/api/accounts/delete", "/api/accounts/session-refresh", "/api/manage/skills/manage", "/api/manage/skills/unmanage", "/api/manage/skills/assign",
                "/api/manage/cloud/test", "/api/manage/cloud/fetch", "/api/manage/cloud/fetch-all", "/api/manage/cloud/push", "/api/manage/cloud/push-all", "/api/manage/cloud/restore", "/api/manage/cloud/overwrite", "/api/manage/accounts/bind", "/api/manage/accounts/link", "/api/manage/accounts/release", "/api/manage/accounts/share", "/api/manage/accounts/delete", "/api/manage/accounts/delete-remote", "/api/manage/accounts/header", "/api/manage/accounts/common-header", "/api/manage/server", "/api/manage/config", "/api/manage/config/reload"
            }
            allowed.add("/api/manage/accounts/common")
            if path not in allowed:
                self.send_json(404, {"error": "Not found"})
                return
            try:
                body = self.read_json_body()
                if path == "/api/control/setup":
                    if not client_host_is_loopback(self.client_address[0]):
                        self.send_json(403, {"error": "Control password setup is allowed only from this computer"})
                        return
                    result = state.cloud.initialize_control_password(body.get("password"))
                    control_auth.update(state.cloud.config()["control"])
                    self.send_json(200, result | {"authenticated": True}, {"Set-Cookie": f"{CONTROL_COOKIE_NAME}={control_auth.create_token()}; HttpOnly; SameSite=Strict; Path=/; Max-Age={CONTROL_COOKIE_MAX_AGE_SECONDS}"})
                    return
                if path == "/api/control/login":
                    if control_auth.is_compromised():
                        self.send_json(409, {"error": "Control password compromised. Remove passwordHash from config.json, restart the monitor, and then create a new control password.", "controlPasswordCompromised": True})
                        return
                    if not control_auth.is_configured():
                        self.send_json(428, {"error": "Create a control password to continue", "setupRequired": True})
                        return
                    if not control_auth.password_matches(body.get("password")):
                        self.send_json(401, {"error": "Incorrect control password"})
                        return
                    self.send_json(200, {"authenticated": True}, {"Set-Cookie": f"{CONTROL_COOKIE_NAME}={control_auth.create_token()}; HttpOnly; SameSite=Strict; Path=/; Max-Age={CONTROL_COOKIE_MAX_AGE_SECONDS}"})
                    return
                if not self.require_control_auth():
                    return
                if path == "/api/accounts":
                    result = state.create_account(body.get("label"), body.get("accountType", "account"), body.get("apiKey"))
                elif path == "/api/accounts/switch":
                    result = state.switch_account(body.get("accountId"))
                elif path == "/api/accounts/rename":
                    result = state.rename_account(body.get("accountId"), body.get("label"))
                elif path == "/api/accounts/delete":
                    result = state.delete_account(body.get("accountId"))
                elif path == "/api/accounts/session-refresh":
                    result = state.accounts.set_session_refresh(body.get("accountId"), body.get("sessionRefresh"))
                    state.session_refresh_event.set()
                elif path == "/api/manage/skills/manage":
                    self.send_json(200, state.skills.manage(body.get("names") if isinstance(body.get("names"), list) else []))
                    return
                elif path == "/api/manage/skills/unmanage":
                    self.send_json(200, state.cloud.unmanage_skill(body.get("name")))
                    return
                elif path == "/api/manage/skills/assign":
                    self.send_json(200, state.skills.assign(body.get("name"), body.get("app"), body.get("enabled") is True))
                    return
                elif path == "/api/manage/server":
                    self.send_json(200, state.cloud.update_server_config(body.get("host")))
                    return
                elif path == "/api/manage/config":
                    result = state.cloud.update_config(body)
                    if result["controlPasswordChanged"]:
                        control_auth.update(state.cloud.config()["control"])
                    self.send_json(200, result)
                    return
                elif path == "/api/manage/config/reload":
                    self.send_json(200, state.cloud.reload_config())
                    return
                elif path == "/api/manage/cloud/test":
                    self.send_json(200, state.cloud.test())
                    return
                elif path == "/api/manage/cloud/fetch":
                    result = state.cloud.fetch(include_usage=True)
                    state._signal_dashboard_cache("cloud")
                    self.send_json(200, result)
                    return
                elif path == "/api/manage/cloud/fetch-all":
                    result = state.cloud.fetch(include_usage=True, force_full=True)
                    state._signal_dashboard_cache("cloud")
                    self.send_json(200, result)
                    return
                elif path == "/api/manage/cloud/push":
                    self.send_json(200, state.cloud.push())
                    return
                elif path == "/api/manage/cloud/push-all":
                    self.send_json(200, state.cloud.push(force_full=True))
                    return
                elif path == "/api/manage/cloud/overwrite":
                    self.send_json(200, state.cloud.overwrite_cloud_from_local())
                    return
                elif path == "/api/manage/cloud/restore":
                    self.send_json(200, state.cloud.restore_skills(body.get("snapshotId")))
                    return
                elif path == "/api/manage/accounts/bind":
                    result = state.cloud.bind_local_account(body.get("accountKey"))
                    state._signal_dashboard_cache("cloud")
                    self.send_json(200, {"accounts": result})
                    return
                elif path == "/api/manage/accounts/link":
                    result = state.cloud.link_local_account(body.get("accountKey"))
                    state._signal_dashboard_cache("cloud")
                    self.send_json(200, {"accounts": result})
                    return
                elif path == "/api/manage/accounts/release":
                    result = state.cloud.release_local_account(body.get("accountId"))
                    state._signal_dashboard_cache("cloud")
                    self.send_json(200, {"accounts": result})
                    return
                elif path == "/api/manage/accounts/share":
                    self.send_json(200, {"accounts": state.cloud.share_local_account(body.get("accountId"))})
                    return
                elif path == "/api/manage/accounts/delete-remote":
                    result = state.cloud.delete_remote_account(body.get("accountKey"))
                    state._signal_dashboard_cache("cloud")
                    self.send_json(200, {"remoteAccounts": result})
                    return
                elif path == "/api/manage/accounts/header":
                    self.send_json(200, {"accounts": state.accounts.update_api_config(body.get("accountId"), body.get("headerToml"))})
                    return
                elif path == "/api/manage/accounts/common-header":
                    self.send_json(200, {"accounts": state.accounts.update_common_account_header(body.get("headerToml"))})
                    return
                elif path == "/api/manage/accounts/common":
                    self.send_json(200, {"accounts": state.accounts.update_common_config(body.get("commonToml"))})
                    return
                elif path == "/api/manage/accounts/delete":
                    self.send_json(200, {"accounts": state.delete_account(body.get("accountId"))})
                    return
                self.send_json(200, {"accounts": result})
            except (AccountError, SkillError, CloudError) as exc:
                self.send_json(exc.status, {"error": str(exc), **({"details": exc.details} if getattr(exc, "details", None) else {}), **({"decryptFailed": True} if getattr(exc, "decrypt_failed", False) else {})})
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                self.send_json(400, {"error": "Request body is not valid JSON"})
            except Exception as exc:
                self.send_json(500, {"error": f"Account operation failed: {exc}"})

        def log_message(self, fmt, *args):
            return

    try:
        server = DashboardHTTPServer((server_host, DASHBOARD_PORT), Handler)
    except OSError as exc:
        instance_lock.release()
        print(f"Cannot start dashboard: {server_host}:{DASHBOARD_PORT} is unavailable ({exc}).", file=sys.stderr, flush=True)
        return 1
    try:
        state = UsageDashboardState(args, opener)
    except Exception:
        server.server_close()
        instance_lock.release()
        raise
    control_auth = ControlAuth(state.cloud.config()["control"])
    thread = threading.Thread(target=state.run, daemon=True)
    thread.start()
    inactive_account_thread = threading.Thread(target=state.run_inactive_account_polling, daemon=True)
    inactive_account_thread.start()
    session_refresh_thread = threading.Thread(target=state.run_session_refreshing, daemon=True)
    session_refresh_thread.start()
    dashboard_cache_thread = threading.Thread(target=state.run_dashboard_cache_maintenance, daemon=True)
    dashboard_cache_thread.start()
    cloud_thread = threading.Thread(target=state.run_cloud_maintenance, daemon=True)
    cloud_thread.start()
    config_thread = threading.Thread(target=state.run_config_monitor, daemon=True)
    config_thread.start()
    url = f"http://127.0.0.1:{DASHBOARD_PORT}/"
    print(f"Dashboard: {url}", flush=True)
    if args.dashboard:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            state.sync_active_account_from_live()
            state.wait_for_external_auth_validations(max(getattr(args, "timeout", 10), 1) + 1)
        except Exception as exc:
            print(f"Final auth.json reconciliation failed: {exc}", file=sys.stderr, flush=True)
        state.running = False
        state.wake_event.set()
        state.inactive_account_poll_event.set()
        state.session_refresh_event.set()
        state.dashboard_cache_event.set()
        state.cloud_maintenance_event.set()
        state.config_monitor_event.set()
        server.server_close()
        instance_lock.release()
    return 0
