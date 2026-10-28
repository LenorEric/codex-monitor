#!/usr/bin/env python3

import base64
import bisect
import copy
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
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from pathlib import Path

from monitor_accounts import AccountError, AccountManager, auth_fingerprint, is_api_auth, remove_directory
from monitor_auto_update import AUTO_UPDATE_RESTART, AutoUpdater, installed_version
from monitor_cloud import CloudError, CloudManager, control_password_is_compromised, control_password_is_configured, control_password_matches, load_server_config
from monitor_cloud_queue import OperationSkipped
from monitor_common import DEFAULT_RETRY_LIMIT, PLAN_MULTIPLIERS, RESET_TIME_JITTER_SECONDS, UsageError, coerce_float, empty_cost_totals, is_client_disconnect, now_iso, parse_timestamp, poll_sleep_seconds, retry_operation
from monitor_device_auth import DeviceAuthManager
from monitor_events import (
    allocate_model_delta_events, compact_delta_event, hydrate_delta_state_from_events,
    print_special_events, process_sample_delta_events, valid_delta_cost_rate, delta_state_needs_hydration,
)
from monitor_quota import fetch_usage
from monitor_history import (
    append_capped_jsonl, append_quota_history_sample, collect_usage_sample, default_dashboard_cache_path,
    load_quota_history, load_state, make_history_sample, normalize_quota_history_row, quota_history_row_from_sample, replace_account_label, reset_runtime_baselines, rewrite_account_labels, write_state,
)
from monitor_skills import SkillError, SkillManager
from monitor_session_refresh import refresh_session
from monitor_token_ledger import default_token_ledger_path, load_token_ledger, sync_token_ledger, token_cost_snapshot, token_sessions_from_ledger
from monitor_usage_sync import UsageDataStore, add_record_provenance

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
DASHBOARD_DISPLAY_CACHE_GAP_SECONDS = 4 * 60 * 60
DASHBOARD_DISPLAY_CACHE_MAINTENANCE_SECONDS = 60
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
    from monitor_streaming import Records
    if isinstance(value, Records):
        return Records(lambda: (dashboard_safe_json(item) for item in value))
    if isinstance(value, dict):
        return {key: dashboard_safe_json(item) for key, item in value.items() if key not in SENSITIVE_DASHBOARD_FIELDS or key in {"password", "encryptionPassphrase"} and isinstance(item, bool)}
    if isinstance(value, list):
        return [dashboard_safe_json(item) for item in value]
    return value

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

def dashboard_account_status(status: dict, usage_account_resolver=None, activation_history: list[dict] | None = None, slot_order: list | None = None) -> dict:
    items = [{key: account.get(key) for key in ("id", "label", "ready", "active", "pollError", "pollErrorAt", "stale", "accountType", "isApiAccount", "sessionRefresh", "configHeaderToml")} for account in status.get("items", [])]
    label_counts = {}
    for account in items:
        label_counts[str(account.get("label") or "Unknown").casefold()] = label_counts.get(str(account.get("label") or "Unknown").casefold(), 0) + 1
    for account in items:
        account["usageAccountId"] = usage_account_resolver(account.get("id")) if usage_account_resolver is not None else account.get("id") or "unknown"
        account["displayLabel"] = f"{account.get('label') or 'Unknown'} · {str(account.get('id') or 'profile')[-6:]}" if label_counts[str(account.get("label") or "Unknown").casefold()] > 1 else account.get("label") or "Unknown"
    groups = {}
    for account in items:
        groups.setdefault(account["usageAccountId"], []).append(account)
    priority = {slot: index for index, slot in enumerate(slot_order if slot_order is not None else status.get("slotOrder", []))}
    usage_groups = []
    for usage_account_id, profiles in groups.items():
        primary = min(profiles, key=lambda profile: priority.get(profile["id"], len(priority)))
        usage_groups.append({
            "id": usage_account_id,
            "label": primary.get("label") or "Unknown",
            "profileCount": len(profiles),
            "isApiAccount": bool(primary.get("isApiAccount")),
        })
    known_groups = {group["id"] for group in usage_groups}
    for row in activation_history or []:
        if row.get("usageAccountId") and row["usageAccountId"] not in known_groups:
            usage_groups.append({"id": row["usageAccountId"], "label": row.get("accountLabel") or "Unknown", "profileCount": 0, "isApiAccount": False})
            known_groups.add(row["usageAccountId"])
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
            "shared": bool(item.get("shared")),
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

def dashboard_usage_account_id(row: dict, slot_usage_accounts: dict[str, str] | None = None) -> str:
    return str(row.get("usageAccountId") or (row.get("sync") or {}).get("accountId") or (slot_usage_accounts or {}).get(str(row.get("accountSlotId") or "")) or row.get("accountSlotId") or row.get("accountLabel") or "unknown")

def dashboard_quota_point(row: dict, slot_usage_accounts: dict[str, str] | None = None) -> dict:
    from monitor_sampling import quota_point
    return quota_point(row, slot_usage_accounts)


def dashboard_quota_points(rows: list[dict], slot_usage_accounts: dict[str, str] | None = None) -> list[dict]:
    from monitor_sampling import sample_rows
    return sample_rows(rows, slot_usage_accounts)


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


def _dashboard_revision(value) -> str:
    return hashlib.sha256(json.dumps(dashboard_safe_json(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()[:20]

def _dashboard_display_timestamp(value: dict) -> float | None:
    if not isinstance(value, dict):
        return None
    timestamp = coerce_float(value.get("timestamp"))
    return timestamp if timestamp is not None else parse_timestamp(value.get("checkedAt"))

def _dashboard_display_sort_key(value: dict) -> tuple:
    timestamp = _dashboard_display_timestamp(value)
    return timestamp is None, timestamp or 0, str(value.get("usageAccountId") or value.get("accountSlotId") or value.get("accountLabel") or "")

def dashboard_compact_quota_points(points: list[dict], now: float | None = None) -> list[dict]:
    # Quota values and rejection boundaries are exact at every display resolution.
    return sorted(points, key=_dashboard_display_sort_key)


def _dashboard_display_next_maintenance_at(quota_points: list[dict], cost_usage: dict[str, list[dict]], now: float) -> float | None:
    return None


def _same_cost_usage_cycle(previous: dict, current: dict, label: str) -> bool:
    left, right = previous[label], current[label]
    if previous["timestamp"] is None or current["timestamp"] is None or not 0 < current["timestamp"] - previous["timestamp"] <= DASHBOARD_DISPLAY_CACHE_GAP_SECONDS:
        return False
    if left.get("continuous") is None or right.get("continuous") is None or left.get("plan") != right.get("plan"):
        return False
    left_reset, right_reset = parse_timestamp(left.get("resetAt")), parse_timestamp(right.get("resetAt"))
    return left_reset is None and right_reset is None or left_reset is not None and right_reset is not None and abs(left_reset - right_reset) <= RESET_TIME_JITTER_SECONDS

def dashboard_cost_usage(quota_points: list[dict], ledger: list[dict], slot_usage_accounts: dict[str, str] | None = None) -> dict[str, list[dict]]:
    usage_events = {}
    for row in ledger:
        if row.get("recordType") != "usage" or (timestamp := parse_timestamp(row.get("occurredAt"))) is None:
            continue
        account_id = dashboard_usage_account_id(row, slot_usage_accounts)
        cost = coerce_float((row.get("cost") or {}).get("totalCostUsd"))
        if cost is None or cost < 0:
            continue
        usage_events.setdefault(account_id, []).append((timestamp, str(row.get("billingModel") or row.get("rawModel") or "unknown"), cost))
    for rows in usage_events.values():
        rows.sort(key=lambda value: (value[0], value[1]))
    result = {"fiveHour": [], "sevenDay": []}
    for output_name, label in (("fiveHour", "fiveHour"), ("sevenDay", "sevenDay")):
        accounts = {}
        for point in quota_points:
            if point.get("resetWindow") and point["resetWindow"] != label:
                continue
            accounts.setdefault(point["usageAccountId"], []).append(point)
        for account_id, points in accounts.items():
            points.sort(key=lambda point: point["timestamp"] if point["timestamp"] is not None else float("inf"))
            account_events = usage_events.get(account_id, [])
            event_times = [event[0] for event in account_events]
            for previous, current in zip(points, points[1:]):
                if not _same_cost_usage_cycle(previous, current, label):
                    continue
                multiplier = PLAN_MULTIPLIERS.get(current[label].get("plan") or "plus", 1.0)
                start = {"timestamp": previous["timestamp"], "usage": previous[label]["continuous"] * multiplier}
                end = {"timestamp": current["timestamp"], "usage": current[label]["continuous"] * multiplier,
                    "accountSlotId": current.get("accountSlotId"), "accountLabel": current.get("accountLabel"), "plan": current[label].get("plan") or "plus", "planMultiplier": multiplier}
                delta = end["usage"] - start["usage"]
                if delta <= 0:
                    continue
                by_model = {}
                for _, model, cost in account_events[bisect.bisect_left(event_times, start["timestamp"]):bisect.bisect_left(event_times, end["timestamp"])]:
                    by_model[model] = round(by_model.get(model, 0.0) + cost, 8)
                total_cost = round(sum(by_model.values()), 8)
                result[output_name].append({
                    "window": "5h" if label == "fiveHour" else "7d", "checkedAt": datetime.fromtimestamp(end["timestamp"], timezone.utc).isoformat().replace("+00:00", "Z"), "timestamp": end["timestamp"],
                    "startedAt": datetime.fromtimestamp(start["timestamp"], timezone.utc).isoformat().replace("+00:00", "Z"), "endedAt": datetime.fromtimestamp(end["timestamp"], timezone.utc).isoformat().replace("+00:00", "Z"),
                    "startUsagePercent": round(start["usage"], 8), "endUsagePercent": round(end["usage"], 8), "deltaPercent": round(delta, 8), "totalCostUsd": total_cost,
                    "costByModelUsd": by_model, "costPer100PercentUsd": round(total_cost / delta * 100, 8), "accountSlotId": end.get("accountSlotId"), "accountLabel": end.get("accountLabel"),
                    "usageAccountId": account_id, "plan": end["plan"], "planMultiplier": end["planMultiplier"],
                })
    for values in result.values():
        values.sort(key=lambda row: (row["timestamp"], row["usageAccountId"]))
    return result

def dashboard_cost_usage_delta_events(cost_usage: dict[str, list[dict]]) -> list[dict]:
    events = []
    for collection, label in (("fiveHour", "5h"), ("sevenDay", "7d")):
        for point in cost_usage.get(collection) or []:
            delta_percent = coerce_float(point.get("deltaPercent")) or 0.0
            delta_cost = coerce_float(point.get("totalCostUsd")) or 0.0
            if not valid_delta_cost_rate(delta_percent, delta_cost):
                continue
            common = {
                "checkedAt": point.get("checkedAt"), "window": label,
                "accountSlotId": point.get("accountSlotId"), "accountLabel": point.get("accountLabel"),
                "usageAccountId": point.get("usageAccountId"), "plan": point.get("plan"),
                "planMultiplier": point.get("planMultiplier"),
            }
            model_events = allocate_model_delta_events(common, delta_percent, delta_cost, point.get("costByModelUsd") or {}, {})
            events.extend(compact_delta_event(event) for event in model_events)
    return events

def dashboard_cost_time(ledger: list[dict], slot_usage_accounts: dict[str, str] | None = None) -> list[dict]:
    rows = []
    for event in ledger:
        timestamp = parse_timestamp(event.get("occurredAt"))
        cost = coerce_float((event.get("cost") or {}).get("totalCostUsd"))
        if event.get("recordType") != "usage" or timestamp is None or cost is None or not math.isfinite(cost) or cost < 0:
            continue
        account = dashboard_usage_account_id(event, slot_usage_accounts)
        model = str(event.get("rawModel") or event.get("billingModel") or "unknown")
        rows.append({"_key": _dashboard_row_key("costTime", (account, event.get("eventId") or _dashboard_revision(event))),
            "timestamp": timestamp, "usageAccountId": account, "accountSlotId": event.get("accountSlotId"), "accountLabel": event.get("accountLabel"),
            "costByModelUsd": {model: cost}, "totalCostUsd": cost})
    return sorted(rows, key=lambda row: (row["timestamp"], row["_key"]))


def _dashboard_display_data(quota_history: list[dict], token_ledger: list[dict], accounts: dict, now: float | None = None, *, processed_quota: list[dict] | None = None) -> dict:
    now = time.time() if now is None else now
    slot_usage_accounts = {str(account.get("id")): str(account.get("usageAccountId")) for account in accounts.get("items", []) if account.get("id") and account.get("usageAccountId")}
    active = next((item for item in accounts.get("items", []) if item.get("id") == accounts.get("activeAccountId")), None)
    quota_points = [] if active and active.get("isApiAccount") else (dashboard_quota_points(quota_history, slot_usage_accounts) if processed_quota is None else processed_quota)
    cost_usage = dashboard_cost_usage(quota_points, token_ledger, slot_usage_accounts)
    token_sessions = token_sessions_from_ledger(token_ledger)
    account_labels = {str(account["id"]): account["label"] for account in accounts.get("items", []) if account.get("id") and account.get("label")}
    for session in token_sessions:
        if str(session.get("accountSlotId")) in account_labels:
            session["accountLabel"] = account_labels[str(session["accountSlotId"])]
    return {
        "quotaPoints": _dashboard_key_rows("quota", dashboard_compact_quota_points(quota_points, now)),
        "tokenSessions": _dashboard_key_rows("token", [dashboard_token_session(row, slot_usage_accounts) for row in token_sessions]),
        "costTime": dashboard_cost_time(token_ledger, slot_usage_accounts),
        "costUsage": {name: _dashboard_key_rows("costUsage", values) for name, values in cost_usage.items()},
        "historyStats": {
            "quotaRows": len(quota_history), "tokenLedgerRows": len(token_ledger),
            "fiveHourCostUsagePoints": len(cost_usage["fiveHour"]), "sevenDayCostUsagePoints": len(cost_usage["sevenDay"]),
        },
        "nextMaintenanceAt": _dashboard_display_next_maintenance_at(quota_points, cost_usage, now),
    }

def _dashboard_row_key(kind: str, identity) -> str:
    return f"{kind}:{_dashboard_revision(identity)}"

def _dashboard_key_rows(kind: str, rows: list[dict]) -> list[dict]:
    identities = []
    for row in rows:
        if kind == "quota":
            identity = (row.get("usageAccountId"), row.get("checkedAt"))
            if row.get("resetWindow"):
                identity += (row["resetWindow"],)
        elif kind == "token":
            identity = (row.get("usageAccountId"), row.get("sessionId"))
        elif kind == "costUsage":
            identity = (row.get("window"), row.get("usageAccountId"), row.get("startedAt"), row.get("endedAt"))
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


def dashboard_transfer_view(display: dict | None) -> dict:
    display = display or {}
    return {
        "quotaPoints": display.get("quotaPoints") or [],
        "tokenSessions": display.get("tokenSessions") or [],
        "costTime": display.get("costTime") or [],
        "costUsage": {
            "fiveHour": (display.get("costUsage") or {}).get("fiveHour") or [],
            "sevenDay": (display.get("costUsage") or {}).get("sevenDay") or [],
        },
        "historyStats": display.get("historyStats") or {},
    }


class UsageDashboardState:
    def __init__(self, args, opener: urllib.request.OpenerDirector | None):
        self.args = args
        data_home = Path(getattr(self.args, "data_home", getattr(self.args, "history", Path.cwd()).parent))
        if not getattr(self.args, "quota_history", None):
            self.args.quota_history = data_home / "usage_monitor_quota_readings.jsonl"
        if not getattr(self.args, "token_ledger", None):
            self.args.token_ledger = default_token_ledger_path(self.args.quota_history)
        if not getattr(self.args, "dashboard_cache", None):
            self.args.dashboard_cache = default_dashboard_cache_path(self.args.quota_history)
        self.opener = opener
        self.lock = threading.RLock()
        print("Startup: loading accounts, skills, and cloud configuration...", flush=True)
        self.accounts = AccountManager(args.auth, getattr(args, "account_root", None) or data_home / "accounts")
        self.device_auth = DeviceAuthManager(args.codex_home, args.auth)
        self.skills = SkillManager(args.codex_home, getattr(args, "data_home", None) or args.auth.parent / ".codex-switch", getattr(args, "gemini_skills", None))
        self.cloud = CloudManager(self.skills.private_root, self.skills, self.accounts)
        self.accounts.cloud = self.cloud
        self.usage_data = UsageDataStore(
            self.args.quota_history, self.args.token_ledger, self.cloud.machine_id, self.cloud.usage_account_id, self.lock, self.cloud.local_usage_account,
            getattr(self.args, "usage_sync_cache", None), self.cloud.usage_account_revision,
        )
        self.args.usage_sync_cache = self.usage_data.cache_path
        self.args.processing_index = self.usage_data.index
        print("Startup: initializing usage processing and dashboard stores...", flush=True)
        self.usage_data.initialize_processing()
        self.usage_data.initialize_v4_index()
        self.usage_data.index.on_new_cost = self.accounts.promote_active_slot_on_cost
        from monitor_projection import DashboardProjector
        self.projector = DashboardProjector(self.usage_data)
        from monitor_delivery import DashboardDelivery
        self.delivery = DashboardDelivery(self.usage_data.index)
        self.cloud.configure_usage_sync(self.usage_data)
        print("Startup: recovering account transitions and reconciling managed skills...", flush=True)
        try:
            self.cloud.recover_account_transition()
        except CloudError as exc:
            self.accounts.error = f"Cloud account recovery requires attention: {exc}"
        self.skills.reconcile()
        self.skills.status()
        self.skill_scan_snapshot = []
        self.skill_scan_requested = True
        self.skill_scan_at = 0
        self.args.auth_lock = self.accounts.lock
        self.args.auth_refreshed_callback = self.sync_active_account_from_live
        self.args.account_attribution_callback = self.accounts.attribution_for_auth
        self.wake_event = threading.Event()
        self.inactive_account_poll_event = threading.Event()
        self.cloud_maintenance_event = threading.Event()
        self.config_monitor_event = threading.Event()
        self.dashboard_cache_event = threading.Event()
        self.token_capture_event = threading.Event()
        self.stop_event = threading.Event()
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
        self._dashboard_build_lock = threading.Lock()
        self.dashboard_cache_event.set()
        print("Startup: restoring runtime state and reconciling live authentication...", flush=True)
        with self.usage_data.index.transaction() as db:
            self.runtime_state = reset_runtime_baselines(self.usage_data.index.get(db, "runtimeState", load_state(args.state)))
        self.restore_console_event_state()
        account_status = self.accounts.status()
        self.account_statuses = {account["id"]: None for account in account_status["items"]}
        from monitor_sampling import accepted_windows
        with self.usage_data.index.transaction() as db:
            for account in account_status["items"]:
                state = self.usage_data.index.get(db, f"samplingState:local:{self.cloud.usage_account_id(account['id'])}", {"windows": {}})
                windows = accepted_windows(state["windows"])
                if windows:
                    checked_at = max(window["percentCheckedAt"] for window in windows.values())
                    self.account_statuses[account["id"]] = {"checkedAt": checked_at, "percentCheckedAt": checked_at, "windows": windows,
                        "accountSlotId": account["id"], "activeAccountSlotId": account["id"], "accountLabel": account["label"]}
        if not self.runtime_state.get("activeAccountSlotId"):
            active_account_id = account_status["activeAccountId"]
            self.runtime_state["activeAccountSlotId"] = active_account_id
            if isinstance(self.runtime_state.get("lastSample"), dict) and not self.runtime_state["lastSample"].get("activeAccountSlotId"):
                self.runtime_state["lastSample"]["activeAccountSlotId"] = active_account_id
            self._write_runtime_state_locked()
        self.sync_active_account_from_live()
        self.sync_device_auth()

    def _write_runtime_state_locked(self) -> None:
        from monitor_history import compact_monitor_state
        with self.usage_data.index.transaction() as db:
            self.usage_data.index.put(db, "runtimeState", compact_monitor_state(self.runtime_state))
        # The legacy status file contains no derived quota state.
        write_state(self.args.state, {key: value for key, value in self.runtime_state.items() if key not in {"windows", "lastSample", "remoteUsageIdentity"}})

    def restore_console_event_state(self) -> None:
        with self.usage_data.index.transaction() as db:
            self.console_event_state = self.usage_data.index.get(db, "consoleState")
        if not isinstance(self.console_event_state, dict):
            self.console_event_state = {"windows": {"5h": {}, "7d": {}}}
        if delta_state_needs_hydration(self.console_event_state):
            windows = self.console_event_state.get("windows") or {}
            self.console_event_state["windows"] = {label: windows.get(label) if isinstance(windows, dict) and isinstance(windows.get(label), dict) else {} for label in ("5h", "7d")}
            _, local_ledger = self.usage_data.datasets("local")
            console_accounts = dashboard_accounts(self)
            slot_usage_accounts = {str(account["id"]): str(account["usageAccountId"]) for account in console_accounts["items"]}
            from monitor_sampling import stored_points
            with self.usage_data.index.transaction() as db:
                quota_points = stored_points(db, "local")
            historical_cost_usage = dashboard_cost_usage(quota_points, local_ledger, slot_usage_accounts)
            hydrate_delta_state_from_events(self.console_event_state, dashboard_cost_usage_delta_events(historical_cost_usage))
        # Seeding lastSample establishes a baseline; it must not count the previous run's final delta twice.
        reset_runtime_baselines(self.console_event_state)
        previous_sample = self.runtime_state.get("lastSample")
        if isinstance(previous_sample, dict) and previous_sample.get("checkedAt"):
            process_sample_delta_events(self.console_event_state, copy.deepcopy(previous_sample), None)
        self.console_event_state["_specialEvents"] = []
        with self.usage_data.index.transaction() as db:
            self.usage_data.index.put(db, "consoleState", self.console_event_state)

    def sync_device_auth(self, status: dict | None = None) -> None:
        if not hasattr(self, "device_auth"):
            return
        status = status or self.accounts.status()
        active = next((account for account in status["items"] if account["id"] == status["activeAccountId"]), None)
        self.device_auth.activate(active["id"] if active and not active.get("ready") and not active.get("isApiAccount") else None)

    def regenerate_device_code(self, account_id: str) -> dict:
        status = self.accounts.status()
        active = next((account for account in status["items"] if account["id"] == status["activeAccountId"]), None)
        if active is None or active["id"] != str(account_id or "") or active.get("ready") or active.get("isApiAccount"):
            raise AccountError("Device authentication is available only for the active OpenAI account awaiting sign-in", 409)
        self.sync_device_auth(status)
        return self.device_auth.regenerate(active["id"])

    def _record_quota_sample_locked(self, sample: dict) -> None:
        """Write raw evidence first; only SQLite decisions may update live quota status."""
        sample["sync"] = sample.get("sync") or {"version": 1, "originMachineId": self.cloud.machine_id, "accountId": self.cloud.usage_account_id(sample.get("accountSlotId"))}
        sample["sync"] = add_record_provenance("quota", sample, self.cloud.machine_id, sample["sync"]["accountId"])["sync"]
        raw_windows = copy.deepcopy(sample.get("windows") or {})
        append_quota_history_sample(self.args.quota_history, sample)
        if getattr(self.args, "sample_log", None):
            append_capped_jsonl(self.args.sample_log, {"checkedAt": sample["checkedAt"], "sample": sample}, self.args.sample_log_max_bytes)
        self.usage_data.ingest_local()
        from monitor_usage_sync import record_account_key
        from monitor_sampling import WINDOWS, accepted_windows
        with self.usage_data.index.transaction() as db:
            state = self.usage_data.index.get(db, f"samplingState:local:{record_account_key(sample)}", {"windows": {}})
            point = db.execute("SELECT data FROM processing_sampled_quota WHERE view='local' AND account=? AND record_key=?", (record_account_key(sample), sample["checkedAt"])).fetchone()
        point = json.loads(point[0]) if point else {}
        sample["windows"], sample["rejectedWindows"] = accepted_windows(state["windows"]), {}
        for name, (label, _) in WINDOWS.items():
            if raw_windows.get(label, {}).get("usedPercent") is not None and point.get(name, {}).get("continuous") is None:
                sample["rejectedWindows"][label] = raw_windows[label]
        sample["usingPreviousWindows"] = bool(sample["rejectedWindows"])
        sample["percentCheckedAt"] = max((window["percentCheckedAt"] for window in sample["windows"].values()), default=None)
        if sample.get("remoteUsage") is not None:
            sample["remoteUsage"]["accepted"] = not sample["rejectedWindows"]

    def _update_account_status_locked(self, account_id: str | None, sample: dict) -> bool:
        row = normalize_quota_history_row(sample)
        if not account_id or row is None:
            return False
        previous = self.account_statuses.get(account_id)
        if isinstance(previous, dict) and (parse_timestamp(row["checkedAt"]) or 0) < (parse_timestamp(previous.get("percentCheckedAt") or previous.get("checkedAt")) or 0):
            return False
        label = sample.get("accountLabel") or row.get("accountLabel") or next((account["label"] for account in self.accounts.status()["items"] if account["id"] == account_id), "Unknown")
        self.account_statuses[account_id] = sample | {
            "checkedAt": sample.get("checkedAt") or row["checkedAt"],
            "percentCheckedAt": sample.get("percentCheckedAt") or row["checkedAt"],
            "accountSlotId": account_id,
            "accountLabel": label,
            "activeAccountSlotId": account_id,
            "windows": ((previous or {}).get("windows") or {}) | sample["windows"],
        }
        if event := getattr(self, "session_refresh_event", None):
            event.set()
        return True

    def _active_account_status_locked(self, accounts: dict, account_id: str | None = None) -> dict | None:
        if accounts["awaitingLogin"] and account_id in (None, accounts["activeAccountId"]):
            return None
        if not hasattr(self, "account_statuses"):
            self.account_statuses = {}
        for account in accounts["items"]:
            self.account_statuses.setdefault(account["id"], None)
        active_id = account_id if account_id is not None else accounts["activeAccountId"]
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
            errors = getattr(self, "inactive_account_poll_errors", {})
            if error := errors.get(account["usageAccountId"]) or errors.get(account["id"]):
                account.update({"pollError": True, "pollErrorAt": error["at"], "stale": True})
        return status


    def status_payload(self) -> dict:
        with self.lock:
            accounts = self._dashboard_accounts_locked()
            if not accounts["awaitingLogin"] and self.last_error and self.last_error.startswith("Waiting for Codex login"):
                self.wake_event.set()
            last_sample = self._active_account_status_locked(accounts)
            if last_sample is not None and getattr(self.args, "background_token_capture", False):
                last_sample = last_sample | {"tokenUsage": getattr(self.args, "token_capture_snapshot", None), "cost": self.runtime_state.get("cost"), "costByModel": self.runtime_state.get("costByModel")}
            sample = dashboard_sample(last_sample)
            account_statuses = {}
            for account in accounts["items"]:
                account_sample = last_sample if account["id"] == accounts["activeAccountId"] else self._active_account_status_locked(accounts, account["id"])
                account_statuses[account["id"]] = {"lastSample": dashboard_sample(account_sample), "display": dashboard_display(account_sample)}
            control_password_configured = control_password_is_configured(self.cloud.config()["control"]) if hasattr(self, "cloud") else True
            processing = {"ingestion": dict(self.usage_data.index.metrics), "projection": dict(self.projector.metrics)} if hasattr(self, "projector") else None
            return {
                "revision": _dashboard_revision({"sample": sample, "accountStatuses": {key: value["lastSample"] for key, value in account_statuses.items()}, "accounts": accounts, "error": self.last_error, "controlPasswordConfigured": control_password_configured, **({"processing": processing} if processing else {})}),
                "controlPasswordConfigured": control_password_configured,
                "lastSample": sample,
                "display": dashboard_display(last_sample),
                "accounts": accounts,
                "accountStatuses": account_statuses,
                **({"processing": processing} if processing else {}),
            }


    def refresh_dashboard_cache(self, force: bool = False, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        with self._dashboard_build_lock:
            with self.lock:
                accounts = self._dashboard_accounts_locked()
            result = self.projector.refresh(accounts, now, force)
            if result is None:
                with self.usage_data.index.transaction() as db:
                    deadlines = [json.loads(row[0]).get("nextMaintenanceAt") for row in db.execute("SELECT data FROM processing_projections")]
                next_maintenance = min((value for value in deadlines if value is not None), default=None)
            else:
                next_maintenance = result["nextMaintenanceAt"]
            with self.lock:
                self._dashboard_next_maintenance = next_maintenance
            return result is not None

    def dashboard_cache_wait_seconds(self, now: float | None = None) -> float:
        now = time.time() if now is None else now
        with self.lock:
            next_maintenance = getattr(self, "_dashboard_next_maintenance", None)
        return DASHBOARD_DISPLAY_CACHE_MAINTENANCE_SECONDS if next_maintenance is None or next_maintenance <= now else min(DASHBOARD_DISPLAY_CACHE_MAINTENANCE_SECONDS, max(1, next_maintenance - now))

    def run_dashboard_cache_maintenance(self) -> None:
        while self.running:
            self.dashboard_cache_event.wait(self.dashboard_cache_wait_seconds())
            self.dashboard_cache_event.clear()
            try:
                with self.lock:
                        self.refresh_dashboard_cache()
                if hasattr(self, "delivery"):
                    self.delivery.prepare()
                    self.usage_data.index.maintain_disk_space()
            except Exception as exc:
                print(f"Dashboard display cache maintenance failed: {exc}", file=sys.stderr, flush=True)

    def _signal_dashboard_cache(self, reason: str = "local") -> None:
        event = getattr(self, "dashboard_cache_event", None)
        if event is not None:
            event.set()

    def state(self) -> dict:
        with self.lock:
            return copy.deepcopy(self.runtime_state)

    def quota_history(self) -> list[dict]:
        with self.lock:
            return load_quota_history(self.args.quota_history)

    def _print_usage_console_events(self, sample: dict) -> None:
        process_sample_delta_events(self.console_event_state, copy.deepcopy(sample), None)
        print_special_events(self.console_event_state.get("_specialEvents") or [])
        if hasattr(self, "usage_data") and hasattr(self.usage_data, "index"):
            with self.usage_data.index.transaction() as db:
                self.usage_data.index.put(db, "consoleState", self.console_event_state)

    def run_token_capture(self) -> None:
        while self.running:
            try:
                token_usage = self.usage_data.index.scan(self.args.codex_home)
                status = self.accounts.status()
                label = next((row["label"] for row in status["items"] if row["id"] == status["activeAccountId"]), "Unknown")
                cost, by_model = self.usage_data.index.sync_ledger(self.args.token_ledger, status["activeAccountId"], label, self.accounts.attribution_timeline(),
                    lambda row: add_record_provenance("tokenLedger", row, self.cloud.machine_id, self.cloud.usage_account_id(row.get("accountSlotId") or (row.get("session") or {}).get("accountSlotId"))),
                    self.usage_data.normalize_new_ledger, self.cloud.machine_id)
                with self.lock:
                    self.args.token_capture_snapshot = token_usage
                    self.runtime_state.update({"tokenUsage": token_usage, "cost": cost, "costByModel": by_model})
                    self.usage_data.ingest_local()
                    self._signal_dashboard_cache()
            except Exception as exc:
                print(f"Token capture failed: {exc}", file=sys.stderr, flush=True)
            self.token_capture_event.wait(min(max(self.args.interval, 1), 5))
            self.token_capture_event.clear()

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
            self.last_acquire_started_at = time.monotonic()
            self.inactive_account_poll_started_at[self.cloud.usage_account_id(account_status["activeAccountId"])] = self.last_acquire_started_at
            previous_token_usage = self.runtime_state.get("tokenUsage")
            if getattr(self.args, "background_token_capture", False):
                previous_token_usage = (self.runtime_state.get("lastSample") or {}).get("tokenUsage") or previous_token_usage
        sample = collect_usage_sample(self.args, self.opener, previous_token_usage, None, self.runtime_state)
        sample["activeAccountSlotId"] = account_status["activeAccountId"]
        sample["accountSlotId"] = account_status["activeAccountId"]
        sample["accountLabel"] = next((account["label"] for account in account_status["items"] if account["id"] == account_status["activeAccountId"]), "Unknown")
        sample["isApiAccount"] = bool(next((account.get("isApiAccount") for account in account_status["items"] if account["id"] == account_status["activeAccountId"]), False))
        sample["originMachineId"] = self.cloud.machine_id
        sample["usageAccountId"] = self.cloud.usage_account_id(account_status["activeAccountId"])
        sample["sync"] = {"version": 1, "originMachineId": self.cloud.machine_id, "accountId": sample["usageAccountId"]}
        if self.accounts.status()["activeAccountId"] != account_status["activeAccountId"]:
            if sample.get("windows"):
                with self.lock:
                    self._record_quota_sample_locked(sample)
                    self._signal_dashboard_cache()
            return {}
        api_account = is_api_auth(json.loads(self.args.auth.read_text(encoding="utf-8")))
        with self.lock:
            token_usage = sample.get("tokenUsage") or {}
            if token_usage.get("incremental") and getattr(self.args, "background_token_capture", False):
                sample["cost"], sample["costByModel"] = self.runtime_state.get("cost") or empty_cost_totals(), self.runtime_state.get("costByModel") or {}
            elif token_usage.get("incremental"):
                sample["cost"], sample["costByModel"] = self.usage_data.index.sync_ledger(
                    self.args.token_ledger, account_status["activeAccountId"], sample["accountLabel"], self.accounts.attribution_timeline(),
                    lambda row: add_record_provenance("tokenLedger", row, self.cloud.machine_id, self.cloud.usage_account_id(row.get("accountSlotId") or (row.get("session") or {}).get("accountSlotId"))),
                    self.usage_data.normalize_new_ledger, self.cloud.machine_id,
                )
            elif token_usage:
                token_sessions = sync_token_ledger(
                    self.args.token_ledger,
                    token_usage.get("events") or [],
                    account_status["activeAccountId"],
                    sample["accountLabel"],
                    self.accounts.attribution_timeline(),
                    record_provenance=lambda row: add_record_provenance("tokenLedger", row, self.cloud.machine_id, self.cloud.usage_account_id(row.get("accountSlotId") or (row.get("session") or {}).get("accountSlotId"))),
                    source_files=token_usage.get("sourceFiles"), own_machine_id=self.cloud.machine_id,
                    on_new_cost=self.accounts.promote_active_slot_on_cost,
                )
                sample["cost"], sample["costByModel"] = token_cost_snapshot(token_sessions)
                token_usage.pop("sessions", None)
                token_usage.pop("events", None)
                token_usage.pop("completeSessionIds", None)
                token_usage.pop("incompleteSessionIds", None)
                token_usage.pop("sourceFiles", None)
            self.runtime_state["activeAccountSlotId"] = account_status["activeAccountId"]
            if not api_account:
                self._record_quota_sample_locked(sample)
            else:
                append_capped_jsonl(self.args.sample_log, {"checkedAt": sample["checkedAt"], "sample": sample}, self.args.sample_log_max_bytes)
            sample.pop("costDelta", None)
            sample.pop("costDeltaByModel", None)
            self.runtime_state.update({"tokenUsage": token_usage or None, "cost": sample.get("cost") or empty_cost_totals(), "costByModel": sample.get("costByModel") or {}, "lastSample": sample, "updatedAt": sample.get("checkedAt")})
            self._write_runtime_state_locked()
            self.usage_data.ingest_local()
            self._signal_dashboard_cache()
            self.last_sample = sample
            self._update_account_status_locked(account_status["activeAccountId"], sample)
            self.inactive_account_poll_errors.pop(sample["usageAccountId"], None)
            self.last_error = None
            self._print_usage_console_events(sample)
            return sample

    def _poll_inactive_account(self, credential: dict) -> None:
        auth = json.loads(credential["data"].decode("utf-8"))
        if is_api_auth(auth):
            return
        expected_fingerprint = credential["fingerprint"]

        def commit_refreshed_credentials():
            nonlocal expected_fingerprint
            data = (json.dumps(auth, indent=2) + "\n").encode("utf-8")
            if not self.accounts.commit_polled_credentials(credential["id"], expected_fingerprint, data):
                raise UsageError("recorded credentials changed during token refresh")
            expected_fingerprint = auth_fingerprint(data)

        debug = {}
        output = fetch_usage(
            auth, self.opener, None, max(self.args.timeout, 1), debug, getattr(self.args, "retry_limit", DEFAULT_RETRY_LIMIT), refreshed_callback=commit_refreshed_credentials,
        )
        usage_account_id = self.cloud.usage_account_id(credential["id"])
        output.update({"checkedAt": now_iso(), "accountSlotId": credential["id"], "accountLabel": credential["label"], "usageAccountId": usage_account_id,
            "remoteUsage": debug, "sync": {"version": 1, "originMachineId": self.cloud.machine_id, "accountId": usage_account_id}})
        sample = make_history_sample(output, None)
        with self.lock:
            self._record_quota_sample_locked(sample)
            self._update_account_status_locked(credential["id"], sample)

    def _inactive_usage_credentials(self) -> list[dict]:
        status = self.accounts.status()
        active = self.cloud.usage_account_id(status["activeAccountId"]) if status.get("activeAccountId") else None
        priority = {slot: -index for index, slot in enumerate(status.get("slotOrder", []))}
        groups = {}
        for credential in self.accounts.inactive_ready_credentials():
            account = self.cloud.usage_account_id(credential["id"])
            if account == active:
                continue
            if account not in groups or priority.get(credential["id"], float("-inf")) > priority.get(groups[account]["id"], float("-inf")):
                groups[account] = credential | {"usageAccountId": account}
        return list(groups.values())

    def poll_due_inactive_accounts(self, now: float | None = None) -> int:
        if self.args.local_only or self.opener is None:
            return 0
        now = time.monotonic() if now is None else now
        polled = 0
        for credential in self._inactive_usage_credentials():
            active_id = self.accounts.status().get("activeAccountId")
            if active_id and self.cloud.usage_account_id(active_id) == credential["usageAccountId"]:
                continue
            if now - self.inactive_account_poll_started_at.get(credential["usageAccountId"], float("-inf")) < INACTIVE_ACCOUNT_POLL_INTERVAL_SECONDS:
                continue
            self.inactive_account_poll_started_at[credential["usageAccountId"]] = now
            try:
                if is_api_auth(json.loads(credential["data"].decode("utf-8"))):
                    continue
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            try:
                self._poll_inactive_account(credential)
                with self.lock:
                    self.inactive_account_poll_errors.pop(credential["id"], None)
                    self.inactive_account_poll_errors.pop(credential["usageAccountId"], None)
                polled += 1
            except Exception as exc:
                with self.lock:
                    self.inactive_account_poll_errors[credential["usageAccountId"]] = {"at": now_iso(), "error": str(exc)}
                print(f"Inactive account usage polling failed for {credential['label']!r}: {exc}", file=sys.stderr, flush=True)
        if polled:
            with self.lock:
                self.usage_data.ingest_local()
                self._signal_dashboard_cache()
        return polled

    def inactive_account_poll_wait_seconds(self, now: float | None = None) -> float:
        now = time.monotonic() if now is None else now
        return min((max(INACTIVE_ACCOUNT_POLL_INTERVAL_SECONDS - (now - self.inactive_account_poll_started_at.get(credential["usageAccountId"], float("-inf"))), 0) for credential in self._inactive_usage_credentials()), default=INACTIVE_ACCOUNT_POLL_INTERVAL_SECONDS)

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
        auth = json.loads(auth_data.decode("utf-8"))

        def commit_refreshed_credentials():
            nonlocal expected_fingerprint
            if credential["active"]:
                self.sync_active_account_from_live()
                return
            data = (json.dumps(auth, indent=2) + "\n").encode("utf-8")
            if not self.accounts.commit_polled_credentials(credential["id"], expected_fingerprint, data):
                raise UsageError("recorded credentials changed during token refresh")
            expected_fingerprint = auth_fingerprint(data)

        debug = {}
        output = fetch_usage(
            auth, self.opener, auth_path if credential["active"] else None, max(self.args.timeout, 1), debug, getattr(self.args, "retry_limit", DEFAULT_RETRY_LIMIT),
            auth_lock=self.accounts.lock if credential["active"] else None, refreshed_callback=commit_refreshed_credentials,
        )
        output.update({"checkedAt": now_iso(), "accountSlotId": credential["id"], "accountLabel": credential["label"], "remoteUsage": debug})
        sample = make_history_sample(output, None)
        with self.lock:
            self._record_quota_sample_locked(sample)
            self._update_account_status_locked(credential["id"], sample)
        return parse_timestamp(((sample.get("windows") or {}).get(label) or {}).get("resetAt"))

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
            if getattr(self, "skill_scan_requested", False) or time.monotonic() - getattr(self, "skill_scan_at", 0) >= 60:
                self.skill_scan_requested = False
                try:
                    self.skill_scan_snapshot = self.skills.scan(refresh=True)
                    self.skill_scan_at = time.monotonic()
                except Exception as exc:
                    print(f"Background skill scan failed: {exc}", file=sys.stderr, flush=True)
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
        self.sync_device_auth()
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
            expected_fingerprint = external_update["fingerprint"]
            try:
                def commit_refreshed_credentials():
                    nonlocal expected_fingerprint
                    data = (json.dumps(auth, indent=2) + "\n").encode("utf-8")
                    if not self.accounts.commit_polled_credentials(external_update["id"], expected_fingerprint, data):
                        raise UsageError("recorded credentials changed during external auth validation")
                    expected_fingerprint = auth_fingerprint(data)

                auth = json.loads(external_update["data"].decode("utf-8"))
                debug = {}
                output = fetch_usage(auth, self.opener, None, max(self.args.timeout, 1), debug, getattr(self.args, "retry_limit", DEFAULT_RETRY_LIMIT), refreshed_callback=commit_refreshed_credentials)
                data = (json.dumps(auth, indent=2) + "\n").encode("utf-8")
                if not self.accounts.commit_polled_credentials(external_update["id"], expected_fingerprint, data):
                    raise UsageError("recorded credentials changed during external auth validation")
                usage_account_id = self.cloud.usage_account_id(external_update["id"])
                output.update({"checkedAt": now_iso(), "accountSlotId": external_update["id"], "accountLabel": external_update["label"], "usageAccountId": usage_account_id, "remoteUsage": debug, "sync": {"version": 1, "originMachineId": self.cloud.machine_id, "accountId": usage_account_id}})
                sample = make_history_sample(output, None)
                with self.lock:
                    self._record_quota_sample_locked(sample)
                    self._update_account_status_locked(external_update["id"], sample)
                    self._signal_dashboard_cache()
                print(f"External credentials for {external_update['label']!r} were validated and saved.", flush=True)
            except Exception as exc:
                print(f"External credentials for {external_update['label']!r} were not saved because background validation failed: {exc}", file=sys.stderr, flush=True)
            finally:
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
        self.sync_device_auth(result)
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
        self.sync_device_auth(result)
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
                (path for path in (getattr(self.args, "quota_history", None), self.args.sample_log, self.args.state) if path is not None), renamed_id, renamed_label,
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

CLOUD_QUEUE_ACTIONS = {
    "/api/manage/cloud/test": ("test", (), {}),
    "/api/manage/cloud/push": ("push", (), {}),
    "/api/manage/cloud/push-all": ("push", (), {"force_full": True}),
    "/api/manage/cloud/fetch": ("fetch", (), {"include_usage": True}),
    "/api/manage/cloud/fetch-all": ("fetch", (), {"include_usage": True, "force_full": True}),
    "/api/manage/cloud/restore": ("restore_skills", ("snapshotId",), {}),
    "/api/manage/cloud/overwrite": ("overwrite_cloud_from_local", (), {}),
    "/api/manage/accounts/bind": ("bind_local_account", ("accountKey",), {}),
    "/api/manage/accounts/link": ("link_local_account", ("accountKey",), {}),
    "/api/manage/accounts/release": ("release_local_account", ("accountId",), {}),
    "/api/manage/accounts/share": ("share_local_account", ("accountId",), {}),
    "/api/manage/accounts/delete-remote": ("delete_remote_account", ("accountKey",), {}),
    "/api/manage/skills/share": ("set_skill_shared", ("name", "shared"), {}),
    "/api/manage/skills/unmanage": ("unmanage_skill", ("name",), {}),
    "/api/manage/config": ("update_config", (), {}),
    "/api/manage/config/reload": ("reload_config", (), {}),
}


def enqueue_management_action(state, control_auth, path, body):
    method, fields, kwargs = CLOUD_QUEUE_ACTIONS[path]
    kwargs = dict(kwargs)
    if not isinstance(body, dict):
        raise CloudError("Expected a JSON object", 400)
    for field in fields:
        if field == "shared":
            if not isinstance(body.get(field), bool):
                raise CloudError("shared must be a boolean", 400)
        elif not isinstance(body.get(field), str) or not body[field].strip() or len(body[field]) > 512:
            raise CloudError(f"{field} must be non-empty text of at most 512 characters", 400)
    for field in ("name", "accountKey"):
        if field in fields and (body[field] in {".", ".."} or any(char in body[field] for char in "/\\\x00?#%")):
            raise CloudError(f"Invalid {field}", 400)
    if method == "update_config" and (not isinstance(body.get("server"), dict) or not isinstance(body.get("webdav"), dict)):
        raise CloudError("Invalid config update", 400)

    def completed(result):
        if method == "update_config" and result.get("controlPasswordChanged"):
            control_auth.update(state.cloud.config()["control"])
        if method in {"update_config", "reload_config"} and getattr(state, "auto_updater", None) is not None:
            state.auto_updater.notify_config_changed()
        state._signal_dashboard_cache("cloud")
        if method in {"bind_local_account", "link_local_account", "release_local_account", "share_local_account"}:
            return {"accounts": result}
        if method == "delete_remote_account":
            return {"remoteAccounts": result, **({"alreadyDeleted": True} if isinstance(result, dict) and result.get("alreadyDeleted") else {})}
        return result

    expected_remote = state.cloud._state.get("remote", {}).get("accounts", {}).get(body.get("accountKey"))
    if method == "delete_remote_account" and expected_remote and expected_remote.get("etag"):
        kwargs["expected_etag"] = expected_remote["etag"]
    operation = state.cloud.submit_operation(method, (body,) if method == "update_config" else tuple(body[field] for field in fields), kwargs,
        context={"path": path, "body": {field: body[field] for field in fields}}, on_success=completed, validate=lambda: state.cloud.validate_queued_action(method, body, expected_remote))
    return {"operationId": operation["public"]["id"], "sessionId": state.cloud.operation_queue.session_id, "status": "queued"}


def management_payload(state: UsageDashboardState, include_remote: bool = False, refresh_scan: bool = False) -> dict:
    if refresh_scan and hasattr(state, "skill_scan_snapshot") and time.monotonic() - state.skill_scan_at >= 30:
        state.skill_scan_requested = True
        state.config_monitor_event.set()
    remote_accounts = state.cloud.cached_remote_accounts()
    api_identity_ids = state.accounts.api_identity_ids() if hasattr(state.accounts, "api_identity_ids") else {}
    accounts = dashboard_accounts(state)
    if hasattr(state, "sync_device_auth"):
        state.sync_device_auth(accounts)
    remote_api_identity_ids = {item.get("_apiIdentityId") or item.get("apiIdentityId") for item in remote_accounts if item.get("accountType") == "api"}
    for account in accounts["items"]:
        if account.get("isApiAccount"):
            account["canShare"] = bool(account.get("ready")) and api_identity_ids.get(account["id"]) not in remote_api_identity_ids
    payload = {
        "programVersion": getattr(state, "program_version", installed_version(Path(__file__).resolve().parent)),
        "server": state.cloud.config()["server"],
        "editableConfig": state.cloud.editable_config(),
        "skills": dashboard_skill_status(state.skills.status()),
        "scan": [{**{key: item.get(key) for key in ("name", "sources", "authoritativeSource", "defaultAssignments")}, **({"error": "Skill scan error"} if item.get("error") else {})} for item in (state.skill_scan_snapshot if hasattr(state, "skill_scan_snapshot") else state.skills.scan(refresh_scan))],
        "cloud": dashboard_cloud_status(state.cloud.redacted_status()),
        "accounts": accounts,
        "deviceAuth": state.device_auth.status() if hasattr(state, "device_auth") else None,
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

def serve_dashboard(args, opener: urllib.request.OpenerDirector | None, update_opener_factory=None, runtime_dir: Path | None = None) -> int:
    try:
        server_config = load_server_config(Path(args.data_home) / "config.json")
    except CloudError as exc:
        print(f"Cannot start dashboard: {exc}.", file=sys.stderr, flush=True)
        return 1
    server_host = server_config["host"]
    instance_lock = getattr(args, "instance_lock", None) or DashboardInstanceLock()
    try:
        instance_acquired = bool(getattr(args, "instance_lock", None)) or instance_lock.acquire()
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
            from monitor_streaming import Records, dashboard_json_chunks
            def streamed(value):
                return isinstance(value, Records) or isinstance(value, dict) and any(streamed(item) for item in value.values())
            if streamed(payload):
                from monitor_streaming import close_records
                with tempfile.TemporaryFile() as spool:
                    compress = "gzip" in (self.headers.get("Accept-Encoding") or "").lower()
                    output = gzip.GzipFile(fileobj=spool, mode="wb") if compress else spool
                    try:
                        for chunk in dashboard_json_chunks(dashboard_safe_json(payload) if sanitize else payload):
                            output.write(chunk)
                    finally:
                        close_records(payload)
                    if compress:
                        output.close()
                    length = spool.tell()
                    spool.seek(0)
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    for key, value in ({"Cache-Control": "no-store", "Vary": "Accept-Encoding"} | (headers or {})).items():
                        self.send_header(key, value)
                    if compress:
                        self.send_header("Content-Encoding", "gzip")
                    self.send_header("Content-Length", str(length))
                    self.end_headers()
                    for chunk in iter(lambda: spool.read(65536), b""):
                        self.wfile.write(chunk)
                return
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
                self.send_json(410, {"error": "The series protocol has been removed; use /api/dashboard scoped manifests and chunks"})
                return
            if path == "/api/dashboard":
                started = time.perf_counter()
                try:
                    query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                    if set(query) - {"view", "range", "start", "end"} or any(len(values) != 1 for values in query.values()):
                        raise ValueError("Invalid dashboard scope")
                    manifest = state.delivery.manifest(query.get("view", ["merged"])[0], query.get("range", ["All"])[0],
                        float(query["start"][0]) if "start" in query else None, float(query["end"][0]) if "end" in query else None)
                except (ValueError, OverflowError) as exc:
                    self.send_json(400, {"error": str(exc)})
                    return
                if manifest is None:
                    state.dashboard_cache_event.set()
                    self.send_json(503, {"error": "History is being prepared", "preparing": True}, {"Retry-After": "1"})
                    return
                etag = f'"dashboard-{manifest["generation"]}"'
                if self.headers.get("If-None-Match") == etag:
                    self.send_not_modified(etag)
                else:
                    self.send_json(200, manifest, {"ETag": etag, "Cache-Control": "no-cache", "Server-Timing": f"scope;dur={(time.perf_counter() - started) * 1000:.2f}"}, cache_key=etag)
                return
            if path == "/api/dashboard/chunk":
                try:
                    query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                    if set(query) != {"id"} or len(query["id"]) != 1:
                        raise ValueError("Expected one chunk identity")
                    digest = query["id"][0]
                    etag = f'"chunk-{digest}"'
                    compressed = "gzip" in (self.headers.get("Accept-Encoding") or "").lower()
                    body = state.delivery.blob(digest, compressed)
                except ValueError as exc:
                    self.send_json(400, {"error": str(exc)})
                    return
                if body is None:
                    state.dashboard_cache_event.set()
                    self.send_json(404, {"error": "Dashboard chunk expired; reload the active scope"})
                    return
                if self.headers.get("If-None-Match") == etag:
                    self.send_not_modified(etag)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Cache-Control", "private, max-age=86400, immutable")
                self.send_header("ETag", etag)
                self.send_header("Vary", "Accept-Encoding")
                if compressed:
                    self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if path == "/api/status":
                payload = dashboard_status_payload(state)
                etag = f'W/"status-{payload["revision"]}"'
                if self.headers.get("If-None-Match") == etag:
                    self.send_not_modified(etag)
                else:
                    self.send_json(200, payload, {"Cache-Control": "no-cache", "ETag": etag}, cache_key=etag)
                return
            if path == "/api/manage/cloud/queue":
                if self.require_control_auth():
                    self.send_json(200, state.cloud.operation_queue.snapshot(), sanitize=False)
                return
            if path == "/api/manage/status":
                if not self.require_control_auth():
                    return
                try:
                    query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                    self.send_json(200, management_payload(state, query.get("remote") == ["1"], query.get("scan") == ["1"]), sanitize=False)
                except (AccountError, SkillError, CloudError, OperationSkipped) as exc:
                    self.send_json(exc.status, {"error": str(exc)})
                return
            self.send_response(404)
            self.end_headers()

        def handle_post(self):
            path = urllib.parse.urlparse(self.path).path
            allowed = {
                "/api/control/login", "/api/control/setup",
                "/api/accounts", "/api/accounts/switch", "/api/accounts/rename", "/api/accounts/delete", "/api/accounts/session-refresh", "/api/accounts/device-code", "/api/manage/skills/manage", "/api/manage/skills/unmanage", "/api/manage/skills/assign", "/api/manage/skills/share",
                "/api/manage/cloud/test", "/api/manage/cloud/fetch", "/api/manage/cloud/fetch-all", "/api/manage/cloud/push", "/api/manage/cloud/push-all", "/api/manage/cloud/restore", "/api/manage/cloud/overwrite", "/api/manage/accounts/bind", "/api/manage/accounts/link", "/api/manage/accounts/release", "/api/manage/accounts/share", "/api/manage/accounts/delete", "/api/manage/accounts/delete-remote", "/api/manage/accounts/header", "/api/manage/accounts/common-header", "/api/manage/accounts/migrate-sessions", "/api/manage/server", "/api/manage/config", "/api/manage/config/reload", "/api/manage/update/check"
            }
            allowed.add("/api/manage/accounts/common")
            allowed.add("/api/manage/cloud/queue/cancel")
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
                if path == "/api/manage/cloud/queue/cancel":
                    if not isinstance(body, dict) or not isinstance(body.get("operationId"), str):
                        raise CloudError("operationId is required", 400)
                    self.send_json(200, state.cloud.operation_queue.cancel(body["operationId"]))
                    return
                if path == "/api/manage/update/check":
                    if getattr(state, "auto_updater", None) is None:
                        self.send_json(503, {"error": "Automatic update support is unavailable"})
                        return
                    try:
                        result = state.auto_updater.check_for_update_details()
                    except Exception as exc:
                        self.send_json(502, {"error": f"Update check failed: {exc}"})
                        return
                    self.send_json(200, result)
                    self.wfile.flush()
                    if result["updated"]:
                        state.auto_updater.request_restart()
                    return
                if path in CLOUD_QUEUE_ACTIONS:
                    self.send_json(202, enqueue_management_action(state, control_auth, path, body))
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
                elif path == "/api/accounts/device-code":
                    self.send_json(200, {"deviceAuth": state.regenerate_device_code(body.get("accountId"))}, sanitize=False)
                    return
                elif path == "/api/manage/skills/manage":
                    self.send_json(200, state.skills.manage(body.get("names") if isinstance(body.get("names"), list) else []))
                    return
                elif path == "/api/manage/skills/assign":
                    self.send_json(200, state.skills.assign(body.get("name"), body.get("app"), body.get("enabled") is True))
                    return
                elif path == "/api/manage/server":
                    self.send_json(200, state.cloud.update_server_config(body.get("host")))
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
                elif path == "/api/manage/accounts/migrate-sessions":
                    self.send_json(200, state.accounts.migrate_all_session_model_providers())
                    return
                elif path == "/api/manage/accounts/delete":
                    self.send_json(200, {"accounts": state.delete_account(body.get("accountId"))})
                    return
                self.send_json(200, {"accounts": result, "status": state.status_payload()})
            except (AccountError, SkillError, CloudError, OperationSkipped) as exc:
                self.send_json(exc.status, {"error": str(exc), **({"details": exc.details} if getattr(exc, "details", None) else {}), **({"decryptFailed": True} if getattr(exc, "decrypt_failed", False) else {})})
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                self.send_json(400, {"error": "Request body is not valid JSON"})
            except Exception as exc:
                self.send_json(500, {"error": f"Account operation failed: {exc}"})

        def log_message(self, fmt, *args):
            return

    try:
        print(f"Startup: binding dashboard server to {server_host}:{DASHBOARD_PORT}...", flush=True)
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
    def managed_worker(operation):
        from monitor_common import REQUEST_STOP
        REQUEST_STOP.event = getattr(state, "stop_event", None)
        operation()
    token_thread = None
    source_watcher = None
    print("Startup: starting token capture, quota polling, session refresh, dashboard, cloud, and configuration workers...", flush=True)
    if hasattr(state, "projector") and not getattr(args, "no_token_scan", True):
        from monitor_processing import SourceWatcher
        try:
            source_watcher = SourceWatcher(args.codex_home)
            state.usage_data.index.source_watcher = source_watcher
        except (ImportError, OSError) as exc:
            print(f"Session notifications unavailable; bounded polling and audits remain active: {exc}", file=sys.stderr, flush=True)
        args.background_token_capture = True
        token_thread = threading.Thread(target=managed_worker, args=(state.run_token_capture,), name="token-capture", daemon=True)
        token_thread.start()
    thread = threading.Thread(target=managed_worker, args=(state.run,), daemon=True)
    thread.start()
    inactive_account_thread = threading.Thread(target=managed_worker, args=(state.run_inactive_account_polling,), daemon=True)
    inactive_account_thread.start()
    session_refresh_thread = threading.Thread(target=managed_worker, args=(state.run_session_refreshing,), daemon=True)
    session_refresh_thread.start()
    dashboard_cache_thread = threading.Thread(target=state.run_dashboard_cache_maintenance, daemon=True)
    dashboard_cache_thread.start()
    cloud_thread = threading.Thread(target=state.run_cloud_maintenance, name="cloud-maintenance", daemon=True)
    cloud_thread.start()
    config_thread = threading.Thread(target=state.run_config_monitor, daemon=True)
    config_thread.start()
    runtime_dir = Path(runtime_dir or Path(__file__).resolve().parent).resolve()
    state.program_version = installed_version(runtime_dir)
    auto_updater = AutoUpdater(
        runtime_dir, lambda: state.cloud.config()["autoUpdate"]["enabled"], update_opener_factory, args.timeout,
    ) if update_opener_factory is not None else None
    state.auto_updater = auto_updater
    print("Startup: starting device authentication and scheduling automatic updates...", flush=True)
    state.device_auth.start()
    print("Startup: initialization complete; dashboard ready.", flush=True)
    url = f"http://127.0.0.1:{DASHBOARD_PORT}/"
    print(f"Dashboard: {url}", flush=True)
    if args.dashboard:
        webbrowser.open(url)
    if auto_updater is not None:
        auto_updater.start(server.shutdown, immediate=not getattr(args, "startup_update_checked", False))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if auto_updater is not None:
            auto_updater.stop()
        try:
            state.sync_active_account_from_live()
            state.wait_for_external_auth_validations(max(getattr(args, "timeout", 10), 1) + 1)
        except Exception as exc:
            print(f"Final auth.json reconciliation failed: {exc}", file=sys.stderr, flush=True)
        state.running = False
        if hasattr(state, "stop_event"):
            state.stop_event.set()
        state.device_auth.stop()
        state.wake_event.set()
        state.inactive_account_poll_event.set()
        state.session_refresh_event.set()
        state.dashboard_cache_event.set()
        state.cloud_maintenance_event.set()
        state.config_monitor_event.set()
        if hasattr(state, "token_capture_event"):
            state.token_capture_event.set()
        state.cloud.operation_queue.close()
        cloud_thread.join()
        if token_thread is not None:
            token_thread.join()
        if source_watcher is not None:
            source_watcher.stop()
        for worker in (thread, inactive_account_thread, session_refresh_thread, dashboard_cache_thread, config_thread):
            worker.join()
        server.server_close()
        instance_lock.release()
    return AUTO_UPDATE_RESTART if auto_updater is not None and auto_updater.restart_requested else 0
