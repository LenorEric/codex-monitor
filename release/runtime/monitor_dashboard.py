#!/usr/bin/env python3

import base64
import hashlib
import hmac
import http.server
import ipaddress
import json
import os
import secrets
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import webbrowser
from datetime import datetime
from http.cookies import SimpleCookie
from pathlib import Path

from monitor_accounts import AccountError, AccountManager
from monitor_cloud import CloudError, CloudManager, control_password_is_compromised, control_password_is_configured, control_password_matches, load_server_config
from monitor_common import DEFAULT_RETRY_LIMIT, coerce_float, empty_cost_totals, is_client_disconnect, now_iso, parse_timestamp, poll_sleep_seconds, retry_operation
from monitor_events import collect_with_bad_remote_usage_retry, compact_delta_event, derive_history_events, print_ratio_warnings, print_special_events, print_valid_delta_events, process_sample_delta_events, sample_debug_log_row
from monitor_history import (
    append_capped_jsonl, append_history, append_quota_history_sample, apply_runtime_cost_measurement, collect_usage_sample, compact_history, compact_quota_history, default_quota_history_path, default_token_session_history_path, load_history,
    fetch_usage_with_percent_arbitration, load_quota_history, load_state, load_token_session_history, make_history_sample, quota_history_row_from_sample, replace_account_label, reset_runtime_baselines, rewrite_account_labels, sync_token_session_history, write_state,
)
from monitor_skills import SkillError, SkillManager
from monitor_usage_sync import UsageDataStore, add_record_provenance, default_usage_sync_cache_path

DASHBOARD_HTML_PATH = Path(__file__).with_name("dashboard.html")
MANAGEMENT_HTML_PATH = Path(__file__).with_name("management.html")
DASHBOARD_PORT = 8765
INACTIVE_ACCOUNT_POLL_INTERVAL_SECONDS = 10 * 60
CONTROL_COOKIE_NAME = "codex_monitor_control"
CONTROL_COOKIE_MAX_AGE_SECONDS = 30 * 24 * 60 * 60
SENSITIVE_DASHBOARD_FIELDS = {
    "access_token", "account_id", "accountId", "authIdentity", "baseUrl", "boundMachineId", "configPath", "email", "encryptionPassphrase", "fingerprint", "id_token", "identity",
    "password", "privatePath", "rawResponse", "refresh_token", "remoteRoot", "revisionId", "statePath", "target", "tokens", "user_id", "username",
}


def client_host_is_loopback(host) -> bool:
    try:
        return ipaddress.ip_address(host.split("%", 1)[0]).is_loopback
    except (AttributeError, ValueError):
        return False

class DashboardHTTPServer(http.server.ThreadingHTTPServer):
    allow_reuse_address = False

    def handle_error(self, request, client_address):
        if is_client_disconnect(sys.exc_info()[1]):
            return
        super().handle_error(request, client_address)

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
    return {"raw": raw, "continuous": raw, "resetAt": window.get("resetAt"), "plan": window.get("plan") or "unknown"}

def dashboard_sample(sample: dict | None) -> dict | None:
    if not isinstance(sample, dict):
        return None
    return {
        "checkedAt": sample.get("checkedAt"),
        "percentCheckedAt": sample.get("percentCheckedAt"),
        "windows": {label: {"usedPercent": ((sample.get("windows") or {}).get(label) or {}).get("usedPercent"), "resetAt": ((sample.get("windows") or {}).get(label) or {}).get("resetAt")} for label in ("5h", "7d")},
        "cost": sample.get("cost") or empty_cost_totals(),
    }

def dashboard_account_status(status: dict) -> dict:
    return {
        "activeAccountId": status.get("activeAccountId"),
        "awaitingLogin": bool(status.get("awaitingLogin")),
        "cloudBindingEnabled": bool(status.get("cloudBindingEnabled")),
        "error": "Account operation failed" if status.get("error") else None,
        "message": status.get("message"),
        "items": [{key: account.get(key) for key in ("id", "label", "ready", "active", "cloudState")} for account in status.get("items", [])],
    }

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
    return [{
        "checkedAt": checked_at,
        "timestamp": parse_timestamp(checked_at),
        "fiveHour": window_point(sample, "5h"),
        "sevenDay": window_point(sample, "7d"),
        "cost": sample.get("cost") or empty_cost_totals(),
    }]

def dashboard_quota_point(row: dict) -> dict:
    return {
        "checkedAt": row.get("checkedAt"),
        "timestamp": parse_timestamp(row.get("checkedAt")),
        "accountSlotId": row.get("accountSlotId"),
        "accountLabel": row.get("accountLabel"),
        "fiveHour": window_point(row, "5h"),
        "sevenDay": window_point(row, "7d"),
    }

def dashboard_token_session(row: dict) -> dict:
    return {
        "sessionId": row.get("sessionId"),
        "startedAt": row.get("startedAt"),
        "updatedAt": row.get("updatedAt") or row.get("startedAt"),
        "accountSlotId": row.get("accountSlotId"),
        "accountLabel": row.get("accountLabel"),
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

class UsageDashboardState:
    def __init__(self, args, opener: urllib.request.OpenerDirector | None):
        self.args = args
        if not getattr(self.args, "quota_history", None):
            self.args.quota_history = default_quota_history_path(self.args.history)
        if not getattr(self.args, "token_session_history", None):
            self.args.token_session_history = default_token_session_history_path(self.args.history)
        self.opener = opener
        self.lock = threading.RLock()
        self.accounts = AccountManager(args.auth, getattr(args, "account_root", None), getattr(args, "legacy_account_root", None))
        self.skills = SkillManager(args.codex_home, getattr(args, "data_home", None) or args.auth.parent / ".codex-switch", getattr(args, "gemini_skills", None))
        self.cloud = CloudManager(self.skills.private_root, self.skills, self.accounts)
        self.accounts.cloud = self.cloud
        self.usage_data = UsageDataStore(
            self.args.history, self.args.quota_history, self.args.token_session_history, self.cloud.machine_id, self.cloud.usage_account_id, self.lock, self.cloud.local_usage_account, getattr(self.args, "usage_sync_cache", None),
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
        self.args.auth_refreshed_callback = self.accounts.sync_active_from_live
        self.args.account_attribution_callback = self.accounts.attribution_for_auth
        self.wake_event = threading.Event()
        self.inactive_account_poll_event = threading.Event()
        self.cloud_maintenance_event = threading.Event()
        self.cloud_maintenance_connection_failed = False
        self.running = True
        self.last_sample = None
        self.last_error = None
        self.last_acquire_started_at = None
        self.inactive_account_poll_started_at = {}
        self._series_cache = {}
        self._series_build_lock = threading.Lock()
        self.runtime_state = reset_runtime_baselines(load_state(args.state))
        account_status = self.accounts.status()
        self.account_statuses = {account["id"]: None for account in account_status["items"]}
        if not self.runtime_state.get("activeAccountSlotId"):
            active_account_id = account_status["activeAccountId"]
            self.runtime_state["activeAccountSlotId"] = active_account_id
            if isinstance(self.runtime_state.get("lastSample"), dict) and not self.runtime_state["lastSample"].get("activeAccountSlotId"):
                self.runtime_state["lastSample"]["activeAccountSlotId"] = active_account_id
            write_state(args.state, self.runtime_state)

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
        return self.account_statuses.get(active_id)

    def _series_revision_locked(self, accounts: dict) -> str:
        return _dashboard_revision({
            "files": [_path_revision(path) for path in (self.args.history, self.args.quota_history, self.args.token_session_history, self.args.state, getattr(self.args, "usage_sync_cache", default_usage_sync_cache_path(self.args.history)))],
            "accounts": accounts,
        })

    def status_payload(self) -> dict:
        with self.lock:
            accounts = dashboard_account_status(self.accounts.status())
            if not accounts["awaitingLogin"] and self.last_error and self.last_error.startswith("Waiting for Codex login"):
                self.wake_event.set()
            last_sample = self._active_account_status_locked(accounts)
            sample = dashboard_sample(last_sample)
            return {
                "revision": _dashboard_revision({"sample": sample, "accounts": accounts, "error": self.last_error}),
                "seriesRevision": self._series_revision_locked(accounts),
                "controlPasswordConfigured": control_password_is_configured(self.cloud.config()["control"]) if hasattr(self, "cloud") else True,
                "lastSample": sample,
                "display": dashboard_display(last_sample),
                "accounts": accounts,
            }

    def cached_series_response(self, view: str = "local") -> tuple[dict, bytes, str]:
        view = "merged" if view == "merged" else "local"
        with self._series_build_lock:
            with self.lock:
                accounts = dashboard_account_status(self.accounts.status())
                if not accounts["awaitingLogin"] and self.last_error and self.last_error.startswith("Waiting for Codex login"):
                    self.wake_event.set()
                revision = self._series_revision_locked(accounts)
                cache = getattr(self, "_series_cache", {})
                if view in cache and cache[view][0] == revision:
                    return cache[view][1], cache[view][2], revision
                if hasattr(self, "usage_data"):
                    history, quota_history, token_sessions = self.usage_data.datasets(view)
                    if view == "local":
                        quota_history = self.usage_data.datasets("merged")[1]
                else:
                    history, quota_history, token_sessions = load_history(self.args.history), load_quota_history(self.args.quota_history), load_token_session_history(self.args.token_session_history)
                current_state = load_state(self.args.state)
                last_sample = self._active_account_status_locked(accounts)
                if last_sample is None:
                    current_state = current_state | {"lastSample": None}
                else:
                    current_state = current_state | {"lastSample": last_sample}
            payload = _dashboard_series_from_snapshot(history, quota_history, current_state, token_sessions, last_sample, accounts, revision, view)
            body = json.dumps(dashboard_safe_json(payload), ensure_ascii=False).encode("utf-8")
            with self.lock:
                self._series_cache = {key: value for key, value in getattr(self, "_series_cache", {}).items() if value[0] == revision}
                self._series_cache[view] = (revision, payload, body)
            return payload, body, revision

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
        sample["originMachineId"] = self.cloud.machine_id
        sample["usageAccountId"] = self.cloud.usage_account_id(account_status["activeAccountId"])
        sample["sync"] = {"version": 1, "originMachineId": self.cloud.machine_id, "accountId": sample["usageAccountId"]}
        with self.lock:
            token_usage = sample.get("tokenUsage") or {}
            if token_usage.get("sessions"):
                sync_token_session_history(
                    self.args.token_session_history,
                    token_usage["sessions"],
                    account_status["activeAccountId"],
                    next((account["label"] for account in account_status["items"] if account["id"] == account_status["activeAccountId"]), "Unknown"),
                    load_quota_history(self.args.quota_history) + history,
                )
                token_usage.pop("sessions", None)
            self.runtime_state["activeAccountSlotId"] = account_status["activeAccountId"]
            apply_runtime_cost_measurement(sample, self.runtime_state)
            append_quota_history_sample(self.args.quota_history, sample)
            events = process_sample_delta_events(self.runtime_state, sample, history)
            append_capped_jsonl(self.args.sample_log, sample_debug_log_row(sample, events, self.runtime_state), self.args.sample_log_max_bytes)
            for interval in self.runtime_state.pop("_pendingCostIntervals", []):
                append_history(self.args.history, add_record_provenance("cost", interval, self.cloud.machine_id, sample["usageAccountId"]))
            write_state(self.args.state, self.runtime_state)
            compact_history(self.args.history, self.args.compact_history_days)
            compact_quota_history(self.args.quota_history, self.args.compact_history_days)
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
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(credential["data"])
                stream.flush()
                os.fsync(stream.fileno())
            auth = json.loads(credential["data"].decode("utf-8"))
            debug = {}
            output = fetch_usage_with_percent_arbitration(
                auth, self.opener, temp_path, max(self.args.timeout, 1), debug, getattr(self.args, "retry_limit", DEFAULT_RETRY_LIMIT), refreshed_callback=None,
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
                    self.accounts.commit_polled_credentials(credential["id"], credential["fingerprint"], temp_path.read_bytes())
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
                self._poll_inactive_account(credential)
                polled += 1
            except Exception as exc:
                print(f"Inactive account usage polling failed for {credential['label']!r}: {exc}", file=sys.stderr, flush=True)
        if polled:
            with self.lock:
                compact_quota_history(self.args.quota_history, self.args.compact_history_days)
        return polled

    def inactive_account_poll_wait_seconds(self, now: float | None = None) -> float:
        now = time.monotonic() if now is None else now
        return min((max(INACTIVE_ACCOUNT_POLL_INTERVAL_SECONDS - (now - self.inactive_account_poll_started_at.get(credential["id"], float("-inf"))), 0) for credential in self.accounts.inactive_ready_credentials()), default=INACTIVE_ACCOUNT_POLL_INTERVAL_SECONDS)

    def run_inactive_account_polling(self) -> None:
        while self.running:
            self.poll_due_inactive_accounts()
            self.inactive_account_poll_event.wait(self.inactive_account_poll_wait_seconds())
            self.inactive_account_poll_event.clear()

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
                self.cloud.maintenance_tick()
            except Exception as exc:
                if not isinstance(exc, CloudError) or exc.category != "network" or not self.cloud_maintenance_connection_failed:
                    print(f"Cloud maintenance failed: {exc}", file=sys.stderr, flush=True)
                self.cloud_maintenance_connection_failed = isinstance(exc, CloudError) and exc.category == "network"
            else:
                self.cloud_maintenance_connection_failed = False
            self.cloud_maintenance_event.wait(5)
            self.cloud_maintenance_event.clear()

    def _account_changed(self) -> None:
        with self.lock:
            self.last_sample = None
            self.last_error = None
        self.wake_event.set()
        self.inactive_account_poll_event.set()

    def create_account(self, label: str) -> dict:
        previous = self.accounts.status()
        result = self.accounts.create_account(label)
        self._account_changed()
        if previous["activeAccountId"] is None:
            print(f"Account event: prepared {str(label).strip()!r} for sign-in.", flush=True)
        else:
            print(f"Account event: saved {next(account['label'] for account in previous['items'] if account['id'] == previous['activeAccountId'])!r} and prepared {str(label).strip()!r} for sign-in.", flush=True)
        return result

    def switch_account(self, account_id: str) -> dict:
        previous_status = self.accounts.status()
        previous_id = previous_status["activeAccountId"]
        result = self.accounts.switch(account_id)
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
        print(f"Account event: renamed {account['label'] if account else None!r} to {str(label).strip()!r}.", flush=True)
        return result

    def delete_account(self, account_id: str) -> dict:
        previous_status = self.accounts.status()
        deleted = next((account for account in previous_status["items"] if account["id"] == str(account_id or "")), None)
        result = self.accounts.delete(account_id)
        with self.lock:
            self.account_statuses.pop(str(account_id or ""), None)
        if result["activeAccountId"] != previous_status["activeAccountId"]:
            self._account_changed()
        print(f"Account event: deleted {deleted['label']!r}.", flush=True)
        return result

def dashboard_html() -> str:
    return DASHBOARD_HTML_PATH.read_text(encoding="utf-8")

def management_html() -> str:
    return MANAGEMENT_HTML_PATH.read_text(encoding="utf-8")

def management_payload(state: UsageDashboardState, include_remote: bool = False, refresh_scan: bool = False) -> dict:
    payload = {
        "server": state.cloud.config()["server"],
        "editableConfig": state.cloud.editable_config(),
        "skills": dashboard_skill_status(state.skills.status()),
        "scan": [{**{key: item.get(key) for key in ("name", "sources", "authoritativeSource", "defaultAssignments")}, **({"error": "Skill scan error"} if item.get("error") else {})} for item in state.skills.scan(refresh_scan)],
        "cloud": dashboard_cloud_status(state.cloud.redacted_status()),
        "accounts": dashboard_account_status(state.accounts.status()),
    }
    if include_remote and payload["cloud"]["webdav"].get("enabled"):
        state.cloud.fetch()
    payload["remoteAccounts"] = [{"accountKey": item.get("accountKey"), "label": item.get("label"), "bindingState": "released"} for item in state.cloud.cached_remote_accounts()]
    return payload

def _dashboard_series_from_snapshot(history: list[dict], quota_history: list[dict], current_state: dict, token_sessions: list[dict], last_sample: dict | None, accounts: dict, revision: str | None = None, view: str = "local") -> dict:
    events = derive_history_events(history)
    return {
        "seriesRevision": revision,
        "dataView": view,
        "quotaDataView": "merged",
        "points": dashboard_points_from_state(current_state),
        "quotaPoints": [dashboard_quota_point(row) for row in quota_history],
        "tokenSessions": [dashboard_token_session(row) for row in token_sessions],
        "events": events,
        "historyStats": {
            "rows": len(history),
            "fiveHourEvents": len(events["fiveHour"]),
            "sevenDayEvents": len(events["sevenDay"]),
            "fiveHourRealEvents": sum(1 for event in events["fiveHour"] if not event.get("synthetic")),
            "sevenDayRealEvents": sum(1 for event in events["sevenDay"] if not event.get("synthetic")),
        },
        "lastSample": dashboard_sample(last_sample),
        "display": dashboard_display(last_sample),
        "accounts": accounts,
    }

def dashboard_series_payload(args, state: UsageDashboardState, view: str = "local") -> dict:
    if hasattr(state, "cached_series_response"):
        return state.cached_series_response(view)[0]
    if hasattr(state, "usage_data"):
        history, quota_history, token_sessions = state.usage_data.datasets(view)
        if view == "local":
            quota_history = state.usage_data.datasets("merged")[1]
    else:
        history, quota_history = state.history(), state.quota_history()
        token_sessions = state.token_session_history() if hasattr(state, "token_session_history") else load_token_session_history(getattr(args, "token_session_history", default_token_session_history_path(args.history)))
    current_state = state.state()
    accounts = dashboard_account_status(state.accounts.status())
    last_sample = state.last_sample or current_state.get("lastSample")
    if accounts["awaitingLogin"] or not isinstance(last_sample, dict) or last_sample.get("activeAccountSlotId") != accounts["activeAccountId"]:
        last_sample = None
        current_state = current_state | {"lastSample": None}
    return _dashboard_series_from_snapshot(history, quota_history, current_state, token_sessions, last_sample, accounts, view=view)

def dashboard_status_payload(state: UsageDashboardState) -> dict:
    if hasattr(state, "status_payload"):
        return state.status_payload()
    accounts = dashboard_account_status(state.accounts.status())
    last_sample = state.last_sample
    if accounts["awaitingLogin"] or not isinstance(last_sample, dict) or last_sample.get("activeAccountSlotId") != accounts["activeAccountId"]:
        last_sample = None
    sample = dashboard_sample(last_sample)
    return {
        "revision": _dashboard_revision({"sample": sample, "accounts": accounts}),
        "seriesRevision": None,
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

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.handle_get()

        def do_POST(self):
            self.handle_post()

        def send_json_body(self, status: int, body: bytes, headers: dict | None = None):
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            if not headers or "Cache-Control" not in headers:
                self.send_header("Cache-Control", "no-store")
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, status: int, payload: dict, headers: dict | None = None, *, sanitize: bool = True):
            self.send_json_body(status, json.dumps(dashboard_safe_json(payload) if sanitize else payload, ensure_ascii=False).encode("utf-8"), headers)

        def send_not_modified(self, etag: str):
            self.send_response(304)
            self.send_header("Cache-Control", "no-cache")
            self.send_header("ETag", etag)
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

        def origin_is_allowed(self) -> bool:
            origin = self.headers.get("Origin")
            if not origin or origin in {f"http://127.0.0.1:{DASHBOARD_PORT}", f"http://localhost:{DASHBOARD_PORT}"}:
                return True
            self.send_json(403, {"error": "Cross-site control requests are not allowed"})
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
                view = "merged" if urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("view") == ["merged"] else "local"
                _, body, revision = state.cached_series_response(view)
                etag = f'"series-{view}-{revision}"'
                if self.headers.get("If-None-Match") == etag:
                    self.send_not_modified(etag)
                else:
                    self.send_json_body(200, body, {"Cache-Control": "no-cache", "ETag": etag})
                return
            if path == "/api/status":
                payload = dashboard_status_payload(state)
                etag = f'"status-{payload["revision"]}"'
                if self.headers.get("If-None-Match") == etag:
                    self.send_not_modified(etag)
                else:
                    self.send_json(200, payload, {"Cache-Control": "no-cache", "ETag": etag})
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
                "/api/accounts", "/api/accounts/switch", "/api/accounts/rename", "/api/accounts/delete", "/api/manage/skills/manage", "/api/manage/skills/unmanage", "/api/manage/skills/assign",
                "/api/manage/cloud/test", "/api/manage/cloud/fetch", "/api/manage/cloud/push", "/api/manage/cloud/restore", "/api/manage/cloud/overwrite", "/api/manage/accounts/bind", "/api/manage/accounts/release", "/api/manage/accounts/delete", "/api/manage/server", "/api/manage/config", "/api/manage/config/reload"
            }
            if path not in allowed:
                self.send_json(404, {"error": "Not found"})
                return
            if not self.origin_is_allowed():
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
                    result = state.create_account(body.get("label"))
                elif path == "/api/accounts/switch":
                    result = state.switch_account(body.get("accountId"))
                elif path == "/api/accounts/rename":
                    result = state.rename_account(body.get("accountId"), body.get("label"))
                elif path == "/api/accounts/delete":
                    result = state.delete_account(body.get("accountId"))
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
                    self.send_json(200, state.cloud.fetch())
                    return
                elif path == "/api/manage/cloud/push":
                    self.send_json(200, state.cloud.push())
                    return
                elif path == "/api/manage/cloud/overwrite":
                    self.send_json(200, state.cloud.overwrite_cloud_from_local())
                    return
                elif path == "/api/manage/cloud/restore":
                    self.send_json(200, state.cloud.restore_skills(body.get("snapshotId")))
                    return
                elif path == "/api/manage/accounts/bind":
                    self.send_json(200, {"accounts": state.cloud.bind_local_account(body.get("accountKey"))})
                    return
                elif path == "/api/manage/accounts/release":
                    self.send_json(200, {"accounts": state.cloud.release_local_account(body.get("accountId"))})
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
        print(f"Cannot start dashboard: {server_host}:{DASHBOARD_PORT} is unavailable ({exc}).", file=sys.stderr, flush=True)
        return 1
    try:
        state = UsageDashboardState(args, opener)
    except Exception:
        server.server_close()
        raise
    control_auth = ControlAuth(state.cloud.config()["control"])
    thread = threading.Thread(target=state.run, daemon=True)
    thread.start()
    inactive_account_thread = threading.Thread(target=state.run_inactive_account_polling, daemon=True)
    inactive_account_thread.start()
    cloud_thread = threading.Thread(target=state.run_cloud_maintenance, daemon=True)
    cloud_thread.start()
    url = f"http://127.0.0.1:{DASHBOARD_PORT}/"
    print(f"Dashboard: {url}", flush=True)
    if args.dashboard:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        state.running = False
        state.wake_event.set()
        state.inactive_account_poll_event.set()
        state.cloud_maintenance_event.set()
        server.server_close()
    return 0
