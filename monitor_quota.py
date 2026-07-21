#!/usr/bin/env python3

from datetime import datetime, timezone
from pathlib import Path
import time
import urllib.request

from monitor_common import DEFAULT_RETRY_LIMIT, TARGET_WINDOWS, USAGE_ENDPOINT, UsageError, now_iso, refresh_access_token, request_json

QUOTA_KEYS = {
    "balance",
    "code_review_rate_limit",
    "credits",
    "has_credits",
    "hasCredits",
    "individualLimit",
    "individual_limit",
    "limit_reached",
    "limit",
    "limit_window_seconds",
    "limitId",
    "limit_id",
    "limitName",
    "limit_name",
    "planType",
    "plan_type",
    "primary",
    "rateLimitReachedType",
    "rate_limit_reached_type",
    "overage_limit_reached",
    "remaining",
    "remainingPercent",
    "remaining_percent",
    "resetsAt",
    "resets_at",
    "reset_after_seconds",
    "reset_at",
    "secondary",
    "unlimited",
    "used",
    "usedPercent",
    "used_percent",
    "windowDurationMins",
    "window_minutes",
    "window_duration_mins",
    "window_duration_minutes",
    "window_seconds",
}

def find_quota_nodes(value, path: str = "$") -> list[tuple[str, dict]]:
    if isinstance(value, dict):
        matches = [(path, value)] if QUOTA_KEYS.intersection(value) else []
        for key, child in value.items():
            matches.extend(find_quota_nodes(child, f"{path}.{key}"))
        return matches
    if isinstance(value, list):
        matches = []
        for index, child in enumerate(value):
            matches.extend(find_quota_nodes(child, f"{path}[{index}]"))
        return matches
    return []

def quota_value(node: dict, *keys: str):
    for key in keys:
        if key in node:
            return node[key]
    return None

def quota_window_minutes(node: dict) -> int | None:
    value = quota_value(node, "windowDurationMins", "window_minutes", "window_duration_mins", "window_duration_minutes")
    if value is None:
        seconds = quota_value(node, "limit_window_seconds", "window_seconds")
        if seconds is not None:
            value = seconds / 60
    try:
        return int(value)
    except (TypeError, ValueError):
        pass
    name = " ".join(str(quota_value(node, "limitName", "limit_name", "name") or "").lower().replace("_", " ").split())
    if any(token in name for token in ("5h", "5 h", "5 hour", "five hour")):
        return TARGET_WINDOWS["5h"]
    if any(token in name for token in ("7d", "7 d", "7 day", "weekly", "week")):
        return TARGET_WINDOWS["7d"]
    return None

def quota_summary(node: dict) -> dict:
    keys = QUOTA_KEYS | {"limit_name", "name", "reset_at", "resets_at", "remaining_percent", "used_percent", "plan", "accountType", "account_type", "subscriptionPlan", "subscription_plan"}
    summary = {k: v for k, v in node.items() if k in keys or isinstance(v, (int, float, bool, str, type(None))) and k.lower().endswith(("limit", "remaining", "used"))}
    used = quota_value(summary, "used_percent", "usedPercent")
    if isinstance(used, (int, float)) and "remaining_percent" not in summary and "remainingPercent" not in summary:
        summary["remaining_percent"] = max(0.0, min(100.0, 100.0 - float(used)))
    return summary

def extract_required_windows(nodes: list[tuple[str, dict]]) -> dict:
    windows = {}
    for path, node in nodes:
        mins = quota_window_minutes(node)
        for label, target_mins in TARGET_WINDOWS.items():
            if mins == target_mins and label not in windows:
                windows[label] = {"path": path, "values": quota_summary(node)}
    return windows

def compact_quota(data: dict) -> dict:
    nodes = sorted(find_quota_nodes(data), key=lambda item: len(QUOTA_KEYS.intersection(item[1])), reverse=True)
    if not nodes:
        return {"complete": False, "missingWindows": list(TARGET_WINDOWS), "windows": {}, "raw": data}
    windows = extract_required_windows(nodes)
    if windows:
        for label in TARGET_WINDOWS:
            if label not in windows:
                windows[label] = {"path": None, "values": {"used_percent": 0.0}, "unavailable": True}
    return {
        "complete": all(label in windows for label in TARGET_WINDOWS),
        "missingWindows": [label for label in TARGET_WINDOWS if label not in windows],
        "windows": windows,
        "matches": [
            {"path": path, "values": quota_summary(node)}
            for path, node in nodes[:8]
        ]
    }

def compact_quota_for_debug(compact: dict) -> dict:
    return {key: value for key, value in compact.items() if key != "matches"}

def fetch_usage(auth: dict, opener: urllib.request.OpenerDirector, auth_path: Path, timeout: int, debug: dict | None = None, retries: int = DEFAULT_RETRY_LIMIT, auth_lock=None, refreshed_callback=None, allow_token_refresh: bool = True) -> dict:
    debug = debug if debug is not None else {}
    token = refresh_access_token(auth, opener, auth_path, timeout, retries, auth_lock, refreshed_callback, allow_token_refresh)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "codex-cli",
    }
    account_id = (auth.get("tokens") or {}).get("account_id")
    if account_id:
        headers["ChatGPT-Account-Id"] = account_id
    debug.update({
        "method": "GET",
        "endpoint": USAGE_ENDPOINT,
        "requestedAt": now_iso(),
        "timeoutSeconds": timeout,
        "accountId": account_id,
        "authIdentity": {"account_id": account_id} if account_id else None,
    })
    started_at = time.monotonic()
    status, data = request_json(opener, "GET", USAGE_ENDPOINT, headers, timeout=timeout, retries=retries)
    compact = compact_quota(data)
    debug.update({
        "completedAt": now_iso(),
        "durationMs": round((time.monotonic() - started_at) * 1000, 3),
        "status": status,
        "rawResponse": data,
        "compactQuota": compact_quota_for_debug(compact),
    })
    result = {"endpoint": USAGE_ENDPOINT, "status": status, "usage": compact}
    if result["usage"]["complete"]:
        return result
    raise UsageError(f"{USAGE_ENDPOINT} did not include required 5h and 7d quota windows; missing {', '.join(result['usage']['missingWindows'])}")
