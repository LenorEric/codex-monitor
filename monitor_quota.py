#!/usr/bin/env python3

import math
import sys
from pathlib import Path
import time
import urllib.request

from monitor_common import DEFAULT_RETRY_LIMIT, TARGET_WINDOWS, USAGE_ENDPOINT, UsageError, UsageHttpError, now_iso, refresh_access_token, request_json

def compact_quota(data: dict) -> dict:
    """Validate each quota window independently. reset_at is the only reset authority."""
    if not isinstance(data, dict) or not isinstance(data.get("plan_type"), str) or not data["plan_type"] or not isinstance(data.get("rate_limit"), dict):
        raise UsageError(f"Incompatible usage API response from {USAGE_ENDPOINT}: plan_type and rate_limit are required")
    windows, errors = {}, []
    for key in ("primary_window", "secondary_window"):
        path = f"$.rate_limit.{key}"
        try:
            node = data["rate_limit"][key]
            if node is None:
                continue
            if type(node["limit_window_seconds"]) is not int or node["limit_window_seconds"] not in (18000, 604800):
                raise ValueError("limit_window_seconds must be 18000 or 604800")
            label = "5h" if node["limit_window_seconds"] == 18000 else "7d"
            if label in windows:
                raise ValueError(f"duplicates the {label} window")
            if type(node["used_percent"]) not in (int, float) or not math.isfinite(node["used_percent"]) or not 0 <= node["used_percent"] <= 100:
                raise ValueError("used_percent must be a finite number between 0 and 100")
            if type(node["reset_at"]) is not int or not 0 <= node["reset_at"] <= 253402300799:
                raise ValueError("reset_at must be a Unix timestamp in seconds")
            windows[label] = {"path": path, "values": {"used_percent": node["used_percent"], "reset_at": node["reset_at"],
                "limit_window_seconds": node["limit_window_seconds"], "plan_type": data["plan_type"]}}
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(f"{path}: {exc}")
    if not windows:
        raise UsageError(f"Incompatible usage API response from {USAGE_ENDPOINT}: " + ("; ".join(errors) or "rate_limit must contain at least one quota window"))
    for label in TARGET_WINDOWS:
        if label not in windows:
            windows[label] = {"path": None, "values": {}, "unavailable": True}
    return {"complete": not errors, "missingWindows": [], "windows": windows, "errors": errors}


def compact_quota_for_debug(compact: dict) -> dict:
    return {key: value for key, value in compact.items() if key != "matches"}

def fetch_usage(auth: dict, opener: urllib.request.OpenerDirector, auth_path: Path | None, timeout: int, debug: dict | None = None, retries: int = DEFAULT_RETRY_LIMIT, auth_lock=None, refreshed_callback=None, allow_token_refresh: bool = True) -> dict:
    debug = debug if debug is not None else {}
    response_callback = debug.pop("_response_callback", None)
    def request():
        received = False
        def capture(status, body):
            nonlocal received
            received = True
            try:
                response_callback(body, status, account_id)
            except Exception as exc:
                print(f"Raw quota response logging failed: {exc}", file=sys.stderr, flush=True)
        try:
            status, body = request_json(opener, "GET", USAGE_ENDPOINT, headers, timeout=timeout, retries=retries,
                **({"response_callback": capture} if response_callback else {}))
        except UsageHttpError as exc:
            if response_callback and not received:
                capture(exc.status, exc.body)
            raise
        if response_callback and not received:
            capture(status, body)
        return status, body
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
    try:
        status, data = request()
    except UsageHttpError as exc:
        if not exc.is_expired_token():
            raise
        headers["Authorization"] = f"Bearer {refresh_access_token(auth, opener, auth_path, timeout, retries, auth_lock, refreshed_callback, True, True)}"
        debug["expiredTokenRefreshAt"] = now_iso()
        status, data = request()
    debug.update({
        "completedAt": now_iso(),
        "durationMs": round((time.monotonic() - started_at) * 1000, 3),
        "status": status,
        "rawResponse": data,
    })
    compact = compact_quota(data)
    debug["compactQuota"] = compact_quota_for_debug(compact)
    return {"endpoint": USAGE_ENDPOINT, "status": status, "usage": compact}
