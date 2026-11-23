#!/usr/bin/env python3

import json
import time
from pathlib import Path
import urllib.request

from monitor_common import atomic_write_bytes
from monitor_common import (
    DEFAULT_REQUEST_TIMEOUT_SECONDS, DEFAULT_RETRY_LIMIT, PLAN_MULTIPLIERS, TARGET_WINDOWS, UNKNOWN_EVENT_ACCOUNT_ID, UNKNOWN_EVENT_ACCOUNT_LABEL, UsageError, coerce_float, empty_cost_totals,
    load_json, now_iso, parse_timestamp,
)
from monitor_accounts import is_api_auth
from monitor_quota import fetch_usage
from monitor_tokens import (
    calculate_token_costs, calculate_token_costs_by_model, cost_progress, cost_progress_by_model, normalize_saved_token_totals,
    sum_cost_totals, token_progress,
)


def quota_used_percent(values: dict) -> float | None:
    return values.get("used_percent")

def quota_reset_at(values: dict) -> int | None:
    return values.get("reset_at")


def normalize_plan_type(value) -> str:
    if value is None:
        return "unknown"
    text = " ".join(str(value).lower().replace("-", " ").replace("_", " ").split())
    if not text:
        return "unknown"
    if "5x" in text or "5 x" in text or ("pro" in text and ("lite" in text or "light" in text or "5" in text)):
        return "pro_lite"
    if "10x" in text or "10 x" in text or "20x" in text or "20 x" in text or ("pro" in text and "plus" not in text):
        return "pro"
    if "plus" in text:
        return "plus"
    return "unknown"

def quota_plan(values: dict) -> dict:
    plan = normalize_plan_type(values.get("plan_type"))
    return {"type": plan, "multiplier": PLAN_MULTIPLIERS[plan]}

def output_plan(output: dict) -> dict:
    usage = output.get("usage") or {}
    for item in list((usage.get("windows") or {}).values()):
        plan = quota_plan(item.get("values") or {})
        if plan["type"] != "unknown":
            return plan
    return {"type": "unknown", "multiplier": PLAN_MULTIPLIERS["unknown"]}

def quota_history_windows(output: dict) -> dict:
    from monitor_interpretation import multiplier
    windows = {}
    fallback_plan = output_plan(output)
    for label, item in ((output.get("usage") or {}).get("windows") or {}).items():
        values = item.get("values") or {}
        plan = quota_plan(values)
        if plan["type"] == "unknown":
            plan = fallback_plan
        used = quota_used_percent(values)
        windows[label] = {
            "usedPercent": used,
            "resetAt": quota_reset_at(values),
            "path": item.get("path"),
            "plan": plan["type"],
            "planMultiplier": multiplier(plan["type"], parse_timestamp(output.get("checkedAt")) or time.time()),
            "unavailable": bool(item.get("unavailable")) or used is None,
        }
    return windows

def normalize_quota_history_row(row: dict) -> dict | None:
    if not isinstance(row, dict) or not row.get("checkedAt"):
        return None
    windows = {}
    for label in TARGET_WINDOWS:
        window = (row.get("windows") or {}).get(label) or {}
        used_percent = coerce_float(window.get("usedPercent"))
        if used_percent is None or window.get("unavailable"):
            continue
        plan = normalize_plan_type(window.get("plan"))
        windows[label] = {"usedPercent": used_percent, "resetAt": window.get("resetAt"), "plan": "plus" if plan == "unknown" else plan}
    if not windows:
        return None
    normalized = {
        "checkedAt": row["checkedAt"],
        "accountSlotId": str(row.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID),
        "accountLabel": str(row.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL),
        "windows": windows,
    }
    if isinstance(row.get("sync"), dict):
        normalized["sync"] = row["sync"]
    return normalized


def quota_history_rows_from_sample(sample: dict) -> list[dict]:
    """Keep independently refreshed windows at their actual observation times."""
    if not isinstance(sample, dict):
        return []
    rejected = sample.get("rejectedWindows")
    if not isinstance(rejected, dict) and (sample.get("usingPreviousWindows") or (sample.get("remoteUsage") or {}).get("accepted") is False):
        return []
    rejected = rejected if isinstance(rejected, dict) else {}
    grouped = {}
    for label, window in ((sample.get("windows") or {}) | rejected).items():
        if label not in TARGET_WINDOWS or not isinstance(window, dict):
            continue
        checked_at = (sample.get("checkedAt") if label in rejected else window.get("percentCheckedAt")
            or sample.get("percentCheckedAt") or sample.get("checkedAt"))
        grouped.setdefault(checked_at, {})[label] = window
    rows = []
    for checked_at, windows in grouped.items():
        row = sample | {"checkedAt": checked_at, "windows": windows}
        if isinstance(row.get("sync"), dict) and checked_at != sample.get("checkedAt"):
            row["sync"] = {key: value for key, value in row["sync"].items() if key != "recordId"}
        if (row := normalize_quota_history_row(row)) is not None:
            rows.append(row)
    return sorted(rows, key=lambda row: parse_timestamp(row["checkedAt"]) or 0)


def replace_account_label(value, account_id: str, label: str) -> int:
    changed = 0
    if isinstance(value, dict):
        if value.get("accountSlotId") == account_id and value.get("accountLabel") != label:
            value["accountLabel"] = label
            changed += 1
        for child in value.values():
            changed += replace_account_label(child, account_id, label)
    elif isinstance(value, list):
        for child in value:
            changed += replace_account_label(child, account_id, label)
    return changed


def write_state(path: Path, state: dict) -> None:
    atomic_write_bytes(path, (json.dumps(compact_monitor_state(state), ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))

def compact_token_usage_for_state(token_usage: dict | None) -> dict | None:
    if not isinstance(token_usage, dict):
        return None
    return {"totals": normalize_saved_token_totals(token_usage.get("totals"))}

def compact_sample_for_state(sample: dict | None) -> dict | None:
    if not isinstance(sample, dict):
        return None
    compact = {
        "checkedAt": sample["checkedAt"],
        "percentCheckedAt": sample.get("percentCheckedAt") or sample["checkedAt"],
        "endpoint": sample.get("endpoint"),
        "status": sample.get("status"),
        "windows": sample["windows"],
        "errors": sample["errors"],
        "usingPreviousWindows": bool(sample.get("usingPreviousWindows")),
        "tokenDelta": normalize_saved_token_totals(sample.get("tokenDelta")),
        "cost": sample.get("cost") or empty_cost_totals(),
        "costByModel": sample.get("costByModel") or {},
    }
    if sample.get("activeAccountSlotId"):
        compact["activeAccountSlotId"] = sample["activeAccountSlotId"]
    if sample.get("accountSlotId"):
        compact["accountSlotId"] = sample["accountSlotId"]
        compact["accountLabel"] = sample.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL
    return compact

def compact_monitor_state(state: dict) -> dict:
    compact = {
        "windows": state.get("windows") or {},
        "tokenUsage": compact_token_usage_for_state(state.get("tokenUsage")),
        "cost": state.get("cost") or empty_cost_totals(),
        "costByModel": state.get("costByModel") or {},
        "lastSample": compact_sample_for_state(state.get("lastSample")),
        "updatedAt": state.get("updatedAt"),
    }
    if "remoteUsageIdentity" in state:
        compact["remoteUsageIdentity"] = state["remoteUsageIdentity"]
    if "activeAccountSlotId" in state:
        compact["activeAccountSlotId"] = state["activeAccountSlotId"]
    return compact

def reset_runtime_baselines(state: dict) -> dict:
    for key in ("runCostUsd", "runCostByModelUsd", "measuredCostIntervals", "hasRuntimeCostBaseline", "_specialEvents"):
        state.pop(key, None)
    for window_state in (state.get("windows") or {}).values():
        for key in (
            "baselinePercent",
            "baselineCostUsd",
            "baselineResetAt",
            "baselinePlan",
            "baselineMultiplier",
            "baselineObservedAt",
            "previousCostUsd",
            "rollbackBaselinePercent",
            "rollbackBaselineCostUsd",
            "rollbackBaselineResetAt",
            "backwardResetCandidateAt",
            "backwardResetCandidatePercent",
            "backwardResetCandidateCostUsd",
            "baselineCostByModelUsd",
            "previousCostByModelUsd",
            "rollbackBaselineCostByModelUsd",
            "awaitingTrustedPercentBaseline",
        ):
            window_state.pop(key, None)
    return state

def make_history_sample(output: dict, previous_token_usage: dict | None, previous_cost: dict | None = None, checked_at: str | None = None, previous_cost_by_model: dict | None = None) -> dict:
    sample = {
        "checkedAt": checked_at if checked_at is not None else output["checkedAt"],
        "endpoint": output.get("endpoint"),
        "status": output.get("status"),
        "windows": quota_history_windows(output),
        "errors": {"quotaWindows": (output.get("usage") or {}).get("errors")} if (output.get("usage") or {}).get("errors") else {},
    }
    if output.get("accountSlotId"):
        sample["accountSlotId"] = output["accountSlotId"]
        sample["accountLabel"] = output.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL
    if output.get("remoteUsage"):
        sample["remoteUsage"] = output["remoteUsage"]
    if isinstance(output.get("sync"), dict):
        sample["sync"] = output["sync"]
    if output.get("tokenUsage"):
        token_usage = output["tokenUsage"]
        token_usage["progressSinceLastCheck"] = token_progress(token_usage, previous_token_usage)
        sample["tokenUsage"] = token_usage
        sample["tokenDelta"] = token_usage["progressSinceLastCheck"]
        sample["cost"] = calculate_token_costs(token_usage)
        sample["costByModel"] = calculate_token_costs_by_model(token_usage)
        sample["costDelta"] = cost_progress(sample["cost"], previous_cost)
        sample["costDeltaByModel"] = cost_progress_by_model(sample["costByModel"], previous_cost_by_model)
    return sample

def collect_usage_sample(args, opener: urllib.request.OpenerDirector | None, previous_token_usage: dict | None, previous_cost: dict | None = None, runtime_state: dict | None = None) -> dict:
    output = {}
    auth = load_json(args.auth)
    if not is_api_auth(auth):
        output["remoteUsage"] = {"_response_callback": getattr(args, "raw_quota_response_callback", None)}
        if opener is None:
            raise UsageError("no HTTP opener is available")
        if getattr(args, "account_attribution_callback", None):
            output.update(args.account_attribution_callback(auth))
        output.update(fetch_usage(
            auth, opener, args.auth, DEFAULT_REQUEST_TIMEOUT_SECONDS, output["remoteUsage"], DEFAULT_RETRY_LIMIT,
            getattr(args, "auth_lock", None), getattr(args, "auth_refreshed_callback", None), False,
        ))
    output["tokenUsage"] = getattr(args, "token_capture_snapshot", None) if getattr(args, "background_token_capture", False) else (
        args.source_ingestor.scan(args.codex_home) if getattr(args, "source_ingestor", None) is not None else None)
    output["checkedAt"] = (output.get("remoteUsage") or {}).get("completedAt") or now_iso()
    return make_history_sample(output, previous_token_usage, previous_cost, previous_cost_by_model=(runtime_state or {}).get("costByModel"))
