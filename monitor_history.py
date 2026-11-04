#!/usr/bin/env python3

import json
import time
from pathlib import Path
import urllib.request

from monitor_common import atomic_write_bytes as _write_atomic_bytes
from monitor_common import (
    DEFAULT_RETRY_LIMIT, PLAN_MULTIPLIERS, TARGET_WINDOWS, UNKNOWN_EVENT_ACCOUNT_ID, UNKNOWN_EVENT_ACCOUNT_LABEL, UsageError, codex_switch_home, coerce_float, empty_cost_totals,
    load_json, now_iso, parse_timestamp,
)
from monitor_accounts import is_api_auth
from monitor_quota import fetch_usage
from monitor_tokens import (
    calculate_token_costs, calculate_token_costs_by_model, cost_progress, cost_progress_by_model, normalize_saved_token_totals,
    scan_codex_token_usage, sum_cost_totals, token_progress,
)


def default_history_path(data_home: Path | None = None) -> Path:
    return (Path(data_home) if data_home is not None else codex_switch_home()) / "usage_monitor_history.jsonl"

def default_sample_log_path(history_path: Path) -> Path:
    """Legacy diagnostic path retained only for released data migrations."""
    return history_path.with_name("usage_monitor_diagnostic_samples.jsonl") if history_path.name == "usage_monitor_history.jsonl" else history_path.with_suffix(".samples.jsonl")

def default_quota_history_path(history_path: Path) -> Path:
    return history_path.with_name("usage_monitor_quota_readings.jsonl") if history_path.name == "usage_monitor_history.jsonl" else history_path.with_suffix(".quota.jsonl")

def default_dashboard_cache_path(history_path: Path) -> Path:
    return history_path.with_name("usage_monitor_dashboard_cache.json") if history_path.name in {"usage_monitor_history.jsonl", "usage_monitor_quota_history.jsonl", "usage_monitor_quota_readings.jsonl"} else history_path.with_suffix(".dashboard-cache.json")

def normalize_token_session_row(row: dict) -> dict | None:
    if not isinstance(row, dict) or not row.get("sessionId") or not isinstance(row.get("tokens"), dict):
        return None
    by_model = {}
    fast_by_model = {}
    for model, value in (row.get("byModel") or {}).items():
        if not isinstance(value, dict):
            continue
        tokens = normalize_saved_token_totals(value.get("tokens"))
        stored_cost = value.get("cost")
        normalized_value = {"tokens": tokens, "cost": stored_cost if isinstance(stored_cost, dict) and stored_cost else calculate_token_costs({"byModel": {model: tokens}})}
        if isinstance(value.get("fastTokens"), dict):
            normalized_value["fastTokens"] = normalize_saved_token_totals(value["fastTokens"])
            fast_by_model[model] = normalized_value["fastTokens"]
            if not (isinstance(stored_cost, dict) and stored_cost):
                normalized_value["cost"] = calculate_token_costs({"byModel": {model: tokens}, "fastByModel": {model: normalized_value["fastTokens"]}})
        by_model[str(model)] = normalized_value
    stored_cost = row.get("cost")
    normalized = {
        "sessionId": str(row["sessionId"]),
        "startedAt": row.get("startedAt"),
        "updatedAt": row.get("updatedAt") or row.get("startedAt"),
        "accountSlotId": str(row.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID),
        "accountLabel": str(row.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL),
        "tokens": normalize_saved_token_totals(row["tokens"]),
        "cost": stored_cost if isinstance(stored_cost, dict) and stored_cost else sum_cost_totals(*(value["cost"] for value in by_model.values())) if by_model else stored_cost or empty_cost_totals(),
        "byModel": by_model,
    }
    if isinstance(row.get("sync"), dict):
        normalized["sync"] = row["sync"]
    return normalized


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

def load_history(path: Path) -> list[dict]:
    rows = []
    try:
        parsed_rows = parse_history_rows(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return rows
    for row in parsed_rows:
        if is_grouped_delta_event_row(row):
            rows.extend(normalize_delta_event_account(event) for event in expand_grouped_delta_event_row(row))
        else:
            rows.append(normalize_delta_event_account(row))
    return rows

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
    if "quotaRunVersion" in row:
        from monitor_quota_runs import validate_run
        validate_run(row)
        normalized["quotaRunVersion"] = row["quotaRunVersion"]
        normalized["windows"] = {label: dict(window) for label, window in row["windows"].items()}
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


def load_quota_history(path: Path) -> list[dict]:
    try:
        content = path.read_bytes()
    except FileNotFoundError:
        return []
    try:
        rows = parse_history_rows(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        if content.endswith((b"\n", b"\r")) or b"\n" not in content:
            raise
        rows = parse_history_rows(content[:content.rfind(b"\n") + 1].decode("utf-8"))
    return [normalized for row in rows if (normalized := normalize_quota_history_row(row)) is not None]


def is_delta_event_row(row: dict) -> bool:
    return "window" in row and "deltaPercent" in row and "deltaCostUsd" in row and "windows" not in row

def normalize_delta_event_account(row: dict) -> dict:
    if not is_delta_event_row(row):
        return row
    return row | {"accountSlotId": str(row.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID), "accountLabel": str(row.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL)}

def parse_history_rows(text: str) -> list[dict]:
    decoder = json.JSONDecoder()
    rows = []
    index = 0
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index >= len(text):
            break
        try:
            row, index = decoder.raw_decode(text, index)
        except json.JSONDecodeError:
            return parse_history_jsonl_rows(text)
        if isinstance(row, dict):
            rows.append(row)
    return rows

def parse_history_jsonl_rows(text: str) -> list[dict]:
    rows = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows

def is_grouped_delta_event_row(row: dict) -> bool:
    return isinstance(row, dict) and row.get("window") in TARGET_WINDOWS and isinstance(row.get("delta"), list)

def expand_grouped_delta_event_row(row: dict) -> list[dict]:
    result = []
    for item in row["delta"]:
        if isinstance(item, dict) and isinstance(item.get("models"), list):
            result.extend(expand_model_delta_event(item, row["window"]))
            continue
        for event in item if isinstance(item, list) else [item]:
            if isinstance(event, dict):
                result.append(event | {"window": row["window"]})
    return result

def expand_model_delta_event(item: dict, window: str) -> list[dict]:
    models = [model for model in item["models"] if isinstance(model, dict) and isinstance(model.get("model"), str) and coerce_float(model.get("deltaCostUsd")) is not None]
    delta_percent = coerce_float(item.get("deltaPercent"))
    total_cost = sum(coerce_float(model["deltaCostUsd"]) for model in models)
    if not models or delta_percent is None or total_cost <= 0:
        return []
    result = []
    used_percent = 0.0
    for index, model in enumerate(models):
        model_percent = delta_percent - used_percent if index == len(models) - 1 else delta_percent * coerce_float(model["deltaCostUsd"]) / total_cost
        result.append({key: value for key, value in item.items() if key != "models"} | model | {"window": window, "deltaPercent": round(model_percent, 8)})
        used_percent += model_percent
    return result

def compact_model_delta_events(events: list[dict]) -> dict | None:
    if len(events) < 2 or any(not isinstance(event.get("model"), str) or coerce_float(event.get("deltaPercent")) is None or coerce_float(event.get("deltaCostUsd")) is None or coerce_float(event.get("costPercentRatio")) is None for event in events):
        return None
    if any(event.get("accountSlotId") != events[0].get("accountSlotId") for event in events):
        return None
    delta_percent = round(sum(coerce_float(event["deltaPercent"]) for event in events), 8)
    delta_cost = sum(coerce_float(event["deltaCostUsd"]) for event in events)
    ratio = coerce_float(events[0].get("costPercentRatio"))
    if delta_percent <= 0 or delta_cost <= 0 or ratio is None:
        return None
    if any(abs(coerce_float(event["deltaPercent"]) - delta_percent * coerce_float(event["deltaCostUsd"]) / delta_cost) > 0.00000001 or abs(coerce_float(event.get("costPercentRatio")) - ratio) > 0.00000001 for event in events):
        return None
    compact = {
        "checkedAt": events[0].get("checkedAt"),
        "accountSlotId": events[0].get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID,
        "accountLabel": events[0].get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL,
        "deltaPercent": delta_percent,
        "models": [{"model": event["model"], "deltaCostUsd": event["deltaCostUsd"]} for event in events],
        "costPercentRatio": events[0]["costPercentRatio"],
    }
    if isinstance(events[0].get("sync"), dict) and all(event.get("sync") == events[0]["sync"] for event in events):
        compact["sync"] = events[0]["sync"]
    return compact

def grouped_delta_event_rows(rows: list[dict]) -> list[dict]:
    result = []
    for label in TARGET_WINDOWS:
        events = [
            {key: value for key, value in row.items() if key != "window"}
            for row in rows
            if row.get("window") == label
        ]
        if events:
            delta = []
            index = 0
            while index < len(events):
                split = [events[index]]
                index += 1
                while index < len(events) and events[index].get("checkedAt") == split[0].get("checkedAt"):
                    split.append(events[index])
                    index += 1
                delta.append(split[0] if len(split) == 1 else compact_model_delta_events(split) or split)
            result.append({"window": label, "delta": delta})
    result.extend(row for row in rows if row.get("window") not in TARGET_WINDOWS)
    return result


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


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    _write_atomic_bytes(path, data)


def write_history(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [normalize_delta_event_account(row) for row in rows]
    output_rows = grouped_delta_event_rows(rows) if all(is_delta_event_row(row) for row in rows) else rows
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for row in output_rows:
            f.write(format_history_row(row))
            f.write("\n")

def format_history_row(row: dict) -> str:
    if not is_grouped_delta_event_row(row):
        return json.dumps(row, ensure_ascii=False, indent=2)
    lines = [
        "{",
        f'  "window": {json.dumps(row["window"], ensure_ascii=False)},',
        '  "delta": [',
    ]
    for index, event in enumerate(row["delta"]):
        suffix = "," if index < len(row["delta"]) - 1 else ""
        lines.append(f"    {json.dumps(event, ensure_ascii=False, separators=(',', ':'))}{suffix}")
    lines.extend([
        "  ]",
        "}",
    ])
    return "\n".join(lines)

def write_state(path: Path, state: dict) -> None:
    _atomic_write_bytes(path, (json.dumps(compact_monitor_state(state), ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8"))

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

def compact_debug_state(state: dict) -> dict:
    compact = {
        "windows": state.get("windows") or {},
        "updatedAt": state.get("updatedAt"),
    }
    if "remoteUsageIdentity" in state:
        compact["remoteUsageIdentity"] = state["remoteUsageIdentity"]
    return compact

def reset_runtime_baselines(state: dict) -> dict:
    for key in ("runCostUsd", "runCostByModelUsd", "measuredCostIntervals", "hasRuntimeCostBaseline", "_pendingCostIntervals", "_specialEvents"):
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

def apply_runtime_cost_measurement(sample: dict, runtime_state: dict) -> None:
    if not sample.get("cost"):
        sample["eventCostUsd"] = coerce_float(runtime_state.get("runCostUsd")) or 0.0
        sample["eventCostByModelUsd"] = runtime_state.get("runCostByModelUsd") or {}
        sample["eventCostReady"] = False
        return
    if not runtime_state.get("hasRuntimeCostBaseline"):
        sample["costDelta"] = empty_cost_totals()
        sample["costDeltaByModel"] = {model: empty_cost_totals() for model in sample.get("costByModel") or {}}
        runtime_state["runCostUsd"] = 0.0
        runtime_state["runCostByModelUsd"] = {}
        runtime_state["measuredCostIntervals"] = 0
        runtime_state["hasRuntimeCostBaseline"] = True
    else:
        runtime_state["runCostUsd"] = round(runtime_state["runCostUsd"] + sample["costDelta"]["totalCostUsd"], 8)
        for model, costs in (sample.get("costDeltaByModel") or {}).items():
            runtime_state["runCostByModelUsd"][model] = round(runtime_state["runCostByModelUsd"].get(model, 0.0) + costs["totalCostUsd"], 8)
        runtime_state["measuredCostIntervals"] += 1
    sample["eventCostUsd"] = runtime_state["runCostUsd"]
    sample["eventCostByModelUsd"] = runtime_state["runCostByModelUsd"].copy()
    sample["eventCostReady"] = runtime_state["measuredCostIntervals"] >= 2

def compact_history(path: Path, retain_days: int | None) -> None:
    if retain_days is None or retain_days <= 0:
        return
    cutoff = time.time() - retain_days * 24 * 60 * 60
    kept = [row for row in load_history(path) if parse_timestamp(row.get("checkedAt")) is None or parse_timestamp(row.get("checkedAt")) >= cutoff]
    write_history(path, kept)

def collect_usage_sample(args, opener: urllib.request.OpenerDirector | None, previous_token_usage: dict | None, previous_cost: dict | None = None, runtime_state: dict | None = None, skip_remote: bool = False) -> dict:
    output = {}
    auth = load_json(args.auth) if not args.local_only and not skip_remote else None
    if not args.local_only and not skip_remote and not is_api_auth(auth or {}):
        output["remoteUsage"] = {"_response_callback": getattr(args, "raw_quota_response_callback", None)}
        if opener is None:
            raise UsageError("no HTTP opener is available")
        if getattr(args, "account_attribution_callback", None):
            output.update(args.account_attribution_callback(auth))
        output.update(fetch_usage(
            auth, opener, args.auth, max(args.timeout, 1), output["remoteUsage"], getattr(args, "retry_limit", DEFAULT_RETRY_LIMIT),
            getattr(args, "auth_lock", None), getattr(args, "auth_refreshed_callback", None), False,
        ))
    if not args.no_token_scan:
        output["tokenUsage"] = getattr(args, "token_capture_snapshot", None) if getattr(args, "background_token_capture", False) else (
            args.source_ingestor.scan(args.codex_home) if getattr(args, "source_ingestor", None) is not None else scan_codex_token_usage(args.codex_home))
    output["checkedAt"] = (output.get("remoteUsage") or {}).get("completedAt") or now_iso()
    return make_history_sample(output, previous_token_usage, previous_cost, previous_cost_by_model=(runtime_state or {}).get("costByModel"))
