#!/usr/bin/env python3

import json
import time
from datetime import datetime, timezone
from pathlib import Path
import urllib.request

from monitor_common import DEFAULT_RETRY_LIMIT, PLAN_MULTIPLIERS, TARGET_WINDOWS, UsageError, coerce_float, empty_cost_totals, empty_token_totals, first_value, load_json, now_iso, parse_timestamp
from monitor_quota import fetch_usage
from monitor_tokens import calculate_token_costs, cost_progress, scan_codex_token_usage, token_progress

MAX_PERCENT_ARBITRATION_RESPONSES = 5

def default_history_path(_home: Path | None = None) -> Path:
    return Path(__file__).resolve().parent / "usage_monitor_history.jsonl"

def default_sample_log_path(history_path: Path) -> Path:
    return history_path.with_name("usage_monitor_samples.jsonl") if history_path.name == "usage_monitor_history.jsonl" else history_path.with_suffix(".samples.jsonl")

def quota_used_percent(values: dict) -> float | None:
    used = coerce_float(first_value(values, "used_percent", "usedPercent", "utilization"))
    if used is not None:
        return max(0.0, used)
    remaining = coerce_float(first_value(values, "remaining_percent", "remainingPercent"))
    if remaining is not None:
        return max(0.0, 100.0 - remaining)
    used_value = coerce_float(first_value(values, "used", "usage"))
    limit_value = coerce_float(first_value(values, "limit", "individualLimit", "individual_limit"))
    if used_value is not None and limit_value and limit_value > 0:
        return max(0.0, used_value / limit_value * 100.0)
    return None

def quota_reset_at(values: dict) -> str | None:
    reset = first_value(values, "reset_at", "resetAt", "resets_at", "resetsAt")
    if isinstance(reset, (int, float)):
        return datetime.fromtimestamp(reset, timezone.utc).isoformat().replace("+00:00", "Z")
    if isinstance(reset, str) and reset:
        return reset
    return None

def normalize_plan_type(value) -> str:
    if value is None:
        return "unknown"
    text = " ".join(str(value).lower().replace("-", " ").replace("_", " ").split())
    if not text:
        return "unknown"
    if "5x" in text or "5 x" in text or ("pro" in text and ("lite" in text or "light" in text or "5" in text)):
        return "pro_lite"
    if "20x" in text or "20 x" in text or ("pro" in text and "plus" not in text):
        return "pro"
    if "plus" in text:
        return "plus"
    return "unknown"

def quota_plan(values: dict) -> dict:
    plan = normalize_plan_type(first_value(values, "planType", "plan_type", "plan", "accountType", "account_type", "subscriptionPlan", "subscription_plan"))
    return {"type": plan, "multiplier": PLAN_MULTIPLIERS[plan]}

def output_plan(output: dict) -> dict:
    usage = output.get("usage") or {}
    for item in list((usage.get("windows") or {}).values()) + list(usage.get("matches") or []):
        plan = quota_plan(item.get("values") or {})
        if plan["type"] != "unknown":
            return plan
    return {"type": "unknown", "multiplier": PLAN_MULTIPLIERS["unknown"]}

def quota_history_windows(output: dict) -> dict:
    windows = {}
    fallback_plan = output_plan(output)
    for label, item in ((output.get("usage") or {}).get("windows") or {}).items():
        values = item.get("values") or {}
        plan = quota_plan(values)
        if plan["type"] == "unknown":
            plan = fallback_plan
        windows[label] = {
            "usedPercent": quota_used_percent(values),
            "resetAt": quota_reset_at(values),
            "path": item.get("path"),
            "plan": plan["type"],
            "planMultiplier": plan["multiplier"],
        }
    return windows

def remote_output_identity(output: dict) -> dict | None:
    raw = ((output.get("remoteUsage") or {}).get("rawResponse") or {})
    auth_identity = ((output.get("remoteUsage") or {}).get("authIdentity") or {})
    identity = {key: raw.get(key) if raw.get(key) is not None else auth_identity.get(key) for key in ("user_id", "account_id", "email", "plan_type")}
    return identity if any(value is not None for value in identity.values()) else None

def normalized_output_percent(output: dict, label: str) -> float | None:
    window = quota_history_windows(output).get(label) or {}
    percent = coerce_float(window.get("usedPercent"))
    return None if percent is None else percent * (coerce_float(window.get("planMultiplier")) or PLAN_MULTIPLIERS["unknown"])

def percent_arbitration_threshold(label: str, multiplier: float | None) -> float:
    return max(25.0 if label == "5h" else 10.0, 2.0 * (multiplier or PLAN_MULTIPLIERS["unknown"]))

def output_percent_diff(output: dict, baseline: dict, label: str) -> float | None:
    current = normalized_output_percent(output, label)
    previous = coerce_float(((baseline.get("windows") or {}).get(label) or {}).get("baselinePercent"))
    return None if current is None or previous is None else current - previous

def output_consecutive_percent_diff(newer: dict, older: dict, label: str) -> float | None:
    current = normalized_output_percent(newer, label)
    previous = normalized_output_percent(older, label)
    return None if current is None or previous is None else current - previous

def output_window_multiplier(output: dict, label: str) -> float:
    return coerce_float((quota_history_windows(output).get(label) or {}).get("planMultiplier")) or PLAN_MULTIPLIERS["unknown"]

def output_identity_or_plan_switched(output: dict, state: dict | None) -> bool:
    if not state:
        return False
    identity = remote_output_identity(output)
    if state.get("remoteUsageIdentity") and identity and identity != state["remoteUsageIdentity"]:
        return True
    for label, window in quota_history_windows(output).items():
        window_state = ((state.get("windows") or {}).get(label) or {})
        if window_state.get("baselinePlan") is not None and window.get("plan") != window_state.get("baselinePlan"):
            return True
        if window_state.get("baselineMultiplier") is not None and window.get("planMultiplier") != window_state.get("baselineMultiplier"):
            return True
    return False

def weird_percent_response(output: dict, state: dict | None) -> list[dict]:
    if not state or output_identity_or_plan_switched(output, state):
        return []
    weird = []
    for label in TARGET_WINDOWS:
        diff = output_percent_diff(output, state, label)
        if diff is not None and diff > percent_arbitration_threshold(label, output_window_multiplier(output, label)):
            weird.append({"window": label, "deltaPercent": round(diff, 8), "thresholdPercent": percent_arbitration_threshold(label, output_window_multiplier(output, label))})
    return weird

def consecutive_percent_stable(newer: dict, older: dict) -> bool:
    for label in TARGET_WINDOWS:
        diff = output_consecutive_percent_diff(newer, older, label)
        if diff is not None and diff > percent_arbitration_threshold(label, output_window_multiplier(newer, label)):
            return False
    return True

def fetch_usage_with_percent_arbitration(auth: dict, opener: urllib.request.OpenerDirector, auth_path: Path, timeout: int, debug: dict, retries: int, state: dict | None = None) -> dict:
    output = fetch_usage(auth, opener, auth_path, timeout, debug, retries)
    output["remoteUsage"] = debug
    weird = weird_percent_response(output, state)
    if not weird:
        return output
    debug.setdefault("percentArbitration", {"maxResponses": MAX_PERCENT_ARBITRATION_RESPONSES, "attempts": []})["attempts"].append({"response": 1, "weird": weird})
    previous = output
    for response_index in range(2, MAX_PERCENT_ARBITRATION_RESPONSES + 1):
        attempt_debug = {}
        output = fetch_usage(auth, opener, auth_path, timeout, attempt_debug, retries)
        output["remoteUsage"] = attempt_debug
        stable = consecutive_percent_stable(output, previous)
        debug["percentArbitration"]["attempts"].append({
            "response": response_index,
            "stableWithPrevious": stable,
            "consecutiveDiffs": {
                label: None if output_consecutive_percent_diff(output, previous, label) is None else round(output_consecutive_percent_diff(output, previous, label), 8)
                for label in TARGET_WINDOWS
            },
            "debug": compact_remote_debug(attempt_debug),
        })
        if stable:
            arbitration = debug["percentArbitration"]
            arbitration["acceptedResponse"] = response_index
            debug.clear()
            debug.update(attempt_debug)
            debug["percentArbitration"] = arbitration
            output["remoteUsage"] = debug
            return output
        previous = output
    raise UsageError(f"weird percent data stayed unstable after {MAX_PERCENT_ARBITRATION_RESPONSES} API responses")

def compact_remote_debug(debug: dict) -> dict:
    return {key: value for key, value in debug.items() if key != "rawResponse"}

def load_history(path: Path) -> list[dict]:
    rows = []
    try:
        parsed_rows = parse_history_rows(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return rows
    for row in parsed_rows:
        if is_grouped_delta_event_row(row):
            rows.extend(expand_grouped_delta_event_row(row))
        else:
            rows.append(row)
    return rows


def is_delta_event_row(row: dict) -> bool:
    return "window" in row and "deltaPercent" in row and "deltaCostUsd" in row and "windows" not in row

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
    return isinstance(row, dict) and row.get("window") in TARGET_WINDOWS and isinstance(row.get("events"), list)

def expand_grouped_delta_event_row(row: dict) -> list[dict]:
    return [event | {"window": row["window"]} for event in row["events"] if isinstance(event, dict)]

def grouped_delta_event_rows(rows: list[dict]) -> list[dict]:
    result = []
    for label in TARGET_WINDOWS:
        events = [
            {key: value for key, value in row.items() if key != "window"}
            for row in rows
            if row.get("window") == label
        ]
        if events:
            result.append({"window": label, "events": events})
    result.extend(row for row in rows if row.get("window") not in TARGET_WINDOWS)
    return result

def append_history(path: Path, sample: dict) -> None:
    if is_delta_event_row(sample):
        write_history(path, load_history(path) + [sample])
        return
    append_jsonl(path, sample)

def append_jsonl(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
        f.write("\n")

def trim_jsonl_to_size(path: Path, max_bytes: int) -> None:
    if max_bytes <= 0:
        return
    try:
        if path.stat().st_size <= max_bytes:
            return
        lines = path.read_bytes().splitlines(keepends=True)
    except FileNotFoundError:
        return
    while lines and sum(len(line) for line in lines) > max_bytes:
        lines = lines[1:]
    path.write_bytes(b"".join(lines))

def append_capped_jsonl(path: Path, row: dict, max_bytes: int) -> None:
    append_jsonl(path, row)
    trim_jsonl_to_size(path, max_bytes)

def write_history(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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
        '  "events": [',
    ]
    for index, event in enumerate(row["events"]):
        suffix = "," if index < len(row["events"]) - 1 else ""
        lines.append(f"    {json.dumps(event, ensure_ascii=False, separators=(',', ':'))}{suffix}")
    lines.extend([
        "  ]",
        "}",
    ])
    return "\n".join(lines)

def state_path_for(history_path: Path) -> Path:
    if history_path.name == "usage_monitor_history.jsonl":
        return history_path.with_name("usage_monitor_state.json")
    return history_path.with_suffix(".state.json")

def load_state(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def write_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        json.dump(compact_monitor_state(state), f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")

def compact_token_usage_for_state(token_usage: dict | None) -> dict | None:
    if not isinstance(token_usage, dict):
        return None
    return {"totals": token_usage.get("totals") or empty_token_totals()}

def compact_sample_for_state(sample: dict | None) -> dict | None:
    if not isinstance(sample, dict):
        return None
    return {
        "checkedAt": sample["checkedAt"],
        "endpoint": sample.get("endpoint"),
        "status": sample.get("status"),
        "windows": sample["windows"],
        "errors": sample["errors"],
        "tokenDelta": sample.get("tokenDelta") or empty_token_totals(),
        "cost": sample.get("cost") or empty_cost_totals(),
        "costDelta": sample.get("costDelta") or empty_cost_totals(),
    }

def compact_monitor_state(state: dict) -> dict:
    compact = {
        "windows": state.get("windows") or {},
        "tokenUsage": compact_token_usage_for_state(state.get("tokenUsage")),
        "cost": state.get("cost") or empty_cost_totals(),
        "lastSample": compact_sample_for_state(state.get("lastSample")),
        "updatedAt": state.get("updatedAt"),
    }
    if "runCostUsd" in state:
        compact["runCostUsd"] = round(state["runCostUsd"], 8)
    if "measuredCostIntervals" in state:
        compact["measuredCostIntervals"] = state["measuredCostIntervals"]
    if "hasRuntimeCostBaseline" in state:
        compact["hasRuntimeCostBaseline"] = state["hasRuntimeCostBaseline"]
    if "remoteUsageIdentity" in state:
        compact["remoteUsageIdentity"] = state["remoteUsageIdentity"]
    return compact

def compact_debug_state(state: dict) -> dict:
    compact = {
        "windows": state.get("windows") or {},
        "updatedAt": state.get("updatedAt"),
    }
    if "runCostUsd" in state:
        compact["runCostUsd"] = round(state["runCostUsd"], 8)
    if "measuredCostIntervals" in state:
        compact["measuredCostIntervals"] = state["measuredCostIntervals"]
    if "hasRuntimeCostBaseline" in state:
        compact["hasRuntimeCostBaseline"] = state["hasRuntimeCostBaseline"]
    if "remoteUsageIdentity" in state:
        compact["remoteUsageIdentity"] = state["remoteUsageIdentity"]
    return compact

def reset_runtime_baselines(state: dict) -> dict:
    state["runCostUsd"] = 0.0
    state["measuredCostIntervals"] = 0
    state["hasRuntimeCostBaseline"] = False
    for window_state in (state.get("windows") or {}).values():
        for key in (
            "baselinePercent",
            "baselineCostUsd",
            "baselineResetAt",
            "baselinePlan",
            "baselineMultiplier",
            "previousCostUsd",
            "rollbackBaselinePercent",
            "rollbackBaselineCostUsd",
            "rollbackBaselineResetAt",
            "backwardResetCandidateAt",
            "backwardResetCandidatePercent",
            "backwardResetCandidateCostUsd",
        ):
            window_state.pop(key, None)
    return state

def make_history_sample(output: dict, previous_token_usage: dict | None, previous_cost: dict | None = None, checked_at: str | None = None) -> dict:
    sample = {
        "checkedAt": checked_at if checked_at is not None else output["checkedAt"],
        "endpoint": output.get("endpoint"),
        "status": output.get("status"),
        "windows": quota_history_windows(output),
        "errors": {},
    }
    if output.get("remoteUsage"):
        sample["remoteUsage"] = output["remoteUsage"]
    if output.get("tokenUsage"):
        token_usage = output["tokenUsage"]
        token_usage["progressSinceLastCheck"] = token_progress(token_usage, previous_token_usage)
        sample["tokenUsage"] = token_usage
        sample["tokenDelta"] = token_usage["progressSinceLastCheck"]
        sample["cost"] = calculate_token_costs(token_usage)
        sample["costDelta"] = cost_progress(sample["cost"], previous_cost)
    return sample

def apply_runtime_cost_measurement(sample: dict, runtime_state: dict) -> None:
    if not sample.get("cost"):
        sample["eventCostUsd"] = coerce_float(runtime_state.get("runCostUsd")) or 0.0
        sample["eventCostReady"] = False
        return
    if not runtime_state.get("hasRuntimeCostBaseline"):
        sample["costDelta"] = empty_cost_totals()
        runtime_state["runCostUsd"] = 0.0
        runtime_state["measuredCostIntervals"] = 0
        runtime_state["hasRuntimeCostBaseline"] = True
    else:
        runtime_state["runCostUsd"] = round(runtime_state["runCostUsd"] + sample["costDelta"]["totalCostUsd"], 8)
        runtime_state["measuredCostIntervals"] += 1
    sample["eventCostUsd"] = runtime_state["runCostUsd"]
    sample["eventCostReady"] = runtime_state["measuredCostIntervals"] >= 2

def compact_history(path: Path, retain_days: int | None) -> None:
    if retain_days is None or retain_days <= 0:
        return
    cutoff = time.time() - retain_days * 24 * 60 * 60
    kept = [row for row in load_history(path) if parse_timestamp(row.get("checkedAt")) is None or parse_timestamp(row.get("checkedAt")) >= cutoff]
    write_history(path, kept)

def collect_usage_sample(args, opener: urllib.request.OpenerDirector | None, previous_token_usage: dict | None, previous_cost: dict | None = None, runtime_state: dict | None = None) -> dict:
    output = {}
    if not args.local_only:
        output["remoteUsage"] = {}
        if opener is None:
            raise UsageError("no HTTP opener is available")
        output.update(fetch_usage_with_percent_arbitration(load_json(args.auth), opener, args.auth, max(args.timeout, 1), output["remoteUsage"], getattr(args, "retry_limit", DEFAULT_RETRY_LIMIT), runtime_state))
    if not args.no_token_scan:
        output["tokenUsage"] = scan_codex_token_usage(args.codex_home)
    output["checkedAt"] = now_iso()
    return make_history_sample(output, previous_token_usage, previous_cost)
