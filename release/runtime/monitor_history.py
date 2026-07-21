#!/usr/bin/env python3

import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
import urllib.request

from monitor_common import (
    DEFAULT_RETRY_LIMIT, PLAN_MULTIPLIERS, RESET_TIME_JITTER_SECONDS, TARGET_WINDOWS, UNKNOWN_EVENT_ACCOUNT_ID, UNKNOWN_EVENT_ACCOUNT_LABEL, UsageError, codex_switch_home, coerce_float, empty_cost_totals,
    empty_token_totals, first_value, load_json, now_iso, parse_timestamp,
)
from monitor_quota import fetch_usage
from monitor_tokens import calculate_token_costs, calculate_token_costs_by_model, cost_progress, cost_progress_by_model, normalize_saved_token_totals, scan_codex_token_usage, token_progress

MAX_PERCENT_ARBITRATION_RESPONSES = 5
SAMPLE_LOG_COMPACT_RATIO = 0.8
QUOTA_HISTORY_DISCONTINUITY_SECONDS = 4 * 60 * 60
JSONL_COPY_CHUNK_BYTES = 1024 * 1024
DEFAULT_DATA_FILES = ("usage_monitor_history.jsonl", "usage_monitor_quota_history.jsonl", "usage_monitor_token_sessions.jsonl", "usage_monitor_samples.jsonl", "usage_monitor_state.json")

def default_history_path(data_home: Path | None = None) -> Path:
    return (Path(data_home) if data_home is not None else codex_switch_home()) / "usage_monitor_history.jsonl"

def default_sample_log_path(history_path: Path) -> Path:
    return history_path.with_name("usage_monitor_samples.jsonl") if history_path.name == "usage_monitor_history.jsonl" else history_path.with_suffix(".samples.jsonl")

def default_quota_history_path(history_path: Path) -> Path:
    return history_path.with_name("usage_monitor_quota_history.jsonl") if history_path.name == "usage_monitor_history.jsonl" else history_path.with_suffix(".quota.jsonl")

def default_token_session_history_path(history_path: Path) -> Path:
    return history_path.with_name("usage_monitor_token_sessions.jsonl") if history_path.name == "usage_monitor_history.jsonl" else history_path.with_suffix(".token-sessions.jsonl")

def normalize_token_session_row(row: dict) -> dict | None:
    if not isinstance(row, dict) or not row.get("sessionId") or not isinstance(row.get("tokens"), dict):
        return None
    by_model = {}
    fast_by_model = {}
    for model, value in (row.get("byModel") or {}).items():
        if not isinstance(value, dict):
            continue
        tokens = normalize_saved_token_totals(value.get("tokens"))
        normalized_value = {"tokens": tokens, "cost": calculate_token_costs({"byModel": {model: tokens}})}
        if isinstance(value.get("fastTokens"), dict):
            normalized_value["fastTokens"] = normalize_saved_token_totals(value["fastTokens"])
            fast_by_model[model] = normalized_value["fastTokens"]
            normalized_value["cost"] = calculate_token_costs({"byModel": {model: tokens}, "fastByModel": {model: normalized_value["fastTokens"]}})
        by_model[str(model)] = normalized_value
    normalized = {
        "sessionId": str(row["sessionId"]),
        "startedAt": row.get("startedAt"),
        "updatedAt": row.get("updatedAt") or row.get("startedAt"),
        "accountSlotId": str(row.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID),
        "accountLabel": str(row.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL),
        "tokens": normalize_saved_token_totals(row["tokens"]),
        "cost": calculate_token_costs({"byModel": {model: value["tokens"] for model, value in by_model.items()}, "fastByModel": fast_by_model}) if by_model else row.get("cost") or empty_cost_totals(),
        "byModel": by_model,
    }
    if isinstance(row.get("sync"), dict):
        normalized["sync"] = row["sync"]
    return normalized

def load_token_session_history(path: Path) -> list[dict]:
    try:
        rows = parse_history_rows(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    return [normalized for row in rows if (normalized := normalize_token_session_row(row)) is not None]

def write_token_session_history(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
            for row in rows:
                if normalized := normalize_token_session_row(row):
                    stream.write(json.dumps(normalized, ensure_ascii=False, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise

def sync_token_session_history(path: Path, scanned_sessions: list[dict], account_slot_id: str, account_label: str, account_timeline: list[dict] | None = None) -> list[dict]:
    existing = {row["sessionId"]: row for row in load_token_session_history(path)}
    timeline = sorted((row for row in account_timeline or [] if row.get("accountSlotId") and parse_timestamp(row.get("checkedAt")) is not None), key=lambda row: parse_timestamp(row["checkedAt"]))
    for scanned in scanned_sessions:
        timestamp = parse_timestamp(scanned.get("updatedAt"))
        attributed = next((row for row in reversed(timeline) if parse_timestamp(row["checkedAt"]) <= timestamp), None) if timestamp is not None else None
        if not (normalized := normalize_token_session_row({**scanned, "accountSlotId": attributed.get("accountSlotId") if attributed else account_slot_id, "accountLabel": attributed.get("accountLabel") or account_label if attributed else account_label})):
            continue
        if previous := existing.get(normalized["sessionId"]):
            normalized["accountSlotId"] = previous["accountSlotId"]
            normalized["accountLabel"] = previous["accountLabel"]
            if isinstance(previous.get("sync"), dict):
                normalized["sync"] = previous["sync"]
        existing[normalized["sessionId"]] = normalized
    rows = sorted(existing.values(), key=lambda row: (parse_timestamp(row.get("updatedAt")) or 0, row["sessionId"]))
    write_token_session_history(path, rows)
    return rows

def migrate_default_monitor_data(legacy_home: Path, data_home: Path | None = None) -> list[str]:
    data_home = Path(data_home) if data_home is not None else codex_switch_home()
    data_home.mkdir(parents=True, exist_ok=True)
    moved = []
    for name in DEFAULT_DATA_FILES:
        source, target = Path(legacy_home) / name, data_home / name
        if not source.exists() or target.exists() or source.resolve() == target.resolve():
            continue
        fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
        try:
            with source.open("rb") as input_stream, os.fdopen(fd, "wb") as output_stream:
                shutil.copyfileobj(input_stream, output_stream)
                output_stream.flush()
                os.fsync(output_stream.fileno())
            try:
                os.chmod(temp_name, 0o600)
            except OSError:
                pass
            os.replace(temp_name, target)
            source.unlink()
        except Exception:
            try:
                os.unlink(temp_name)
            except OSError:
                pass
            raise
        moved.append(name)
    return moved

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
            "unavailable": bool(item.get("unavailable")),
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

def fetch_usage_with_percent_arbitration(auth: dict, opener: urllib.request.OpenerDirector, auth_path: Path, timeout: int, debug: dict, retries: int, state: dict | None = None, auth_lock=None, refreshed_callback=None, allow_token_refresh: bool = True) -> dict:
    output = fetch_usage(auth, opener, auth_path, timeout, debug, retries, auth_lock, refreshed_callback, allow_token_refresh)
    output["remoteUsage"] = debug
    weird = weird_percent_response(output, state)
    if not weird:
        return output
    debug.setdefault("percentArbitration", {"maxResponses": MAX_PERCENT_ARBITRATION_RESPONSES, "attempts": []})["attempts"].append({"response": 1, "weird": weird})
    previous = output
    for response_index in range(2, MAX_PERCENT_ARBITRATION_RESPONSES + 1):
        attempt_debug = {}
        output = fetch_usage(auth, opener, auth_path, timeout, attempt_debug, retries, auth_lock, refreshed_callback, allow_token_refresh)
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
        windows[label] = {"usedPercent": used_percent, "resetAt": window.get("resetAt"), "plan": normalize_plan_type(window.get("plan"))}
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
    compaction = row.get("compaction")
    if isinstance(compaction, dict) and compaction.get("continuousFrom") and isinstance(compaction.get("omittedSamples"), int) and compaction["omittedSamples"] > 0:
        normalized["compaction"] = {"continuousFrom": compaction["continuousFrom"], "omittedSamples": compaction["omittedSamples"]}
    return normalized

def quota_history_row_from_sample(sample: dict) -> dict | None:
    if not isinstance(sample, dict):
        return None
    rejected_windows = sample.get("rejectedWindows") if isinstance(sample.get("rejectedWindows"), dict) else None
    if rejected_windows is None and (sample.get("usingPreviousWindows") or (sample.get("remoteUsage") or {}).get("accepted") is False):
        return None
    return normalize_quota_history_row({
        "checkedAt": sample.get("checkedAt") if rejected_windows is not None else sample.get("percentCheckedAt") or sample.get("checkedAt"),
        "accountSlotId": sample.get("accountSlotId"),
        "accountLabel": sample.get("accountLabel"),
        "windows": rejected_windows if rejected_windows is not None else sample.get("windows") or {},
        "sync": sample.get("sync"),
    })

def load_quota_history(path: Path) -> list[dict]:
    try:
        rows = parse_history_rows(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    return [normalized for row in rows if (normalized := normalize_quota_history_row(row)) is not None]

def same_quota_history_state(left: dict, right: dict) -> bool:
    left_windows, right_windows = left.get("windows") or {}, right.get("windows") or {}
    if set(left_windows) != set(right_windows):
        return False
    for label in left_windows:
        left_window, right_window = left_windows[label] or {}, right_windows[label] or {}
        if coerce_float(left_window.get("usedPercent")) != coerce_float(right_window.get("usedPercent")) or normalize_plan_type(left_window.get("plan")) != normalize_plan_type(right_window.get("plan")):
            return False
        left_reset, right_reset = parse_timestamp(left_window.get("resetAt")), parse_timestamp(right_window.get("resetAt"))
        if left_reset is not None or right_reset is not None:
            if left_reset is None or right_reset is None or abs(left_reset - right_reset) > RESET_TIME_JITTER_SECONDS:
                return False
        elif left_window.get("resetAt") != right_window.get("resetAt"):
            return False
    return True

def quota_history_samples_are_continuous(left: dict, right: dict) -> bool:
    left_at, right_at = parse_timestamp(left.get("checkedAt")), parse_timestamp(right.get("checkedAt"))
    return left_at is not None and right_at is not None and 0 <= right_at - left_at <= QUOTA_HISTORY_DISCONTINUITY_SECONDS

def compact_quota_history_rows(rows: list[dict]) -> list[dict]:
    normalized = [value for row in rows if (value := normalize_quota_history_row(row)) is not None]
    normalized.sort(key=lambda row: (parse_timestamp(row["checkedAt"]) is None, parse_timestamp(row["checkedAt"]) or 0, row["checkedAt"], row["accountSlotId"]))
    by_account = {}
    for row in normalized:
        account_key = (row.get("sync") or {}).get("accountId") or f"local:{row['accountSlotId']}"
        by_account.setdefault(account_key, []).append(row)
    compacted = []
    for account_rows in by_account.values():
        start = 0
        while start < len(account_rows):
            end = start + 1
            while end < len(account_rows) and same_quota_history_state(account_rows[start], account_rows[end]) and quota_history_samples_are_continuous(account_rows[end - 1], account_rows[end]):
                end += 1
            compacted.append(account_rows[start])
            if end - start > 1:
                last = account_rows[end - 1]
                if end - start > 2:
                    last = last | {"compaction": {"continuousFrom": account_rows[start]["checkedAt"], "omittedSamples": end - start - 2}}
                compacted.append(last)
            start = end
    return sorted(compacted, key=lambda row: (parse_timestamp(row["checkedAt"]) is None, parse_timestamp(row["checkedAt"]) or 0, row["checkedAt"], row["accountSlotId"]))

def write_quota_history(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            normalized = [value for row in rows if (value := normalize_quota_history_row(row)) is not None]
            normalized.sort(key=lambda row: (parse_timestamp(row["checkedAt"]) is None, parse_timestamp(row["checkedAt"]) or 0, row["checkedAt"], row["accountSlotId"]))
            for row in normalized:
                f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
                f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise

def merge_quota_history_rows(rows: list[dict]) -> list[dict]:
    merged = {}
    for row in rows:
        normalized = normalize_quota_history_row(row)
        if normalized is None:
            continue
        key = (normalized["checkedAt"], normalized["accountSlotId"])
        if key in merged:
            windows = merged[key]["windows"] | normalized["windows"]
            for label in merged[key]["windows"].keys() & normalized["windows"].keys():
                if normalized["windows"][label]["plan"] == "unknown" and merged[key]["windows"][label]["plan"] != "unknown":
                    windows[label] = normalized["windows"][label] | {"plan": merged[key]["windows"][label]["plan"]}
            normalized = normalized | {"windows": windows}
        merged[key] = normalized
    return sorted(merged.values(), key=lambda row: (parse_timestamp(row["checkedAt"]) is None, parse_timestamp(row["checkedAt"]) or 0, row["accountSlotId"]))

def backfill_quota_history(sample_log_path: Path, quota_history_path: Path) -> int:
    existing = load_quota_history(quota_history_path)
    try:
        sample_rows = parse_history_rows(sample_log_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        sample_rows = []
    imported = [row for item in sample_rows if (row := quota_history_row_from_sample(item.get("sample") if isinstance(item.get("sample"), dict) else item)) is not None]
    merged = merge_quota_history_rows(imported + existing)
    if merged != existing:
        write_quota_history(quota_history_path, merged)
    return len(merged) - len(existing)

def append_quota_history_sample(path: Path, sample: dict) -> bool:
    row = quota_history_row_from_sample(sample)
    if row is None:
        return False
    write_quota_history(path, load_quota_history(path) + [row])
    return True


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
    return isinstance(row, dict) and row.get("window") in TARGET_WINDOWS and isinstance(row.get("delta") if "delta" in row else row.get("events"), list)

def expand_grouped_delta_event_row(row: dict) -> list[dict]:
    result = []
    for item in row.get("delta") if "delta" in row else row.get("events"):
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

def trim_jsonl_to_size(path: Path, max_bytes: int, current_size: int | None = None) -> None:
    if max_bytes <= 0 or current_size is not None and current_size <= max_bytes:
        return
    try:
        current_size = max(current_size or 0, path.stat().st_size)
        if current_size <= max_bytes:
            return
    except FileNotFoundError:
        return
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with path.open("rb") as source, os.fdopen(fd, "wb") as target:
            source.seek(max(0, current_size - int(max_bytes * SAMPLE_LOG_COMPACT_RATIO)))
            source.readline()
            shutil.copyfileobj(source, target, JSONL_COPY_CHUNK_BYTES)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise

def append_capped_jsonl(path: Path, row: dict, max_bytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("ab") as stream:
        stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n")
        current_size = stream.tell()
    trim_jsonl_to_size(path, max_bytes, current_size)

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

def parse_json_sequence(text: str) -> list:
    decoder = json.JSONDecoder()
    values = []
    index = 0
    while index < len(text):
        while index < len(text) and text[index].isspace():
            index += 1
        if index < len(text):
            value, index = decoder.raw_decode(text, index)
            values.append(value)
    return values

def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise

def rewrite_account_labels(paths, account_id: str, label: str):
    originals = {}
    try:
        for path in dict.fromkeys(Path(path) for path in paths):
            try:
                original = path.read_bytes()
            except FileNotFoundError:
                continue
            values = parse_json_sequence(original.decode("utf-8"))
            if not sum(replace_account_label(value, account_id, label) for value in values):
                continue
            originals[path] = original
            _atomic_write_bytes(path, ("\n".join(json.dumps(value, ensure_ascii=False, separators=(",", ":")) for value in values) + "\n").encode("utf-8"))
    except Exception:
        for path, original in originals.items():
            _atomic_write_bytes(path, original)
        raise
    def rollback() -> None:
        for path, original in originals.items():
            _atomic_write_bytes(path, original)
    return rollback

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

def state_path_for(history_path: Path) -> Path:
    if history_path.name == "usage_monitor_history.jsonl":
        return history_path.with_name("usage_monitor_state.json")
    return history_path.with_suffix(".state.json")

def load_state(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if not isinstance(data, dict):
                return {}
            if isinstance((data.get("tokenUsage") or {}).get("totals"), dict):
                data["tokenUsage"]["totals"] = normalize_saved_token_totals(data["tokenUsage"]["totals"])
            if isinstance((data.get("lastSample") or {}).get("tokenDelta"), dict):
                data["lastSample"]["tokenDelta"] = normalize_saved_token_totals(data["lastSample"]["tokenDelta"])
            return data
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
        "costDelta": sample.get("costDelta") or empty_cost_totals(),
        "costByModel": sample.get("costByModel") or {},
        "costDeltaByModel": sample.get("costDeltaByModel") or {},
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
    if "runCostUsd" in state:
        compact["runCostUsd"] = round(state["runCostUsd"], 8)
    if "runCostByModelUsd" in state:
        compact["runCostByModelUsd"] = {model: round(cost, 8) for model, cost in state["runCostByModelUsd"].items()}
    if "measuredCostIntervals" in state:
        compact["measuredCostIntervals"] = state["measuredCostIntervals"]
    if "hasRuntimeCostBaseline" in state:
        compact["hasRuntimeCostBaseline"] = state["hasRuntimeCostBaseline"]
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
    if "runCostUsd" in state:
        compact["runCostUsd"] = round(state["runCostUsd"], 8)
    if "runCostByModelUsd" in state:
        compact["runCostByModelUsd"] = {model: round(cost, 8) for model, cost in state["runCostByModelUsd"].items()}
    if "measuredCostIntervals" in state:
        compact["measuredCostIntervals"] = state["measuredCostIntervals"]
    if "hasRuntimeCostBaseline" in state:
        compact["hasRuntimeCostBaseline"] = state["hasRuntimeCostBaseline"]
    if "remoteUsageIdentity" in state:
        compact["remoteUsageIdentity"] = state["remoteUsageIdentity"]
    return compact

def reset_runtime_baselines(state: dict) -> dict:
    state["runCostUsd"] = 0.0
    state["runCostByModelUsd"] = {}
    state["measuredCostIntervals"] = 0
    state["hasRuntimeCostBaseline"] = False
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
        "errors": {},
    }
    if output.get("accountSlotId"):
        sample["accountSlotId"] = output["accountSlotId"]
        sample["accountLabel"] = output.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL
    if output.get("remoteUsage"):
        sample["remoteUsage"] = output["remoteUsage"]
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

def compact_quota_history(path: Path, retain_days: int | None) -> None:
    rows = load_quota_history(path)
    if retain_days is not None and retain_days > 0:
        cutoff = time.time() - retain_days * 24 * 60 * 60
        rows = [row for row in rows if parse_timestamp(row.get("checkedAt")) is None or parse_timestamp(row.get("checkedAt")) >= cutoff]
    write_quota_history(path, rows)

def collect_usage_sample(args, opener: urllib.request.OpenerDirector | None, previous_token_usage: dict | None, previous_cost: dict | None = None, runtime_state: dict | None = None, skip_remote: bool = False) -> dict:
    output = {}
    if not args.local_only and not skip_remote:
        output["remoteUsage"] = {}
        if opener is None:
            raise UsageError("no HTTP opener is available")
        auth = load_json(args.auth)
        if getattr(args, "account_attribution_callback", None):
            output.update(args.account_attribution_callback(auth))
        output.update(fetch_usage_with_percent_arbitration(
            auth, opener, args.auth, max(args.timeout, 1), output["remoteUsage"], getattr(args, "retry_limit", DEFAULT_RETRY_LIMIT), runtime_state,
            getattr(args, "auth_lock", None), getattr(args, "auth_refreshed_callback", None), False,
        ))
    if not args.no_token_scan:
        output["tokenUsage"] = scan_codex_token_usage(args.codex_home)
    output["checkedAt"] = now_iso()
    return make_history_sample(output, previous_token_usage, previous_cost, previous_cost_by_model=(runtime_state or {}).get("costByModel"))
