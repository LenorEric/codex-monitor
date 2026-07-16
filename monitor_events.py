#!/usr/bin/env python3

from datetime import datetime

from monitor_common import (
    MIN_DELTA_COST_PER_PERCENT_USD, PLAN_MULTIPLIERS, RATIO_DEVIATION_MULTIPLIER, RESET_TIME_JITTER_SECONDS,
    UNKNOWN_EVENT_ACCOUNT_ID, UNKNOWN_EVENT_ACCOUNT_LABEL, coerce_float, parse_timestamp,
)
from monitor_history import compact_debug_state, compact_sample_for_state, compact_token_usage_for_state, is_delta_event_row
from monitor_tokens import normalize_codex_model
from monitor_usage_sync import COST_INTERVAL_TYPE

DEFAULT_EVENT_MODEL = "gpt-5.5"
MAX_MANUAL_RESET_CONFIRMATION_RESPONSES = 5
REQUIRED_MANUAL_RESET_CONFIRMATION_RESPONSES = 3

def reset_moved_forward(previous_reset: str | None, current_reset: str | None) -> bool:
    previous_ts = parse_timestamp(previous_reset)
    current_ts = parse_timestamp(current_reset)
    return previous_ts is not None and current_ts is not None and current_ts - previous_ts > RESET_TIME_JITTER_SECONDS

def reset_moved_backward(previous_reset: str | None, current_reset: str | None) -> bool:
    previous_ts = parse_timestamp(previous_reset)
    current_ts = parse_timestamp(current_reset)
    return previous_ts is not None and current_ts is not None and previous_ts - current_ts > RESET_TIME_JITTER_SECONDS

def reset_same_window(previous_reset: str | None, current_reset: str | None) -> bool:
    previous_ts = parse_timestamp(previous_reset)
    current_ts = parse_timestamp(current_reset)
    return previous_ts is not None and current_ts is not None and abs(previous_ts - current_ts) <= RESET_TIME_JITTER_SECONDS

def clear_backward_reset_candidate(window_state: dict) -> None:
    for key in ("backwardResetCandidateAt", "backwardResetCandidatePercent", "backwardResetCandidateCostUsd"):
        window_state.pop(key, None)

def clear_reset_recovery_state(window_state: dict) -> None:
    for key in ("rollbackBaselinePercent", "rollbackBaselineCostUsd", "rollbackBaselineCostByModelUsd", "rollbackBaselineResetAt"):
        window_state.pop(key, None)
    clear_backward_reset_candidate(window_state)

def checked_before_reset(checked_at: str | None, reset_at: str | None) -> bool:
    checked_ts = parse_timestamp(checked_at)
    reset_ts = parse_timestamp(reset_at)
    return checked_ts is not None and reset_ts is not None and checked_ts < reset_ts - RESET_TIME_JITTER_SECONDS

def remote_usage_identity(sample: dict) -> dict | None:
    raw = ((sample.get("remoteUsage") or {}).get("rawResponse") or {})
    auth_identity = ((sample.get("remoteUsage") or {}).get("authIdentity") or {})
    identity = {key: raw.get(key) if raw.get(key) is not None else auth_identity.get(key) for key in ("user_id", "account_id", "email", "plan_type")}
    return identity if any(value is not None for value in identity.values()) else None

def remote_usage_identity_changed(state: dict, sample: dict) -> bool:
    identity = remote_usage_identity(sample)
    return bool(state.get("remoteUsageIdentity") and identity and identity != state["remoteUsageIdentity"])

def confirmed_manual_reset_windows(sample: dict) -> set[str]:
    confirmation = ((sample.get("remoteUsage") or {}).get("manualResetConfirmation") or {})
    return set(confirmation.get("confirmedWindows") or []) if confirmation.get("confirmed") else set()

def direct_zero_or_missing_manual_reset_windows(state: dict, sample: dict) -> list[str] | None:
    identity = remote_usage_identity(sample)
    if state.get("remoteUsageIdentity") and identity != state["remoteUsageIdentity"]:
        return None
    confirmed = []
    for label in ("5h", "7d"):
        window = (sample.get("windows") or {}).get(label) or {}
        raw_percent = coerce_float(window.get("usedPercent"))
        if raw_percent is None:
            continue
        window_state = (state.get("windows") or {}).get(label) or {}
        if (
            raw_percent != 0
            or window_state.get("baselinePlan") is not None and window.get("plan") != window_state.get("baselinePlan")
            or window_state.get("baselineMultiplier") is not None and window.get("planMultiplier") != window_state.get("baselineMultiplier")
        ):
            return None
        if (coerce_float(window_state.get("baselinePercent")) or 0) > 0:
            confirmed.append(label)
    return confirmed or None

def confirm_direct_zero_or_missing_manual_reset(sample: dict, windows: list[str]) -> None:
    sample.setdefault("remoteUsage", {})["manualResetConfirmation"] = {
        "attempted": False,
        "confirmed": True,
        "directZeroOrMissing": True,
        "requiredConsecutiveResponses": 1,
        "maxResponses": 1,
        "confirmedWindows": windows,
    }

def manual_reset_rejection_windows(state: dict, sample: dict) -> list[str]:
    labels = []
    for label in ("5h", "7d"):
        window = (sample.get("windows") or {}).get(label) or {}
        window_state = (state.get("windows") or {}).get(label) or {}
        raw_percent = coerce_float(window.get("usedPercent"))
        baseline_percent = coerce_float(window_state.get("baselinePercent"))
        if raw_percent is None or baseline_percent is None or not checked_before_reset(sample.get("checkedAt"), window_state.get("baselineResetAt")):
            continue
        normalized = raw_percent * (coerce_float(window.get("planMultiplier")) or PLAN_MULTIPLIERS["unknown"])
        if normalized < baseline_percent and (
            reset_moved_forward(window_state.get("baselineResetAt"), window.get("resetAt"))
            or reset_moved_backward(window_state.get("baselineResetAt"), window.get("resetAt"))
        ):
            labels.append(label)
    return labels

def manual_reset_percent_increase_limit(label: str, plan: str | None) -> float | None:
    if plan == "plus":
        return 5.0 if label == "5h" else 1.0
    if plan in {"pro_lite", "pro"}:
        return 1.0
    return None

def manual_reset_sample_consistent(newer: dict, older: dict, labels: list[str]) -> bool:
    if remote_usage_identity(newer) != remote_usage_identity(older):
        return False
    for label in labels:
        current = (newer.get("windows") or {}).get(label) or {}
        previous = (older.get("windows") or {}).get(label) or {}
        limit = manual_reset_percent_increase_limit(label, current.get("plan"))
        current_percent = coerce_float(current.get("usedPercent"))
        previous_percent = coerce_float(previous.get("usedPercent"))
        if (
            limit is None
            or current.get("plan") != previous.get("plan")
            or current.get("planMultiplier") != previous.get("planMultiplier")
            or not reset_same_window(previous.get("resetAt"), current.get("resetAt"))
        ):
            return False
        if current_percent is None or previous_percent is None or not 0 <= current_percent - previous_percent <= limit:
            return False
    return True

def raw_reset_after_inconsistent(sample: dict, label: str, raw_window: dict) -> str | None:
    reset_at = coerce_float(raw_window.get("reset_at") if "reset_at" in raw_window else raw_window.get("resetAt"))
    reset_after = coerce_float(raw_window.get("reset_after_seconds"))
    if reset_at is None or reset_after is None:
        return None
    completed_ts = parse_timestamp((sample.get("remoteUsage") or {}).get("completedAt")) or parse_timestamp(sample.get("checkedAt"))
    if completed_ts is None:
        return None
    drift = abs((reset_at - completed_ts) - reset_after)
    return None if drift <= 300 else f"{label} reset_after_seconds inconsistent with reset_at by {drift:.0f}s"

def remote_usage_rejection_reasons(state: dict, sample: dict) -> list[str]:
    reasons = []
    if ((sample.get("remoteUsage") or {}).get("manualResetConfirmation") or {}).get("directZeroOrMissing"):
        return reasons
    confirmed_windows = confirmed_manual_reset_windows(sample)
    identity_changed = remote_usage_identity_changed(state, sample)
    if identity_changed and not sample.get("windows"):
        reasons.append("identity changed without usable quota windows")
    if identity_changed and sample.get("windows"):
        return reasons
    if sample.get("remoteUsage") is not None and all(coerce_float(((sample.get("windows") or {}).get(label) or {}).get("usedPercent")) is None for label in ("5h", "7d")):
        reasons.append("both quota window percentages are missing")
    rate_limit = (((sample.get("remoteUsage") or {}).get("rawResponse") or {}).get("rate_limit") or {})
    for label, raw_key in (("5h", "primary_window"), ("7d", "secondary_window")):
        if isinstance(rate_limit.get(raw_key), dict):
            reason = raw_reset_after_inconsistent(sample, label, rate_limit[raw_key])
            if reason:
                reasons.append(reason)
        window = (sample.get("windows") or {}).get(label) or {}
        raw_percent = coerce_float(window.get("usedPercent"))
        window_state = (state.get("windows") or {}).get(label) or {}
        baseline_percent = coerce_float(window_state.get("baselinePercent"))
        if raw_percent is None or baseline_percent is None or not checked_before_reset(sample.get("checkedAt"), window_state.get("baselineResetAt")):
            continue
        normalized = raw_percent * (coerce_float(window.get("planMultiplier")) or PLAN_MULTIPLIERS["unknown"])
        reset_at = window.get("resetAt")
        if label in confirmed_windows:
            continue
        if normalized < baseline_percent and reset_moved_forward(window_state.get("baselineResetAt"), reset_at):
            reasons.append(f"{label} percent rolled back and reset moved forward before trusted reset time")
        elif normalized < baseline_percent and reset_moved_backward(window_state.get("baselineResetAt"), reset_at):
            reasons.append(f"{label} percent rolled back with an older reset time before trusted reset time")
    return reasons

def record_remote_usage_rejection(state: dict, sample: dict, reasons: list[str]) -> None:
    signature = tuple(reasons)
    suppress_console = state.get("_lastRemoteUsageRejectionSignature") == signature
    state["_lastRemoteUsageRejectionSignature"] = signature
    state.setdefault("_specialEvents", []).append({
        "checkedAt": sample["checkedAt"],
        "window": "remote",
        "reason": "bad-remote-usage-discarded",
        "previous": {},
        "current": {},
        "extra": {"reasons": reasons, "suppressConsole": suppress_console},
    })

def collect_with_bad_remote_usage_retry(collector, state: dict) -> dict:
    sample = collector()
    direct_reset_windows = direct_zero_or_missing_manual_reset_windows(state, sample)
    if direct_reset_windows:
        confirm_direct_zero_or_missing_manual_reset(sample, direct_reset_windows)
        return sample
    rejection_reasons = remote_usage_rejection_reasons(state, sample)
    if not rejection_reasons:
        return sample
    initial_rejection_reasons = rejection_reasons
    initial_checked_at = sample.get("checkedAt")
    manual_reset_windows = manual_reset_rejection_windows(state, sample)
    if len(manual_reset_windows) != len(rejection_reasons):
        retry_sample = collector()
        direct_reset_windows = direct_zero_or_missing_manual_reset_windows(state, retry_sample)
        if direct_reset_windows:
            confirm_direct_zero_or_missing_manual_reset(retry_sample, direct_reset_windows)
        if retry_sample.get("remoteUsage") is not None:
            retry_sample["remoteUsage"]["badRemoteUsageRetry"] = {
                "attempted": True,
                "initialCheckedAt": initial_checked_at,
                "initialRejectionReasons": initial_rejection_reasons,
            }
        return retry_sample
    attempts = [{"checkedAt": initial_checked_at, "windows": {label: ((sample.get("windows") or {}).get(label) or {}).get("usedPercent") for label in manual_reset_windows}}]
    consecutive = 1
    previous = sample
    for _ in range(1, MAX_MANUAL_RESET_CONFIRMATION_RESPONSES):
        sample = collector()
        direct_reset_windows = direct_zero_or_missing_manual_reset_windows(state, sample)
        if direct_reset_windows:
            confirm_direct_zero_or_missing_manual_reset(sample, direct_reset_windows)
            return sample
        rejection_reasons = remote_usage_rejection_reasons(state, sample)
        current_windows = manual_reset_rejection_windows(state, sample)
        consistent = current_windows == manual_reset_windows and manual_reset_sample_consistent(sample, previous, manual_reset_windows)
        attempts.append({
            "checkedAt": sample.get("checkedAt"),
            "windows": {label: ((sample.get("windows") or {}).get(label) or {}).get("usedPercent") for label in manual_reset_windows},
            "consistentWithPrevious": consistent,
        })
        if sample.get("remoteUsage") is not None:
            sample["remoteUsage"]["badRemoteUsageRetry"] = {
                "attempted": True,
                "initialCheckedAt": initial_checked_at,
                "initialRejectionReasons": initial_rejection_reasons,
            }
        if not rejection_reasons:
            return sample
        if len(current_windows) != len(rejection_reasons) or set(current_windows) != set(manual_reset_windows):
            return sample
        consecutive = consecutive + 1 if consistent else 1
        previous = sample
        if consecutive >= REQUIRED_MANUAL_RESET_CONFIRMATION_RESPONSES:
            sample.setdefault("remoteUsage", {})["manualResetConfirmation"] = {
                "attempted": True,
                "confirmed": True,
                "requiredConsecutiveResponses": REQUIRED_MANUAL_RESET_CONFIRMATION_RESPONSES,
                "maxResponses": MAX_MANUAL_RESET_CONFIRMATION_RESPONSES,
                "confirmedWindows": manual_reset_windows,
                "attempts": attempts,
            }
            return sample
    if sample.get("remoteUsage") is not None:
        sample["remoteUsage"]["manualResetConfirmation"] = {
            "attempted": True,
            "confirmed": False,
            "requiredConsecutiveResponses": REQUIRED_MANUAL_RESET_CONFIRMATION_RESPONSES,
            "maxResponses": MAX_MANUAL_RESET_CONFIRMATION_RESPONSES,
            "attempts": attempts,
        }
    return sample

def valid_delta_cost_rate(delta_percent: float, delta_cost: float) -> bool:
    return delta_percent > 0 and delta_cost / delta_percent >= MIN_DELTA_COST_PER_PERCENT_USD

def requires_trusted_percent_baseline(label: str, plan: str) -> bool:
    return label == "7d" and plan in {"plus", "pro_lite", "pro"} or label == "5h" and plan in {"pro_lite", "pro"}

def set_percent_baseline(
    window_state: dict, normalized: float, total_cost: float, total_cost_by_model: dict[str, float], reset_at: str | None, plan: str | None = None, multiplier: float | None = None,
    awaiting_trusted: bool = False, observed_at: str | None = None,
) -> None:
    window_state.update({
        "baselinePercent": normalized,
        "baselineCostUsd": total_cost,
        "baselineCostByModelUsd": total_cost_by_model,
        "baselineResetAt": reset_at,
        "previousCostUsd": total_cost,
        "previousCostByModelUsd": total_cost_by_model,
    })
    if observed_at is not None:
        window_state["baselineObservedAt"] = observed_at
    if plan is not None:
        window_state["baselinePlan"] = plan
    if multiplier is not None:
        window_state["baselineMultiplier"] = multiplier
    if awaiting_trusted:
        window_state["awaitingTrustedPercentBaseline"] = True
    else:
        window_state.pop("awaitingTrustedPercentBaseline", None)

def cost_percent_ratio(delta_cost: float, delta_percent: float) -> float | None:
    return None if delta_percent <= 0 else delta_cost / delta_percent

def ratio_deviation(cost_percent: float | None, average_cost_percent: float | None) -> float | None:
    return None if cost_percent is None or average_cost_percent is None or average_cost_percent <= 0 else cost_percent / average_cost_percent

def ratio_deviation_warning(cost_percent: float | None, average_cost_percent: float | None) -> bool:
    deviation = ratio_deviation(cost_percent, average_cost_percent)
    return deviation is not None and (deviation >= RATIO_DEVIATION_MULTIPLIER or deviation <= 1 / RATIO_DEVIATION_MULTIPLIER)

def event_cost_usd(sample: dict) -> float:
    value = coerce_float(sample.get("eventCostUsd"))
    if value is not None:
        return value
    return coerce_float((sample.get("cost") or {}).get("totalCostUsd")) or 0.0

def event_cost_by_model_usd(sample: dict) -> dict[str, float]:
    values = sample.get("eventCostByModelUsd")
    if isinstance(values, dict):
        result = {normalize_codex_model(model): cost for model, value in values.items() if (cost := coerce_float(value)) is not None}
        if result or event_cost_usd(sample) == 0:
            return result
    costs = sample.get("costByModel") or {}
    result = {normalize_codex_model(model): cost for model, values in costs.items() if isinstance(values, dict) and (cost := coerce_float(values.get("totalCostUsd"))) is not None}
    return result or {DEFAULT_EVENT_MODEL: event_cost_usd(sample)}

def event_model(event: dict) -> str:
    model = event.get("model")
    return normalize_codex_model(model) if isinstance(model, str) and model else DEFAULT_EVENT_MODEL

def event_account(event: dict) -> dict:
    return {"accountSlotId": event.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID, "accountLabel": event.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL}

def record_special_event(state: dict, reason: str, sample: dict, label: str, previous: dict, current: dict, extra: dict | None = None) -> None:
    if not (extra or any(previous.get(key) != value for key, value in current.items())):
        return
    state.setdefault("_specialEvents", []).append({
        "checkedAt": sample["checkedAt"],
        "window": label,
        "reason": reason,
        "previous": {key: previous.get(key) for key in ("baselinePercent", "baselineCostUsd", "baselineResetAt", "baselinePlan", "baselineMultiplier")},
        "current": current,
        "extra": extra or {},
    })

def build_delta_event_series(history: list[dict], label: str) -> list[dict]:
    events = []
    baseline_percent = None
    baseline_cost = None
    baseline_reset = None
    baseline_plan = None
    baseline_multiplier = None
    previous_cost = None
    cumulative_percent = 0.0
    cumulative_cost = 0.0
    awaiting_trusted_percent_baseline = False

    for sample in sorted(history, key=lambda item: item.get("checkedAt") or ""):
        checked_at = sample.get("checkedAt")
        if not checked_at:
            continue
        window = (sample.get("windows") or {}).get(label) or {}
        if window.get("unavailable"):
            baseline_percent = None
            baseline_cost = None
            baseline_reset = None
            baseline_plan = None
            baseline_multiplier = None
            previous_cost = None
            awaiting_trusted_percent_baseline = False
            continue
        raw = coerce_float(window.get("usedPercent"))
        if raw is None:
            continue
        plan = window.get("plan") or "unknown"
        multiplier = coerce_float(window.get("planMultiplier")) or PLAN_MULTIPLIERS["unknown"]
        normalized = raw * multiplier
        total_cost = event_cost_usd(sample)
        reset_at = window.get("resetAt")

        if baseline_percent is None:
            baseline_percent = normalized
            baseline_cost = total_cost
            baseline_reset = reset_at
            previous_cost = total_cost
            baseline_plan = plan
            baseline_multiplier = multiplier
            awaiting_trusted_percent_baseline = requires_trusted_percent_baseline(label, plan)
            events.append({
                "checkedAt": checked_at,
                "timestamp": parse_timestamp(checked_at),
                "window": label,
                "model": DEFAULT_EVENT_MODEL,
                **event_account(sample),
                "rawPercent": raw,
                "normalizedPercent": normalized,
                "plan": plan,
                "planMultiplier": multiplier,
                "resetAt": reset_at,
                "deltaPercent": 0.0,
                "deltaCostUsd": 0.0,
                "cumulativePercent": 0.0,
                "cumulativeCostUsd": 0.0,
                "sampleCostUsd": total_cost,
            })
            continue

        if plan != baseline_plan or multiplier != baseline_multiplier:
            baseline_percent = normalized
            baseline_cost = total_cost
            baseline_reset = reset_at
            baseline_plan = plan
            baseline_multiplier = multiplier
            previous_cost = total_cost
            awaiting_trusted_percent_baseline = requires_trusted_percent_baseline(label, plan)
            continue

        if awaiting_trusted_percent_baseline:
            if normalized > baseline_percent:
                awaiting_trusted_percent_baseline = False
            baseline_percent = normalized
            baseline_cost = total_cost
            baseline_reset = reset_at
            previous_cost = total_cost
            continue

        if normalized < baseline_percent:
            baseline_percent = 0.0
            baseline_cost = previous_cost if previous_cost is not None else total_cost
            baseline_reset = reset_at

        if normalized > baseline_percent:
            delta_percent = normalized - baseline_percent
            delta_cost = max(0.0, total_cost - (baseline_cost if baseline_cost is not None else total_cost))
            if valid_delta_cost_rate(delta_percent, delta_cost):
                ratio = cost_percent_ratio(delta_cost, delta_percent)
                average_ratio = cost_percent_ratio(cumulative_cost, cumulative_percent)
                cumulative_percent += delta_percent
                cumulative_cost += delta_cost
                events.append({
                    "checkedAt": checked_at,
                    "timestamp": parse_timestamp(checked_at),
                    "window": label,
                    "model": DEFAULT_EVENT_MODEL,
                    **event_account(sample),
                    "rawPercent": raw,
                    "normalizedPercent": normalized,
                    "plan": plan,
                    "planMultiplier": multiplier,
                    "resetAt": reset_at,
                    "deltaPercent": round(delta_percent, 8),
                    "deltaCostUsd": round(delta_cost, 8),
                    "costPercentRatio": round(ratio, 8),
                    "averageCostPercentRatio": None if average_ratio is None else round(average_ratio, 8),
                    "ratioDeviation": None if ratio_deviation(ratio, average_ratio) is None else round(ratio_deviation(ratio, average_ratio), 8),
                    "ratioDeviationWarning": ratio_deviation_warning(ratio, average_ratio),
                    "cumulativePercent": round(cumulative_percent, 8),
                    "cumulativeCostUsd": round(cumulative_cost, 8),
                    "sampleCostUsd": total_cost,
                })
            else:
                awaiting_trusted_percent_baseline = requires_trusted_percent_baseline(label, plan)
            baseline_percent = normalized
            baseline_cost = total_cost
            baseline_reset = reset_at
        else:
            if raw >= 100:
                baseline_cost = total_cost
            if reset_moved_forward(baseline_reset, reset_at):
                baseline_reset = reset_at

        previous_cost = total_cost

    return events

def derive_history_events(history: list[dict]) -> dict:
    from monitor_usage_sync import aggregate_cost_intervals, is_cost_interval_row
    if any(is_cost_interval_row(row) for row in history):
        return delta_event_log_events([row for row in history if is_delta_event_row(row)] + aggregate_cost_intervals(history))
    if not history or all(is_delta_event_row(row) for row in history):
        return delta_event_log_events(history)
    return {
        "fiveHour": build_delta_event_series(history, "5h"),
        "sevenDay": build_delta_event_series(history, "7d"),
    }

def compact_delta_event(event: dict) -> dict:
    return {
        "checkedAt": event["checkedAt"],
        "window": event["window"],
        "model": event_model(event),
        **event_account(event),
        "deltaPercent": round(event["deltaPercent"], 8),
        "deltaCostUsd": round(event["deltaCostUsd"], 8),
        "costPercentRatio": round(event["costPercentRatio"], 8),
    }

def sample_debug_log_row(sample: dict, events: list[dict], state: dict) -> dict:
    return {
        "checkedAt": sample["checkedAt"],
        "sample": sample,
        "events": [compact_delta_event(event) for event in events],
        "state": compact_debug_state(state),
    }

def baseline_delta_event(label: str) -> dict:
    return {
        "checkedAt": None,
        "timestamp": None,
        "window": label,
        "model": None,
        "deltaPercent": 0.0,
        "deltaCostUsd": 0.0,
        "cumulativePercent": 0.0,
        "cumulativeCostUsd": 0.0,
        "synthetic": True,
    }

def delta_event_log_events(rows: list[dict]) -> dict:
    result = {
        "fiveHour": [baseline_delta_event("5h")],
        "sevenDay": [baseline_delta_event("7d")],
    }
    cumulative = {"5h": {"percent": 0.0, "cost": 0.0}, "7d": {"percent": 0.0, "cost": 0.0}}
    model_cumulative = {"5h": {}, "7d": {}}
    for row in sorted(rows, key=lambda item: (item["checkedAt"] or "", item["window"])):
        label = row["window"]
        key = {"5h": "fiveHour", "7d": "sevenDay"}[label]
        delta_percent = row["deltaPercent"]
        delta_cost = row["deltaCostUsd"]
        model = event_model(row)
        if not valid_delta_cost_rate(delta_percent, delta_cost):
            continue
        ratio = cost_percent_ratio(delta_cost, delta_percent)
        model_totals = model_cumulative[label].setdefault(model, {"percent": 0.0, "cost": 0.0})
        average_ratio = cost_percent_ratio(model_totals["cost"], model_totals["percent"])
        cumulative[label]["percent"] += delta_percent
        cumulative[label]["cost"] += delta_cost
        model_totals["percent"] += delta_percent
        model_totals["cost"] += delta_cost
        result[key].append({
            "checkedAt": row["checkedAt"],
            "timestamp": parse_timestamp(row["checkedAt"]),
            "window": label,
            "model": model,
            **event_account(row),
            "deltaPercent": round(delta_percent, 8),
            "deltaCostUsd": round(delta_cost, 8),
            "costPercentRatio": round(ratio, 8),
            "averageCostPercentRatio": None if average_ratio is None else round(average_ratio, 8),
            "ratioDeviation": None if ratio_deviation(ratio, average_ratio) is None else round(ratio_deviation(ratio, average_ratio), 8),
            "ratioDeviationWarning": ratio_deviation_warning(ratio, average_ratio),
            "cumulativePercent": round(cumulative[label]["percent"], 8),
            "cumulativeCostUsd": round(cumulative[label]["cost"], 8),
            "modelCumulativePercent": round(model_totals["percent"], 8),
            "modelCumulativeCostUsd": round(model_totals["cost"], 8),
        })
    return result

def hydrate_delta_state_from_events(state: dict, rows: list[dict]) -> None:
    for label in ("5h", "7d"):
        state.setdefault("windows", {}).setdefault(label, {})
    events_by_window = derive_history_events(rows)
    for label, key in (("5h", "fiveHour"), ("7d", "sevenDay")):
        event = events_by_window[key][-1]
        state["windows"][label]["cumulativePercent"] = event["cumulativePercent"]
        state["windows"][label]["cumulativeCostUsd"] = event["cumulativeCostUsd"]
        state["windows"][label]["models"] = {}
        for model_event in events_by_window[key]:
            if model_event.get("model"):
                state["windows"][label]["models"][model_event["model"]] = {
                    "cumulativePercent": model_event["modelCumulativePercent"],
                    "cumulativeCostUsd": model_event["modelCumulativeCostUsd"],
                }

def allocate_model_delta_events(common: dict, delta_percent: float, delta_cost: float, total_cost_by_model: dict[str, float], window_state: dict) -> list[dict]:
    baseline = window_state.get("baselineCostByModelUsd") or {}
    costs = {model: max(0.0, cost - baseline.get(model, 0.0)) for model, cost in total_cost_by_model.items()}
    costs = {model: cost for model, cost in costs.items() if cost > 0}
    attributed_cost = sum(costs.values())
    if attributed_cost < delta_cost - 0.00000001:
        costs["unknown"] = costs.get("unknown", 0.0) + delta_cost - attributed_cost
    elif attributed_cost > delta_cost:
        costs = {model: cost * delta_cost / attributed_cost for model, cost in costs.items()}
    elif not costs:
        costs[DEFAULT_EVENT_MODEL] = delta_cost
    models = sorted(costs)
    events = []
    used_percent = 0.0
    used_cost = 0.0
    for index, model in enumerate(models):
        model_cost = delta_cost - used_cost if index == len(models) - 1 else min(costs[model], delta_cost - used_cost)
        model_percent = delta_percent - used_percent if index == len(models) - 1 else delta_percent * model_cost / delta_cost
        model_state = window_state.setdefault("models", {}).setdefault(model, {"cumulativePercent": 0.0, "cumulativeCostUsd": 0.0})
        ratio = cost_percent_ratio(model_cost, model_percent)
        average_ratio = cost_percent_ratio(model_state["cumulativeCostUsd"], model_state["cumulativePercent"])
        model_state["cumulativePercent"] = round(model_state["cumulativePercent"] + model_percent, 8)
        model_state["cumulativeCostUsd"] = round(model_state["cumulativeCostUsd"] + model_cost, 8)
        events.append(common | {
            "model": model,
            "deltaPercent": round(model_percent, 8),
            "deltaCostUsd": round(model_cost, 8),
            "costPercentRatio": round(ratio, 8),
            "averageCostPercentRatio": None if average_ratio is None else round(average_ratio, 8),
            "ratioDeviation": None if ratio_deviation(ratio, average_ratio) is None else round(ratio_deviation(ratio, average_ratio), 8),
            "ratioDeviationWarning": ratio_deviation_warning(ratio, average_ratio),
            "modelCumulativePercent": model_state["cumulativePercent"],
            "modelCumulativeCostUsd": model_state["cumulativeCostUsd"],
        })
        used_percent += model_percent
        used_cost += model_cost
    return events

def cost_interval_from_sample(sample: dict, label: str, normalized: float, delta_cost: float, total_cost_by_model: dict[str, float], window_state: dict, plan: str, multiplier: float, reset_at: str | None) -> dict | None:
    start = coerce_float(window_state.get("baselinePercent"))
    if start is None or normalized <= start or delta_cost <= 0:
        return None
    baseline = window_state.get("baselineCostByModelUsd") or {}
    costs = {normalize_codex_model(model): max(0.0, cost - baseline.get(model, 0.0)) for model, cost in total_cost_by_model.items()}
    costs = {model: cost for model, cost in costs.items() if cost > 0}
    attributed = sum(costs.values())
    if attributed < delta_cost - 0.00000001:
        costs["unknown"] = costs.get("unknown", 0.0) + delta_cost - attributed
    elif attributed > delta_cost:
        costs = {model: cost * delta_cost / attributed for model, cost in costs.items()}
    return {
        "recordType": COST_INTERVAL_TYPE, "startedAt": window_state.get("baselineObservedAt"), "checkedAt": sample["checkedAt"], "window": label,
        "accountSlotId": sample.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID, "accountLabel": sample.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL,
        "startPercent": round(start, 8), "endPercent": round(normalized, 8), "plan": plan, "planMultiplier": multiplier, "resetAt": reset_at,
        "modelCostsUsd": {model: round(cost, 8) for model, cost in sorted(costs.items())}, "deltaCostUsd": round(delta_cost, 8),
        "sync": {"version": 1, "originMachineId": sample.get("originMachineId"), "accountId": sample.get("usageAccountId"), "recordId": sample.get("usageRecordId")},
    }

def build_delta_event_from_sample(state: dict, sample: dict, label: str) -> list[dict]:
    window = (sample.get("windows") or {}).get(label) or {}
    if window.get("unavailable"):
        state["windows"].pop(label, None)
        state["windows"][label] = {}
        return []
    raw = coerce_float(window.get("usedPercent"))
    if raw is None:
        return []
    plan = window["plan"]
    multiplier = window["planMultiplier"]
    normalized = raw * multiplier
    total_cost = event_cost_usd(sample)
    total_cost_by_model = event_cost_by_model_usd(sample)
    reset_at = window.get("resetAt")
    window_state = state["windows"][label]

    if sample.get("eventCostReady") is False:
        record_special_event(state, "runtime-cost-baseline-pending", sample, label, window_state, {
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
            "baselinePlan": plan,
            "baselineMultiplier": multiplier,
        })
        set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, plan, multiplier, requires_trusted_percent_baseline(label, plan), sample["checkedAt"])
        clear_reset_recovery_state(window_state)
        return []

    if window_state.get("baselinePercent") is None:
        record_special_event(state, "baseline-initialized", sample, label, window_state, {
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
            "baselinePlan": plan,
            "baselineMultiplier": multiplier,
        })
        set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, plan, multiplier, requires_trusted_percent_baseline(label, plan), sample["checkedAt"])
        clear_reset_recovery_state(window_state)
        return []

    identity_changed = remote_usage_identity_changed(state, sample)
    if identity_changed or plan != window_state["baselinePlan"] or multiplier != window_state["baselineMultiplier"]:
        record_special_event(state, "account-switch", sample, label, window_state, {
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
            "baselinePlan": plan,
            "baselineMultiplier": multiplier,
        }, {"previousIdentity": state.get("remoteUsageIdentity"), "currentIdentity": remote_usage_identity(sample)} if identity_changed else None)
        set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, plan, multiplier, requires_trusted_percent_baseline(label, plan), sample["checkedAt"])
        clear_reset_recovery_state(window_state)
        return []

    if label in confirmed_manual_reset_windows(sample):
        record_special_event(state, "manual-reset-confirmed", sample, label, window_state, {
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
            "baselinePlan": plan,
            "baselineMultiplier": multiplier,
        })
        set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, plan, multiplier, observed_at=sample["checkedAt"])
        clear_reset_recovery_state(window_state)
        return []

    if window_state.get("awaitingTrustedPercentBaseline"):
        if normalized > window_state["baselinePercent"]:
            record_special_event(state, "trusted-percent-baseline-established", sample, label, window_state, {
                "baselinePercent": normalized,
                "baselineCostUsd": total_cost,
                "baselineResetAt": reset_at,
                "baselinePlan": plan,
                "baselineMultiplier": multiplier,
            })
            set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, plan, multiplier, observed_at=sample["checkedAt"])
            clear_reset_recovery_state(window_state)
        elif normalized < window_state["baselinePercent"] or reset_moved_forward(window_state.get("baselineResetAt"), reset_at) or reset_moved_backward(window_state.get("baselineResetAt"), reset_at):
            set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, plan, multiplier, True, sample["checkedAt"])
            clear_reset_recovery_state(window_state)
        else:
            window_state["previousCostUsd"] = total_cost
            window_state["previousCostByModelUsd"] = total_cost_by_model
        return []

    if reset_moved_backward(window_state.get("baselineResetAt"), reset_at):
        if reset_same_window(window_state.get("rollbackBaselineResetAt"), reset_at):
            record_special_event(state, "transient-reset-rollback", sample, label, window_state, {
                "baselinePercent": window_state["rollbackBaselinePercent"],
                "baselineCostUsd": window_state["rollbackBaselineCostUsd"],
                "baselineResetAt": reset_at,
            })
            window_state["baselinePercent"] = window_state.pop("rollbackBaselinePercent")
            window_state["baselineCostUsd"] = window_state.pop("rollbackBaselineCostUsd")
            window_state["baselineCostByModelUsd"] = window_state.pop("rollbackBaselineCostByModelUsd")
            window_state["baselineResetAt"] = window_state.pop("rollbackBaselineResetAt")
            clear_backward_reset_candidate(window_state)
        elif reset_same_window(window_state.get("backwardResetCandidateAt"), reset_at):
            record_special_event(state, "consistent-backward-reset-rebased", sample, label, window_state, {
                "baselinePercent": normalized,
                "baselineCostUsd": total_cost,
                "baselineResetAt": reset_at,
            }, {
                "discardedPercent": max(0.0, normalized - (coerce_float(window_state.get("backwardResetCandidatePercent")) or normalized)),
                "discardedCostUsd": max(0.0, total_cost - (coerce_float(window_state.get("backwardResetCandidateCostUsd")) or total_cost)),
            })
            set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, awaiting_trusted=requires_trusted_percent_baseline(label, plan), observed_at=sample["checkedAt"])
            clear_reset_recovery_state(window_state)
            return []
        else:
            record_special_event(state, "reset-time-moved-backward-discarded", sample, label, window_state, {
                "baselinePercent": window_state["baselinePercent"],
                "baselineCostUsd": window_state["baselineCostUsd"],
                "baselineResetAt": window_state.get("baselineResetAt"),
            }, {"observedPercent": normalized, "observedResetAt": reset_at})
            window_state["backwardResetCandidateAt"] = reset_at
            window_state["backwardResetCandidatePercent"] = normalized
            window_state["backwardResetCandidateCostUsd"] = total_cost
            return []
    else:
        clear_backward_reset_candidate(window_state)

    if normalized < window_state["baselinePercent"]:
        if reset_moved_forward(window_state.get("baselineResetAt"), reset_at):
            window_state["rollbackBaselinePercent"] = window_state["baselinePercent"]
            window_state["rollbackBaselineCostUsd"] = window_state["baselineCostUsd"]
            window_state["rollbackBaselineCostByModelUsd"] = window_state.get("baselineCostByModelUsd") or {}
            window_state["rollbackBaselineResetAt"] = window_state.get("baselineResetAt")
        record_special_event(state, "usage-percent-rollback", sample, label, window_state, {
            "baselinePercent": normalized if requires_trusted_percent_baseline(label, plan) else 0.0,
            "baselineCostUsd": total_cost if requires_trusted_percent_baseline(label, plan) else window_state["previousCostUsd"],
            "baselineResetAt": reset_at,
        })
        if requires_trusted_percent_baseline(label, plan):
            set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, awaiting_trusted=True, observed_at=sample["checkedAt"])
            clear_reset_recovery_state(window_state)
            return []
        window_state["baselinePercent"] = 0.0
        window_state["baselineCostUsd"] = window_state["previousCostUsd"]
        window_state["baselineCostByModelUsd"] = window_state.get("previousCostByModelUsd") or {}
        window_state["baselineResetAt"] = reset_at
        if reset_moved_forward(window_state.get("rollbackBaselineResetAt"), reset_at):
            window_state["previousCostUsd"] = total_cost
            window_state["previousCostByModelUsd"] = total_cost_by_model
            return []

    if normalized <= window_state["baselinePercent"]:
        if reset_moved_forward(window_state["baselineResetAt"], reset_at):
            record_special_event(state, "reset-time-moved-forward", sample, label, window_state, {"baselineResetAt": reset_at})
            window_state["baselineResetAt"] = reset_at
        if raw >= 100:
            window_state["baselineCostUsd"] = total_cost
            window_state["baselineCostByModelUsd"] = total_cost_by_model
        window_state["previousCostUsd"] = total_cost
        window_state["previousCostByModelUsd"] = total_cost_by_model
        return []

    delta_percent = normalized - window_state["baselinePercent"]
    delta_cost = max(0.0, total_cost - window_state["baselineCostUsd"])
    if interval := cost_interval_from_sample(sample, label, normalized, delta_cost, total_cost_by_model, window_state, plan, multiplier, reset_at):
        state.setdefault("_pendingCostIntervals", []).append(interval)
    if not valid_delta_cost_rate(delta_percent, delta_cost):
        record_special_event(state, "low-cost-delta-discarded", sample, label, window_state, {
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
        }, {"deltaPercent": delta_percent, "deltaCostUsd": delta_cost})
        set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, awaiting_trusted=requires_trusted_percent_baseline(label, plan), observed_at=sample["checkedAt"])
        clear_reset_recovery_state(window_state)
        return []
    window_state["cumulativePercent"] = round(window_state["cumulativePercent"] + delta_percent, 8)
    window_state["cumulativeCostUsd"] = round(window_state["cumulativeCostUsd"] + delta_cost, 8)
    common = {
        "checkedAt": sample["checkedAt"],
        "timestamp": parse_timestamp(sample["checkedAt"]),
        "window": label,
        "accountSlotId": sample.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID,
        "accountLabel": sample.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL,
        "rawPercent": raw,
        "normalizedPercent": normalized,
        "plan": plan,
        "planMultiplier": multiplier,
        "resetAt": reset_at,
        "cumulativePercent": window_state["cumulativePercent"],
        "cumulativeCostUsd": window_state["cumulativeCostUsd"],
        "sampleCostUsd": total_cost,
    }
    events = allocate_model_delta_events(common, delta_percent, delta_cost, total_cost_by_model, window_state)
    window_state.update({
        "baselinePercent": normalized,
        "baselineCostUsd": total_cost,
        "baselineCostByModelUsd": total_cost_by_model,
        "baselineResetAt": reset_at,
        "previousCostUsd": total_cost,
        "previousCostByModelUsd": total_cost_by_model,
        "baselineObservedAt": sample["checkedAt"],
    })
    clear_reset_recovery_state(window_state)
    return events

def process_sample_delta_events(state: dict, sample: dict, existing_events: list[dict]) -> list[dict]:
    state["_specialEvents"] = []
    state["_pendingCostIntervals"] = []
    hydrate_delta_state_from_events(state, existing_events)
    rejection_reasons = remote_usage_rejection_reasons(state, sample)
    if rejection_reasons:
        sample.setdefault("errors", {})["remoteUsageRejected"] = "; ".join(rejection_reasons)
        if sample.get("remoteUsage"):
            sample["remoteUsage"]["accepted"] = False
            sample["remoteUsage"]["rejectionReasons"] = rejection_reasons
        sample["percentCheckedAt"] = ((state.get("lastSample") or {}).get("percentCheckedAt") or (state.get("lastSample") or {}).get("checkedAt"))
        sample["rejectedWindows"] = sample.get("windows") or {}
        sample["windows"] = ((state.get("lastSample") or {}).get("windows") or {})
        sample["usingPreviousWindows"] = bool(sample["windows"])
        record_remote_usage_rejection(state, sample, rejection_reasons)
        state["tokenUsage"] = compact_token_usage_for_state(sample.get("tokenUsage"))
        state["cost"] = sample.get("cost")
        state["costByModel"] = sample.get("costByModel") or {}
        state["lastSample"] = compact_sample_for_state(sample)
        state["updatedAt"] = sample["checkedAt"]
        return []
    if sample.get("remoteUsage"):
        sample["remoteUsage"]["accepted"] = True
    sample["percentCheckedAt"] = sample["checkedAt"]
    events = build_delta_event_from_sample(state, sample, "5h") + build_delta_event_from_sample(state, sample, "7d")
    identity = remote_usage_identity(sample)
    if identity:
        state["remoteUsageIdentity"] = identity
    state["tokenUsage"] = compact_token_usage_for_state(sample.get("tokenUsage"))
    state["cost"] = sample.get("cost")
    state["costByModel"] = sample.get("costByModel") or {}
    state["lastSample"] = compact_sample_for_state(sample)
    state["updatedAt"] = sample["checkedAt"]
    return events

def event_identity(event: dict) -> tuple:
    return (
        event["window"],
        event_model(event),
        event_account(event)["accountSlotId"],
        event["checkedAt"],
        round(event["deltaPercent"], 8),
        round(event["deltaCostUsd"], 8),
        round(event.get("costPercentRatio") or cost_percent_ratio(event["deltaCostUsd"], event["deltaPercent"]) or 0, 8),
        event.get("plan"),
        coerce_float(event.get("planMultiplier")) or 1.0,
    )

def valid_delta_events(events: dict) -> list[dict]:
    rows = []
    for label in ("fiveHour", "sevenDay"):
        for event in events[label]:
            if event["deltaPercent"] > 0:
                rows.append(event)
    return sorted(rows, key=lambda item: (item["checkedAt"] or "", item["window"]))

def new_valid_delta_events(before: list[dict], after: list[dict]) -> list[dict]:
    seen = {event_identity(event) for event in valid_delta_events(derive_history_events(before))}
    return [event for event in valid_delta_events(derive_history_events(after)) if event_identity(event) not in seen]

def normalized_window_status(sample: dict, label: str) -> dict:
    window = (sample.get("windows") or {}).get(label) or {}
    raw = coerce_float(window.get("usedPercent"))
    multiplier = coerce_float(window.get("planMultiplier")) or PLAN_MULTIPLIERS["unknown"]
    return {
        "rawPercent": raw,
        "normalizedPercent": None if raw is None else raw * multiplier,
        "plan": window.get("plan") or "unknown",
        "planMultiplier": multiplier,
    }

def format_console_timestamp(value: str | None) -> str:
    if not value:
        return "-"
    try:
        return f"{value} (local {datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone().isoformat(timespec='seconds')})"
    except ValueError:
        return value

def format_valid_delta_event(event: dict, sample: dict | None = None) -> str:
    sample = sample or {}
    five = normalized_window_status(sample, "5h")
    seven = normalized_window_status(sample, "7d")
    return (
        f"[delta] {format_console_timestamp(event['checkedAt'])} {event['window']} {event_model(event)} "
        f"+{event['deltaPercent']:.4g}% / +${event['deltaCostUsd']:.6g}; "
        f"ratio ${event.get('costPercentRatio', cost_percent_ratio(event['deltaCostUsd'], event['deltaPercent'])):.6g}/%; "
        f"cumulative {event['cumulativePercent']:.4g}% / ${event['cumulativeCostUsd']:.6g}; "
        f"event plan {event.get('plan') or 'unknown'} ({coerce_float(event.get('planMultiplier')) or 1.0:g}x); "
        f"current 5h {format_percent(five['normalizedPercent'])} [{five['plan']} {five['planMultiplier']:g}x], "
        f"7d {format_percent(seven['normalizedPercent'])} [{seven['plan']} {seven['planMultiplier']:g}x]"
    )

def format_ratio_warning(event: dict) -> str:
    return (
        f"[warning] {format_console_timestamp(event['checkedAt'])} {event['window']} {event_model(event)} cost/percent ratio "
        f"${event['costPercentRatio']:.6g}/% deviates from average ${event['averageCostPercentRatio']:.6g}/% "
        f"({event['ratioDeviation']:.6g}x)"
    )

def format_special_event(event: dict) -> str:
    previous = event.get("previous") or {}
    current = event.get("current") or {}
    extra = event.get("extra") or {}
    reason = event.get("reason") or "special-event"
    label = event.get("window") or "?"
    previous_percent = previous.get("baselinePercent")
    current_percent = current.get("baselinePercent", previous_percent)
    previous_cost = coerce_float(previous.get("baselineCostUsd")) or 0
    current_cost = coerce_float(current.get("baselineCostUsd"))
    old_plan = previous.get("baselinePlan") or "unknown"
    new_plan = current.get("baselinePlan") or old_plan
    parts = [
        f"[event] {format_console_timestamp(event.get('checkedAt'))} {label} {reason};",
        f"baseline {format_percent(previous_percent)} -> {format_percent(current_percent)},",
        f"cost ${previous_cost:g} -> ${(current_cost if current_cost is not None else previous_cost):g},",
        f"reset {previous.get('baselineResetAt') or '-'} -> {current.get('baselineResetAt') or previous.get('baselineResetAt') or '-'},",
        f"plan {old_plan} ({coerce_float(previous.get('baselineMultiplier')) or 1:g}x) -> {new_plan} ({coerce_float(current.get('baselineMultiplier')) or coerce_float(previous.get('baselineMultiplier')) or 1:g}x)",
    ]
    if "deltaPercent" in extra or "deltaCostUsd" in extra:
        parts.append(f"discarded delta +{coerce_float(extra.get('deltaPercent')) or 0:.4g}% / +${coerce_float(extra.get('deltaCostUsd')) or 0:.6g}")
    if extra.get("reasons"):
        parts.append("reasons " + "; ".join(str(reason) for reason in extra["reasons"]))
    return " ".join(parts)

def format_percent(value: float | None) -> str:
    return "-" if value is None else f"{value:.4g}%"

def print_valid_delta_events(events: list[dict], sample: dict | None = None) -> None:
    for event in events:
        print(format_valid_delta_event(event, sample), flush=True)

def print_ratio_warnings(events: list[dict]) -> None:
    for event in events:
        if event.get("ratioDeviationWarning"):
            print(format_ratio_warning(event), flush=True)

def print_special_events(events: list[dict]) -> None:
    for event in events:
        if (event.get("extra") or {}).get("suppressConsole"):
            continue
        print(format_special_event(event), flush=True)
