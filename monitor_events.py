#!/usr/bin/env python3

from datetime import datetime

from monitor_common import MIN_DELTA_COST_PER_PERCENT_USD, PLAN_MULTIPLIERS, RATIO_DEVIATION_MULTIPLIER, RESET_TIME_JITTER_SECONDS, coerce_float, parse_timestamp
from monitor_history import compact_debug_state, compact_sample_for_state, compact_token_usage_for_state, is_delta_event_row

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
    for key in ("rollbackBaselinePercent", "rollbackBaselineCostUsd", "rollbackBaselineResetAt"):
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
    identity_changed = remote_usage_identity_changed(state, sample)
    if identity_changed and not sample.get("windows"):
        reasons.append("identity changed without usable quota windows")
    if identity_changed and sample.get("windows"):
        return reasons
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

def valid_delta_cost_rate(delta_percent: float, delta_cost: float) -> bool:
    return delta_percent > 0 and delta_cost / delta_percent >= MIN_DELTA_COST_PER_PERCENT_USD

def percent_cost_ratio(delta_percent: float, delta_cost: float) -> float | None:
    return None if delta_cost <= 0 else delta_percent / delta_cost

def ratio_deviation(percent_cost: float | None, average_percent_cost: float | None) -> float | None:
    return None if percent_cost is None or average_percent_cost is None or average_percent_cost <= 0 else percent_cost / average_percent_cost

def ratio_deviation_warning(percent_cost: float | None, average_percent_cost: float | None) -> bool:
    deviation = ratio_deviation(percent_cost, average_percent_cost)
    return deviation is not None and (deviation >= RATIO_DEVIATION_MULTIPLIER or deviation <= 1 / RATIO_DEVIATION_MULTIPLIER)

def event_cost_usd(sample: dict) -> float:
    value = coerce_float(sample.get("eventCostUsd"))
    if value is not None:
        return value
    return coerce_float((sample.get("cost") or {}).get("totalCostUsd")) or 0.0

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

    for sample in sorted(history, key=lambda item: item.get("checkedAt") or ""):
        checked_at = sample.get("checkedAt")
        if not checked_at:
            continue
        window = (sample.get("windows") or {}).get(label) or {}
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
            events.append({
                "checkedAt": checked_at,
                "timestamp": parse_timestamp(checked_at),
                "window": label,
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
            continue

        if normalized < baseline_percent:
            baseline_percent = 0.0
            baseline_cost = previous_cost if previous_cost is not None else total_cost
            baseline_reset = reset_at

        if normalized > baseline_percent:
            delta_percent = normalized - baseline_percent
            delta_cost = max(0.0, total_cost - (baseline_cost if baseline_cost is not None else total_cost))
            if valid_delta_cost_rate(delta_percent, delta_cost):
                ratio = percent_cost_ratio(delta_percent, delta_cost)
                average_ratio = percent_cost_ratio(cumulative_percent, cumulative_cost)
                cumulative_percent += delta_percent
                cumulative_cost += delta_cost
                events.append({
                    "checkedAt": checked_at,
                    "timestamp": parse_timestamp(checked_at),
                    "window": label,
                    "rawPercent": raw,
                    "normalizedPercent": normalized,
                    "plan": plan,
                    "planMultiplier": multiplier,
                    "resetAt": reset_at,
                    "deltaPercent": round(delta_percent, 8),
                    "deltaCostUsd": round(delta_cost, 8),
                    "percentCostRatio": round(ratio, 8),
                    "averagePercentCostRatio": None if average_ratio is None else round(average_ratio, 8),
                    "ratioDeviation": None if ratio_deviation(ratio, average_ratio) is None else round(ratio_deviation(ratio, average_ratio), 8),
                    "ratioDeviationWarning": ratio_deviation_warning(ratio, average_ratio),
                    "cumulativePercent": round(cumulative_percent, 8),
                    "cumulativeCostUsd": round(cumulative_cost, 8),
                    "sampleCostUsd": total_cost,
                })
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
        "deltaPercent": round(event["deltaPercent"], 8),
        "deltaCostUsd": round(event["deltaCostUsd"], 8),
        "percentCostRatio": round(event["percentCostRatio"], 8),
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
    for row in sorted(rows, key=lambda item: (item["checkedAt"] or "", item["window"])):
        label = row["window"]
        key = {"5h": "fiveHour", "7d": "sevenDay"}[label]
        delta_percent = row["deltaPercent"]
        delta_cost = row["deltaCostUsd"]
        if not valid_delta_cost_rate(delta_percent, delta_cost):
            continue
        ratio = percent_cost_ratio(delta_percent, delta_cost)
        average_ratio = percent_cost_ratio(cumulative[label]["percent"], cumulative[label]["cost"])
        cumulative[label]["percent"] += delta_percent
        cumulative[label]["cost"] += delta_cost
        result[key].append({
            "checkedAt": row["checkedAt"],
            "timestamp": parse_timestamp(row["checkedAt"]),
            "window": label,
            "deltaPercent": round(delta_percent, 8),
            "deltaCostUsd": round(delta_cost, 8),
            "percentCostRatio": round(ratio, 8),
            "averagePercentCostRatio": None if average_ratio is None else round(average_ratio, 8),
            "ratioDeviation": None if ratio_deviation(ratio, average_ratio) is None else round(ratio_deviation(ratio, average_ratio), 8),
            "ratioDeviationWarning": ratio_deviation_warning(ratio, average_ratio),
            "cumulativePercent": round(cumulative[label]["percent"], 8),
            "cumulativeCostUsd": round(cumulative[label]["cost"], 8),
        })
    return result

def hydrate_delta_state_from_events(state: dict, rows: list[dict]) -> None:
    for label in ("5h", "7d"):
        state.setdefault("windows", {}).setdefault(label, {})
    events_by_window = delta_event_log_events(rows)
    for label, key in (("5h", "fiveHour"), ("7d", "sevenDay")):
        event = events_by_window[key][-1]
        state["windows"][label]["cumulativePercent"] = event["cumulativePercent"]
        state["windows"][label]["cumulativeCostUsd"] = event["cumulativeCostUsd"]

def build_delta_event_from_sample(state: dict, sample: dict, label: str) -> dict | None:
    window = (sample.get("windows") or {}).get(label) or {}
    raw = coerce_float(window.get("usedPercent"))
    if raw is None:
        return None
    plan = window["plan"]
    multiplier = window["planMultiplier"]
    normalized = raw * multiplier
    total_cost = event_cost_usd(sample)
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
        window_state.update({
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
            "baselinePlan": plan,
            "baselineMultiplier": multiplier,
            "previousCostUsd": total_cost,
        })
        clear_reset_recovery_state(window_state)
        return None

    if window_state.get("baselinePercent") is None:
        record_special_event(state, "baseline-initialized", sample, label, window_state, {
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
            "baselinePlan": plan,
            "baselineMultiplier": multiplier,
        })
        window_state.update({
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
            "baselinePlan": plan,
            "baselineMultiplier": multiplier,
            "previousCostUsd": total_cost,
        })
        clear_reset_recovery_state(window_state)
        return None

    identity_changed = remote_usage_identity_changed(state, sample)
    if identity_changed or plan != window_state["baselinePlan"] or multiplier != window_state["baselineMultiplier"]:
        record_special_event(state, "account-switch", sample, label, window_state, {
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
            "baselinePlan": plan,
            "baselineMultiplier": multiplier,
        }, {"previousIdentity": state.get("remoteUsageIdentity"), "currentIdentity": remote_usage_identity(sample)} if identity_changed else None)
        window_state.update({
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
            "baselinePlan": plan,
            "baselineMultiplier": multiplier,
            "previousCostUsd": total_cost,
        })
        clear_reset_recovery_state(window_state)
        return None

    if reset_moved_backward(window_state.get("baselineResetAt"), reset_at):
        if reset_same_window(window_state.get("rollbackBaselineResetAt"), reset_at):
            record_special_event(state, "transient-reset-rollback", sample, label, window_state, {
                "baselinePercent": window_state["rollbackBaselinePercent"],
                "baselineCostUsd": window_state["rollbackBaselineCostUsd"],
                "baselineResetAt": reset_at,
            })
            window_state["baselinePercent"] = window_state.pop("rollbackBaselinePercent")
            window_state["baselineCostUsd"] = window_state.pop("rollbackBaselineCostUsd")
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
            window_state.update({
                "baselinePercent": normalized,
                "baselineCostUsd": total_cost,
                "baselineResetAt": reset_at,
                "previousCostUsd": total_cost,
            })
            clear_reset_recovery_state(window_state)
            return None
        else:
            record_special_event(state, "reset-time-moved-backward-discarded", sample, label, window_state, {
                "baselinePercent": window_state["baselinePercent"],
                "baselineCostUsd": window_state["baselineCostUsd"],
                "baselineResetAt": window_state.get("baselineResetAt"),
            }, {"observedPercent": normalized, "observedResetAt": reset_at})
            window_state["backwardResetCandidateAt"] = reset_at
            window_state["backwardResetCandidatePercent"] = normalized
            window_state["backwardResetCandidateCostUsd"] = total_cost
            return None
    else:
        clear_backward_reset_candidate(window_state)

    if normalized < window_state["baselinePercent"]:
        if reset_moved_forward(window_state.get("baselineResetAt"), reset_at):
            window_state["rollbackBaselinePercent"] = window_state["baselinePercent"]
            window_state["rollbackBaselineCostUsd"] = window_state["baselineCostUsd"]
            window_state["rollbackBaselineResetAt"] = window_state.get("baselineResetAt")
        record_special_event(state, "usage-percent-rollback", sample, label, window_state, {
            "baselinePercent": 0.0,
            "baselineCostUsd": window_state["previousCostUsd"],
            "baselineResetAt": reset_at,
        })
        window_state["baselinePercent"] = 0.0
        window_state["baselineCostUsd"] = window_state["previousCostUsd"]
        window_state["baselineResetAt"] = reset_at
        if reset_moved_forward(window_state.get("rollbackBaselineResetAt"), reset_at):
            window_state["previousCostUsd"] = total_cost
            return None

    if normalized <= window_state["baselinePercent"]:
        if reset_moved_forward(window_state["baselineResetAt"], reset_at):
            record_special_event(state, "reset-time-moved-forward", sample, label, window_state, {"baselineResetAt": reset_at})
            window_state["baselineResetAt"] = reset_at
        if raw >= 100:
            window_state["baselineCostUsd"] = total_cost
        window_state["previousCostUsd"] = total_cost
        return None

    delta_percent = normalized - window_state["baselinePercent"]
    delta_cost = max(0.0, total_cost - window_state["baselineCostUsd"])
    if not valid_delta_cost_rate(delta_percent, delta_cost):
        record_special_event(state, "low-cost-delta-discarded", sample, label, window_state, {
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
        }, {"deltaPercent": delta_percent, "deltaCostUsd": delta_cost})
        window_state.update({
            "baselinePercent": normalized,
            "baselineCostUsd": total_cost,
            "baselineResetAt": reset_at,
            "previousCostUsd": total_cost,
        })
        clear_reset_recovery_state(window_state)
        return None
    ratio = percent_cost_ratio(delta_percent, delta_cost)
    average_ratio = percent_cost_ratio(window_state["cumulativePercent"], window_state["cumulativeCostUsd"])
    window_state["cumulativePercent"] = round(window_state["cumulativePercent"] + delta_percent, 8)
    window_state["cumulativeCostUsd"] = round(window_state["cumulativeCostUsd"] + delta_cost, 8)
    window_state.update({
        "baselinePercent": normalized,
        "baselineCostUsd": total_cost,
        "baselineResetAt": reset_at,
        "previousCostUsd": total_cost,
    })
    clear_reset_recovery_state(window_state)
    return {
        "checkedAt": sample["checkedAt"],
        "timestamp": parse_timestamp(sample["checkedAt"]),
        "window": label,
        "rawPercent": raw,
        "normalizedPercent": normalized,
        "plan": plan,
        "planMultiplier": multiplier,
        "resetAt": reset_at,
        "deltaPercent": round(delta_percent, 8),
        "deltaCostUsd": round(delta_cost, 8),
        "percentCostRatio": round(ratio, 8),
        "averagePercentCostRatio": None if average_ratio is None else round(average_ratio, 8),
        "ratioDeviation": None if ratio_deviation(ratio, average_ratio) is None else round(ratio_deviation(ratio, average_ratio), 8),
        "ratioDeviationWarning": ratio_deviation_warning(ratio, average_ratio),
        "cumulativePercent": window_state["cumulativePercent"],
        "cumulativeCostUsd": window_state["cumulativeCostUsd"],
        "sampleCostUsd": total_cost,
    }

def process_sample_delta_events(state: dict, sample: dict, existing_events: list[dict]) -> list[dict]:
    state["_specialEvents"] = []
    hydrate_delta_state_from_events(state, existing_events)
    rejection_reasons = remote_usage_rejection_reasons(state, sample)
    if rejection_reasons:
        sample.setdefault("errors", {})["remoteUsageRejected"] = "; ".join(rejection_reasons)
        if sample.get("remoteUsage"):
            sample["remoteUsage"]["accepted"] = False
            sample["remoteUsage"]["rejectionReasons"] = rejection_reasons
        sample["rejectedWindows"] = sample.get("windows") or {}
        sample["windows"] = {}
        record_remote_usage_rejection(state, sample, rejection_reasons)
        state["tokenUsage"] = compact_token_usage_for_state(sample.get("tokenUsage"))
        state["cost"] = sample.get("cost")
        state["lastSample"] = compact_sample_for_state(sample)
        state["updatedAt"] = sample["checkedAt"]
        return []
    if sample.get("remoteUsage"):
        sample["remoteUsage"]["accepted"] = True
    events = [event for event in (build_delta_event_from_sample(state, sample, "5h"), build_delta_event_from_sample(state, sample, "7d")) if event]
    identity = remote_usage_identity(sample)
    if identity:
        state["remoteUsageIdentity"] = identity
    state["tokenUsage"] = compact_token_usage_for_state(sample.get("tokenUsage"))
    state["cost"] = sample.get("cost")
    state["lastSample"] = compact_sample_for_state(sample)
    state["updatedAt"] = sample["checkedAt"]
    return events

def event_identity(event: dict) -> tuple:
    return (
        event["window"],
        event["checkedAt"],
        round(event["deltaPercent"], 8),
        round(event["deltaCostUsd"], 8),
        round(event.get("percentCostRatio") or percent_cost_ratio(event["deltaPercent"], event["deltaCostUsd"]) or 0, 8),
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
        f"[delta] {format_console_timestamp(event['checkedAt'])} {event['window']} "
        f"+{event['deltaPercent']:.4g}% / +${event['deltaCostUsd']:.6g}; "
        f"ratio {event.get('percentCostRatio', percent_cost_ratio(event['deltaPercent'], event['deltaCostUsd'])):.6g}%/$; "
        f"cumulative {event['cumulativePercent']:.4g}% / ${event['cumulativeCostUsd']:.6g}; "
        f"event plan {event.get('plan') or 'unknown'} ({coerce_float(event.get('planMultiplier')) or 1.0:g}x); "
        f"current 5h {format_percent(five['normalizedPercent'])} [{five['plan']} {five['planMultiplier']:g}x], "
        f"7d {format_percent(seven['normalizedPercent'])} [{seven['plan']} {seven['planMultiplier']:g}x]"
    )

def format_ratio_warning(event: dict) -> str:
    return (
        f"[warning] {format_console_timestamp(event['checkedAt'])} {event['window']} percent/cost ratio "
        f"{event['percentCostRatio']:.6g}%/$ deviates from average {event['averagePercentCostRatio']:.6g}%/$ "
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
