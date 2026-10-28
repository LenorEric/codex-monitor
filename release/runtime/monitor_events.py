#!/usr/bin/env python3

from datetime import datetime

from monitor_common import (
    MIN_DELTA_COST_PER_PERCENT_USD, PLAN_MULTIPLIERS, RATIO_DEVIATION_MULTIPLIER, RESET_TIME_JITTER_SECONDS,
    UNKNOWN_EVENT_ACCOUNT_ID, UNKNOWN_EVENT_ACCOUNT_LABEL, coerce_float, parse_timestamp,
)
from monitor_history import compact_debug_state, compact_sample_for_state, compact_token_usage_for_state, is_delta_event_row
from monitor_tokens import normalize_codex_model

DEFAULT_EVENT_MODEL = "gpt-5.5"
COST_INTERVAL_TYPE = "costInterval"

def reset_moved_forward(previous_reset: str | None, current_reset: str | None) -> bool:
    previous_ts = parse_timestamp(previous_reset)
    current_ts = parse_timestamp(current_reset)
    return previous_ts is not None and current_ts is not None and current_ts - previous_ts > RESET_TIME_JITTER_SECONDS

def reset_moved_backward(previous_reset: str | None, current_reset: str | None) -> bool:
    previous_ts = parse_timestamp(previous_reset)
    current_ts = parse_timestamp(current_reset)
    return previous_ts is not None and current_ts is not None and previous_ts - current_ts > RESET_TIME_JITTER_SECONDS

def remote_usage_identity(sample: dict) -> dict | None:
    raw = ((sample.get("remoteUsage") or {}).get("rawResponse") or {})
    auth_identity = ((sample.get("remoteUsage") or {}).get("authIdentity") or {})
    identity = {key: raw.get(key) if raw.get(key) is not None else auth_identity.get(key) for key in ("user_id", "account_id", "email", "plan_type")}
    return identity if any(value is not None for value in identity.values()) else None

def remote_usage_identity_changed(state: dict, sample: dict) -> bool:
    identity = remote_usage_identity(sample)
    return bool(state.get("remoteUsageIdentity") and identity and identity != state["remoteUsageIdentity"])

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
    account = {
        "accountSlotId": event.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID,
        "accountLabel": event.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL,
    }
    if usage_account_id := event.get("usageAccountId") or (event.get("sync") or {}).get("accountId"):
        account["usageAccountId"] = usage_account_id
    return account

def record_special_event(state: dict, reason: str, sample: dict, label: str, previous: dict, current: dict, extra: dict | None = None) -> None:
    if not (extra or any(previous.get(key) != value for key, value in current.items())):
        return
    event_extra = dict(extra or {})
    state.setdefault("_lastSpecialEventReasonByWindow", {})
    if reason == "reset-time-moved-forward":
        event_extra["suppressConsole"] = state["_lastSpecialEventReasonByWindow"].get(label) == reason
    state["_lastSpecialEventReasonByWindow"][label] = reason
    state.setdefault("_specialEvents", []).append({
        "checkedAt": sample["checkedAt"],
        "window": label,
        "reason": reason,
        "previous": {key: previous.get(key) for key in ("baselinePercent", "baselineCostUsd", "baselineResetAt", "baselinePlan", "baselineMultiplier")},
        "current": current,
        "extra": event_extra,
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


def delta_state_needs_hydration(state: dict) -> bool:
    windows = state.get("windows")
    if not isinstance(windows, dict):
        return True
    for label in ("5h", "7d"):
        window = windows.get(label)
        if not isinstance(window, dict) or any(coerce_float(window.get(key)) is None for key in ("cumulativePercent", "cumulativeCostUsd")) or not isinstance(window.get("models"), dict):
            return True
        if any(not isinstance(model, dict) or any(coerce_float(model.get(key)) is None for key in ("cumulativePercent", "cumulativeCostUsd")) for model in window["models"].values()):
            return True
    return False

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
    if label in (sample.get("rejectedWindows") or {}):
        return []
    window = (sample.get("windows") or {}).get(label) or {}
    if window.get("unavailable"):
        # Forget the live baseline, retaining historical totals for the incremental (history=None) path.
        state["windows"][label] = {key: value for key, value in state["windows"][label].items() if key in {"cumulativePercent", "cumulativeCostUsd", "models"}}
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
        elif normalized < window_state["baselinePercent"] or reset_moved_forward(window_state.get("baselineResetAt"), reset_at) or reset_moved_backward(window_state.get("baselineResetAt"), reset_at):
            set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, plan, multiplier, True, sample["checkedAt"])
        else:
            window_state["previousCostUsd"] = total_cost
            window_state["previousCostByModelUsd"] = total_cost_by_model
        return []

    if normalized < window_state["baselinePercent"]:
        # Sampling has already accepted this decrease; establish a new cost baseline.
        record_special_event(state, "usage-percent-rollback", sample, label, window_state, {
            "baselinePercent": normalized, "baselineCostUsd": total_cost, "baselineResetAt": reset_at,
        })
        set_percent_baseline(window_state, normalized, total_cost, total_cost_by_model, reset_at, plan, multiplier, observed_at=sample["checkedAt"])
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
    return events

def process_sample_delta_events(state: dict, sample: dict, existing_events: list[dict] | None) -> list[dict]:
    state["_specialEvents"] = []
    state["_pendingCostIntervals"] = []
    if existing_events is not None:
        hydrate_delta_state_from_events(state, existing_events)
    else:
        for label in ("5h", "7d"):
            window = state.setdefault("windows", {}).setdefault(label, {})
            window.setdefault("cumulativePercent", 0.0)
            window.setdefault("cumulativeCostUsd", 0.0)
            window.setdefault("models", {})
    if sample.get("remoteUsage"):
        sample["remoteUsage"]["accepted"] = not bool(sample.get("rejectedWindows"))
    sample.setdefault("percentCheckedAt", sample["checkedAt"])
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

def print_special_events(events: list[dict]) -> None:
    for event in events:
        if (event.get("extra") or {}).get("suppressConsole"):
            continue
        print(format_special_event(event), flush=True)
