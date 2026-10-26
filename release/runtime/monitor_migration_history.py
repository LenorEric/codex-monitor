"""Readers used only by historical migrations."""
from monitor_history import *
from monitor_migration_ledger import normalize_saved_token_totals

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
