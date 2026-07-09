#!/usr/bin/env python3

import json
from datetime import datetime, timezone
from pathlib import Path

from monitor_common import empty_cost_totals, empty_token_totals, parse_timestamp

MODEL_PRICES_PER_MILLION = {
    "gpt-5.5": {"input": 5.00, "cachedInput": 0.50, "output": 30.00},
    "chat-latest": {"input": 5.00, "cachedInput": 0.50, "output": 30.00},
    "gpt-5.5-pro": {"input": 30.00, "cachedInput": None, "output": 180.00},
    "gpt-5.4": {"input": 2.50, "cachedInput": 0.25, "output": 15.00},
    "gpt-5.4-mini": {"input": 0.75, "cachedInput": 0.075, "output": 4.50},
    "gpt-5.4-nano": {"input": 0.20, "cachedInput": 0.02, "output": 1.25},
    "gpt-5.4-pro": {"input": 30.00, "cachedInput": None, "output": 180.00},
    "gpt-5.3-codex": {"input": 1.75, "cachedInput": 0.175, "output": 14.00},
    "gpt-5-codex": {"input": 1.75, "cachedInput": 0.175, "output": 14.00},
    "gpt-5.2-codex": {"input": 1.75, "cachedInput": 0.175, "output": 14.00},
    "codex-auto-review": {"input": 1.75, "cachedInput": 0.175, "output": 14.00},
}

def normalize_codex_model(raw: str) -> str:
    name = raw.lower()
    if "/" in name:
        name = name.rsplit("/", 1)[1]
    if len(name) > 11:
        suffix = name[-11:]
        if suffix[0] == "-" and suffix[1:5].isdigit() and suffix[5] == "-" and suffix[6:8].isdigit() and suffix[8] == "-" and suffix[9:11].isdigit():
            name = name[:-11]
    if len(name) > 9:
        prefix, sep, suffix = name.rpartition("-")
        if sep and len(suffix) == 8 and suffix.isdigit():
            name = prefix
    return name

def pricing_for_model(model: str) -> dict | None:
    normalized = normalize_codex_model(model)
    if normalized in MODEL_PRICES_PER_MILLION:
        return MODEL_PRICES_PER_MILLION[normalized]
    for prefix in sorted(MODEL_PRICES_PER_MILLION, key=len, reverse=True):
        if normalized.startswith(prefix):
            return MODEL_PRICES_PER_MILLION[prefix]
    return None

def add_cost_delta(costs: dict, tokens: dict, pricing: dict | None) -> None:
    if not pricing:
        return
    input_price = pricing.get("input") or 0.0
    cached_price = pricing.get("cachedInput")
    if cached_price is None:
        cached_price = input_price
    output_price = pricing.get("output") or 0.0
    input_cost = (tokens.get("freshInputTokens", 0) or 0) * input_price / 1_000_000
    cached_cost = (tokens.get("cachedInputTokens", 0) or 0) * cached_price / 1_000_000
    output_cost = (tokens.get("outputTokens", 0) or 0) * output_price / 1_000_000
    costs["inputCostUsd"] += input_cost
    costs["cachedInputCostUsd"] += cached_cost
    costs["outputCostUsd"] += output_cost
    costs["totalCostUsd"] += input_cost + cached_cost + output_cost

def calculate_token_costs(token_usage: dict) -> dict:
    costs = empty_cost_totals()
    by_model = token_usage.get("byModel") or {}
    if by_model:
        for model, totals in by_model.items():
            add_cost_delta(costs, totals, pricing_for_model(model))
    else:
        add_cost_delta(costs, token_usage.get("totals") or {}, pricing_for_model("gpt-5.5"))
    return {key: round(value, 8) for key, value in costs.items()}

def cost_progress(current: dict | None, previous: dict | None) -> dict:
    current_costs = current or empty_cost_totals()
    previous_costs = previous or empty_cost_totals()
    return {key: round(max(0.0, (current_costs.get(key) or 0.0) - (previous_costs.get(key) or 0.0)), 8) for key in empty_cost_totals()}

def collect_codex_session_files(home: Path) -> list[Path]:
    files = []
    sessions_dir = home / "sessions"
    if sessions_dir.is_dir():
        for path in sessions_dir.rglob("*.jsonl"):
            if path.is_file():
                files.append(path)
    archived_dir = home / "archived_sessions"
    if archived_dir.is_dir():
        for path in archived_dir.glob("*.jsonl"):
            if path.is_file():
                files.append(path)
    return sorted(files)

def parse_token_usage(value: dict) -> dict | None:
    if not isinstance(value, dict):
        return None
    return {
        "input": int(value.get("input_tokens") or 0),
        "cachedInput": int(value.get("cached_input_tokens") or value.get("cache_read_input_tokens") or 0),
        "output": int(value.get("output_tokens") or 0),
    }

def token_delta(previous: dict | None, current: dict) -> dict:
    if previous is None:
        return current.copy()
    return {
        "input": max(0, current["input"] - previous["input"]),
        "cachedInput": max(0, current["cachedInput"] - previous["cachedInput"]),
        "output": max(0, current["output"] - previous["output"]),
    }

def add_token_delta(totals: dict, delta: dict) -> None:
    totals["inputTokens"] += delta["input"]
    totals["freshInputTokens"] += max(0, delta["input"] - delta["cachedInput"])
    totals["cachedInputTokens"] += delta["cachedInput"]
    totals["outputTokens"] += delta["output"]
    totals["totalTokens"] += delta["input"] + delta["output"]
    totals["requests"] += 1

def scan_codex_token_usage(home: Path) -> dict:
    files = collect_codex_session_files(home)
    totals = empty_token_totals()
    by_model: dict[str, dict] = {}
    latest_timestamp = None

    for path in files:
        previous_total = None
        current_model = "unknown"
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                if "\"event_msg\"" not in line and "\"turn_context\"" not in line:
                    continue
                if "\"event_msg\"" in line and "\"token_count\"" not in line:
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue

                event_type = event.get("type")
                if event_type == "turn_context":
                    payload = event.get("payload") or {}
                    model = payload.get("model") or (payload.get("info") or {}).get("model")
                    if isinstance(model, str) and model:
                        current_model = normalize_codex_model(model)
                    continue

                if event_type != "event_msg":
                    continue
                payload = event.get("payload") or {}
                if payload.get("type") != "token_count":
                    continue
                info = payload.get("info")
                if not isinstance(info, dict):
                    continue

                model = info.get("model") or info.get("model_name") or payload.get("model")
                if isinstance(model, str) and model:
                    current_model = normalize_codex_model(model)

                if isinstance(info.get("total_token_usage"), dict):
                    current = parse_token_usage(info["total_token_usage"])
                    if current is None:
                        continue
                    delta = token_delta(previous_total, current)
                    previous_total = current
                elif isinstance(info.get("last_token_usage"), dict):
                    delta = parse_token_usage(info["last_token_usage"])
                    if delta is None:
                        continue
                else:
                    continue

                delta["cachedInput"] = min(delta["cachedInput"], delta["input"])
                if delta["input"] == 0 and delta["cachedInput"] == 0 and delta["output"] == 0:
                    continue

                add_token_delta(totals, delta)
                add_token_delta(by_model.setdefault(current_model, empty_token_totals()), delta)
                latest_timestamp = max(latest_timestamp or 0, parse_timestamp(event.get("timestamp")) or 0) or latest_timestamp

    return {
        "source": "codex_session_logs",
        "home": str(home),
        "filesScanned": len(files),
        "latestEventAt": datetime.fromtimestamp(latest_timestamp, timezone.utc).isoformat().replace("+00:00", "Z") if latest_timestamp else None,
        "totals": totals,
        "byModel": by_model,
        "errors": [],
    }

def token_progress(current: dict, previous: dict | None) -> dict:
    if previous is None:
        return empty_token_totals()
    current_totals = current["totals"]
    previous_totals = previous["totals"]
    return {
        "inputTokens": max(0, current_totals["inputTokens"] - previous_totals["inputTokens"]),
        "freshInputTokens": max(0, current_totals["freshInputTokens"] - previous_totals.get("freshInputTokens", 0)),
        "cachedInputTokens": max(0, current_totals["cachedInputTokens"] - previous_totals["cachedInputTokens"]),
        "outputTokens": max(0, current_totals["outputTokens"] - previous_totals["outputTokens"]),
        "totalTokens": max(0, current_totals["totalTokens"] - previous_totals["totalTokens"]),
        "requests": max(0, current_totals["requests"] - previous_totals["requests"]),
    }
