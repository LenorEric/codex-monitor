#!/usr/bin/env python3

import json
import threading
from datetime import datetime, timezone
from pathlib import Path

from monitor_common import empty_cost_totals, empty_token_totals, parse_timestamp

MODEL_PRICES_PER_MILLION = {
    "gpt-5.6-sol": {"input": 5.00, "cachedInput": 0.50, "cacheWriteInput": 6.25, "output": 30.00},
    "gpt-5.6-terra": {"input": 2.50, "cachedInput": 0.25, "cacheWriteInput": 3.125, "output": 15.00},
    "gpt-5.6-luna": {"input": 1.00, "cachedInput": 0.10, "cacheWriteInput": 1.25, "output": 6.00},
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

FAST_MODE_COST_MULTIPLIERS = {
    "gpt-5.4": 2.0,
    "gpt-5.5": 2.5,
    "gpt-5.6": 2.5,
}
DEFAULT_FAST_MODE_COST_MULTIPLIER = 2.0
MAX_CODEX_APPEND_DRAIN_READS = 3

_CODEX_SESSION_SCAN_CACHE: dict[str, dict[str, dict]] = {}
_CODEX_SESSION_SCAN_LOCK = threading.RLock()

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

def fast_mode_cost_multiplier(model: str) -> float:
    normalized = normalize_codex_model(model)
    return next((multiplier for prefix, multiplier in FAST_MODE_COST_MULTIPLIERS.items() if normalized.startswith(prefix)), DEFAULT_FAST_MODE_COST_MULTIPLIER)

def normalize_service_tier(value) -> str:
    return "fast" if str(value or "").strip().lower() in {"fast", "priority"} else "default"

def add_cost_delta(costs: dict, tokens: dict, pricing: dict | None, multiplier: float = 1.0) -> None:
    if not pricing:
        return
    input_price = pricing.get("input") or 0.0
    cached_price = pricing.get("cachedInput")
    if cached_price is None:
        cached_price = input_price
    cache_write_price = pricing.get("cacheWriteInput")
    if cache_write_price is None:
        cache_write_price = input_price
    output_price = pricing.get("output") or 0.0
    input_cost = (tokens.get("freshInputTokens", 0) or 0) * input_price * multiplier / 1_000_000
    cached_cost = (tokens.get("cachedInputTokens", 0) or 0) * cached_price * multiplier / 1_000_000
    cache_write_cost = (tokens.get("cacheWriteInputTokens", 0) or 0) * cache_write_price * multiplier / 1_000_000
    output_cost = (tokens.get("outputTokens", 0) or 0) * output_price * multiplier / 1_000_000
    costs["inputCostUsd"] += input_cost
    costs["cachedInputCostUsd"] += cached_cost
    costs["cacheWriteInputCostUsd"] += cache_write_cost
    costs["outputCostUsd"] += output_cost
    costs["totalCostUsd"] += input_cost + cached_cost + cache_write_cost + output_cost

def calculate_token_costs(token_usage: dict) -> dict:
    costs = empty_cost_totals()
    for model_costs in calculate_token_costs_by_model(token_usage).values():
        for key in costs:
            costs[key] += model_costs[key]
    return {key: round(value, 8) for key, value in costs.items()}

def calculate_token_costs_by_model(token_usage: dict) -> dict[str, dict]:
    result = {}
    by_model = token_usage.get("byModel") or {}
    if by_model:
        for model, totals in by_model.items():
            normalized = normalize_codex_model(model)
            costs = result.setdefault(normalized, empty_cost_totals())
            add_cost_delta(costs, totals, pricing_for_model(normalized))
            fast_totals = (token_usage.get("fastByModel") or {}).get(model) or (token_usage.get("fastByModel") or {}).get(normalized)
            if fast_totals:
                add_cost_delta(costs, fast_totals, pricing_for_model(normalized), fast_mode_cost_multiplier(normalized) - 1)
    else:
        costs = result.setdefault("gpt-5.5", empty_cost_totals())
        add_cost_delta(costs, token_usage.get("totals") or {}, pricing_for_model("gpt-5.5"))
    return {model: {key: round(value, 8) for key, value in costs.items()} for model, costs in result.items()}

def cost_progress(current: dict | None, previous: dict | None) -> dict:
    current_costs = current or empty_cost_totals()
    previous_costs = previous or empty_cost_totals()
    return {key: round(max(0.0, (current_costs.get(key) or 0.0) - (previous_costs.get(key) or 0.0)), 8) for key in empty_cost_totals()}

def cost_progress_by_model(current: dict | None, previous: dict | None) -> dict[str, dict]:
    current = current or {}
    previous = previous or {}
    return {model: cost_progress(current.get(model), previous.get(model)) for model in current.keys() | previous.keys()}

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
    input_details = value.get("input_tokens_details") or value.get("input_token_details") or value.get("prompt_tokens_details") or {}
    if not isinstance(input_details, dict):
        input_details = {}
    result = {
        "input": int(value.get("input_tokens") or value.get("prompt_tokens") or 0),
        "cachedInput": int(value.get("cached_input_tokens") or value.get("cache_read_input_tokens") or input_details.get("cached_tokens") or input_details.get("cache_read_tokens") or 0),
        "cacheWriteInput": int(value.get("cache_write_input_tokens") or value.get("cache_creation_input_tokens") or value.get("cache_write_tokens") or input_details.get("cache_write_tokens") or input_details.get("cache_creation_tokens") or 0),
        "output": int(value.get("output_tokens") or value.get("completion_tokens") or 0),
    }
    return result

def token_delta(previous: dict | None, current: dict) -> dict:
    if previous is None:
        return current.copy()
    return {
        "input": max(0, current["input"] - previous["input"]),
        "cachedInput": max(0, current["cachedInput"] - previous["cachedInput"]),
        "cacheWriteInput": max(0, current["cacheWriteInput"] - previous["cacheWriteInput"]),
        "output": max(0, current["output"] - previous["output"]),
    }

def add_token_delta(totals: dict, delta: dict) -> None:
    totals["inputTokens"] += delta["input"]
    totals["freshInputTokens"] += max(0, delta["input"] - delta["cachedInput"] - delta["cacheWriteInput"])
    totals["cachedInputTokens"] += delta["cachedInput"]
    totals["cacheWriteInputTokens"] += delta["cacheWriteInput"]
    totals["outputTokens"] += delta["output"]
    totals["totalTokens"] += delta["input"] + delta["output"]
    totals["requests"] += 1

def normalize_saved_token_totals(value: dict | None) -> dict:
    value = value if isinstance(value, dict) else {}
    input_tokens = max(0, int(value.get("inputTokens") or 0))
    cached_input_tokens = min(input_tokens, max(0, int(value.get("cachedInputTokens") or 0)))
    cache_write_input_tokens = min(input_tokens - cached_input_tokens, max(0, int(value.get("cacheWriteInputTokens") or value.get("cachedOutputTokens") or 0)))
    output_tokens = max(0, int(value.get("outputTokens") or 0))
    return {
        "inputTokens": input_tokens,
        "freshInputTokens": max(0, input_tokens - cached_input_tokens - cache_write_input_tokens),
        "cachedInputTokens": cached_input_tokens,
        "cacheWriteInputTokens": cache_write_input_tokens,
        "outputTokens": output_tokens,
        "totalTokens": input_tokens + output_tokens,
        "requests": max(0, int(value.get("requests") or 0)),
    }

def _codex_session_identity(payload: dict, path: Path) -> tuple[str, bool]:
    thread_id = payload.get("id") or payload.get("thread_id") or payload.get("threadId") or payload.get("session_id") or payload.get("sessionId") or path.stem
    parent_id = payload.get("session_id") or payload.get("sessionId")
    source = payload.get("source") or {}
    carries_history = bool(payload.get("forked_from_id") or isinstance(source, dict) and source.get("subagent") or parent_id and parent_id != thread_id)
    return str(thread_id), carries_history

def _session_totals() -> dict:
    return empty_token_totals()

def _new_codex_file_state(path: Path, identity: tuple[int, int]) -> dict:
    return {
        "identity": identity, "size": 0, "mtimeNs": 0, "offset": 0, "partial": b"", "unterminatedComplete": False, "lineIndex": 0, "sessionId": path.stem, "carriesHistory": False, "replayBoundary": None,
        "previousTotal": None, "currentModel": "unknown", "currentServiceTier": "default", "eventIndex": 0, "events": [],
    }

def _consume_codex_session_line(state: dict, path: Path, raw_line: bytes) -> None:
    line_index = state["lineIndex"]
    state["lineIndex"] += 1
    try:
        line = raw_line.decode("utf-8")
    except UnicodeDecodeError:
        return
    event = None
    if "\"session_meta\"" in line or "\"thread_settings_applied\"" in line or "\"inter_agent_communication" in line:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            return
        event_type = event.get("type")
        if event_type == "session_meta":
            state["sessionId"], state["carriesHistory"] = _codex_session_identity(event.get("payload") or {}, path)
            return
        elif str(event_type or "").startswith("inter_agent_communication") or event_type == "event_msg" and (event.get("payload") or {}).get("type") == "thread_settings_applied":
            state["replayBoundary"] = line_index
            return
    if "\"event_msg\"" not in line and "\"turn_context\"" not in line or "\"event_msg\"" in line and "\"token_count\"" not in line:
        return
    if event is None:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            return
    event_type = event.get("type")
    if event_type == "turn_context":
        payload = event.get("payload") or {}
        model = payload.get("model") or (payload.get("info") or {}).get("model")
        if isinstance(model, str) and model:
            state["currentModel"] = normalize_codex_model(model)
        service_tier = (payload.get("thread_settings") or {}).get("service_tier") or payload.get("service_tier")
        if service_tier is not None:
            state["currentServiceTier"] = normalize_service_tier(service_tier)
        return
    if event_type != "event_msg":
        return
    payload = event.get("payload") or {}
    if payload.get("type") != "token_count" or not isinstance(payload.get("info"), dict):
        return
    info = payload["info"]
    model = info.get("model") or info.get("model_name") or payload.get("model")
    if isinstance(model, str) and model:
        state["currentModel"] = normalize_codex_model(model)
    service_tier = info.get("service_tier") or payload.get("service_tier")
    if service_tier is not None:
        state["currentServiceTier"] = normalize_service_tier(service_tier)
    if isinstance(info.get("total_token_usage"), dict):
        current = parse_token_usage(info["total_token_usage"])
        if current is None:
            return
        delta = token_delta(state["previousTotal"], current)
        state["previousTotal"] = current
    elif isinstance(info.get("last_token_usage"), dict):
        delta = parse_token_usage(info["last_token_usage"])
        if delta is None:
            return
    else:
        return
    delta["cachedInput"] = min(delta["cachedInput"], delta["input"])
    delta["cacheWriteInput"] = min(delta["cacheWriteInput"], delta["input"] - delta["cachedInput"])
    if delta["input"] == 0 and delta["cachedInput"] == 0 and delta["cacheWriteInput"] == 0 and delta["output"] == 0:
        return
    state["eventIndex"] += 1
    state["events"].append({
        "index": state["eventIndex"], "lineIndex": line_index, "timestamp": parse_timestamp(event.get("timestamp")), "checkedAt": event.get("timestamp"),
        "model": state["currentModel"], "serviceTier": state["currentServiceTier"], "tokens": delta,
    })

def _update_codex_file_state(path: Path, state: dict | None, retry_race: bool = True) -> dict | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    identity = (stat.st_dev, stat.st_ino)
    if state is not None and state["identity"] == identity and state["size"] == stat.st_size and state["offset"] == stat.st_size and state["mtimeNs"] == stat.st_mtime_ns:
        return state
    if state is None or state["identity"] != identity or stat.st_size < state["offset"] or stat.st_size == state["size"]:
        state = _new_codex_file_state(path, identity)
    initial_size, initial_mtime_ns = stat.st_size, stat.st_mtime_ns
    reads = 0
    while True:
        try:
            with path.open("rb") as stream:
                stream.seek(state["offset"])
                appended = stream.read()
        except OSError:
            return None
        reads += 1
        bytes_read = len(appended)
        if state["unterminatedComplete"] and appended:
            if appended.startswith(b"\r\n"):
                appended = appended[2:]
            elif appended.startswith((b"\n", b"\r")):
                appended = appended[1:]
            else:
                return _update_codex_file_state(path, None, False)
            state["unterminatedComplete"] = False
        state["offset"] += bytes_read
        lines = (state["partial"] + appended).splitlines(keepends=True)
        state["partial"] = b""
        if lines and not lines[-1].endswith((b"\n", b"\r")):
            candidate = lines.pop()
            try:
                json.loads(candidate.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                state["partial"] = candidate
            else:
                lines.append(candidate)
                state["unterminatedComplete"] = True
        for line in lines:
            _consume_codex_session_line(state, path, line)
        try:
            final_stat = path.stat()
        except OSError:
            return None
        if (final_stat.st_dev, final_stat.st_ino) != identity or final_stat.st_size < state["offset"]:
            return _update_codex_file_state(path, None, False)
        if retry_race and final_stat.st_size == initial_size and final_stat.st_mtime_ns != initial_mtime_ns:
            return _update_codex_file_state(path, None, False)
        if final_stat.st_size == state["offset"] or reads >= MAX_CODEX_APPEND_DRAIN_READS:
            break
    state["size"] = state["offset"]
    state["mtimeNs"] = final_stat.st_mtime_ns
    return state

def _scan_codex_token_events_unlocked(home: Path) -> tuple[list[dict], int, float | None]:
    files = collect_codex_session_files(home)
    file_keys = [(path, str(path.resolve())) for path in files]
    cache = _CODEX_SESSION_SCAN_CACHE.setdefault(str(home.resolve()), {})
    current_paths = {key for _, key in file_keys}
    for cached_path in cache.keys() - current_paths:
        del cache[cached_path]
    for path, key in file_keys:
        state = _update_codex_file_state(path, cache.get(key))
        if state is None:
            cache.pop(key, None)
        else:
            cache[key] = state
    usage_events = []
    seen_event_ids = set()
    latest_timestamp = None
    for _, key in file_keys:
        state = cache.get(key)
        if state is None:
            continue
        replay_boundary = state["replayBoundary"] if state["carriesHistory"] else None
        for cached_event in state["events"]:
            if replay_boundary is not None and cached_event["lineIndex"] < replay_boundary:
                continue
            event_id = f'{state["sessionId"]}:{cached_event["index"]}'
            if event_id in seen_event_ids:
                continue
            seen_event_ids.add(event_id)
            event = {"eventId": event_id, "sessionId": state["sessionId"], **{key: value for key, value in cached_event.items() if key not in {"index", "lineIndex"}}}
            usage_events.append(event)
            latest_timestamp = max(latest_timestamp or 0, event.get("timestamp") or 0) or latest_timestamp
    return usage_events, len(files), latest_timestamp

def _scan_codex_token_events(home: Path) -> tuple[list[dict], int, float | None]:
    with _CODEX_SESSION_SCAN_LOCK:
        return _scan_codex_token_events_unlocked(home)

def token_sessions_from_events(usage_events: list[dict]) -> list[dict]:
    sessions = {}
    for event in usage_events:
        session = sessions.setdefault(event["sessionId"], {"sessionId": event["sessionId"], "startedAt": event.get("checkedAt"), "updatedAt": event.get("checkedAt"), "tokens": _session_totals(), "byModel": {}, "fastByModel": {}})
        if (event.get("timestamp") or 0) < (parse_timestamp(session.get("startedAt")) or float("inf")):
            session["startedAt"] = event.get("checkedAt")
        if (event.get("timestamp") or 0) >= (parse_timestamp(session.get("updatedAt")) or 0):
            session["updatedAt"] = event.get("checkedAt")
        add_token_delta(session["tokens"], event["tokens"])
        add_token_delta(session["byModel"].setdefault(event["model"], _session_totals()), event["tokens"])
        if event.get("serviceTier") == "fast":
            add_token_delta(session["fastByModel"].setdefault(event["model"], _session_totals()), event["tokens"])
    for session in sessions.values():
        model_tokens = {}
        fast_by_model = session.pop("fastByModel")
        for model, tokens in session.pop("byModel").items():
            model_tokens[model] = {"tokens": tokens, "cost": calculate_token_costs({"byModel": {model: tokens}, "fastByModel": {model: fast_by_model[model]} if model in fast_by_model else {}})}
            if model in fast_by_model:
                model_tokens[model]["fastTokens"] = fast_by_model[model]
        session["byModel"] = model_tokens
        session["cost"] = calculate_token_costs({"byModel": {model: values["tokens"] for model, values in model_tokens.items()}, "fastByModel": fast_by_model})
    return sorted(sessions.values(), key=lambda session: (parse_timestamp(session.get("updatedAt")) or 0, session["sessionId"]))

def scan_codex_token_usage(home: Path) -> dict:
    usage_events, files_scanned, latest_timestamp = _scan_codex_token_events(home)
    totals = _session_totals()
    by_model: dict[str, dict] = {}
    fast_by_model: dict[str, dict] = {}
    for event in usage_events:
        add_token_delta(totals, event["tokens"])
        add_token_delta(by_model.setdefault(event["model"], _session_totals()), event["tokens"])
        if event.get("serviceTier") == "fast":
            add_token_delta(fast_by_model.setdefault(event["model"], _session_totals()), event["tokens"])

    return {
        "source": "codex_session_logs",
        "home": str(home),
        "filesScanned": files_scanned,
        "latestEventAt": datetime.fromtimestamp(latest_timestamp, timezone.utc).isoformat().replace("+00:00", "Z") if latest_timestamp else None,
        "totals": totals,
        "byModel": by_model,
        "fastByModel": fast_by_model,
        "sessions": token_sessions_from_events(usage_events),
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
        "cacheWriteInputTokens": max(0, current_totals.get("cacheWriteInputTokens", 0) - previous_totals.get("cacheWriteInputTokens", 0)),
        "outputTokens": max(0, current_totals["outputTokens"] - previous_totals["outputTokens"]),
        "totalTokens": max(0, current_totals["totalTokens"] - previous_totals["totalTokens"]),
        "requests": max(0, current_totals["requests"] - previous_totals["requests"]),
    }
