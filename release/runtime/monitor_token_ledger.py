#!/usr/bin/env python3

import hashlib
import json
import os
from pathlib import Path

from monitor_common import UNKNOWN_EVENT_ACCOUNT_ID, UNKNOWN_EVENT_ACCOUNT_LABEL, empty_cost_totals, empty_token_totals, parse_timestamp
from monitor_tokens import add_cost_delta, add_token_delta, normalize_codex_model, normalize_saved_token_totals, pricing_epoch_for_model, sum_cost_totals, token_totals_delta

LEDGER_SCHEMA_VERSION = 1
TOKEN_KEYS = tuple(empty_token_totals())

def default_token_ledger_path(history_path: Path) -> Path:
    return history_path.with_name("usage_monitor_token_ledger.jsonl") if history_path.name == "usage_monitor_history.jsonl" else history_path.with_suffix(".token-ledger.jsonl")

def load_token_ledger(path: Path) -> list[dict]:
    try:
        content = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError(f"Cannot read token ledger: {exc}") from exc
    rows = []
    lines = content.splitlines()
    for index, line in enumerate(lines):
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid token ledger row {index + 1}: {exc}") from exc
        if not isinstance(row, dict) or row.get("schemaVersion") != LEDGER_SCHEMA_VERSION or row.get("recordType") not in {"priceEpoch", "legacyBaseline", "usage"}:
            raise ValueError(f"Unsupported token ledger row {index + 1}")
        rows.append(row)
    return rows

def append_token_ledger(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_separator = False
    if path.exists() and path.stat().st_size:
        with path.open("rb") as existing:
            existing.seek(-1, os.SEEK_END)
            needs_separator = existing.read(1) not in {b"\n", b"\r"}
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        if needs_separator:
            stream.write("\n")
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        stream.flush()
        os.fsync(stream.fileno())

def _subtract_tokens(current: dict | None, previous: dict | None) -> dict:
    current, previous = normalize_saved_token_totals(current), normalize_saved_token_totals(previous)
    return normalize_saved_token_totals({key: max(0, current[key] - previous[key]) for key in TOKEN_KEYS})

def _add_tokens(target: dict, value: dict | None) -> None:
    value = normalize_saved_token_totals(value)
    for key in TOKEN_KEYS:
        target[key] += value[key]

def _tier_totals(value: dict) -> dict[str, dict]:
    fast = normalize_saved_token_totals(value.get("fastTokens"))
    return {"default": _subtract_tokens(value.get("tokens"), fast), "fast": fast}

def _legacy_baseline(session: dict) -> dict:
    by_model = session.get("byModel") or {"gpt-5.5": {"tokens": session.get("tokens"), "cost": session.get("cost")}}
    if not session.get("byModel"):
        session = session | {"byModel": by_model}
    return {
        "schemaVersion": LEDGER_SCHEMA_VERSION, "recordType": "legacyBaseline", "pricingBasis": "legacyRecordedCost", "session": session,
        "sourceTotals": {model: _tier_totals(value) for model, value in by_model.items() if isinstance(value, dict)},
    }

def _legacy_baselines(sessions: list[dict]) -> list[dict]:
    return [_legacy_baseline(session) for session in sessions if session.get("sessionId")]

def _source_highwaters(rows: list[dict]) -> dict[tuple[str, str, str], dict]:
    result = {}
    for row in rows:
        if row.get("recordType") == "legacyBaseline":
            session_id = str((row.get("session") or {}).get("sessionId") or "")
            for model, tiers in (row.get("sourceTotals") or {}).items():
                for tier, totals in (tiers or {}).items():
                    result[(session_id, str(model), str(tier))] = normalize_saved_token_totals(totals)
        elif row.get("recordType") == "usage":
            key = (str(row.get("sessionId") or ""), str(row.get("rawModel") or ""), str(row.get("serviceTier") or "default"))
            _add_tokens(result.setdefault(key, empty_token_totals()), row.get("tokens"))
    return result

def _pricing_record(epoch: dict) -> dict:
    identity = json.dumps(epoch, sort_keys=True, separators=(",", ":"))
    return {"schemaVersion": LEDGER_SCHEMA_VERSION, "recordType": "priceEpoch", "pricingId": hashlib.sha256(identity.encode()).hexdigest()[:24], **epoch}

def _account_for_event(event: dict, account_slot_id: str, account_label: str, account_timeline: list[dict]) -> tuple[str, str]:
    timestamp = parse_timestamp(event.get("checkedAt"))
    attributed = next((row for row in reversed(account_timeline) if timestamp is not None and (parse_timestamp(row.get("checkedAt")) or float("inf")) <= timestamp), None)
    return str((attributed or {}).get("accountSlotId") or account_slot_id or UNKNOWN_EVENT_ACCOUNT_ID), str((attributed or {}).get("accountLabel") or account_label or UNKNOWN_EVENT_ACCOUNT_LABEL)

def _usage_records(events: list[dict], highwaters: dict, price_ids: dict, account_slot_id: str, account_label: str, account_timeline: list[dict]) -> tuple[list[dict], list[dict]]:
    cumulative, prices, usages = {}, [], []
    for event in events:
        key = (str(event.get("sessionId") or ""), str(event.get("model") or "unknown"), "fast" if event.get("serviceTier") == "fast" else "default")
        before = cumulative.setdefault(key, empty_token_totals()).copy()
        add_token_delta(cumulative[key], event["tokens"])
        baseline = highwaters.get(key, empty_token_totals())
        uncovered_before, uncovered_after = token_totals_delta(before, baseline), token_totals_delta(cumulative[key], baseline)
        delta = _subtract_tokens(uncovered_after, uncovered_before)
        if not any(delta[key] for key in TOKEN_KEYS):
            continue
        epoch = pricing_epoch_for_model(key[1], key[2], event.get("checkedAt"))
        pricing_id, cost = None, empty_cost_totals()
        if epoch is not None:
            pricing = _pricing_record(epoch)
            pricing_id = pricing["pricingId"]
            if pricing_id not in price_ids:
                prices.append(pricing)
                price_ids[pricing_id] = pricing
            add_cost_delta(cost, delta, pricing["rates"])
            cost = {name: round(value, 8) for name, value in cost.items()}
        slot_id, label = _account_for_event(event, account_slot_id, account_label, account_timeline)
        usages.append({
            "schemaVersion": LEDGER_SCHEMA_VERSION, "recordType": "usage", "eventId": str(event.get("eventId") or ""), "occurredAt": event.get("checkedAt"), "sessionId": key[0],
            "rawModel": key[1], "billingModel": normalize_codex_model(key[1]), "serviceTier": key[2], "accountSlotId": slot_id, "accountLabel": label,
            "pricingId": pricing_id, "tokens": delta, "cost": cost,
        })
    return prices, usages

def _empty_session(row: dict) -> dict:
    return {
        "sessionId": str(row.get("sessionId") or ""), "startedAt": row.get("occurredAt"), "updatedAt": row.get("occurredAt"),
        "accountSlotId": str(row.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID), "accountLabel": str(row.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL),
        "tokens": empty_token_totals(), "cost": empty_cost_totals(), "byModel": {},
    }

def token_sessions_from_ledger(rows: list[dict]) -> list[dict]:
    sessions = {}
    for row in rows:
        if row.get("recordType") == "legacyBaseline" and isinstance(row.get("session"), dict):
            session = row["session"]
            sessions[str(session.get("sessionId"))] = json.loads(json.dumps(session))
    for row in rows:
        if row.get("recordType") != "usage" or not row.get("sessionId"):
            continue
        session = sessions.setdefault(str(row["sessionId"]), _empty_session(row))
        occurred_at = row.get("occurredAt")
        if occurred_at and (parse_timestamp(occurred_at) or 0) < (parse_timestamp(session.get("startedAt")) or float("inf")):
            session["startedAt"] = occurred_at
        if occurred_at and (parse_timestamp(occurred_at) or 0) >= (parse_timestamp(session.get("updatedAt")) or 0):
            session["updatedAt"] = occurred_at
        _add_tokens(session["tokens"], row.get("tokens"))
        session["cost"] = sum_cost_totals(session.get("cost"), row.get("cost"))
        value = session["byModel"].setdefault(str(row.get("rawModel") or "unknown"), {"tokens": empty_token_totals(), "cost": empty_cost_totals()})
        _add_tokens(value["tokens"], row.get("tokens"))
        value["cost"] = sum_cost_totals(value.get("cost"), row.get("cost"))
        if row.get("serviceTier") == "fast":
            value.setdefault("fastTokens", empty_token_totals())
            _add_tokens(value["fastTokens"], row.get("tokens"))
    return sorted(sessions.values(), key=lambda row: (parse_timestamp(row.get("updatedAt")) or 0, row.get("sessionId") or ""))

def token_cost_snapshot(sessions: list[dict]) -> tuple[dict, dict[str, dict]]:
    by_model = {}
    for session in sessions:
        for model, value in (session.get("byModel") or {}).items():
            normalized = normalize_codex_model(model)
            by_model[normalized] = sum_cost_totals(by_model.get(normalized), value.get("cost") if isinstance(value, dict) else None)
    return sum_cost_totals(*(session.get("cost") for session in sessions)), by_model

def sync_token_ledger(path: Path, legacy_sessions: list[dict], events: list[dict], account_slot_id: str, account_label: str, account_timeline: list[dict] | None = None) -> list[dict]:
    rows = load_token_ledger(path)
    additions = _legacy_baselines(legacy_sessions) if not any(row.get("recordType") in {"legacyBaseline", "usage"} for row in rows) else []
    combined = rows + additions
    timeline = sorted(account_timeline or [], key=lambda row: parse_timestamp(row.get("checkedAt")) or 0)
    prices, usages = _usage_records(events, _source_highwaters(combined), {row["pricingId"]: row for row in combined if row.get("recordType") == "priceEpoch" and row.get("pricingId")}, account_slot_id, account_label, timeline)
    append_token_ledger(path, additions + prices + usages)
    sessions = token_sessions_from_ledger(combined + prices + usages)
    labels = {str(row["accountSlotId"]): str(row["accountLabel"]) for row in timeline if row.get("accountSlotId") and row.get("accountLabel")} | {str(account_slot_id): str(account_label)}
    for session in sessions:
        if session.get("accountSlotId") in labels:
            session["accountLabel"] = labels[session["accountSlotId"]]
    return sessions
