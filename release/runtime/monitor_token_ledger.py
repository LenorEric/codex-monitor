#!/usr/bin/env python3

import json
from pathlib import Path

from monitor_common import UNKNOWN_EVENT_ACCOUNT_ID, UNKNOWN_EVENT_ACCOUNT_LABEL, empty_cost_totals, empty_token_totals, parse_timestamp
from monitor_tokens import add_cost_delta, add_token_delta, normalize_codex_model, normalize_saved_token_totals, pricing_epoch_for_model, sum_cost_totals, token_totals_delta

LEDGER_SCHEMA_VERSION = 2
TOKEN_KEYS = tuple(empty_token_totals())

def default_token_ledger_path(history_path: Path) -> Path:
    return history_path.with_name("usage_monitor_token_events.jsonl") if history_path.name in {"usage_monitor_history.jsonl", "usage_monitor_quota_history.jsonl", "usage_monitor_quota_readings.jsonl"} else history_path.with_suffix(".token-ledger.jsonl")


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

def canonical_token_ledger_rows(rows: list[dict]) -> list[dict]:
    for row in rows:
        if row.get("schemaVersion") != LEDGER_SCHEMA_VERSION or row.get("recordType") not in {"usage", "sessionBaseline"}:
            raise ValueError("Token ledger requires data migration")
        if any(key in row for key in ("pricingId", "pricingBasis")):
            raise ValueError("Token ledger contains obsolete pricing metadata")
        if "sourceCoverage" in row:
            validate_source_coverage(row["sourceCoverage"])
    return rows


def validate_source_coverage(coverage):
    if not isinstance(coverage, dict):
        raise ValueError("Invalid token source coverage")
    for model, tiers in coverage.items():
        if not isinstance(model, str) or not model or not isinstance(tiers, dict) or not set(tiers).issubset({"default", "fast"}):
            raise ValueError("Invalid token source coverage")
        for totals in tiers.values():
            if not isinstance(totals, dict) or set(totals) != set(TOKEN_KEYS) or any(type(value) is not int or value < 0 for value in totals.values()):
                raise ValueError("Invalid token source coverage")


def _source_highwaters(rows: list[dict]) -> dict[tuple[str, str, str], dict]:
    result = {}
    for row in rows:
        if row.get("recordType") == "sessionBaseline" or row.get("sourceCoverage"):
            session = row.get("session") or row
            session_id = str(session.get("sessionId") or "")
            source_totals = row.get("sourceCoverage") or row.get("sourceTotals")
            if not isinstance(source_totals, dict):
                by_model = session.get("byModel") or {"gpt-5.5": {"tokens": session.get("tokens"), "cost": session.get("cost")}}
                source_totals = {model: _tier_totals(value) for model, value in by_model.items() if isinstance(value, dict)}
            for model, tiers in source_totals.items():
                for tier, totals in (tiers or {}).items():
                    key = (session_id, str(model), str(tier))
                    value = normalize_saved_token_totals(totals)
                    result[key] = {name: max(value[name], result.get(key, {}).get(name, 0)) for name in TOKEN_KEYS}
    return result

def _account_for_event(event: dict, account_slot_id: str, account_label: str, account_timeline: list[dict]) -> tuple[str, str]:
    timestamp = parse_timestamp(event.get("checkedAt"))
    attributed = next((row for row in reversed(account_timeline) if timestamp is not None and (parse_timestamp(row.get("checkedAt")) or float("inf")) <= timestamp), None)
    return str((attributed or {}).get("accountSlotId") or account_slot_id or UNKNOWN_EVENT_ACCOUNT_ID), str((attributed or {}).get("accountLabel") or account_label or UNKNOWN_EVENT_ACCOUNT_LABEL)

def _usage_records(events: list[dict], highwaters: dict, account_slot_id: str, account_label: str, account_timeline: list[dict], cumulative: dict | None = None) -> list[dict]:
    cumulative, usages = cumulative if cumulative is not None else {}, []
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
        cost = empty_cost_totals()
        if epoch is not None:
            add_cost_delta(cost, delta, epoch["rates"])
            cost = {name: round(value, 8) for name, value in cost.items()}
        slot_id, label = _account_for_event(event, account_slot_id, account_label, account_timeline)
        usages.append({
            "schemaVersion": LEDGER_SCHEMA_VERSION, "recordType": "usage", "eventId": str(event.get("eventId") or ""), "occurredAt": event.get("checkedAt"), "sessionId": key[0],
            "rawModel": key[1], "billingModel": normalize_codex_model(key[1]), "serviceTier": key[2], "accountSlotId": slot_id, "accountLabel": label,
            "tokens": delta, "cost": cost,
        } | ({"sourceFile": event["sourceFile"]} if event.get("sourceFile") else {}))
    return usages

def _empty_session(row: dict) -> dict:
    session = {
        "sessionId": str(row.get("sessionId") or ""), "startedAt": row.get("occurredAt"), "updatedAt": row.get("occurredAt"),
        "accountSlotId": str(row.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID), "accountLabel": str(row.get("accountLabel") or UNKNOWN_EVENT_ACCOUNT_LABEL),
        "tokens": empty_token_totals(), "cost": empty_cost_totals(), "byModel": {},
    }
    if (row.get("sync") or {}).get("accountId"):
        session["usageAccountId"] = row["sync"]["accountId"]
    return session

def token_sessions_from_ledger(rows: list[dict]) -> list[dict]:
    sessions = {}
    for row in rows:
        if row.get("recordType") == "sessionBaseline" and isinstance(row.get("session"), dict):
            session = row["session"]
            sessions[(str(session.get("sessionId")), str(session.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID))] = json.loads(json.dumps(session))
            if (row.get("sync") or {}).get("accountId"):
                sessions[(str(session.get("sessionId")), str(session.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID))].setdefault("usageAccountId", row["sync"]["accountId"])
    for row in rows:
        if row.get("recordType") != "usage" or not row.get("sessionId"):
            continue
        session = sessions.setdefault((str(row["sessionId"]), str(row.get("accountSlotId") or UNKNOWN_EVENT_ACCOUNT_ID)), _empty_session(row))
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

def reconcile_token_ledger(rows, events, account_slot_id, account_label, account_timeline=None, record_provenance=None, source_files=None, own_machine_id=None):
    """Calculate ledger corrections from database rows and original source events."""
    timeline = sorted(account_timeline or [], key=lambda row: parse_timestamp(row.get("checkedAt")) or 0)
    usages = _usage_records(events, _source_highwaters(rows), account_slot_id, account_label, timeline)
    source_files = set(source_files or ())
    canonical = {row["eventId"]: row for row in usages if row.get("eventId")}
    retained, existing = [], {}
    for row in rows:
        if row.get("recordType") != "usage":
            retained.append(row)
            continue
        event_id = str(row.get("eventId") or "")
        local_origin = own_machine_id is None or not (row.get("sync") or {}).get("originMachineId") or (row.get("sync") or {}).get("originMachineId") == own_machine_id
        if local_origin and (event_id in canonical or row.get("sourceFile") in source_files):
            if event_id in canonical:
                existing[event_id] = row
            continue
        retained.append(row)
    reconciled, new_costs = [], []
    for usage in usages:
        if (previous := existing.get(usage["eventId"])) is not None:
            if previous.get("accountSlotId") not in {None, "", UNKNOWN_EVENT_ACCOUNT_ID}:
                usage = usage | {"accountSlotId": previous["accountSlotId"], "accountLabel": previous.get("accountLabel") or usage["accountLabel"]}
            if previous.get("sync") and previous.get("accountSlotId") not in {None, "", UNKNOWN_EVENT_ACCOUNT_ID}:
                usage["sync"] = previous["sync"]
            elif record_provenance is not None:
                usage = record_provenance(usage)
        else:
            if record_provenance is not None:
                usage = record_provenance(usage)
            new_costs.append(usage)
        reconciled.append(usage)
    return retained + reconciled, new_costs
