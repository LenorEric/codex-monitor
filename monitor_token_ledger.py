#!/usr/bin/env python3

from pathlib import Path

from monitor_common import UNKNOWN_EVENT_ACCOUNT_ID, UNKNOWN_EVENT_ACCOUNT_LABEL, empty_cost_totals, empty_token_totals, parse_timestamp
from monitor_tokens import add_cost_delta, add_token_delta, normalize_codex_model, normalize_saved_token_totals, pricing_epoch_for_model, sum_cost_totals

LEDGER_SCHEMA_VERSION = 2
TOKEN_KEYS = tuple(empty_token_totals())

def default_token_ledger_path(history_path: Path) -> Path:
    return history_path.with_name("usage_monitor_token_events.jsonl") if history_path.name in {"usage_monitor_history.jsonl", "usage_monitor_quota_history.jsonl", "usage_monitor_quota_readings.jsonl"} else history_path.with_suffix(".token-ledger.jsonl")


def _add_tokens(target: dict, value: dict | None) -> None:
    value = normalize_saved_token_totals(value)
    for key in TOKEN_KEYS:
        target[key] += value[key]

def canonical_token_ledger_rows(rows: list[dict]) -> list[dict]:
    for row in rows:
        if not isinstance(row, dict) or row.get("schemaVersion") != LEDGER_SCHEMA_VERSION or row.get("recordType") != "usage":
            raise ValueError("Token ledger requires data migration")
        if any(key in row for key in ("pricingId", "pricingBasis", "sourceCoverage", "sourceTotals")):
            raise ValueError("Token ledger contains obsolete metadata; data migration required")
    return rows


def _account_for_event(event: dict, account_slot_id: str, account_label: str, account_timeline: list[dict]) -> tuple[str, str]:
    timestamp = parse_timestamp(event.get("checkedAt"))
    attributed = next((row for row in reversed(account_timeline) if timestamp is not None and (parse_timestamp(row.get("checkedAt")) or float("inf")) <= timestamp), None)
    return str((attributed or {}).get("accountSlotId") or account_slot_id or UNKNOWN_EVENT_ACCOUNT_ID), str((attributed or {}).get("accountLabel") or account_label or UNKNOWN_EVENT_ACCOUNT_LABEL)

def _usage_records(events: list[dict], account_slot_id: str, account_label: str, account_timeline: list[dict], cumulative: dict | None = None) -> list[dict]:
    cumulative, usages = cumulative if cumulative is not None else {}, []
    for event in events:
        key = (str(event.get("sessionId") or ""), str(event.get("model") or "unknown"), "fast" if event.get("serviceTier") == "fast" else "default")
        delta = empty_token_totals()
        add_token_delta(delta, event["tokens"])
        _add_tokens(cumulative.setdefault(key, empty_token_totals()), delta)
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
    canonical_token_ledger_rows(rows)
    sessions = {}
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
    canonical_token_ledger_rows(rows)
    usages = _usage_records(events, account_slot_id, account_label, timeline)
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
