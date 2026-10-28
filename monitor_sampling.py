"""Deterministic quota sampling; raw history is immutable, decisions live in SQLite."""

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path

from monitor_common import coerce_float, parse_timestamp

WINDOWS = {"fiveHour": ("5h", 18000), "sevenDay": ("7d", 604800)}


def quota_point(row, slot_usage_accounts=None):
    from monitor_history import normalize_plan_type
    point = {
        "checkedAt": row.get("checkedAt"), "timestamp": parse_timestamp(row.get("checkedAt")),
        "compactedFrom": parse_timestamp((row.get("compaction") or {}).get("continuousFrom")),
        "accountSlotId": row.get("accountSlotId"), "accountLabel": row.get("accountLabel"),
        "usageAccountId": str(row.get("usageAccountId") or (row.get("sync") or {}).get("accountId") or (slot_usage_accounts or {}).get(str(row.get("accountSlotId") or "")) or row.get("accountSlotId") or row.get("accountLabel") or "unknown"),
    }
    for name, (label, _) in WINDOWS.items():
        window = (row.get("windows") or {}).get(label) or {}
        raw = None if window.get("unavailable") else coerce_float(window.get("usedPercent"))
        plan = normalize_plan_type(window.get("plan"))
        from monitor_interpretation import multiplier
        point[name] = {"raw": raw, "continuous": raw, "resetAt": window.get("resetAt"), "plan": "plus" if plan == "unknown" else plan, "multiplier": multiplier(plan, point["timestamp"] or 0)}
    return point


def sample_point(point, state, reset_points=None):
    """Advance only this observation. Rejected observations are never reconsidered."""
    for name, (_, period) in WINDOWS.items():
        window = point[name]
        raw, timestamp = window["raw"], point["timestamp"]
        if raw is None or not math.isfinite(raw) or not 0 <= raw <= 100 or timestamp is None:
            window["continuous"] = None
            continue
        previous = state.get(name)
        if previous is not None and previous["plan"] == window["plan"]:
            reset_at, previous_reset = parse_timestamp(window.get("resetAt")), parse_timestamp(previous.get("resetAt"))
            reset = raw < previous["raw"] and reset_at is not None and previous_reset is not None and reset_at > previous_reset and reset_at > previous["timestamp"] + period
            if not reset and raw < previous.get("protection", previous["raw"]):
                previous["protection"] = raw
                window["continuous"] = None
                continue
            if reset_points is not None and previous["raw"] != 0 and reset_at is not None and previous_reset is not None and reset_at > previous_reset and previous["timestamp"] < reset_at - period <= timestamp:
                point.setdefault("resetWindows", []).append(name)
                reset_points.append({**{key: point[key] for key in ("accountSlotId", "accountLabel", "usageAccountId")},
                    "checkedAt": datetime.fromtimestamp(reset_at - period, timezone.utc).isoformat(), "timestamp": reset_at - period,
                    "compactedFrom": None, "resetWindow": name,
                    **{other: {"raw": 0 if other == name else None, "continuous": 0 if other == name else None,
                        "resetAt": window["resetAt"] if other == name else None, "plan": point[other]["plan"]} for other in WINDOWS}})
        state[name] = {"raw": raw, "timestamp": timestamp, "checkedAt": point["checkedAt"], "resetAt": window.get("resetAt"), "plan": window["plan"]}
    return point


def accepted_windows(state):
    from monitor_interpretation import multiplier
    return {label: {"usedPercent": state[name]["raw"], "resetAt": state[name]["resetAt"], "plan": state[name]["plan"],
        "planMultiplier": multiplier(state[name]["plan"], state[name]["timestamp"]), "percentCheckedAt": state[name]["checkedAt"]}
        for name, (label, _) in WINDOWS.items() if name in state}


def sample_rows(rows, slot_usage_accounts=None, states=None):
    """Reference replay, also used for correction rebuilds. Merge duplicate observations first."""
    merged = {}
    for row in rows:
        point = quota_point(row, slot_usage_accounts)
        key = point["usageAccountId"], point["checkedAt"]
        if key in merged:
            for name in WINDOWS:
                if point[name]["raw"] is None:
                    point[name] = merged[key][name]
        merged[key] = point
    states = {} if states is None else states
    points = sorted(merged.values(), key=lambda point: (point["timestamp"] is None, point["timestamp"] or 0, point["checkedAt"] or "", point["usageAccountId"]))
    reset_points = []
    for point in points:
        sample_point(point, states.setdefault(point["usageAccountId"], {}), reset_points)
    return sorted(points + reset_points, key=lambda point: (point["timestamp"] is None, point["timestamp"] or 0, point["usageAccountId"], not bool(point.get("resetWindow")), point.get("resetWindow", "")))


def point_key(point):
    return f"{point['checkedAt']}:reset:{point['resetWindow']}" if point.get("resetWindow") else point["checkedAt"]


def initialize(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS processing_sampled_quota (
            view TEXT NOT NULL, account TEXT NOT NULL, record_key TEXT NOT NULL, event_at REAL, data TEXT NOT NULL,
            PRIMARY KEY(view,account,record_key)
        );
        CREATE INDEX IF NOT EXISTS processing_sampled_quota_order ON processing_sampled_quota(view,account,event_at,record_key);
        CREATE TABLE IF NOT EXISTS processing_sampling_changes (
            view TEXT NOT NULL, account TEXT NOT NULL, record_key TEXT NOT NULL, PRIMARY KEY(view,account,record_key)
        );
    """)
    rule = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    previous = db.execute("SELECT value FROM processing_meta WHERE name='samplingRule'").fetchone()
    if previous is None or json.loads(previous[0]) != rule:
        db.execute("DELETE FROM processing_meta WHERE name LIKE 'samplingState:%'")
        db.execute("DELETE FROM processing_sampled_quota")
        for view in ("local", "merged"):
            db.execute("INSERT OR IGNORE INTO processing_sampling_changes SELECT ?,account,'' FROM processing_facts WHERE kind='quota'", (view,))
        db.execute("INSERT OR REPLACE INTO processing_meta VALUES('samplingRule',?)", (json.dumps(rule),))


def changed(db, account, views=("local", "merged"), key=""):
    for view in views:
        db.execute("INSERT OR IGNORE INTO processing_sampling_changes VALUES(?,?,?)", (view, account, key))


def refresh(db, index):
    """Commit decisions and state in the same transaction as raw fact/checkpoint changes."""
    from monitor_processing import encode
    for view, account in list(db.execute("SELECT DISTINCT view,account FROM processing_sampling_changes")):
        keys = [row[0] for row in db.execute("SELECT record_key FROM processing_sampling_changes WHERE view=? AND account=?", (view, account))]
        state_key = f"samplingState:{view}:{account}"
        state = index.get(db, state_key)
        rebuild = "" in keys or state is None
        source_filter = "" if view == "merged" else " AND source NOT LIKE 'peer:%'"
        rows = []
        if not rebuild:
            for key in keys:
                rows.extend(json.loads(row[0]) for row in db.execute("SELECT data FROM processing_facts WHERE kind='quota' AND account=? AND record_key=?" + source_filter + " ORDER BY source,position", (account, key)))
            points = sample_rows(rows)
            # An overlap, replacement or duplicate timestamp requires replay from retained facts.
            rebuild = any(point["timestamp"] is None or point["timestamp"] <= state["timestamp"] for point in points)
        if rebuild:
            rows = [json.loads(row[0]) for row in db.execute("SELECT data FROM processing_facts WHERE kind='quota' AND account=?" + source_filter + " ORDER BY source,position", (account,))]
            state = {"timestamp": -1, "windows": {}}
            db.execute("DELETE FROM processing_sampled_quota WHERE view=? AND account=?", (view, account))
            index.metrics["quotaSamplingRebuilds"] = index.metrics.get("quotaSamplingRebuilds", 0) + 1
        points = sample_rows([row | {"usageAccountId": account} for row in rows], states={account: state["windows"]})
        index.metrics["quotaSamplesProcessed"] = index.metrics.get("quotaSamplesProcessed", 0) + len(points)
        for point in points:
            db.execute("INSERT OR REPLACE INTO processing_sampled_quota VALUES(?,?,?,?,?)", (view, account, point_key(point), point["timestamp"], encode(point)))
            if point["timestamp"] is not None:
                state["timestamp"] = point["timestamp"]
        index.put(db, state_key, state)
        db.execute("DELETE FROM processing_sampling_changes WHERE view=? AND account=?", (view, account))


def stored_points(db, view, account=None):
    query = "SELECT data FROM processing_sampled_quota WHERE view=?"
    values = [view]
    if account is not None:
        query += " AND account=?"
        values.append(account)
    points = [json.loads(row[0]) for row in db.execute(query + " ORDER BY event_at IS NULL,event_at,account,record_key", values)]
    return sorted(points, key=lambda point: (point["timestamp"] is None, point["timestamp"] or 0, point["usageAccountId"], not bool(point.get("resetWindow")), point.get("resetWindow", "")))
