"""Sample legacy facts and consume canonical quota runs with saved decisions."""

import json
import math
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone

from monitor_common import coerce_float, parse_timestamp
import monitor_sample_store as samples

WINDOWS = {"fiveHour": ("5h", 18000), "sevenDay": ("7d", 604800)}
SAMPLING_RULE_VERSION = 1
_refresh_deferred = ContextVar("sampling_refresh_deferred", default=False)


@contextmanager
def defer_refresh():
    """Keep queued sampling work until the complete migration cascade finishes."""
    token = _refresh_deferred.set(True)
    try:
        yield
    finally:
        _refresh_deferred.reset(token)


def quota_point(row):
    from monitor_history import normalize_plan_type
    point = {
        "checkedAt": row.get("checkedAt"), "timestamp": parse_timestamp(row.get("checkedAt")),
        "accountSlotId": row.get("accountSlotId"), "accountLabel": row.get("accountLabel"),
        "usageAccountId": str(row.get("usageAccountId") or (row.get("sync") or {}).get("accountId") or row.get("accountSlotId") or row.get("accountLabel") or "unknown"),
    }
    if row.get("quotaRunVersion") == 1:
        from monitor_usage_sync import record_key
        point["_key"] = record_key("quota", row)
    for name, (label, _) in WINDOWS.items():
        window = (row.get("windows") or {}).get(label) or {}
        raw = None if window.get("unavailable") else coerce_float(window.get("usedPercent"))
        plan = normalize_plan_type(window.get("plan"))
        from monitor_interpretation import multiplier
        point[name] = {"raw": raw, "continuous": raw, "resetAt": window.get("resetAt"), "plan": "plus" if plan == "unknown" else plan, "multiplier": multiplier(plan, point["timestamp"] or 0)}
        if row.get("quotaRunVersion") == 1 and label in (row.get("windows") or {}):
            point[name].update({key: window[key] for key in ("continuous", "multiplier", "firstAt", "lastAt", "resetBoundary")})
            point[name]["storedRun"] = True
            point.setdefault("runWindows", []).append(name)
            if window["resetBoundary"]:
                point["resetWindow"] = name
    return point


def sample_point(point, state, reset_points=None):
    """Advance only this observation. Rejected observations are never reconsidered."""
    for name, (_, period) in WINDOWS.items():
        if "runWindows" in point and name not in point["runWindows"]:
            continue
        window = point[name]
        if window.get("storedRun"):
            from monitor_quota_runs import restore_window
            restore_window(point, name, window, state)
            continue
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
                    "resetWindow": name,
                    **{other: {"raw": 0 if other == name else None, "continuous": 0 if other == name else None,
                        "resetAt": window["resetAt"] if other == name else None, "plan": point[other]["plan"]} for other in WINDOWS}})
        state[name] = {"raw": raw, "timestamp": timestamp, "checkedAt": point["checkedAt"], "resetAt": window.get("resetAt"), "plan": window["plan"]}
    return point


def accepted_windows(state):
    from monitor_interpretation import multiplier
    return {label: {"usedPercent": state[name]["raw"], "resetAt": state[name]["resetAt"], "plan": state[name]["plan"],
        "planMultiplier": multiplier(state[name]["plan"], state[name]["timestamp"]), "percentCheckedAt": state[name]["checkedAt"]}
        for name, (label, _) in WINDOWS.items() if name in state}


def sample_rows(rows, states=None):
    """Replay legacy observations; restore run decisions without sampling them again."""
    merged = {}
    for row in rows:
        point = quota_point(row)
        key = point["usageAccountId"], point["checkedAt"], point.get("resetWindow"), point.get("_key")
        if key in merged:
            for name in WINDOWS:
                if point[name]["raw"] is None:
                    point[name] = merged[key][name]
            if "runWindows" in point and "runWindows" in merged[key]:
                point["runWindows"] = sorted(set(point["runWindows"] + merged[key]["runWindows"]))
        merged[key] = point
    states = {} if states is None else states
    points = sorted(merged.values(), key=lambda point: (point["timestamp"] is None, point["timestamp"] or 0, point["checkedAt"] or "", point["usageAccountId"]))
    reset_points = []
    for point in points:
        sample_point(point, states.setdefault(point["usageAccountId"], {}), reset_points)
    return sorted(points + reset_points, key=lambda point: (point["timestamp"] is None, point["timestamp"] or 0, point["usageAccountId"], not bool(point.get("resetWindow")), point.get("resetWindow", "")))


def point_key(point):
    return point.get("_key") or (f"{point['checkedAt']}:reset:{point['resetWindow']}" if point.get("resetWindow") else point["checkedAt"])


def initialize(db):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='processing_sampled_quota'").fetchone():
        for name, in db.execute("SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='processing_sampled_quota'").fetchall():
            db.execute(f"DROP TRIGGER IF EXISTS {name}")
        db.execute(
            "CREATE TABLE IF NOT EXISTS processing_sampled_quota_refs ("
            "view TEXT NOT NULL, account TEXT NOT NULL, record_key TEXT NOT NULL, event_at REAL, "
            "payload_id INTEGER NOT NULL REFERENCES processing_payloads(id), PRIMARY KEY(view,account,record_key))"
        )
        db.execute(
            "INSERT OR REPLACE INTO processing_sampled_quota_refs(view,account,record_key,event_at,payload_id) "
            "SELECT view,account,record_key,event_at,put_payload(data,NULL) FROM processing_sampled_quota"
        )
        db.execute("DROP TABLE processing_sampled_quota")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS processing_sampled_quota_refs (
            view TEXT NOT NULL,
            account TEXT NOT NULL,
            record_key TEXT NOT NULL,
            event_at REAL,
            payload_id INTEGER NOT NULL REFERENCES processing_payloads(id),
            PRIMARY KEY(view,account,record_key)
        );
        CREATE INDEX IF NOT EXISTS processing_sampled_quota_order ON processing_sampled_quota_refs(view,account,event_at,record_key);
        CREATE INDEX IF NOT EXISTS processing_sampled_quota_payload ON processing_sampled_quota_refs(payload_id);
        CREATE TRIGGER IF NOT EXISTS processing_sampled_quota_refs_delete AFTER DELETE ON processing_sampled_quota_refs BEGIN
            INSERT OR IGNORE INTO processing_payload_releases VALUES(OLD.payload_id);
        END;
        CREATE TRIGGER IF NOT EXISTS processing_sampled_quota_refs_update AFTER UPDATE ON processing_sampled_quota_refs BEGIN
            INSERT OR IGNORE INTO processing_payload_releases VALUES(OLD.payload_id);
        END;
        CREATE VIEW IF NOT EXISTS processing_sampled_quota AS
            SELECT r.view,r.account,r.record_key,r.event_at,p.data
            FROM processing_sampled_quota_refs r
            JOIN processing_payloads p ON p.id=r.payload_id;
        CREATE TRIGGER IF NOT EXISTS processing_sampled_quota_insert INSTEAD OF INSERT ON processing_sampled_quota BEGIN
            INSERT OR REPLACE INTO processing_sampled_quota_refs(view,account,record_key,event_at,payload_id)
            VALUES(NEW.view,NEW.account,NEW.record_key,NEW.event_at,put_payload(NEW.data, NULL));
        END;
        CREATE TRIGGER IF NOT EXISTS processing_sampled_quota_update INSTEAD OF UPDATE ON processing_sampled_quota BEGIN
            UPDATE processing_sampled_quota_refs
            SET view=NEW.view,account=NEW.account,record_key=NEW.record_key,event_at=NEW.event_at,
                payload_id=put_payload(NEW.data, NULL)
            WHERE view=OLD.view AND account=OLD.account AND record_key=OLD.record_key;
        END;
        CREATE TRIGGER IF NOT EXISTS processing_sampled_quota_delete INSTEAD OF DELETE ON processing_sampled_quota BEGIN
            DELETE FROM processing_sampled_quota_refs WHERE view=OLD.view AND account=OLD.account AND record_key=OLD.record_key;
        END;
        CREATE TABLE IF NOT EXISTS processing_sampling_changes (
            view TEXT NOT NULL, account TEXT NOT NULL, record_key TEXT NOT NULL, PRIMARY KEY(view,account,record_key)
        );
    """)
    previous = db.execute("SELECT value FROM processing_meta WHERE name='samplingRule'").fetchone()
    previous = json.loads(previous[0]) if previous else None
    # Adopt legacy file fingerprints without invalidating unchanged saved results.
    legacy = isinstance(previous, str) and len(previous) == 64 and all(char in "0123456789abcdef" for char in previous)
    if previous != SAMPLING_RULE_VERSION and not legacy:
        db.execute("DELETE FROM processing_meta WHERE name LIKE 'samplingState:%'")
        db.execute("DELETE FROM processing_sampled_quota_refs")
        for view in ("local", "merged"):
            db.execute("INSERT OR IGNORE INTO processing_sampling_changes SELECT ?,account,'' FROM processing_facts WHERE kind='quota'", (view,))
    if previous != SAMPLING_RULE_VERSION:
        db.execute("INSERT OR REPLACE INTO processing_meta VALUES('samplingRule',?)", (json.dumps(SAMPLING_RULE_VERSION),))


def changed(db, account, views=("local", "merged"), key=""):
    for view in views:
        db.execute("INSERT OR IGNORE INTO processing_sampling_changes VALUES(?,?,?)", (view, account, key))


def refresh(db, index):
    """Commit point/run projections in the same transaction as fact/checkpoint changes."""
    if _refresh_deferred.get():
        return
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
            # Canonical runs restore saved decisions and never require legacy overlap sampling.
            if any(row.get("quotaRunVersion") != 1 for row in rows):
                points = sample_rows(rows)
                # An overlap, replacement or duplicate timestamp requires replay from retained facts.
                rebuild = any(not point.get("runWindows") and (point["timestamp"] is None or point["timestamp"] <= state["timestamp"]) for point in points)
        if rebuild:
            rows = [json.loads(row[0]) for row in db.execute("SELECT data FROM processing_facts WHERE kind='quota' AND account=?" + source_filter + " ORDER BY source,position", (account,))]
            state = {"timestamp": -1, "windows": {}}
            db.execute("DELETE FROM processing_sampled_quota_refs WHERE view=? AND account=?", (view, account))
            index.metrics["quotaSamplingRebuilds"] = index.metrics.get("quotaSamplingRebuilds", 0) + 1
        points = sample_rows([row | {"usageAccountId": account} for row in rows], states={account: state["windows"]})
        index.metrics["quotaSamplesProcessed"] = index.metrics.get("quotaSamplesProcessed", 0) + len(points)
        for point in points:
            payload_id = samples.put_payload(db, encode(point))
            db.execute("INSERT OR REPLACE INTO processing_sampled_quota_refs VALUES(?,?,?,?,?)", (view, account, point_key(point), point["timestamp"], payload_id))
            if point["timestamp"] is not None:
                state["timestamp"] = max(state["timestamp"], point["timestamp"], *[point[name].get("lastAt", point["timestamp"]) for name in WINDOWS])
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
