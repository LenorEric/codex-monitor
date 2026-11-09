"""Quota observation helpers."""

import json
from contextlib import contextmanager
from contextvars import ContextVar

from monitor_common import coerce_float, parse_timestamp
from monitor_interpretation import quota_point_order

WINDOWS = {"fiveHour": ("5h", 18000), "sevenDay": ("7d", 604800)}
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
            point[name].update({key: window[key] for key in ("multiplier", "firstAt", "lastAt")})
            # Retired JSON stores retain saved decisions for sequential migrations only.
            point[name]["continuous"] = window.get("continuous", raw)
            point[name]["storedRun"] = True
            point.setdefault("runWindows", []).append(name)
    return point


def accepted_windows(state):
    from monitor_interpretation import multiplier
    return {label: {"usedPercent": state[name]["raw"], "resetAt": state[name]["resetAt"], "plan": state[name]["plan"],
        "planMultiplier": multiplier(state[name]["plan"], state[name]["timestamp"]), "percentCheckedAt": state[name]["checkedAt"]}
        for name, (label, _) in WINDOWS.items() if name in state}


def point_key(point):
    return point.get("_key") or (f"{point['checkedAt']}:reset:{point['resetWindow']}" if point.get("resetWindow") else point["checkedAt"])


def stored_points(db, view, account=None):
    query = "SELECT data FROM processing_sampled_quota WHERE view=?"
    values = [view]
    if account is not None:
        query += " AND account=?"
        values.append(account)
    points = [json.loads(row[0]) for row in db.execute(query + " ORDER BY event_at IS NULL,event_at,account,record_key", values)]
    return sorted(points, key=quota_point_order)
