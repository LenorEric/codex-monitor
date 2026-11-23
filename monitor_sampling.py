"""Quota observation helpers."""

from monitor_common import coerce_float, parse_timestamp

WINDOWS = {"fiveHour": ("5h", 18000), "sevenDay": ("7d", 604800)}


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
