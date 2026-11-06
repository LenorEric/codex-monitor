"""Canonical quota runs: sample once, then extend the latest compatible window."""

import copy
import hashlib
import json
import math
from datetime import datetime, timezone

from monitor_common import parse_timestamp


def is_run(row):
    return row.get("quotaRunVersion") == 1


def validate_run(row):
    if not is_run(row) or not row.get("windows") or not set(row["windows"]).issubset({"5h", "7d"}):
        raise ValueError("Invalid canonical quota run")
    for window in row["windows"].values():
        first, last = window.get("firstAt"), window.get("lastAt")
        if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) for value in (first, last, window.get("multiplier"))):
            raise ValueError("Invalid quota run timestamps or multiplier")
        if first > last or first != parse_timestamp(row.get("checkedAt")) or window["multiplier"] <= 0:
            raise ValueError("Invalid quota run bounds")
        if "continuous" not in window or isinstance(window["continuous"], bool) or window["continuous"] not in (None, window.get("usedPercent")):
            raise ValueError("Invalid quota run decision")
        if window["continuous"] is None and first != last:
            raise ValueError("Rejected quota observations cannot span multiple observations")


def legacy_tail(db, account, row):
    """Convert only the tail being extended, reusing its saved decisions."""
    if is_run(row):
        return row
    from monitor_sampling import WINDOWS
    point = db.execute("SELECT data FROM processing_sampled_quota WHERE view='local' AND account=? AND record_key=?", (account, row["checkedAt"])).fetchone()
    if point is None:
        return None
    point = json.loads(point[0])
    return row | {"quotaRunVersion": 1, "windows": {label: value | {
        "firstAt": point["timestamp"], "lastAt": point["timestamp"], "continuous": point[name]["continuous"],
        "multiplier": point[name]["multiplier"]}
        for name, (label, _) in WINDOWS.items() if (value := row["windows"].get(label)) is not None}}


def can_extend(previous, current):
    """Same rule as usage_runs, with chronological append eligibility."""
    return (previous["continuous"] is not None and current["continuous"] is not None
        and previous["continuous"] == current["continuous"] and previous["multiplier"] == current["multiplier"]
        and parse_timestamp(previous.get("resetAt")) == parse_timestamp(current.get("resetAt"))
        and current["firstAt"] > previous["lastAt"])


def latest_window(store, db, account, label):
    key = store.index.get(db, f"quotaTail:{account}:{label}")
    if key:
        row = db.execute("SELECT data FROM processing_facts WHERE source='local:quota' AND account=? AND record_key=?", (account, key)).fetchone()
        if row:
            return legacy_tail(db, account, json.loads(row[0]))
    # A retained legacy database or imported backup has no tail checkpoint yet.
    rows = (json.loads(row[0]) for row in db.execute("SELECT data FROM processing_facts WHERE source='local:quota' AND account=?", (account,)))
    row = max((row for row in rows if label in row.get("windows", {})),
        key=lambda row: row["windows"][label].get("lastAt", parse_timestamp(row["checkedAt"])), default=None)
    return legacy_tail(db, account, row) if row else None


def record_runs(store, db, rows):
    from monitor_sampling import WINDOWS, quota_point, sample_point
    from monitor_usage_sync import record_account_key, record_key
    changed, observations = False, []
    for row in rows:
        if is_run(row):
            validate_run(row)
            changed |= store.index.write_records(db, "quota", [row])
            continue
        account = record_account_key(row)
        observed = store.index.get(db, f"quotaRecorderObserved:{account}", {})
        row = row | {"windows": {label: window for label, window in row["windows"].items()
            if observed.get(label) != [row["checkedAt"], window]}}
        if not row["windows"]:
            continue
        observations.append(row)
        for label, window in row["windows"].items():
            observed[label] = [row["checkedAt"], window]
        store.index.put(db, f"quotaRecorderObserved:{account}", observed)
        state_key = f"quotaRecorderState:{account}"
        state = store.index.get(db, state_key)
        if state is None:
            state = copy.deepcopy(store.index.get(db, f"samplingState:local:{account}", {"windows": {}})["windows"])
        point, resets = quota_point(row), []
        sample_point(point, state, resets)
        store.index.put(db, state_key, state)
        store.index.put(db, f"quotaObservation:{account}", point)
        for observation in sorted(resets + [point], key=lambda value: value["timestamp"]):
            for name, (label, _) in WINDOWS.items():
                if label not in row["windows"] or observation.get("resetWindow") not in (None, name):
                    continue
                value = observation[name]
                if value["raw"] is None:
                    continue
                stamp = observation["timestamp"]
                window = {"usedPercent": value["raw"], "resetAt": value.get("resetAt"), "plan": value["plan"],
                    "firstAt": stamp, "lastAt": stamp, "continuous": value["continuous"],
                    "multiplier": value.get("multiplier", point[name]["multiplier"])}
                previous = latest_window(store, db, account, label)
                if previous and can_extend(previous["windows"][label], window):
                    previous["windows"][label]["lastAt"] = stamp
                    canonical = previous
                else:
                    checked = datetime.fromtimestamp(stamp, timezone.utc).isoformat()
                    identity = f"quota-run:{account}:{label}:{checked}:{window['usedPercent']}"
                    canonical = row | {"quotaRunVersion": 1, "checkedAt": checked, "windows": {label: window},
                        "sync": row["sync"] | {"recordId": hashlib.sha256(identity.encode()).hexdigest()}}
                changed |= store.index.write_records(db, "quota", [canonical])
                if previous is None or stamp >= previous["windows"][label]["lastAt"]:
                    store.index.put(db, f"quotaTail:{account}:{label}", record_key("quota", canonical))
    return changed, observations


def restore_window(point, name, window, state):
    """Consume the saved sampling decision, never sample a run again."""
    point[name] = {"raw": window["raw"], "continuous": window["continuous"], "resetAt": window.get("resetAt"),
        "plan": window["plan"], "multiplier": window["multiplier"], "firstAt": window["firstAt"], "lastAt": window["lastAt"], "storedRun": True}
    if window["continuous"] is None:
        if name in state:
            state[name]["protection"] = window["raw"]
    else:
        if name not in state or window["lastAt"] >= state[name]["timestamp"]:
            state[name] = {"raw": window["raw"], "timestamp": window["lastAt"],
                "checkedAt": datetime.fromtimestamp(window["lastAt"], timezone.utc).isoformat(), "resetAt": window.get("resetAt"), "plan": window["plan"]}
