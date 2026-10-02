#!/usr/bin/env python3
"""Disk-backed dashboard rows and exact account-scoped quota dependencies."""

import hashlib
import json
import math
from datetime import datetime, timezone

from monitor_common import parse_timestamp
from monitor_processing import encode


class RankIndex:
    """A counted deterministic treap: exact percentile selection visits O(log n) nodes."""
    def __init__(self, db, index, scope):
        self.db, self.index, self.scope = db, index, scope
        self.root = index.get(db, f"rankRoot:{scope}")

    def node(self, value):
        return self.db.execute("SELECT priority,frequency,size,left_value,right_value FROM processing_rank_nodes WHERE scope=? AND value=?", (self.scope, value)).fetchone() if value is not None else None

    def size(self, value):
        return self.node(value)[2] if value is not None else 0

    def save(self, value, priority, frequency, left, right):
        self.db.execute("INSERT OR REPLACE INTO processing_rank_nodes VALUES(?,?,?,?,?,?,?)", (
            self.scope, value, priority, frequency, frequency + self.size(left) + self.size(right), left, right))

    def insert(self, root, value):
        if root is None:
            self.save(value, int.from_bytes(hashlib.sha256(f"{self.scope}:{value!r}".encode()).digest()[:8], "big") & ((1 << 63) - 1), 1, None, None)
            return value
        priority, frequency, _, left, right = self.node(root)
        if root == value:
            self.save(root, priority, frequency + 1, left, right)
        elif value < root:
            left = self.insert(left, value)
            child = self.node(left)
            if child[0] < priority:
                self.save(root, priority, frequency, child[4], right)
                self.save(left, child[0], child[1], child[3], root)
                return left
            self.save(root, priority, frequency, left, right)
        else:
            right = self.insert(right, value)
            child = self.node(right)
            if child[0] < priority:
                self.save(root, priority, frequency, left, child[3])
                self.save(right, child[0], child[1], root, child[4])
                return right
            self.save(root, priority, frequency, left, right)
        return root

    def add(self, value):
        self.root = self.insert(self.root, value)
        self.index.put(self.db, f"rankRoot:{self.scope}", self.root)

    def percentile(self, label):
        from monitor_dashboard import USAGE_TIME_RATE_FLOORS
        floor = USAGE_TIME_RATE_FLOORS[label]
        count = self.size(self.root)
        if count < 10:
            return floor
        rank, root = int((count - 1) * .9), self.root
        while root is not None:
            node = self.node(root)
            left = self.size(node[3])
            if rank < left:
                root = node[3]
            elif rank < left + node[1]:
                return max(floor, min(floor * 3, root * 2.5))
            else:
                rank -= left + node[1]
                root = node[4]
        raise ValueError("Invalid quota rate rank index")

    def clear(self):
        self.db.execute("DELETE FROM processing_rank_nodes WHERE scope=?", (self.scope,))
        self.root = None
        self.index.put(self.db, f"rankRoot:{self.scope}", None)


class IndexedViews(dict):
    def __init__(self, projector):
        super().__init__({"local": None, "merged": None})
        self.projector = projector

    def __getitem__(self, view):
        return self.projector.snapshot(view)

    def get(self, view, default=None):
        return self[view] if view in self else default


class DashboardProjector:
    def __init__(self, store):
        self.store, self.index = store, store.index
        self.metrics = {"quotaRowsReplayed": 0, "quotaFastAppends": 0, "accountRebuilds": 0, "displayRowsChanged": 0}
        from pathlib import Path
        import monitor_dashboard
        self.rule = hashlib.sha256(Path(__file__).read_bytes() + Path(monitor_dashboard.__file__).read_bytes()).hexdigest()

    def _datasets(self, db, view, account):
        from monitor_usage_sync import merge_quota_rows, merge_token_ledger_rows
        quota, ledger = [], []
        sources = [str(self.store.quota_path.resolve()), str(self.store.token_ledger_path.resolve())]
        for source, kind, data in db.execute("SELECT source,kind,data FROM processing_facts WHERE account=? ORDER BY source,position", (account,)):
            if source not in sources and (view != "merged" or not source.startswith("peer:")):
                continue
            (quota if kind == "quota" else ledger).append(json.loads(data))
        if view == "merged":
            ledger, _ = merge_token_ledger_rows(ledger)
            quota = merge_quota_rows(quota)
        return quota, ledger

    def _row(self, db, view, account, kind, row, patches):
        key, data = row["_key"], encode(row)
        previous = db.execute("SELECT data FROM processing_display WHERE view=? AND kind=? AND record_key=?", (view, kind, key)).fetchone()
        if previous and previous[0] == data:
            return
        stamp = parse_timestamp(row.get("updatedAt")) if kind == "tokenSessions" else row.get("timestamp")
        db.execute("INSERT OR REPLACE INTO processing_display VALUES(?,?,?,?,?,?)", (view, account, kind, key, stamp, data))
        if kind.startswith("costUsage:"):
            db.execute("INSERT OR REPLACE INTO processing_cost_ranges VALUES(?,?,?,?,?,?)", (view, account, kind, key, parse_timestamp(row["startedAt"]), parse_timestamp(row["endedAt"])))
        patches.setdefault(kind, {"upsert": [], "delete": []})["upsert"].append(row)
        self.metrics["displayRowsChanged"] += 1

    def _replace(self, db, view, account, kind, rows, patches):
        desired = {row["_key"] for row in rows}
        for (key,) in list(db.execute("SELECT record_key FROM processing_display WHERE view=? AND account=? AND kind=?", (view, account, kind))):
            if key not in desired:
                db.execute("DELETE FROM processing_display WHERE view=? AND kind=? AND record_key=?", (view, kind, key))
                db.execute("DELETE FROM processing_cost_ranges WHERE view=? AND kind=? AND record_key=?", (view, kind, key))
                patches.setdefault(kind, {"upsert": [], "delete": []})["delete"].append(key)
                self.metrics["displayRowsChanged"] += 1
        for row in rows:
            self._row(db, view, account, kind, row, patches)

    def _rate(self, rank, previous, current, label):
        from monitor_dashboard import USAGE_TIME_ROUNDING_ALLOWANCE
        left, right = previous[label]["raw"], current[label]["raw"]
        if left is not None and right is not None and current["timestamp"] > previous["timestamp"] and right > left:
            rank.add(max(0.0, right - left - USAGE_TIME_ROUNDING_ALLOWANCE) / ((current["timestamp"] - previous["timestamp"]) / 60))

    def _rebuild(self, db, view, account, accounts, now, patches, reason="quota continuity, rate, or ordering change"):
        from monitor_dashboard import _dashboard_display_data, dashboard_quota_points, _usage_time_continuous_values, _same_cost_usage_cycle
        rows, ledger = self._datasets(db, view, account)
        frame = _dashboard_display_data(rows, ledger, accounts, now)
        slots = {str(row["id"]): str(row["usageAccountId"]) for row in accounts.get("items", []) if row.get("usageAccountId")}
        points = sorted(dashboard_quota_points(rows, slots), key=lambda row: (row["timestamp"] is None, row["timestamp"] or 0, row["checkedAt"] or ""))
        db.execute("DELETE FROM processing_quota_points WHERE view=? AND account=?", (view, account))
        for point in points:
            if point["timestamp"] is not None:
                db.execute("INSERT INTO processing_quota_points VALUES(?,?,?,?,?)", (view, account, point["timestamp"], point["checkedAt"], encode(point)))
        state = {"last": points[-1] if points else None, "windows": {}}
        for label in ("fiveHour", "sevenDay"):
            rank = RankIndex(db, self.index, f"{view}:{account}:{label}")
            rank.clear()
            for previous, current in zip(points, points[1:]):
                if previous["timestamp"] is not None and current["timestamp"] is not None:
                    self._rate(rank, previous, current, label)
            records = [{"timestamp": point["timestamp"], "raw": point[label]["raw"], "resetAt": parse_timestamp(point[label].get("resetAt")), "window": point[label]}
                for point in points if point["timestamp"] is not None]
            state["windows"][label] = _usage_time_continuous_values(records, label, rank.percentile(label))
            if records and all(record["raw"] is None for record in records):
                state["windows"][label]["safe"] = True
            turnpoint = None
            for previous, current in zip(points, points[1:]):
                if not _same_cost_usage_cycle(previous, current, label) or current[label]["continuous"] < previous[label]["continuous"]:
                    turnpoint = None
                elif current[label]["continuous"] > previous[label]["continuous"]:
                    turnpoint = [previous, current]
            self.index.put(db, f"turnpoint:{view}:{account}:{label}", turnpoint)
        self.index.put(db, f"quotaState:{view}:{account}", state)
        for kind in ("quotaPoints", "tokenSessions"):
            self._replace(db, view, account, kind, frame[kind], patches)
        for label, values in frame["costUsage"].items():
            self._replace(db, view, account, f"costUsage:{label}", values, patches)
        db.execute("INSERT OR REPLACE INTO processing_projections VALUES(?,?,?)", (view, account, encode({"historyStats": frame["historyStats"], "nextMaintenanceAt": frame["nextMaintenanceAt"]})))
        self.metrics["quotaRowsReplayed"] += len(points) * 2
        self.metrics["accountRebuilds"] += 1
        self.metrics["lastReplay"] = {"view": view, "account": account, "windows": ["fiveHour", "sevenDay"], "reason": reason,
            "quotaRows": len(points), "tokenLedgerRows": len(ledger), "at": now}

    def _fast_quota(self, db, view, account, changes, accounts, now, patches):
        from monitor_dashboard import dashboard_quota_point, _dashboard_key_rows, USAGE_TIME_ROUNDING_ALLOWANCE, dashboard_display_factor, _same_cost_usage_cycle
        state = self.index.get(db, f"quotaState:{view}:{account}")
        if not any(row[0] == "quota" for row in changes):
            return True
        if not state or not state.get("last"):
            return False
        active = next((row for row in accounts.get("items", []) if row.get("id") == accounts.get("activeAccountId")), None)
        if active and active.get("isApiAccount"):
            return False
        points = []
        for kind, key, stamp, session, action in changes:
            if kind != "quota":
                continue
            if action != "append" or stamp is None or stamp <= state["last"]["timestamp"] or dashboard_display_factor(stamp, now) != 1:
                return False
            candidates = [json.loads(row[1]) for row in db.execute("SELECT source,data FROM processing_facts WHERE account=? AND kind='quota' AND record_key=? ORDER BY source,position", (account, key))
                if row[0] == str(self.store.quota_path.resolve()) or view == "merged" and row[0].startswith("peer:")]
            if len(candidates) != 1:
                return False
            points.append(dashboard_quota_point(candidates[0]))
        points.sort(key=lambda row: row["timestamp"])
        if not points:
            return True
        previous = state["last"]
        for point in points:
            for label in ("fiveHour", "sevenDay"):
                left, right = previous[label]["raw"], point[label]["raw"]
                if right is None:
                    point[label]["continuous"] = None
                    if left is not None:
                        state["windows"][label]["safe"] = False
                    continue
                if not state["windows"][label].get("safe") or not 0 <= right <= 100 or left is not None and right < left:
                    return False
                if left is None:
                    continue
                rank = RankIndex(db, self.index, f"{view}:{account}:{label}")
                self._rate(rank, previous, point, label)
                if rank.percentile(label) != state["windows"][label]["rate"]:
                    return False
                same_plan = point[label].get("plan") == previous[label].get("plan")
                if same_plan and right - left > USAGE_TIME_ROUNDING_ALLOWANCE + state["windows"][label]["rate"] * ((point["timestamp"] - previous["timestamp"]) / 60):
                    return False
            previous = point
        # Eligibility is established before writing display rows. A failed fast-path is rebuilt in this transaction.
        previous = state["last"]
        for point in points:
            db.execute("INSERT INTO processing_quota_points VALUES(?,?,?,?,?)", (view, account, point["timestamp"], point["checkedAt"], encode(point)))
            for row in _dashboard_key_rows("quota", [point]):
                self._row(db, view, account, "quotaPoints", row, patches)
            self._tail_cost(db, view, account, previous, point, patches)
            previous = point
        state["last"] = previous
        self.index.put(db, f"quotaState:{view}:{account}", state)
        self.metrics["quotaFastAppends"] += len(points)
        return True

    def _tail_cost(self, db, view, account, previous, point, patches):
        from monitor_dashboard import _same_cost_usage_cycle, dashboard_cost_usage, _dashboard_key_rows
        for label in ("fiveHour", "sevenDay"):
            key = f"turnpoint:{view}:{account}:{label}"
            prior = self.index.get(db, key)
            if not _same_cost_usage_cycle(previous, point, label) or point[label]["continuous"] < previous[label]["continuous"]:
                self.index.put(db, key, None)
                continue
            if point[label]["continuous"] == previous[label]["continuous"]:
                continue
            if prior is not None:
                minimal = prior + [point]
                # Intermediate unchanged quota points matter for the new midpoint.
                if minimal[-2]["checkedAt"] != previous["checkedAt"]:
                    minimal.insert(-1, previous)
                start = (prior[0]["timestamp"] + prior[1]["timestamp"]) / 2
                end = (previous["timestamp"] + point["timestamp"]) / 2
                costs = dashboard_cost_usage(minimal, [])[label]
                for row in _dashboard_key_rows("costUsage", costs[-1:]):
                    row["costByModelUsd"] = self._range_costs(db, view, account, start, end)
                    row["totalCostUsd"] = round(sum(row["costByModelUsd"].values()), 8)
                    row["costPer100PercentUsd"] = round(row["totalCostUsd"] / row["deltaPercent"] * 100, 8)
                    self._row(db, view, account, f"costUsage:{label}", row, patches)
            self.index.put(db, key, [previous, point])

    def _ledger_range(self, db, view, account, start, end):
        if view == "merged":
            return (json.loads(row[0]) for row in db.execute("SELECT data FROM processing_effective WHERE account=? AND event_at>=? AND event_at<? ORDER BY event_at,record_key", (account, start, end)))
        return (json.loads(row[0]) for row in db.execute("SELECT data FROM processing_facts WHERE source=? AND kind='tokenLedger' AND account=? AND event_at>=? AND event_at<? ORDER BY event_at,event_id",
            (str(self.store.token_ledger_path.resolve()), account, start, end)))

    def _range_costs(self, db, view, account, start, end):
        from monitor_common import coerce_float
        result = {}
        for event in self._ledger_range(db, view, account, start, end):
            if event.get("recordType") == "usage" and (cost := coerce_float((event.get("cost") or {}).get("totalCostUsd"))) is not None and cost >= 0:
                model = str(event.get("billingModel") or event.get("rawModel") or "unknown")
                result[model] = round(result.get(model, 0) + cost, 8)
        return result

    def _sessions(self, db, view, account, changes, accounts, patches):
        from monitor_dashboard import dashboard_token_session, _dashboard_key_rows
        source = str(self.store.token_ledger_path.resolve()) if view == "local" else f"view:merged:{account}"
        labels = {str(row["id"]): row["label"] for row in accounts.get("items", [])}
        for session in {item[3] for item in changes if item[0] == "tokenLedger"}:
            rows = []
            for (data,) in db.execute("SELECT data FROM processing_ledger_sessions WHERE source=? AND session=?", (source, session)):
                value = json.loads(data)
                if view == "local" and str(value.get("usageAccountId") or "") != account:
                    continue
                value["accountLabel"] = labels.get(str(value.get("accountSlotId")), value.get("accountLabel"))
                rows.append(dashboard_token_session(value))
            desired = _dashboard_key_rows("token", rows)
            old = list(db.execute("SELECT record_key,data FROM processing_display WHERE view=? AND account=? AND kind='tokenSessions' AND json_extract(data,'$.sessionId')=?", (view, account, session)))
            for key, _ in old:
                if key not in {row["_key"] for row in desired}:
                    db.execute("DELETE FROM processing_display WHERE view=? AND kind='tokenSessions' AND record_key=?", (view, key))
                    patches.setdefault("tokenSessions", {"upsert": [], "delete": []})["delete"].append(key)
            for row in desired:
                self._row(db, view, account, "tokenSessions", row, patches)

    def _stats(self, db, view, account):
        def count(scope, kind):
            return db.execute("SELECT COALESCE(SUM(count),0) FROM processing_counts WHERE scope=? AND account=? AND kind=?", (scope, account, kind)).fetchone()[0]
        return {"quotaRows": count(f"quota:{view}", "quota") if view == "merged" else count(str(self.store.quota_path.resolve()), "quota"),
            "tokenLedgerRows": count("effective" if view == "merged" else str(self.store.token_ledger_path.resolve()), "tokenLedger"),
            "fiveHourCostUsagePoints": count(f"display:{view}", "costUsage:fiveHour"), "sevenDayCostUsagePoints": count(f"display:{view}", "costUsage:sevenDay")}

    def refresh(self, accounts, now, force=False):
        self.store.ingest_local()
        changes = {"local": {}, "merged": {}}
        with self.index.transaction() as db:
            policy = {"activeApi": any(row.get("id") == accounts.get("activeAccountId") and row.get("isApiAccount") for row in accounts.get("items", [])),
                "accounts": [{key: row.get(key) for key in ("id", "label", "usageAccountId")} for row in accounts.get("items", [])]}
            if force or self.index.get(db, "projectionPolicy") != policy or self.index.get(db, "projectionRule") != self.rule:
                for (account,) in list(db.execute("SELECT DISTINCT account FROM processing_facts")):
                    self.index.dirty(db, account, "policy")
                self.index.put(db, "projectionPolicy", policy)
                self.index.put(db, "projectionRule", self.rule)
            for view, account, data in list(db.execute("SELECT view,account,data FROM processing_projections")):
                deadline = json.loads(data).get("nextMaintenanceAt")
                if deadline is not None and now >= deadline:
                    self.index.dirty(db, account, "ageing", (view,))
            dirty = list(db.execute("SELECT view,account,reason FROM processing_dirty"))
            if not dirty:
                return None
            for view, account, reason in dirty:
                rows = list(db.execute("SELECT kind,record_key,event_at,session,action FROM processing_changes WHERE view=? AND account=? ORDER BY event_at,record_key", (view, account)))
                if reason != "records" or not db.execute("SELECT 1 FROM processing_projections WHERE view=? AND account=?", (view, account)).fetchone() or any(row[0] == "tokenLedger" and row[4] != "append" for row in rows) or not self._fast_quota(db, view, account, rows, accounts, now, changes[view]):
                    self._rebuild(db, view, account, accounts, now, changes[view], reason if reason != "records" else "quota continuity, rate, ordering, or initial projection")
                else:
                    self._refresh_cost_intervals(db, view, account, rows, changes[view])
                    self._sessions(db, view, account, rows, accounts, changes[view])
                    metadata = db.execute("SELECT data FROM processing_projections WHERE view=? AND account=?", (view, account)).fetchone()
                    frame = json.loads(metadata[0]) if metadata else {}
                    frame["historyStats"] = self._stats(db, view, account)
                    from monitor_dashboard import _dashboard_display_next_maintenance_at
                    points = [json.loads(row[0]) for row in db.execute("SELECT data FROM processing_quota_points WHERE view=? AND account=? ORDER BY event_at DESC LIMIT 1", (view, account))]
                    deadline = _dashboard_display_next_maintenance_at(points, {"fiveHour": [], "sevenDay": []}, now)
                    deadlines = [value for value in (frame.get("nextMaintenanceAt"), deadline) if value is not None]
                    frame["nextMaintenanceAt"] = min(deadlines, default=None)
                    db.execute("INSERT OR REPLACE INTO processing_projections VALUES(?,?,?)", (view, account, encode(frame)))
                db.execute("DELETE FROM processing_dirty WHERE view=? AND account=?", (view, account))
                db.execute("DELETE FROM processing_changes WHERE view=? AND account=?", (view, account))
            revision = self.index.get(db, "displayRevision", 0) + 1
            self.index.put(db, "displayRevision", revision)
            for view in changes:
                costs = {kind.split(":", 1)[1]: changes[view].pop(kind) for kind in list(changes[view]) if kind.startswith("costUsage:")}
                if costs:
                    changes[view]["costUsage"] = costs
                stats = self.stats(db, view)
                if self.index.get(db, f"displayStats:{view}") != stats:
                    changes[view]["historyStats"] = stats
                    self.index.put(db, f"displayStats:{view}", stats)
            deadlines = [json.loads(row[0]).get("nextMaintenanceAt") for row in db.execute("SELECT data FROM processing_projections")]
            return {"changes": changes, "revision": revision, "nextMaintenanceAt": min((value for value in deadlines if value is not None), default=None)}

    def stats(self, db, view):
        result = {"quotaRows": 0, "tokenLedgerRows": 0, "fiveHourCostUsagePoints": 0, "sevenDayCostUsagePoints": 0}
        for (data,) in db.execute("SELECT data FROM processing_projections WHERE view=?", (view,)):
            for key, value in json.loads(data).get("historyStats", {}).items():
                result[key] = result.get(key, 0) + value
        return result

    def _refresh_cost_intervals(self, db, view, account, changes, patches):
        from monitor_common import coerce_float
        affected = {}
        for kind, _, stamp, _, _ in changes:
            if kind == "tokenLedger" and stamp is not None:
                for name, key, start, end in db.execute("SELECT kind,record_key,start,end FROM processing_cost_ranges WHERE view=? AND account=? AND end>? AND start<=?", (view, account, stamp, stamp)):
                    affected[(name, key)] = (start, end)
        for (kind, key), (start, end) in affected.items():
            row = json.loads(db.execute("SELECT data FROM processing_display WHERE view=? AND kind=? AND record_key=?", (view, kind, key)).fetchone()[0])
            costs = self._range_costs(db, view, account, start, end)
            row["costByModelUsd"] = costs
            row["totalCostUsd"] = round(sum(costs.values()), 8)
            row["costPer100PercentUsd"] = round(row["totalCostUsd"] / row["deltaPercent"] * 100, 8)
            self._row(db, view, account, kind, row, patches)

    def snapshot(self, view, streamed=False):
        from monitor_streaming import Records
        def rows(db, kind):
            order = "COALESCE(event_at,0),json_extract(data,'$.sessionId'),account,record_key" if kind == "tokenSessions" else "event_at IS NULL,event_at,account,record_key"
            return (json.loads(row[0]) for row in db.execute(f"SELECT data FROM processing_display WHERE view=? AND kind=? ORDER BY {order}", (view, kind)))
        if streamed:
            import sqlite3
            db = sqlite3.connect(self.index.path)
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            stats = self.stats(db, view)
            remaining = [4]
            def release():
                remaining[0] -= 1
                if not remaining[0]:
                    db.close()
            return {"quotaPoints": Records(lambda: rows(db, "quotaPoints"), release), "tokenSessions": Records(lambda: rows(db, "tokenSessions"), release),
                "costUsage": {label: Records(lambda label=label: rows(db, f"costUsage:{label}"), release) for label in ("fiveHour", "sevenDay")}, "historyStats": stats}
        result = {"quotaPoints": [], "tokenSessions": [], "costUsage": {label: [] for label in ("fiveHour", "sevenDay")}}
        with self.index.transaction() as db:
            for kind in ("quotaPoints", "tokenSessions", "costUsage:fiveHour", "costUsage:sevenDay"):
                for row in rows(db, kind):
                    (result["costUsage"][kind.split(":", 1)[1]] if kind.startswith("costUsage:") else result[kind]).append(row)
            result["historyStats"] = self.stats(db, view)
        return result
