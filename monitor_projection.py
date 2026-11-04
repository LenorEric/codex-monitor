#!/usr/bin/env python3
"""Prepare account-scoped quota points and forward ingestion work to delivery."""

import json

from monitor_processing import encode

PROJECTION_RULE_VERSION = 1


class DashboardProjector:
    def __init__(self, store):
        self.store, self.index = store, store.index
        self.metrics = {"quotaRowsReplayed": 0, "quotaFastAppends": 0, "accountRebuilds": 0}
        self.rule = PROJECTION_RULE_VERSION

    def _rebuild(self, db, view, account, now, reason="quota ordering or source change"):
        from monitor_sampling import point_key, stored_points
        points = stored_points(db, view, account)
        db.execute("DELETE FROM processing_quota_points WHERE view=? AND account=?", (view, account))
        for point in points:
            if point["timestamp"] is not None:
                db.execute("INSERT INTO processing_quota_points VALUES(?,?,?,?,?)", (view, account, point["timestamp"], point_key(point), encode(point)))
        self.index.put(db, f"quotaState:{view}:{account}", {"last": points[-1] if points else None})
        db.execute("INSERT OR REPLACE INTO processing_projections VALUES(?,?,?)", (view, account, encode({})))
        self.metrics["quotaRowsReplayed"] += len(points) * 2
        self.metrics["accountRebuilds"] += 1
        self.metrics["lastReplay"] = {"view": view, "account": account, "windows": ["fiveHour", "sevenDay"], "reason": reason, "quotaRows": len(points), "at": now}

    def _fast_quota(self, db, view, account, changes):
        state = self.index.get(db, f"quotaState:{view}:{account}")
        if not any(row[0] == "quota" for row in changes):
            return True
        if not state or not state.get("last"):
            return False
        points = []
        for kind, key, stamp, session, action in changes:
            if kind != "quota":
                continue
            if action not in {"append", "extend"} or stamp is None or action == "append" and stamp < state["last"]["timestamp"]:
                return False
            candidates = [json.loads(row[1]) for row in db.execute("SELECT source,data FROM processing_facts WHERE account=? AND kind='quota' AND record_key=? ORDER BY source,position", (account, key))
                if row[0] == "local:quota" or view == "merged" and row[0].startswith("peer:")]
            if len(candidates) != 1:
                return False
            sampled = db.execute("SELECT data FROM processing_sampled_quota WHERE view=? AND account=? AND record_key=?", (view, account, key if candidates[0].get("quotaRunVersion") == 1 else candidates[0]["checkedAt"])).fetchone()
            if sampled is None:
                return False
            points.append(json.loads(sampled[0]))
        points.sort(key=lambda row: row["timestamp"])
        if not points:
            return True
        # A reset can insert earlier, window-specific points; replay the account to preserve sibling intervals.
        if any(point.get("resetWindows") for point in points):
            return False
        # Eligibility is established before writing quota points. A failed fast-path is rebuilt in this transaction.
        for point in points:
            from monitor_sampling import point_key
            db.execute("INSERT OR REPLACE INTO processing_quota_points VALUES(?,?,?,?,?)", (view, account, point["timestamp"], point_key(point), encode(point)))
        state["last"] = points[-1]
        self.index.put(db, f"quotaState:{view}:{account}", state)
        self.metrics["quotaFastAppends"] += len(points)
        return True

    def refresh(self, accounts, now, force=False):
        self.store.ingest_local()
        with self.index.transaction() as db:
            policy = {"activeApi": any(row.get("id") == accounts.get("activeAccountId") and row.get("isApiAccount") for row in accounts.get("items", [])),
                "accounts": [{key: row.get(key) for key in ("id", "label", "usageAccountId")} for row in accounts.get("items", [])]}
            previous_rule = self.index.get(db, "projectionRule")
            legacy_rule = isinstance(previous_rule, str) and len(previous_rule) == 64 and all(char in "0123456789abcdef" for char in previous_rule)
            if force or self.index.get(db, "projectionPolicy") != policy or (previous_rule != self.rule and not legacy_rule):
                for (account,) in list(db.execute("SELECT DISTINCT account FROM processing_facts")):
                    self.index.dirty(db, account, "policy")
                self.index.put(db, "projectionPolicy", policy)
            if previous_rule != self.rule:
                self.index.put(db, "projectionRule", self.rule)
            dirty = list(db.execute("SELECT view,account,reason FROM processing_dirty"))
            if not dirty:
                return None
            for view, account, reason in dirty:
                rows = list(db.execute("SELECT kind,record_key,event_at,session,action FROM processing_changes WHERE view=? AND account=? ORDER BY event_at,record_key", (view, account)))
                db.execute("INSERT OR IGNORE INTO delivery_dirty VALUES(?,?)", (view, account))
                db.executemany("INSERT OR IGNORE INTO delivery_work VALUES(?,?,?,?)", [(view, account, row[0], row[1]) for row in rows])
                if reason != "records" or not db.execute("SELECT 1 FROM processing_projections WHERE view=? AND account=?", (view, account)).fetchone() or not self._fast_quota(db, view, account, rows):
                    self._rebuild(db, view, account, now, reason if reason != "records" else "quota ordering, source change, or initial projection")
                db.execute("DELETE FROM processing_dirty WHERE view=? AND account=?", (view, account))
                db.execute("DELETE FROM processing_changes WHERE view=? AND account=?", (view, account))
            return True
