"""Incremental, immutable dashboard delivery artifacts. Canonical histories are untouched."""

import gzip
import hashlib
import json
import math
import sqlite3
import threading
import time
from collections import OrderedDict, defaultdict
from contextlib import contextmanager

from monitor_common import parse_timestamp

PROTOCOL_VERSION = 4
LEVELS = (0, 300, 1800, 7200, 86400)
RANGES = {"5h": 18000, "24h": 86400, "7d": 604800, "30d": 2592000, "All": None}
DAY = 86400


def initialize_delivery(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS dashboard_days(view TEXT,day INTEGER,revision INTEGER NOT NULL,dirty INTEGER NOT NULL,PRIMARY KEY(view,day));
        CREATE TABLE IF NOT EXISTS dashboard_parts(view TEXT,day INTEGER,level INTEGER,digest TEXT NOT NULL,PRIMARY KEY(view,day,level));
        CREATE TABLE IF NOT EXISTS dashboard_day_meta(view TEXT,day INTEGER,data TEXT NOT NULL,PRIMARY KEY(view,day));
        CREATE TABLE IF NOT EXISTS dashboard_blobs(digest TEXT PRIMARY KEY,body BLOB NOT NULL,compressed BLOB NOT NULL,created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dashboard_manifests(scope TEXT PRIMARY KEY,signature TEXT NOT NULL,data TEXT NOT NULL,updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS dashboard_rollups(signature TEXT PRIMARY KEY,digest TEXT NOT NULL,created REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS dashboard_quota_time ON processing_quota_points(view,event_at,account);
        CREATE INDEX IF NOT EXISTS dashboard_display_time ON processing_display(view,kind,event_at,account);
    """)
    for table, condition in (("processing_quota_points", "1"), ("processing_display", "{row}.kind!='quotaPoints'")):
        for action, rows in (("INSERT", ("NEW",)), ("DELETE", ("OLD",)), ("UPDATE", ("OLD", "NEW"))):
            body = "".join(f"""
                INSERT INTO dashboard_days SELECT {row}.view,CAST(COALESCE({row}.event_at,0)/86400 AS INTEGER),1,1 WHERE {condition.format(row=row)}
                ON CONFLICT(view,day) DO UPDATE SET revision=revision+1,dirty=1;
            """ for row in rows)
            db.executescript(f"CREATE TRIGGER IF NOT EXISTS dashboard_{table}_{action.lower()} AFTER {action} ON {table} BEGIN {body} END;")
    if not db.execute("SELECT 1 FROM processing_meta WHERE name='dashboardDeliveryInitialized'").fetchone():
        for table, condition in (("processing_quota_points", "1"), ("processing_display", "kind!='quotaPoints'")):
            db.execute(f"INSERT OR IGNORE INTO dashboard_days SELECT DISTINCT view,CAST(COALESCE(event_at,0)/86400 AS INTEGER),1,1 FROM {table} WHERE {condition}")
        db.execute("INSERT INTO processing_meta VALUES('dashboardDeliveryInitialized','true')")


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def empty_view():
    return {"quotaPoints": [], "tokenSessions": [], "costUsage": {"fiveHour": [], "sevenDay": []}, "historyStats": {}}


def session_groups(rows):
    """Group by the complete model set so arbitrary model selection keeps exact session counts."""
    groups = {}
    for row in rows:
        models = tuple(sorted((row.get("byModel") or {}).keys()))
        key = (row.get("usageAccountId") or row.get("accountSlotId"), models)
        if key not in groups:
            groups[key] = {k: row.get(k) for k in ("usageAccountId", "accountSlotId", "accountLabel", "startedAt", "updatedAt")}
            groups[key].update({"byModel": {}, "sessionCount": 0})
            groups[key]["_key"] = "summary:" + hashlib.sha256(encoded(key)).hexdigest()
        group = groups[key]
        group["sessionCount"] += row.get("sessionCount", 1)
        for name in ("startedAt", "updatedAt"):
            values = [value for value in (group.get(name), row.get(name)) if value]
            group[name] = (min(values) if name == "startedAt" else max(values)) if values else None
        for model, value in (row.get("byModel") or {}).items():
            target = group["byModel"].setdefault(model, {"usageTokens": {}, "cost": {}})
            for field, source in (("usageTokens", value.get("usageTokens") or value.get("tokens") or {}), ("cost", value.get("cost") or {})):
                for name, amount in source.items():
                    if isinstance(amount, (float, int)):
                        target[field][name] = target[field].get(name, 0) + amount
    return list(groups.values())


def reduce_quota(rows, seconds):
    if not seconds:
        return rows
    buckets, retained = defaultdict(list), {}
    by_account = defaultdict(list)
    for row in rows:
        by_account[row["usageAccountId"]].append(row)
    for points in by_account.values():
        segment = 0
        buckets[(points[0]["usageAccountId"], int(points[0]["timestamp"] // seconds), segment)].append(points[0])
        for index, (left, right) in enumerate(zip(points, points[1:])):
            covered = any((row.get("overviewBucket") or {}).get("start", math.inf) <= left["timestamp"] and right["timestamp"] <= (row.get("overviewBucket") or {}).get("end", -math.inf) for row in (left, right))
            changed = right["timestamp"] - left["timestamp"] > 4 * 3600 and not covered
            for label in ("fiveHour", "sevenDay"):
                a, b = left.get(label) or {}, right.get(label) or {}
                changed |= any(a.get(k) != b.get(k) for k in ("resetAt", "plan"))
                changed |= (a.get("continuous") is None) != (b.get("continuous") is None)
                changed |= a.get("continuous") is not None and b.get("continuous") is not None and b["continuous"] < a["continuous"]
                # Keep the ends of flat runs without retaining every observation inside them.
                if a.get("continuous") == b.get("continuous"):
                    if index == 0 or (points[index - 1].get(label) or {}).get("continuous") != a.get("continuous"):
                        retained[left["_key"]] = left
                    if index + 2 == len(points) or (points[index + 2].get(label) or {}).get("continuous") != b.get("continuous"):
                        retained[right["_key"]] = right
            if changed:
                retained[left["_key"]], retained[right["_key"]] = left, right
                segment += 1
            buckets[(right["usageAccountId"], int(right["timestamp"] // seconds), segment)].append(right)
    for points in buckets.values():
        keep = {0, len(points) - 1}
        for label in ("fiveHour", "sevenDay"):
            for field in ("raw", "continuous"):
                candidates = [i for i, row in enumerate(points) if isinstance((row.get(label) or {}).get(field), (float, int))]
                if candidates:
                    keep.update((min(candidates, key=lambda i: points[i][label][field]), max(candidates, key=lambda i: points[i][label][field])))
        for i in keep:
            retained[points[i]["_key"]] = points[i] | {"overviewBucket": {"start": min((row.get("overviewBucket") or {}).get("start", row["timestamp"]) for row in points),
                "end": max((row.get("overviewBucket") or {}).get("end", row["timestamp"]) for row in points), "count": len(points)}}
    return sorted(retained.values(), key=lambda row: (row["timestamp"], row["usageAccountId"], row["_key"]))


def reduce_cost(rows, seconds):
    if not seconds:
        return rows
    groups, previous, segments = {}, {}, defaultdict(int)
    for row in rows:
        account = row.get("usageAccountId")
        if account in previous and (previous[account].get("endedAt") != row.get("startedAt") or previous[account].get("plan") != row.get("plan")):
            segments[account] += 1
        previous[account] = row
        key = (account, row.get("plan"), int(row["timestamp"] // seconds), segments[account])
        if key not in groups:
            groups[key] = row | {"costByModelUsd": dict(row.get("costByModelUsd") or {}), "mergedCount": 1}
            continue
        value = groups[key]
        value["mergedCount"] += 1
        value["startedAt"] = min(value["startedAt"], row["startedAt"])
        value["endedAt"] = max(value["endedAt"], row["endedAt"])
        value["deltaPercent"] += row.get("deltaPercent", 0)
        value["totalCostUsd"] += row.get("totalCostUsd", 0)
        for model, amount in (row.get("costByModelUsd") or {}).items():
            value["costByModelUsd"][model] = value["costByModelUsd"].get(model, 0) + amount
        value["costPer100PercentUsd"] = value["totalCostUsd"] / value["deltaPercent"] * 100 if value["deltaPercent"] else 0
    return sorted(groups.values(), key=lambda row: (row["timestamp"], row.get("usageAccountId") or ""))


def cost_groups(rows):
    groups = {}
    for row in rows:
        key = row.get("usageAccountId") or row.get("accountSlotId")
        if key not in groups:
            groups[key] = {name: row.get(name) for name in ("usageAccountId", "accountSlotId", "accountLabel", "window")}
            groups[key].update({"deltaPercent": 0, "totalCostUsd": 0, "costByModelUsd": {}, "usageByModel": {}})
        group = groups[key]
        group["deltaPercent"] += row.get("deltaPercent", 0)
        group["totalCostUsd"] += row.get("totalCostUsd", 0)
        for model, amount in (row.get("costByModelUsd") or {}).items():
            group["costByModelUsd"][model] = group["costByModelUsd"].get(model, 0) + amount
            usage = (row.get("usageByModel") or {}).get(model, row.get("deltaPercent", 0) * amount / row["totalCostUsd"] if row.get("totalCostUsd", 0) > 0 else 0)
            group["usageByModel"][model] = group["usageByModel"].get(model, 0) + usage
    return list(groups.values())


class DashboardDelivery:
    def __init__(self, index):
        self.index = index
        self.prepare_lock = threading.Lock()
        self.cache_lock = threading.Lock()
        self.day_access = OrderedDict()
        self.metrics = {"daysPrepared": 0, "chunksEncoded": 0, "prepareSeconds": 0}

    @contextmanager
    def reader(self):
        db = sqlite3.connect(self.index.path.resolve().as_uri() + "?mode=ro", uri=True, timeout=10)
        try:
            db.execute("BEGIN")
            yield db
        finally:
            db.close()

    def _load(self, db, view, start, end):
        result = empty_view()
        for account, key, data in db.execute("SELECT account,record_key,data FROM processing_quota_points WHERE view=? AND event_at>=? AND event_at<? ORDER BY event_at,account,record_key", (view, start, end)):
            row = json.loads(data)
            row["_key"] = f"quota:{account}:{key}"
            result["quotaPoints"].append(row)
        for kind in ("tokenSessions", "costUsage:fiveHour", "costUsage:sevenDay"):
            rows = [json.loads(row[0]) for row in db.execute("SELECT data FROM processing_display WHERE view=? AND kind=? AND event_at>=? AND event_at<? ORDER BY event_at,account,record_key", (view, kind, start, end))]
            if kind == "tokenSessions":
                result[kind] = rows
            else:
                result["costUsage"][kind.split(":")[1]] = rows
        return result

    def prepare(self, now=None):
        now = time.time() if now is None else now
        started = time.perf_counter()
        with self.prepare_lock:
            with self.reader() as db:
                dirty = list(db.execute("SELECT view,day,revision FROM dashboard_days WHERE dirty=1 ORDER BY day DESC,view"))
            for view, day, revision in dirty:
                with self.reader() as db:
                    # Capture the source rows and claim from one committed generation.
                    claim = db.execute("SELECT revision FROM dashboard_days WHERE view=? AND day=?", (view, day)).fetchone()[0]
                    exact = self._load(db, view, day * DAY, (day + 1) * DAY)
                meta = {"tokens": session_groups(exact["tokenSessions"]), "quotaMax": max((r["timestamp"] for r in exact["quotaPoints"]), default=None),
                    "costMax": max((r["timestamp"] for values in exact["costUsage"].values() for r in values), default=None),
                    "tokenMax": max((parse_timestamp(r.get("updatedAt") or r.get("startedAt")) for r in exact["tokenSessions"]), default=None),
                    "rates": {name: cost_groups(rows) for name, rows in exact["costUsage"].items()}}
                artifacts = []
                for level in LEVELS:
                    frame = empty_view()
                    frame["quotaPoints"] = reduce_quota(exact["quotaPoints"], level)
                    frame["costUsage"] = {name: reduce_cost(rows, level) for name, rows in exact["costUsage"].items()}
                    # Encoding and compression happen outside the database writer lock.
                    from monitor_dashboard import dashboard_safe_json
                    body = encoded(dashboard_safe_json(frame))
                    artifacts.append((level, hashlib.sha256(body).hexdigest(), body, gzip.compress(body, compresslevel=3, mtime=0)))
                with self.index.transaction() as db:
                    if db.execute("SELECT revision FROM dashboard_days WHERE view=? AND day=?", (view, day)).fetchone()[0] != claim:
                        continue
                    for level, digest, body, compressed in artifacts:
                        db.execute("INSERT OR IGNORE INTO dashboard_blobs VALUES(?,?,?,?)", (digest, body, compressed, now))
                        db.execute("INSERT OR REPLACE INTO dashboard_parts VALUES(?,?,?,?)", (view, day, level, digest))
                    db.execute("INSERT OR REPLACE INTO dashboard_day_meta VALUES(?,?,?)", (view, day, encoded(meta).decode()))
                    db.execute("UPDATE dashboard_days SET dirty=0 WHERE view=? AND day=? AND revision=?", (view, day, claim))
                self.metrics["daysPrepared"] += 1
                self.metrics["chunksEncoded"] += len(LEVELS)
            for view in ("local", "merged"):
                self._publish(view, now)
            # Keep previous generations available for downloads already in flight.
            with self.index.transaction() as db:
                db.execute("DELETE FROM dashboard_manifests WHERE scope LIKE '%:Date:%' AND updated<?", (now - DAY,))
                references = set(row[0] for row in db.execute("SELECT digest FROM dashboard_parts"))
                for (data,) in db.execute("SELECT data FROM dashboard_manifests"):
                    references.update(json.loads(data).get("chunks", []))
                for signature, digest in list(db.execute("SELECT signature,digest FROM dashboard_rollups WHERE created<?", (now - DAY,))):
                    if digest not in references:
                        db.execute("DELETE FROM dashboard_rollups WHERE signature=?", (signature,))
                references.update(row[0] for row in db.execute("SELECT digest FROM dashboard_rollups"))
                for (digest,) in list(db.execute("SELECT digest FROM dashboard_blobs WHERE created<?", (now - DAY,))):
                    if digest not in references:
                        db.execute("DELETE FROM dashboard_blobs WHERE digest=?", (digest,))
            self.metrics["prepareSeconds"] = round(time.perf_counter() - started, 4)

    def _publish(self, view, now):
        with self.reader() as db:
            if db.execute("SELECT 1 FROM dashboard_days WHERE view=? AND dirty=1 LIMIT 1", (view,)).fetchone():
                return
            days = [(day, revision, json.loads(data)) for day, revision, data in db.execute(
                "SELECT d.day,d.revision,m.data FROM dashboard_days d JOIN dashboard_day_meta m USING(view,day) WHERE d.view=? ORDER BY d.day", (view,))]
            signature = hashlib.sha256(encoded(([(day, revision) for day, revision, _ in days], int(now // 3600)))).hexdigest()
            current = list(db.execute("SELECT signature FROM dashboard_manifests WHERE scope IN (?,?,?,?,?)", tuple(f"{view}:{selected}" for selected in RANGES)))
            if len(current) == len(RANGES) and all(row[0] == signature for row in current):
                return
            maxima = {name: max((meta[name] for _, _, meta in days if meta[name] is not None), default=None) for name in ("quotaMax", "costMax", "tokenMax")}
            manifests = []
            for selected, seconds in RANGES.items():
                chunks, tokens = [], []
                rates = {"fiveHour": [], "sevenDay": []}
                rollups = defaultdict(list)
                bounds = {name: ((stamp - seconds) if seconds and stamp is not None else None) for name, stamp in maxima.items()}
                for day, _, meta in days:
                    if seconds and not any(stamp is not None and day * DAY <= stamp and (day + 1) * DAY > stamp - seconds for stamp in maxima.values()):
                        continue
                    age = now - (day + 1) * DAY
                    level = 0 if age <= DAY else 300 if age <= 7 * DAY else 1800 if age <= 30 * DAY else 7200
                    if selected in {"5h", "24h"}:
                        level = 0
                    if selected == "All" and len(days) > 365 and age > 30 * DAY:
                        level = max(level, 86400)
                    part = db.execute("SELECT digest FROM dashboard_parts WHERE view=? AND day=? AND level=?", (view, day, level)).fetchone()
                    if part:
                        if selected == "All" and level == 86400:
                            # Stable partitions span days only for very old, long histories.
                            width = 7 if len(days) <= 2000 else 30 if len(days) <= 10000 else 180
                            rollups[(day // width, width)].append(part[0])
                        else:
                            chunks.append(part[0])
                    if bounds["tokenMax"] is None or day * DAY >= bounds["tokenMax"]:
                        tokens.extend(meta["tokens"])
                    elif (day + 1) * DAY > bounds["tokenMax"]:
                        tokens.extend(self._load_tokens(db, view, bounds["tokenMax"], (day + 1) * DAY))
                    for name in rates:
                        if bounds["costMax"] is None or day * DAY >= bounds["costMax"]:
                            rates[name].extend(meta["rates"][name])
                        elif (day + 1) * DAY > bounds["costMax"]:
                            rates[name].extend(json.loads(row[0]) for row in db.execute(
                                "SELECT data FROM processing_display WHERE view=? AND kind=? AND event_at>=? AND event_at<?", (view, f"costUsage:{name}", bounds["costMax"], (day + 1) * DAY)))
                for (_, width), digests in sorted(rollups.items()):
                    chunks.append(self._rollup(db, digests, width * DAY, now))
                manifests.append((f"{view}:{selected}", signature, self._manifest(view, selected, chunks, session_groups(tokens), maxima, now,
                    rates={name: cost_groups(rows) for name, rows in rates.items()})))
        with self.index.transaction() as db:
            # If a projection committed while encoding, leave the old published generation intact.
            actual = list(db.execute("SELECT day,revision FROM dashboard_days WHERE view=? ORDER BY day", (view,)))
            if actual != [(day, revision) for day, revision, _ in days] or db.execute("SELECT 1 FROM dashboard_days WHERE view=? AND dirty=1 LIMIT 1", (view,)).fetchone():
                return
            for scope, signature, manifest in manifests:
                db.execute("INSERT OR REPLACE INTO dashboard_manifests VALUES(?,?,?,?)", (scope, signature, encoded(manifest).decode(), now))

    def _rollup(self, reader, digests, seconds, now):
        signature = hashlib.sha256(encoded([digests, seconds])).hexdigest()
        cached = reader.execute("SELECT r.digest FROM dashboard_rollups r JOIN dashboard_blobs b USING(digest) WHERE r.signature=?", (signature,)).fetchone()
        if cached:
            return cached[0]
        frame = empty_view()
        for digest in digests:
            part = json.loads(reader.execute("SELECT body FROM dashboard_blobs WHERE digest=?", (digest,)).fetchone()[0])
            frame["quotaPoints"].extend(part["quotaPoints"])
            for name in frame["costUsage"]:
                frame["costUsage"][name].extend(part["costUsage"][name])
        frame["quotaPoints"] = reduce_quota(sorted(frame["quotaPoints"], key=lambda row: (row["timestamp"], row["usageAccountId"])), seconds)
        frame["costUsage"] = {name: reduce_cost(sorted(rows, key=lambda row: row["timestamp"]), seconds) for name, rows in frame["costUsage"].items()}
        body = encoded(frame)
        digest = hashlib.sha256(body).hexdigest()
        compressed = gzip.compress(body, compresslevel=3, mtime=0)
        with self.index.transaction() as db:
            db.execute("INSERT OR IGNORE INTO dashboard_blobs VALUES(?,?,?,?)", (digest, body, compressed, now))
            db.execute("INSERT OR REPLACE INTO dashboard_rollups VALUES(?,?,?)", (signature, digest, now))
        self.metrics["chunksEncoded"] += 1
        return digest

    @staticmethod
    def _load_tokens(db, view, start, end):
        return [json.loads(row[0]) for row in db.execute("SELECT data FROM processing_display WHERE view=? AND kind='tokenSessions' AND event_at>=? AND event_at<?", (view, start, end))]

    @staticmethod
    def _manifest(view, selected, chunks, tokens, maxima, now, bounds=None, rates=None):
        value = {"protocolVersion": PROTOCOL_VERSION, "view": view, "range": selected, "chunks": list(dict.fromkeys(chunks)), "tokenSessions": tokens,
            "maxima": maxima, "bounds": bounds, "builtAt": now, "rateRows": rates or {"fiveHour": [], "sevenDay": []}}
        value["generation"] = hashlib.sha256(encoded(value | {"builtAt": 0})).hexdigest()
        return value

    def manifest(self, view, selected, start=None, end=None):
        if view not in {"local", "merged"} or selected not in {*RANGES, "Date"}:
            raise ValueError("Unknown dashboard view or range")
        if selected != "Date":
            with self.reader() as db:
                row = db.execute("SELECT data FROM dashboard_manifests WHERE scope=?", (f"{view}:{selected}",)).fetchone()
                return json.loads(row[0]) if row else None
        if start is None or end is None or not all(math.isfinite(value) for value in (start, end)) or not 0 < end - start <= 26 * 3600:
            raise ValueError("Expected finite start/end instants for one local calendar day")
        scope = f"{view}:Date:{start}:{end}"
        with self.reader() as db:
            if not db.execute("SELECT 1 FROM processing_meta WHERE name='displayRevision'").fetchone() and db.execute("SELECT 1 FROM processing_facts LIMIT 1").fetchone():
                return None
            revisions = list(db.execute("SELECT day,revision FROM dashboard_days WHERE view=? AND day>=? AND day<=? ORDER BY day", (view, int(start // DAY), int((end - .001) // DAY))))
            context = []
            accounts = list(db.execute("SELECT account FROM processing_projections WHERE view=?", (view,)))
            if not accounts:
                accounts = list(db.execute("SELECT DISTINCT account FROM processing_quota_points WHERE view=?", (view,)))
            for (account,) in accounts:
                for comparison, boundary, order in (("<", start, "DESC"), (">=", end, "ASC")):
                    row = db.execute(f"SELECT record_key,data FROM processing_quota_points WHERE view=? AND account=? AND event_at{comparison}? ORDER BY event_at {order} LIMIT 1", (view, account, boundary)).fetchone()
                    if row:
                        context.append(json.loads(row[1]) | {"_key": f"quota:{account}:{row[0]}", "contextOnly": True})
            signature = hashlib.sha256(encoded([revisions, context])).hexdigest()
            cached = db.execute("SELECT signature,data FROM dashboard_manifests WHERE scope=?", (scope,)).fetchone()
            if cached and cached[0] == signature:
                with self.cache_lock:
                    self.day_access.pop(scope, None)
                    self.day_access[scope] = time.time()
                    while len(self.day_access) > 32:
                        self.day_access.popitem(last=False)
                return json.loads(cached[1])
            frame = self._load(db, view, start, end)
            frame["quotaPoints"] = sorted(frame["quotaPoints"] + context, key=lambda row: (row["timestamp"], row["usageAccountId"], row["_key"]))
        tokens = session_groups(frame.pop("tokenSessions"))
        rates = {name: cost_groups(rows) for name, rows in frame["costUsage"].items()}
        frame["tokenSessions"] = []
        # Bound individual immutable responses even for an unusually dense day.
        artifacts = []
        from monitor_dashboard import dashboard_safe_json
        for kind, rows in (("quotaPoints", frame["quotaPoints"]), ("fiveHour", frame["costUsage"]["fiveHour"]), ("sevenDay", frame["costUsage"]["sevenDay"])):
            for offset in range(0, len(rows), 1000):
                part = empty_view()
                (part if kind == "quotaPoints" else part["costUsage"])[kind] = rows[offset:offset + 1000]
                body = encoded(dashboard_safe_json(part))
                artifacts.append((hashlib.sha256(body).hexdigest(), body, gzip.compress(body, compresslevel=3, mtime=0)))
        now = time.time()
        with self.index.transaction() as db:
            for digest, body, compressed in artifacts:
                db.execute("INSERT OR IGNORE INTO dashboard_blobs VALUES(?,?,?,?)", (digest, body, compressed, now))
            manifest = self._manifest(view, selected, [digest for digest, _, _ in artifacts], tokens, {}, now, [start, end], rates)
            db.execute("INSERT OR REPLACE INTO dashboard_manifests VALUES(?,?,?,?)", (scope, signature, encoded(manifest).decode(), now))
            # Access tracking is in memory so a warm day request remains read-only.
            with self.cache_lock:
                self.day_access.pop(scope, None)
                self.day_access[scope] = now
                scopes = list(db.execute("SELECT scope,updated FROM dashboard_manifests WHERE scope LIKE '%:Date:%'"))
                for name, _ in sorted(scopes, key=lambda row: self.day_access.get(row[0], row[1]), reverse=True)[32:]:
                    db.execute("DELETE FROM dashboard_manifests WHERE scope=?", (name,))
                    self.day_access.pop(name, None)
        return manifest

    def blob(self, digest, compressed):
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("Invalid dashboard chunk identity")
        with self.reader() as db:
            row = db.execute(f"SELECT {'compressed' if compressed else 'body'} FROM dashboard_blobs WHERE digest=?", (digest,)).fetchone()
        if row:
            return row[0]
        # A lost derived artifact must be repairable without deleting canonical data or restarting.
        with self.index.transaction() as db:
            db.execute("UPDATE dashboard_days SET dirty=1,revision=revision+1 WHERE (view,day) IN (SELECT view,day FROM dashboard_parts WHERE digest=?)", (digest,))
            db.execute("DELETE FROM dashboard_rollups WHERE digest=?", (digest,))
            db.execute("DELETE FROM dashboard_manifests WHERE EXISTS(SELECT 1 FROM json_each(dashboard_manifests.data,'$.chunks') WHERE value=?)", (digest,))
        return None
