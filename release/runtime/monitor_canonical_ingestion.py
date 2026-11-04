"""Incremental source checkpoints, with parsed events retained only until commit."""

import base64
import hashlib
import json
import threading
from pathlib import Path

from monitor_common import empty_token_totals
from monitor_sources import SOURCE_READ_BYTES, serialized
from monitor_tokens import _update_codex_file_state, collect_codex_session_files
from monitor_usage_sync import canonical_json


class CanonicalSourceIngestor:
    def __init__(self, store):
        self.store, self.index, self.lock = store, store.index, threading.RLock()
        self.pending, self.home, self.on_new_cost = {}, "", None

    @serialized
    def scan(self, home):
        self.home = str(Path(home).resolve())
        if getattr(self, "source_watcher", None) is not None:
            self.source_watcher.drain()
        with self.index.transaction() as db:
            saved = dict(db.execute("SELECT path,state FROM source_checkpoints"))
            for path in collect_codex_session_files(Path(home)):
                source = str(path.resolve())
                previous = self.pending.get(source, (json.loads(saved[source]) if source in saved else None, []))[0]
                state = dict(previous) if previous else None
                if state:
                    state["identity"] = tuple(state["identity"])
                    state["partial"] = base64.b64decode(state["partial"])
                    state["events"] = []
                    stat = path.stat()
                    if [state["size"], state["mtimeNs"]] != [stat.st_size, stat.st_mtime_ns]:
                        with path.open("rb") as stream:
                            for block, size, digest in db.execute("SELECT block,size,digest FROM source_blocks WHERE path=? ORDER BY block", (source,)):
                                stream.seek(block * 4096)
                                data = stream.read(size)
                                self.index.metrics["sourceVerificationBytes"] += len(data)
                                if hashlib.sha256(data).hexdigest() != digest:
                                    state = None
                                    break
                updated = _update_codex_file_state(path, state, max_bytes=SOURCE_READ_BYTES)
                if updated is None:
                    continue
                events = list(self.pending.get(source, (None, []))[1]) if state is not None else []
                for event in updated.pop("events"):
                    identity = [updated["sessionId"], event["checkedAt"], event["cumulative"]]
                    events.append({"eventId": f'{updated["sessionId"]}:{hashlib.sha256(canonical_json(identity)).hexdigest()[:24]}',
                        "sessionId": updated["sessionId"], "sourceFile": path.name,
                        **{key: value for key, value in event.items() if key not in {"index", "lineIndex", "cumulative"}}})
                self.index.metrics["sourcesChecked"] += 1
                self.index.metrics["sourceBytesRead"] += max(0, updated["offset"] - (state or {}).get("offset", 0))
                updated["observedSize"] = path.stat().st_size
                updated["partial"] = base64.b64encode(updated["partial"]).decode()
                if not events and previous and updated["offset"] == previous["offset"] and updated["mtimeNs"] == previous["mtimeNs"] and tuple(updated["identity"]) == tuple(previous["identity"]):
                    continue
                self.pending[source] = updated, events
        from monitor_tokens import add_token_delta
        with self.index.transaction() as db:
            usage = self.index.usage_totals(db)
            seen = set()
            for _, events in self.pending.values():
                for event in events:
                    if event["eventId"] in seen or db.execute("SELECT 1 FROM canonical_records WHERE view='local' AND kind='tokenLedger' AND json_extract(data,'$.eventId')=? LIMIT 1", (event["eventId"],)).fetchone():
                        continue
                    seen.add(event["eventId"])
                    tokens = empty_token_totals()
                    add_token_delta(tokens, event["tokens"])
                    self.index.add_usage(usage, {"rawModel": event["model"], "serviceTier": event.get("serviceTier"), "tokens": tokens})
        return {"source": "codex_session_logs", "home": str(home), "filesScanned": len(saved.keys() | self.pending.keys()), **usage, "errors": [], "incremental": True}

    @serialized
    def sync_ledger(self, path, slot, label, timeline, provenance, normalize, machine_id):
        from monitor_token_ledger import _usage_records
        with self.index.transaction() as db:
            for source, (state, events) in self.pending.items():
                try:
                    stat = Path(source).stat()
                except FileNotFoundError:
                    continue
                if (stat.st_size, stat.st_mtime_ns) != (state.get("observedSize", state["size"]), state["mtimeNs"]):
                    continue  # Rescan from the last committed checkpoint.
                additions = []
                for row in _usage_records(events if state.get("hasSessionMeta") else [], slot, label, timeline):
                    row = provenance(row)
                    if self.store._record_ledger(db, [row]):
                        additions.append(row)
                if additions:
                    self.index.put(db, f"ledgerNotifications:{self.home}", self.index.get(db, f"ledgerNotifications:{self.home}", []) + additions)
                db.execute("INSERT OR REPLACE INTO source_checkpoints VALUES(?,?)", (source, json.dumps(state)))
                db.execute("DELETE FROM source_blocks WHERE path=?", (source,))
                try:
                    with Path(source).open("rb") as stream:
                        for block in range((state["offset"] + 4095) // 4096):
                            data = stream.read(min(4096, state["offset"] - stream.tell()))
                            db.execute("INSERT INTO source_blocks VALUES(?,?,?,?)", (source, block, len(data), hashlib.sha256(data).hexdigest()))
                except FileNotFoundError:
                    pass
        self.pending.clear()
        with self.index.transaction() as db:
            notifications = self.index.get(db, f"ledgerNotifications:{self.home}", [])
        if self.on_new_cost:
            for row in notifications:
                self.on_new_cost(row)
        with self.index.transaction() as db:
            db.execute("DELETE FROM processing_meta WHERE name=?", (f"ledgerNotifications:{self.home}",))
        return self.index.ledger_cost()
