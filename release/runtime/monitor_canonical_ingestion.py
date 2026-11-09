"""Incremental source checkpoints, with parsed events retained only until commit."""

import base64
import hashlib
import json
import threading
from pathlib import Path

from monitor_canonical_store import CanonicalRepository
from monitor_common import empty_token_totals
from monitor_sources import SOURCE_READ_BYTES, serialized
from monitor_tokens import _update_codex_file_state, collect_codex_session_files
from monitor_usage_sync import canonical_json


BLOCK_BYTES = 4096


def _file_stamp(stat):
    return (stat.st_dev, stat.st_ino), stat.st_size, stat.st_mtime_ns


class CanonicalSourceIngestor:
    def __init__(self, store):
        self.store, self.index, self.lock = store, store.index, threading.RLock()
        self.pending, self.home, self.on_new_cost = {}, "", None

    def _prefix_matches(self, db, source, offset):
        verified = 0
        with Path(source).open("rb") as stream:
            for block, size, digest in db.execute("SELECT block,size,digest FROM source_blocks WHERE path=? ORDER BY block", (source,)):
                if block * BLOCK_BYTES != verified or size != min(BLOCK_BYTES, offset - verified) or size <= 0:
                    return False
                data = stream.read(size)
                self.index.metrics["sourceVerificationBytes"] += len(data)
                if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
                    return False
                verified += size
        return verified == offset

    @serialized
    def scan(self, home):
        self.home = str(Path(home).resolve())
        self.pending.clear()
        with self.index.transaction() as db:
            saved = dict(db.execute("SELECT path,state FROM source_checkpoints"))
            for path in collect_codex_session_files(Path(home)):
                source = str(path.resolve())
                state = json.loads(saved[source]) if source in saved else None
                self.index.metrics["sourcesChecked"] += 1
                try:
                    identity, size, modified = _file_stamp(path.stat())
                    if state and tuple(state["identity"]) == identity:
                        if state["offset"] == size and state["mtimeNs"] == modified:
                            continue
                        if size > state["offset"] and self._prefix_matches(db, source, state["offset"]):
                            state["identity"] = identity
                            state["partial"] = base64.b64decode(state["partial"])
                            state["events"] = []
                        else:
                            state = None
                    else:
                        state = None
                    offset = state["offset"] if state else 0
                    updated = _update_codex_file_state(path, state, max_bytes=SOURCE_READ_BYTES)
                    observed = _file_stamp(path.stat())
                except FileNotFoundError:
                    continue
                if updated is None:
                    continue
                if observed[0] != updated["identity"] or observed[2] != updated["mtimeNs"] or observed[1] < updated["offset"]:
                    continue
                # A verified prefix belongs to this exact file version.
                if offset and observed != (identity, size, modified):
                    continue
                events = []
                for event in updated.pop("events"):
                    identity = [updated["sessionId"], event["checkedAt"], event["cumulative"]]
                    events.append({"eventId": f'{updated["sessionId"]}:{hashlib.sha256(canonical_json(identity)).hexdigest()[:24]}',
                        "sessionId": updated["sessionId"], "sourceFile": path.name,
                        **{key: value for key, value in event.items() if key not in {"index", "lineIndex", "cumulative"}}})
                self.index.metrics["sourceBytesRead"] += max(0, updated["offset"] - offset)
                updated["observedSize"] = observed[1]
                updated["partial"] = base64.b64encode(updated["partial"]).decode()
                self.pending[source] = updated, events, min(offset, updated["offset"]) // BLOCK_BYTES
        from monitor_tokens import add_token_delta
        with self.index.transaction() as db:
            usage = self.index.usage_totals(db)
            seen = set()
            for _, events, _ in self.pending.values():
                for event in events:
                    if event["eventId"] in seen or CanonicalRepository.has_event(db, event["eventId"]):
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
            for source, (state, events, first_block) in self.pending.items():
                expected = state["identity"], state["observedSize"], state["mtimeNs"]
                try:
                    with Path(source).open("rb") as stream:
                        if _file_stamp(Path(source).stat()) != expected:
                            continue
                        stream.seek(first_block * BLOCK_BYTES)
                        blocks = []
                        while stream.tell() < state["offset"]:
                            data = stream.read(min(BLOCK_BYTES, state["offset"] - stream.tell()))
                            if not data:
                                break
                            blocks.append((source, first_block + len(blocks), len(data), hashlib.sha256(data).hexdigest()))
                        if stream.tell() != state["offset"] or _file_stamp(Path(source).stat()) != expected:
                            continue
                except FileNotFoundError:
                    continue
                additions = []
                for row in _usage_records(events if state.get("hasSessionMeta") else [], slot, label, timeline):
                    row = provenance(row)
                    if self.store._record_ledger(db, [row]):
                        additions.append(row)
                if additions:
                    self.index.put(db, f"ledgerNotifications:{self.home}", self.index.get(db, f"ledgerNotifications:{self.home}", []) + additions)
                db.execute("INSERT OR REPLACE INTO source_checkpoints VALUES(?,?)", (source, json.dumps(state)))
                db.execute("DELETE FROM source_blocks WHERE path=? AND block>=?", (source, first_block))
                db.executemany("INSERT INTO source_blocks VALUES(?,?,?,?)", blocks)
        self.pending.clear()
        with self.index.transaction() as db:
            notifications = self.index.get(db, f"ledgerNotifications:{self.home}", [])
        if self.on_new_cost:
            for row in notifications:
                self.on_new_cost(row)
        with self.index.transaction() as db:
            db.execute("DELETE FROM processing_meta WHERE name=?", (f"ledgerNotifications:{self.home}",))
        return self.index.ledger_cost()
