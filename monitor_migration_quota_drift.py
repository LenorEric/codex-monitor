"""Compact local quota runs split by small reset timestamp drift."""

import sqlite3
from contextlib import closing

from monitor_common import parse_timestamp, resets_match
from monitor_canonical_store import CanonicalRepository as records


def migrate_local(paths):
    if not paths["processing_index"].exists():
        return
    with closing(sqlite3.connect(paths["processing_index"])) as db:
        db.execute("BEGIN IMMEDIATE")
        try:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_quota'").fetchone():
                db.commit()
                return
            for dataset, account, label in db.execute("SELECT DISTINCT dataset,account,window FROM canonical_quota").fetchall():
                previous, removed, last_observation = None, [], None

                def flush():
                    if removed:
                        key, row = previous
                        records.write(db, dataset, "quota", row, key=key, account=account)
                        db.executemany("DELETE FROM canonical_quota WHERE dataset=? AND account=? AND record_key=?",
                            ((dataset, account, key) for key in removed))
                        removed.clear()

                for key, row in records.items(db, dataset, "quota", account, condition="window=?", parameters=(label,), order="range_first,range_last,record_key"):
                    window = row["windows"][label]
                    if last_observation is not None:
                        old_reset, new_reset = parse_timestamp(last_observation.get("resetAt")), parse_timestamp(window.get("resetAt"))
                        gap = window["firstAt"] - last_observation["lastAt"]
                        if 0 < gap <= 14400 and old_reset is not None and new_reset is not None and old_reset != new_reset and resets_match(old_reset, new_reset):
                            db.execute("INSERT INTO delivery_pending(view,account,kind,window,first,last) VALUES(?,?,'quota',?,?,?)",
                                (dataset, account, label, last_observation["lastAt"], window["firstAt"]))
                    last_observation = window.copy()
                    if previous is not None:
                        prior = previous[1]["windows"][label]
                        gap = window["firstAt"] - prior["lastAt"]
                        if 0 < gap <= 14400 and prior["usedPercent"] != 0 and all(prior.get(field) == window.get(field) for field in ("usedPercent", "plan", "multiplier")) and resets_match(prior.get("resetAt"), window.get("resetAt")):
                            prior.update(lastAt=window["lastAt"], resetAt=window.get("resetAt"))
                            removed.append(key)
                            continue
                    flush()
                    previous = key, row
                flush()
            if db.execute("PRAGMA foreign_key_check").fetchone():
                raise ValueError("Foreign key validation failed during quota drift consolidation")
            db.commit()
        except BaseException:
            db.rollback()
            raise
