"""Contract 25: consolidate zero quota history with moving reset deadlines.

This explicitly requested historical compaction materially reduces record counts.
Preserve the earliest key/provenance and the latest observation's reset value.
Only affected accounts enter the ordinary quota preparation queue; no raw replay.
"""

import sqlite3
from contextlib import closing

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
                previous, removed = None, []

                def flush():
                    if not removed:
                        return
                    key, row = previous
                    records.write(db, dataset, "quota", row, key=key, account=account)
                    db.executemany("DELETE FROM canonical_quota WHERE dataset=? AND account=? AND record_key=?",
                        ((dataset, account, key) for key in removed))
                    db.execute("INSERT OR IGNORE INTO delivery_dirty VALUES(?,?)", (dataset, account))
                    db.execute("INSERT OR IGNORE INTO delivery_work VALUES(?,?,'quota',?)", (dataset, account, key))
                    removed.clear()

                for key, row in records.items(db, dataset, "quota", account, condition="window=?", parameters=(label,), order="range_first,range_last,record_key"):
                    window = row["windows"][label]
                    if previous is not None:
                        prior = previous[1]["windows"][label]
                        if prior["usedPercent"] == window["usedPercent"] == 0 and prior["lastAt"] < window["firstAt"] and all(
                                prior.get(field) == window.get(field) for field in ("plan", "multiplier")):
                            prior.update(lastAt=window["lastAt"], resetAt=window.get("resetAt"))
                            removed.append(key)
                            continue
                    flush()
                    previous = key, row
                flush()
            if db.execute("PRAGMA foreign_key_check").fetchone():
                raise ValueError("Foreign key validation failed during zero quota consolidation")
            db.commit()
        except BaseException:
            db.rollback()
            raise
