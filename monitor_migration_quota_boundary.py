"""Local contract 24: remove retired quota data and compact compatible history."""

import json
import sqlite3
from contextlib import closing

from monitor_usage_sync import canonical_json
from monitor_migration_canonical_schema import QUOTA as RELEASED_QUOTA
from monitor_canonical_store import QUOTA


def _preprocess_quota(rows):
    """Replay raw history with contract-24 decrease confirmation and reset rules.

    A saved range supplies repeated evidence at both endpoints. Its equal-valued
    last observation confirms a decrease without inventing intermediate points.
    """
    previous, pending = None, None
    for row in rows:
        if previous is not None and row[1] <= previous[2]:
            if row[2] <= previous[2] or row[3:7] != previous[3:7]:
                continue  # Earlier retained coverage wins every conflict.
        if pending is not None and row[1] <= pending[1]:
            continue
        if previous is None or row[7] != previous[7]:
            pending = None
        elif pending is not None:
            if row[3] < pending[3]:
                pending = row
                if row[1] == row[2]:
                    continue
            else:
                yield pending
            pending = None
        elif row[3] < previous[3]:
            if row[1] == row[2]:
                pending = row
                continue
        yield row
        previous = row


def _migrate_seen(db):
    if any(column[1] == "kind" for column in db.execute("PRAGMA table_info(canonical_seen)")):
        db.execute("ALTER TABLE canonical_seen RENAME TO canonical_seen_legacy")
        db.execute("CREATE TABLE canonical_seen(view TEXT NOT NULL,record_key TEXT NOT NULL,PRIMARY KEY(view,record_key)) WITHOUT ROWID")
        db.execute("INSERT INTO canonical_seen SELECT view,record_key FROM canonical_seen_legacy WHERE kind='tokenLedger'")
        db.execute("DROP TABLE canonical_seen_legacy")


def _compact_quota(db):
    """Extend adjacent compatible runs, retaining the earliest record's provenance.

    Process earliest first; discard later conflicting records overlapping retained
    coverage. Transport stays intact; changed acceptance requests delivery refresh.
    """
    replay_changed = False
    for dataset, account, window in db.execute("SELECT DISTINCT dataset,account,window FROM canonical_quota").fetchall():
        rows = db.execute("SELECT record_key,range_first,range_last,used_percent,multiplier,plan,"
            "CASE WHEN reset_at_text IS NOT NULL THEN reset_at_text ELSE reset_at END,reset_at FROM canonical_quota "
            "WHERE dataset=? AND account=? AND window=? ORDER BY range_first,used_percent,range_last,record_key", (dataset, account, window)).fetchall()
        previous = None
        accepted = list(_preprocess_quota(rows))
        retained = {row[0] for row in accepted}
        updates, deletions = {}, [(dataset, account, row[0]) for row in rows if row[0] not in retained]
        if deletions:
            replay_changed = True
        for row in accepted:
            if previous is not None and previous[3:7] == row[3:7]:
                last = max(previous[2], row[2])
                updates[previous[0]] = last
                deletions.append((dataset, account, row[0]))
                previous = previous[:2] + (last,) + previous[3:]
            elif previous is not None and row[1] <= previous[2]:
                deletions.append((dataset, account, row[0]))
            else:
                previous = row
        db.executemany("UPDATE canonical_quota SET range_last=?,last=MAX(last,?) WHERE dataset=? AND account=? AND record_key=?",
            ((last, last, dataset, account, key) for key, last in updates.items()))
        db.executemany("DELETE FROM canonical_quota WHERE dataset=? AND account=? AND record_key=?", deletions)
    return replay_changed


def migrate_local(paths):
    if not paths["processing_index"].exists():
        return
    with closing(sqlite3.connect(paths["processing_index"])) as db:
        db.execute("BEGIN IMMEDIATE")
        try:
            _migrate_seen(db)
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_quota'").fetchone():
                db.commit()
                return
            columns = {column[1] for column in db.execute("PRAGMA table_info(canonical_quota)")}
            replay_changed = False
            if "continuous" in columns:
                bit = next(index for index, field in enumerate(RELEASED_QUOTA) if field[0] == "continuous")
                replay_changed = db.execute("SELECT 1 FROM canonical_quota WHERE (present & ?) != 0 AND continuous IS NULL LIMIT 1", (1 << bit,)).fetchone() is not None
                # Explicitly requested replay treats saved decisions as raw observations.
                # Removing a mapped field shifts every subsequent presence/type-mask bit.
                for mask in ("present", "integers"):
                    db.execute(f"UPDATE canonical_quota SET {mask}=(({mask} & ?) | (({mask} >> ?) << ?)) & ?",
                        ((1 << bit) - 1, bit + 1, bit, (1 << len(QUOTA)) - 1))
                db.execute("ALTER TABLE canonical_quota DROP COLUMN continuous")
            if "reset_boundary" in columns:
                db.execute("ALTER TABLE canonical_quota DROP COLUMN reset_boundary")
            # Earlier development builds may have retained the field as extension JSON.
            for dataset, account, key, data in db.execute("SELECT dataset,account,record_key,extras_json FROM canonical_quota WHERE extras_json IS NOT NULL").fetchall():
                extra = json.loads(data)
                changed = False
                for label, window in list(extra.get("windows", {}).items()):
                    if "continuous" in window:
                        replay_changed |= window["continuous"] is None
                        window.pop("continuous")
                        changed = True
                        if not window:
                            del extra["windows"][label]
                            continue
                    if "resetBoundary" in window:
                        del window["resetBoundary"]
                        changed = True
                        if not window:
                            del extra["windows"][label]
                if changed:
                    if not extra["windows"]:
                        del extra["windows"]
                    db.execute("UPDATE canonical_quota SET extras_json=? WHERE dataset=? AND account=? AND record_key=?",
                        (canonical_json(extra).decode() if extra else None, dataset, account, key))
            replay_changed = _compact_quota(db) or replay_changed
            if replay_changed and db.execute("SELECT 1 FROM sqlite_master WHERE name='processing_meta'").fetchone():
                db.execute("DELETE FROM processing_meta WHERE name LIKE 'samplingState:local:%'")
            if db.execute("PRAGMA foreign_key_check").fetchone():
                raise ValueError("Foreign key validation failed during quota boundary removal")
            db.commit()
        except BaseException:
            db.rollback()
            raise
    if replay_changed:
        from monitor_migrations import request_migration_rebuild
        request_migration_rebuild(paths, "explicit chronological quota preprocessing replay", delivery_only=True)
