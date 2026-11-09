"""Local contract 23: lossless JSON-to-typed canonical representation conversion."""

import json
import sqlite3
from contextlib import closing

from monitor_migration_canonical_schema import CanonicalRepository, TABLES, initialize
from monitor_usage_sync import canonical_json


def remove_seen(paths):
    """Contract 26: retain token identities in their existing primary key only."""
    if not paths["processing_index"].exists():
        return
    with closing(sqlite3.connect(paths["processing_index"])) as db, db:
        db.execute("BEGIN IMMEDIATE")
        if not db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_seen'").fetchone():
            return
        # Never discard a tombstone for an event missing from permanent history.
        if db.execute("SELECT view,record_key FROM canonical_seen EXCEPT SELECT dataset,record_key FROM canonical_tokens LIMIT 1").fetchone():
            raise ValueError("Cannot remove canonical_seen: an identity has no permanent token record; original data was preserved")
        db.execute("DROP TABLE canonical_seen")


def migrate_local(paths):
    path = paths["processing_index"]
    if not path.exists():
        return
    with closing(sqlite3.connect(path)) as db:
        db.execute("BEGIN IMMEDIATE")
        try:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name='canonical_records'").fetchone():
                if not all(db.execute("SELECT 1 FROM sqlite_master WHERE name=?", (table,)).fetchone() for table, _ in TABLES.values()):
                    raise ValueError("Local contract 23 requires the contract 22 canonical store")
                db.rollback()
                return
            if any(db.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() for table, _ in TABLES.values()
                    if db.execute("SELECT 1 FROM sqlite_master WHERE name=?", (table,)).fetchone()):
                raise ValueError("Both JSON and populated typed canonical stores exist; refusing ambiguous conversion")
            initialize(db)
            counts = {}
            for dataset, kind, account, key, first, last, data in db.execute("SELECT view,kind,account,record_key,first,last,data FROM canonical_records"):
                try:
                    row = json.loads(data)
                    CanonicalRepository.write(db, dataset, kind, row, key, account, (first, last))
                    restored = CanonicalRepository.rows(db, dataset, kind, account, condition="record_key=?", parameters=(key,))
                    if len(restored) != 1 or canonical_json(restored[0]) != canonical_json(row):
                        raise ValueError("Canonical field round-trip mismatch")
                    counts[dataset, kind] = counts.get((dataset, kind), 0) + 1
                except Exception as exc:
                    raise ValueError(f"Cannot convert canonical {dataset}/{kind}/{key}: {exc}") from exc
            actual = {(dataset, kind): count for kind, (table, _) in TABLES.items() for dataset, count in db.execute(f"SELECT dataset,COUNT(*) FROM {table} GROUP BY dataset")}
            if counts != actual:
                raise ValueError("Canonical dataset/type counts changed during conversion")
            if db.execute("PRAGMA foreign_key_check").fetchone():
                raise ValueError("Foreign key validation failed during canonical conversion")
            db.execute("DROP TABLE canonical_records")
            db.commit()
        except BaseException:
            db.rollback()
            raise
