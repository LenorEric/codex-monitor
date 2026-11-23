"""Shared SQLite metadata persistence for persistent indexes."""

import json


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class IndexState:
    @staticmethod
    def get(db, name, default=None):
        row = db.execute("SELECT value FROM processing_meta WHERE name=?", (name,)).fetchone()
        return json.loads(row[0]) if row else default

    @staticmethod
    def put(db, name, value):
        db.execute("INSERT INTO processing_meta VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value", (name, encode(value)))


