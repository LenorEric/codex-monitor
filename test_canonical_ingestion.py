import hashlib
import tempfile
import threading
import unittest
from pathlib import Path

from monitor_canonical import CanonicalDataStore
from monitor_canonical_ingestion import BLOCK_BYTES


class CanonicalIngestionTests(unittest.TestCase):
    def test_append_reuses_complete_blocks_and_growing_rewrite_reparses(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        source = root / "sessions" / "session.jsonl"
        source.parent.mkdir()
        original = b"{}" + b" " * BLOCK_BYTES + b"\n"
        source.write_bytes(original)
        store = CanonicalDataStore(root / "usage.sqlite3", "device", lambda slot: slot, threading.RLock())
        self.addCleanup(store.index.close)

        def capture():
            store.sources.scan(root)
            store.sources.sync_ledger("slot", "Slot", [], lambda row: row)

        capture()
        with store.index.transaction() as db:
            db.execute("CREATE TRIGGER protect_complete_block BEFORE DELETE ON source_blocks WHEN OLD.block=0 BEGIN SELECT RAISE(ABORT,'rehashed complete block'); END")
        capture()
        self.assertEqual(store.index.metrics["sourceBytesRead"], len(original))
        with source.open("ab") as stream:
            stream.write(b"{}\n")
        capture()
        self.assertEqual(store.index.metrics["sourceBytesRead"], len(original) + 3)
        self.assertEqual(store.index.metrics["sourceVerificationBytes"], len(original))

        with store.index.transaction() as db:
            db.execute("DROP TRIGGER protect_complete_block")
        rewritten = b"[]" + original[2:] + b"{}\n{}\n"
        source.write_bytes(rewritten)
        capture()
        self.assertEqual(store.index.metrics["sourceBytesRead"], len(original) + 3 + len(rewritten))
        with store.index.transaction() as db:
            self.assertEqual(db.execute("SELECT digest FROM source_blocks WHERE block=0").fetchone()[0], hashlib.sha256(rewritten[:BLOCK_BYTES]).hexdigest())
