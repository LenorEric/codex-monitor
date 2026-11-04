import json
import unittest
from unittest import mock

import monitor_migrations
import test_processing
from monitor_migrations import DATA_MIGRATION_STATE_FILENAME, migrate_history_data
from monitor_delivery import DAY, DashboardDelivery
from test_support_projection import DashboardProjector


class CostTimeProcessingTests(unittest.TestCase):
    setUp = test_processing.ProcessingTests.setUp
    cleanup = test_processing.ProcessingTests.cleanup
    event = staticmethod(test_processing.ProcessingTests.event)
    write = test_processing.ProcessingTests.write
    sync = test_processing.ProcessingTests.sync

    def tokens(self, view):
        delivery = DashboardDelivery(self.store.index)
        delivery.prepare(1893542602, {"accounts": {"items": []}}, force=True)
        with self.store.index.transaction() as db:
            return delivery._tokens(db, view, "v3:account")

    def test_same_session_crosses_midnight_and_incremental_append_does_not_rebuild(self):
        accounts = {"items": [{"id": "slot", "label": "Account", "usageAccountId": "account"}]}
        self.sync()
        projector = DashboardProjector(self.store)
        projector.refresh(accounts, 1893456100)
        first = self.tokens("local")
        self.assertEqual(len(first), 1)
        event = self.event(2, 200)
        event["timestamp"] = "2030-01-02T00:02:00Z"
        self.write([event], append=True)
        self.sync()
        with mock.patch.object(projector, "_rebuild", side_effect=AssertionError("append rebuilt account")):
            projector.refresh(accounts, 1893542600)
        local, merged = self.tokens("local"), self.tokens("merged")
        self.assertEqual(local, merged)
        self.assertEqual(len(local), 2)
        self.assertGreater(local[1][1] - local[0][1], DAY)
        self.assertAlmostEqual(sum(row[-1] for row in local) / 100000000, self.store.index.ledger_cost()[0]["totalCostUsd"])
        # Importing the same event from a second source must not double merged cost.
        ledger = self.store.index.rows("tokenLedger")
        with self.store.index.transaction() as db:
            for position, row in enumerate(ledger):
                self.store.index.fact(db, "peer:duplicate", position, "tokenLedger", row)
        projector.refresh(accounts, 1893542601)
        self.assertEqual(self.tokens("merged"), merged)

    def test_migration_rebuilds_from_facts_preserves_parser_state_and_is_retryable(self):
        self.sync()
        projector = DashboardProjector(self.store)
        projector.refresh({}, 1893456100)
        before = self.tokens("local")
        with self.store.index.transaction() as db:
            saved = {table: list(db.execute(f"SELECT * FROM {table}")) for table in ("processing_scan_sources", "processing_scan_events", "processing_effective")}
        # Point migration at the same index path used by this test store.
        cache = self.store.index.path.with_name(self.store.index.path.name.removesuffix("-v4.sqlite3") + ".json")
        state = self.root / DATA_MIGRATION_STATE_FILENAME
        state.write_text('{"dataContractVersion":9}', encoding="utf-8")
        ledger = self.store.token_ledger_path.read_bytes()
        def migrate(target=10):
            return migrate_history_data(self.root, self.root / "history", self.store.quota_path, self.root / "sessions-old", self.store.token_ledger_path,
                self.root / "samples", usage_sync_cache_path=cache, target_version=target)
        migration = monitor_migrations.DATA_MIGRATIONS[10]
        def interrupted(paths):
            migration(paths)
            raise RuntimeError("interrupted")
        with mock.patch.dict(monitor_migrations.DATA_MIGRATIONS, {10: interrupted}), self.assertRaisesRegex(RuntimeError, "interrupted"):
            migrate()
        self.assertEqual(self.tokens("local"), before)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 9)
        self.assertEqual(migrate(), [10])
        self.assertEqual(self.store.token_ledger_path.read_bytes(), ledger)
        with self.store.index.transaction() as db:
            for table, rows in saved.items():
                self.assertEqual(list(db.execute(f"SELECT * FROM {table}")), rows)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM processing_display").fetchone()[0], 0)
        projector.refresh({}, 1893456100)
        self.assertEqual(self.tokens("local"), before)
        self.assertEqual(migrate(), [])
