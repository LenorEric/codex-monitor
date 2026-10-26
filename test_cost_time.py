import json
import unittest
from unittest import mock

import monitor_auto_update
import test_delivery
import test_processing
from monitor_auto_update import DATA_MIGRATION_STATE_FILENAME, migrate_history_data
from monitor_dashboard import dashboard_cost_time
from monitor_delivery import DAY, reduce_cost_time
from monitor_projection import DashboardProjector


class CostTimeDeliveryTests(unittest.TestCase):
    setUp = test_delivery.DeliveryTests.setUp
    materialize = test_delivery.DeliveryTests.materialize

    def cost(self, stamp, amount, key="event", account="a", model="m", view="local"):
        row = {"_key": key, "timestamp": stamp, "usageAccountId": account, "accountSlotId": "slot", "accountLabel": account,
            "costByModelUsd": {model: amount}, "totalCostUsd": amount}
        with self.index.transaction() as db:
            db.execute("INSERT OR REPLACE INTO processing_display VALUES(?,?,?,?,?,?)", (view, account, "costTime", key, stamp, json.dumps(row)))
        return row

    def test_preset_lower_boundary_is_exact_without_quota(self):
        self.cost(self.start + 599, 100, "outside")
        self.cost(self.start + 600, 2, "boundary")
        self.cost(self.start + 650, 3, "inside", model="n")
        self.cost(self.start + 5 * 3600 + 600, 4, "latest")
        self.delivery.prepare(self.start + 100 * DAY)
        manifest = self.delivery.manifest("local", "5h")
        self.assertEqual(manifest["costTimeBounds"], [self.start + 600, self.start + 5 * 3600 + 600])
        self.assertEqual(manifest["costTimeTotals"][0]["costByModelUsd"], {"m": 6, "n": 3})
        rows = [row for part in self.materialize(manifest) for row in part["costTime"]]
        self.assertEqual(sum(row["totalCostUsd"] for row in rows), 9)
        self.assertEqual(min(row["timestamp"] for row in rows), self.start + 600)

    def test_short_scopes_keep_minute_precision_even_for_old_events(self):
        self.cost(self.start + 10, 1, "first")
        self.cost(self.start + 7 * 60 + 10, 2, "second")
        self.delivery.prepare(self.start + 100 * DAY)
        for scope in ("5h", "24h"):
            manifest = self.delivery.manifest("local", scope)
            self.assertEqual(manifest["costTimeResolution"], 60)
            rows = [row for part in self.materialize(manifest) for row in part["costTime"]]
            self.assertEqual(len(rows), 2)
            self.assertEqual(sum(row["totalCostUsd"] for row in rows), 3)
        self.assertEqual(self.delivery.manifest("local", "All")["costTimeResolution"], 3600)

    def test_date_boundaries_are_half_open_and_support_dst_and_non_utc_midnight(self):
        for hours in (23, 25):
            with self.subTest(hours=hours):
                start = self.start + 1800
                end = start + hours * 3600
                self.cost(start - 1, 100, "before")
                self.cost(start, 2, "first")
                self.cost(end - 1, 3, "last", account="b", model="n")
                self.cost(end, 100, "after")
                manifest = self.delivery.manifest("local", "Date", start, end)
                self.assertEqual(sum(row["totalCostUsd"] for row in manifest["costTimeTotals"]), 5)
                rows = [row for part in self.materialize(manifest) for row in part["costTime"]]
                self.assertEqual({row["_key"] for row in rows}, {"first", "last"})
                self.assertEqual(manifest["costTimeBounds"], [start, end])

    def test_rollup_preserves_daily_sums_models_accounts_and_zero_events(self):
        rows = [self.cost(self.start + minute * 60, .1, str(minute), account="a" if minute % 2 else "b", model="m" if minute % 3 else "n") for minute in range(60)]
        rows.append(self.cost(self.start + DAY, 0, "zero"))
        totals = reduce_cost_time(rows, 0)
        self.assertAlmostEqual(sum(row["totalCostUsd"] for row in totals), 6)
        self.assertEqual(reduce_cost_time(reduce_cost_time(rows, 3600), 0), totals)
        self.delivery.prepare(self.start + 200 * DAY)
        manifest = self.delivery.manifest("local", "All")
        with self.delivery.reader() as db:
            digest = self.delivery._rollup(db, manifest["chunks"], 7 * DAY, self.start + 200 * DAY)
        rolled = json.loads(self.delivery.blob(digest, False))["costTime"]
        self.assertAlmostEqual(sum(row["totalCostUsd"] for row in rolled), 6)
        self.assertTrue(any(row["timestamp"] == self.start + DAY and row["totalCostUsd"] == 0 for row in rolled))
        self.assertEqual(reduce_cost_time(rolled, 0), [{key: value for key, value in row.items() if key not in {"accountSlotId", "accountLabel"}} for row in totals])

    def test_updates_replace_only_affected_cost_day_and_idle_is_read_only(self):
        self.cost(self.start, 1)
        self.cost(self.start + DAY, 2, "second")
        self.delivery.prepare(self.start + 2 * DAY)
        previous = self.delivery.manifest("local", "All")
        self.cost(self.start + DAY + 60, 3, "third")
        self.delivery.prepare(self.start + 2 * DAY)
        current = self.delivery.manifest("local", "All")
        self.assertEqual(previous["chunks"][0], current["chunks"][0])
        self.assertNotEqual(previous["chunks"][1], current["chunks"][1])
        self.assertEqual(current["costTimeTotals"][0]["totalCostUsd"], 6)
        with mock.patch.object(self.delivery, "_load", side_effect=AssertionError("idle history replay")):
            self.delivery.prepare(self.start + 2 * DAY)
        self.cost(self.start + 60, 10, "event")
        self.delivery.prepare(self.start + 2 * DAY)
        self.assertEqual(self.delivery.manifest("local", "All")["costTimeTotals"][0]["totalCostUsd"], 15)

    def test_invalid_or_legacy_cost_is_not_assigned_an_invented_timestamp(self):
        rows = [{"recordType": "legacyBaseline", "occurredAt": "2030-01-01T00:00:00Z", "cost": {"totalCostUsd": 20}},
            {"recordType": "usage", "cost": {"totalCostUsd": 2}},
            {"recordType": "usage", "occurredAt": "2030-01-01T00:00:00Z", "cost": {"totalCostUsd": float("nan")}}]
        self.assertEqual(dashboard_cost_time(rows), [])


class CostTimeProcessingTests(unittest.TestCase):
    setUp = test_processing.ProcessingTests.setUp
    cleanup = test_processing.ProcessingTests.cleanup
    event = staticmethod(test_processing.ProcessingTests.event)
    write = test_processing.ProcessingTests.write
    sync = test_processing.ProcessingTests.sync

    def test_same_session_crosses_midnight_and_incremental_append_does_not_rebuild(self):
        accounts = {"items": [{"id": "slot", "label": "Account", "usageAccountId": "account"}]}
        self.sync()
        projector = DashboardProjector(self.store)
        projector.refresh(accounts, 1893456100)
        first = projector.snapshot("local")["costTime"]
        self.assertEqual(len(first), 1)
        event = self.event(2, 200)
        event["timestamp"] = "2030-01-02T00:02:00Z"
        self.write([event], append=True)
        self.sync()
        with mock.patch.object(projector, "_rebuild", side_effect=AssertionError("append rebuilt account")):
            projector.refresh(accounts, 1893542600)
        local, merged = projector.snapshot("local"), projector.snapshot("merged")
        self.assertEqual(local["costTime"], merged["costTime"])
        self.assertEqual(len(local["costTime"]), 2)
        self.assertGreater(local["costTime"][1]["timestamp"] - local["costTime"][0]["timestamp"], DAY)
        self.assertAlmostEqual(sum(row["totalCostUsd"] for row in local["costTime"]), local["tokenSessions"][0]["byModel"]["gpt-5.5"]["cost"]["totalCostUsd"])
        # Importing the same event from a second source must not double merged cost.
        ledger = self.store.index.rows(self.store.token_ledger_path, "tokenLedger")
        with self.store.index.transaction() as db:
            for position, row in enumerate(ledger):
                self.store.index.fact(db, "peer:duplicate", position, "tokenLedger", row)
        projector.refresh(accounts, 1893542601)
        self.assertEqual(projector.snapshot("merged")["costTime"], merged["costTime"])

    def test_migration_rebuilds_from_facts_preserves_parser_state_and_is_retryable(self):
        self.sync()
        projector = DashboardProjector(self.store)
        projector.refresh({}, 1893456100)
        before = projector.snapshot("local")["costTime"]
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
        migration = monitor_auto_update.DATA_MIGRATIONS[10]
        def interrupted(paths):
            migration(paths)
            raise RuntimeError("interrupted")
        with mock.patch.dict(monitor_auto_update.DATA_MIGRATIONS, {10: interrupted}), self.assertRaisesRegex(RuntimeError, "interrupted"):
            migrate()
        self.assertEqual(projector.snapshot("local")["costTime"], before)
        self.assertEqual(json.loads(state.read_text())["dataContractVersion"], 9)
        self.assertEqual(migrate(), [10])
        self.assertEqual(self.store.token_ledger_path.read_bytes(), ledger)
        with self.store.index.transaction() as db:
            for table, rows in saved.items():
                self.assertEqual(list(db.execute(f"SELECT * FROM {table}")), rows)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM processing_display").fetchone()[0], 0)
        projector.refresh({}, 1893456100)
        self.assertEqual(projector.snapshot("local")["costTime"], before)
        self.assertEqual(migrate(), [])
