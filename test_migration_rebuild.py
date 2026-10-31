import json
import shutil
import threading
import unittest
import uuid
from pathlib import Path
from unittest import mock

import monitor_migrations as migrations
import monitor_sampling as sampling
from monitor_processing import ProcessingIndex
from monitor_projection import DashboardProjector
from monitor_usage_sync import UsageDataStore


class MigrationRebuildTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-migration-rebuild-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.cache = self.root / "cache.json"
        self.store = UsageDataStore(self.root / "quota.jsonl", self.root / "ledger.jsonl", "machine", lambda _: "v3:a", threading.RLock(), cache_path=self.cache)
        self.accounts = {"activeAccountId": "slot", "items": [{"id": "slot", "label": "A", "usageAccountId": "v3:a"}]}
        self.projector = DashboardProjector(self.store)
        self.store.record_quota({"checkedAt": "2030-01-01T00:00:00Z", "accountSlotId": "slot", "accountLabel": "A",
            "windows": {"5h": {"usedPercent": 10, "plan": "plus", "resetAt": 1893474000}}})
        self.projector.refresh(self.accounts, 1893456000)
        self.paths = {"processing_index": self.store.index.path, "quota_history": self.store.quota_path, "token_ledger": self.store.token_ledger_path}

    def migrate(self, current, steps):
        (self.root / migrations.DATA_MIGRATION_STATE_FILENAME).write_text(json.dumps({"dataContractVersion": current}))
        with mock.patch.object(migrations, "DATA_CONTRACT_VERSION", max(steps)), mock.patch.dict(migrations.DATA_MIGRATIONS, steps):
            return migrations.migrate_history_data(self.root, self.root / "history", self.store.quota_path, self.root / "sessions", self.store.token_ledger_path,
                self.root / "samples", usage_sync_cache_path=self.cache)

    def queues(self):
        with self.store.index.transaction() as db:
            return {table: list(db.execute("SELECT * FROM processing_" + table)) for table in ("dirty", "changes", "sampling_changes")}

    def test_identity_migration_preserves_results_without_rebuild(self):
        with self.store.index.transaction() as db:
            before = list(db.execute("SELECT * FROM processing_display"))
            points = list(db.execute("SELECT * FROM processing_sampled_quota"))
        with mock.patch.object(sampling, "sample_rows", wraps=sampling.sample_rows) as replay:
            self.assertEqual(self.migrate(18, {19: migrations._migrate_history_to_v19}), [19])
            replay.assert_not_called()
        self.assertTrue(all(not rows for rows in self.queues().values()))
        self.assertIsNone(self.projector.refresh(self.accounts, 1893456001))
        with self.store.index.transaction() as db:
            self.assertEqual(list(db.execute("SELECT * FROM processing_display")), before)
            self.assertEqual(list(db.execute("SELECT * FROM processing_sampled_quota")), points)

    def test_identity_migration_preserves_preexisting_projection_work(self):
        with self.store.index.transaction() as db:
            self.store.index.dirty(db, "v3:a", "existing correction")
        before = self.queues()
        migrations._migrate_history_to_v19(self.paths)
        self.assertEqual(self.queues(), before)

    def test_identity_migration_requests_rebuild_for_conflicting_readings(self):
        row = self.store.index.rows("quota")[0]
        row["windows"]["5h"]["usedPercent"] = 20
        with self.store.index.transaction() as db:
            self.store.index.fact(db, str(self.root / "legacy-quota.jsonl"), 0, "quota", row)
            db.execute("DELETE FROM processing_dirty")
            db.execute("DELETE FROM processing_changes")
        with mock.patch.object(sampling, "sample_rows", wraps=sampling.sample_rows) as replay:
            self.assertEqual(self.migrate(18, {19: migrations._migrate_history_to_v19}), [19])
            self.assertEqual(replay.call_count, 2)
        self.assertEqual(len(self.queues()["dirty"]), 2)
        self.assertIsNotNone(self.projector.refresh(self.accounts, 1893456001))

    def test_cascade_without_request_does_not_invalidate_projections(self):
        self.assertEqual(self.migrate(17, {18: lambda paths: None, 19: lambda paths: None}), [18, 19])
        self.assertTrue(all(not rows for rows in self.queues().values()))
        self.assertIsNone(self.projector.refresh(self.accounts, 1893456001))

    def test_cascade_coalesces_explicit_requests_and_sampling(self):
        def step(paths):
            migrations.request_migration_rebuild(paths, "changed quota interpretation", sampling=True)
            # A step may use runtime writers; their transactions must not replay early.
            with ProcessingIndex(paths["processing_index"]).transaction() as db:
                sampling.changed(db, "v3:a")
        with mock.patch.object(sampling, "sample_rows", wraps=sampling.sample_rows) as replay, mock.patch.object(migrations, "_apply_migration_rebuild", wraps=migrations._apply_migration_rebuild) as apply:
            self.assertEqual(self.migrate(17, {18: step, 19: step}), [18, 19])
            self.assertEqual(replay.call_count, 2)  # One replay for each final Local/Merged view.
            apply.assert_called_once()
        with self.store.index.transaction() as db:
            self.assertIsNone(self.store.index.get(db, "migrationRebuildPending"))
        self.assertIsNotNone(self.projector.refresh(self.accounts, 1893456001))
        self.assertEqual(self.projector.metrics["accountRebuilds"], 4)  # Initial build + one final rebuild for each view.
        self.assertIsNone(self.projector.refresh(self.accounts, 1893456002))

    def test_failed_cascade_preserves_request_for_retry(self):
        def first(paths):
            migrations.request_migration_rebuild(paths, "changed interpretation", sampling=True)
        def fail(paths):
            migrations.request_migration_rebuild(paths, "rolled back request")
            raise RuntimeError("interrupted")
        with mock.patch.object(sampling, "sample_rows", wraps=sampling.sample_rows) as replay:
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                self.migrate(17, {18: first, 19: fail})
            replay.assert_not_called()
        with self.store.index.transaction() as db:
            self.assertEqual(self.store.index.get(db, "migrationRebuildPending"), {"reasons": ["changed interpretation"], "sampling": True})
        with mock.patch.object(sampling, "sample_rows", wraps=sampling.sample_rows) as replay:
            self.assertEqual(self.migrate(18, {19: lambda paths: None}), [19])
            self.assertEqual(replay.call_count, 2)
        self.assertIsNotNone(self.projector.refresh(self.accounts, 1893456001))

    def test_legacy_file_fingerprints_are_adopted_without_rebuild(self):
        with self.store.index.transaction() as db:
            self.store.index.put(db, "samplingRule", "a" * 64)
            self.store.index.put(db, "projectionRule", "b" * 64)
        with mock.patch.object(sampling, "sample_rows", wraps=sampling.sample_rows) as replay:
            ProcessingIndex(self.store.index.path)
            self.assertIsNone(self.projector.refresh(self.accounts, 1893456001))
            replay.assert_not_called()

    def test_semantic_rule_version_change_still_rebuilds(self):
        with self.store.index.transaction() as db:
            self.store.index.put(db, "samplingRule", 0)
            self.store.index.put(db, "projectionRule", 0)
        with mock.patch.object(sampling, "sample_rows", wraps=sampling.sample_rows) as replay:
            ProcessingIndex(self.store.index.path)
            self.assertEqual(replay.call_count, 2)
        self.assertIsNotNone(self.projector.refresh(self.accounts, 1893456001))


if __name__ == "__main__":
    unittest.main()
