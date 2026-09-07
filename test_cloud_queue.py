import http.server
import io
import json
from pathlib import Path
import shutil
import threading
import unittest
import urllib.error
import urllib.request
import uuid
from types import SimpleNamespace
from unittest import mock

import monitor_dashboard
from monitor_cloud import CloudError, CloudManager, _serialized_cloud_operation, hash_control_password, new_control_password_salt
from monitor_cloud_queue import CloudOperationQueue, OperationCancelled, OperationSkipped
from monitor_skills import SkillManager


class CloudQueueTests(unittest.TestCase):
    def setUp(self):
        self.queue = CloudOperationQueue()
        self.addCleanup(self.queue.close)

    def block(self):
        started, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        operation = self.queue.submit("running", lambda: (started.set(), release.wait(5)))
        self.assertTrue(started.wait(2))
        return operation, release

    def assert_lifecycle(self, operation):
        self.assertEqual([event["phase"] for event in self.queue.snapshot()["events"] if event["operation"]["id"] == operation["public"]["id"]], ["start", "end"])

    def test_fifo_cancel_and_snapshot_do_not_wait_for_network(self):
        first, release = self.block()
        calls = []
        second = self.queue.submit("second", lambda: calls.append(2))
        third = self.queue.submit("third", lambda: calls.append(3))
        fourth = self.queue.submit("fourth", lambda: calls.append(4))
        self.assertEqual([item["action"] for item in self.queue.snapshot()["operations"]], ["running", "second", "third", "fourth"])
        self.assertEqual(self.queue.cancel(third["public"]["id"])["status"], "cancelled")
        with self.assertRaises(OperationSkipped):
            self.queue.cancel(first["public"]["id"])
        release.set()
        self.queue.wait(fourth)
        self.assertEqual(calls, [2, 4])
        with self.assertRaises(OperationCancelled):
            self.queue.wait(third)
        for operation in (first, second, third, fourth):
            self.assert_lifecycle(operation)

    def test_identical_adjacent_requests_merge_without_crossing_barriers(self):
        _, release = self.block()
        calls = []
        operations = [self.queue.submit("sync", lambda: calls.append("sync"), combine_key="sync") for _ in range(3)]
        self.queue.cancel(operations[1]["public"]["id"])
        self.queue.submit("barrier", lambda: calls.append("barrier"))
        final = self.queue.submit("sync", lambda: calls.append("sync"), combine_key="sync")
        release.set()
        self.queue.wait(final)
        self.assertEqual(calls, ["sync", "barrier", "sync"])
        for operation in operations + [final]:
            self.assert_lifecycle(operation)

    def test_concurrent_submitters_keep_acceptance_order(self):
        _, release = self.block()
        calls = []
        submitters = [threading.Thread(target=lambda index=index: self.queue.submit(str(index), lambda: calls.append(str(index)))) for index in range(20)]
        for submitter in submitters:
            submitter.start()
        for submitter in submitters:
            submitter.join(2)
            self.assertFalse(submitter.is_alive())
        expected = [item["action"] for item in self.queue.snapshot()["operations"] if item["status"] == "queued"]
        drained = self.queue.submit("drain", lambda: None)
        release.set()
        self.queue.wait(drained)
        self.assertEqual(calls, expected)

    def test_skipped_batch_member_is_not_counted_while_valid_member_runs(self):
        _, release = self.block()
        running, finish = threading.Event(), threading.Event()
        self.addCleanup(finish.set)
        skipped = self.queue.submit("invalid", mock.Mock(), combine_key="same", validate=mock.Mock(side_effect=OperationSkipped("stale")))
        valid = self.queue.submit("valid", lambda: (running.set(), finish.wait(5)), combine_key="same")
        release.set()
        self.assertTrue(running.wait(2))
        self.assertEqual([item["id"] for item in self.queue.snapshot()["operations"]], [valid["public"]["id"]])
        self.assertEqual(self.queue.cancel(skipped["public"]["id"])["status"], "skipped")
        finish.set()
        self.queue.wait(valid)

    def test_running_batch_is_not_extended(self):
        started, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        first = self.queue.submit("sync", lambda: (started.set(), release.wait(5)), combine_key="sync")
        self.assertTrue(started.wait(2))
        callback = mock.Mock(return_value="second")
        second = self.queue.submit("sync", callback, combine_key="sync")
        release.set()
        self.assertEqual(self.queue.wait(second), "second")
        callback.assert_called_once()
        self.assert_lifecycle(first)

    def test_invalid_work_skips_and_failure_does_not_stop_worker(self):
        _, release = self.block()
        callback = mock.Mock()
        skipped = self.queue.submit("invalid", callback, validate=mock.Mock(side_effect=OperationSkipped("target disappeared")))
        failed = self.queue.submit("failed", mock.Mock(side_effect=RuntimeError("offline")))
        final = self.queue.submit("valid", lambda: 42)
        release.set()
        self.assertEqual(self.queue.wait(final), 42)
        callback.assert_not_called()
        self.assertEqual(skipped["public"]["status"], "skipped")
        self.assertEqual(failed["public"]["status"], "failed")
        for operation in (skipped, failed, final):
            self.assert_lifecycle(operation)

    def test_shutdown_cancels_waiters_and_joins_running_operation(self):
        first, release = self.block()
        callback = mock.Mock()
        waiting = self.queue.submit("waiting", callback)
        stopped = threading.Event()
        closer = threading.Thread(target=lambda: (self.queue.close(), stopped.set()))
        closer.start()
        try:
            self.assertTrue(waiting["done"].wait(2))
            self.assertFalse(stopped.is_set())
            self.assertEqual(waiting["public"]["status"], "cancelled")
            with self.assertRaises(OperationCancelled):
                self.queue.submit("later", callback)
        finally:
            release.set()
            closer.join(3)
        self.assertFalse(closer.is_alive())
        callback.assert_not_called()
        self.assert_lifecycle(first)
        self.assert_lifecycle(waiting)

    def test_history_is_bounded_and_restart_has_new_identity(self):
        for index in range(205):
            self.queue.wait(self.queue.submit("sync", lambda: index))
        snapshot = self.queue.snapshot()
        self.assertEqual(len(snapshot["completed"]), 200)
        self.assertEqual(len(snapshot["events"]), 400)
        self.assertNotEqual(snapshot["sessionId"], CloudOperationQueue().session_id)
        snapshot["completed"].clear()
        self.assertEqual(len(self.queue.snapshot()["completed"]), 200)


class CloudQueueIntegrationTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parent / (".test-cloud-queue-" + uuid.uuid4().hex)
        root.mkdir()
        self.addCleanup(shutil.rmtree, root)
        self.cloud = CloudManager(root / "private", SkillManager(root / "codex", root / "private", root / "gemini"), None)
        self.addCleanup(self.cloud.operation_queue.close)
        self.cloud._config["webdav"]["enabled"] = True
        self.state = SimpleNamespace(cloud=self.cloud, _signal_dashboard_cache=mock.Mock())

    def block(self):
        started, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        self.cloud.operation_queue.submit("block", lambda: (started.set(), release.wait(5)))
        self.assertTrue(started.wait(2))
        return release

    def wait_id(self, operation_id):
        # Join this finite worker, which exits as soon as the queue drains.
        worker = self.cloud.operation_queue.worker
        if worker:
            worker.join(5)
            self.assertFalse(worker.is_alive())
        return next(item for item in self.cloud.operation_queue.snapshot()["completed"] if item["id"] == operation_id)

    def test_nested_sync_adapter_has_one_lifecycle_and_same_worker(self):
        calls = []

        class NestedCloud(CloudManager):
            @_serialized_cloud_operation
            def outer(self):
                calls.append(threading.current_thread())
                return self.inner()

            @_serialized_cloud_operation
            def inner(self):
                calls.append(threading.current_thread())
                return {"done": True}

        self.cloud.__class__ = NestedCloud
        self.assertEqual(self.cloud.outer(), {"done": True})
        self.assertIs(calls[0], calls[1])
        self.assertEqual([event["phase"] for event in self.cloud.operation_queue.snapshot()["events"]], ["start", "end"])

    def test_generation_change_skips_waiting_request(self):
        release = self.block()
        with mock.patch.object(self.cloud, "test", return_value={}) as execute:
            accepted = monitor_dashboard.enqueue_management_action(self.state, None, "/api/manage/cloud/test", {})
            self.cloud._config["webdav"]["remoteRoot"] = "changed"
            release.set()
            result = self.wait_id(accepted["operationId"])
        self.assertEqual(result["status"], "skipped")
        execute.assert_not_called()

    def test_removing_whole_skill_action_prevents_local_changes(self):
        release = self.block()
        accepted = monitor_dashboard.enqueue_management_action(self.state, None, "/api/manage/skills/share", {"name": "alpha", "shared": True})
        with mock.patch.object(self.cloud, "set_skill_shared") as execute:
            self.cloud.operation_queue.cancel(accepted["operationId"])
            release.set()
            self.assertEqual(self.wait_id(accepted["operationId"])["status"], "cancelled")
        execute.assert_not_called()

    def test_missing_skill_is_skipped_before_mutation(self):
        with mock.patch.object(self.cloud, "unmanage_skill") as execute:
            accepted = monitor_dashboard.enqueue_management_action(self.state, None, "/api/manage/skills/unmanage", {"name": "missing"})
            self.assertEqual(self.wait_id(accepted["operationId"])["status"], "skipped")
        execute.assert_not_called()

    def test_remote_revision_and_restore_snapshot_are_revalidated(self):
        self.cloud.accounts = SimpleNamespace(lock=threading.RLock(), manifest={"accounts": []})
        with mock.patch.object(self.cloud, "account_state", return_value=({"revisionId": "new"}, '"new"')):
            with self.assertRaises(OperationSkipped):
                self.cloud.validate_queued_action("bind_local_account", {"accountKey": "key"}, {"etag": '"old"', "state": {"revisionId": "old"}})
        with mock.patch.object(self.cloud, "_remote_snapshot", return_value=(b"data", "new", '"etag"')):
            with self.assertRaises(OperationSkipped):
                self.cloud.validate_queued_action("restore_skills", {"snapshotId": "old"})

    def test_invalid_requests_are_rejected_before_acceptance(self):
        for body in ({"name": "../alpha", "shared": True}, {"name": "alpha", "shared": "false"}, []):
            with self.assertRaises(CloudError):
                monitor_dashboard.enqueue_management_action(self.state, None, "/api/manage/skills/share", body)
        self.assertEqual(self.cloud.operation_queue.snapshot()["events"], [])

    def test_automatic_batch_falls_back_per_skill_and_reports_partial_failure(self):
        with mock.patch.object(self.cloud, "upload_skills", side_effect=(CloudError("index changed", 409), {"changed": True}, CloudError("offline", 502))) as upload:
            result = self.cloud._upload_due_skills({"alpha", "beta"})
        self.assertEqual(upload.call_args_list, [mock.call({"alpha", "beta"}), mock.call({"alpha"}), mock.call({"beta"})])
        self.assertEqual(set(result["errors"]), {"beta"})
        snapshot = self.cloud.operation_queue.snapshot()
        self.assertEqual([event["phase"] for event in snapshot["events"]], ["start", "end"])
        self.assertEqual(snapshot["completed"][-1]["status"], "failed")
        json.dumps(snapshot)

    def test_pending_local_change_is_failed_but_sync_adapter_preserves_result(self):
        result = {"cloud": {"pending": True, "error": "offline"}}
        with mock.patch.object(self.cloud, "set_skill_shared", return_value=result):
            self.assertEqual(self.cloud.operation_queue.wait(self.cloud.submit_operation("set_skill_shared", ("alpha", True))), result)
        self.assertEqual(self.cloud.operation_queue.snapshot()["completed"][-1]["status"], "failed")

    def test_public_records_redact_secrets_and_keep_useful_flags(self):
        self.cloud._config["webdav"]["password"] = "private-password"
        with mock.patch.object(self.cloud, "test", return_value={"password": "private-password", "controlPasswordChanged": True, "nested": {"apiKey": "private-key"}}):
            self.cloud.operation_queue.wait(self.cloud.submit_operation("test"))
        snapshot = self.cloud.operation_queue.snapshot()
        self.assertTrue(snapshot["completed"][-1]["result"]["controlPasswordChanged"])
        self.assertNotIn("private-password", json.dumps(snapshot))
        self.assertNotIn("private-key", json.dumps(snapshot))

    def test_new_config_secret_is_redacted_even_when_update_fails(self):
        with mock.patch.object(self.cloud, "update_config", side_effect=CloudError("Cannot use new-private-passphrase", 409)):
            operation = self.cloud.submit_operation("update_config", ({"webdav": {"encryptionPassphrase": "new-private-passphrase"}},))
            with self.assertRaises(CloudError):
                self.cloud.operation_queue.wait(operation)
        self.assertNotIn("new-private-passphrase", json.dumps(self.cloud.operation_queue.snapshot()))

    def test_cancelled_automatic_upload_is_not_resubmitted_without_new_changes(self):
        release = self.block()
        self.cloud._observed_skill_hashes = {"alpha": "modified"}
        self.cloud._pending_skill_pushes = {"alpha": {"hash": "modified", "since": 0, "nextAttemptAt": 120, "attempts": 0}}
        self.cloud._last_auto_fetch_at = 1000
        submitted = threading.Event()
        submit = self.cloud.operation_queue.submit

        def record_submit(*args, **kwargs):
            operation = submit(*args, **kwargs)
            submitted.set()
            return operation

        with mock.patch.object(self.cloud.skills, "content_hashes", return_value={"alpha": "modified"}), mock.patch.object(self.cloud, "upload_skills") as upload, mock.patch.object(self.cloud.operation_queue, "submit", side_effect=record_submit):
            scheduler = threading.Thread(target=lambda: self.cloud.maintenance_tick(now=120), name="cloud-maintenance")
            scheduler.start()
            try:
                self.assertTrue(submitted.wait(2))
                waiting = next(item for item in self.cloud.operation_queue.snapshot()["operations"] if item["action"] == "_upload_due_skills")
                self.assertEqual(waiting["source"], "automatic")
                self.cloud.operation_queue.cancel(waiting["id"])
            finally:
                release.set()
                scheduler.join(3)
            self.assertFalse(scheduler.is_alive())
            self.cloud.maintenance_tick(now=150)
            upload.assert_not_called()
        self.assertNotIn("alpha", self.cloud._pending_skill_pushes)

    def test_http_queue_is_authenticated_nonblocking_and_cancellable(self):
        salt = new_control_password_salt()
        self.cloud._config["control"].update(passwordSalt=salt, passwordHash=hash_control_password("test-queue-password", salt))
        auth = monitor_dashboard.ControlAuth(self.cloud.config()["control"])
        fake_state = mock.Mock(cloud=self.cloud)
        fake_server = mock.Mock()
        fake_server.serve_forever.side_effect = KeyboardInterrupt
        with mock.patch.object(monitor_dashboard, "DashboardInstanceLock"), mock.patch.object(monitor_dashboard, "DashboardHTTPServer", return_value=fake_server) as server_factory, mock.patch.object(monitor_dashboard, "UsageDashboardState", return_value=fake_state), mock.patch.object(monitor_dashboard.threading, "Thread"), mock.patch("sys.stdout", new=io.StringIO()):
            monitor_dashboard.serve_dashboard(SimpleNamespace(data_home=self.cloud.private_root, dashboard=False, timeout=1), None)
        self.cloud._queue = CloudOperationQueue()
        self.addCleanup(self.cloud.operation_queue.close)
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server_factory.call_args.args[1])
        worker = threading.Thread(target=server.serve_forever)
        worker.start()

        def stop_server():
            server.shutdown()
            server.server_close()
            worker.join(3)
            self.assertFalse(worker.is_alive())

        self.addCleanup(stop_server)
        url = f"http://127.0.0.1:{server.server_port}"

        def request(path, body=None, authenticated=True):
            headers = {"Content-Type": "application/json"}
            if authenticated:
                headers["Cookie"] = f"{monitor_dashboard.CONTROL_COOKIE_NAME}={auth.create_token()}"
            with urllib.request.urlopen(urllib.request.Request(url + path, data=json.dumps(body).encode() if body is not None else None, headers=headers), timeout=2) as response:
                return response.status, json.load(response)

        with self.assertRaises(urllib.error.HTTPError) as error:
            request("/api/manage/cloud/queue", authenticated=False)
        self.assertEqual(error.exception.code, 401)
        error.exception.close()
        release = self.block()
        with mock.patch.object(self.cloud, "test", return_value={}) as execute:
            status, accepted = request("/api/manage/cloud/test", {})
            self.assertEqual(status, 202)
            self.assertEqual(len(request("/api/manage/cloud/queue")[1]["operations"]), 2)
            self.assertEqual(request("/api/manage/cloud/queue/cancel", {"operationId": accepted["operationId"]})[1]["status"], "cancelled")
            release.set()
            self.wait_id(accepted["operationId"])
            execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()
