import json
import re
import shutil
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import monitor_migrations
import monitor_cloud
from monitor_cloud import CLOUD_LOCK_PATH, CloudError, CloudManager, WebDavClient
from monitor_migrations import CLOUD_DATA_CONTRACT_PATH


def lock_response(token="opaquelocktoken:test", timeout="Second-300", depth="infinity"):
    return (f'<D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock><D:lockscope><D:exclusive/></D:lockscope><D:depth>{depth}</D:depth>'
        f'<D:timeout>{timeout}</D:timeout><D:locktoken><D:href>{token}</D:href></D:locktoken></D:activelock></D:lockdiscovery></D:prop>').encode()


class CloudContractFixture(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-cloud-contract-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.files = {CLOUD_LOCK_PATH: (b'{"writing":0}', '"lock"'), "crypto.json": (b'{"format":"codex-switch-plain","version":1}', '"1"')}
        self.calls = []
        self.token = None
        self.revision = 1
        self.timeout = "Second-300"
        self.directories = set()
        self.reentrant = False
        self.tokens = set()
        self.reject_unmapped_locks = False
        self.fail_idle_status = False
        self.fail_busy_status = False
        self.ignore_tagged_etags = False
        self.ignore_if_match = False
        self.omit_etags = False
        self.native_status = None
        self.mutex = threading.RLock()
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def handle_request(self):
                with owner.mutex:
                    path = self.path.removeprefix("/root").strip("/")
                    body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                    owner.calls.append((self.command, path, dict(self.headers)))
                    status, result, etag = 200, b"", None
                    conditions = re.findall(r"<([^>]+)>\s*\(([^)]*)\)", self.headers.get("If", ""))
                    invalid = False
                    for resource, condition in conditions:
                        resource = resource.split("/root/", 1)[-1]
                        for expected_etag in re.findall(r"\[([^]]+)\]", condition):
                            invalid |= not owner.ignore_tagged_etags and owner.files.get(resource, (None, None))[1] != expected_etag
                        for expected_token in re.findall(r"<([^>]+)>", condition):
                            invalid |= expected_token not in owner.tokens
                    if invalid:
                        status = 412
                    elif self.command == "LOCK":
                        if owner.native_status:
                            status = owner.native_status
                        elif path != CLOUD_LOCK_PATH:
                            status = 403
                        elif path not in owner.files and owner.reject_unmapped_locks:
                            status = 404
                        elif not owner.reentrant and owner.token and owner.token not in self.headers.get("If", ""):
                            status = 423
                        else:
                            if path not in owner.files:
                                owner.files[path] = b"", '"empty"'
                                status = 201
                            owner.token = f"opaquelocktoken:{uuid.uuid4().hex}" if owner.reentrant else owner.token or f"opaquelocktoken:{uuid.uuid4().hex}"
                            owner.tokens.add(owner.token)
                            result = lock_response(owner.token, timeout=owner.timeout, depth="0")
                    elif self.command == "PROPFIND":
                        status, result = 207, b'<D:multistatus xmlns:D="DAV:"></D:multistatus>'
                    elif self.command == "UNLOCK":
                        token = self.headers.get("Lock-Token", "").strip("<>")
                        if token not in owner.tokens:
                            status = 409
                        else:
                            owner.tokens.remove(token)
                            owner.token, status = next(iter(owner.tokens), None), 204
                    elif self.command == "MKCOL":
                        status = 405 if not path or path in owner.directories else 201
                        owner.directories.add(path)
                    elif path == CLOUD_LOCK_PATH and owner.token and not any(token in self.headers.get("If", "") for token in (owner.tokens if owner.reentrant else [owner.token])) and self.command != "GET":
                        status = 423
                    elif self.command == "GET":
                        if path not in owner.files:
                            status = 404
                        else:
                            result, etag = owner.files[path]
                    elif self.command == "PUT":
                        if path == CLOUD_LOCK_PATH and (owner.fail_idle_status and json.loads(body) == {"writing": 0} or owner.fail_busy_status and json.loads(body).get("writing") == 1):
                            status = 503
                        elif self.headers.get("If-None-Match") == "*" and path in owner.files or not owner.ignore_if_match and self.headers.get("If-Match") and self.headers["If-Match"] != owner.files.get(path, (None, None))[1]:
                            status = 412
                        else:
                            owner.revision += 1
                            etag = f'"{owner.revision}"'
                            owner.files[path], status = (body, etag), 201
                    elif self.command == "DELETE":
                        owner.files.pop(path, None)
                        owner.directories.discard(path)
                        status = 204
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(result)))
                    if etag and not owner.omit_etags:
                        self.send_header("ETag", etag)
                    self.end_headers()
                    self.wfile.write(result)

            do_GET = do_PUT = do_DELETE = do_MKCOL = do_LOCK = do_UNLOCK = handle_request

        Handler.do_PROPFIND = Handler.handle_request
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.config = {"enabled": True, "baseUrl": f"http://127.0.0.1:{self.server.server_port}/", "remoteRoot": "root", "encryptionPassphraseHash": ""}

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def client(self):
        client = WebDavClient(self.config)
        def stop():
            client.access_lock.stop.set()
            if client.access_lock.worker:
                client.access_lock.worker.join(3)
                self.assertFalse(client.access_lock.worker.is_alive())
        self.addCleanup(stop)
        return client

    def manager(self, name="one"):
        manager = CloudManager(self.root / name, SimpleNamespace(), None)
        manager._config["webdav"].update(self.config)
        self.addCleanup(manager.operation_queue.close)
        manager.read_payload = lambda: manager._connection()[0].get("payload")
        manager.write_payload = lambda: manager._connection()[0].put("payload", b"new")
        return manager

    def run_operation(self, manager, action="read_payload"):
        return manager.operation_queue.wait(manager.submit_operation(action))

    def mark_version(self, version):
        self.files[CLOUD_DATA_CONTRACT_PATH] = json.dumps({"dataContractVersion": version}).encode(), '"contract"'


class CloudContractTests(CloudContractFixture):
    def test_missing_marker_is_adopted_without_rewriting_existing_data(self):
        self.files["payload"] = b"old", '"payload"'
        manager = self.manager()
        self.assertEqual(self.run_operation(manager)[0], b"old")
        self.assertEqual(json.loads(self.files[CLOUD_DATA_CONTRACT_PATH][0]), {"dataContractVersion": monitor_migrations.CLOUD_DATA_CONTRACT_VERSION})
        self.assertEqual(self.files["payload"][0], b"old")
        self.assertIsNone(self.token)
        events = manager.operation_queue.snapshot()["events"]
        self.assertEqual([event["phase"] for event in events], ["start", "end"])
        self.assertEqual(events[-1]["operation"]["status"], "succeeded")

    def test_equal_version_does_not_run_migration(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        self.files["payload"] = b"old", '"payload"'
        with mock.patch.dict(monitor_migrations.CLOUD_DATA_MIGRATIONS, {1: mock.Mock(side_effect=AssertionError("unexpected migration"))}):
            self.run_operation(self.manager())
        self.assertFalse(any(method == "PUT" and path != CLOUD_LOCK_PATH and not path.startswith(".lock-probe-") for method, path, _ in self.calls))

    def test_newer_contract_blocks_reads_writes_and_overwrite_before_locking(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION + 1)
        manager = self.manager()
        for action in ("read_payload", "write_payload", "overwrite_cloud_from_local"):
            with self.subTest(action=action), self.assertRaisesRegex(CloudError, "local script is updated"):
                self.run_operation(manager, action)
        self.assertTrue(all(method == "GET" and path == CLOUD_DATA_CONTRACT_PATH for method, path, _ in self.calls))
        self.assertEqual(len(manager.operation_queue.snapshot()["events"]), 6)

    def test_recheck_under_lock_blocks_upgrade_race(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        original = WebDavClient.acquire_file_lock
        def racing_acquire(client):
            original(client)
            self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION + 1)
        with mock.patch.object(WebDavClient, "acquire_file_lock", racing_acquire), self.assertRaisesRegex(CloudError, "newer"):
            self.run_operation(self.manager())
        self.assertIsNone(self.token)
        self.assertFalse(any(path == "payload" for _, path, _ in self.calls))

    def test_failed_migration_preserves_marker_and_payload_and_retries(self):
        self.mark_version(0)
        self.files["payload"] = b"old", '"payload"'
        manager = self.manager()
        with mock.patch.dict(monitor_migrations.CLOUD_DATA_MIGRATIONS, {1: mock.Mock(side_effect=RuntimeError("interrupted"))}), self.assertRaisesRegex(RuntimeError, "interrupted"):
            self.run_operation(manager)
        self.assertEqual(json.loads(self.files[CLOUD_DATA_CONTRACT_PATH][0])["dataContractVersion"], 0)
        self.assertEqual(self.files["payload"][0], b"old")
        self.assertIsNone(self.token)
        self.assertEqual(self.run_operation(manager)[0], b"old")

    def test_pending_migrations_run_in_order_and_resume_at_last_commit(self):
        self.mark_version(1)
        self.files["payload"] = b"old", '"payload"'
        applied = []
        def fail(client):
            applied.append(3)
            raise RuntimeError("interrupted")
        manager = self.manager()
        with mock.patch.object(monitor_migrations, "CLOUD_DATA_CONTRACT_VERSION", 3), mock.patch.dict(monitor_migrations.CLOUD_DATA_MIGRATIONS, {2: lambda client: applied.append(2), 3: fail}):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                self.run_operation(manager)
            self.assertEqual(json.loads(self.files[CLOUD_DATA_CONTRACT_PATH][0])["dataContractVersion"], 2)
            with mock.patch.dict(monitor_migrations.CLOUD_DATA_MIGRATIONS, {3: lambda client: applied.append(3)}):
                self.run_operation(manager)
        self.assertEqual(applied, [2, 3, 3])

    def test_missing_step_and_invalid_marker_fail_closed(self):
        manager = self.manager()
        for value in (b"broken", b'{"dataContractVersion":true}', b'{"dataContractVersion":-1}', b"[]"):
            self.files[CLOUD_DATA_CONTRACT_PATH] = value, '"contract"'
            with self.subTest(value=value), self.assertRaisesRegex(CloudError, "Invalid cloud"):
                self.run_operation(manager)
        self.mark_version(0)
        with mock.patch.dict(monitor_migrations.CLOUD_DATA_MIGRATIONS, {}, clear=True), self.assertRaisesRegex(CloudError, "Missing cloud"):
            self.run_operation(manager)
        self.assertIsNone(self.token)

    def test_other_client_cannot_migrate_or_write_during_operation(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        manager = self.manager()
        def held_operation():
            manager._connection()
            other = self.manager("two")
            with self.assertRaises(CloudError) as blocked:
                self.run_operation(other, "write_payload")
            self.assertEqual(blocked.exception.http_status, 423)
            # Uncooperative clients are outside the coordination protocol; all app clients lock the file.
            manager._connection()[0].put("payload", b"safe")
        manager.held_operation = held_operation
        self.run_operation(manager, "held_operation")
        self.assertEqual(self.files["payload"][0], b"safe")
        self.assertEqual(sum(method == "UNLOCK" for method, _, _ in self.calls), 3)

    def test_lock_refresh_and_expiration_stop_access(self):
        client = self.client()
        client.acquire_file_lock()
        client.access_lock.renew()
        self.assertEqual(sum(method == "LOCK" for method, _, _ in self.calls), 2)
        client.put("payload", b"new")
        client.access_lock.deadline = 0
        before = len(self.calls)
        with self.assertRaisesRegex(CloudError, "expired"):
            client.get("payload")
        self.assertEqual(len(self.calls), before)
        with self.assertRaisesRegex(CloudError, "unusable|expired"):
            client.release_file_lock()
        self.assertIsNone(self.token)
        self.assertEqual(json.loads(self.files[CLOUD_LOCK_PATH][0])["writing"], 1)

    def test_unsupported_locks_block_payload_access(self):
        manager = self.manager()
        with mock.patch.object(WebDavClient, "acquire_file_lock", side_effect=CloudError("LOCK unsupported", 502)), self.assertRaisesRegex(CloudError, "unsupported"):
            self.run_operation(manager)
        self.assertFalse(any(path in {"payload", "crypto.json"} for _, path, _ in self.calls))

    def test_lock_response_requires_exclusive_valid_file_lock(self):
        for response in (b"bad", lock_response(timeout="Second-0"), lock_response(depth="1"), lock_response().replace(b"exclusive", b"shared")):
            client = self.client()
            with mock.patch.object(client.access_lock, "_request", return_value=(response, None, 200)), self.subTest(response=response), self.assertRaises(CloudError):
                with client.access_lock._native():
                    pass

    def test_read_only_operation_claims_owner_without_busy_status(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        self.files["payload"] = b"old", '"payload"'
        self.run_operation(self.manager())
        self.assertFalse(any(method == "PUT" and path != CLOUD_LOCK_PATH and not path.startswith(".lock-probe-") for method, path, _ in self.calls))
        self.assertEqual(sum(method == "LOCK" for method, _, _ in self.calls), 2)
        self.assertTrue(all(path == CLOUD_LOCK_PATH for method, path, _ in self.calls if method in {"LOCK", "UNLOCK"}))
        payload_headers = next(headers for method, path, headers in self.calls if path == "payload")
        self.assertNotIn("If", payload_headers)

    def test_write_status_wraps_all_nested_writes_once(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        manager = self.manager()
        observed = []
        def write_twice():
            client = manager._connection()[0]
            client.put("payload", b"one")
            observed.append(json.loads(self.files[CLOUD_LOCK_PATH][0]))
            manager._connection()[0].put("payload", b"two")
            observed.append(json.loads(self.files[CLOUD_LOCK_PATH][0]))
        manager.write_twice = write_twice
        self.run_operation(manager, "write_twice")
        self.assertTrue(all(state["writing"] == 1 and state.get("owner") for state in observed))
        self.assertEqual(json.loads(self.files[CLOUD_LOCK_PATH][0]), {"writing": 0})
        self.assertEqual(sum(method == "PUT" and path == CLOUD_LOCK_PATH for method, path, _ in self.calls), 3)
        self.assertEqual(sum(method == "LOCK" for method, _, _ in self.calls), 3)
        self.assertEqual([e["phase"] for e in manager.operation_queue.snapshot()["events"]], ["start", "end"])

    def test_writer_excludes_both_readers_and_writers_until_cleanup(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        manager = self.manager()
        other = self.manager("other")
        def held_write():
            manager._connection()[0].put("payload", b"published")
            for action in ("read_payload", "write_payload"):
                with self.assertRaises(CloudError) as busy:
                    self.run_operation(other, action)
                self.assertEqual(busy.exception.http_status, 423)
            self.assertEqual(self.files["payload"][0], b"published")
        manager.held_write = held_write
        self.run_operation(manager, "held_write")
        self.assertEqual(self.run_operation(other)[0], b"published")

    def test_exception_clears_status_and_unlocks(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        manager = self.manager()
        def interrupted():
            manager._connection()[0].put("payload", b"published")
            raise RuntimeError("interrupted")
        manager.interrupted = interrupted
        with self.assertRaisesRegex(RuntimeError, "interrupted"):
            self.run_operation(manager, "interrupted")
        self.assertEqual(json.loads(self.files[CLOUD_LOCK_PATH][0]), {"writing": 0})
        self.assertIsNone(self.token)
        self.assertEqual(manager.operation_queue.snapshot()["events"][-1]["operation"]["status"], "failed")

    def test_infinite_file_lock_is_accepted_and_released(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        self.timeout = "Infinite"
        self.run_operation(self.manager(), "write_payload")
        self.assertIsNone(self.token)
        self.assertEqual(json.loads(self.files[CLOUD_LOCK_PATH][0]), {"writing": 0})

    def test_bootstrap_missing_lock_file_for_both_server_behaviors(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        self.files["payload"] = b"retained", '"payload"'
        for reject in (False, True):
            self.files.pop(CLOUD_LOCK_PATH, None)
            self.reject_unmapped_locks = reject
            self.assertEqual(self.run_operation(self.manager(str(reject)))[0], b"retained")
            self.assertEqual(json.loads(self.files[CLOUD_LOCK_PATH][0]), {"writing": 0})
            self.assertIsNone(self.token)

    def test_interrupted_or_invalid_status_blocks_payload_access(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        for state in (b'{"writing":1}', b'{"writing":false}', b'{}', b'broken', b''):
            self.files[CLOUD_LOCK_PATH] = state, '"state"'
            with self.subTest(state=state), self.assertRaisesRegex(CloudError, "lock (file|status)"):
                self.run_operation(self.manager(state.hex()), "write_payload")
            self.assertNotIn("payload", self.files)
            self.assertEqual(self.files[CLOUD_LOCK_PATH][0], state)
            self.assertIsNone(self.token)

    def test_status_reset_failure_unlocks_and_reports_failure(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        self.fail_idle_status = True
        manager = self.manager()
        with self.assertRaises(CloudError):
            self.run_operation(manager, "write_payload")
        self.assertIsNone(self.token)
        self.assertEqual(json.loads(self.files[CLOUD_LOCK_PATH][0])["writing"], 1)
        self.assertEqual(manager.operation_queue.snapshot()["events"][-1]["operation"]["status"], "failed")

    def test_busy_status_failure_blocks_caught_retry_before_payload_write(self):
        client = self.client()
        client.acquire_file_lock()
        self.fail_busy_status = True
        try:
            with self.assertRaises(CloudError):
                client.put("payload", b"first")
            with self.assertRaisesRegex(CloudError, "unusable"):
                client.put("payload", b"retry")
            self.assertNotIn("payload", self.files)
        finally:
            with self.assertRaises(CloudError):
                client.release_file_lock()

    def test_lost_lease_fences_payload_writes(self):
        client = self.client()
        client.acquire_file_lock()
        client.access_lock.prepare("PUT")
        self.files[CLOUD_LOCK_PATH] = b'{"writing":0,"owner":"another-owner","expiresAt":9999999999}', '"replacement"'
        with self.assertRaises(CloudError):
            client.access_lock.renew()
        with self.assertRaises(CloudError):
            client.put("payload", b"stale")
        self.assertNotIn("payload", self.files)
        with self.assertRaises(CloudError):
            client.release_file_lock()
        self.assertEqual(json.loads(self.files[CLOUD_LOCK_PATH][0])["owner"], "another-owner")

    def test_overwrite_preserves_lock_file_and_contract(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        manager = self.manager()
        manager.upload_skills = lambda: {}
        self.files["payload"] = b"old", '"payload"'
        with mock.patch.object(WebDavClient, "list", return_value=[CLOUD_LOCK_PATH, CLOUD_DATA_CONTRACT_PATH, "payload"]):
            self.run_operation(manager, "overwrite_cloud_from_local")
        self.assertNotIn("payload", self.files)
        self.assertIn(CLOUD_LOCK_PATH, self.files)
        self.assertIn(CLOUD_DATA_CONTRACT_PATH, self.files)
        self.assertIsNone(self.token)

    def test_reentrant_native_locks_cannot_bypass_atomic_owner_claim(self):
        self.reentrant = True
        first = self.client()
        second = self.client()
        # Both saw idle before either native LOCK; rechecking under LOCK alone
        # is still racy, so interleave both reads immediately before conditional PUT.
        original = first.access_lock._write
        def claim_after_other(state, etag, token, deadline):
            if not first.access_lock.claimed:
                second.acquire_file_lock()
            return original(state, etag, token, deadline)
        with mock.patch.object(first.access_lock, "_write", side_effect=claim_after_other), self.assertRaises(CloudError) as lost:
            first.acquire_file_lock()
        self.assertEqual(lost.exception.http_status, 412)
        self.assertEqual(json.loads(self.files[CLOUD_LOCK_PATH][0])["owner"], second.access_lock.owner)
        second.put("payload", b"winner")
        second.release_file_lock()
        self.assertEqual(self.files["payload"][0], b"winner")
        self.assertIsNone(self.token)

    def test_stranded_initialization_gate_is_not_stolen(self):
        self.files.pop(CLOUD_LOCK_PATH)
        self.directories.add(monitor_cloud.CLOUD_LOCK_INIT_PATH)
        with self.assertRaisesRegex(CloudError, "initialization is busy"):
            self.run_operation(self.manager())
        self.assertNotIn(CLOUD_LOCK_PATH, self.files)
        self.assertIn(monitor_cloud.CLOUD_LOCK_INIT_PATH, self.directories)

    def test_released_v1_migrates_without_changing_payloads(self):
        self.mark_version(1)
        self.files["payload"] = b"retained", '"payload"'
        self.assertEqual(self.run_operation(self.manager())[0], b"retained")
        self.assertEqual(json.loads(self.files[CLOUD_DATA_CONTRACT_PATH][0]), {"dataContractVersion": monitor_migrations.CLOUD_DATA_CONTRACT_VERSION})
        self.assertEqual(self.files["payload"][0], b"retained")

    def test_automatic_skill_batch_checks_busy_lock_only_once(self):
        self.mark_version(monitor_migrations.CLOUD_DATA_CONTRACT_VERSION)
        self.files[CLOUD_LOCK_PATH] = b'{"writing":0,"owner":"another-operation"}', '"busy"'
        manager = self.manager()
        names = {f"skill-{index}" for index in range(16)}
        result = manager._upload_due_skills(names)
        self.assertEqual(set(result["errors"]), names)
        self.assertTrue(all(error.category == "lock" for error in result["errors"].values()))
        self.assertEqual([(method, path) for method, path, _ in self.calls], [("GET", CLOUD_DATA_CONTRACT_PATH), ("GET", CLOUD_LOCK_PATH)])
        self.assertEqual(self.files[CLOUD_LOCK_PATH][0], b'{"writing":0,"owner":"another-operation"}')
        self.assertEqual([event["phase"] for event in manager.operation_queue.snapshot()["events"]], ["start", "end"])

    def test_automatic_fetch_retries_contention_without_waiting_an_hour(self):
        manager = self.manager()
        manager._last_auto_fetch_at = 0
        busy = CloudError("busy", 409, category="lock")
        with mock.patch.object(manager, "fetch", side_effect=[busy, {}]) as fetch:
            self.assertFalse(manager._auto_fetch_if_due(monitor_cloud.AUTO_FETCH_INTERVAL_SECONDS))
            self.assertFalse(manager._auto_fetch_if_due(monitor_cloud.AUTO_FETCH_INTERVAL_SECONDS + monitor_cloud.AUTO_PUSH_RETRY_SECONDS - 1))
            self.assertTrue(manager._auto_fetch_if_due(monitor_cloud.AUTO_FETCH_INTERVAL_SECONDS + monitor_cloud.AUTO_PUSH_RETRY_SECONDS))
        self.assertEqual(fetch.call_count, 2)

    def test_automatic_usage_retries_contention_without_waiting_normal_interval(self):
        manager = self.manager()
        manager._usage_data = SimpleNamespace()
        manager._next_usage_sync_at = 0
        busy = CloudError("busy", 409, category="lock")
        with mock.patch.object(manager, "sync_usage_data", side_effect=[busy, {}]) as sync:
            self.assertFalse(manager._auto_usage_sync_if_due(0))
            self.assertFalse(manager._auto_usage_sync_if_due(monitor_cloud.AUTO_PUSH_RETRY_SECONDS - 1))
            self.assertTrue(manager._auto_usage_sync_if_due(monitor_cloud.AUTO_PUSH_RETRY_SECONDS))
        self.assertEqual(sync.call_count, 2)


if __name__ == "__main__":
    unittest.main()
