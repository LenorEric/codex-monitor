import json
import shutil
import threading
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import monitor_cloud
from monitor_cloud import CLOUD_DATA_CONTRACT_PATH, CloudError, CloudManager, WebDavClient


def lock_response(token="opaquelocktoken:test", timeout="Second-300", depth="infinity"):
    return (f'<D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock><D:lockscope><D:exclusive/></D:lockscope><D:depth>{depth}</D:depth>'
        f'<D:timeout>{timeout}</D:timeout><D:locktoken><D:href>{token}</D:href></D:locktoken></D:activelock></D:lockdiscovery></D:prop>').encode()


class CloudContractTests(unittest.TestCase):
    def setUp(self):
        self.root = Path.cwd() / f".test-cloud-contract-{uuid.uuid4().hex}"
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.files = {"crypto.json": (b'{"format":"codex-switch-plain","version":1}', '"1"')}
        self.calls = []
        self.token = None
        self.revision = 1
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
                    if self.command == "LOCK":
                        if owner.token and owner.token not in self.headers.get("If", ""):
                            status = 423
                        else:
                            owner.token = owner.token or f"opaquelocktoken:{uuid.uuid4().hex}"
                            result = lock_response(owner.token)
                    elif self.command == "UNLOCK":
                        if self.headers.get("Lock-Token") != f"<{owner.token}>":
                            status = 409
                        else:
                            owner.token, status = None, 204
                    elif self.command == "MKCOL":
                        status = 405
                    elif owner.token and owner.token not in self.headers.get("If", "") and self.command != "GET":
                        status = 423
                    elif self.command == "GET":
                        if path not in owner.files:
                            status = 404
                        else:
                            result, etag = owner.files[path]
                    elif self.command == "PUT":
                        if self.headers.get("If-None-Match") == "*" and path in owner.files or self.headers.get("If-Match") and self.headers["If-Match"] != owner.files.get(path, (None, None))[1]:
                            status = 412
                        else:
                            owner.revision += 1
                            etag = f'"{owner.revision}"'
                            owner.files[path], status = (body, etag), 201
                    elif self.command == "DELETE":
                        owner.files.pop(path, None)
                        status = 204
                    self.send_response(status)
                    self.send_header("Content-Length", str(len(result)))
                    if etag:
                        self.send_header("ETag", etag)
                    self.end_headers()
                    self.wfile.write(result)

            do_GET = do_PUT = do_DELETE = do_MKCOL = do_LOCK = do_UNLOCK = handle_request

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        self.addCleanup(self.stop_server)
        self.config = {"enabled": True, "baseUrl": f"http://127.0.0.1:{self.server.server_port}/", "remoteRoot": "root", "encryptionPassphraseHash": ""}

    def stop_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

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

    def test_missing_marker_is_adopted_without_rewriting_existing_data(self):
        self.files["payload"] = b"old", '"payload"'
        manager = self.manager()
        self.assertEqual(self.run_operation(manager)[0], b"old")
        self.assertEqual(json.loads(self.files[CLOUD_DATA_CONTRACT_PATH][0]), {"dataContractVersion": 1})
        self.assertEqual(self.files["payload"][0], b"old")
        self.assertIsNone(self.token)
        events = manager.operation_queue.snapshot()["events"]
        self.assertEqual([event["phase"] for event in events], ["start", "end"])
        self.assertEqual(events[-1]["operation"]["status"], "succeeded")

    def test_equal_version_does_not_run_migration(self):
        self.mark_version(1)
        self.files["payload"] = b"old", '"payload"'
        with mock.patch.dict(monitor_cloud.CLOUD_DATA_MIGRATIONS, {1: mock.Mock(side_effect=AssertionError("unexpected migration"))}):
            self.run_operation(self.manager())
        self.assertFalse(any(method == "PUT" for method, _, _ in self.calls))

    def test_newer_contract_blocks_reads_writes_and_overwrite_before_locking(self):
        self.mark_version(2)
        manager = self.manager()
        for action in ("read_payload", "write_payload", "overwrite_cloud_from_local"):
            with self.subTest(action=action), self.assertRaisesRegex(CloudError, "local script is updated"):
                self.run_operation(manager, action)
        self.assertTrue(all(method == "GET" and path == CLOUD_DATA_CONTRACT_PATH for method, path, _ in self.calls))
        self.assertEqual(len(manager.operation_queue.snapshot()["events"]), 6)

    def test_recheck_under_lock_blocks_upgrade_race(self):
        self.mark_version(1)
        original = WebDavClient.acquire_root_lock
        def racing_acquire(client):
            original(client)
            self.mark_version(2)
        with mock.patch.object(WebDavClient, "acquire_root_lock", racing_acquire), self.assertRaisesRegex(CloudError, "newer"):
            self.run_operation(self.manager())
        self.assertIsNone(self.token)
        self.assertFalse(any(path == "payload" for _, path, _ in self.calls))

    def test_failed_migration_preserves_marker_and_payload_and_retries(self):
        self.mark_version(0)
        self.files["payload"] = b"old", '"payload"'
        manager = self.manager()
        with mock.patch.dict(monitor_cloud.CLOUD_DATA_MIGRATIONS, {1: mock.Mock(side_effect=RuntimeError("interrupted"))}), self.assertRaisesRegex(RuntimeError, "interrupted"):
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
        with mock.patch.object(monitor_cloud, "CLOUD_DATA_CONTRACT_VERSION", 3), mock.patch.dict(monitor_cloud.CLOUD_DATA_MIGRATIONS, {2: lambda client: applied.append(2), 3: fail}):
            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                self.run_operation(manager)
            self.assertEqual(json.loads(self.files[CLOUD_DATA_CONTRACT_PATH][0])["dataContractVersion"], 2)
            with mock.patch.dict(monitor_cloud.CLOUD_DATA_MIGRATIONS, {3: lambda client: applied.append(3)}):
                self.run_operation(manager)
        self.assertEqual(applied, [2, 3, 3])

    def test_missing_step_and_invalid_marker_fail_closed(self):
        manager = self.manager()
        for value in (b"broken", b'{"dataContractVersion":true}', b'{"dataContractVersion":-1}', b"[]"):
            self.files[CLOUD_DATA_CONTRACT_PATH] = value, '"contract"'
            with self.subTest(value=value), self.assertRaisesRegex(CloudError, "Invalid cloud"):
                self.run_operation(manager)
        self.mark_version(0)
        with mock.patch.dict(monitor_cloud.CLOUD_DATA_MIGRATIONS, {}, clear=True), self.assertRaisesRegex(CloudError, "Missing cloud"):
            self.run_operation(manager)
        self.assertIsNone(self.token)

    def test_other_client_cannot_migrate_or_write_during_operation(self):
        self.mark_version(1)
        manager = self.manager()
        def held_operation():
            manager._connection()
            other = self.manager("two")
            with self.assertRaises(CloudError) as blocked:
                self.run_operation(other, "write_payload")
            self.assertEqual(blocked.exception.http_status, 423)
            with self.assertRaises(CloudError):
                WebDavClient(self.config).put("payload", b"conflict")
            manager._connection()[0].put("payload", b"safe")
        manager.held_operation = held_operation
        self.run_operation(manager, "held_operation")
        self.assertEqual(self.files["payload"][0], b"safe")
        self.assertEqual(sum(method == "UNLOCK" for method, _, _ in self.calls), 1)

    def test_lock_refresh_and_expiration_stop_access(self):
        client = WebDavClient(self.config)
        client.acquire_root_lock()
        self.addCleanup(client.release_root_lock)
        client._root_lock_refresh = 0
        client.put("payload", b"new")
        self.assertEqual(sum(method == "LOCK" for method, _, _ in self.calls), 2)
        client._root_lock_deadline = 0
        before = len(self.calls)
        with self.assertRaisesRegex(CloudError, "expired"):
            client.get("payload")
        self.assertEqual(len(self.calls), before)

    def test_unsupported_locks_block_payload_access(self):
        manager = self.manager()
        with mock.patch.object(WebDavClient, "acquire_root_lock", side_effect=CloudError("LOCK unsupported", 502)), self.assertRaisesRegex(CloudError, "unsupported"):
            self.run_operation(manager)
        self.assertFalse(any(path in {"payload", "crypto.json"} for _, path, _ in self.calls))

    def test_lock_response_requires_exclusive_recursive_lock(self):
        client = WebDavClient(self.config)
        for response in (b"bad", lock_response(timeout="Second-30"), lock_response(timeout="Infinite"), lock_response(depth="0"), lock_response().replace(b"exclusive", b"shared")):
            with self.subTest(response=response), self.assertRaises(CloudError):
                client._accept_root_lock(response, 0)


if __name__ == "__main__":
    unittest.main()
