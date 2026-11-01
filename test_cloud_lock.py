import io
import json
import threading
from unittest import mock

import monitor_cloud_lock
from monitor_cloud import CloudError
from monitor_cloud_lock import CLOUD_LOCK_PATH
from monitor_webdav_log import WebDavLog
from test_cloud_contract import CloudContractFixture


class Clock:
    def __init__(self):
        self.wall = 1800000000
        self.elapsed = 1000

    def time(self):
        return self.wall

    def monotonic(self):
        return self.elapsed

    def advance(self, seconds):
        self.wall += seconds
        self.elapsed += seconds


class CloudLockTests(CloudContractFixture):
    def setUp(self):
        super().setUp()
        self.clock = Clock()
        self.clock_patch = mock.patch.object(monitor_cloud_lock, "time", self.clock)
        self.clock_patch.start()
        self.addCleanup(self.clock_patch.stop)

    def record(self):
        return json.loads(self.files[CLOUD_LOCK_PATH][0])

    def abandon(self, client):
        client.access_lock.stop.set()
        client.access_lock.worker.join(2)
        self.assertFalse(client.access_lock.worker.is_alive())

    def test_native_lock_is_short_and_not_held_during_operation(self):
        client = self.client()
        client.acquire_file_lock()
        self.assertIsNone(self.token)
        self.assertEqual(self.record(), {"writing": 0, "owner": client.access_lock.owner, "expiresAt": self.clock.wall + 300})
        client.put("one", b"one")
        client.put("two", b"two")
        self.assertIsNone(self.token)
        self.assertTrue(all(headers["Timeout"] == "Second-60" for method, _, headers in self.calls if method == "LOCK"))
        self.assertEqual(sum(method == "LOCK" for method, _, _ in self.calls), 2)
        client.release_file_lock()
        self.assertEqual(self.record(), {"writing": 0})
        self.assertFalse(client.access_lock.worker.is_alive())
        self.assertEqual(sum(method == "LOCK" for method, _, _ in self.calls), 3)
        with self.assertRaises(CloudError):
            client.get("one")

    def test_expired_writer_is_replaced_and_old_owner_cannot_write_or_release(self):
        first, second = self.client(), self.client()
        first.acquire_file_lock()
        first.put("payload", b"before")
        self.abandon(first)
        self.clock.advance(300)
        second.acquire_file_lock()
        second.put("payload", b"after")
        replacement = self.files[CLOUD_LOCK_PATH]
        with self.assertRaises(CloudError):
            first.put("payload", b"stale")
        with self.assertRaises(CloudError):
            first.release_file_lock()
        self.assertEqual(self.files[CLOUD_LOCK_PATH], replacement)
        self.assertEqual(self.files["payload"][0], b"after")
        second.release_file_lock()

    def test_legacy_owner_is_eternal_and_malformed_status_is_preserved(self):
        for state in ({"writing": 0, "owner": "legacy"}, {"writing": 1, "owner": "legacy"}, {"writing": 1},
                {"writing": 0, "owner": "x", "expiresAt": True}, {"writing": 0, "owner": "x", "expiresAt": 1.5},
                {"writing": 0, "owner": "x", "expiresAt": "0"}, {"writing": 0, "owner": "x", "expiresAt": -1},
                {"writing": 0, "expiresAt": 0}):
            with self.subTest(state=state):
                self.files[CLOUD_LOCK_PATH] = json.dumps(state).encode(), '"old"'
                before = self.files[CLOUD_LOCK_PATH]
                with self.assertRaises(CloudError):
                    self.client().acquire_file_lock()
                self.assertEqual(self.files[CLOUD_LOCK_PATH], before)
                self.assertIsNone(self.token)

    def test_expiry_boundary_and_renewal_preserve_writing_state(self):
        self.files[CLOUD_LOCK_PATH] = json.dumps({"writing": 1, "owner": "dead", "expiresAt": self.clock.wall}).encode(), '"old"'
        client = self.client()
        client.acquire_file_lock()
        client.put("payload", b"one")
        self.clock.advance(240)
        client.access_lock.renew()
        self.assertEqual(self.record(), {"writing": 1, "owner": client.access_lock.owner, "expiresAt": self.clock.wall + 300})
        client.release_file_lock()

    def test_monotonic_expiry_survives_backward_wall_clock_adjustment(self):
        client = self.client()
        client.acquire_file_lock()
        self.abandon(client)
        self.clock.elapsed += 300
        self.clock.wall -= 3600
        with self.assertRaisesRegex(CloudError, "expired"):
            client.get("payload")
        with self.assertRaises(CloudError):
            client.release_file_lock()

    def test_renewal_failure_stops_access_and_preserves_expiring_record(self):
        client = self.client()
        client.acquire_file_lock()
        before = self.files[CLOUD_LOCK_PATH]
        with mock.patch.object(client.access_lock, "_write", side_effect=CloudError("offline", 502)):
            with self.assertRaises(CloudError):
                client.access_lock.renew()
        calls = len(self.calls)
        with self.assertRaisesRegex(CloudError, "unusable"):
            client.put("payload", b"blocked")
        self.assertEqual(len(self.calls), calls)
        with self.assertRaises(CloudError):
            client.release_file_lock()
        self.assertEqual(self.files[CLOUD_LOCK_PATH], before)
        self.assertIsNone(self.token)

    def test_late_renewal_response_does_not_resurrect_expired_operation(self):
        self.timeout = "Infinite"
        client = self.client()
        client.acquire_file_lock()
        self.abandon(client)
        self.clock.advance(240)
        original = client.access_lock._write
        def late_response(*args):
            result = original(*args)
            self.clock.advance(61)
            return result
        with mock.patch.object(client.access_lock, "_write", side_effect=late_response), self.assertRaisesRegex(CloudError, "expired"):
            client.access_lock.renew()
        with self.assertRaisesRegex(CloudError, "unusable"):
            client.put("payload", b"blocked")
        with self.assertRaises(CloudError):
            client.release_file_lock()

    def test_renewal_racing_replacement_cannot_overwrite_successor(self):
        self.reentrant = True
        first, second = self.client(), self.client()
        first.acquire_file_lock()
        self.abandon(first)
        original = first.access_lock._write
        def replace(state, etag, token, deadline):
            self.clock.advance(301)
            second.acquire_file_lock()
            return original(state, etag, token, deadline)
        with mock.patch.object(first.access_lock, "_write", side_effect=replace), self.assertRaises(CloudError):
            first.access_lock.renew()
        self.assertEqual(self.record()["owner"], second.access_lock.owner)
        with self.assertRaises(CloudError):
            first.release_file_lock()
        second.release_file_lock()

    def test_stale_etag_is_rejected_even_when_old_local_deadline_has_not_elapsed(self):
        first = self.client()
        first.acquire_file_lock()
        first.put("payload", b"before")
        self.files[CLOUD_LOCK_PATH] = json.dumps({"writing": 0, "owner": "replacement", "expiresAt": self.clock.wall + 300}).encode(), '"replacement"'
        with self.assertRaisesRegex(CloudError, "ownership changed"):
            first.put("payload", b"stale")
        self.assertIsNotNone(first.access_lock.failure)
        self.assertEqual(self.files["payload"][0], b"before")
        with self.assertRaises(CloudError):
            first.release_file_lock()

    def test_only_renewal_conflicts_retry_and_payload_etags_are_preserved(self):
        client = self.client()
        client.acquire_file_lock()
        client.put("payload", b"before")
        original = client._send_request
        triggered = False
        def renew_before_send(method, path, *args, **kwargs):
            nonlocal triggered
            if path == "payload" and method == "PUT" and not triggered:
                triggered = True
                self.clock.advance(240)
                client.access_lock.renew()
            return original(method, path, *args, **kwargs)
        before = len(self.calls)
        with mock.patch.object(client, "_send_request", side_effect=renew_before_send):
            client.put("payload", b"after", etag=self.files["payload"][1])
        writes = [headers for method, path, headers in self.calls[before:] if method == "PUT" and path == "payload"]
        self.assertEqual(len(writes), 2)
        self.assertEqual(writes[0]["If-Match"], writes[1]["If-Match"])
        with self.assertRaises(CloudError):
            client.put("payload", b"wrong", etag='"wrong-payload-etag"')
        self.assertEqual(self.files["payload"][0], b"after")
        client.release_file_lock()

    def test_network_failures_do_not_retry_mutations(self):
        client = self.client()
        client.acquire_file_lock()
        client.access_lock.prepare("PUT")
        with mock.patch.object(client, "_send_request", side_effect=CloudError("response lost", 502, category="network")) as send:
            with self.assertRaises(CloudError):
                client.put("payload", b"ambiguous")
            self.assertEqual(send.call_count, 1)
        client.release_file_lock()

    def test_metadata_accounting_and_renewal_errors_do_not_leak_secrets(self):
        client = self.client()
        client.transfer_category = "payload"
        client.diagnostic_log = WebDavLog(self.root / "metadata.log")
        client._diagnostic_secrets += ("private-renewal-secret",)
        client.acquire_file_lock()
        self.assertTrue(all(transfer["category"] == "metadata" for transfer in client.transfers))
        with mock.patch.object(client.access_lock, "_write", side_effect=CloudError("offline private-renewal-secret", 502)), self.assertRaises(CloudError):
            client.access_lock.renew()
        self.assertNotIn("private-renewal-secret", client.diagnostic_log.path.read_text())
        with self.assertRaises(CloudError):
            client.release_file_lock()

    def test_stranded_infinite_native_lock_still_requires_manual_recovery(self):
        self.files[CLOUD_LOCK_PATH] = json.dumps({"writing": 0, "owner": "dead", "expiresAt": self.clock.wall - 1}).encode(), '"old"'
        self.token = "opaquelocktoken:stranded"
        self.tokens.add(self.token)
        before = self.files[CLOUD_LOCK_PATH]
        with self.assertRaises(CloudError) as blocked:
            self.client().acquire_file_lock()
        self.assertEqual(blocked.exception.http_status, 423)
        self.assertEqual(blocked.exception.category, "lock")
        self.assertEqual(self.files[CLOUD_LOCK_PATH], before)
        self.assertEqual(self.token, "opaquelocktoken:stranded")

    def test_capability_probe_rejects_ignored_conditions_and_is_cached(self):
        for flag in ("ignore_tagged_etags", "ignore_if_match"):
            with self.subTest(flag=flag):
                setattr(self, flag, True)
                with self.assertRaisesRegex(CloudError, "must enforce"):
                    self.client().acquire_file_lock()
                self.assertEqual(self.record(), {"writing": 0})
                self.assertFalse(any(path.startswith(".lock-probe-") for path in self.files))
                setattr(self, flag, False)
        first = self.client()
        first.acquire_file_lock()
        first.release_file_lock()
        calls = len(self.calls)
        second = self.client()
        second.acquire_file_lock()
        second.release_file_lock()
        self.assertFalse(any(path.startswith(".lock-probe-") for _, path, _ in self.calls[calls:]))

    def test_background_renewal_during_blocked_request_keeps_log_correlation(self):
        entered, renewed = threading.Event(), threading.Event()
        clock = self.clock
        class WakeOnRequest(threading.Event):
            def wait(event, timeout=None):
                if not renewed.is_set():
                    if not entered.wait(2):
                        return True
                    clock.advance(240)
                    return False
                return super().wait(timeout)
        client = self.client()
        client.diagnostic_log = WebDavLog(self.root / "lease.log")
        client.diagnostic_log.context.operation_id = "parent-operation"
        client.access_lock.operation_id = "parent-operation"
        client.access_lock.stop = WakeOnRequest()
        self.files["payload"] = b"download", '"payload"'
        original_log, original_send = client.access_lock._log, client._send_request
        def record(event, **fields):
            original_log(event, **fields)
            if event == "lease_renewed":
                renewed.set()
        def block(method, path, *args, **kwargs):
            if method == "GET" and path == "payload":
                entered.set()
                self.assertTrue(renewed.wait(3))
            return original_send(method, path, *args, **kwargs)
        with mock.patch.object(client.access_lock, "_log", side_effect=record), mock.patch.object(client, "_send_request", side_effect=block):
            client.acquire_file_lock()
            output = io.BytesIO()
            client.get_file("payload", output)
            self.assertEqual(output.read(), b"download")
            self.assertEqual(self.record()["expiresAt"], clock.wall + 300)
            client.release_file_lock()
        records = [json.loads(line) for line in client.diagnostic_log.path.read_text().splitlines()]
        self.assertTrue(any(item["event"] == "lease_renewed" for item in records))
        self.assertTrue(all(item["operationId"] == "parent-operation" for item in records))
        self.assertFalse(client.access_lock.worker.is_alive())
