import io
import json
from pathlib import Path
import shutil
import threading
import unittest
import urllib.error
import uuid
from unittest import mock

from monitor_cloud import CloudError, WebDavClient
from monitor_cloud_queue import CloudOperationQueue, OperationCancelled, OperationSkipped
from monitor_webdav_log import WebDavLog, WEBDAV_LOG_MAX_BYTES


class WebDavLogTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(__file__).parent / (".test-webdav-log-" + uuid.uuid4().hex)
        self.root.mkdir()
        self.addCleanup(shutil.rmtree, self.root)
        self.log = WebDavLog(self.root / "webdav.log")

    def records(self):
        return [json.loads(line) for line in self.log.path.read_text().splitlines()]

    def client(self):
        return WebDavClient({"baseUrl": "https://dav.example/", "username": "private-user", "password": "private-password"}, self.log)

    def test_limit_discards_oldest_complete_entries_and_keeps_newest(self):
        self.assertEqual(WEBDAV_LOG_MAX_BYTES, 5_000_000)
        self.log.max_bytes = 1000
        for index in range(30):
            self.log.write("entry", index=index, content="中文" * 30)
            self.assertLessEqual(self.log.path.stat().st_size, 1000)
            self.assertEqual(self.records()[-1]["index"], index)
        self.assertGreater(self.records()[0]["index"], 0)
        self.assertEqual([item["index"] for item in self.records()], sorted(item["index"] for item in self.records()))

    def test_preexisting_oversized_log_and_concurrent_writes(self):
        self.log.max_bytes = 1000
        self.log.path.write_bytes(b"old\n" * 2000)
        workers = [threading.Thread(target=lambda: [self.log.write("entry", index=i) for i in range(20)]) for _ in range(4)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        self.assertLessEqual(self.log.path.stat().st_size, 1000)
        self.assertTrue(all(item["event"] == "entry" for item in self.records()))

    def test_rate_limit_records_headers_without_secrets_or_body(self):
        client = self.client()
        error = urllib.error.HTTPError(client.base, 429, "Too Many Requests", {"Retry-After": "120", "X-RateLimit-Remaining": "0", "X-Request-ID": "server-id",
            "Set-Cookie": "private-cookie", "Lock-Token": "private-lock"}, io.BytesIO(b"private-response-body"))
        client.opener.open = mock.Mock(side_effect=error)
        with self.assertRaises(CloudError) as raised:
            client.request("PUT", "test", b"private-payload")
        self.assertEqual(raised.exception.http_status, 429)
        record = self.records()[-1]
        self.assertEqual(record["status"], 429)
        self.assertTrue(record["rateLimited"])
        self.assertEqual(record["responseHeaders"]["retry-after"], "120")
        self.assertEqual(record["responseHeaders"]["x-request-id"], "server-id")
        self.assertEqual(record["transfer"]["attemptedUploadBytes"], len(b"private-payload"))
        self.assertEqual(record["outcome"], "failed")
        self.assertGreaterEqual(record["elapsedMs"], 0)
        for secret in ("private-password", "private-user", "private-cookie", "private-lock", "private-payload", "private-response-body", client.authorization):
            self.assertNotIn(secret, self.log.path.read_text())
        self.assertTrue(error.fp.closed)

    def test_success_expected_http_error_and_streamed_bytes(self):
        client = self.client()
        response = mock.MagicMock(status=200, headers={"ETag": '"1"'})
        response.__enter__.return_value = response
        response.read.side_effect = [b"one", b"two", b""]
        client.opener.open = mock.Mock(return_value=response)
        client.request("GET", "file", output=io.BytesIO())
        self.assertEqual(self.records()[-1]["transfer"]["downloadBytes"], 6)
        client.opener.open.side_effect = urllib.error.HTTPError(client.base, 404, "missing", {}, io.BytesIO(b""))
        self.assertEqual(client.request("DELETE", "missing", expected=(204, 404))[2], 404)
        self.assertEqual(self.records()[-1]["outcome"], "succeeded")

    def test_network_failure_redacts_credentials_and_url_queries(self):
        client = self.client()
        client.opener.open = mock.Mock(side_effect=urllib.error.URLError("TLS failed private-password https://private-user:private-password@dav.example/path?token=private-query"))
        with self.assertRaises(CloudError):
            client.request("GET", "file")
        self.assertEqual(self.records()[-1]["errorCategory"], "network")
        for secret in ("private-user", "private-password", "private-query"):
            self.assertNotIn(secret, self.log.path.read_text())

    def test_retry_logged_with_delay_and_attempt(self):
        client = self.client()
        client.opener.open = mock.Mock(side_effect=[urllib.error.HTTPError(client.base, 503, "busy", {"Retry-After": "1"}, io.BytesIO()),
            urllib.error.HTTPError(client.base, 405, "exists", {}, io.BytesIO())])
        with mock.patch("monitor_cloud.time.sleep"):
            client.ensure_directories("test")
        retry = next(item for item in self.records() if item["event"] == "request_retry")
        self.assertEqual((retry["attempt"], retry["status"], retry["delaySeconds"]), (1, 503, 1))

    def test_operation_lifecycle_and_request_correlation(self):
        queue = CloudOperationQueue(self.log)
        self.addCleanup(queue.close)
        client = self.client()
        client.opener.open = mock.Mock(side_effect=urllib.error.URLError("offline"))
        operation = queue.submit("fetch", lambda: client.request("GET", "file"), source="automatic")
        with self.assertRaises(CloudError):
            queue.wait(operation)
        records = self.records()
        self.assertEqual([item["event"] for item in records], ["operation_start", "operation_running", "request_start", "request_end", "operation_end"])
        self.assertTrue(all(item["operationId"] == operation["public"]["id"] for item in records))
        self.assertEqual(records[-1]["error"]["httpStatus"], None)
        self.assertEqual(records[-1]["error"]["category"], "network")
        self.assertEqual(records[-1]["status"], "failed")

    def test_skipped_cancelled_and_partial_operations_have_end_records(self):
        queue = CloudOperationQueue(self.log)
        self.addCleanup(queue.close)
        started, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        first = queue.submit("block", lambda: (started.set(), release.wait(5)))
        self.assertTrue(started.wait(2))
        cancelled = queue.submit("cancel", lambda: None)
        queue.cancel(cancelled["public"]["id"])
        skipped = queue.submit("skip", lambda: None, validate=mock.Mock(side_effect=OperationSkipped("disabled")))
        partial = queue.submit("partial", lambda: {"errors": {"skill": "offline"}})
        release.set()
        queue.wait(first)
        with self.assertRaises(OperationCancelled):
            queue.wait(cancelled)
        with self.assertRaises(OperationSkipped):
            queue.wait(skipped)
        queue.wait(partial)
        ends = {item["action"]: item for item in self.records() if item["event"] == "operation_end"}
        self.assertEqual(ends["cancel"]["status"], "cancelled")
        self.assertEqual(ends["skip"]["error"]["message"], "disabled")
        self.assertTrue(ends["partial"]["partial"])
        self.assertIn("offline", ends["partial"]["failures"])

    def test_logging_io_failure_does_not_break_request(self):
        self.log.path = self.root  # A directory cannot be opened as a log file.
        client = self.client()
        client.opener.open = mock.Mock(side_effect=urllib.error.HTTPError(client.base, 405, "exists", {}, io.BytesIO()))
        self.assertEqual(client.request("MKCOL", "test", expected=(405,))[2], 405)


if __name__ == "__main__":
    unittest.main()
