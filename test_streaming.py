import io
import json
import unittest
from unittest import mock


from monitor_cloud import WebDavClient
from monitor_streaming import Records


class StreamingTests(unittest.TestCase):
    def test_dashboard_encoding_buffers_complete_sanitized_records(self):
        from monitor_dashboard import dashboard_safe_json
        from monitor_streaming import dashboard_json_chunks
        rows = [{"_key": str(i), "label": "中文", "nested": {"values": [i, None, True], "access_token": "private"}} for i in range(10000)]
        chunks = list(dashboard_json_chunks(dashboard_safe_json({"rows": Records(lambda: iter(rows))})))
        self.assertEqual(json.loads(b"".join(chunks)), dashboard_safe_json({"rows": rows}))
        self.assertNotIn(b"private", b"".join(chunks))
        self.assertLess(len(chunks), 30)

    def test_webdav_stream_uses_content_length_and_bounded_reads(self):
        client = WebDavClient({"baseUrl": "http://localhost", "remoteRoot": "test"})
        response = mock.MagicMock()
        response.status, response.headers = 200, {"ETag": '"1"'}
        response.read.side_effect = [b"one", b"two", b""]
        client.opener.open = mock.MagicMock(return_value=response)
        response.__enter__.return_value = response
        source, output = io.BytesIO(b"upload"), io.BytesIO()
        client.request("PUT", "file", source, output=output)
        request = client.opener.open.call_args.args[0]
        self.assertEqual(request.get_header("Content-length"), "6")
        self.assertEqual(output.read(), b"onetwo")
        self.assertTrue(all(call.args == (65536,) for call in response.read.call_args_list))
        self.assertEqual(client.transfers[-1]["downloadBytes"], 6)


if __name__ == "__main__":
    unittest.main()
