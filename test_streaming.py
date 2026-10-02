import io
import json
import random
import tempfile
import unittest
import zlib
from unittest import mock

from cryptography.exceptions import InvalidTag

from monitor_cloud import CryptoBox, WebDavClient, CloudManager
from monitor_streaming import Records, compressed_payload, encrypt_file, decrypt_file, decode_payload, json_chunks, logical_hash_sorted
from monitor_usage_sync import canonical_json, v4_logical_hash


class StreamingTests(unittest.TestCase):
    def box(self, plaintext=False):
        box = object.__new__(CryptoBox)
        box.plaintext, box.key = plaintext, b"a" * 32
        return box

    def test_canonical_compression_is_identical_across_chunk_boundaries(self):
        rows = [{"recordKey": str(index), "record": {"kind": "opaque", "unicode": "中文", "values": [None, True, index, index / 3]}} for index in range(4000)]
        value = {"version": 1, "records": Records(lambda: iter(rows)), "machineId": "machine"}
        with tempfile.TemporaryFile() as output:
            digest, size = compressed_payload(value, output)
            expected_digest, expected = CloudManager._usage_payload_bytes(value | {"records": rows})
            self.assertEqual((digest, size, output.read()), (expected_digest, len(expected), expected))
        self.assertEqual(logical_hash_sorted(sorted(rows, key=lambda row: row["recordKey"])), v4_logical_hash(rows))

    def test_streamed_encryption_matches_existing_wire_format(self):
        data = random.Random(7).randbytes(200000)
        for plain in (False, True):
            box = self.box(plain)
            with mock.patch("monitor_streaming.secrets.token_bytes", return_value=b"n" * 12), io.BytesIO() as output:
                digest = __import__("hashlib").sha256(data).hexdigest()
                length = encrypt_file(box, "purpose", io.BytesIO(data), output, len(data), digest)
                expected = box.encrypt("purpose", data)
                self.assertEqual((output.read(), length), (expected, len(expected)))
                recovered = io.BytesIO()
                self.assertEqual(decrypt_file(box, "purpose", io.BytesIO(expected), recovered), digest)
                self.assertEqual(recovered.read(), data)

    def test_authenticated_record_spool_and_tampering(self):
        rows = [{"value": index, "text": "🙂" * 100} for index in range(2500)]
        value = {"version": 1, "machineId": "machine", "day": "2030-01-01", "records": rows}
        compressed = zlib.compress(canonical_json(value), 9)
        digest = __import__("hashlib").sha256(compressed).hexdigest()
        for plain in (False, True):
            box = self.box(plain)
            payload = box.encrypt("purpose", compressed)
            decoded = decode_payload(box, "purpose", io.BytesIO(payload), digest)
            try:
                self.assertEqual(list(decoded["records"]), rows)
                self.assertEqual(len(decoded["records"]), len(rows))
                self.assertEqual(list(decoded["records"]), rows)
            finally:
                decoded["records"].close()
            if not plain:
                tampered = json.loads(payload)
                tampered["ciphertext"] = ("A" if tampered["ciphertext"][0] != "A" else "B") + tampered["ciphertext"][1:]
                with self.assertRaises((ValueError, InvalidTag)):
                    decode_payload(box, "purpose", io.BytesIO(json.dumps(tampered).encode()), digest)
                with self.assertRaises(ValueError):
                    decode_payload(box, "wrong-purpose", io.BytesIO(payload), digest)

    def test_corrupt_compression_and_trailing_data_are_rejected(self):
        for compressed in (zlib.compress(b'{"version":1,"records":[]}')[:-1], zlib.compress(b'{"version":1,"records":[]}') + b"garbage"):
            with self.assertRaises(ValueError):
                decode_payload(self.box(True), "purpose", io.BytesIO(compressed), __import__("hashlib").sha256(compressed).hexdigest())

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
