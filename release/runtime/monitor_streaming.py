"""Bounded spooling for canonical usage payloads and dashboard snapshots."""

import base64
import hashlib
import heapq
import json
import secrets
import tempfile
import zlib

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

CHUNK_BYTES = 64 * 1024
PAYLOAD_LIMIT = 600 * 1024 * 1024
RECORD_LIMIT = 8 * 1024 * 1024


class Records:
    """Repeatable, disk-backed rows recognized by the streaming JSON encoder."""
    def __init__(self, iterator, on_close=None):
        self.iterator, self.on_close = iterator, on_close

    def __iter__(self):
        return self.iterator()

    def close(self):
        if self.on_close is not None:
            callback, self.on_close = self.on_close, None
            callback()

    def __del__(self):
        self.close()


class RecordSpool(Records):
    def __init__(self):
        self.file = tempfile.TemporaryFile()
        self.count = 0

    def __iter__(self):
        return self.rows()

    def append(self, row):
        self.file.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode() + b"\n")
        self.count += 1

    def rows(self):
        self.file.seek(0)
        for line in self.file:
            yield json.loads(line)

    def __len__(self):
        return self.count

    def close(self):
        self.file.close()

    def __del__(self):
        self.close()


def json_chunks(value, *, sort_keys=True):
    if isinstance(value, dict):
        yield b"{"
        for position, key in enumerate(sorted(value) if sort_keys else value):
            if position:
                yield b","
            yield json.dumps(key, ensure_ascii=False).encode() + b":"
            yield from json_chunks(value[key], sort_keys=sort_keys)
        yield b"}"
    elif isinstance(value, (list, tuple, Records)):
        yield b"["
        for position, row in enumerate(value):
            if position:
                yield b","
            yield from json_chunks(row, sort_keys=sort_keys)
        yield b"]"
    else:
        yield json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


def close_records(value):
    if isinstance(value, Records):
        value.close()
    elif isinstance(value, dict):
        for item in value.values():
            close_records(item)


def compressed_payload(value, target):
    compressor, digest, size = zlib.compressobj(9), hashlib.sha256(), 0
    for chunk in json_chunks(value):
        compressed = compressor.compress(chunk)
        target.write(compressed)
        digest.update(compressed)
        size += len(compressed)
    compressed = compressor.flush()
    target.write(compressed)
    digest.update(compressed)
    size += len(compressed)
    target.seek(0)
    return digest.hexdigest(), size


def encrypt_file(box, purpose, source, target, size, digest):
    if box.plaintext:
        for chunk in iter(lambda: source.read(CHUNK_BYTES), b""):
            target.write(chunk)
    else:
        nonce = secrets.token_bytes(12)
        header = {"format": "codex-switch-encrypted", "version": 1, "purpose": purpose, "size": size, "sha256": digest}
        encryptor = Cipher(algorithms.AES(box.key), modes.GCM(nonce)).encryptor()
        encryptor.authenticate_additional_data(json.dumps(header, sort_keys=True, separators=(",", ":")).encode())
        target.write(b'{"header":' + json.dumps(header, separators=(",", ":")).encode() + b',"nonce":' + json.dumps(base64.b64encode(nonce).decode()).encode() + b',"ciphertext":"')
        carry = b""
        for chunk in iter(lambda: source.read(CHUNK_BYTES), b""):
            carry += encryptor.update(chunk)
            boundary = len(carry) // 3 * 3
            target.write(base64.b64encode(carry[:boundary]))
            carry = carry[boundary:]
        carry += encryptor.finalize() + encryptor.tag
        target.write(base64.b64encode(carry) + b'"}')
    length = target.tell()
    target.seek(0)
    return length


class JsonReader:
    def __init__(self, stream):
        self.stream, self.buffer = stream, b""

    def fill(self):
        chunk = self.stream.read(CHUNK_BYTES)
        self.buffer += chunk
        return bool(chunk)

    def whitespace(self):
        while True:
            self.buffer = self.buffer.lstrip()
            if self.buffer or not self.fill():
                return

    def expect(self, byte):
        self.whitespace()
        if not self.buffer.startswith(byte):
            raise ValueError("Invalid streamed JSON")
        self.buffer = self.buffer[len(byte):]

    def value(self):
        self.whitespace()
        while True:
            try:
                text = self.buffer.decode("utf-8")
                value, end = json.JSONDecoder().raw_decode(text)
                consumed = len(text[:end].encode())
                # A number at a buffer boundary may not be complete yet.
                if consumed == len(self.buffer) and isinstance(value, (int, float)) and self.fill():
                    continue
                self.buffer = self.buffer[consumed:]
                return value
            except (UnicodeDecodeError, json.JSONDecodeError):
                if len(self.buffer) > RECORD_LIMIT or not self.fill():
                    raise ValueError("Incomplete or oversized streamed JSON record")

    def string_chunks(self):
        self.expect(b'"')
        while True:
            end = self.buffer.find(b'"')
            chunk = self.buffer if end < 0 else self.buffer[:end]
            if b"\\" in chunk or any(byte < 32 for byte in chunk):
                raise ValueError("Unsupported encrypted string encoding")
            yield chunk
            if end >= 0:
                self.buffer = self.buffer[end + 1:]
                return
            self.buffer = b""
            if not self.fill():
                raise ValueError("Incomplete encrypted string")

    def finish(self):
        self.whitespace()
        if self.buffer:
            raise ValueError("Trailing streamed JSON content")


def decrypt_file(box, purpose, source, target, limit=PAYLOAD_LIMIT):
    digest, size = hashlib.sha256(), 0
    if box.plaintext:
        for chunk in iter(lambda: source.read(CHUNK_BYTES), b""):
            size += len(chunk)
            if size > limit:
                raise ValueError("Payload exceeds safety bound")
            target.write(chunk)
            digest.update(chunk)
    else:
        reader, header, nonce, authenticated = JsonReader(source), None, None, False
        reader.expect(b"{")
        while True:
            key = reader.value()
            reader.expect(b":")
            if key == "header":
                header = reader.value()
            elif key == "nonce":
                nonce = base64.b64decode(reader.value(), validate=True)
            elif key == "ciphertext":
                if not isinstance(header, dict) or header.get("format") != "codex-switch-encrypted" or header.get("version") != 1 or header.get("purpose") != purpose or not isinstance(header.get("size"), int) or not 0 <= header["size"] <= limit or nonce is None or len(nonce) != 12:
                    raise ValueError("Invalid encrypted payload header")
                decryptor = Cipher(algorithms.AES(box.key), modes.GCM(nonce)).decryptor()
                decryptor.authenticate_additional_data(json.dumps(header, sort_keys=True, separators=(",", ":")).encode())
                carry, tail = b"", b""
                for chunk in reader.string_chunks():
                    carry += chunk
                    boundary = len(carry) // 4 * 4
                    decoded = base64.b64decode(carry[:boundary], validate=True)
                    carry = carry[boundary:]
                    tail += decoded
                    if len(tail) > 16:
                        plain = decryptor.update(tail[:-16])
                        tail = tail[-16:]
                        size += len(plain)
                        if size > header["size"]:
                            raise ValueError("Payload exceeds declared size")
                        target.write(plain)
                        digest.update(plain)
                if carry or len(tail) != 16:
                    raise ValueError("Incomplete encrypted payload")
                decryptor.finalize_with_tag(tail)
                authenticated = True
            else:
                raise ValueError("Unknown encrypted payload field")
            reader.whitespace()
            if reader.buffer.startswith(b"}"):
                reader.expect(b"}")
                break
            reader.expect(b",")
        reader.finish()
        if not authenticated or size != header["size"] or digest.hexdigest() != header.get("sha256"):
            raise ValueError("Encrypted payload completion check failed")
    target.seek(0)
    return digest.hexdigest()


def decode_payload(box, purpose, source, expected_id):
    with tempfile.TemporaryFile() as compressed, tempfile.TemporaryFile() as plain:
        if decrypt_file(box, purpose, source, compressed) != expected_id:
            raise ValueError("Usage payload identity check failed")
        decoder, size = zlib.decompressobj(), 0
        for chunk in iter(lambda: compressed.read(CHUNK_BYTES), b""):
            while chunk:
                decoded = decoder.decompress(chunk, CHUNK_BYTES)
                size += len(decoded)
                if size > PAYLOAD_LIMIT:
                    raise ValueError("Usage payload exceeds decoder safety bound")
                plain.write(decoded)
                chunk = decoder.unconsumed_tail
        if not decoder.eof or decoder.unused_data:
            raise ValueError("Incomplete compressed payload")
        plain.seek(0)
        reader, value = JsonReader(plain), {}
        reader.expect(b"{")
        while True:
            key = reader.value()
            if not isinstance(key, str) or key in value:
                raise ValueError("Invalid usage payload field")
            reader.expect(b":")
            if key == "records":
                rows = value[key] = RecordSpool()
                reader.expect(b"[")
                reader.whitespace()
                while not reader.buffer.startswith(b"]"):
                    rows.append(reader.value())
                    reader.whitespace()
                    if reader.buffer.startswith(b"]"):
                        break
                    reader.expect(b",")
                reader.expect(b"]")
            else:
                value[key] = reader.value()
            reader.whitespace()
            if reader.buffer.startswith(b"}"):
                reader.expect(b"}")
                break
            reader.expect(b",")
        reader.finish()
        if value.get("version") != 1:
            raise ValueError("Unsupported usage payload")
        return value


def sorted_records(parts):
    return Records(lambda: heapq.merge(*(iter(rows) for rows in parts.values()), key=lambda row: row["recordKey"]))


def logical_hash_sorted(entries):
    digest = hashlib.sha256()
    digest.update(b"[")
    for position, entry in enumerate(entries):
        if position:
            digest.update(b",")
        digest.update(json.dumps([entry["recordKey"], hashlib.sha256(b"".join(json_chunks(entry["record"]))).hexdigest()], ensure_ascii=False, separators=(",", ":")).encode())
    digest.update(b"]")
    return digest.hexdigest()
