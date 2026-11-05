"""Bounded record spooling and streaming dashboard JSON."""

from array import array
import json
import tempfile


CHUNK_BYTES = 64 * 1024
PAYLOAD_LIMIT = 600 * 1024 * 1024


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
        self.offsets = array("Q")

    def __iter__(self):
        return self.rows()

    def append(self, row):
        self.append_encoded(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode())

    def append_encoded(self, data):
        self.offsets.append(self.file.tell())
        self.file.write(data + b"\n")
        self.count += 1

    def rows(self):
        for line in self.encoded_rows():
            yield json.loads(line)

    def encoded_rows(self, offset=0, count=None):
        if offset < 0 or offset > self.count or count is not None and (count < 0 or offset + count > self.count):
            raise ValueError("Usage record range is outside its pack")
        self.file.seek(self.offsets[offset] if offset < self.count else 0, 0)
        for _ in range(self.count - offset if count is None else count):
            yield self.file.readline().rstrip(b"\n")

    def range_rows(self, offset, count):
        return (json.loads(line) for line in self.encoded_rows(offset, count))

    def __len__(self):
        return self.count

    def close(self):
        self.file.close()

    def __del__(self):
        self.close()


def close_records(value):
    if isinstance(value, Records):
        value.close()
    elif isinstance(value, dict):
        for item in value.values():
            close_records(item)


def dashboard_json_chunks(value):
    """Encode complete sanitized dashboard records, buffering writes; canonical wire encoding is unchanged."""
    def chunks(item):
        if isinstance(item, Records):
            yield b"["
            for position, row in enumerate(item):
                if position:
                    yield b","
                yield json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            yield b"]"
        elif isinstance(item, dict):
            yield b"{"
            for position, (key, row) in enumerate(item.items()):
                if position:
                    yield b","
                yield json.dumps(key, ensure_ascii=False).encode("utf-8") + b":"
                yield from chunks(row)
            yield b"}"
        else:
            yield json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    buffer = bytearray()
    for chunk in chunks(value):
        buffer.extend(chunk)
        if len(buffer) >= CHUNK_BYTES:
            yield bytes(buffer)
            buffer.clear()
    if buffer:
        yield bytes(buffer)
