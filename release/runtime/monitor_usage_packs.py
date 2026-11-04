"""Physical usage packs and object-reference based downloads."""

from contextlib import AbstractContextManager

from monitor_streaming import RecordSpool, Records
from monitor_usage_sync import canonical_json


PACK_RAW_BYTES = 75_000_000


class HistoricalPackWriter(AbstractContextManager):
    def __init__(self, manager, client, box, machine):
        self.manager, self.client, self.box, self.machine = manager, client, box, machine
        self.catalog, self.pending, self.previous = {}, {}, None
        self.rows = RecordSpool()
        self.size = len(canonical_json({"version": 2, "machineId": machine, "records": []}))

    def add(self, day, entry, encoded=None):
        order = entry["collectionPeriod"], entry["recordKey"]
        if entry["collectionPeriod"][:10] != day or self.previous is not None and order <= self.previous:
            raise ValueError("Historical pack records must have unique, chronological ownership")
        self.previous = order
        if encoded is None:
            encoded = canonical_json(entry)
        self.size += len(encoded) + bool(len(self.rows))
        segment = self.pending.setdefault(day, {"recordOffset": len(self.rows), "recordCount": 0})
        segment["recordCount"] += 1
        self.rows.append_encoded(encoded)
        if self.size > PACK_RAW_BYTES:
            self.flush()

    def flush(self):
        if not len(self.rows):
            return
        digest = self.manager._put_usage_payload(self.client, self.box, "v4/data", self.machine,
            {"version": 2, "machineId": self.machine, "records": self.rows})[0]
        for day, segment in self.pending.items():
            self.catalog.setdefault(day, {"layout": "packed", "segments": []})["segments"].append({"payloadId": digest, **segment})
        self.rows.close()
        self.rows, self.pending = RecordSpool(), {}
        self.size = len(canonical_json({"version": 2, "machineId": self.machine, "records": []}))

    def finish(self):
        self.flush()
        return self.catalog

    def __exit__(self, *args):
        self.rows.close()


class UsageObjectReader(AbstractContextManager):
    """One decode per physical object; ranges seek directly into disk-backed rows."""
    def __init__(self, manager, client, box, machine):
        self.manager, self.client, self.box, self.machine = manager, client, box, machine
        self.cache, self.downloaded = {}, 0

    def payload(self, digest):
        if digest not in self.cache:
            from monitor_cloud import CloudError
            from monitor_usage_sync import require_usage_fields
            value = self.manager._download_usage_payload(self.client, self.box, "v4/data", self.machine, digest)
            try:
                require_usage_fields(value, version=int, machineId=str, records=(list, RecordSpool))
            except ValueError as exc:
                if isinstance(value.get("records"), RecordSpool):
                    value["records"].close()
                raise CloudError("Invalid usage data fields", 409) from exc
            self.cache[digest] = value
            self.downloaded += 1
        return self.cache[digest]

    def entries(self, day, reference):
        if reference["layout"] == "packed":
            def rows():
                for segment in reference["segments"]:
                    value = self.payload(segment["payloadId"])
                    offset, count = segment["recordOffset"], segment["recordCount"]
                    start, stop, _ = slice(offset, offset + count).indices(len(value["records"]))
                    yield from value["records"].range_rows(start, max(0, stop - start)) if isinstance(value["records"], RecordSpool) else value["records"][start:stop]
            return Records(rows)
        manifest = self.manager._v4_day_manifest(self.client, self.box, self.machine, day, reference)
        return Records(lambda: (entry for part, digest in manifest["parts"].items() for entry in self.part(day, part, digest)))

    def part(self, day, part, digest):
        from monitor_cloud import CloudError
        from monitor_usage_sync import require_usage_fields
        value = self.payload(digest)
        try:
            require_usage_fields(value, day=str, partId=str)
        except ValueError as exc:
            raise CloudError("Invalid usage v4 data part fields", 409) from exc
        return value["records"]

    def __exit__(self, *args):
        for value in self.cache.values():
            if isinstance(value.get("records"), RecordSpool):
                value["records"].close()
