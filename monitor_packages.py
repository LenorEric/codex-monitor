"""Immutable usage packages; only the pending local buffer owns unpublished data."""

import hashlib
import json
import time
import zlib

from monitor_common import parse_timestamp
from monitor_usage_sync import canonical_json, record_key, validate_sync_operation

PACK_RAW_BYTES = 75_000_000


def envelope_bytes(machine, first, last):
    return len(canonical_json({"version": 1, "machineId": machine, "firstBufferedAt": first, "lastBufferedAt": last, "records": []}))


def destination(manager):
    config = manager.config()["webdav"]
    return hashlib.sha256(canonical_json([config.get("baseUrl"), config.get("username"), config.get("remoteRoot")])).hexdigest()


def package_path(machine, package):
    return f"usage/v5/packages/{machine}/{package}.enc"


def prepare(machine, records, first, last):
    times = [parse_timestamp(record["row"].get("checkedAt") or record["row"].get("occurredAt")) for record in records]
    value = {"version": 1, "machineId": machine, "firstBufferedAt": first, "lastBufferedAt": last, "records": records}
    compressed = zlib.compress(canonical_json(value), 9)
    package = hashlib.sha256(compressed).hexdigest()
    return package, compressed, {"version": 1, "machineId": machine, "payloadId": package, "recordCount": len(records),
        "firstBufferedAt": first, "lastBufferedAt": last, "minEventAt": min(times), "maxEventAt": max(times)}


def publish(manager, client, box, machine, package, compressed, descriptor):
    from monitor_cloud import CloudError
    client._package_descriptor_started = False
    for path in (f"usage/v5/data/{machine}", f"usage/v5/packages/{machine}"):
        client.ensure_directories(path)
    for path, purpose, data in (
        (f"usage/v5/data/{machine}/{package}.enc", f"usage-v5-data:{machine}:{package}", compressed),
        (package_path(machine, package), f"usage-v5-package:{machine}:{package}", canonical_json(descriptor)),
    ):
        try:
            if path == package_path(machine, package):
                client._package_descriptor_started = True
            client.put(path, box.encrypt(purpose, data), create=True)
        except CloudError as exc:
            if exc.http_status != 412:
                raise
            if box.decrypt(purpose, client.get(path)[0], 200 * 1024 * 1024) != data:
                raise CloudError("Immutable package content does not match its identity", 409) from exc


def _descriptor(client, box, machine, package):
    from monitor_cloud import CloudError
    try:
        value = json.loads(box.decrypt(f"usage-v5-package:{machine}:{package}", client.get(package_path(machine, package))[0]))
    except CloudError as exc:
        if exc.http_status == 404:
            return None
        raise
    if value.get("version") != 1 or value.get("machineId") != machine or value.get("payloadId") != package or type(value.get("recordCount")) is not int:
        raise CloudError("Invalid usage package descriptor", 409)
    return value


def records(client, box, machine, package, descriptor):
    from monitor_cloud import CloudError
    compressed = box.decrypt(f"usage-v5-data:{machine}:{package}", client.get(f"usage/v5/data/{machine}/{package}.enc")[0], 200 * 1024 * 1024)
    if hashlib.sha256(compressed).hexdigest() != package:
        raise CloudError("Usage package checksum mismatch", 409)
    decoder = zlib.decompressobj()
    raw = decoder.decompress(compressed, PACK_RAW_BYTES + 8 * 1024 * 1024)
    if decoder.unconsumed_tail or not decoder.eof or decoder.unused_data:
        raise CloudError("Usage package exceeds the decode limit", 409)
    value = json.loads(raw)
    if value.get("version") != 1 or value.get("machineId") != machine or not isinstance(value.get("records"), list) or len(value["records"]) != descriptor["recordCount"]:
        raise CloudError("Invalid usage package payload", 409)
    for record in value["records"]:
        validate_sync_operation({"action": "upsert", "key": record_key(record["kind"], record["row"]), "record": record})
    return value["records"]


def _acknowledge(store, dest, machine, package, cutoff):
    with store.index.transaction() as db:
        db.execute("DELETE FROM upload_buffer WHERE seq<=?", (cutoff,))
        db.execute("INSERT OR IGNORE INTO package_receipts VALUES(?,?,?)", (dest, machine, package))
        db.execute("DELETE FROM processing_meta WHERE name='uploadAttempt'")


def push(manager, client, box, force=True):
    store, dest = manager._usage_data, destination(manager)
    with store.index.transaction() as db:
        attempt = store.index.get(db, "uploadAttempt")
        limit = db.execute("SELECT MAX(seq) FROM upload_buffer").fetchone()[0]
    if attempt:
        if attempt["destination"] != dest:
            from monitor_cloud import CloudError
            raise CloudError("Resolve the pending upload on its original cloud destination before changing destinations", 409)
        descriptor = _descriptor(client, box, store.machine_id, attempt["package"])
        if descriptor is not None:
            _acknowledge(store, dest, store.machine_id, attempt["package"], attempt["cutoff"])
        else:
            # A descriptor PUT might still have completed after its response was lost.
            # Retry the identical attempt before allowing another package to be published.
            with store.index.transaction() as db:
                pending = list(db.execute("SELECT seq,arrived,data FROM upload_buffer WHERE seq<=? ORDER BY seq", (attempt["cutoff"],)))
            package, compressed, descriptor = prepare(store.machine_id, [json.loads(row[2]) for row in pending], pending[0][1], pending[-1][1])
            publish(manager, client, box, store.machine_id, package, compressed, descriptor)
            _acknowledge(store, dest, store.machine_id, package, attempt["cutoff"])
    uploaded = 0
    while limit is not None:
        with store.index.transaction() as db:
            pending, size = [], 0
            for seq, arrived, data in db.execute("SELECT seq,arrived,data FROM upload_buffer WHERE seq<=? ORDER BY seq", (limit,)):
                pending.append((seq, arrived, json.loads(data)))
                size += len(data.encode("utf-8")) + (len(pending) > 1)
                if size + envelope_bytes(store.machine_id, pending[0][1], arrived) > PACK_RAW_BYTES:
                    break
        if not pending or not force and time.time() < pending[0][1] + 1800:
            break
        package, compressed, descriptor = prepare(store.machine_id, [row[2] for row in pending], pending[0][1], pending[-1][1])
        with store.index.transaction() as db:
            store.index.put(db, "uploadAttempt", {"destination": dest, "package": package, "cutoff": pending[-1][0]})
        try:
            publish(manager, client, box, store.machine_id, package, compressed, descriptor)
        except Exception as exc:
            # Before descriptor publication the failure is definitive. The buffer may
            # grow and its next snapshot includes those new arrivals.
            if not getattr(client, "_package_descriptor_started", False) or getattr(exc, "http_status", None) in {400, 401, 403, 404, 405, 409, 413, 423, 507}:
                with store.index.transaction() as db:
                    db.execute("DELETE FROM processing_meta WHERE name='uploadAttempt'")
            raise
        _acknowledge(store, dest, store.machine_id, package, pending[-1][0])
        uploaded += 1
    return {"packagesUploaded": uploaded, **store.buffer_status()}


def fetch(manager, client, box):
    store, dest = manager._usage_data, destination(manager)
    downloaded = 0
    bootstrap = False
    with store.index.transaction() as db:
        bootstrap = store.index.get(db, "legacyPublicationPending", False)
        seen = set(db.execute("SELECT machine,package FROM package_receipts WHERE destination=?", (dest,)))
    for machine in sorted(manager._optional_remote_list(client, "usage/v5/packages")):
        for name in sorted(manager._optional_remote_list(client, f"usage/v5/packages/{machine}")):
            if not name.endswith(".enc"):
                continue
            package = name[:-4]
            if (machine, package) in seen and not bootstrap:
                continue
            descriptor = _descriptor(client, box, machine, package)
            data = records(client, box, machine, package, descriptor)
            if bootstrap and machine == store.machine_id:
                with store.index.transaction() as db:
                    db.executemany("DELETE FROM upload_buffer WHERE record_key=? AND seq<=?", ((record_key(row["kind"], row["row"]), store.index.get(db, "legacyPublicationCutoff", 0)) for row in data))
            store.import_package(dest, machine, package, data)
            downloaded += 1
    if bootstrap:
        with store.index.transaction() as db:
            db.execute("DELETE FROM processing_meta WHERE name IN ('legacyPublicationPending','legacyPublicationCutoff')")
    return {"packagesDownloaded": downloaded, "payloadsDownloaded": downloaded}


def sync(manager, client, box, force=True, download=True, upload=True):
    from monitor_cloud import CloudError
    result, errors = {}, []
    # Bootstrap must reconcile old publication candidates before attempting upload.
    for name, enabled, operation in (("fetched", download, lambda: fetch(manager, client, box)), ("published", upload, lambda: push(manager, client, box, force))):
        if not enabled:
            result[name] = {"skipped": True}
            continue
        try:
            result[name] = operation()
        except Exception as exc:
            errors.append((name, exc))
            if name == "fetched":
                with manager._usage_data.index.transaction() as db:
                    if manager._usage_data.index.get(db, "legacyPublicationPending", False):
                        break
    if errors:
        raise CloudError("Usage synchronization partially completed; " + "; ".join(f"{name}: {exc}" for name, exc in errors), 502,
            details=[result], decrypt_failed=any(getattr(exc, "decrypt_failed", False) for _, exc in errors))
    return result
