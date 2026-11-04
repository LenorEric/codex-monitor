"""Frozen cloud-4 writer/reader used only to construct and verify migration fixtures.

These obsolete runtime strategies are deliberately excluded from release sources.
"""

import tempfile
import time
from monitor_cloud import CloudManager, CloudError, CryptoBox, WebDavClient, _timestamp
from monitor_usage_sync import v4_period


class LegacyUsageCloudManager(CloudManager):
    @staticmethod
    def _v4_sealed_day(day: str, content: dict, current_period: str, machine_id: str) -> dict | None:
        parts = {period: entries for period, entries in content["parts"].items() if period < current_period}
        if not parts:
            return None
        from monitor_streaming import Records, sorted_records
        streamed = any(isinstance(rows, Records) for rows in parts.values())
        entries = sorted_records(parts) if streamed else [entry for records in parts.values() for entry in records]
        layout = "parts" if day == current_period[:10] else "bulk"
        payloads = ({period: {"version": 1, "machineId": machine_id, "day": day, "partId": period, "records": records} for period, records in parts.items()} if layout == "parts" else {"bulk": {"version": 1, "machineId": machine_id, "day": day, "partId": "bulk", "records": entries if streamed else sorted(entries, key=lambda entry: entry["recordKey"])}})
        return {"day": day, "layout": layout, "payloads": payloads}


    def _v4_publish(self, client: WebDavClient, box: CryptoBox, force_full: bool = False) -> dict:
        return self._v4_publish_objects(client, box, force_full)


    def _v4_publish_objects(self, client, box, force_full):
        machine_id = self.machine_id
        for path in ("usage/v4/heads", f"usage/v4/months/{machine_id}", f"usage/v4/days/{machine_id}", f"usage/v4/data/{machine_id}"):
            client.ensure_directories(path)
        head, etag = self._v4_head(client, box, machine_id)
        old_catalog = self._v4_catalog(client, box, machine_id, head)
        now_period = v4_period(time.time())
        content, claims = self._usage_data.v4_publication_changes(time.time(), old_catalog, force_full)
        desired = {day: sealed for day, rows in content.items() if rows is not None and (sealed := self._v4_sealed_day(day, rows, now_period, machine_id)) is not None}
        removed = {day for day, rows in content.items() if rows is None}
        catalog = dict(old_catalog)
        published_manifests = {}
        uploaded, changed_days, consolidated = 0, set(), False
        for day, source in sorted(desired.items(), reverse=True):
            previous = old_catalog.get(day)
            if previous and previous["layout"] == "packed" and day not in claims:
                continue
            old_manifest = self._v4_day_manifest(client, box, machine_id, day, previous) if previous and previous["layout"] != "packed" else None
            consolidated |= bool(previous and previous["layout"] == "parts" and source["layout"] == "bulk")
            parts = {}
            for part, value in source["payloads"].items():
                from monitor_streaming import compressed_payload
                with tempfile.TemporaryFile() as spool:
                    payload_id = compressed_payload(value, spool)[0]
                parts[part] = payload_id
                if old_manifest is None or old_manifest["parts"].get(part) != payload_id:
                    client.transfer_category = "consolidation" if source["layout"] == "bulk" and previous and previous["layout"] == "parts" else "backfill" if previous is None and day < now_period[:10] else "payload"
                    self._put_usage_payload(client, box, "v4/data", machine_id, value)
                    uploaded += 1
            if old_manifest is not None and old_manifest["layout"] == source["layout"] and old_manifest["parts"] == parts:
                continue
            manifest = {"version": 1, "machineId": machine_id, "day": day, "layout": source["layout"], "parts": parts}
            if previous and "logicalHash" in previous:
                manifest["logicalHash"] = previous["logicalHash"]
            published_manifests[day] = manifest
            client.transfer_category = "metadata"
            manifest_id, _ = self._put_usage_payload(client, box, "v4/days", machine_id, manifest)
            catalog[day] = {key: manifest[key] for key in ("layout", "logicalHash") if key in manifest} | {"manifestId": manifest_id}
            changed_days.add(day)
        for day in old_catalog.keys() & removed:
            catalog.pop(day)
            changed_days.add(day)
        months = {}
        for day, reference in catalog.items():
            if reference["layout"] in ("bulk", "packed"):
                months.setdefault(day[:7], {})[day] = reference
        month_ids = {}
        for month, days in sorted(months.items()):
            value = {"version": 2 if any(ref["layout"] == "packed" for ref in days.values()) else 1, "machineId": machine_id, "month": month, "days": days}
            payload_id = self._usage_payload_bytes(value)[0]
            if head is None or head["months"].get(month) != payload_id:
                client.transfer_category = "metadata"
                self._put_usage_payload(client, box, "v4/months", machine_id, value)
            month_ids[month] = payload_id
            self._v4_month_cache[(machine_id, payload_id)] = value
        active_days = [(day, reference) for day, reference in catalog.items() if reference["layout"] == "parts"]
        if len(active_days) > 1:
            raise CloudError("Usage v4 has multiple active days", 409)
        active = {"day": active_days[0][0], **active_days[0][1]} if active_days else None
        changed = head is None or head["months"] != month_ids or head.get("activeDay") != active
        if changed:
            updated = {"version": 4, "machineId": machine_id, "generation": (head or {}).get("generation", 0) + 1, "months": month_ids, "activeDay": active}
            client.transfer_category = "metadata"
            new_etag = self._v4_write_head(client, box, updated, etag)
            self._state.setdefault("usageV4", {})["localHeadEtag"] = new_etag
            self._save_state()
        self._usage_data.acknowledge_publication({day: generation for day, generation in claims.items() if day in removed or day in desired}, now_period)
        if consolidated or old_catalog.keys() & removed or self._state.setdefault("usageV4", {}).get("lastGcDay") != now_period[:10]:
            try:
                self._v4_gc(client, box, machine_id, updated if changed else head, catalog=catalog, manifests=published_manifests)
            except Exception as exc:
                raise CloudError(f"Usage v4 publication completed, but daily cleanup is pending: {exc}", 502) from exc
            self._state["usageV4"]["lastGcDay"] = now_period[:10]
            self._save_state()
        return {"daysChanged": len(changed_days), "partsUploaded": uploaded, "dailyFiles": sum(ref["layout"] != "parts" for ref in catalog.values()), "headChanged": changed}


    def _v4_fetch(self, client: WebDavClient, box: CryptoBox, force_full: bool = False) -> dict:
        client.transfer_category = "metadata"
        try:
            items = client.list_details("usage/v4/heads")
        except CloudError as exc:
            if exc.http_status == 404:
                return {"machinesChanged": 0, "payloadsDownloaded": 0}
            raise
        remote = self._state.setdefault("usageV4", {}).setdefault("remote", {})
        changed, downloaded = 0, 0
        for item in items:
            if not item["name"].endswith(".enc"):
                continue
            machine_id = item["name"][:-4]
            if machine_id == self.machine_id or not force_full and item.get("etag") and remote.get(machine_id) == item["etag"] and self._usage_data.v4_cached_days(machine_id):
                continue
            for attempt in range(2):
                head, etag = self._v4_head(client, box, machine_id)
                if head is None:
                    break
                try:
                    catalog = self._v4_catalog(client, box, machine_id, head)
                    cached = self._usage_data.v4_cached_days(machine_id)
                    from monitor_usage_packs import UsageObjectReader
                    with UsageObjectReader(self, client, box, machine_id) as reader:
                        for day, reference in sorted(catalog.items()):
                            identity = {key: value for key, value in reference.items() if key != "logicalHash"}
                            if not force_full and cached.get(day, {}).get("reference") == identity:
                                continue
                            manifest = self._v4_day_manifest(client, box, machine_id, day, reference)
                            client.transfer_category = "backfill" if day < _timestamp()[:10] and day not in cached else "payload"
                            old_parts = cached.get(day, {}).get("layout", {})
                            if reference["layout"] == "packed":
                                payloads = {"bulk": reader.entries(day, reference)}
                            else:
                                required = set(manifest["parts"]) if force_full else {
                                    part for part, payload_id in manifest["parts"].items() if old_parts.get(part) != payload_id}
                                payloads = {part: reader.part(day, part, manifest["parts"][part]) for part in sorted(required)}
                            self._usage_data.v4_apply_day(machine_id, manifest, payloads, force_full, reference=identity)
                        downloaded += reader.downloaded
                    self._usage_data.v4_remove_days(machine_id, set(cached) - set(catalog))
                    remote[machine_id] = etag
                    self._save_state()
                    changed += 1
                    break
                except CloudError as exc:
                    if attempt or exc.http_status != 404:
                        raise
                    self._v4_month_cache.clear()
                    continue
        return {"machinesChanged": changed, "payloadsDownloaded": downloaded}


    def _fetch_usage_data(self, client, box, force_full=False):
        return self._v4_fetch(client, box, force_full) | {"fetchedAt": _timestamp()}

    def sync_usage_data(self, automatic=False):
        client, box = self._connection(True)
        try:
            return {"published": self._v4_publish(client, box), "fetched": self._v4_fetch(client, box)}
        finally:
            self._record_usage_transfers(client)
