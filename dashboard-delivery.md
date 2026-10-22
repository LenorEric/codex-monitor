# Dashboard delivery and startup

Local data contract: 9. Scoped dashboard HTTP protocol: 4. The existing v3 series endpoint remains available for older clients.

## Loading order

The dashboard starts status and selected-history requests independently. Status renders before password setup completes. Exact token/cost summaries arrive in the scope manifest, followed by chart chunks.

The selected scope is restored before requesting history. A selected day requests full detail for that day directly. Presets request only the selected Local/Merged view. The inactive view and detailed historical days are not automatically downloaded.

Usage charts render before scatter charts, with a paint opportunity between tasks. Offscreen charts wait for intersection with the viewport. Scope changes invalidate queued chart work, and obsolete responses cannot replace the active selection.

VS Code retains an existing webview when reopening it. Its history bridge deduplicates identical requests while keeping different scopes independent.

## Incremental processing

The existing processing_quota_points table retains exact calculated quota points. Exact sessions and cost intervals remain in processing_display. Display-range queries do not rerun historical continuity or ledger attribution algorithms.

Delivery triggers track both old and new UTC day partitions when exact records change. A background worker rebuilds only dirty day partitions, then atomically publishes their manifest generation after all dirty partitions for that view are complete.

Each day has native, 5-minute, 30-minute, 2-hour, and daily overview levels. The most recent 24 hours use native detail; older presets progressively select coarser levels. The 5h and 24h presets retain native detail even if their latest available records are old.

Long All-history overviews reuse stable multiday rollups of old data. Recent data remains detailed. Quota reduction retains endpoints, extrema, flat-run edges, resets, plan changes, missing-data boundaries, and explicit gaps. Mandatory boundaries can exceed an ordinary point budget.

Costs are aggregated by summed cost and usage, with ratios derived afterward. Per-model usage contributions are separately maintained for exact filtered rate summaries. Token summaries group sessions by account and complete model set, preserving session counts for arbitrary combinations of selected models.

Exact summaries are built from exact rows, independently of plotted points. Presets retain the original latest-record time anchors. Sessions retain updatedAt/startedAt attribution; grouping does not switch to per-ledger-event day attribution.

## Prepared artifacts and consistency

Overview chunks are sanitized, encoded and compressed outside the writer transaction. Content hashes identify immutable bytes. HTTP manifest/chunk requests do not ingest logs or rebuild projections.

The manifest generation identifies one complete set of chunks and exact summaries. Clients conditionally fetch it with an ETag, reuse cached chunks with unchanged hashes, and replace changed chunks. Updates do not depend on an in-memory journal surviving a restart.

If source partitions change while preparation is running, the affected claim is rejected and the previous published generation remains available. Failed preparation does not publish partial data. Old artifacts have a download grace period; current manifest and partition references remain protected from garbage collection.

Missing derived blobs invalidate their references and schedule automatic preparation. Canonical histories are not deleted as part of this repair.

## Selected-day detail

The browser sends UTC instants for its local midnight boundaries, including 23/25-hour daylight-saving days. The server reads exact rows through time indexes, includes quota boundary context, and returns immutable pages of at most 1,000 collection rows.

Prepared day scopes are cached by relevant day revisions and boundary context. Unrelated changes do not invalidate them. The server retains up to 32 day scopes with access tracking; the client separately bounds its scope and chunk caches.

Day detail reflects retained canonical/exact history. Data already removed by a retention policy cannot be reconstructed by this feature.

## Migration and packaging

Migration 9 adds delivery indexes and fixes missing session timestamp indexes without rewriting canonical histories or deleting synchronization ownership metadata. The delivery worker builds the new artifacts automatically. Existing migration backup/rollback includes committed SQLite WAL data.

package.json and DATA_CONTRACT_VERSION both declare 9. Runtime packaging includes monitor_delivery.py and propagates the contract into version.json. The implementation does not publish a release or restart an existing monitor automatically.

Startup validates the saved console checkpoint's window and model cumulative totals. Missing totals are rebuilt once from retained local history, and the repaired checkpoint is persisted. Healthy checkpoints avoid this replay.

Unavailable quota windows clear live baselines while preserving historical totals. Restart seeds the previous sample as a baseline without counting its delta again. This repairs handling of the existing checkpoint format; it does not introduce another contract version.

## Reproducible measurements

Run `python benchmark_dashboard.py --url http://127.0.0.1:8765` against a running monitor. It performs one read-only history request, replays the served rows into an isolated delivery index, reports preparation/read timings, and removes its temporary database afterward.

The final replay on 2026-10-22 used 30,557 records:

| Operation | Time / size |
| --- | ---: |
| Existing live history request | 6.70 s |
| Legacy JSON/gzip serialization replay | 4.47 s / 1,728,243 writes |
| Initial background artifact preparation | 7.79 s |
| Prepared 5h manifest read | 3.24 ms |
| Prepared 5h manifest plus chunk reads | 10.61 ms / 58,310 compressed chunk bytes |
| Prepared All manifest read | 2.98 ms |
| Prepared All manifest plus chunk reads | 208.60 ms / 531,709 compressed chunk bytes |
| Uncached selected-day preparation and manifest | 44.09 ms |
| Cached selected-day manifest | 12.11 ms |
| Idle maintenance | 117.76 ms / zero chunks re-encoded |

This is a delivery replay of the served snapshot, not a benchmark of canonical ingestion, recovery, migration, full-precision reconstruction, browser painting, or VS Code transport. Timings vary with hardware and concurrent activity.

Validation includes exact summary/model filtering, full-detail day retrieval, local-day boundaries, moved sessions, late corrections, concurrent preparation, unchanged-chunk reuse, warm restart, missing-artifact recovery, HTTP ETags/gzip, and migration preservation.

`npm test`: 551 Python tests completed with one skip; all remaining Python tests and all 13 JavaScript tests passed. Python syntax, JavaScript syntax, runtime source/manifest consistency, and Git whitespace checks also passed.

Browser verification used an isolated fixture server, including Date, All, 5h, and Local/Merged switching. The browser tab, HTTP server, worker, and fixture were closed afterward.
