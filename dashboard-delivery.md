# Dashboard delivery and startup

Pending local data contract: 15. Scoped dashboard HTTP protocol: 4. The v3 series endpoint returns 410; the extension and benchmark use scoped manifests and chunks. Projection maintenance deadlines live in SQLite, without a JSON display-cache fallback.

## Loading order

The dashboard starts status and selected-history requests independently. Status renders before password setup completes. Exact token/cost summaries arrive in the scope manifest, followed by chart chunks.

The selected scope is restored before requesting history. A selected day requests full detail for that day directly. Presets request only the selected Local/Merged view. The inactive view and detailed historical days are not automatically downloaded.

Usage charts render before scatter charts, with a paint opportunity between tasks. Offscreen charts wait for intersection with the viewport. Scope changes invalidate queued chart work, and obsolete responses cannot replace the active selection.

VS Code retains an existing webview when reopening it. Its history bridge deduplicates identical requests while keeping different scopes independent.

## Incremental processing

The existing processing_quota_points table retains exact calculated quota points. Exact sessions and cost intervals remain in processing_display. Display-range queries do not rerun historical continuity or ledger attribution algorithms.

Delivery triggers track both old and new UTC day partitions when exact records change. A background worker rebuilds only dirty day partitions, then atomically publishes their manifest generation after all dirty partitions for that view are complete.

Each day has native, 5-minute, 30-minute, 2-hour, and daily overview levels. The most recent 24 hours use native detail; older presets progressively select coarser levels. The 5h and 24h presets retain native detail even if their latest available records are old.

Long All-history overviews reuse stable multiday rollups of old data. Recent data remains detailed. Quota reduction emits one point per account and time bucket, averaging raw and continuous percentages rounded to one decimal place. Resets, plan changes, missing values, and explicit gaps separate incompatible segments. Original sums and counts accompany averaged buckets so multiday rollups weight observations correctly without averaging rounded averages.

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

package.json and DATA_CONTRACT_VERSION both declare the pending contract 15; released contracts remain immutable. See [the unified-format audit](legacy-format-audit.md) for migration coverage. Runtime packaging includes monitor_delivery.py and propagates the contract into version.json. The implementation does not publish a release or restart an existing monitor automatically.

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

## Cost vs Time

The full-width chart uses costTime usage-event projections independently of quota intervals and session summaries. Event costs retain raw model keys and account identity; merged projections use the existing effective-event deduplication. Older session-only baselines are excluded from this chart.

Scoped protocol 4 adds costTime chunk collections and costTimeBounds/costTimeTotals manifest fields. Totals are exact and independently filterable by account/model. Presets anchor to the latest cost event, include that event, and replace the lower-boundary day with exact in-range costs before hourly aggregation. Selected local days use half-open midnight boundaries, including daylight-saving transitions.

Overview buckets sum costs per account/model into UTC minutes, hours or days. Multiday rollups retain daily cost buckets for exact regrouping into whole-day chart intervals. The browser fills chronological gaps, aggregates selected costs, and offers Spending bars or a Cumulative line; the combined chart also overlays both account quota windows on a right-hand percentage axis. Model selection applies to costs; quota lines remain account totals. The normalized usage option applies to the quota lines. Both windows reuse the account color from the other charts: 5h is dashed and 7d is solid, with matching legend swatches. The toggle defaults to Spending on reload.

Contract 11 adds costTimeResolution to scope manifests and retains minute buckets in native delivery levels, including old 5h/24h scopes. Coarser overviews declare their minimum hour/day resolution. The chart targets approximately 30 CSS pixels per complete bar and rounds the interval to any integer number of minutes, hours, or days (including 7 minutes). It never subdivides a coarser source bucket. The Collapse gaps / Collapse flat options fold only intervals with zero spending and unchanged 5h AND 7d quota for every selected account; missing quota never counts as zero. Any spending or percentage change preserves that interval. The chart reselects integer intervals using the unfolded width to keep active bars near 30 pixels. The time axis uses the selected bounds; clipped edge bars have widths proportional to their actual durations, and hover lookup uses time coordinates. Migration 11 invalidates delivery artifacts for automatic regeneration while preserving exact projections, canonical histories and parser/sync state.

Ordered migration 10 invalidates only derived dashboard projections and delivery artifacts. Canonical histories, peer records, parser checkpoints and synchronization ownership remain intact; projections rebuild from retained facts before delivery publication. Existing migration backups recover failures for safe retry.

Migration 12 rebuilds derived projections and delivery artifacts once from retained canonical facts. Canonical histories, peer records, parser checkpoints and synchronization ownership remain intact. Cost buckets sum cost and usage per account/plan/time bucket, including noncontiguous intervals, and derive rates from the summed values.

## Cost versus usage sampling intervals

Contract 12 calculates cost/usage from adjacent quota samples in the same valid cycle. It sums event costs over [previous sample time, current sample time), divides by the normalized continuous usage difference, and places the chart point at the current sample time. Flat/decreasing intervals and cycle breaks produce no cost/usage point; flat-interval costs are not carried into later intervals. Full rebuilds and incremental appends use the same calculation. Migration 12 invalidates derived projections and delivery artifacts for automatic rebuilding from preserved canonical records through the existing migration backup/rollback mechanism.

## Usage-account records (contract 12)

Delivery records carry usageAccountId as their only account identifier, omit repeated account labels and slot identifiers, and use numeric timestamp instead of checkedAt. Stable row keys remain for chart identity. Manifests carry fallback account labels once; current account metadata selects the surviving slot name using saved local slot order. Activation history retains deleted slots and their usage identities, while saved slotOrder preserves the relative priority of surviving slots across deletion and restart.

Quota polling deduplicates slots by usage identity. An account with an active slot is excluded from inactive polling and uses the active interval. Other accounts use one ready credential, preferring the highest-priority surviving slot in saved order, on a shared inactive-account timer. Credential refresh still writes back only to the selected slot with fingerprint validation.

Migration 12 rebuilds derived projections and delivery artifacts from preserved canonical facts through the existing backup/rollback mechanism. Canonical collection records retain internal provenance needed for credential management, session attribution and synchronization; this change normalizes dashboard delivery records.

## Device-local saved slot order (contract 12)

Each local accounts.json stores slotOrder, with first entry highest priority. New slots append at the tail; deleting a slot removes only that entry. The list is local account-management metadata and is not included in cloud account exports. Switching and quota polling leave it unchanged.

The cost-writing path notifies AccountManager only about new usage records. If the attributed slot is still active and is not already first for its usage account, it moves to the front and the manifest is atomically saved. An already-first active slot causes no write. Inactive slots, baselines and reconciled existing cost records do not cause promotion. The incremental ledger acknowledges the batch after the order save, so an interrupted save retries idempotently. Dashboard names, local account mapping and inactive credential selection all use the same saved order.

Startup reads the saved list rather than scanning cost history. Migration 12 initializes missing lists from existing slot insertion order, preserves any saved order and rebuilds derived projections and delivery caches. It does not create a history-derived priority index. Account manifests participate in migration backup/rollback. New/custom account vaults normalize the list before use through the same rules.

## SQLite disk reclamation (pending contract 13)

Release 1.5.14 is packaged at data contract 12, including the 11-to-12 migration. Its ZIP and VSIX are frozen. Contract 13 converts the existing SQLite database to incremental auto-vacuum and runs one full compaction before workers start. It preserves all tables and data, without rebuilding contract-12 caches. Existing migration backup/rollback handles failed or interrupted conversion. Compaction requires temporary disk space and can take time on the first startup.

New sync/processing databases enable incremental auto-vacuum before creating tables. After dashboard cleanup, maintenance checks at most once per minute and reclaims only when free pages exceed both 100 MiB and 20% of database pages. Each call processes at most 256 pages within a 50 ms scheduling budget, skips busy writers and processing locks, and performs a passive WAL checkpoint. SQLite statements already executing can exceed that budget; active readers may defer physical file shrink. Free space below the threshold remains available for normal cache churn. Live maintenance never performs a full vacuum.

## Pending contract 16: exact quota delivery

Quota observations now use backend sampling decisions persisted in SQLite. All dashboard resolutions retain exact accepted quota values and null rejected values; quota averaging and age-based point compaction are removed. Cost/session aggregation remains available. Delivery artifacts are rebuilt once with the pending reset-timestamp and sampling migration. Raw quota JSONL and cloud synchronization preserve every repeated and rejected observation.
