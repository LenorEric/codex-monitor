# Canonical → final dashboard delivery

The pending local contract is 27, following released local 26/cloud 5 (v1.5.35). Cloud contract 5 and the HTTP/WebSocket wire formats stay unchanged. Both permanent canonical datasets use the same `FinalDelivery` processor; only dataset selection and stream routing differ. Contract 27 targets quota ranges affected by reset drift for recalculation after historical compaction.

## Durable state

Canonical transactions enqueue changes through SQLite triggers, together with ingestion checkpoints, upload entries or cloud receipts. Duplicate inserts and unchanged quota writes produce no work. Cloud inputs change Merged only.

| Table | Purpose |
| --- | --- |
| `delivery_items` | Final serialized wire rows: view, account, channel, stable ID, sorting timestamp, interval bounds, body |
| `delivery_accounts`, `delivery_models`, `delivery_meta` | Small account/model catalogs, shared state, epoch and stream cursors |
| `delivery_pending` | Unconsumed token identities, quota old/new extents and account invalidations with increasing sequence |
| `delivery_journal` | Sequenced wire messages, bounded to 24 hours and 64 MiB |

Indexes support ordered output, interval predecessors/successors and overlapping ranges. Valid chart rows, including intervals with no costs, form the attribution directory. There is no token-to-interval membership table, separate prepared token/quota copy, or permanent full JSON/gzip snapshot.

## Incremental processing

New token identities are read by canonical primary key and sorted within the pending batch. Projection queries select time, model, tier, four display counters and saved cost directly, without reconstructing canonical dictionaries. Integer money conversion remains the existing 1e-8 USD display precision; stored canonical costs are unchanged.

Only the new batch is grouped: API accounts use existing UTC minute groups; other accounts use current 5h/7d boundaries and nearest assignments. Ordered event/interval cursors hold two neighboring edges per window. Accumulators retain model/tier totals and weighted-time statistics, never contributing-event lists. Each published group receives an independent stable ID.

**Published token rows are immutable.** Adding `7` to `1234 / 56` produces `1234 / 56 / 7`. Later quota boundaries may resolve to `12 / 34 / 56 / 7`, but neither live output nor a refreshed snapshot splits the old `1234` row. Quota changes alter attribution charts only, preserving tokens, recorded costs and old display rows.

Quota work retains discontinuous extents and merges only overlaps. The processor reads actual neighboring canonical records, updates usage connection flags and valid cost edges, and recalculates changed or neighboring attribution intervals. It scans candidate canonical tokens within each interval plus the existing 600-second nearest-assignment allowance, comparing neighboring edges to preserve the earlier-interval tie rule. This is not a fixed ±600-second quota invalidation: interval length and adjacent edges determine the affected region.

At service startup, delivery rows are rebuilt once from canonical data before dashboard workers start. The rebuild stages replacements and publishes them atomically; failure preserves the previous delivery rows. A successful rebuild resets the delivery epoch so clients request a fresh snapshot. Legacy delivery rows without canonical sources remain intact.

Usage IDs depend on window, start and business state, not a moving range end/reset deadline. Chart IDs depend on both endpoint usage IDs. An unchanged range tail extends the usage row and current reset without scanning historical tokens unless its actual adjacency changes. Empty time gaps do not produce buckets, work or loops.

A single delivery lock serializes expensive calculation and snapshot encoding. Calculation uses a consistent SQLite read transaction and a system-temporary staging database, not a long main-database write transaction. Publication atomically applies final row edits, appends journal messages, advances cursors and acknowledges work through the captured sequence. Concurrent ingestion remains queued for the next batch. Pre-publication failures leave old delivery intact; committed publication cannot append the same token work again, including recovery after acknowledgement failure.

Restart loads small metadata and resumes pending work. Ordinary ingestion, cloud imports and restart do not bootstrap history or reset epoch. One streaming bootstrap is reserved for a missing derived store; explicit repair queues quota/attribution work while retaining frozen token rows. Old final rows remain until the replacement diff commits.

## Complete snapshots

`GET /api/dashboard` preserves `{epoch,cursors,models,state,accounts}`. Each account contains `{id,label,local,merged}`. Each view has `quota`, `tokens`, `legacy`, `usage:5h`, `usage:7d`, `chart:5h` and `chart:7d`.

An HTTP request reads final delivery only, with no canonical business calculation. A consistent read transaction streams individual rows through JSON and optional level-3 gzip into a system temporary file. The read transaction ends before network transmission, so a slow client does not retain a database snapshot or grow WAL. No complete Python history tree or simultaneous raw/gzip byte buffers are created.

Concurrent requests for the same head/encoding share one temporary artifact and independent readers. New heads invalidate the reusable artifact; in-flight old readers can finish and catch up through journal cursors. Updates never generate unrequested snapshots. Temporary files are removed when their final owner closes or the service shuts down.

| Collection | Positional wire fields |
| --- | --- |
| `usage:5h`, `usage:7d` | `[id, first, last, percentage-or-null, multiplier, flags]` |
| `tokens` | `[id, weightedTimestamp, models]` |
| Token model entry | `[modelIndex, tier, freshInput, cachedInput, output, cacheWrite, costUnits]` |
| `chart:5h`, `chart:7d` | `[id, start, end, weightedTimestamp, multiplier, normalizedUsageIncrease, models]` |
| Chart model entry | `[modelIndex, costUnits, attributedNormalizedUsage]` |
| `legacy` | `[startedTimestamp, updatedTimestamp, tokenModelEntries]` |

Bulk model entries index the root model dictionary; live rows use model strings. Weighted timestamps are integer Unix seconds: nearest-integer cost-weighted time, or arithmetic mean for zero-cost groups. The quota object maps 5h/7d to current reset timestamps or null. Usage flag bit 0 marks a break before a run. Historical canonical multipliers and costs are used without replay or repricing.

## WebSocket recovery

`/api/dashboard/live` uses the existing listener. Subscribe with `{type:subscribe,epoch,cursors}`; packets are `{epoch,stream,seq,channel,account,data}`.

- `state`: shared current account/control/availability state.
- `account`: account discovery or label change.
- `quota:5h`, `quota:7d`: `[resetTimestamp]`.
- `tokens`: append-only new groups; no replacement/deletion protocol.
- `usage:5h`, `usage:7d`, `chart:5h`, `chart:7d`: `{set:[rows],remove:[ids]}`.

Journal replay is paged at roughly 256 KiB rather than loading an entire backlog. An individual outbound message remains limited to 4 MiB. Socket timeouts and ping/pong bound slow-client work. Missing journal coverage or changed epoch requests a new snapshot; ordinary cloud imports retain epoch and send Merged deltas. Epoch changes are reserved for conversion or delivery discontinuity.

The WebUI retains Local/Merged switching and local date/model/account filtering. The extension shares the live connection; status-only subscribers need no history download. An old snapshot remains usable through its included cursors, including late events whose occurrence time precedes the snapshot time.

## Contract 26 conversion

Contract 26 combined identity-store cleanup with final-output storage conversion. Keeping the old layout would retain preparation copies and permanent snapshots; reader changes alone cannot remove these saved structures.

Conversion streams the old snapshot BLOB through a bounded JSON reader, then applies its covered journal suffix. Duplicate baseline IDs or failed row round trips abort safely. When suffix coverage is unavailable, saved prepared rows supply a streaming baseline without resampling canonical history or repricing. Outstanding legacy work becomes pending invalidations. Only after validation are preparation/snapshot tables removed and the delivery epoch changed once. Canonical, upload attempts/buffer, parser checkpoints and receipts remain intact. Existing migration backup/journal recovery applies; no automatic VACUUM or release packaging runs.

## Validation and measurements

Run `python -m unittest discover -p "test_*.py"`, `node test_dashboard_loading.js`, and `npm run check`. `test_final_delivery.py` compares local chart edits against the full reference algorithm, verifies frozen groups, publication/restart recovery, migration rollback, cloud isolation and actual HTTP/WebSocket transport. Fixture servers and temporary roots are closed after testing.

Run `python benchmark_final_delivery.py --source-db SOURCE.sqlite3 --report final-delivery-benchmark.json` with optional development dependencies `psutil` and `apsw`. It backs up the read-only source into isolated system-temp databases and reports CPU, sampled peak RSS, canonical rows returned, process write bytes and table/index allocations. See [measured results](final-delivery-benchmark.md). No production migration or automatic file compaction is performed by the benchmark.
