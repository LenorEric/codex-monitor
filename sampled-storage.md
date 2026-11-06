# Canonical storage and package synchronization

The pending target is local contract 25. The successful v1.5.33 release contains local 24/cloud 5. Cloud contract 5 is unchanged; generated release artifacts remain unchanged until an explicit release.

## Permanent canonical data

Consecutive 0% quota records with the same dataset/account/window, plan and multiplier can merge even when reset timestamps differ. The resulting range uses the reset value of the chronologically latest observation, not the greatest deadline; older covered points cannot replace it. Positive usage still requires exact reset equality, and intervening percentage/plan/multiplier changes remain barriers. Existing gap behavior is unchanged.

Migration 25 applies this explicitly requested consolidation to existing Local/Merged history. The measured estimate removes about 54,500 of 76,474 quota records (71.3%); leaving saved history untouched would retain those redundant moving-deadline records. This substantial reduction justifies a local contract step despite an unchanged table schema. The transaction preserves the earliest record identity/provenance and newest reset, queues ordinary quota preparation only for affected accounts, and leaves tokens/costs, parser checkpoints, buffer entries/attempts and cloud receipts intact. Journaled database backup/recovery supports retries. No raw replay, cloud migration, global delivery reset or automatic VACUUM is requested.

`monitor_canonical_store.py` owns typed `canonical_quota` and `canonical_tokens` tables through `CanonicalRepository`. Both use `dataset IN ('local','merged')`, stable account/record keys, identical columns and constraints, and the same in-memory record mapping and preparation helpers. The selector is outside the record payload. `canonical_seen` is a WITHOUT ROWID table keyed by `(view,record_key)` and retains cost/token input identities only; quota observations have no persistent input-ID registry. No canonical record refers to a cloud package, day, part, or publication generation.

| Table | Typed business columns |
| --- | --- |
| `canonical_quota` | Account slot/label, window, integer Unix-second coverage bounds, observation timestamp, accepted observed percentage, multiplier, plan, reset timestamp, version and identity provenance |
| `canonical_tokens` | Account slot/label, event/session identity, fractional Unix occurrence timestamp, raw/billing model, tier, all seven integer token counters, all five REAL USD cost components, optional source filename and provenance |

Tables use SQLite STRICT typing, compound primary keys `(dataset,account,record_key)`, and indexes for chronological reads, quota overlap/predecessor lookup, and token-event lookup. Counters/costs are nonnegative; numeric values must be finite; windows, datasets, tiers and versions are constrained. Quota bounds cannot be reversed. No extra historical accounting equations or acceptance rules are introduced.

Known business values are not stored as JSON. Small presence/integer masks retain optional-field and integer-versus-float semantics. Timestamp columns are numeric; companion text columns preserve original ISO spellings where the existing record contract uses strings. `extras_json` is nullable and contains only unknown extension fields in their original nesting. The shared mapper never parses JSON for ordinary canonical records. Stored costs retain their existing numeric values without repricing, scaling, or rounding. The `first`/`last` columns preserve released chronological indexing semantics; quota `range_first`/`range_last` retain the actual stored window boundaries.

The upload buffer, parser checkpoints, and variable operational metadata keep their existing JSON representation. Prepared quota/token tables remain typed; bulk snapshots and sequenced journals retain serialized client JSON. Automatic canonical compaction is disabled: deleted pages remain available for SQLite reuse.

Quota records use `quotaRunVersion: 1`, one window per record, and integer Unix-second bounds. `usedPercent` preserves the accepted observed value. Records retain the resolved multiplier, plan and reset time; the schema has no `continuous` or `reset_boundary` column. `QuotaPreprocessor` handles local raw observations once, before upload and canonical processing. Pending decreases exist only inside this preprocessor, isolated by local account/window, and require a later nondecreasing observation to confirm both points. Duplicate or older observations cannot confirm pending data. Accepted observations feed the upload buffer and both canonical datasets through individually committed local statements. A local quota failure prints its traceback and aborts remaining work; completed writes and preprocessing state are retained, without pending restoration, automatic retry or backfill.

Cloud observations were accepted by their source and bypass local preprocessing. Imports only map them into canonical points and merge Merged coverage, without sampling state, pending points, or uploads. Compatible observations extend a range. A conflicting point inside a range splits its coverage at t-1 and t+1 seconds. Existing same-second points and points inside compatible ranges suppress repeated observations without input-ID filtering. Local changes also enter Merged, and cloud imports change Merged alone.

Token records retain the current usage-event schema, stable account/event identity, model/tier, token components, and recorded monetary costs. Imports never reprice existing events. The same UI preparation implementation reads either canonical dataset directly.

## Temporary local state

`upload_buffer` contains immutable preprocessed records, a monotonically increasing sequence, and their actual arrival times. New quota windows have separate entries, preventing an in-flight upload from acknowledging a concurrently arriving window. Success deletes only the acknowledged sequence prefix. Failure retains the buffer; confirmed pre-publication failure permits the next attempt to include new arrivals.

`processing_meta.uploadAttempt` holds only the destination, package ID and sequence cutoff necessary to resolve uncertain publication. `package_receipts` records imported immutable package IDs per destination/machine. Receipts and canonical imports commit atomically. Neither table links packages to canonical history.

`source_checkpoints` retains file offsets, partial-line/parser context and counters. `source_blocks` contains consumed-prefix checksums. Parsed events are held in memory until canonical records, upload entries and checkpoints commit together. A changed prefix is parsed as a new file; committed canonical history is never removed merely because the original file changes or disappears.

## Cloud packages

The buffer becomes due 1,800 seconds after its first remaining arrival, including after restart. Manual upload flushes immediately. Packages are independent of UTC partitions. Append complete records until uncompressed canonical UTF-8 JSON crosses 75,000,000 bytes, then use another package. The crossing record stays in its package.

`usage/v5/data/<machine>/<id>.enc` contains a compressed/encrypted payload. `usage/v5/packages/<machine>/<id>.enc` is its immutable publication descriptor. The descriptor is created only after payload success. IDs address compressed content; descriptors contain record counts and actual event/arrival bounds. The downloader lists descriptors and GETs only unseen packages. Normal downloads and decoding use memory, not disk spools. Missing cloud files never prune canonical history.

Automatic downloads have their own 30-minute cadence. Upload retries start at 30 seconds and back off to 15 minutes. Upload failure does not prevent an independently due download. Every logical operation reports one queued/start and one final outcome.

## Prepared data and diagnostics

Existing delivery tables, bulk snapshots, sequenced journals and Local/Merged channels remain derived preparation state. Canonical commits queue the same preparation work for either view. The WebUI continues switching views without another history request.

Canonical JSONL backups and their runtime queue are retired. `saveRawQuotaResponses`, disabled by default, logs quota API response bodies only, with capture time, account and HTTP status. It excludes credentials, token events and processed output. `--raw-quota-log` optionally changes the diagnostic path. Existing backup files are untouched and are never ingestion inputs.

## Conversion and compatibility

Local migration 23 converts contract-22 JSON canonical records directly into the two typed tables. It verifies every record round trip, stable keys, bounds and dataset/type counts before dropping the old table. The existing migration journal and SQLite backup cover this database only; interrupted conversion or marker publication is recoverable and retryable. Invalid/unrepresentable records fail with their identity, leaving the original representation intact.

Local migration 24 physically drops `continuous` and `reset_boundary`, removes their retired extension JSON fields and shifts presence/integer masks to the new field positions. No replacement marker, column or table is added. Stopping access alone cannot remove saved columns, so the schema conversion requires the pending contract after released contract 23.

At the user's explicit request, migration 24 also treats all existing Local/Merged quota points as raw observations, ignoring historical `continuous` decisions. Each dataset/account/window is replayed chronologically (equal timestamps use ascending percentage). First observations and reset changes are accepted; a decrease waits for a later nondecreasing observation to confirm both points, further decreases replace the pending point, and unconfirmed tails are discarded. Saved ranges supply equal-valued observations at their two endpoints without inventing intermediate samples. Accepted records then merge in chronological order when `usedPercent`, `multiplier`, `resetAt` and `plan` match. Earlier retained coverage wins conflicts: a later incompatible record starting inside it is discarded in full, including any overhanging suffix. Different nonoverlapping states remain separate. The earliest merged record keeps its identity, metadata and lower bounds, while its upper bounds extend and absorbed records are deleted.

This explicit saved-data replay reduces historical record and index space; future writer changes alone cannot remove the saved redundancy. Conversion shares one transaction with field cleanup, rolls back on failure and supports retries. Upload buffers, package receipts and token history remain intact. Changed acceptance clears stale local preprocessing checkpoints and requests one delivery/attribution rebuild through the migration cascade's durable pending flag; no second legacy sampling or projection replay is requested. Pure compatible compaction preserves prepared results. Freed pages remain reusable by SQLite; no VACUUM or automatic database-file shrink is requested.

Released migration 24 also replaces the old `(view,kind,record_key)` identity table with the cost/token-only `(view,record_key)` table. It copies every `tokenLedger` identity, discards quota identities and drops the old table and indexes in the same transaction. Merely changing runtime filtering would leave the explicitly retired per-observation quota IDs on disk, so existing stores require this conversion. Released migrations remain intact, retries are safe, and a failure rolls back both identity and quota-schema changes.

Migration 23 preserves recorded costs, saved quota decisions, cost/token identity history, parser checkpoints, upload sequence/arrival/attempt state, package receipts, cached totals, prepared snapshots, journals and stream sequences without historical replay. Migration 24 adds only the explicitly requested quota replay and acceptance-dependent delivery refresh described above. Sequential local upgrades retain the released contract-22 initializer and conversion helpers in migration-only code. Custom data paths and historical JSONL files are unchanged.


Migration 22 preserves saved costs, quota decisions and both datasets, converts outstanding parsed source events before deleting the old event queue, and retires the legacy typed fact/publication/remote layout. Existing publication records become temporary candidates. The first online synchronization removes candidates already represented in migrated cloud packages, retaining unsent records; offline startup remains supported.

Cloud history and account files have already been converted offline. Cloud contract 5 is now checked without migration: older roots and populated roots with no marker are rejected, while empty roots can be initialized. Retired cloud readers, pack/catalog converters and migration registries are removed. Sequential local upgrades remain supported.

## Reproducible storage comparison

Run `python benchmark_canonical_storage.py <contract-22.sqlite3> --output canonical-storage-benchmark.json`. The source is read-only; conversion and measurements use temporary SQLite backups which are removed afterward. APSW is needed only for this development benchmark when system SQLite omits `dbstat`; it is not a runtime dependency. Compare table plus index allocations, excluding reusable free pages, rather than raw JSON length or total file size. Conversion temporarily needs both layouts and the migration backup; no VACUUM is performed.

See `canonical-storage-benchmark.md` for the measured fixture, storage and timing results. Preparation timings cover account reads and the shared interpretation functions, not network delivery, delivery-table writes, or gzip. Single-value edit timings are not full range-merge benchmarks.
