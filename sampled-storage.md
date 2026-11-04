# Canonical storage and package synchronization

The pending targets are local contract 22 and cloud contract 5. The successful v1.5.29 release contains local 21/cloud 4; generated release artifacts remain unchanged until an explicit release.

## Permanent canonical data

`monitor_canonical.py` owns `canonical_records`. Local and Merged use the same `(view, kind, account, record_key, first, last, data)` layout, JSON record types, units, and processing helpers. The selector is outside the payload. `canonical_seen` stores stable input identities for duplicate suppression; no canonical record refers to a cloud package, day, part, or publication generation.

Quota records use `quotaRunVersion: 1`, one window per record, and integer Unix-second bounds. `usedPercent` preserves the observed value; `continuous` is the accepted value or null. Records also retain the resolved multiplier, plan, reset time, and reset boundary. Compatible observations extend a range. A conflicting point inside a range splits its coverage at t-1 and t+1 seconds. Explicit points win over inferred coverage; identical identities do not apply twice. Local changes also enter Merged, and cloud imports change Merged alone.

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

Migration 22 preserves saved costs, quota decisions and both datasets, converts outstanding parsed source events before deleting the old event queue, and retires the legacy typed fact/publication/remote layout. Existing publication records become temporary candidates. The first online synchronization removes candidates already represented in migrated cloud packages, retaining unsent records; offline startup remains supported.

Cloud migration 5 converts the complete v4 history directly into chronological packages, preserving old sources until the new contract marker commits. It uses migration-only scratch for global chronological ordering, not a runtime remote cache. Failed conversion retains sources and retries deterministically. Historical codecs and migrations remain available for sequential upgrades.

These contract changes are required by the new permanent canonical representation, disposable upload state and immutable package discovery. They do not introduce historical repricing or replay solely to apply new business rules. Older clients reject the newer contract. The historical typed-storage benchmark and its report describe the released pre-22 layout, not the new canonical layout.
