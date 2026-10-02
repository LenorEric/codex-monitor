# Incremental processing implementation and validation

Implemented on 2026-10-02 as runtime 1.5.9, local data contract 8. The WebDAV v4 wire contract is unchanged.

Normal capture resumes persisted parser checkpoints, consumes bounded appended bytes, and updates indexed facts, totals, sessions, dependent cost intervals, and dirty publication days. Token capture has its own worker, independent of quota network requests. Watchdog notifications feed a bounded queue, with polling, discovery, and block audits providing fallback verification.

Quota appends use persisted continuity state and an exact rate index. Stable chronological appends update only new points. Late data, corrections, rate changes, reset lookahead, rule changes, and display ageing can trigger a replay for the affected account and view. The processing diagnostics report replay reasons and row counts. These exceptional replays still materialize the affected account history; they are not a constant-memory operation.

HTTP history snapshots and large WebDAV data payloads stream through disk-backed spools. Changed peer payloads import only changed logical rows. Publication rebuilds dirty days and due consolidation scopes; collection periods and validated peer ownership survive derived-state repair.

Migration 8 preserves canonical histories and durable v4 metadata. Migration backups and rollback use SQLite's backup API to include committed WAL data. Prepared ledger batches recover idempotently after durable append. `--repair-processing` invalidates derived checkpoints and projections without deleting canonical histories or v4 ownership metadata.

## Verification

- `python -m unittest discover -q`: 532 tests run, all passing, one skipped.
- `node --check extension.js`: passed.
- `node test_management_queue.js`: 7 tests passed, including notification lifecycle and restart behavior.
- New tests cover warm idle/restart, append-only work, crash recovery, source rewrites and audits, quota equivalence against full reference algorithms, peer additions/removals, repair preservation, streaming snapshot isolation, encryption and content-hash compatibility, malformed payloads, and migration rollback with WAL.

All test servers and watchers are stopped by teardown. No production monitor was started for this validation.

## Synthetic scaling check

Run `python benchmark_processing.py --rows 1000 10000 100000` to reproduce the workload. Each fixture contains one account with constant quota percentages and chronological timestamps. The benchmark asserts no idle ingestion work, exactly one imported append row, no historical quota replay, and canonical bytes read equal to the appended row.

| Existing quota rows | Initial import and projection | Average idle refresh | One quota append |
| --- | ---: | ---: | ---: |
| 1,000 | 0.279 s | 7.9 ms | 16.8 ms |
| 10,000 | 3.523 s | 6.8 ms | 14.0 ms |
| 100,000 | 49.705 s | 6.3 ms | 13.3 ms |

The recorded run overlapped other validation, so these are illustrative local timings, not hardware guarantees. Each append imported one record, read 192 canonical bytes, and replayed zero historical quota rows. Bounded verification reads were 11,456, 10,432, and 8,384 bytes respectively. Windows process CPU timing was too coarse for useful per-append CPU figures.

Initial migration/bootstrap, periodic metadata inventory, audits, retention compaction, source replacement, explicit repair, and retrospective continuity replay retain costs proportional to their affected scope. Full responses and changed closed-day transfers also retain their output-size cost. This check does not establish performance for 100,000 source files, one million records, percentile-changing workloads, or live WebDAV throughput.
