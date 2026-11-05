# Canonical storage conversion comparison

Measured on Windows with system Python on 2026-11-05. The source was an active contract-22 database, read only through SQLite backup; all conversion and edits ran on isolated workspace copies. No live data was migrated. Temporary database copies were removed after measurement.

## Allocation, including indexes

| Metric | Contract 22: JSON | Contract 23: typed |
| --- | ---: | ---: |
| Canonical tables | 294.17 MiB | 167.86 MiB |
| Canonical indexes | 106.11 MiB | 144.94 MiB |
| Canonical total | 400.27 MiB | 312.80 MiB |
| Identity tracking (unchanged) | 76.71 MiB | 76.71 MiB |
| Other allocated objects | 292.50 MiB | 292.51 MiB |
| Reusable free pages | 320.65 MiB | 408.11 MiB |
| Database logical file size | 1,091.46 MiB | 1,091.46 MiB |

Canonical tables plus indexes decreased by **21.85%**, or **87.47 MiB**. The file was not compacted; reclaimed pages are available for subsequent writes. Conversion temporarily retains both layouts, and the production migration also takes a journaled database backup. Other allocated objects differ by schema-page allocation, not a delivery rebuild.

## Preserved record counts

| Dataset | Kind | Entries |
| --- | --- | ---: |
| local | quota | 60,488 |
| local | tokenLedger | 41,746 |
| merged | quota | 165,750 |
| merged | tokenLedger | 47,832 |

Conversion with per-record round-trip verification took **45.96 seconds**, excluding initial backup time. This is a single-machine measurement, not a startup-time guarantee.

## Representative workload

The largest Merged quota account contained 97,125 quota rows and 25,165 token rows. Timings are medians of three warm runs; tests were also running during this measurement, so small timing differences are not controlled performance results.

| Operation | JSON | Typed |
| --- | ---: | ---: |
| Read and materialize account records | 1.377038 s | 1.122039 s |
| Read + common quota/token interpretation | 2.358200 s | 2.612175 s |
| One observed-value SQL update + rollback | 0.000051 s | 0.000021 s |

Reads and single-value edits were faster in this run; interpretation was slower. Typed storage does not establish a universal speed improvement. Preparation covers `quota_point`, `usage_runs` and `token_row`; it excludes delivery-table writes, full cost attribution, gzip, network delivery and browser rendering. The SQL update is not a benchmark of the full quota range merge processor.

## Reproduction

```text
python benchmark_canonical_storage.py <contract-22.sqlite3> --output canonical-storage-benchmark.json
```

The benchmark requires APSW only when system SQLite lacks `dbstat`. It adds no production dependency. Reports contain aggregate counts, sizes and timings; account IDs and payloads are excluded. The JSON report contains the individual table/index allocations.
