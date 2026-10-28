# Sampled storage and dashboard interpretation

Released local contract 17 is preserved. Pending contract 18 backfills observation-time multipliers and rebuilds derived interpretation storage. The cloud remains contract 4. `delivery_runs`, `delivery_tokens`, `delivery_charts`, and `delivery_members` contain typed, account-scoped interpretation state. `delivery_journal` commits sequenced changes with that state; `delivery_snapshot` holds the serialized/gzip bulk. None of these tables replace canonical histories or cloud ownership. SQL and network layouts are independent.

The following describes the original normalized sampled-store implementation; historical migration version references below describe that implementation’s development.

# Sampled storage, pending local contract 16

`monitor_sample_store.py` owns normalized raw payload insertion and reconstruction. Quota JSONL remains a complete append-only raw archive, including repeats and rejected readings; cloud synchronization preserves these records. `monitor_sampling.py` owns deterministic backend decisions. SQLite commits processed points, per-window accepted/protection state, and raw ingestion checkpoints together. Released local contract 15 / cloud contract 3 is the baseline; pending local contract 16 combines Unix reset-timestamp conversion with rebuilding processed sampling and display data. Pending cloud contract 4 retains the raw record shape with Unix reset timestamps.

| Stored table | Contents |
| --- | --- |
| `sample_records` | Integer sample identity; kind/subtype/schema, exact original timestamps, event/session/model identity, account/device references, canonical payload hash |
| `sample_accounts`, `sample_devices`, `sample_sources`, `sample_sessions`, `sample_models`, `sample_plans`, `sample_tiers` | Shared identifier dictionaries with integer foreign keys |
| `sample_sync` | Original provenance, record identity and local-only flag |
| `quota_samples`, `quota_windows` | Compaction boundary/count and separate 5h/7d readings, reset timestamps and historical plans; no derived continuous chart values |
| `processing_sampled_quota` | Exact raw/continuous decisions for each account, timestamp and local/merged view; rejected values remain null in continuous |
| `processing_sampling_changes`, `processing_meta` | Transactional replay work, per-window accepted sample timestamp/reset/plan, decrease protection, and runtime snapshots |
| `token_samples` | Seven token counters and five cost totals; event/session/model/source identity comes from `sample_records` |
| `baseline_sessions`, `baseline_models`, `baseline_totals`, `baseline_tiers` | Legacy session totals, model breakdowns, fast tokens and source high-water values |
| `processing_fact_refs` | Sample reference, integer source/account/session references, source position, lookup identity and source-specific slot/label attribution |
| `local_record_refs` | Sample reference, collection period, synchronization hash and record extensions |
| `remote_record_refs` | Sample reference, origin device, partition/day, envelope identity, schema/time metadata and envelope extensions |
| `sample_releases` | Transaction-local release candidates; only unreferenced candidates are deleted at commit |

Recognized payloads have no stored full JSON copy. Presence, null, float and boolean masks reproduce absent/null distinctions and original numeric types. Small container markers preserve empty, missing and null structures; extension sidecars preserve unknown fields. Exceptional numeric values that SQLite would coerce are preserved in extensions. Unknown record kinds remain opaque. Timestamp text and Python float precision are retained.

Source-specific slot/label fields are factored out before payload hashing and restored from each owner's attribution. Only identical remaining canonical payloads share a sample; conflicting versions retain different samples. Local synchronization hashes remain hashes of the complete original transport record. Owner replacement/deletion queues the old sample for indexed cleanup in the same transaction; another owner's reference prevents deletion.

The old `processing_facts`, `local_records` and `remote_records` names are SQL views. Their JSON columns are computed by the repository codec and occupy no payload storage. Internal callers use `monitor_sample_store.connect()` to register those functions. For direct SQL inspection with a normal SQLite client, query the typed physical tables instead. Processing predicates use typed ownership fields, including a source/session index and a typed legacy-baseline partial index.

Migration streams 256 owners per batch inside one SQLite transaction. It verifies every reconstructed record, synchronization hash, owner count and foreign key before dropping the old tables. Startup's existing backup/journal wraps the conversion and subsequent processing initialization. A failed conversion does not advance the contract marker; an interruption after commit but before marker update restores the original database on restart. JSONL, parser checkpoints, prepared ledger batches, slot order and remote ownership are retained.

## Measured database snapshot

The 2026-10-26 benchmark used a consistent backup of the current `usage_monitor_sync_cache-v4.sqlite3`. It converted 104,154 processing facts, 77,707 local records and 26,271 remote records to 104,154 shared samples. Migration took **171.7 seconds** on this machine. Per-record verification is included; the outer startup backup duration is excluded.

| Allocated sampled storage | Before | After |
| --- | ---: | ---: |
| Tables, including dictionaries and ownership | 204.72 MiB | 79.91 MiB |
| Indexes | 115.16 MiB | 96.66 MiB |
| Total | **319.89 MiB** | **176.57 MiB** |

The reduction is **44.8%**, or 143.32 MiB. These are SQLite `dbstat` allocations, including overflow pages and automatic indexes, excluding unchanged processing/chart caches. Dropped pages enter SQLite's freelist; existing incremental vacuum reclaims file space gradually. The live database was not modified by the benchmark.

| Query, warm median of seven runs | Before | After |
| --- | ---: | ---: |
| Source/position payload, one row | 0.022 ms | 0.157 ms |
| Account's latest 100 quota records | 0.147 ms | 5.599 ms |
| Source/session lookup, one matching row | 201.888 ms | 0.148 ms |
| Remote day partition, 100 envelopes | 0.152 ms | 7.159 ms |

JSON reconstruction adds CPU cost to payload reads. Metadata-only existence checks do not reconstruct samples. Idle processing retains its incremental checkpoints; cleanup visits release candidates rather than scanning sample history. The source/session lookup now uses its dedicated typed index.

Machine-readable table/index sizes, query plans and complete owner-history fingerprints are in `sample-storage-benchmark.json`. Reproduce on a fresh copy with `python benchmark_sample_storage.py SOURCE.sqlite3 NEW_COPY.sqlite3 REPORT.json`; APSW supplies `dbstat` when the system sqlite3 module lacks it. The command refuses to overwrite its copy destination.

Validation covers exact canonical/hash round-trips, unknown envelope kinds, compaction, legacy high-waters, numeric precision, missing/null fields, sharing, conflicting versions, source attribution, final-reference cleanup, failed conversion, journal recovery and retry. Existing ingestion, correction, source replacement, retention, projection, synchronization and idle/restart tests exercise the new repository paths. Python suite: 627 tests passed with one existing skip; JavaScript suites: 28 tests passed. Test processes terminate after completion.
