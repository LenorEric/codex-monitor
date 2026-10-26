# Code simplification audit — 2026-10-23

The repository has concrete stale and redundant code. The largest opportunities are retiring superseded protocols and removing test-only legacy APIs. File length alone is a poor guide: much of the account, recovery, indexing, and synchronization code still has active callers.

This audit covers the current working tree, including the existing uncommitted Cost vs Time changes. Application code was not changed. All test scripts must be retained; reduce deployed/runtime code separately from test coverage. Tests are excluded from the standalone runtime; some are accidentally included in the inspected VSIX, which should be fixed through packaging exclusions. Three GPT-6 Luna subagents at max effort reviewed backend processing, UI, and cloud/account code; findings were checked against callers, tests, and shared infrastructure before inclusion.

**Size and baseline.** The root runtime sources contain 16,991 lines across 24 Python/JS/HTML files. Tests add 11,033 lines; the generated `release/runtime` tree adds another 17,342 lines including support files. These categories have different purposes and should not be treated as independent implementations. Verification passed: 563 Python tests run, one skipped; 7 management JavaScript tests and 18 dashboard JavaScript tests passed; extension syntax check passed. Test commands exited, and no live-traffic monitor was started.

| Order | Recommendation | Evidence / benefit | Risk |
| --- | --- | --- | --- |
| 1 | Retire the superseded dashboard stream protocol | Current UI uses manifests/chunks, but stream journals and projector patches are still maintained | Public read-API compatibility |
| 2 | Resolve and retire the unreachable hash-pack sync implementation | 256 function-body lines outside production call paths; old tests still pin it | Decide legacy remote-data support explicitly |
| 3 | Remove unused history/event APIs and orphan helpers | 223 function-body lines in 22 top-level functions, plus unused methods | Tests and incidental Python imports |
| 4 | Consolidate atomic file writes | Several implementations of temporary write / flush / fsync / replace / cleanup | Preserve permissions, Windows retries, streaming, and exact serialization |
| 5 | Remove write-only state and redundant work | Unused snapshots/diagnostics; duplicate sorting and expensive hover lookup | Low, with side effects retained |
| 6 | Tighten extension packaging and refresh documentation/tests | Built VSIX contains development artifacts; docs describe the old architecture | Low |

**1. Retire `/api/series` deliberately; remove its maintenance work as well as its route.**

The current [dashboard loader](D:/Workspace/codex_monitor/dashboard.html:627) fetches scoped `/api/dashboard` manifests and immutable chunks. Its extension bridge sends `getCodexDashboard`, handled in [extension.js](D:/Workspace/codex_monitor/extension.js:292). Neither HTML page sends `getCodexUsageSeries`. The extension's [getSeries method](D:/Workspace/codex_monitor/extension.js:102) and [series message handler](D:/Workspace/codex_monitor/extension.js:337) therefore have no current UI caller.

Nevertheless, [monitor_dashboard.py](D:/Workspace/codex_monitor/monitor_dashboard.py:1036) retains stream IDs, consecutive indexes, snapshots, merged revisions, patch comparison, and a journal capped at 16 MiB / 512 batches / 24 hours. [DashboardProjector.refresh](D:/Workspace/codex_monitor/monitor_projection.py:324) builds upsert/delete collections that [dashboard cache maintenance](D:/Workspace/codex_monitor/monitor_dashboard.py:1207) feeds into that journal. This is ongoing work even when no client calls the old route.

Retirement can remove the series handler at line 2123, the stream-specific methods around lines 1036–1133, patch-comparison helpers around lines 766–786, stream constants and status fields, the old extension bridge, and eventually projector patch collection. Keep projector database updates, delivery preparation, cloud merging, and status revision/ETag behavior. The full-snapshot `Records` serialization branch is another candidate after its final consumer is gone; the general streaming/encryption module still has live consumers.

[benchmark_dashboard.py](D:/Workspace/codex_monitor/benchmark_dashboard.py:21) is an in-tree caller that must move to manifests/chunks. Both READMEs still describe `/api/series` as the normal frontend protocol. The route is also a documented public read API, so this is an explicit API retirement rather than an incidental private-function deletion. Choose a compatibility window or a clear retired response, update the benchmark/docs, and transfer restart/recovery/cloud-merge coverage to the current API. No persistent data format needs to change for this cleanup.

**2. The old cloud protocol is test-only, and its supposed upgrade path is disconnected.**

Normal fetch, push, and automatic sync use [v4 entry points](D:/Workspace/codex_monitor/monitor_cloud.py:2230). The older `_publish_usage` and `_fetch_usage` methods are not called by those entry points or by the queued-operation dispatch map. Their isolated helper graph includes:

- `_usage_pointer_path` at line 1631.
- The pointer parsing/writing, record-pack construction, pack validation, verification sampling, and publication family at lines 1786–1942.
- `_fetch_legacy_usage`, `_migrate_legacy_usage`, and `_fetch_usage` at lines 2115–2228.

Those methods total **256 function-body lines**, excluding blank gaps, related constants, and any further storage-side cleanup. [Old contract tests](D:/Workspace/codex_monitor/test_cloud_contract_v3.py:45) directly invoke the private old methods. Meanwhile, the [integration test](D:/Workspace/codex_monitor/test_monitor_codex_usage.py:7498) is skipped with the reason that legacy hash-pack integration was replaced by v4.

The important distinction: current [v4 fetch](D:/Workspace/codex_monitor/monitor_cloud.py:2044) only lists `usage/v4/heads`; [encryption inventory](D:/Workspace/codex_monitor/monitor_cloud.py:1335) also enumerates v4 usage data. Legacy remote pointers are already outside those live paths. Deleting unreachable methods would not newly cause that omission, but retaining them gives a misleading impression that old remote history is still migrated.

Resolve the support decision first: either wire a verified legacy import into the upgrade process, or explicitly retire that format and remove its dead runtime implementation. Retain the test scripts and adapt affected assertions to the supported contracts and preservation/recovery scenarios. A persistent-contract change must follow the repository's consecutive migration/version requirements. Keep shared v4 helpers such as `_usage_payload_path`, `_usage_payload_bytes`, `_decode_usage_payload`, `_put_usage_payload`, and `_download_usage_payload`.

**3. Remove the unused legacy history/event surface in a focused pass.**

A conservative reference graph, treating every class method as potentially live, found **22 unreachable top-level functions totaling 223 body lines**. Repository searches found no callback/string lookup that restores a production entry point. The main groups are:

| File | Unused functions | Body lines |
| --- | --- | --- |
| [monitor_history.py](D:/Workspace/codex_monitor/monitor_history.py:264) | `load_history`, `normalize_delta_event_account`, grouped-delta detection/expansion/compaction helpers, `append_history`, `write_history`, `format_history_row` | 114 |
| [monitor_history.py](D:/Workspace/codex_monitor/monitor_history.py:710) | `state_path_for`, `compact_debug_state`, `apply_runtime_cost_measurement`, `compact_history` | 39 |
| [monitor_events.py](D:/Workspace/codex_monitor/monitor_events.py:894) | `sample_debug_log_row`, `event_identity`, `valid_delta_events`, `new_valid_delta_events`, `normalized_window_status`, `format_valid_delta_event`, `print_valid_delta_events` | 55 |
| [monitor_dashboard.py](D:/Workspace/codex_monitor/monitor_dashboard.py:283) | `dashboard_points_from_state` | 15 |

The grouped reader/writer island is exercised by old tests. Startup migrations use separate parsers in `monitor_auto_update`; removing these unused functions does not require changing stored files or migration behavior. However, the star-import chain through `codex_monitor_daemon` and `monitor_codex_usage` exposes the names incidentally. Keep the documented legacy CLI entry point; decide whether those Python imports are supported before removing their exports.

Keep `parse_history_rows`, `parse_history_jsonl_rows`, and `is_delta_event_row`: quota loading/backfill and persisted console-event hydration still use them. Keep historical `eventCostUsd` readers even if the unused runtime-cost producer is removed.

Additional orphan methods are [AccountManager._snapshot_live_into](D:/Workspace/codex_monitor/monitor_accounts.py:759) (25 lines), [CloudManager.rename_local_account](D:/Workspace/codex_monitor/monitor_cloud.py:1584) (3 lines), [new_placeholder_account_key](D:/Workspace/codex_monitor/monitor_cloud.py:2438) (2 lines), and [UsageDataStore.account_datasets](D:/Workspace/codex_monitor/monitor_usage_sync.py:518) (14 lines). No in-repository caller uses them. Keep active reconciliation and credential validation, especially `_validate_live_matches_saved`, which still guards release.

Retain all test scripts. If a legacy runtime API is retired, adapt affected tests to exercise the supported canonical quota/ledger path and preserve relevant recovery/data-preservation scenarios. A test that directly invokes an otherwise unreachable helper is not evidence that the helper is needed in production.

**4. Consolidate atomic-write mechanics without flattening their guarantees.**

The same basic algorithm appears in [monitor_common.write_json](D:/Workspace/codex_monitor/monitor_common.py:117), [monitor_accounts.atomic_write_bytes](D:/Workspace/codex_monitor/monitor_accounts.py:108), [monitor_auto_update._atomic_write](D:/Workspace/codex_monitor/monitor_auto_update.py:95), [monitor_history._atomic_write_bytes](D:/Workspace/codex_monitor/monitor_history.py:646), and [UsageDataStore._atomic_write](D:/Workspace/codex_monitor/monitor_usage_sync.py:776). Quota/ledger writers repeat parts of it too.

Use a small dependency-neutral write primitive for shared runtime paths; keep JSON serialization at the callers. Preserve private-file permissions and the bounded Windows `PermissionError` retry currently present in the accounts writer. Preserve large-file streaming, fsync, replacement failure cleanup, rollback behavior, and each caller's exact byte representation. Assess whether updater bootstrap independence warrants keeping its local primitive. This is a useful reduction in maintenance duplication, not a reason to combine all persistence logic into one abstraction.

Four timestamp wrappers also duplicate `monitor_common.now_iso`: accounts `timestamp`, cloud `_timestamp`, queue `timestamp`, and skills `_timestamp`. Consolidating those is safe but small.

**5. Remove write-only state, then reduce repeated UI work.**

- [AccountManager._config_snapshot](D:/Workspace/codex_monitor/monitor_accounts.py:247) is assigned at initialization and lines 654, 665, 669, and 679, with no readers. Fingerprints drive change detection. Remove the field, assignments, and reads used solely to populate it; retain the TOML reads needed for parsing.
- [CloudManager._observed_skill_revision](D:/Workspace/codex_monitor/monitor_cloud.py:441) has four assignments and no readers. [UsageDataStore._legacy_records](D:/Workspace/codex_monitor/monitor_usage_sync.py:465) is initialized and never used.
- [Dashboard state](D:/Workspace/codex_monitor/monitor_dashboard.py:890) stores unused `recovered_account_transition` and `projection_errors` results. Preserve `recover_account_transition()` and `skills.reconcile()` side effects. Consider surfacing reconciliation diagnostics instead of silently retaining an unread list.
- Browser [historyMetrics](D:/Workspace/codex_monitor/dashboard.html:187) only receives entries at line 671; nothing reads, displays, or exports it. Remove it unless an intentional diagnostic consumer is added. The [refreshSeries wrapper](D:/Workspace/codex_monitor/dashboard.html:691) only delegates to `load` and has one caller.
- The current uncommitted [costTimeUsageSeries](D:/Workspace/codex_monitor/dashboard.html:1206) filters/sorts an account's quota points separately for 5h and 7d. Group/sort once per account and share the ordered input.
- [costTimeUsageAt](D:/Workspace/codex_monitor/dashboard.html:1252) scans points and reconstructs compacted ranges during tooltip movement, called at line 1339. Prepare ranges once per curve and use binary search for the timestamp. Preserve interpolation, exact points, gaps, and account boundaries; add focused behavioral checks because this helper has no direct coverage.

An import-use scan also found unused imports: `OperationCancelled` in cloud; `empty_token_totals` in history; `os` in processing; `math`, `datetime`, and `timezone` in projection; `datetime` and `timezone` in quota; `tempfile` in skills. The daemon's star imports are intentional exports and were excluded from that diagnosis.

**6. Packaging, documentation, and test scaffolding need refinement.**

The existing `release/codex-usage-monitor-1.5.13.vsix` contains nine development test modules, `benchmark_processing.py`, the processing design/validation documents, and the usage contract document: **142,144 uncompressed bytes** for those inspected entries. [.vscodeignore](D:/Workspace/codex_monitor/.vscodeignore:1) excludes individual older tests but misses newer ones. Use a packaging allowlist or comprehensive development-file patterns and verify the archive contents. The extension uses bundled HTML as a fallback, so retain those assets.

[README.md](D:/Workspace/codex_monitor/README.md:46) and [README.zh-CN.md](D:/Workspace/codex_monitor/README.zh-CN.md:45) describe the replaced series architecture; both also show a 1.5.0 badge and installation commands for a 1.3.0 VSIX while package metadata is 1.5.13. Update the architecture and generate/version-neutralize artifact references.

Some tests lock implementation text instead of behavior. For example, [test_dashboard_exposes_account_filtered_usage_time_curves_with_nodes](D:/Workspace/codex_monitor/test_monitor_codex_usage.py:4671) contains 116 literal `assertIn`/`assertNotIn` checks. These assertions make safe renaming and consolidation expensive while providing limited evidence that a chart works. Move behavior checks into the existing Node VM/DOM fixtures, which already exercise loading, filters, chart coordinates, and tooltips. Retain useful security/contract checks; do not shrink the suite by indiscriminately deleting tests.

**Removal must reach installed runtimes too.** [AutoUpdater._install](D:/Workspace/codex_monitor/monitor_auto_update.py:696) replaces manifest-listed files but never deletes retired runtime files. An isolated mock installation confirmed that an old `retired_module.py` survives an update whose manifest omits it. This does not currently establish a functional failure, but future module removal needs explicit installation-file ownership/tombstones and rollback-safe deletion; never infer ownership of arbitrary files in the runtime directory. A new persisted ownership format would require a contract version/migration.

**What should remain.** Keep all ordered data migrations and their recovery/journal logic; generated `release/runtime` files currently back the updater's repository URLs; account-transfer verification and rollback; rotating-token retry restrictions; shared streaming/encryption code; canonical ledger/provenance and provisional legacy peer-cache import paths. Similar-looking token subtraction helpers are also not interchangeable as written: `_subtract_tokens` normalizes the result, whereas `token_totals_delta` returns independently clamped differences. A direct substitution can change totals when token categories shift.

The recommended implementation order is: remove confirmed orphan helpers/write-only fields and fix packaging; consolidate shared write mechanics; deliberately retire the series protocol including patch generation; resolve legacy cloud import policy and retire its disconnected graph. Re-run the existing relevant tests after each focused change. No persistent-contract changes are needed for purely unreachable-function removal or byte-preserving refactors.


## Implementation status

The safe first pass is implemented: shared durable byte-write mechanics for common/accounts/history/sync writers (existing writer entry points and serialization retained); unused private methods and definition-only helpers removed; write-only account/cloud/dashboard/browser state removed with side effects preserved; UTC helpers consolidated through compatible aliases; Cost vs Time account sorting, range selection, compacted-range preparation, and tooltip lookup optimized; test/development packaging exclusions generalized; README architecture and artifact versions refreshed. All test scripts remain. New tests cover write/replace failures, interruption cleanup, Windows retry behavior, serialized bytes, duplicate timestamps, and gap boundaries.

The public series protocol, legacy cloud graph, tested legacy history APIs, import facade, ordered migrations, and standalone updater writer remain intact. These are explicit compatibility boundaries rather than safe deletions in this pass. No persistent filenames, serialization, schemas, or field meanings changed, so no data-contract bump is required. Generated runtime files and manifest are synchronized without creating a release or changing the package version.

Final implementation validation: 569 Python tests run, one skipped; all 27 JavaScript tests passed; extension syntax and Git diff whitespace checks passed. VSCE file-list verification confirms test scripts, benchmarks, and design/audit documents are excluded from the extension package. Nested diagnostic logs are excluded as well. All test commands and their fixture processes have exited.
