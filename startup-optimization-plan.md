# Dashboard startup optimization plan

Status: implemented in the working tree, 2026-10-22. See [dashboard-delivery.md](dashboard-delivery.md) for the resulting behavior, measurements, and validation. Restart the monitor to load the updated runtime.

## Goal and measured baseline

Display current status immediately, load only the selected chart scope, and continuously prepare reusable history representations outside HTTP requests.

Measurements from the running monitor on 2026-10-22:

| Operation | Measured time |
| --- | ---: |
| Fetch dashboard HTML | 48 ms |
| Fetch current status | 4–86 ms |
| Full history request, before first response byte | 4.89–4.97 s |
| Transfer approximately 1.46 MB compressed history over loopback | 1–3 ms |
| V8 parse of approximately 15.5 MB JSON | 108 ms |
| Frontend snapshot cloning and validation | 122 ms |
| Current response preparation in an isolated reproduction | 3.59 s |
| Same rows and sanitization, complete-record encoding experiment | 1.07 s |

The full response contained approximately 30,238 records across Local and Merged. Its encoder produced approximately 1.71 million chunks and 1.03 million calls to json.dumps().

Frontend logic with DOM/canvas calls mocked took approximately 105 ms for today's view and 1.40 s for All. These exclude browser drawing, layout, and VS Code transport. The reported ten-second experience has not been fully traced end to end.

## 1. Order of work visible to the user

1. Render the page structure, restore the selected view/range/date, and enable navigation immediately. Show loading placeholders rather than zero values for unavailable data.
2. Request lightweight current status and the selected history scope independently. In VS Code, a timestamped cached status can be shown while revalidating it.
3. Render account, quota, reset times, and last-update information as soon as status arrives. Password setup must not block rendering available read-only information.
4. Render the selected scope's exact token/cost summaries and filter controls as soon as their response arrives.
5. Draw visible charts first, with a browser paint opportunity between chart tasks. Defer charts outside the viewport until needed.
6. Fetch history updates from the delivered snapshot cursor. Prefetch only small likely-next scopes when idle; do not download all detailed history automatically.

If the restored selection is Date, request that day directly. Do not make that day wait for an All overview. If the selection is 5h, 24h, 7d, 30d, or All, request the prepared overview for the selected Local/Merged view.

Status and history have separate loading and error states. A history failure must not replace valid current status. An older history response must not overwrite newer status.

When switching scopes, update the selection immediately and show the requested scope's loading state. Use an in-memory cached result when available, and discard responses for selections that are no longer active.

## 2. Data processing pipeline

```text
Canonical quota readings and token ledger
  -> Existing incremental ingestion and exact calculation in SQLite
  -> Changed accounts, records, intervals, and sessions
  -> Exact detail projection + exact summary indexes
  -> Incremental multiresolution chart buckets and gap/flat metadata
  -> Prepared overview chunks + small revision manifest
  -> HTTP delivery of completed artifacts
  -> Browser scope filtering and final chart layout
```

### Exact calculation remains authoritative

- Retain canonical histories and existing identity, provenance, deduplication, quota continuity, plan normalization, and cost attribution rules.
- Keep exact calculated points independently of the reduced overview. Selected-day detail must not be queried from age-compacted overview rows.
- Compute continuity and cost intervals with the required neighboring history before applying a display-range restriction. Do not run the full historical reference calculation on every day request.
- Preserve existing range semantics in this optimization: preset ranges currently end at the relevant dataset's latest timestamp; Date uses browser-local midnight boundaries.
- Preserve current session attribution: token summaries currently select sessions by updatedAt, falling back to startedAt. Do not silently change this to ledger-event-day accounting.

### Incrementally maintained overview

- Maintain fixed time buckets keyed by view, account, collection/window, and resolution; retain model breakdowns where model filtering affects the result.
- Update only buckets touched by changed records. Maintain a small hierarchy of resolutions so broad ranges can choose a bounded representation without scanning raw history.
- Initially retain full source detail for the most recent 24 hours; use roughly 5-minute buckets through 7 days, 30-minute buckets through 30 days, and 2-hour or coarser buckets for older history.
- Treat these resolutions as initial tuning values. Select a coarser prepared level when needed to target approximately 2,000 ordinary usage-curve points per account/window. Mandatory discontinuity points may exceed the target.
- Within a bucket retain first/last points, extrema, and explicit boundaries for resets, gaps, discontinuities, and account/plan changes. Never merge unrelated continuity segments.
- Cost/usage overview aggregation sums cost and normalized usage deltas and derives the ratio from those sums. Never average ratios directly. Keep per-model costs and the represented time interval.
- Mark aggregated points as such in tooltips. Selected-day detail exposes the original calculated points rather than an aggregate presented as a real observation.
- Maintain gap and flat intervals alongside points. Preserve the existing logarithmic gap/flat-fold behavior; screen coordinates and final fold widths remain browser calculations.
- Reconcile bucket boundary context when points arrive late or change. A correction may affect neighboring buckets or an account's continuity segment, not just the timestamp's bucket.

Fixed buckets avoid regrouping all history whenever a new record arrives. Advancing time selects different prepared levels; a lightweight scheduled worker updates the overview manifest even when no new records arrive.

### Exact summaries are separate from plotted points

- Maintain exact token, cost, model, and session summary indexes using the existing session/range attribution rules.
- Use interval or prefix indexes for precise range boundaries. Whole-bucket sums alone are insufficient for arbitrary 5h/24h boundaries.
- Replacing or moving a session removes its previous contribution and adds the corrected contribution, including old and new affected dates.
- Never derive totals, session counts, or rates from downsampled plotted points. Derive rates from exact numerator/denominator aggregates.
- Return compact metadata needed for filter controls and available dates instead of all token-session objects. Retain existing filter membership behavior for each selected scope.

## 3. Preparation and delivery

### Prepared overview chunks

- Encode complete sanitized records with the standard JSON encoder and combine output into approximately 64 KiB writes. Remove per-value gzip writes.
- Prepare immutable overview chunks in the background, keyed by view, collection, time partition, and resolution. Unchanged chunks retain their content hashes and compressed bytes.
- A small manifest lists the chunks for each overview generation, its exact summaries/metadata, coverage, and committed cursor.
- Publish new chunks first, then atomically switch the manifest to the completed generation. Retain referenced previous chunks until active downloads can finish; garbage-collect unused artifacts afterward.
- HTTP requests serve completed bytes with ETags. They do not ingest logs, replay account history, sanitize entire histories, or compress entire snapshots.
- Prepare Local and Merged independently. Load the inactive view only when selected or through bounded idle prefetch.

A single gzip document is not edited in place. Incremental maintenance operates on exact projections, buckets, and independent compressed chunks. Small manifests can be regenerated cheaply.

### Selected-day detail

- The client sends explicit UTC start/end instants calculated from the selected local calendar date. This handles timezone differences and 23/25-hour daylight-saving days.
- Query indexed exact projections for the requested scope, plus boundary context required by the existing chart and cost semantics. Exclude context-only rows from totals and counts.
- Read one consistent database generation and sanitize/encode by complete record. Fetching an uncached day can assemble its bounded response on demand; full-history recomputation is prohibited.
- Cache the prepared result by view, UTC bounds, relevant partition revisions, and calculation/schema version. Bound the cache by bytes and evict least-recently-used entries.
- Late corrections, session changes, account metadata changes, and changed neighboring intervals invalidate the corresponding dependencies. Unaffected historical days remain reusable.
- If unusually dense day detail is too large, deliver ordered bounded pages from one immutable snapshot generation. Do not silently reduce its precision.

### Snapshot/update consistency

- Each scope identifies its view, bounds, representation, generation, and included update cursor.
- Build the data and cursor from the same committed projection transaction. Independently serving chunks must not mix generations.
- Apply updates strictly after the snapshot's included cursor. Replace changed aggregate buckets; do not append a new copy of the same bucket.
- Scope update streams to the loaded representation. An unloaded view or day does not require a full replacement payload on every poll.
- On restart, expired cursor, missing chunk, or schema mismatch, replace only the active scope from a completed compatible generation.
- Continue serving a valid previous generation while its replacement builds, with an explicit data timestamp/refresh state. Coalesce repeated changes so preparation does not restart for each incoming record.
- If preparation fails, retain the previous completed generation, report the failure, and retry. Never publish incomplete artifacts.

## 4. Background execution and startup

- Keep migrations, recovery, and validation required for safe data access before opening affected stores.
- Remove refresh_dashboard_cache() and ingestion from the history request path. Use the existing maintenance notifications to schedule the projection/preparation worker.
- Coordinate one projection writer, bounded dirty scopes, and one coalescing artifact-preparation queue. Prioritize the active recent scope over old-history maintenance.
- Keep database write transactions short and perform response encoding/compression outside the writer lock using a consistent read snapshot.
- On a warm restart, validate and reuse completed artifacts. On first install or invalid cache, show current status and a preparing-history state while background reconstruction proceeds.
- Make derived-cache rebuilds retryable. Preserve valid exact projections and canonical histories if a rebuild fails.

## 5. Frontend and VS Code changes

- Replace the global all-history payload with per-scope state and stable keyed collections. Cache only loaded scopes within a memory budget.
- Avoid JSON stringify/parse cloning of every view for a snapshot or small update. Validate at the response boundary and copy only changed collections.
- Cache parsed timestamps, account colors, model metadata, sorted collections, and fold inputs by data revision. Recompute viewport-dependent geometry only when its dependencies change.
- Coalesce updates and resize events. Redraw only affected charts, and cancel obsolete chart work when the user changes scope.
- Reuse retained VS Code webview HTML when reopening the same panel. Preserve selections and refresh through the lightweight status/scope protocol.
- Pass scope and request identifiers through the extension bridge. Deduplicate only requests with identical scopes; unrelated requests must not share one pending promise.
- Management follows the same ordering: cached configuration/accounts first, scan results later. Run skill scans in the background and reuse their results rather than synchronously scanning every five seconds.
- Preserve exactly one start/queued and one terminal notification for each logical WebDAV operation, including cancellation, skipping, partial completion, and failures.

## 6. Implementation sequence

| Phase | Changes | Completion evidence |
| --- | --- | --- |
| 1. Instrument and unblock display | Stage timings, independent status rendering, scope-aware request sequencing, retained-panel reuse | Status appears while history is deliberately delayed; no stale status overwrite |
| 2. Remove measured encoder bottleneck | Complete-record JSON encoding and buffered writes in the existing protocol | Equivalent sanitized payloads; far fewer writes; repeated measurements against the recorded baseline |
| 3. Establish exact detail and summaries | Separate exact display projection from reduced overview, scoped queries, summary indexes, migration | Day/range results match reference behavior, including boundaries and session corrections |
| 4. Maintain overview incrementally | Multiresolution buckets, boundary metadata, dirty-scope propagation, ageing selection | Append/correction work is bounded to its dependencies; overview preserves required features |
| 5. Publish reusable artifacts | Immutable compressed chunks, atomic manifests, scope cursors, conditional delivery | Warm page opening performs no ingestion, projection rebuild, or history serialization |
| 6. Finish progressive loading and rendering | Selected scope only, on-demand day detail, bounded client cache, chart scheduling, management scans | Browser and VS Code timings meet targets; scope changes remain responsive |

Phases 1–2 deliver immediate measurable improvements. Phases 3–5 remove repeated preparation from page opening. Phase 6 completes the user-visible loading and interaction behavior.

## 7. Persistent contract and compatibility

The new exact projection, overview buckets, artifact metadata, and cursor meanings change persistent contracts. For one combined release, increment package.json dataContractVersion and DATA_CONTRACT_VERSION from 8 to 9 and add migration 9 to DATA_MIGRATIONS.

Propagate the version into packaged version.json. Run every pending migration consecutively before affected data is read or written. If changes ship in separate releases, give each persistent contract change its own next migration number.

Migration 9 must preserve canonical histories and durable synchronization/account metadata. Back up SQLite with WAL-aware backup, introduce the new representation, and rebuild equivalent derived data through a defined automatic path.

On failure, preserve or restore the original data. Retrying migration or derived reconstruction must be safe. Do not retain only old compacted points when exact source history is required to recover day detail.

Version the new HTTP/scope protocol explicitly and update the browser assets and extension bridge together. Handle incompatible clients explicitly rather than applying mismatched cursors or snapshot shapes.

## 8. Validation and acceptance targets

Targets below are goals for warm localhost operation on the measured dataset, not performance already achieved:

- Current status visibly rendered within 300 ms; visible selected charts within 1 second where the prepared scope fits its ordinary point budget.
- Cached overview manifest/chunk endpoints start responding within 100 ms, with no historical database scans, serialization, or compression on the request path.
- Ordinary uncached selected-day data available within 500 ms on the current dataset; cached day responses substantially faster. Report dense-day exceptions separately.
- Changing overview presets reuses loaded overview data; selecting a day does not trigger a full-history download.
- Warm restart reuses prepared generations. Old-history growth must not increase ordinary recent-scope update work proportionally.

Measure cold bootstrap, warm startup, first and repeated openings, Date/5h/All, Local/Merged switching, and browser versus VS Code separately. Record p50/p95, first byte, response bytes, parse/apply time, first chart paint, CPU time, and memory.

Correctness coverage must include late/corrected records, resets, gaps, plan changes, model filters, exact totals, moved sessions, timezone/DST boundaries, concurrent updates during download, cursor expiry, restart, interrupted publication, and migration rollback with WAL.

Use reference-algorithm comparisons for exact detail and summaries, and explicit feature-preservation assertions for coarse overviews. A coarse overview is intentionally not point-for-point identical to full detail.

Stop every test instance and watcher after verification, including instances used for live traffic or integration testing.
