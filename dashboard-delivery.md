# Dashboard bulk and live delivery

The same backend interpretation layer directly consumes either permanent canonical dataset. Both datasets have identical schemas, fields, units and storage helpers; only selection and stream routing differ. Local contract 23 is pending against released local 22/cloud 5 (v1.5.31); cloud contract 5 is unchanged. Typed canonical quota/token rows are mapped directly into the same preparation implementation for either dataset. The representation migration preserves existing prepared tables, snapshots, journals and stream sequences without requesting a rebuild. HTTP has no explicit protocol version.

## Pipeline and preparation

Preprocessed API/session records fork to the durable local upload buffer and in-memory local processing. Local canonical changes also feed Merged. Unseen immutable cloud packages are downloaded/interpreted in memory and merged into Merged; they never enter Local or a persistent raw-cloud cache. Both permanent datasets directly feed the same `DashboardDelivery._account` preparation implementation. The WebUI retains its Local/Merged switch and both independent live streams.

Quota canonical records store accepted points or compatible constant ranges with integer Unix-second bounds, multiplier, plan and reset metadata. A late conflicting point at t splits an existing range at t-1 and t+1. Affected intervals and cost memberships are prepared again without repricing token events or retaining raw remote history. Quota duplicates are ignored by point/range handling without a persistent input-ID registry; cost/token events retain identity deduplication. Imported package receipts are separate from canonical records.

The typed delivery runs, token payload/reference tables and charts retain account/window/time data and interval attribution. `delivery_work` tracks changed canonical identities for either view. Prepared state and journals commit together; serialization and compression happen outside the ingestion writer lock. Runtime canonical JSONL backups are retired; optional diagnostics contain only raw quota API responses.

Bulk preparation is coalesced to at most once per second. HTTP requests read already prepared bytes; they do not query raw history, rebuild charts, or compress the bulk. A connection may download an earlier complete snapshot and catch up from its cursors.

## Bulk shape

GET /api/dashboard returns one account-based JSON document containing both Local and Merged, all retained time ranges, shared model names, account/slot metadata, current state, a dataset epoch, and independent local/merged/state cursors. The entire document is serialized and then gzip-compressed at level 3. Compression is the last preparation step. There is no fixed payload-size cutoff.

The root is {epoch,cursors,models,state,accounts}. Each account is {id,label,local,merged}. Each view contains quota, tokens, legacy, usage:5h, usage:7d, chart:5h, and chart:7d. Account identity occurs once per view container rather than once per event. Current slot labels and bindings are shared in state.accounts; historical labels remain with their accounts.

Rows are positional:

| Collection | Fields |
| --- | --- |
| usage:5h / usage:7d | [id, first, last, percentage-or-null, multiplier, flags] |
| tokens | [id, weightedTimestamp, models] |
| token model entry | [modelIndex, tier, freshInput, cachedInput, output, cacheWrite, costUnits] |
| chart:5h / chart:7d | [id, start, end, weightedTimestamp, multiplier, normalizedUsageIncrease, models] |
| chart model entry | [modelIndex, costUnits, attributedNormalizedUsage] |
| legacy | [startedTimestamp, updatedTimestamp, tokenModelEntries] |

All timestamps are Unix seconds. Weighted timestamps are integers. Monetary units are 1e-8 USD; counters are integers. Usage flag bit 0 means break before this run. A run with different first/last timestamps explicitly connects its endpoints. The current quota object maps 5h/7d to reset timestamps or null; current percentages and last-update time derive from the newest accepted usage endpoints. Unknown quota values remain null, never zero.

Model names in bulk rows index the root models dictionary. Incremental rows use model strings directly because dictionaries can change as new models arrive. No subscription/plan string is transmitted. Service tiers remain distinct in token rows so stored pricing attribution is retained.

## Usage and cost interpretation

Historical Pro multipliers are 20 before 2026-10-29T00:00:00Z and 10 afterward; Plus is 1 and Pro Lite is 5. Migration rebuilds processed historical multipliers from observation time. Canonical cloud records retain their existing format.

Only consecutive accepted observations with identical processed percentage, multiplier, and reset timestamp collapse into a run. Rejections and window-specific reset boundaries remain explicit. Runs connect internally, even when long. Between their last/first endpoints, gaps over four hours break, gaps at most 30 minutes connect, and intermediate gaps break for a normalized drop of at least 3 points or an absolute rate of at least 0.5 points/minute (5h) or 0.1 (7d). The backend calculates flags independently of the client normalization toggle.

A cost interval needs connected endpoints, increasing time, valid/increasing usage, and an unchanged effective plan/multiplier. There is no additional reset-within-60-seconds eligibility test. Token groups split at the union of 5h/7d interval boundaries and nearest-interval assignments. Per-model/tier tokens and costs sum exactly at the existing eight-decimal monetary precision. Their timestamp is the nearest-integer cost-weighted original timestamp; zero-cost groups use the arithmetic mean. API bulk history uses fixed UTC minute groups. An OpenAI account with no valid intervals keeps independent events.

Each event belongs to the containing interval, otherwise the closest interval by distance to its bounds only when that distance is at most 600 seconds, independently for each window; ties choose the earlier interval. Costs farther away remain unassigned and are retained in token totals and Cost vs Time. Pending/unattributable costs stay visible. New token events arrive immediately; later quota can reassign chart attribution without recounting tokens. Historical session-only totals remain in legacy and do not acquire fabricated cost-event timestamps.

## Live channels and recovery

/api/dashboard/live upgrades to WebSocket on the same port. Clients send {type:subscribe,epoch,cursors}; cursors select local, merged, and/or state. Packets are {epoch,stream,seq,channel,account,data}.

- state: current shared account/control/availability state.
- account: a newly discovered account label; initializes empty view containers before its first data packets.
- quota:5h / quota:7d: [resetTimestamp] when it changes.
- usage:5h / usage:7d: {set:[usageRows],remove:[ids]} for new or extended runs.
- tokens: new token rows, each counted once.
- chart:5h / chart:7d: {set:[absoluteChartRows],remove:[ids]} for affected attribution.

The snapshot cursor excludes future additions even if their event timestamp is older. Duplicates at/below an applied cursor are ignored. The durable journal retains at most 24 hours and 64 MiB. Missing coverage or a changed epoch produces {type:refresh}; the client obtains another bulk. WebSocket messages are not compressed. Ping/pong detects dead connections; reconnect delay backs off from 1 to 30 seconds. Individual outbound backlogs are bounded, and slow clients reconnect and resume.

A completed cloud operation that changes effective usage data replaces the complete combined bulk and advances the epoch. Unchanged Fetch operations do not resend history. Clients keep the previous complete dataset until the replacement is ready and discard obsolete generations atomically. Source-history correction is not a supported incremental operation; cloud refreshes may reconstruct derived history.

## Frontend and extension

Dates, model/account filters, presets, and Local/Merged switching require no history requests. Local-calendar date bounds retain daylight-saving behavior. Normalization consumes the transmitted multiplier, never a hardcoded plan table. Quota time/percentage curves, folding, account colors, reset indicators, and animations remain renderer responsibilities.

Cost vs Time chooses whole-minute/hour/day bars from display width rather than data age. Hover shows model: $xx (Cum. $yy), summed across selected accounts: xx is the hovered bar’s cost, yy is the total since chart start through the pointer timestamp. Prefix indexes support pointer lookup. Token totals and chart placement use transmitted aggregate timestamps; interval merging intentionally changes fine time-boundary attribution. Session counts are removed.

The extension shares one live connection. With its dashboard closed it subscribes only to state, avoiding a history download. Its host relays updates to the webview and retains existing authenticated management bridges. Browser HTTP and WebSocket addresses follow the configured origin, including wss behind HTTPS proxies.

## Validation and measurement

Run the Python suite, node test_dashboard_loading.js, and npm run check. The old manifest/chunk tests are replaced by bulk/replay tests; released migration recovery tests remain.

Run python benchmark_dashboard.py --source-db SOURCE.sqlite3 --report dashboard-transmission-benchmark.json to read a consistent source snapshot, prepare an isolated database, and measure actual HTTP/WebSocket transfers. It reports raw/gzip bytes, keyed-JSON comparison, preparation and warm response timing, real client decoding, idle bytes, and incremental channel sizes. It does not alter source data or impose a fixed size target, and closes the fixture server, connections, and temporary files afterward.
