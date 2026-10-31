# SQLite redundancy audit — 2026-10-30

Inspected all 68 application tables in the installed database and traced current runtime writers/readers, including sampling, ingestion, canonical ownership, synchronization, projections and delivery. Measurements come from read-only snapshots of the installed database, which still has the released layout; current source contains pending contract-20 changes. No service was started, migration run, or application data modified. Aggregate query results and the table inventory are in [sqlite-redundancy-audit.json](D:/Workspace/codex_monitor/sqlite-redundancy-audit.json).

The earlier claim that a separate quota-reading pool saves redundant data was insufficient: sharing was possible in theory, but none occurs under its proposed identity in the measured history. Several other relationships also store fixed fields or full copies rather than reusable children.

## Confirmed priorities

| Priority | Area | Evidence | Appropriate direction |
| --- | --- | --- | --- |
| High | `quota_windows` as an independent observation pool | 42,957 5h readings and 64,362 7d readings; every reading is unique by account, origin, window, timestamp and exact value/type metadata. Each would have one parent. | Make each window observation a main record with its percentage/time inline. Preserve 5h/7d independence. If transport envelopes still group observations, preserve their ownership and reconstruct that grouping separately. |
| High | Repeated reset/plan metadata inside quota readings | Those 107,319 readings contain 14,097 distinct 5h and 873 distinct 7d combinations of account, origin, window, reset and plan. A 7d combination appears in up to 3,769 readings. | If a child table is retained to save repeated data, use it for this stable metadata, with many observations referencing it. This is separate from observation identity; never merge observation times or percentages. |
| High | `delivery_members` | 165,896 rows for 82,948 tokens. Every participating token has exactly two rows, one per fixed window. No membership refers to a missing token. | Store nullable 5h/7d interval references on `delivery_tokens`, with indexes for interval-to-token lookup. One interval can still contain many tokens; each token has only one assigned interval per window. |
| High | Three stored representations of quota decisions | All 102,945 `processing_quota_points` payloads exactly equal their `processing_sampled_quota` counterparts. All 102,945 `processing_display` quota-point payloads match an existing quota point after removing `_key`. | Keep one canonical processed-point payload and references or computed presentation fields. Indexed account/time/key columns can remain without copying the JSON. |
| High | `processing_effective.data` | All 42,032 current usage-event payloads exactly match an existing fact payload. This repeats 38,170,948 JSON characters. Another 999 rows are legacy baselines. | Store the selected sample/fact identity and its attribution, rather than copying the winner. Preserve deterministic conflict selection and update references transactionally. Contract 20 already handles retiring baseline records. |
| Medium | `processing_peer_rows` | All 26,670 measured rows repeat a fact's source/key/position and a remote record's hash. | Attach the applied-checkpoint fields to the peer fact/import relationship. Keep an explicit marker for skipped or unmapped remote records, which legitimately may have no fact. |
| Medium | Metadata repeated inside `sample_records` and its owners | All 107,394 facts repeat sample kind, subtype, event identity, event timestamp and account. Root sample account and origin also repeat sync metadata in all 107,394 samples. | Choose one stored source for invariant fields and expose aliases/generated fields through views. Keep source position, ownership, and any genuine account-mapping override distinct. Do not infer universal equality solely from this database. |
| Medium | Local/Merged payload copies | All 41,578 Local delivery-token rows have an identical Merged counterpart. Of 38,319 matching sampled-quota identities, 38,287 are identical; 33,497 of 33,744 matching usage runs are identical. | Share immutable payloads with view-specific membership/overrides. Local and Merged must remain independent where peer data or sampling decisions differ. |

Quota-point duplication alone stores 44,338,478 JSON characters in each of two tables, plus 48,044,498 characters in presentation rows. These are payload counts, not SQLite allocation measurements or guaranteed disk savings.

## Additional findings

**The typed reset column is unused because its type is wrong for current data.** `FIELDS["quota_windows"]` declares `resetAt` as `TEXT`, while current timestamps are numeric. The codec treats numeric values as unexpected for text and puts them in `extensions`. All 107,319 rows have a null typed reset column and a numeric `extensions.resetAt`; the extension strings total 2,361,018 characters and contain no other fields in this database. A numeric reset column would remove this JSON fallback. Exact representation compatibility for historical or unusual values must still be handled by migration/codecs. See [field definitions and codec](D:/Workspace/codex_monitor/monitor_sample_store.py:25).

**Legacy `baseline_totals` is a fixed-container split.** Its 4,026 rows are exactly 2,013 `tokens` rows plus 2,013 `cost` rows. These categories are fields of a session/model node, not independently shared objects. If baseline storage were retained, their counters belong inline on that node. Pending contract 20 retires these records, so creating another replacement baseline schema would be unnecessary work.

**`baseline_models` cannot be called universally one-to-one.** Of 999 parent samples, 984 have one model, but 15 genuinely have two. The measured majority does not justify flattening an arbitrary model dimension into one fixed field. The table is legacy storage slated for retirement.

**Raw-event winner storage duplicates candidate payloads.** All 3,779 currently retained occurrence payloads equal their `processing_scan_events` winner payload. However, `_winner()` intentionally supports different candidate versions from different sources. Interning candidate payloads and referencing the winning payload is safer than assuming sources can never disagree. Replacement staging must remain transactional. See [winner selection](D:/Workspace/codex_monitor/monitor_ingestion.py:156).

**There is a retention candidate requiring a separate correctness check.** `processing_scan_events` contains 65,476 rows with no occurrence and no valid references, totaling 26,217,714 JSON characters. They are not automatically disposable: pending migration 20 can use retained source events to recover history. Define their recovery/retention ownership before deleting anything.

**Common extension defaults are repeated.** All 80,724 local owner rows have `record_meta='{}'` and `attribution='{}'`. All 26,670 remote rows have `record_meta='{}'`. Sparse/default encoding could reduce overhead, but nonempty attribution and unknown extensions still need preservation. The benefit is smaller than eliminating full payload copies.

**Some prepared token data is stored in overlapping forms.** `processing_display` has 83,610 `costTime` rows with 24,629,728 JSON characters, while `delivery_tokens` has 83,610 typed event rows. These are not byte-identical payloads: the display rows include presentation/account metadata and potentially different model normalization. Review whether both persisted representations are needed, while keeping prepared HTTP delivery and stored pricing semantics intact.

## Structures that should not be collapsed blindly

- **Canonical ownership is real sharing.** Every one of the 107,394 samples has two owner references: a processing fact and either a local or remote synchronization owner. Keeping independent owners lets one owner disappear without deleting another's history.
- **Identifier dictionaries provide real reuse.** Accounts, devices, models, plans, tiers, sessions and source names are referenced across many events. Their value dictionaries are not one-to-one child tables.
- **Sampled decisions and raw quota observations have different meanings.** Raw readings preserve evidence; sampled decisions preserve acceptance, rejection and reset semantics. The avoidable copy is between duplicate processed-point representations.
- **Delivery runs/charts are transformations.** Runs compress repeated observations; charts attribute costs to quota intervals. Their existence alone is not redundant, although their Local/Merged payloads and repeated keys can be shared.
- **Journals, snapshots and queues have distinct lifecycle requirements.** Replay packets, a prepared serialized/gzip snapshot and pending backup work serve different consumers. Byte overlap alone does not make their storage unnecessary. Snapshot compression remains useful for serving prepared HTTP responses.
- **Temporary change queues are not history.** All 118,871 measured `processing_changes` rows belong to dirty accounts and are consumed/deleted by projection refresh. The snapshot does not establish a leak.
- **Empty compatibility tables are low priority.** Several legacy dashboard/cache tables are empty. Their definitions add complexity but do not explain the large payload footprint.

The file is about 1.39 GiB, including approximately 501.9 MiB of free pages. Free space is already reusable by SQLite and must not be counted as redundant live data. Per-table allocated-byte accounting was unavailable because this Python SQLite build lacks `dbstat`.

## Suggested implementation order

1. Correct quota storage ownership: independent main observations, inline observation fields, and optionally shared stable reset/plan metadata; correct numeric reset storage at the same time.
2. Inline the two delivery interval references and remove the membership table after transactional conversion and index replacement.
3. Replace duplicate processed quota payloads and selected-event payloads with references/views.
4. Consolidate peer checkpoints and invariant identity metadata, retaining mapping/skip exceptions.
5. Share identical Local/Merged payloads where worthwhile, then review sparse defaults and legacy cleanup.

These are audit findings and proposed changes, not changes applied to application storage. Any implementation must merge its persistent conversion into pending local contract 20, preserve released migrations, and retain ownership/recovery behavior on failure.
