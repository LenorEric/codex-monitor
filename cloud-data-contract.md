# Global cloud data contract

Each WebDAV sync root has one unencrypted `data-contract.json` containing `{"dataContractVersion":5}`. This is the authoritative global cloud contract. The app target is `CLOUD_DATA_CONTRACT_VERSION` in `monitor_migrations.py`, also declared as `cloudDataContractVersion` in `package.json` and packaged `version.json`. Object-level format identifiers remain for decoding and validation; they do not independently authorize access or trigger global upgrades.

Every logical cloud operation checks the marker before accessing payloads. It then claims `sync-lock.json` as described below, rechecks the marker, and retains ownership until the entire operation, including nested account, skills, and usage requests, finishes. Busy manual operations report the conflict immediately; automatic fetch and usage sync retry after 30 seconds instead of waiting their normal interval. All coordination and migration requests stay inside the existing single queued/start and outcome notification pair.

- Equal version: permit normal reads and writes under file ownership.
- Lower version, including an absent marker treated as zero: run every pending `CLOUD_DATA_MIGRATIONS` step consecutively, commit each version with conditional PUT, and verify it before proceeding.
- Higher version: block payload reads, writes, destructive overwrite, and passphrase rotation; report that the local script needs an update. Reading the marker itself is necessary to make this decision.
- Invalid metadata, a missing migration, invalid coordination status, or unsupported required locking/conditional updates: stop before payload access. `allowOptimisticWrites` cannot bypass coordination.

Released cloud contracts 1–4 remain available for ordered upgrades. Contract 4 converted quota reset timestamps to Unix seconds. Pending cloud contract 5 removes legacy session baselines, source coverage metadata and dependent token records that cannot be converted to complete current events from cloud data. The runtime accepts only current `usage` events. Conversion checks dependent sessions across all day partitions, publishes replacement immutable payloads and conditionally updates each head under `migrations/contract-5` rollback protection. The contract marker commits last. Failures restore original heads and allow retry; successful commit removes obsolete payloads and rollback roots. Local contract 20 can rebuild affected events from retained SQLite source events; it discards records that cannot be rebuilt without guessing identities or account ownership.

## File coordination

The shared `CloudAccessLock` class owns one lease per logical cloud operation. The idle file remains exactly `{"writing":0}`. An owned file contains `{"writing":0,"owner":"<operation UUID>","expiresAt":<Unix seconds>}`. Acquisition checks the record, takes a short native file `LOCK` with `Depth: 0` and requested `Timeout: Second-60`, rereads the record and strong ETag, and conditionally claims it using `If-Match`. The native lock is released immediately after the metadata update. Strong ETag conditions remain mandatory because Jianguoyun can grant independent native tokens to clients using the same account.

An owner sets `expiresAt` to current Unix time plus 300 seconds. One background worker renews the lease when 60 seconds remain, including during transfers and local processing. Renewal and release each take a short native lock, reread and validate ownership, conditionally update the record, and unlock. Local monotonic deadlines prevent a backward wall-clock adjustment from prolonging ownership; participating machines must have synchronized clocks. Renewal failure, ownership loss, or expiry stops further access; the same lease instance never silently reacquires ownership.

Immediately before the first payload mutation, the lease conditionally sets `writing` to 1 while retaining its owner and expiry. Nested operations and all skills in a batch reuse this lease. Completion stops and joins the renewal worker, validates the still-live owner, and conditionally restores `{"writing":0}`. A failed or expired owner leaves its record for recovery rather than clearing a successor. Cleanup failures are reported even when payload changes have already completed.

Every protected payload request carries a tagged WebDAV `If` condition matching the current strong ETag of `sync-lock.json`, alongside any payload-specific conditional headers. This fences resumed former owners after takeover without holding a native lock throughout the operation. Only a definitive HTTP 412 associated with the current owner's concurrent renewal may retry once after checking current ownership; ambiguous transport failures never blindly retry mutations. On first access per endpoint/root/credential identity in a process, isolated `.lock-probe-<UUID>` objects verify both cross-resource tagged ETag enforcement and `If-Match`; unsupported providers are rejected before ordinary payload access. These temporary probes are removed and successful verification is cached in memory.

Finite native metadata locks are preferred, but an infinite grant is accepted. Native locks are held only while updating metadata, reducing the crash window; a crash within that window can still leave an infinite native lock requiring manual provider recovery. Requests for finite timeouts cannot force a provider to grant them.

On first use only, a missing file is provisioned through an atomic `MKCOL .sync-lock-init` gate, rechecking absence after acquiring that gate. This avoids depending on `If-None-Match: *`, which Jianguoyun may ignore on PUT. The gate is removed after provisioning. It is a one-time initialization guard, not a recursive directory LOCK or a normal-operation lock. Malformed lock files are never silently overwritten. Owned records without an expiry remain eternal; a valid expired lease, including `writing:1`, may be replaced by a conditional claim.

Cloud overwrite preserves `data-contract.json`, `sync-lock.json`, and `.sync-lock-init`. Passphrase rotation leaves these unencrypted coordination files unchanged. The lock file contains no credentials or payload content.

## Interruption and recovery

A crashed operation's valid expiry lease can be reclaimed at or after `expiresAt`. Contenders reread under a short native lock and use strong ETag conditional claims, so only one succeeds even when native locks are reentrant. A resumed former owner cannot renew, mutate payloads, or clear the replacement owner. Takeover does not roll back interrupted multi-object writes; the existing account and migration recovery checks still apply.

Legacy owner records without `expiresAt` remain eternal, malformed records fail closed, and stranded initialization gates or infinite native locks still require manual recovery. Stop participating clients, preserve originals, inspect retained sources and interrupted account/passphrase operations, restore consistency, and release provider locks before clearing legacy status. Never clear a live owner's record.

This additive lease format requires no data-contract bump or saved-data conversion: existing idle and legacy owner records remain valid, old clients already reject owned records, and release restores the original idle representation. Previously abandoned legacy owners are not automatically assigned an expiry.

The protocol prevents overlapping operations among updated, cooperating clients on a service that enforces atomic MKCOL and strong ETag `If-Match`. It does not prevent manual edits or software bypassing the coordination file. Upgrade all participating clients before using the new protocol. Released cloud-contract-1 clients reject marker 2; during transition, a conforming server's old recursive root lock conflicts with the new file lock. Pre-contract clients cannot enforce the version gate and must be stopped or upgraded.

Future migrations must preserve originals, be safe to retry, and use verified copy-on-write publication or a durable backup/recovery path for payload changes. The marker commits only after a step succeeds. Never remove released migration steps.

Pending cloud contract 5 also removes retired quota `compaction` metadata while preserving observation timestamps and window readings. It updates record content hashes and partition logical hashes within the same verified, retryable head migration.
