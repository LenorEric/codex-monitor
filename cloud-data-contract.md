# Global cloud data contract

Each WebDAV sync root has one unencrypted `data-contract.json` containing `{"dataContractVersion":5}`. This is the authoritative global cloud contract. The app target is `CLOUD_DATA_CONTRACT_VERSION` in `monitor_cloud_contract.py`, also declared as `cloudDataContractVersion` in `package.json` and packaged `version.json`. Object-level format identifiers remain for decoding and validation; they do not independently authorize access or trigger global upgrades.

Every logical cloud operation reads the marker before accessing payloads, claims `sync-lock.json`, and checks the marker again under that lease. Current cloud data is already converted; the script contains no cloud migration registry, old-format readers or conversion routines.

- Contract 5 permits normal reads and writes.
- Older or newer contract markers block access without rewriting the marker or payloads. Newer contracts require an updated script; older data must already have been converted outside this runtime.
- A missing marker is initialized to 5 only for an empty root containing at most synchronization lock metadata. Existing unmarked data is left unchanged and access is blocked.
- Invalid metadata blocks access. Contract checks and nested requests remain inside the single queued/start and completion notification pair for each logical operation.

Local data contract 22 and its sequential migrations remain supported independently. Cloud synchronization does not replay, convert or delete old remote layouts.

## Current usage packages

`usage/v5/data/<machine>/<id>.enc` stores compressed encrypted records. The content-addressed descriptor at `usage/v5/packages/<machine>/<id>.enc` is published only after payload success, with identity, count and actual time bounds. Count uncompressed canonical UTF-8 JSON bytes, sealing after a complete record crosses 75,000,000 bytes. Packages have no day/month ownership links.

One durable logical upload buffer becomes due 30 minutes after its oldest remaining arrival. Manual uploads flush immediately, startup schedules overdue data, and successful publication clears only the snapshot prefix. Concurrent arrivals remain buffered. Confirmed failures retain the growing buffer; retry begins after 30 seconds and backs off to 15 minutes. Minimal attempt metadata resolves uncertain publication, including remote success before local acknowledgement, before replacement or clearing.

Download discovery lists descriptors and fetches only unseen IDs for the current destination. Decoding stays in memory; canonical import and receipt commit together. Missing remote packages never remove canonical history. Downloads keep an independent 30-minute cadence, including after upload failures.

## Account files

Each account occupies one encrypted `accounts/<account-key>.enc` file, authenticated with purpose `account:<account-key>`. It contains the account metadata and base64-encoded credential bytes together, with their byte length and SHA-256 checksum. The checksum verifies credentials; it is not a revision pointer. Account lists, caches and management responses exclude credential bytes.

Release/Share publishes one file. Bind/Link reads and validates that file; deletion removes that file. Retrying an identical Release preserves the existing account, and conflicting credentials remain rejected. Fetch uses the file's ETag to skip unchanged accounts, and passphrase rotation reencrypts the same file. There are no separate state files, revision directories or revision cleanup operations.

This account layout is converted offline in the paused local synchronization folder at the user's request. The cloud contract remains 5; this change adds no automatic cloud migration. Existing pending local transfers still accept their historical credential-checksum field during recovery.

## File coordination

The shared `CloudAccessLock` class owns one cooperative lease per logical cloud operation. The idle file remains exactly `{"writing":0}`. An owned file contains `{"writing":0,"owner":"<operation UUID>","expiresAt":<Unix seconds>}`. Acquisition checks the record, requests a short native file `LOCK` with `Depth: 0` and `Timeout: Second-60`, rereads the owner, and writes its claim. Native locks are released immediately after metadata updates. Servers returning 405 or 501 for LOCK use the same JSON owner lease without native locking. Genuine lock conflicts and network failures are still reported.

An owner sets `expiresAt` to current Unix time plus 300 seconds. One background worker renews the lease when 60 seconds remain, including during transfers and local processing. Renewal and release reread and validate ownership before updating the record. Local monotonic deadlines prevent a backward wall-clock adjustment from prolonging ownership; participating machines must have synchronized clocks. Renewal failure, observed ownership loss, or expiry stops further access; the same lease instance never silently reacquires ownership.

Immediately before the first payload mutation, the lease sets `writing` to 1 while retaining its owner and expiry. Subsequent payload reads and mutations use the same operation lease and only check its local failure state and expiry before and after the request; they do not reread `sync-lock.json` or acquire per-resource native locks. Remote ownership is validated when marking the first mutation, renewing the lease, and releasing it. An unexpected owner replacement is detected at the next such validation rather than before every payload write. Nested operations and all skills in a batch reuse this lease. Completion stops and joins the renewal worker, validates the still-live owner, and restores `{"writing":0}`. A failed or expired owner leaves its record for recovery rather than clearing a successor. Cleanup failures are reported even when payload changes have already completed.

Cloud access does not run automatic capability probes, require strong ETags, attach cross-resource tagged ETag conditions to payload requests, or require a successful conditional-write test. Available strong ETags are still sent as ordinary If-Match hints, and available ETags support conditional downloads; absent ETags cause ordinary reads instead. The legacy `allowOptimisticWrites` config field remains accepted for older clients but no longer gates synchronization or appears as an editable checkbox. Explicit connection tests may report unsupported conditional writes as warnings. Encryption setup creates its descriptor once without read-back or a duplicate conditional creation.

Native locks and conditional headers are opportunistic: Jianguoyun may grant multiple native tokens to the same account and ignore conditional headers. Ownership and expiry checks therefore provide cooperative coordination, not guaranteed atomic exclusion between simultaneous claimants. Content integrity, encryption authentication, contract-version checks and interrupted-operation recovery remain enforced. An infinite native grant is accepted for short metadata updates; a crash during that window may require manual provider recovery.

On first use only, a missing file is provisioned through an atomic `MKCOL .sync-lock-init` gate, rechecking absence after acquiring that gate. This avoids depending on `If-None-Match: *`, which Jianguoyun may ignore on PUT. The gate is removed after provisioning. It is a one-time initialization guard, not a recursive directory LOCK or a normal-operation lock. Malformed lock files are never silently overwritten. Owned records without an expiry remain eternal; a valid expired lease, including `writing:1`, may be replaced by a conditional claim.

Cloud overwrite preserves `data-contract.json`, `sync-lock.json`, and `.sync-lock-init`. Passphrase rotation leaves these unencrypted coordination files unchanged. The lock file contains no credentials or payload content.

## Interruption and recovery

A crashed operation's valid expiry lease can be reclaimed at or after `expiresAt`. Contenders reread ownership before claiming; available native locks and conditional headers are used without assuming the provider enforces them. Local expiry and owner checks stop a former owner from continuing after observed ownership loss, but cannot make a read/write sequence atomic on an unsupported provider. Takeover does not roll back interrupted multi-object writes; the existing account recovery checks still apply.

Owner records require `expiresAt`; malformed records fail closed, and stranded initialization gates or infinite native locks still require manual recovery. Stop participating clients, preserve originals, inspect retained sources and interrupted account/passphrase operations, restore consistency, and release provider locks before clearing legacy status. Never clear a live owner's record.

Idle records remain valid, and releasing an owned lease restores the idle representation. Owner records without an expiry are rejected and preserved for manual recovery.

The protocol coordinates updated, cooperating clients through the owner lease; it does not promise server-enforced atomic exclusion when the provider ignores conditional updates or native locking. It does not prevent manual edits or software bypassing the coordination file. Upgrade all participating clients before using the new protocol. Released cloud-contract-1 clients reject marker 2; during transition, a conforming server's old recursive root lock conflicts with the new file lock. Pre-contract clients cannot enforce the version gate and must be stopped or upgraded.

Cloud uploads trust successful PUT responses, including responses without a strong ETag. Successful uploads need no verification read-back or cloud rollback journal. An uncertain package descriptor outcome is resolved before acknowledging its local buffer prefix. Business data is read when needed, modified locally, and uploaded. Network errors and transient HTTP 408/425/429/500/502/503/504 responses trigger up to three PUT attempts with a one-second delay, reusing the exact local bytes and rewinding streams. Under a live operation lease, retry requests omit stale create/update preconditions so an upload whose response was lost can be resent without a verification GET. Initial conflicts, authorization failures, and failed/expired leases are not bypassed. If attempts are exhausted, report failure and leave completed uploads in place. Multi-file operations, including passphrase rotation, may therefore remain partially applied; there is no automatic cloud rollback or transaction recovery. Download-time authentication, format and identity checks remain enforced. Local data migrations and local credential preservation retain their own safety mechanisms.
