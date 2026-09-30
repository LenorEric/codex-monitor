# Usage sync v4 contract

Each client owns `usage/v4/heads/<machine-id>.enc`, `months/<machine-id>/`, `days/<machine-id>/`, and `data/<machine-id>/`. All objects are encrypted with a purpose containing their kind, owner, and content ID. Immutable object names use the SHA-256 of canonical compressed JSON; the mutable head is written with a strong WebDAV ETag condition.

The head (`version: 4`) contains its owner, generation, month index IDs, and at most one active day reference. A month index refers to consolidated day manifests. An active day manifest refers to sealed UTC half-hour parts. Each part has an independently compressed and encrypted list of envelopes. A closed day manifest refers to one bulk file. Empty periods create no part.

An envelope contains `kind`, `schemaVersion`, `recordKey`, `sourceMachineId`, `collectionPeriod`, `eventAt`, `contentSha256`, and `record`. The collection period is fixed when the key first enters the local index. A later correction retains that period. A deletion removes the key. The day logical hash covers sorted record keys and record contents, excluding packing fields, so consolidation leaves it unchanged. Future record kinds are validated for bounded shape and retained without interpretation.

Publish immutable data before manifests, month indexes, and the conditional head. A failed upload or head commit leaves the previous head readable. A reader lists heads once, gets changed indexes, and compares day logical hashes with its validated local cache before getting record payloads. A packing-only change updates metadata without a payload download. The local SQLite cache keeps own key-to-period assignments and verified peer days; old peer cache records are provisional until that peer publishes v4.

Legacy cloud usage paths are ignored by v4 sync and passphrase rotation. Local historical quota and token records are published again from each owner, regardless of old upload checkpoints. Initial backfill uses historical event times as collection periods when no collection time was stored. The local data contract version is 6.
