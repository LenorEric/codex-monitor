# Testing

- Stop every test instance after the test finishes, including instances started for live traffic or integration testing.

# WebDAV notifications

- Emit exactly one start/queued message and one end message for every logical WebDAV operation, including automatic synchronization. Report success or failure, and explicitly explain cancellation, skipping, or partial completion. Nested WebDAV requests must not duplicate these messages.

# Persistent data contracts

- Increment the data contract only when changes to locally or cloud-saved data require conversion or rebuilding, including changes to persistent formats, schemas, field meanings, filenames, or derived data. Transport-only client/server changes do not require a bump.
- Keep at most one unreleased migration: if the latest released contract is N, the pending target is N+1. Create it for the first persistent-contract change; merge all further unreleased changes into that same migration instead of incrementing again.
- Keep `package.json.dataContractVersion`, `DATA_CONTRACT_VERSION`, and the newest `DATA_MIGRATIONS` entry aligned. Packaging propagates the target to `release/runtime/version.json`; do not increment it merely for a build or a release without saved-data changes.
- A successful explicit release through `build_release.py` freezes the included migration. The next persistent-contract change then creates the next number. Failed builds, commits, tests, and generated runtime/manifest refreshes do not freeze a pending migration.
- Determine the released baseline from the last successful release, not working-tree metadata or refreshed `release/runtime` files. Never rewrite or merge migrations already released to users; leave every released intermediate version available for sequential upgrades.
- The pending migration must convert the latest released data directly to the final intended representation. Avoid repeated cache rebuilds and temporary schemas or indexes superseded before release.
- Every migration must cover existing data by preserving it or safely rebuilding an equivalent representation from retained sources. Preserve originals on failure, make retries safe, and run pending steps consecutively on first startup before affected data is used.
