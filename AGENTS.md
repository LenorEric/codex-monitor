# Testing

- Stop every test instance after the test finishes, including instances started for live traffic or integration testing.

# WebDAV notifications

- Emit exactly one start/queued message and one end message for every logical WebDAV operation, including automatic synchronization. Report success or failure, and explicitly explain cancellation, skipping, or partial completion. Nested WebDAV requests must not duplicate these messages.

# Persistent data contracts

- Whenever a persistent data contract changes, including a file format, schema, field meaning, or filename, increment `dataContractVersion` in the local program metadata (`package.json`, propagated to the packaged `version.json`), update `DATA_CONTRACT_VERSION`, and add the corresponding ordered migration to `DATA_MIGRATIONS`. Never skip intermediate migration numbers: cross-version updates must run every pending migration consecutively. Each migration must cover all existing data by preserving it directly or safely rebuilding an equivalent representation through another defined path, preserve the original data on failure, be safe to retry, and run automatically on the first startup after an update before affected data is read or written.
