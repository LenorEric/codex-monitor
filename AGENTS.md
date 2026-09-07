# Testing

- Stop every test instance after the test finishes, including instances started for live traffic or integration testing.

# WebDAV notifications

- Emit exactly one start/queued message and one end message for every logical WebDAV operation, including automatic synchronization. Report success or failure, and explicitly explain cancellation, skipping, or partial completion. Nested WebDAV requests must not duplicate these messages.
