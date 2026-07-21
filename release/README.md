# Codex Usage Monitor deployment

This release contains matching deployment artifacts for version 1.1.0:

- `codex-usage-monitor-1.1.0.vsix`: install this extension package in VS Code.
- `runtime/`: copy this complete standalone monitor directory to the target machine and follow its `README.md`.

The extension connects to the monitor at `http://127.0.0.1:8765`; it does not start the Python service itself. Keep the VSIX and runtime directory from the same release together.
