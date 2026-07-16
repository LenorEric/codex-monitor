import json
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RELEASE_DIR = ROOT / "release"
RUNTIME_DIR = RELEASE_DIR / "runtime"
VSCE_VERSION = "3.9.2"
RUNTIME_FILES = (
    "LICENSE",
    "dashboard.html",
    "management.html",
    "monitor_accounts.py",
    "monitor_cloud.py",
    "monitor_codex_usage.py",
    "monitor_common.py",
    "monitor_dashboard.py",
    "monitor_events.py",
    "monitor_history.py",
    "monitor_quota.py",
    "monitor_skills.py",
    "monitor_tokens.py",
    "monitor_usage_sync.py",
    "requirements.txt",
)


def package_version() -> str:
    return str(json.loads((ROOT / "package.json").read_text(encoding="utf-8"))["version"])


def rebuild_runtime(version: str) -> None:
    if RUNTIME_DIR.exists():
        shutil.rmtree(RUNTIME_DIR)
    RUNTIME_DIR.mkdir(parents=True)
    for name in RUNTIME_FILES:
        shutil.copy2(ROOT / name, RUNTIME_DIR / name)
    shutil.copy2(ROOT / "release-runtime.md", RUNTIME_DIR / "README.md")
    (RELEASE_DIR / "README.md").write_text((ROOT / "release-package.md").read_text(encoding="utf-8").replace("{{VERSION}}", version), encoding="utf-8")


def build_vsix(output: Path) -> None:
    npx = shutil.which("npx.cmd") or shutil.which("npx")
    if npx is None:
        raise RuntimeError("npx is required to package the VS Code extension")
    temporary_output = output.with_suffix(".vsix.tmp")
    temporary_output.unlink(missing_ok=True)
    try:
        subprocess.run([npx, "--yes", f"@vscode/vsce@{VSCE_VERSION}", "package", "--out", str(temporary_output)], cwd=ROOT, check=True)
        temporary_output.replace(output)
    finally:
        temporary_output.unlink(missing_ok=True)


def main() -> None:
    RELEASE_DIR.mkdir(exist_ok=True)
    version = package_version()
    rebuild_runtime(version)
    for old_package in RELEASE_DIR.glob("codex-usage-monitor-*.vsix"):
        if old_package.name != f"codex-usage-monitor-{version}.vsix":
            old_package.unlink()
    build_vsix(RELEASE_DIR / f"codex-usage-monitor-{version}.vsix")
    print(f"Release {version} built in {RELEASE_DIR}")


if __name__ == "__main__":
    main()
