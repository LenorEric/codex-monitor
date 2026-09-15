import hashlib
import json
import re
import shutil
import subprocess
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RELEASE_DIR = ROOT / "release"
RELEASE_PACK_DIR = ROOT / "release_pack"
RUNTIME_DIR = RELEASE_DIR / "runtime"
VSCE_VERSION = "3.9.2"
VERSION_PATTERN = re.compile(r"^(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)$")
RUNTIME_FILES = (
    "LICENSE",
    "dashboard.html",
    "management.html",
    "codex_monitor_daemon.py",
    "monitor_accounts.py",
    "monitor_auto_update.py",
    "monitor_cloud.py",
    "monitor_cloud_queue.py",
    "monitor_codex_usage.py",
    "monitor_common.py",
    "monitor_device_auth.py",
    "monitor_dashboard.py",
    "monitor_events.py",
    "monitor_history.py",
    "monitor_quota.py",
    "monitor_skills.py",
    "monitor_session_refresh.py",
    "monitor_token_ledger.py",
    "monitor_tokens.py",
    "monitor_usage_sync.py",
    "requirements.txt",
)


def next_patch_version(version: str) -> str:
    match = VERSION_PATTERN.fullmatch(version)
    if match is None:
        raise ValueError(f"package.json version must be major.minor.patch, got {version!r}")
    return f"{match['major']}.{match['minor']}.{int(match['patch']) + 1}"


def bump_package_version() -> str:
    package_path = ROOT / "package.json"
    with package_path.open(encoding="utf-8", newline="") as package_file:
        package_text = package_file.read()
    package = json.loads(package_text)
    current_version = package.get("version")
    if not isinstance(current_version, str):
        raise ValueError("package.json must contain a string version")
    version = next_patch_version(current_version)
    updated_text, replacements = re.subn(
        rf'(?m)^([ \t]*"version"[ \t]*:[ \t]*)"{re.escape(current_version)}"',
        rf'\g<1>"{version}"',
        package_text,
        count=1,
    )
    if replacements != 1:
        raise ValueError("could not update the package.json version field")
    with package_path.open("w", encoding="utf-8", newline="") as package_file:
        package_file.write(updated_text)
    return version


def write_text_lf(path: Path, text: str) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as output:
        output.write(text)


def rebuild_runtime(version: str) -> None:
    if RUNTIME_DIR.exists():
        shutil.rmtree(RUNTIME_DIR)
    RUNTIME_DIR.mkdir(parents=True)
    for name in RUNTIME_FILES:
        write_text_lf(RUNTIME_DIR / name, (ROOT / name).read_text(encoding="utf-8"))
    write_text_lf(RUNTIME_DIR / "README.md", (ROOT / "release-runtime.md").read_text(encoding="utf-8"))
    write_text_lf(RUNTIME_DIR / "version.json", json.dumps({"version": version, "dataContractVersion": json.loads((ROOT / "package.json").read_text(encoding="utf-8"))["dataContractVersion"]}, indent=2) + "\n")
    write_text_lf(RELEASE_DIR / "README.md", (ROOT / "release-package.md").read_text(encoding="utf-8").replace("{{VERSION}}", version))


def write_release_version(version: str) -> None:
    files = {}
    for path in sorted(RUNTIME_DIR.iterdir(), key=lambda item: item.name.casefold()):
        if path.is_file():
            data = path.read_bytes()
            files[path.name] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    write_text_lf(RELEASE_DIR / "version.json", json.dumps({"version": version, "files": files}, indent=2) + "\n")


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


def build_release_zip(version: str) -> Path:
    RELEASE_PACK_DIR.mkdir(exist_ok=True)
    output = RELEASE_PACK_DIR / f"code-monitor-v{version}.zip"
    temporary_output = output.with_suffix(".zip.tmp")
    temporary_output.unlink(missing_ok=True)
    try:
        with zipfile.ZipFile(temporary_output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(RELEASE_DIR.rglob("*")):
                if path.is_file():
                    archive.write(path, path.relative_to(RELEASE_DIR))
        temporary_output.replace(output)
    finally:
        temporary_output.unlink(missing_ok=True)
    for old_package in RELEASE_PACK_DIR.glob("code-monitor-v*.zip"):
        if old_package.name != output.name:
            old_package.unlink()
    return output


def build_release(version: str) -> None:
    RELEASE_DIR.mkdir(exist_ok=True)
    rebuild_runtime(version)
    write_release_version(version)
    for old_package in RELEASE_DIR.glob("codex-usage-monitor-*.vsix"):
        if old_package.name != f"codex-usage-monitor-{version}.vsix":
            old_package.unlink()
    build_vsix(RELEASE_DIR / f"codex-usage-monitor-{version}.vsix")
    archive = build_release_zip(version)
    print(f"Release {version} built in {RELEASE_DIR}")
    print(f"Archive created at {archive}")


def commit_release(version: str) -> None:
    git = shutil.which("git")
    if git is None:
        raise RuntimeError("git is required to commit the release")
    subprocess.run([git, "add", "--all"], cwd=ROOT, check=True)
    subprocess.run([git, "commit", "-m", f"v{version}"], cwd=ROOT, check=True)
    print(f"Release commit v{version} created")


def main() -> None:
    version = bump_package_version()
    build_release(version)
    commit_release(version)


if __name__ == "__main__":
    main()
