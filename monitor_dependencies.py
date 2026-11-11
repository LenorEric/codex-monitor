"""Dependency preflight using only the Python standard library."""

import importlib
import importlib.metadata
import os
import re
import subprocess
import sys
from pathlib import Path

from monitor_common import console_log


def validate_dependencies(runtime_dir: Path) -> bool:
    try:
        requirements = (Path(runtime_dir) / "requirements.txt").read_text(encoding="utf-8")
    except OSError as exc:
        console_log(f"Cannot validate Python libraries: {exc}", level="error")
        return False
    failures = []
    for requirement in requirements.splitlines():
        requirement = requirement.split("#", 1)[0].strip()
        if not requirement:
            continue
        match = re.match(r"^[A-Za-z0-9][A-Za-z0-9_.-]*", requirement)
        if match is None:
            failures.append(f"Invalid library requirement: {requirement}")
            continue
        name = match.group()
        try:
            importlib.metadata.distribution(name)
            importlib.import_module(name.replace("-", "_"))
        except importlib.metadata.PackageNotFoundError:
            failures.append(f"Missing Python library: {requirement}")
        except Exception as exc:
            failures.append(f"Cannot load Python library {name}: {exc}")
    if failures:
        for failure in failures:
            console_log(failure, level="error")
        console_log(f'Install required libraries with: {"& " if os.name == "nt" else ""}"{sys.executable}" -m pip install -r "{Path(runtime_dir) / "requirements.txt"}"', level="error")
        return False
    return True


def install_dependencies(runtime_dir: Path) -> None:
    console_log("Installing updated Python requirements...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", str(Path(runtime_dir) / "requirements.txt")], check=True)
    console_log("Updated Python requirements installed successfully.")
