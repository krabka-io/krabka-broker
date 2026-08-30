#!/usr/bin/env python3

import subprocess
import sys
import tempfile
from pathlib import Path

from creusot_packages import discover, uncovered


def write_crate(root: Path, name: str, source: str, dependency: bool = False) -> None:
    crate = root / "crates" / name
    (crate / "src").mkdir(parents=True)
    dependencies = (
        '[dependencies]\ncreusot-std = { path = "../../../creusot-std" }\n'
        if dependency
        else ""
    )
    (crate / "Cargo.toml").write_text(
        f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2024"\n'
        + dependencies
    )
    (crate / "src/lib.rs").write_text(source)


with tempfile.TemporaryDirectory() as directory:
    temporary = Path(directory)
    workspace = temporary / "workspace"
    workspace.mkdir()
    (workspace / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/*"]\nresolver = "2"\n')

    creusot = temporary / "creusot-std"
    (creusot / "src").mkdir(parents=True)
    (creusot / "Cargo.toml").write_text(
        '[package]\nname = "creusot-std"\nversion = "0.1.0"\nedition = "2024"\n'
    )
    (creusot / "src/lib.rs").write_text("")

    write_crate(workspace, "by-dependency", "", dependency=True)
    write_crate(workspace, "by-requires", "#[requires(true)]\npub fn checked() {}\n")
    write_crate(
        workspace,
        "by-cfg-requires",
        "#[cfg_attr(\n    creusot,\n    requires(true)\n)]\npub fn checked_cfg() {}\n",
    )
    write_crate(
        workspace,
        "by-cfg-ensures",
        "#[ cfg_attr ( creusot , ensures(true) ) ]\npub fn ensured_cfg() {}\n",
    )
    write_crate(workspace, "third-kernel", "#[ensures(true)]\npub fn third() {}\n")
    write_crate(workspace, "ordinary", "pub fn ordinary() {}\n")
    subprocess.run(["cargo", "generate-lockfile", "--offline"], cwd=workspace, check=True)

    actual = discover(workspace)
    expected = [
        "by-cfg-ensures",
        "by-cfg-requires",
        "by-dependency",
        "by-requires",
        "third-kernel",
    ]
    if actual != expected:
        sys.exit(f"expected {expected}, got {actual}")

    covered = ["by-dependency", "by-requires"]
    missing = uncovered(workspace, covered)
    expected_missing = ["by-cfg-ensures", "by-cfg-requires", "third-kernel"]
    if missing != expected_missing:
        sys.exit(f"expected uncovered {expected_missing}, got {missing}")

    check = subprocess.run(
        [
            sys.executable,
            Path(__file__).with_name("creusot_packages.py"),
            *covered,
        ],
        cwd=workspace,
        capture_output=True,
        text=True,
    )
    if check.returncode == 0 or not all(name in check.stderr for name in expected_missing):
        sys.exit(f"expected uncovered {expected_missing} failure, got {check.stderr!r}")
