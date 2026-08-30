#!/usr/bin/env python3
"""Check that the proofs job covers every Creusot workspace package."""

import json
import re
import subprocess
import sys
from pathlib import Path


CONTRACT = re.compile(
    r"^\s*#\s*\[\s*(?:(?:requires|ensures)\b|cfg_attr\s*\(\s*creusot\s*,\s*(?:requires|ensures)\b)",
    re.MULTILINE,
)


def discover(root: Path) -> list[str]:
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--no-deps", "--format-version=1", "--locked"],
            cwd=root,
            text=True,
        )
    )
    members = set(metadata["workspace_members"])
    packages = []

    for package in metadata["packages"]:
        if package["id"] not in members:
            continue

        crate_root = Path(package["manifest_path"]).parent
        has_dependency = any(
            dependency["name"] == "creusot-std"
            for dependency in package["dependencies"]
        )
        has_contract = any(
            CONTRACT.search(source.read_text()) for source in crate_root.rglob("*.rs")
        )
        if has_dependency or has_contract:
            packages.append(package["name"])

    return sorted(packages)


def uncovered(root: Path, covered: list[str]) -> list[str]:
    return sorted(set(discover(root)) - set(covered))


if __name__ == "__main__":
    missing = uncovered(Path.cwd(), sys.argv[1:])
    if missing:
        sys.exit(f"Creusot packages missing from the proofs job: {', '.join(missing)}")
