#!/usr/bin/env python3
"""Check that `.cargo/mutants.toml`'s `exclude_re` entries still name real files.

Every `exclude_re` entry documents a specific mutant as equivalent or
untestable, and most of them pin that mutant down with a literal path
fragment -- `segment/io.rs`, `txn_index.rs:94`, and so on. A refactor that
moves or renames the file the fragment names does not touch the regex, so the
entry silently stops matching anything: the mutant it used to suppress goes
back to being tested, and if it is a genuine equivalent mutant it comes back
as an unexplained survivor. `.cargo/mutants.toml`'s own header says the sweep
has no survivor baseline, so a single one of these fails its shard.

This script extracts the literal path fragment from each `exclude_re` entry
-- the run of identifier characters and `/` immediately before a `.rs` -- and
fails when no file under `crates/` ends with it. An entry with no such
fragment (matched purely on a function or type name) has nothing to check and
always passes.

    tools/check-mutants-config.py
"""

import argparse
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / ".cargo" / "mutants.toml"
CRATES = ROOT / "crates"

# A path fragment inside an `exclude_re` entry: identifier characters and `/`
# ending in a (possibly regex-escaped) `.rs`.
FRAGMENT = re.compile(r"[A-Za-z0-9_/]+\\?\.rs")


def path_fragments(pattern: str) -> list[str]:
    """Every literal `.rs` path fragment `pattern` names."""
    return [match.replace("\\.rs", ".rs") for match in FRAGMENT.findall(pattern)]


def crate_paths(crates: Path) -> set[str]:
    """Every `.rs` file under `crates`, as a path relative to the repo root."""
    return {path.relative_to(crates.parent).as_posix() for path in crates.rglob("*.rs")}


def dead_fragments(patterns: list[str], files: set[str]) -> list[tuple[str, str]]:
    """`(pattern, fragment)` pairs whose fragment matches no file in `files`."""
    dead = []
    for pattern in patterns:
        for fragment in path_fragments(pattern):
            if not any(f == fragment or f.endswith(f"/{fragment}") for f in files):
                dead.append((pattern, fragment))
    return dead


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--crates", type=Path, default=CRATES)
    args = parser.parse_args()

    config = tomllib.loads(args.config.read_text())
    patterns = config.get("exclude_re", [])
    if not patterns:
        print(f"{args.config}: no exclude_re entries found", file=sys.stderr)
        return 1

    failed = False
    for pattern, fragment in dead_fragments(patterns, crate_paths(args.crates)):
        print(
            f"{args.config}: exclude_re entry names a path with no match "
            f"under {args.crates.name}/: {fragment!r} (from {pattern!r})",
            file=sys.stderr,
        )
        failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
