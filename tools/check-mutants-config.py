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
fails when no file under `crates/` ends with it. When the fragment is
immediately followed by `:<line>`, it also fails when every file that matches
the fragment is shorter than that line: a module split that leaves a thin
facade file behind (`segment.rs` declaring `mod io;` alongside
`segment/io.rs`) passes the bare path check while the pinned line has long
since moved out from under it, so the two checks together catch more of the
same rot than either alone.

What this does NOT check: that the specific mutation the entry's prose
describes (an operator, a match guard, a whole-function replacement) is still
generated at the resolved location. A path that still exists, at a still-long
line, but whose content changed underneath it -- the referenced function
renamed, or moved to a different line within the same file -- passes this
script and would need `cargo-mutants --list` against the real crate to catch.
That check needs a Rust toolchain this script's callers do not all have; a
dead path or a pinned line past the end of the file is the rot this repository
has actually hit, and is what this catches.

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

# A path fragment inside an `exclude_re` entry -- identifier characters and
# `/` ending in a (possibly regex-escaped) `.rs` -- with the line number
# pinned directly after it via `:<line>`, if any.
FRAGMENT = re.compile(r"([A-Za-z0-9_/]+\\?\.rs)(?::(\d+))?")


def path_fragments(pattern: str) -> list[tuple[str, int | None]]:
    """Every `(fragment, pinned_line)` pair `pattern` names, in order."""
    found = []
    for match in FRAGMENT.finditer(pattern):
        fragment = match.group(1).replace("\\.rs", ".rs")
        line = int(match.group(2)) if match.group(2) else None
        found.append((fragment, line))
    return found


def crate_line_counts(crates: Path) -> dict[str, int]:
    """Every `.rs` file under `crates`, as a repo-root-relative path to its line count."""
    return {
        path.relative_to(crates.parent).as_posix(): len(path.read_text().splitlines())
        for path in crates.rglob("*.rs")
    }


def matching_files(fragment: str, line_counts: dict[str, int]) -> list[str]:
    """Every file in `line_counts` that `fragment` names, as a path suffix."""
    return [f for f in line_counts if f == fragment or f.endswith(f"/{fragment}")]


def dead_fragments(patterns: list[str], line_counts: dict[str, int]) -> list[tuple[str, str, str]]:
    """`(pattern, fragment, reason)` triples for a fragment this repository no longer supports."""
    dead = []
    for pattern in patterns:
        for fragment, line in path_fragments(pattern):
            matches = matching_files(fragment, line_counts)
            if not matches:
                dead.append((pattern, fragment, "no file under crates/ matches this path"))
            elif line is not None and line > max(line_counts[f] for f in matches):
                longest = max(line_counts[f] for f in matches)
                dead.append(
                    (
                        pattern,
                        fragment,
                        f"pinned to line {line}, but the longest matching file has only {longest} lines",
                    )
                )
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
    for pattern, fragment, reason in dead_fragments(patterns, crate_line_counts(args.crates)):
        print(
            f"{args.config}: exclude_re entry names a path this repository no longer "
            f"supports: {fragment!r} -- {reason} (from {pattern!r})",
            file=sys.stderr,
        )
        failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
