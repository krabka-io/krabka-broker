#!/usr/bin/env python3
"""Check that every `#[cfg(creusot)]` `fn` in `crates/verified/src/` skips mutation testing.

cargo-mutants generates mutants syntactically and does not evaluate `#[cfg]`,
so a `#[cfg(creusot)]` function -- compiled only under the Creusot proof
toolchain, never in a normal build or test run -- is mutated anyway and
reported as a survivor: no test can tell, because the mutated code never
runs. `.cargo/mutants.toml`'s own header says the sweep has no survivor
baseline, so one of these fails `//crates/verified:verified_mutants`.

The fix lives on the function itself: `#[cfg_attr(test, mutants::skip)]`
directly under `#[cfg(creusot)]`. cargo-mutants reads that attribute from
source, so it works on a function a normal build never compiles, and unlike a
path-and-line regex in `exclude_re` it cannot rot when the function moves.

    python3 tools/lint/creusot_skip.py [root]
"""

import re
import sys
from pathlib import Path

SKIP = "#[cfg_attr(test, mutants::skip)]"

# A `#[...]` attribute, including one whose content spans multiple lines
# (an `#[ensures(...)]` with a multi-line predicate, for instance).
ATTRIBUTE = re.compile(r"\A\s*#\[")


def _skip_attribute(lines: list[str], start: int) -> int:
    """The line index just past the attribute or doc comment at `lines[start]`.

    An attribute's `[...]` can span multiple lines, so this tracks bracket
    depth rather than assuming one line closes it.
    """
    stripped = lines[start].strip()
    if stripped.startswith("///") or stripped.startswith("//!"):
        return start + 1
    depth = 0
    index = start
    while index < len(lines):
        for char in lines[index]:
            if char == "[":
                depth += 1
            elif char == "]":
                depth -= 1
        index += 1
        if depth <= 0:
            break
    return index


def creusot_fns(text: str) -> list[tuple[int, str]]:
    """`(line, name)` for every `#[cfg(creusot)]` attribute that sits on a `fn`.

    Skips forward over any other attributes and doc comments between the
    `#[cfg(creusot)]` line and the item it decorates, so a `#[logic]` or
    `#[requires(...)]` stack in between does not hide the function underneath.
    An item that is not a `fn` -- a `use`, a statement inside a function body,
    a `mod` -- is not this check's concern and is skipped.
    """
    lines = text.splitlines()
    found = []
    index = 0
    while index < len(lines):
        if lines[index].strip() != "#[cfg(creusot)]":
            index += 1
            continue
        cfg_line = index
        cursor = index + 1
        while cursor < len(lines):
            stripped = lines[cursor].strip()
            if stripped == "":
                cursor += 1
                continue
            if ATTRIBUTE.match(lines[cursor]) or stripped.startswith(("///", "//!")):
                cursor = _skip_attribute(lines, cursor)
                continue
            break
        match = re.match(r"\s*(pub(\(\w+\))?\s+)?fn\s+([A-Za-z0-9_]+)", lines[cursor]) if cursor < len(lines) else None
        if match:
            found.append((cfg_line + 1, match.group(3)))
        index = cursor
    return found


def unskipped(root: Path) -> list[tuple[str, int, str]]:
    """`(path, line, name)` for every `#[cfg(creusot)]` `fn` with no skip attribute directly under it."""
    offenders = []
    verified_src = root / "crates" / "verified" / "src"
    for source in sorted(verified_src.rglob("*.rs")):
        lines = source.read_text().splitlines()
        for line, name in creusot_fns(source.read_text()):
            next_line = lines[line].strip() if line < len(lines) else ""
            if next_line != SKIP:
                offenders.append((str(source.relative_to(root)), line, name))
    return offenders


if __name__ == "__main__":
    found = unskipped(Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd())
    if found:
        listing = "\n".join(f"  {path}:{line}: {name}" for path, line, name in found)
        sys.exit(
            f"Every #[cfg(creusot)] fn in crates/verified/src/ needs "
            f"`{SKIP}` directly under #[cfg(creusot)], or cargo-mutants "
            f"reports it as an unexplained survivor:\n{listing}"
        )
