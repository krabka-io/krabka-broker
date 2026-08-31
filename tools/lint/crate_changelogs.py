#!/usr/bin/env python3
"""Check that no crate CHANGELOG documents a release.

Krabka versions and tags the whole workspace as one unit, so the root
`CHANGELOG.md` is the only record of what shipped. A crate `CHANGELOG.md` is
allowed to hold an `[Unreleased]` heading and prose, and nothing else. This
check fails when one carries a version heading, which is what the twelve files
extracted from robot-head/crabka did: they documented that repository's
releases under this repository's crate names.

The rule here is stricter than "no version heading below `[Unreleased]`". A
version heading anywhere in a crate changelog is a release record, and the
position of the `[Unreleased]` heading does not change that.

    python3 tools/lint/crate_changelogs.py [root]
"""

import re
import sys
from pathlib import Path

# The text of a Markdown heading that names a version: `## [0.3.8] - 2026-06-23`,
# `## 1.0.0`, `### [v2.1]`. The link brackets and the `v` prefix are optional,
# and the release date, if there is one, follows the version.
VERSION_HEADING = re.compile(r"#{1,6}\s+\[?v?\d+(?:\.\d+)+\]?")

FENCE = "```"


def version_headings(text: str) -> list[str]:
    """Return the version headings of one changelog, in file order.

    A heading inside a fenced code block is an example, not a release, so this
    skips fenced blocks.
    """
    headings = []
    fenced = False

    for line in text.splitlines():
        if line.lstrip().startswith(FENCE):
            fenced = not fenced
        elif not fenced and VERSION_HEADING.match(line):
            headings.append(line.strip())

    return headings


def offenders(root: Path) -> list[tuple[str, str]]:
    """Return every (path, heading) pair that breaks the rule under `root`."""
    return [
        (str(changelog.relative_to(root)), heading)
        for changelog in sorted((root / "crates").rglob("CHANGELOG.md"))
        for heading in version_headings(changelog.read_text())
    ]


if __name__ == "__main__":
    found = offenders(Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd())
    if found:
        listing = "\n".join(f"  {path}: {heading}" for path, heading in found)
        sys.exit(
            "A crate changelog must record no release. Move the entry to the "
            f"root CHANGELOG.md and delete these headings:\n{listing}"
        )
