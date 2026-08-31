#!/usr/bin/env python3
"""Self-test for crate_changelogs.py."""

import subprocess
import sys
import tempfile
from pathlib import Path

from crate_changelogs import offenders, version_headings

# (changelog body, the version headings it holds).
CASES = [
    ("# Changelog\n\n## [Unreleased]\n", []),
    (
        "## [Unreleased]\n\n## [0.3.8] - 2026-06-23\n\n### Added\n",
        ["## [0.3.8] - 2026-06-23"],
    ),
    ("## [1.0.0]\n\n## [Unreleased]\n", ["## [1.0.0]"]),
    ("## [v2.1] - 2026-01-01\n", ["## [v2.1] - 2026-01-01"]),
    ("## 0.1.0\n", ["## 0.1.0"]),
    ("## [Unreleased]\n\n- The 1.2.3 wire format is unchanged.\n", []),
    ("## [Unreleased]\n\n```\n## [0.1.0]\n```\n", []),
]

for body, expected in CASES:
    actual = version_headings(body)
    if actual != expected:
        sys.exit(f"expected {expected}, got {actual} for {body!r}")

with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    (root / "CHANGELOG.md").write_text("## [Unreleased]\n\n## [0.5.1] - 2026-08-30\n")

    for crate, body in [
        ("truthful", "# Changelog\n\n## [Unreleased]\n"),
        ("extracted", "## [Unreleased]\n## [0.3.8] - 2026-06-23\n"),
        ("nested/deeper", "## [0.1.0]\n"),
    ]:
        (root / "crates" / crate).mkdir(parents=True)
        (root / "crates" / crate / "CHANGELOG.md").write_text(body)
    (root / "crates" / "silent").mkdir()

    expected_offenders = [
        ("crates/extracted/CHANGELOG.md", "## [0.3.8] - 2026-06-23"),
        ("crates/nested/deeper/CHANGELOG.md", "## [0.1.0]"),
    ]
    actual_offenders = offenders(root)
    if actual_offenders != expected_offenders:
        sys.exit(f"expected {expected_offenders}, got {actual_offenders}")

    check = [sys.executable, Path(__file__).with_name("crate_changelogs.py"), root]

    failure = subprocess.run(check, capture_output=True, text=True)
    if failure.returncode == 0 or "crates/extracted/CHANGELOG.md" not in failure.stderr:
        sys.exit(f"expected a failure that names the offender, got {failure.stderr!r}")

    (root / "crates" / "extracted" / "CHANGELOG.md").write_text("## [Unreleased]\n")
    (root / "crates" / "nested" / "deeper" / "CHANGELOG.md").unlink()

    success = subprocess.run(check, capture_output=True, text=True)
    if success.returncode != 0:
        sys.exit(f"expected the truthful tree to pass, got {success.stderr!r}")
