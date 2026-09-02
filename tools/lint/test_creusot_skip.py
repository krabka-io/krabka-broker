#!/usr/bin/env python3
"""Self-test for creusot_skip.py."""

import subprocess
import sys
import tempfile
from pathlib import Path

from creusot_skip import creusot_fns, unskipped

# (source text, the (line, name) pairs it holds).
CASES = [
    ("fn plain() {}\n", []),
    (
        "#[cfg(creusot)]\n#[cfg_attr(test, mutants::skip)]\n#[logic]\npub fn a() -> Int {}\n",
        [(1, "a")],
    ),
    # No fn under it at all: a `use`, out of this check's concern.
    ("#[cfg(creusot)]\nuse std::clone::Clone;\n", []),
    # A statement inside a function body, not an item.
    ("fn f() {\n    #[cfg(creusot)]\n    g();\n}\n", []),
    # A doc comment and a multi-line attribute sit between the cfg and the fn.
    (
        "#[cfg(creusot)]\n/// Doc.\n#[requires(a@ > 0\n    && b@ > 0)]\npub fn h(a: i32, b: i32) {}\n",
        [(1, "h")],
    ),
    # Two in one file.
    (
        "#[cfg(creusot)]\npub fn one() {}\n\n#[cfg(creusot)]\npub fn two() {}\n",
        [(1, "one"), (4, "two")],
    ),
]

for text, expected in CASES:
    actual = creusot_fns(text)
    if actual != expected:
        sys.exit(f"expected {expected}, got {actual} for {text!r}")

with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    verified_src = root / "crates" / "verified" / "src"
    verified_src.mkdir(parents=True)

    (verified_src / "skipped.rs").write_text(
        "#[cfg(creusot)]\n#[cfg_attr(test, mutants::skip)]\n#[logic]\npub fn ok() -> Int {}\n"
    )
    (verified_src / "bare.rs").write_text("#[cfg(creusot)]\n#[logic]\npub fn missing() -> Int {}\n")
    (verified_src / "wrong_order.rs").write_text(
        "#[cfg_attr(test, mutants::skip)]\n#[cfg(creusot)]\n#[logic]\npub fn reversed() -> Int {}\n"
    )
    (verified_src / "unrelated.rs").write_text("pub fn plain() {}\n")

    expected_offenders = [
        ("crates/verified/src/bare.rs", 1, "missing"),
        ("crates/verified/src/wrong_order.rs", 2, "reversed"),
    ]
    actual_offenders = unskipped(root)
    if actual_offenders != expected_offenders:
        sys.exit(f"expected {expected_offenders}, got {actual_offenders}")

    check = [sys.executable, Path(__file__).with_name("creusot_skip.py"), root]

    failure = subprocess.run(check, capture_output=True, text=True)
    if failure.returncode == 0 or "bare.rs" not in failure.stderr:
        sys.exit(f"expected a failure that names the offender, got {failure.stderr!r}")

    (verified_src / "bare.rs").write_text(
        "#[cfg(creusot)]\n#[cfg_attr(test, mutants::skip)]\n#[logic]\npub fn missing() -> Int {}\n"
    )
    (verified_src / "wrong_order.rs").write_text(
        "#[cfg(creusot)]\n#[cfg_attr(test, mutants::skip)]\n#[logic]\npub fn reversed() -> Int {}\n"
    )

    success = subprocess.run(check, capture_output=True, text=True)
    if success.returncode != 0:
        sys.exit(f"expected the fixed tree to pass, got {success.stderr!r}")
