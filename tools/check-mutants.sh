#!/usr/bin/env bash
set -euo pipefail

package="${1:?usage: check-mutants.sh PACKAGE}"

# Build cargo-mutants from the repository's pinned tools/mutants/Cargo.lock,
# then let Cargo run every unit and integration test belonging to the package.
exec bazel run @mutants//:cargo-mutants__cargo-mutants -- \
  mutants --package "${package}" --test-tool cargo --jobs 4
