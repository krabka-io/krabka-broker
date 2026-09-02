#!/usr/bin/env bash
# Every workspace crate has a README, and the root README's crate table names
# every crate. The `docs` CI job runs this; it needs no Bazel.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
failed=0

for manifest in "${root}"/crates/*/Cargo.toml; do
  pkg_dir="$(dirname "${manifest}")"
  name="$(sed -n 's/^name = "\(.*\)"$/\1/p' "${manifest}" | head -n 1)"
  if [ ! -f "${pkg_dir}/README.md" ]; then
    echo "crates/$(basename "${pkg_dir}") has no README.md" >&2
    failed=1
  fi
  if ! grep -q "^| \`${name}\` |" "${root}/README.md"; then
    echo "README.md crate table does not list ${name}" >&2
    failed=1
  fi
done

exit "${failed}"
