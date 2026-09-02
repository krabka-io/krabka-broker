#!/usr/bin/env bash
# Self-test for check-kfcs.sh.
#
# Copies docs/KFCs to a temporary directory and proves three things:
#
# 1. The checked-in tree passes.
# 2. Deleting any one of the seven required headings from any KFC fails.
# 3. A Status value that disagrees with the README index row fails.
#
# Usage: check-kfcs-test.sh
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
check="${script_dir}/check-kfcs.sh"
source_dir="${script_dir}/../docs/KFCs"

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT

reset_copy() {
  rm -rf "${tmp}/KFCs"
  cp -r "${source_dir}" "${tmp}/KFCs"
}

failures=0
expect_pass() {
  local label="$1"
  if ! "${check}" "${tmp}/KFCs" >/dev/null 2>&1; then
    echo "FAIL: ${label}: check-kfcs.sh reported a failure on a valid tree" >&2
    failures=$((failures + 1))
  fi
}
expect_fail() {
  local label="$1" pattern="$2" output
  if output="$("${check}" "${tmp}/KFCs" 2>&1)"; then
    echo "FAIL: ${label}: check-kfcs.sh passed, expected a failure" >&2
    failures=$((failures + 1))
  elif ! grep -q -- "${pattern}" <<<"${output}"; then
    echo "FAIL: ${label}: failure output does not mention '${pattern}':" >&2
    echo "${output}" >&2
    failures=$((failures + 1))
  fi
}

headings=(
  "Status"
  "Motivation"
  "Public Interfaces"
  "Proposed Changes"
  "Compatibility, Deprecation, and Migration Plan"
  "Test Plan"
  "Rejected Alternatives"
)

reset_copy
expect_pass "checked-in tree"

# Deleting a heading from any KFC fails the check.
for path in "${source_dir}"/KFC-*.md; do
  name="$(basename "${path}")"
  for heading in "${headings[@]}"; do
    reset_copy
    grep -v -x -F -- "## ${heading}" "${path}" >"${tmp}/KFCs/${name}"
    expect_fail "${name} without '## ${heading}'" "${name}: level-two headings"
  done
done

# Swapping two headings fails the check, because order is part of the rule.
reset_copy
first="$(basename "$(ls "${source_dir}"/KFC-*.md | head -n 1)")"
sed -i \
  -e 's/^## Motivation$/## PLACEHOLDER/' \
  -e 's/^## Public Interfaces$/## Motivation/' \
  -e 's/^## PLACEHOLDER$/## Public Interfaces/' \
  "${tmp}/KFCs/${first}"
expect_fail "${first} with Motivation and Public Interfaces swapped" "${first}: level-two headings"

# A Status value that disagrees with the README index row fails the check.
reset_copy
sed -i '0,/^\*\*[A-Za-z ]*\.\*\*/s//**Withdrawn.**/' "${tmp}/KFCs/${first}"
expect_fail "${first} with Status 'Withdrawn'" "${first}: Status section says 'Withdrawn'"

# The same mismatch seen from the README side.
reset_copy
sed -i "s/^\(| \[KFC-[0-9]*\](${first}) |[^|]*|\) [^|]* |\$/\1 Withdrawn |/" "${tmp}/KFCs/README.md"
expect_fail "README row for ${first} says 'Withdrawn'" "the README index says 'Withdrawn'"

# A KFC file with no index row fails the check.
reset_copy
cp "${tmp}/KFCs/${first}" "${tmp}/KFCs/KFC-999-unindexed.md"
expect_fail "unindexed KFC" "KFC-999-unindexed.md: no row in the README index"

if [[ "${failures}" -ne 0 ]]; then
  echo "check-kfcs-test: ${failures} failure(s)" >&2
  exit 1
fi

echo "check-kfcs-test: all cases passed"
