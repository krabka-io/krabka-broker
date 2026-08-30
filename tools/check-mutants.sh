#!/usr/bin/env bash
set -euo pipefail

target="${1:-//crates/raft:raft_mutants}"
baseline=".cargo/mutants-baseline.txt"
logs="bazel-testlogs/crates/raft/raft_mutants"
actual="$(mktemp)"
trap 'rm -f "${actual}"' EXIT

status=0
bazel test "${target}" --test_output=errors || status=$?

# Bazel 3 is a completed test run with test failures (the reviewed survivors).
# Preserve build, infrastructure, and interrupted-run failures.
if ((status != 0 && status != 3)); then
  exit "${status}"
fi

mapfile -t shard_logs < <(find "${logs}" -name test.log -type f | sort)
test "${#shard_logs[@]}" -eq 16
test "$(grep -El '^[0-9]+ mutants: [0-9]+ caught, [0-9]+ missed, [0-9]+ unviable$' "${shard_logs[@]}" | wc -l)" -eq 16
test "$(grep -El '^cargo_mutants: [0-9]+ mutants survived$' "${shard_logs[@]}" | wc -l)" -eq 16
grep -h '^MISSED ' "${shard_logs[@]}" |
  sed 's/^MISSED //' | sort -u >"${actual}"
diff -u "${baseline}" "${actual}"
