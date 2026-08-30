#!/usr/bin/env bash
set -euo pipefail

target="${1:-//crates/raft:raft_mutants}"
baseline=".cargo/mutants-baseline.txt"
logs="bazel-testlogs/crates/raft/raft_mutants"
actual="$(mktemp)"
trap 'rm -f "${actual}"' EXIT

status=0
bazel test "${target}" --test_output=errors || status=$?

mapfile -t shard_logs < <(find "${logs}" -name test.log -type f | sort)
test "${#shard_logs[@]}" -eq 16
test "$(grep -El '^[0-9]+ mutants: ' "${shard_logs[@]}" | wc -l)" -eq 16
grep -h '^MISSED ' "${shard_logs[@]}" |
  sed 's/^MISSED //' | sort -u >"${actual}"
diff -u "${baseline}" "${actual}"

# A nonzero Bazel result is expected only for the reviewed survivors above.
if ((status != 0)) && [[ ! -s "${actual}" ]]; then
  exit "${status}"
fi
