#!/usr/bin/env bash
set -euo pipefail

target="${1:?usage: check-mutants.sh TARGET SHARD_INDEX SHARD_COUNT}"
shard_index="${2:?usage: check-mutants.sh TARGET SHARD_INDEX SHARD_COUNT}"
shard_count="${3:?usage: check-mutants.sh TARGET SHARD_INDEX SHARD_COUNT}"

# The pinned rules_rs_mutants runner reads Bazel's standard test-sharding
# variables even when launched with `bazel run`.
export TEST_SHARD_INDEX="${shard_index}"
export TEST_TOTAL_SHARDS="${shard_count}"
exec bazel run --test_sharding_strategy=disabled "${target}"
