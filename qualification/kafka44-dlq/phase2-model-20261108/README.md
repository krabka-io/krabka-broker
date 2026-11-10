# DLQ phase-2 persistence crash schedules

This is a bounded production-acquisition-machine regression, not another live
Kafka differential run. Baseline: `234dadc776e574fd91b0ec9c4d2d84cfa1a0c33e` (origin/main when the work began).
No runtime behavior, wire encoding, persisted format or feature level changes.

The previous `dlq_model` combined local ARCHIVED completion with cache pruning,
then confirmed persistence. That represented the ordering before the production
manager's persist-before-prune correction. The model now follows
`SharePartitionLeaderManager::dead_letter`: complete ARCHIVED, attempt its
persistence, then prune the terminal prefix after either outcome.

The new schedules distinguish these cases:

| Schedule | Expected recovery |
| --- | --- |
| Successful append; ARCHIVED persistence fails; crash | Durable ARCHIVING is replayed; a second DLQ copy is possible. |
| Terminal authorization denial after a retry; ARCHIVED persistence fails; crash | Durable ARCHIVING is replayed, even though the live cache had advanced. |
| Successful append or terminal denial; ARCHIVED persistence confirms; crash | No DLQ requeue; the terminal source range is not redelivered. |

The graph requires witnesses for authorization denial after retry with zero
appends, failed-persistence recovery, duplicate copies after failed persistence,
and successful durable completion across reload. It checks that pruning occurs
only after a persistence attempt and retains the lost-recovery mutation test.
Two deterministic tests pin the failed/confirmed persistence cuts for both
successful and terminal-denied destination outcomes.

Authorization denial here is an **abstract terminal transport outcome**; the
model does not execute the ACL evaluator, requests, a multi-broker persister,
real destination writes, or power-loss durability. The existing
[RF3 denial receipt](../rf3-denial-20261107/README.md) and
[append-before-confirmation witness](../README.md#actual-append-before-archived-source-crash)
remain the live evidence. The added model must not be presented as wire or RF3
fault/ACL parity. Lost confirmations and failed phase-2 persistence can create
duplicates; terminal errors can archive without a DLQ copy. **No exactly-once
archival guarantee.**

## Reproduction

From the repository root:

```console
bazel test //crates/broker:broker_test --test_arg=share_partition::state::dlq_model --jobs=2 --local_resources=memory=8192
```

This executes the graph and cut-point tests against the production acquisition
machine. The graph bounds remain depth 32 and 10,000 generated states, with one
source offset, two attempts per recovered operation and one crash. Exhaustion
within these bounds is not an arbitrary-schedule or liveness proof.

## Observed verification

All four targeted tests passed. The normal graph exhausted 128 unique / 185
generated states, maximum depth 11. The lost-recovery mutant exhausted 46 unique
/ 67 generated states, maximum depth 8; its two expected invariants fail at
`[Persist, Crash]` (depth 2). See [raw output](test-output.txt) and
[source/binary identity receipt](receipt.json). Targeted nightly rustfmt and
`git diff --check` passed. No full broker suite or broad Clippy run is claimed.
