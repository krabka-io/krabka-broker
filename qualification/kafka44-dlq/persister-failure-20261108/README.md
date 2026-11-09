# Live RF3 phase-two persister transport failure (2026-11-08)

This schedule extends the earlier source-crash receipt by waiting for an actual
phase-two persister failure **before** killing the source leader. It is not a
synthetic state-machine failure, storage EIO, or application-level state-write
refusal.

The loopback runner creates fresh, private three-node clusters, one source
partition, one destination partition and one share-state partition, each RF3,
minISR2. Source leader is node 1, destination leader node 2, state leader node 3.
It holds the successful DLQ Produce response after append; confirms one intact
DLQ copy and a durable ARCHIVING row; pauses nodes 2 and 3; then releases the
buffered success response. The state persister cannot reach its paused leader.
Only after observing a real source-broker persistence transport failure does it SIGKILL
node 1, resume nodes 2/3 and restart node 1. The recovered client polls for 15s
and verifies the intact DLQ copies and lag zero. Native observed two copies;
oracle observed one. The timed-out oracle write was still buffered at the
paused state leader and committed after that leader resumed. Its ARCHIVED row
is timestamped after source kill, followed by a leader-epoch-only empty delta.
The verifier replays that single-offset delta instead of incorrectly treating
the latest empty state-batch update as an empty persisted state. Replica dumps are checked
separately, since a runner PASS alone is not a replica-state assertion.

## Reproduction

Use retained Kafka 4.4 jars, the pinned Kafka image recorded in the receipt,
and a native broker/formatter built from the recorded candidate revision:

```sh
python3 qualification/kafka44-dlq/persister-failure-20261108/run.py \
  --run /absolute/fresh/private/directory \
  --libs /absolute/kafka-4.4/libs \
  --broker /absolute/krabka-broker --formatter /absolute/krabka-format
```

Ports 64801–64820 and 64901–64920 must be free. The runner uses a fresh path,
creates only loopback listeners and owns all processes/containers it stops.
Its finally block resumes paused nodes before shutting down. It does not delete
data or containers. Python, Java 21+, Docker and the pinned image are required.

## Scope

The immutable candidate binary is from `c51d444b3`, not a freshly built combined
tree. Its manager dead-letter and persistence files match the integrated tree;
this does not make its executable a combined-tree build. The bounded model is
separate evidence. Native's two copies demonstrate replay; oracle's one copy demonstrates ambiguous
completion of a timed-out state RPC. Neither is evidence of
exactly-once behavior. One record/partition, one quorum outage and one source
kill per peer do not qualify arbitrary schedules or power-loss durability.
The outage also pauses the controller quorum; this is not an isolated storage
failure. Native's observed failure is during persister connection/API negotiation,
not a negative `WriteShareGroupState` response. Oracle is killed after the actual
`WRITE_SHARE_GROUP_STATE` request timeout, while its higher-level retry loop may
still be pending; native is killed after its persister returns the connect error.
These are explicitly different transport-failure cut points, not matching final
callback semantics. An initial retained pilot waited for the oracle final
callback and was interrupted after observing RPC timeout; it is not a passing
paired run. Final rows, timings, hashes and
observed peer outcomes are in the receipt and retained logs.

The original fixed-two-copy oracle assertion failed and is retained as
`oracle-verify-expected2-failed.txt`. An owned all-node restart followed by the
exact one-copy verifier passed. The harness now accepts oracle one or two copies
after recovery, records the observed count, and still requires native replay2.

The corrected harness was syntax-checked, not rerun end-to-end after its count
expectation fix. The retained final run was completed stepwise: original fault
injection and recovery; original two-copy assertion failure; replica decoding;
then owned restart, live passthrough gate, rejoin/15s empty polling, and exact
one-copy/lag-zero verification. Logs preserve that distinction. Earlier failed
postrestart probes ran without a live advertised gate and are retained privately;
they are not successful broker assertions.

## Later rerun

[2026-11-09 combined-tree evidence](../combined-persister-20261109/README.md) completes the corrected harness end-to-end. Scripts here now use unique container names and validate the observed permitted oracle count. This directory’s original receipt hashes describe the historical script versions at `86afb6fdd`; new script hashes are retained in the later receipt. Historical results and limitations above are preserved.
