# Barrier Markers Design

A log-embedded primitive that gives a Kafka cluster an exact, durable, and
reproducible cut across a declared set of topics.

## Design Goals

Kafka has never had a consistent cut across topics. An operator who needs "the
state of all inputs at one point" has to guess with record timestamps.
Timestamps are approximate, they move with clock skew, and two runs of the same
recovery procedure do not agree. Disaster-recovery replay points, dual-run and
shadow tests, cross-topic state reconstruction, and audit snapshots all suffer
from the same gap.

This subsystem gives that cut a name and an exact position. A coordinator puts
an epoch-stamped **barrier marker** into every partition of a named **barrier
group**. The offset of epoch N's marker in each partition defines cut N. The
design has four goals.

1. **Exactness.** A cut is a set of offsets, not a timestamp range. Two replays
   of cut N read the same records.
2. **Invisibility.** An ordinary Kafka consumer must not see a barrier marker
   and must not change its behaviour because of one.
3. **Portability.** A client in any language must be able to read a cut. It must
   not need a new RPC, and it must not need to parse the log.
4. **Durability.** A cut survives broker restart, leader failover, log
   compaction, and replication.

The subsystem exists in this shape, and not as a simpler timestamp index,
because only a record in the log gives property 1. An index outside the log
cannot say which record came first when two records share a millisecond.

## Architecture Overview

Four pieces work together.

**The marker.** A Kafka control batch with one record. The broker writes it
through the same partition writer that serves every produce request, so the
marker takes a real offset and holds a fixed place in the partition order.

**The coordinator.** A broker-side component with the same shape as
`TxnCoordinator` and the share coordinator. It owns the group definitions and
the epoch counter. It keeps its state in an internal topic, and it recovers
that state from the topic at startup.

**The injection.** The coordinator picks the next epoch, freezes the target
set, and writes a marker into every target partition. Partitions that another
broker leads get an inter-broker request. The coordinator collects the offset
that each append returned.

**The manifest.** The coordinator writes the collected offsets back to the
internal topic as a cut record. Any Kafka consumer can read that topic. This is
the portable read path of goal 3.

```
 trigger ──► BarrierCoordinator ──► inject markers into every group partition
                    │                          │
                    │                          └──► offsets come back
                    ▼
              __barrier_state  ◄── group definitions, injection starts, cuts
                    │
                    └──► any Kafka consumer reads cut N
```

## What This Guarantees, and What It Does Not

This section is the contract. Read it before you build on the primitive.

### Guaranteed

- Epoch N's marker sits at exactly one offset in each partition of the group.
- The marker holds a total order against every other append to that partition.
  Records before it are before the cut. Records after it are after the cut.
- Every partition in the group gets the same epoch.
- The cut is durable. It survives restart, failover, and replication.
- Compaction keeps the markers.
- A replay from cut N always reads the same set of records.

### Not guaranteed

A cut that a broker injects is **not** causally consistent across independent
producers. Here is the failure case. A producer writes to topic A after epoch
N's marker lands in A. The same producer then writes to topic B before epoch
N's marker lands in B. Its second write falls before the cut. Its first write
falls after the cut. The cut inverts the producer's own order.

True Chandy-Lamport consistency needs the markers to travel along the channels
that connect the processes. A broker that injects into every partition cannot
supply that property to a producer it does not control.

### What this supports

The guarantee is enough for every case that made an operator reach for a
timestamp: disaster-recovery replay points, audit snapshots, shadow-run
alignment, and stream-processor checkpoints. Each of those needs an exact,
reproducible, agreed cut. None of them needs cross-producer write causality.

The guarantee is not enough to answer "did this producer's write to A happen
before its write to B" from the cut alone.

## Key Design Decisions

### The marker is a Kafka control record

A control batch carries the control bit in its attributes. Every Kafka consumer
skips a control batch. The JVM `KafkaConsumer` drops it before the application
sees it. `crabka-client-core` and `crabka-client-consumer` do the same. The
consumer still advances its position past the marker's offset, so no consumer
stalls and no offset arithmetic breaks.

That gives goal 2 for free. A topic that carries barrier markers behaves the
same as one that does not, for every client that does not ask about them.

The alternative was a marker in a side topic. That was rejected: a side topic
cannot say which record in the data partition came first.

### The control-record type code starts a krabka-private range

Kafka assigns control-record types 0 to 6: `ABORT`, `COMMIT`, `LEADER_CHANGE`,
`SNAPSHOT_HEADER`, `SNAPSHOT_FOOTER`, `KRAFT_VERSION`, and `VOTERS`. The barrier
type is 1000. It starts a krabka-private range that Kafka cannot reach by
normal growth.

This mirrors the api-key convention that `crates/raft/src/wire.rs` already
uses, where krabka-private RPCs sit at 1003 and 1004.

### The marker carries no producer id

The marker sets `producer_id` to -1 and clears the transactional bit. That is
one decision with three consequences, and each one is a property the design
wants.

- The log's transaction bookkeeping ignores the marker. Last-stable-offset,
  the transaction index, and the producer state stay untouched.
- `crates/verified/src/compaction.rs::retain_decision` returns `Keep` for a
  control batch whose producer is not transactional. Compaction keeps every
  barrier marker for the life of the log.
- A `read_committed` consumer treats the marker as it treats any control batch
  and does not confuse it with a transaction outcome.

The compaction result has a cost. Markers on a compacted topic accumulate, and
nothing removes them. The design accepts that cost. A cut point that
disappeared would break goal 4, and a marker is 60 bytes.

### The log knows the barrier type explicitly

`Log::append_preserving_offset` handles a control batch. Before this change it
assumed that every control batch was a transaction marker. A barrier marker
with `producer_id` of -1 passed through that code without harm, but only by
accident.

The log now names the barrier type and does nothing for it. The two recovery
paths, `apply_recovered_batch_state` and `rebuild_pending_stamp_ranges`, agree.
A test states the property directly, so a later edit to the transaction path
cannot break barriers in silence.

### Cuts go to a topic, not only to an RPC

The control plane needs request and response semantics, so it uses RPCs. The
read path does not. Any Kafka consumer can read an internal topic, and no JVM
`AdminClient` can send a krabka-private api key.

So the coordinator publishes each cut to `__barrier_state`, and the three
krabka streams libraries read cuts with a plain assign-seek-poll loop. The
markers in the log stay the durable ground truth for a recovery tool.

### A partial cut is published, not hidden

An injection can fail to reach every partition. A leader can be down, or an
election can be in flight past the deadline.

The coordinator writes the cut with a partial status and names the partitions
that got no marker. It consumes the epoch either way, and it never reuses one.

The alternative was to hide a failed injection. That was rejected. A consumer
that already saw epoch N's marker on some partitions would wait for markers
that never arrive. A published partial cut lets every reader skip that epoch.

### The target set is frozen before the first append

The coordinator writes an injection-start record before it appends any marker.
That record names the group's topics and each topic's partition count, as the
metadata image reported them at that instant.

This decides three questions at once. An edit to the group's topic set applies
from the next epoch. A partition count change applies from the next epoch. A
coordinator that crashes mid-injection finds a fixed, recoverable target set.

The shape copies the `PrepareCommit` and `PrepareAbort` records that
`TxnCoordinator` writes before it dispatches transaction markers.

## Wire Formats

These formats are frozen. The broker, `krabka-streams-rs`,
`krabka-streams-java`, and `krabka-streams-go` all implement them.

A `string` is an `i16` byte length and then UTF-8 bytes. All integers are
big-endian. An `i32` count precedes each array.

### Barrier marker

The key follows Kafka's control-record key layout.

```
key:
  version i16 = 0
  type    i16 = 1000

value:
  version      i16 = 0
  group        string
  epoch        i64
  triggered_at i64          milliseconds since the Unix epoch
```

The batch holds one record. `producer_id` is -1, `producer_epoch` is -1, and
`base_sequence` is -1. The attributes set the control bit and clear the
transactional bit.

### Internal topic records

`__barrier_state` carries three record kinds. The key discriminates them.

```
key:
  version  i16 = 0
  kind     i16               0 group, 1 injection start, 2 cut
  group    string
  epoch    i64               -1 for kind 0
```

A group record with a null value is a tombstone, and it deletes the group.

```
group value:
  version       i16 = 0
  topics        i32 [ topic string ]
  interval_ms   i64           -1 turns off periodic injection
  retained_cuts i32
  last_epoch    i64

injection-start value:
  version           i16 = 0
  coordinator_epoch i32
  triggered_at      i64
  targets           i32 [ topic string | partition_count i32 ]

cut value:
  version      i16 = 0
  triggered_at i64
  completed_at i64
  status       i8            0 complete, 1 partial
  topics       i32 [ topic string | partitions i32 [ partition i32 | offset i64 ] ]
  missing      i32 [ topic string | partition i32 ]
```

The `missing` array repeats what the frozen target set implies. It is present
so that a reader can classify a partial cut from one record.

### Snapshot container

The three streams libraries share one container for the state that a task
snapshots at a cut. The layout is the one that
`krabka-streams-go` already uses in `columnar/statestore.go`, and that
`FileColumnarStateStore` mirrors in `krabka-streams-java`.

```
version u32 = 1
count   u32
entries count x [ name_len u32 | name UTF-8 | value_len u32 | value bytes ]
```

Entries are in ascending byte order of `name`, so the bytes are deterministic.

The payload inside each entry stays language-specific. `PARITY.md` in
`krabka-streams-go` already records that divergence. The cut identity is not in
the container. It is the storage key, which is the task, the group, and the
epoch. A reader that needs the cut offsets reads them from the manifest.

## Integration

**The log.** `crates/log` learns the barrier control type and states that the
type changes no producer state and no transaction state.

**The partition writer.** Injection calls `Partition::produce_batch`, which
returns the assigned base offset. That offset is the manifest entry. The
partition writer keeps the marker ordered against every produce and replication
append.

**The KRaft controller.** The coordinator reads the metadata image to find the
group's topics, their partition counts, and each partition's leader. It creates
`__barrier_state` with the same `submit_change` path that the transaction and
share coordinators use for their topics.

**The replicator supervisor.** `reconcile` refreshes the coordinator's set of
led partitions, beside the same call for the transaction and share
coordinators.

**Inter-broker traffic.** A partition that another broker leads gets a
`WriteBarrierMarkers` request. The fan-out copies `dispatch_markers`, but it
collects the returned offsets, which the transaction path does not need.

**Clients.** The three streams libraries read cuts from `__barrier_state` with
an interface each one already has. No library needs a new I/O seam.

## Kafka / KIP Compliance

Barrier markers are a krabka extension. No KIP defines them.

The extension holds Kafka's rules where they apply.

- The marker is a valid Kafka control batch. Its key follows the
  `version i16, type i16` layout that Kafka uses for every control record.
- The broker rejects a control batch that a client sends, as it did before.
  Only the coordinator injects markers.
- Down-conversion for a Fetch below v4 drops control batches, so a v0 or v1
  consumer never receives a marker.
- A share consumer never receives a marker, because share-fetch archives
  control batches.
- The control-plane api keys sit at 1000 and above, so a future Kafka api-key
  assignment cannot collide with them.

The deliberate divergence is the control-record type code 1000. Kafka's
`ControlRecordType.parse` returns an unknown result for it and skips the batch,
which is the behaviour this design needs.

## Testing

The Kafka-compatibility claim is the one that a reader should not take on
trust. `crates/broker/tests/jvm_barrier_markers.rs` boots a real JVM consumer
against a krabka broker whose topics carry barrier markers. It asserts that the
consumer reads exactly the data records, in order, and that its position moves
past the marker offsets.

The other suites cover cut exactness across a multi-broker cluster, coordinator
recovery after restart, and a barrier that a broker injects in the middle of an
open transaction.
