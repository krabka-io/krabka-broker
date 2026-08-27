# KFC-4: Consistent Cross-Topic Snapshots

A coordinator writes epoch-stamped barrier markers into every partition of a declared topic set, so that "the state of all inputs as of cut N" names one exact position in each of them.

## Status

**Adopted.** The implementation is the `barrier` module of `crabka-broker`, the `crabka-barrier` crate and its binary, and the barrier control-plane messages in `krabka-protocol`. It merged in [#2](https://github.com/krabka-io/krabka-broker/pull/2). This document lands on branch `claude/kfc-4-cross-topic-snapshots-0pjllz`.

No KIP defines a cross-topic cut, so this document is the specification for it. The design is the marker half of the Chandy-Lamport snapshot algorithm, which Apache Flink uses for its own checkpoints. The difference is where the marker lives. Flink injects a barrier into a dataflow that Flink controls, and this design injects one into a Kafka log that any client can read.

## Motivation

Kafka orders records inside one partition and nowhere else.

That single guarantee is the whole basis of every consumer, every connector and every stream processor. An offset is a durable name for a position, so two readers of one partition agree about what came before offset N and what came after it. Across two partitions there is no such name, and across two topics there is no such name either. Kafka has never offered one.

Four jobs need one, and all four are ordinary.

**Disaster-recovery replay.** An operator restores a cluster or fails over to a second one, and every consumer of every topic has to restart from a position the operator can state. Positions chosen per topic do not compose. A downstream join that consumed `orders` to one point and `payments` to another produces a result that neither topic alone explains.

**Dual-run and shadow testing.** A team runs a new version of a service beside the old one over the same inputs, and compares the outputs. The comparison is only meaningful when both runs saw the same input prefix. Without a cut, "the same input prefix" is not a statement anyone can check.

**Cross-topic state reconstruction.** A projection reads several topics and holds derived state. Rebuilding that state, or auditing it, needs the inputs replayed to a point where the state is well defined. Every topic has to stop at a position that belongs to the same point.

**Audit snapshots.** A regulator or an internal control asks what the system held at a stated moment. The answer has to be reproducible. Two people who ask the same question a month apart must get the same answer.

Every one of these falls back to a timestamp today, and a timestamp is the wrong instrument for three separate reasons.

A record timestamp is not a position. `offsetsForTimes` reads the time index and returns the first offset whose timestamp is at or above the target, which is an approximation over a sparse index. It is also not stable. Retention deletes segments, and the same query against the same cluster on a later day returns a different offset for the same instant.

A record timestamp is not monotonic within a partition. `message.timestamp.type` defaults to `CreateTime`, so the value is the producer's clock, and a partition holds records whose timestamps go backwards. `LogAppendTime` fixes the ordering and loses the fact the producer recorded. Neither setting gives a position.

A record timestamp is not comparable across producers. Two producers on two hosts have two clocks, and clock skew is exactly the size of the window a cut has to be exact about. A snapshot built from timestamps is correct up to the skew, and nothing in Kafka bounds the skew.

The broker is the only place that can fix this. A cut has to name a position in the total order of a partition, and only the writer of that total order can place something at a position of its choosing. A client cannot do it. A client can produce a record, and a produced record is a record: it lands in the topic, every consumer sees it, and every downstream schema has to know about it. A marker has to be at a position without being content.

Kafka already has the mechanism, and has had it since transactions landed. A control batch holds an offset in the log and no Kafka consumer ever returns it to an application. The transaction markers use this. A barrier marker is a second use of it, with a control type of its own.

The primitive is worth building in the broker rather than in one stream processor, because four independent readers want the same cut. `krabka-streams-rs`, `krabka-streams-java` and `krabka-streams-go` each read the published cuts, and an operator reads them from the command line. A cut computed inside one library is not available to the other three.

A barrier needs a KFC by the test in the [README](README.md). A stock Kafka client can tell the difference. Offsets advance in a topic without the consumer seeing a record, and the log end offset the JVM tooling reports exceeds the number of records that were produced. No KIP explains why.

### What a Cut Guarantees, and What It Does Not

This section states the guarantee before the design, because the design is only worth reading if the guarantee is the one a reader needs.

A cut of epoch N gives four properties.

It is **exact**. Epoch N's marker sits at one offset in each partition of the group, totally ordered against every other append to that partition. A record is before the cut or after it, and never both.

It is **uniform**. Every partition of the group takes the same epoch, so "cut 7" names one thing across the whole set.

It is **durable**. The marker is a batch in the log. It survives broker restart and leader failover, replication carries it to every follower, and log compaction keeps it.

It is **reproducible**. The cut is a list of offsets. Two readers who replay to cut 7 a month apart replay to the same records, because an offset does not move.

A cut is **not causally consistent across independent producers**. A producer can write to topic A after A's marker lands, and then write to topic B before B's marker lands. Its second write falls before the cut and its first falls after it. Chandy-Lamport consistency needs the markers to travel along the channels between the processes, and a broker cannot put a marker into a channel it does not own. The producer is that channel.

The four jobs above need those four properties and do not need causal consistency. Reasoning about cross-topic write causality does need it, and this primitive does not supply it. The [Rejected Alternatives](#true-causal-consistency-through-producer-side-markers) section says why nothing here can.

## Public Interfaces

The feature adds five API keys, one error code, one internal topic, one control-record type, eight broker configs, seven metric families and one command.

### The Marker Control Record

A marker is a Kafka control batch that holds one record. The batch clears the transactional attribute bit and keeps `producer_id` -1, `producer_epoch` -1 and `base_sequence` -1.

The record key follows Kafka's control-record layout. The type code is 1000, beyond Kafka's assigned range of 0 to 6.

```text
key:
  version i16 = 0
  type    i16 = 1000

value:
  version      i16 = 0
  group        string          i16 byte length, then UTF-8
  epoch        i64
  triggered_at i64             milliseconds since the Unix epoch
```

Every Kafka consumer drops a control batch, so no application sees a marker. The offset it holds is the only part a client observes.

### The `__barrier_state` Topic

`__barrier_state` is a compacted internal topic. It carries three record kinds, and the key names which kind a record is. A group record holds a group definition. An injection-start record freezes the target set of one injection. A cut record holds the published offsets of one epoch.

```text
key:
  version i16 = 0
  kind    i16                0 group, 1 injection start, 2 cut
  group   string
  epoch   i64                -1 for a group record

group value:
  version       i16 = 0
  topics        i32 [ topic string ]
  interval_ms   i64          -1 turns off periodic injection
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

A group record with a null value is a tombstone, and it deletes the group.

This topic is a public interface and not an implementation detail. It is the read path that every non-Rust client uses, because the barrier API keys are krabka-private and no JVM `AdminClient` can send them. The layout is deliberately plain for that reason. Every integer is big-endian, a string is an `i16` byte length and then UTF-8 bytes, and an `i32` count precedes every array. There are no compact lengths and no tagged fields, because `krabka-streams-java` and `krabka-streams-go` decode a cut record by hand.

### The API Keys

| Key | Request | Purpose |
| :--- | :--- | :--- |
| 1010 | `AlterBarrierGroups` | Create, update and delete groups. |
| 1011 | `DescribeBarrierGroups` | Read the group definitions. |
| 1012 | `TriggerBarrier` | Make one cut on demand. |
| 1013 | `ListBarrierCuts` | Read the cuts a group retains. |
| 1014 | `WriteBarrierMarkers` | Append markers into locally-led partitions. |

Keys 1010 to 1013 are a control plane that an operator drives. Key 1014 is inter-broker traffic that one coordinator sends to the leader of a target partition.

All five sit in the krabka-private range at 1000 and above, and all five speak version 0 only, with flexible framing. The broker registers them for dispatch and never advertises them, so `kafka-broker-api-versions.sh` prints no row for them. [Compatibility](#compatibility-deprecation-and-migration-plan) says why.

`TriggerBarrier.timeout_ms` and `AlterableBarrierGroup.interval_ms` both spell a duration in milliseconds. `ListBarrierCuts.max_results` takes -1 for every retained cut, which is the request default, so 0 asks for none.

### Authorization

| Key | Operation | Resource |
| :--- | :--- | :--- |
| 1010, 1012 | `Alter` | `Cluster("kafka-cluster")` |
| 1011, 1013 | `Describe` | `Cluster("kafka-cluster")` |
| 1014 | `ClusterAction` | `Cluster("kafka-cluster")` |

A denied request gets `CLUSTER_AUTHORIZATION_FAILED` (31) in every row of the response, or in the top-level field where the response has no rows. Kafka gates inter-broker traffic on `ClusterAction`, and key 1014 follows that.

### The New Error Code

`BARRIER_INJECTION_IN_PROGRESS` (1000) says that an injection for this group is already running. The coordinator runs one injection per group at a time. A caller should retry after a brief back-off.

Every other outcome maps onto a code Kafka already defines.

| Condition | Code |
| :--- | :--- |
| An injection is already in flight | `BARRIER_INJECTION_IN_PROGRESS` (1000) |
| Another broker coordinates the group | `NOT_COORDINATOR` (16) |
| The coordinator lost the group mid-injection | `NOT_COORDINATOR` (16) |
| No group of that name is live | `RESOURCE_NOT_FOUND` (91) |
| A group of that name is already live | `TOPIC_ALREADY_EXISTS` (36) |
| A setting is out of range | `INVALID_CONFIG` (40) |
| The coordinator cannot serve the group now | `COORDINATOR_NOT_AVAILABLE` (15) |
| The receiver of key 1014 does not lead the partition | `NOT_LEADER_OR_FOLLOWER` (6) |
| The receiver of key 1014 has a stale leader epoch | `FENCED_LEADER_EPOCH` (74) |

`TOPIC_ALREADY_EXISTS` is the only already-exists code Kafka gives, and a barrier group is not a topic. The alternative was a second krabka-private code, and reusing one Kafka code that a caller can already read is worth more than an exact name.

A malformed topic list is refused with `INVALID_REQUEST` before the coordinator sees the entry, so what reaches `INVALID_CONFIG` is only a retention or an interval out of range.

Every response carries its outcome as a code in a row or in a top-level field, and never as a transport failure. A caller reads one response shape whatever happened.

### Broker Configuration

| Key | Default | Meaning |
| :--- | :--- | :--- |
| `barrier_state_num_partitions` | 50 | Partition count of `__barrier_state`. |
| `barrier_state_replication_factor` | 3 | Replication factor of `__barrier_state`. |
| `barrier_injection_timeout` | 30s | Deadline for one injection to reach every target partition. |
| `barrier_recovery_read_max` | 1 MiB | Bytes per `__barrier_state` recovery read. |
| `barrier_retained_cuts` | 100 | Cuts a group keeps when the caller names no value. |
| `barrier_min_injection_interval` | 1s | Shortest periodic interval a group may ask for. |
| `barrier_max_groups` | 100 | Largest number of barrier groups. |
| `barrier_max_topics_per_group` | 100 | Largest number of topics in one group. |

`barrier_state_num_partitions` fixes the group-to-partition map. A change moves every group to a new coordinator, so an operator should set it once.

The last three keys in that table are declared and are not enforced. No code path reads `barrier_max_groups`, `barrier_max_topics_per_group` or `barrier_min_injection_interval`. The coordinator's own `validate_spec` rejects an empty topic list, an empty topic name, a duplicate topic name, a `retained_cuts` below one, and an interval at or below zero, and it checks nothing against the operator's configured bounds. So a cluster accepts an unbounded number of groups, an unbounded topic list, and a one-millisecond injection interval.

This note is here because nothing in the repository will report it. The `BarrierGroupLabel` rustdoc states that metric cardinality is bounded by `barrier_max_groups` "because the coordinator rejects a new group past that cap", and no such rejection exists. Group name reaches a Prometheus label, so an unbounded group count is unbounded metric cardinality. Closing the gap is three checks in `validate_spec` against a `BarrierConfig` that carries the three values, and not a change to this design.

The retry back-off, its ceiling and the scheduler tick are not operator-tunable. They are compiled defaults of 100 ms, 5 s and 1 s.

### Metrics

| Family | Labels | Meaning |
| :--- | :--- | :--- |
| `barrier_epochs_started_total` | group | Injection-start records written. |
| `barrier_epochs_committed_total` | group | Complete cuts published. |
| `barrier_epochs_published_partial_total` | group | Partial cuts published. |
| `barrier_injection_duration_seconds` | group | Seconds from the start record to the cut. |
| `barrier_latest_epoch` | group | Epoch of the newest cut this coordinator published. |
| `barrier_markers_written_total` | topic | Markers this broker appended. |
| `barrier_groups_coordinated` | none | Groups this broker coordinates now. |

A partial cut is an outcome and not a failure, so it has a counter of its own rather than sharing one with the complete case. That split is what an alert reads.

`barrier_latest_epoch` moves when a cut is published and not when an injection starts. A started epoch that never publishes must not advance it.

A marker append that fails feeds no counter. Every call site logs the topic, the partition and the error at `warn`, and a run of failures shows up as a partial cut.

### The Command

`crabka barrier` is the operator's spelling, and the binary is `crabka-barrier`. The monorepo carries it as a subcommand of `crabka-cli`. That crate also drives the gres layer, which is why it could not follow the broker out.

The crate is a library as well as a binary, for the same reason `crabka-format` is one. A test that spawns the binary needs a Cargo working tree to build it from, and a Bazel test sandbox has none, so the tests call `run_from_args` in process.

| Subcommand | Key | Purpose |
| :--- | :--- | :--- |
| `define` | 1010 | Create a group, or update one that exists. |
| `delete` | 1010 | Delete a group and its cuts. |
| `describe` | 1011 | Print a group's definition and its latest epoch. |
| `trigger` | 1012 | Inject a cut now and print the offsets it took. |
| `list` | 1013 | Print a group's retained cuts. |
| `verify` | 1013 and `Fetch` | Read the log at a cut's offsets and prove each marker is there. |

`--bootstrap-server` is required and also reads `CRABKA_BOOTSTRAP_SERVER`.

A time flag takes any unit `crabka_units` accepts, so `500ms`, `30s`, `5m` and `1h` all work. A number with no unit is refused, and only zero is exempt. A `--timeout 30` from someone who meant milliseconds would otherwise wait thirty seconds without complaining.

| Exit code | Meaning |
| :--- | :--- |
| 0 | The broker accepted the request. |
| 1 | The broker refused the request. |
| 2 | A transport failure. Nothing is known about the outcome. |
| 3 | A cut whose log does not match what it claims. |

Code 3 is `verify` only, and it is the code a runbook branches on.

## Proposed Changes

### A Group, an Epoch, and a Cut

A barrier group is a named set of topics. An injection allocates the next epoch, writes an epoch-stamped marker into every partition of the group, and collects the offset each marker took. Those offsets are cut N. The coordinator publishes them as a cut record on `__barrier_state`, so an ordinary consumer reads the cut without any barrier API key.

The first injection allocates epoch 1. The broker spells never-injected as 0, and the wire field's own default is -1, so a reader treats any epoch at or below zero as none yet.

### The Marker Is a Control Batch, and Carries No Producer

A marker has to hold a position without being content. A control batch is the only thing in Kafka that does both, so the marker is one.

The batch keeps `producer_id` -1, `producer_epoch` -1 and `base_sequence` -1, and it clears the transactional bit. Those three values are load-bearing rather than incidental. They keep the marker out of the log's producer bookkeeping and out of its transaction bookkeeping, and they are what makes compaction keep it: `retain_decision` already keeps a control batch whose producer is not transactional, and the dedup map already skips a control key.

The consequence a client sees is that the isolation level has nothing to say about a marker. A `read_committed` consumer and a `read_uncommitted` consumer see exactly the same records, because a marker carries no producer id and belongs to no transaction.

The marker is stamped with its trigger time in both `base_timestamp` and `max_timestamp`. A zero timestamp would look older than the Unix epoch to time-based retention, and the segment that holds the marker could age out at once.

### Control Type 1000, and Making the Log Say So

Kafka assigns control types 0 to 6. `BARRIER_CONTROL_TYPE` is 1000, well clear of that range, so a barrier marker cannot be mistaken for a Kafka control record by type.

The log's append path needed a change to match. A control batch that reached `append_preserving_offset` was assumed to be a transaction marker: the code parsed the marker type, branched on ABORT and COMMIT, removed the producer from the pending map and updated the last stable offset. A barrier marker carries `producer_id` -1, so it passed through without harm. It passed through only because removing -1 from the map happens to be a no-op.

That is an accident, not a design. A later edit to the transaction path could have broken barriers in silence. `control_batch_kind` now classifies a control batch by its type rather than by its producer id, and the barrier arm advances the last stable offset through the same helper an ordinary non-transactional batch uses and then returns. Nothing below it reaches the pending map, the stamp ranges, the coordinator epochs, the transaction index or the producer state. The log's two recovery paths classify a batch the same way, because after a restart they are the only source of truth about what the log holds.

### An Internally Built Control Batch Needs Its Own Append Path

The injection cannot use the ordinary produce path, for two reasons that were already latent faults in the transaction markers.

The partition writer rewrites an owned batch's compression to the topic's `compression.type`. Kafka never compresses a control batch that arrived uncompressed. A topic whose `compression.type` is not `producer` would then get a compressed marker, which is not a shape Kafka writes. The default of `producer` is what kept this hidden.

The same path leaves `partition_leader_epoch` at the `RecordBatch` default of zero. The produce handler stamps the real epoch onto a client batch, and nothing stamped it onto an internally built one, so every marker header carried a false leader epoch.

`ProduceData::OwnedControl` skips the compression rewrite on the leader path and on the follower path, and `append_marker_and_materialize` stamps the partition's current leader epoch before it appends. The barrier markers take that path, and so do the transaction markers.

### The Coordinator Sits Where the Transaction Coordinator Sits

`murmur2(group) % num_partitions` maps a group name to one `__barrier_state` partition, and the broker that leads that partition coordinates the group. This is Kafka's `Utils.abs(murmur2(...)) % numPartitions` convention, the same one `__transaction_state` and `__share_group_state` use, including the JVM rule that `Integer.MIN_VALUE` maps to 0.

One partition per group is what gives the group's epochs a total order. Every group record, injection-start record and cut record of one group lands in one partition, so a single log decides which epoch came first. Two brokers never allocate the same epoch, because only one of them leads that partition.

The coordinator keeps one entry per group behind a mutex and holds that mutex for a whole injection. A scheduled tick, a triggered injection and a group edit serialise against each other. The mutex is the only concurrency device in the module.

An injection takes that mutex without waiting. A trigger or a scheduled tick that finds it held is refused with `BARRIER_INJECTION_IN_PROGRESS`, and the caller can retry. Queueing a trigger behind a running injection would give the caller a cut it did not ask for, at a time it cannot predict.

A group edit waits for the mutex instead of being refused, because an edit has one correct outcome and a queued edit still reaches it. The running injection finishes over the set it froze, and the edit applies to the injection after it.

### The Injection Protocol

1. Refuse when this broker does not coordinate the group, and refuse when the group entry is busy.
2. Allocate the next epoch, and write the injection-start record before the first marker append.
3. Freeze the target set in that record.
4. Fan the markers out over the frozen set, grouped by current leader.
5. Retry the partitions that carry no marker, up to the deadline.
6. Write the cut record.
7. Tombstone the epoch that leaves the retention window.

### The Start Record Comes Before Any Marker

Step 2 is the ordering that makes the protocol safe across a crash.

The injection-start record is durable in `__barrier_state` before a single marker exists in any data topic. A coordinator that dies after writing markers and before writing the cut leaves a record that says epoch N started. The next coordinator reads it, so epoch N is never allocated a second time.

Without that record the failure is silent and bad. A coordinator would write markers for epoch N, die, restart, read the last published cut as epoch N-1, and allocate N again. Two disjoint sets of markers in the log would then both claim epoch N, and no reader could tell which one a cut of that epoch names.

### The Target Set Is Frozen, So Edits Apply From the Next Epoch

Step 3 records the topics of the group and the partition count of each one, as they stood when the injection started.

The set has to be frozen because a partition count can grow while an injection runs. An injection that read the count twice could mark partitions 0 to 3 and then publish a cut over partitions 0 to 4, naming a partition it never marked. Freezing makes the cut a statement about a set the coordinator committed to before it started.

The visible consequence is that a topic-set edit and a partition-count change both apply from the next epoch. A group edited during an injection cuts the old set once more and the new set after that. This is the right way round: a cut belongs to the definition that was live when it started.

A group may name a topic that does not exist yet. The coordinator resolves the target set at each injection and not at definition, so a group defined ahead of its topics starts cutting them as they appear.

### The Fan-Out, and the Leg That Leaves the Broker

Step 4 groups the frozen targets by their current leader. A partition this broker leads takes `Partition::produce_control_batch` directly. A partition another broker leads takes `WriteBarrierMarkers`, api key 1014, sent to that leader through `InterBrokerClient` so that TLS and inter-broker SASL apply. The peer endpoint is resolved the way the transaction markers resolve one: the endpoint whose name matches the inter-broker listener, falling back to the record's top-level host and port.

The remote leg is not an optimisation. Without it a coordinator marks only the partitions it leads, every remote partition lands in the `missing` list, and a multi-broker cluster cannot make a complete cut at all.

Each requested partition carries the leader epoch the coordinator resolved when it froze the target set, and the receiver refuses a mismatch with `FENCED_LEADER_EPOCH`. The reason is the batch header. A marker stamps `partition_leader_epoch` into its own header, so a request built against a stale image would write a false epoch into the log. A -1 says the coordinator had no epoch, and the receiver does not fence on it.

The refusal is per partition. One request that names a led partition and an unled one marks the first and refuses the second, and the coordinator retries only what came back unmarked.

Step 5 retries the unmarked subset with a doubling back-off, from 100 ms up to 5 s. A leader that is down or mid-election is the common failure, and it usually resolves inside the deadline.

### A Failed Injection Publishes a Partial Cut

Step 6 publishes a cut whether or not every partition took a marker. A cut that missed partitions carries status `Partial` and names them in its `missing` list. The epoch is consumed either way.

The reason is that a marker cannot be withdrawn. Once a marker is in a partition's log it holds an offset there for good, and no API removes it. An injection that reached nine partitions of ten has already changed nine logs. Rolling the epoch back would leave nine markers of an epoch that officially never happened, and a later reader of those logs would find markers that no cut explains.

A partial cut is the accounting record for exactly that state. It says which partitions the epoch reached and which it did not, so every marker in the log belongs to a published epoch.

The reason is not that a client would otherwise be stranded. No client can see a marker, so none can wait for one.

### An Interrupted Injection Is Finalised as Fully Missing

Recovery replays every locally-led `__barrier_state` partition and folds the records into group entries. An injection-start record with no cut record after it is a pending injection, and the new coordinator finalises it.

It finalises it as a partial cut that names **every** frozen target as missing, because the coordinator that wrote the markers is the one that observed their offsets, and that coordinator is gone. The offsets are not recoverable from the start record.

This is the honest limitation of the recovery path. The markers may really be in the log, at offsets the published cut does not name, and the cut under-reports what the injection achieved. The epoch is still consumed and still accounted for, which is what the correctness argument needs, and the cut is unusable as a replay point. A caller that wanted a cut triggers the next epoch.

A coordinator that finds a pending injection frozen at a higher coordinator epoch than its own leaves it open and logs it. A newer coordinator owns that injection, and two coordinators must not both finalise one epoch.

### Old Cuts Leave Through Tombstones, Not a Log Trim

Step 7 tombstones the cut record of an epoch that falls out of the group's retention window, and the compaction of `__barrier_state` then removes it.

The share coordinator trims its log instead, and that does not work here for two reasons. Share state folds, so the newest record supersedes every older one and a trim loses nothing. Cuts do not fold. Cut 5 and cut 6 are two independent facts, and neither replaces the other. The group definitions also sit in the same key prefix, so a trim that removed old cuts would take the group definition with them.

A tombstone is per key, so it removes one cut and leaves the definition alone.

### The State Topic Is Created on First Use

`__barrier_state` is created when the first group is defined, and not at broker startup.

Eager creation was the first design and it was wrong. Every broker created the topic at startup with 50 partitions at replication factor 3, whether or not anything used barriers. A cluster that cannot satisfy that factor leaves all 50 partitions without a leader, and the leader-election sweep then walks them on every pass. That is enough metadata churn to change election timing, and it broke two JVM acceptance suites that have nothing to do with barriers.

A broker that never defines a group now carries no barrier state at all.

Lazy creation needs a wait that eager creation did not. Topic creation, leader assignment and opening the log locally are three separate rounds, and `is_coordinator_for` reads the leader set while the write path needs the partition open. `create_group` waits for both. Without the wait, a caller is told it is not the coordinator for a group it just asked to create.

### A Trigger Timeout Bounds the Retry, Not the Injection

`TriggerBarrier.timeout_ms` shortens how long the fan-out retries the partitions that carry no marker. A value at or below zero asks for the broker-wide `barrier_injection_timeout`, and a value above it is clamped to it, so a caller cannot hold a group's lock for longer than the operator allows.

The bound moves the fan-out deadline and never drops the injection future. Abandoning one would leave the epoch's injection-start record with no cut record after it, which is exactly the state a crashed coordinator leaves behind. A caller's impatience must not manufacture that state. A request that runs out of time gets a partial cut naming the partitions that took no marker.

### A Cut Is Worth What Its Markers Are

`crabka barrier verify` reads the log at each offset a cut names and checks that the batch there is a barrier control batch carrying that group and that epoch.

The subcommand exists because a cut record is just a list of integers. Nothing about the record proves the log agrees with it, and a cut used as a disaster-recovery replay point is trusted at the worst possible moment. Verify turns the claim into a check.

It cannot read through `crabka_client_core::fetch_partition`, because that drops control batches the way every Kafka consumer does, and a marker being invisible is the whole design. So verify sends a raw `Fetch` and decodes the batches itself. From `Fetch` v13 a request names a topic by id rather than by name, so the metadata read that finds each leader also carries the topic id back, and the fetch is correct at any version the client negotiates.

A partition the log disagrees with is a mismatch in the outcome and not an error. The point is to report every one of them and not to stop at the first.

### Markers Survive Compaction and Do Not Survive Retention

Compaction keeps a marker, as [above](#the-marker-is-a-control-batch-and-carries-no-producer).

Retention does not. `Log::tick` applies time retention and size retention whatever the cleanup policy says, so a compacted topic with a finite `retention.ms` drops its markers when their segments age out. A cut whose markers are gone still names offsets, and the offsets still bound a replay, and `verify` reports the markers as absent.

An operator should keep a group's cut retention at or below the shortest retention of its member topics. Nothing enforces this, and nothing can: the broker would have to refuse a topic-config change on behalf of a barrier group, and a topic does not know which groups name it.

## Compatibility, Deprecation, and Migration Plan

No Kafka wire format changes. No Kafka API key changes, and no request or response shape that a JVM client sends or parses is touched. The five new keys are in the krabka-private range at 1000 and above, and the new error code is 1000, clear of Kafka's assigned codes.

The five keys are registered for dispatch and deliberately absent from the `ApiVersions` catalog. A client negotiates version 0 for an unadvertised key anyway, and advertising them would put `UNKNOWN(1010)` rows into `kafka-broker-api-versions.sh` output that a real Kafka broker never shows. The registry coverage test encodes that carve-out explicitly rather than being weakened for it: every advertised key is registered, every registered Kafka key is advertised, and every advertised key sits below the private floor.

The cost of not advertising is that no JVM `AdminClient` can drive the control plane. That is what `crabka-barrier` is for, and it is the second reason every cut is published to `__barrier_state`, where any consumer in any language reads it with no new API key.

What a stock Kafka client observes on a topic that belongs to a barrier group is the contract this document adds.

- **Offsets advance without records appearing.** Each marker holds one offset that no consumer will ever return. A consumer sees a gap, which is what compaction produces already, and every Kafka client steps over one.
- **The log end offset exceeds the record count.** Seek-to-end lands past the last record by the number of markers the retained log holds. A client that computes lag as `endOffset - position` counts a marker as one unit of lag that it will never consume.
- **The isolation level changes nothing.** A marker carries no producer id and belongs to no transaction, so `read_committed` and `read_uncommitted` return the same records.
- **A marker's offset is stable.** Compaction keeps the marker, so an offset a cut names does not become a different record later.
- **Retention removes markers with their segments.** A cut older than the member topics' retention names offsets whose markers are gone.

A consumer group's committed offset can land on a marker's offset, and that is harmless. The next fetch returns the records after it, which is what a commit on any skipped offset does.

krabka is greenfield and undeployed, so there is no migration and no compatibility shim. A cluster that defines no barrier group carries no `__barrier_state` topic and behaves exactly as it did.

The three unenforced limits in [Broker Configuration](#broker-configuration) are the one place where a documented interface and the code disagree today.

## Test Plan

The load-bearing claim of this feature is that an ordinary Kafka consumer is unaffected by the markers, so the test plan is built around proving that against a real JVM client rather than against krabka's own.

**Unit.** Each module is tested against constructed values with no log, no metadata image and no partition. The pure decisions in `state` carry the largest share: epoch allocation, target expansion, cut construction from a placement map, the retention window, and the record fold that recovery uses. `marker` round-trips the control record and pins the three producer fields and the control bit. `partitioner` asserts the canonical JVM `murmur2` vectors, so a barrier group and a transactional id of the same name land on the same index. `coordinator` drives the injection protocol against a mocked `RemoteMarkerWriter`, including the crash cases: a start record with no cut, and a pending injection frozen at a higher coordinator epoch.

**Golden vector.** The cut wire format is pinned by a vector encoded straight from the specification, with no implementation in the loop. Four implementations read this format and only the broker writes it, so a drift between them surfaces as a wrong cut rather than as a decode error. The broker decodes the vector, re-encodes it, and compares byte for byte. `krabka-streams-rs`, `krabka-streams-java` and `krabka-streams-go` assert the same bytes.

**Log integration.** A test appends a real marker to a real log with a transaction open, and checks that the log reads it back as a barrier and leaves the last stable offset alone. The markers in these tests deliberately carry a producer id, so the routing decision is observable: a well-formed barrier has to be indistinguishable through the old classification and the new one, which is the accident being removed. One further test pins both compaction claims at once: compaction keeps a marker, and the dedup map never indexes its key. Two more reopen the log and assert that a marker rebuilds identical state, because the recovery paths are the only source of truth after a restart.

**JVM differential.** `jvm_barrier_markers.rs` produces records, injects a cut, produces more records, and injects a second cut, so the markers are interleaved rather than trailing and a consumer that stops at the first one fails. Per partition it asserts three things. Apache Kafka's own consumer reads exactly the produced records in produce order, so no marker reaches it as a record. The log end offset that the JVM tooling reports exceeds the record count by exactly the number of markers, so the markers hold real offsets and the consumer stepped over them. Neither claim changes under `read_committed`. The end-offset check is what separates "the consumer skipped the markers" from "the broker never wrote them": without it, a broker that dropped every marker would pass.

**Command line.** `barrier_cli.rs` runs the operator loop end to end against a live broker, in process. It also pins two behaviours that were assumed rather than known: a group may name a topic that does not exist yet, and the first epoch is 1 rather than 0.

Two things are not proved. No test drives an injection across more than one broker, so the remote leg of the fan-out and the `FENCED_LEADER_EPOCH` fencing are covered by unit tests against the transport seam and not by a multi-broker cluster. No test asserts that a cut survives a coordinator failover with its markers intact, which is the test that would exercise the recovery path against a real log rather than against a folded record stream. Both are worth having.

## Rejected Alternatives

### Timestamps

The status quo. A snapshot names an instant, and every consumer calls `offsetsForTimes` to turn it into a position.

This is what the feature replaces, and the [Motivation](#motivation) states the three reasons it fails. The result is not exact, because the time index is sparse. It is not reproducible, because retention moves the answer. It is not comparable across producers, because `CreateTime` is a producer clock and nothing bounds skew.

The deeper objection is that a timestamp is a property of a record and a cut is a property of a log. No amount of care with clocks turns the first into the second.

### A Data Record as the Marker

The marker could be an ordinary record with a reserved key or a header, which needs no control-batch handling at all.

Every consumer would then see it. Every application that reads a topic in a barrier group would have to know the reserved key and filter it, and every schema would have to admit a record that carries no domain payload. A stream processor that did not know about barriers would emit a marker into its own output.

That inverts the design. The whole value of the primitive is that a topic can join a barrier group without any of its existing readers changing. A data-record marker makes the group a breaking change for every consumer of every member topic.

### A Producer-Side or Library-Side Barrier

A client library could write the markers, so the broker needs no new API key, no coordinator and no internal topic.

A client cannot place anything at a position without also placing content, which is the previous alternative. It also cannot make one marker land in every partition of every member topic without racing every other producer to those partitions, and it has no way to allocate an epoch that another instance of the same library will not reuse.

A library barrier would additionally be per library. `krabka-streams-java` and `krabka-streams-go` would each need their own, and the cuts they produced would not be the same cuts. The primitive exists because four readers want one answer.

### Transaction Markers

Kafka already writes control batches. A barrier could reuse the transaction marker path and take an ABORT or COMMIT type.

The type is the problem. A transaction marker means something to the log: it resolves a producer's pending state, moves the last stable offset, and feeds the transaction index and the aborted-transaction index that a `read_committed` consumer reads. A barrier resolves nothing. Reusing the type would have the log apply transaction semantics to a batch that carries no transaction, and would put a `read_committed` consumer's view at the mercy of a coordinator that is not a transaction coordinator.

Control type 1000 keeps the two apart at the point of classification. `control_batch_kind` returns `Barrier` and the transaction path is never entered.

### Cuts Only in the RPC

The coordinator could keep the cuts in memory and serve them through `ListBarrierCuts` alone, which removes a whole internal topic.

The cuts would not survive a coordinator restart, and a replay point that does not survive a restart is not a replay point.

Durability is not the only reason. The API keys are krabka-private and no JVM client can send them, so a cut reachable only through an RPC is a cut that only a Rust client and the operator's own tool can read. Publishing to a compacted topic makes every cut readable by an ordinary consumer in any language, with no new API key and no new client code. That is what lets the three streams libraries read cuts at all.

### Rolling Back a Failed Injection

An injection that could not reach every partition could abandon its epoch, so that every published epoch is complete and a reader never has to handle a partial cut.

A marker cannot be withdrawn. An injection that reached nine of ten partitions has already put nine markers in nine logs, and abandoning the epoch leaves those markers belonging to an epoch that no cut record explains. A later reader, or `verify`, finds markers that account for nothing.

The partial cut is not a convenience for the caller. It is the record that keeps every marker in the log attributable to a published epoch.

### Trimming the State Log

The share coordinator trims its log to drop state that no longer matters, and the barrier coordinator could copy it.

The share coordinator can do this because share state folds: the newest record for a key supersedes every older one, so a trim to the newest position loses nothing. Cuts do not fold. Cut 5 and cut 6 are independent facts and neither replaces the other.

A trim would also take the group definitions, which sit under the same key prefix and have to outlive every cut they produced. A tombstone removes one cut by key and leaves the definition standing.

### Advertising the API Keys in `ApiVersions`

The five keys could appear in the `ApiVersions` response, which is where a client expects to learn what a broker speaks.

A JVM client that reads a key it does not know prints it as `UNKNOWN(1010)`. `kafka-broker-api-versions.sh` would then show five rows that no Apache Kafka broker ever shows, and an operator comparing a krabka broker against a Kafka broker would see a difference that means nothing to them. A client negotiates version 0 for an unadvertised key in any case, so advertising buys nothing.

The keys stay registered and unadvertised, and the registry coverage test encodes the carve-out so that the invariant "every registered Kafka key is advertised" still holds for every Kafka key.

### Eager Creation of `__barrier_state`

The topic could be created at broker startup, beside `__transaction_state` and `__share_group_state`, which is simpler than waiting for a first group.

It cost two unrelated JVM acceptance suites. Fifty partitions at replication factor 3 on a cluster that cannot satisfy the factor stay leaderless, and the leader-election sweep walks all fifty on every pass. The metadata churn changed election timing enough to break `jvm_static_quorum_spike` and `jvm_kip320_divergence`, neither of which uses barriers.

A feature that nothing uses should cost nothing. Lazy creation is more code, and it is the correct trade.

### Bounding `TriggerBarrier` by Dropping the Injection

A request timeout could be honoured by dropping the injection future, which is the obvious way to bound a wait in async Rust.

Dropping the future stops the coordinator between the injection-start record and the cut record. That is precisely the state a crashed coordinator leaves behind, and the recovery path then finalises the epoch as fully missing on the next restart. A caller who was impatient must not be able to manufacture a crash-recovery state on a healthy broker.

The timeout moves the fan-out deadline instead. The injection always runs to a cut record, and an impatient caller gets a partial cut rather than an open epoch.

### True Causal Consistency Through Producer-Side Markers

The design could give the Chandy-Lamport guarantee in full, if a marker travelled along every channel into the system rather than being placed at the end of one.

In Chandy-Lamport the channels are the message paths between processes, and every process forwards the marker along its outgoing channels when it first receives one. The channels into a Kafka topic are the producers. Making the cut causally consistent means every producer must stop writing to any member topic on receiving a marker, forward it, and resume, which means every producer has to participate in the protocol.

A broker cannot make a producer do that. It does not control the producer's code, and Kafka's produce path has no back-channel that would tell a producer a barrier is in flight. Any design that gets there is a client-side protocol that every producer of every member topic has to adopt, and it stops being a broker primitive.

The guarantee this design gives is stated in the [Motivation](#what-a-cut-guarantees-and-what-it-does-not) rather than assumed, because the difference matters for exactly one use case and not for the four that motivated the work.
