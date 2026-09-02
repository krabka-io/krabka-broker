# Replication and ISR Design

How a replicated partition moves records from its leader to its followers, how the leader decides what is committed, and how the controller changes the in-sync replica set and the leader when brokers fail.

This document follows the [design document style guide](../../../docs/style_guides/design_doc_style_guide.md). It describes the classic Kafka replication path. A diskless partition replaces the durability half of this design with a WAL quorum, which the [diskless WAL design](diskless-wal-design.md) covers.

## Design Goals

- **Kafka's durability contract for `acks=all`.** A producer that asks for `acks=-1` receives its offset only once the high watermark has passed the batch, and the high watermark is the minimum log end offset over the leader and its ISR followers. A partition whose ISR is smaller than `min.insync.replicas` refuses the write with `NOT_ENOUGH_REPLICAS` before the append, and a gate that times out after the append answers `NOT_ENOUGH_REPLICAS_AFTER_APPEND`.
- **No zombie writes after failover.** Every batch carries the leader epoch that appended it. A follower that rejoins after a leader change truncates its divergent suffix at the epoch boundary the new leader reports, by the KIP-101 and KIP-320 rules, before it appends again.
- **Safe-by-default leader election.** The controller elects a new leader from the live ISR, or from the KIP-966 eligible leader replicas, and elects outside both only when the topic has opted in to unclean election or to offset-aware unclean recovery.
- **The same control plane as Kafka.** Followers replicate with the ordinary `Fetch` RPC and `replica_id` set. Leaders propose ISR changes with `AlterPartition`. The controller publishes leaders, ISRs, and fencing through the metadata log, and the JVM admin tools read the same projections.

## Architecture Overview

### Who does what

A partition has one leader broker and zero or more follower brokers, all named in the `PartitionRecord` the controller publishes. Every node reacts to the metadata image:

- The replicator supervisor, in [`replicator_supervisor.rs`](../src/replicator_supervisor.rs), subscribes to the image watch channel. On every image it materializes the on-disk partition for every `(topic, partition)` whose replicas include this broker, and it runs one follower task for every such partition this broker does not lead. [`replicator_supervisor/desired_sets.rs`](../src/replicator_supervisor/desired_sets.rs) derives both sets in one walk over the image.
- A follower task, in [`replicator.rs`](../src/replicator.rs), loops on `Fetch` against the leader with `replica_id` set to this broker's node id and appends every returned batch to the local log at its leader-assigned offset, through `Partition::replicate_batch`, so replicated appends stay ordered with everything else the partition's writer does.
- The leader records each follower's progress in `ReplicaState`, in [`replica_state.rs`](../src/replica_state.rs), when it serves the follower's fetch. The fetch offset a follower sends is its persisted log end offset from the leader's point of view.
- The ISR maintenance task, in [`isr_maintenance.rs`](../src/isr_maintenance.rs), scans the partitions this broker leads and proposes an ISR shrink or expand to the controller with `AlterPartition`.
- The controller leader, which is the broker that currently leads the KRaft quorum, validates ISR proposals, tracks broker liveness, and elects leaders. See the [KRaft design](../../raft/docs/design.md) for how the quorum itself works.

### The high watermark

`ReplicaState` keeps, for each follower in the replica set, its last fetched offset, the time of its last fetch, and the last time it was caught up to the leader's end. The high watermark is `isr_high_watermark(leader_leo, isr_follower_leos)`, the minimum log end represented by the leader and its ISR followers, computed by the proved kernel in [`crates/verified/src/isr.rs`](../../verified/src/isr.rs). A follower that is not in the ISR still has its progress tracked, because the maintenance task needs its `last_caught_up` time to expand it back in, but it never lowers the watermark.

`install_isr` keys the progress map by the replica set without the leader, not by the ISR. A map keyed by the ISR would discard a shrunk-out follower's progress on every image reconcile and starve its re-admission. On a leadership change the leader reseeds missing follower progress at zero, so the watermark cannot advance on stale data.

The produce path, in [`handlers/produce/append.rs`](../src/handlers/produce/append.rs), computes the batch's durability frontier, `base_offset + last_offset_delta + 1`, and for `acks=-1` waits on `Partition::await_hw_at_least` until the watermark reaches it or the request timeout expires. The idempotent-producer commit runs on both outcomes, because the append is durable on the leader either way and a retry must be recognized as a duplicate.

### Follower recovery after a leader change

[`replicator/response.rs`](../src/replicator/response.rs) interprets every error code in the leader's response. `NOT_LEADER_OR_FOLLOWER` ends the task so the next reconcile re-evaluates the partition. `OFFSET_OUT_OF_RANGE` resets the local log to the leader's log start and drops the producer-state entries for the discarded range. A `diverging_epoch` in a successful response truncates to the reported end offset in band, which is KIP-320. `FENCED_LEADER_EPOCH` runs the KIP-101 `OffsetForLeaderEpoch` lookup in [`replicator/truncation.rs`](../src/replicator/truncation.rs) and truncates to the boundary the leader reports. Every one of those rewrites first takes the partition's replication-target lock, so a response from a stale leader target cannot truncate a log the supervisor has already handed to a new target.

The leader answers `OffsetForLeaderEpoch`, in [`handlers/offset_for_leader_epoch.rs`](../src/handlers/offset_for_leader_epoch.rs), from the partition's `.leader-epoch-checkpoint` file, which `krabka-log` keeps in Kafka's format.

### ISR maintenance and `AlterPartition`

Every `scan_interval` the maintenance task computes one proposal per led partition, in [`isr_maintenance/proposal.rs`](../src/isr_maintenance/proposal.rs), under a single `replica_state` lock so the ISR cannot shift between reading it and proposing. The proved `isr_maintenance_selected` kernel decides each candidate: the leader stays, an ISR follower stays only while its last fetch is within `replica_lag_time_max`, and a non-ISR replica joins only when both its last fetch and its last catch-up are within that bound. `isr_proposal_changed` suppresses a proposal with no unique addition or removal.

[`isr_maintenance/request_builder.rs`](../src/isr_maintenance/request_builder.rs) stamps each proposed member with its broker epoch from the image (KIP-903) and encodes both the v2 `new_isr` and the v3 `new_isr_with_epochs` shapes. [`isr_maintenance/alter_partition.rs`](../src/isr_maintenance/alter_partition.rs) sends the request to the controller leader over a short-lived client.

On the controller, [`handlers/alter_partition/isr_update.rs`](../src/handlers/alter_partition/isr_update.rs) decides each partition row with the proved `isr_admission` kernel, in order: leader-epoch fencing, then the non-empty-subset-of-replicas rule, then the KIP-903 broker-epoch eligibility check. The first failure decides the row's error code. An accepted row contributes one `PartitionRecord` change, which the handler submits through the metadata quorum. The whole request needs `ClusterAction` on the cluster resource, and a non-leader controller answers `NOT_CONTROLLER`.

### Leader election and failover

[`heartbeat/controller_state.rs`](../src/heartbeat/controller_state.rs) tracks every registered broker's last heartbeat on the controller leader and emits an alive-to-dead transition when a session expires. The failover driver in [`leader_election/driver.rs`](../src/leader_election/driver.rs) runs on every liveness tick: it re-drives stuck failovers and runs the dead-broker scan on each fresh death edge. [`leader_election/scan.rs`](../src/leader_election/scan.rs) walks the image once, asks the pure policy about every affected partition, and turns the answers into a `FailoverPlan` of partition records and asynchronous recoveries.

The policy in [`leader_election/policy.rs`](../src/leader_election/policy.rs) is the proved `failover_action` kernel with the broker's inputs. For a partition whose leader died, it elects the first live ISR replica. If the live ISR is empty, it elects a surviving member of the partition's KIP-966 eligible leader replicas, cleanly, exactly as Apache Kafka's `PartitionChangeBuilder.electAnyLeader` does. Only when both sets are empty does it consult the topic's recovery policy: offset-aware unclean recovery through the Unclean Recovery Manager in [`unclean_recovery.rs`](../src/unclean_recovery.rs), or the KIP-841 unclean election of the first live replica with a singleton ISR when `unclean.leader.election.enable` is `true`. Otherwise the partition stays unavailable until a former ISR member returns. A dead non-leader ISR member is shrunk out with the leader and epoch kept. A witness replica, from KFC-2, stays in the ISR but is never picked as leader.

Every leader change bumps `leader_epoch` through the proved `exact_epoch_successor` adapter in [`metadata_epoch.rs`](../src/metadata_epoch.rs). The controller publishes broker fencing as a controller-managed broker config, in [`heartbeat/fencing.rs`](../src/heartbeat/fencing.rs), because `BrokerRegistrationRecord` carries no fencing flag, and it publishes the KIP-966 ELR state the same way, in [`elr.rs`](../src/elr.rs), because `PartitionRecord` carries no ELR field.

Operator-triggered elections, `ElectLeaders` and the preferred-leader rebalance, share the pure selectors in [`leader_election/operator.rs`](../src/leader_election/operator.rs). The controlled-shutdown drain in [`handlers/broker_heartbeat/shutdown.rs`](../src/handlers/broker_heartbeat/shutdown.rs) moves leadership off a broker that asked to stop, and the KIP-112 offline-log-dir failover in [`handlers/broker_heartbeat/failover.rs`](../src/handlers/broker_heartbeat/failover.rs) moves it off directories a live broker reports as lost.

## Key Design Decisions

### The leader owns the watermark; the controller owns the ISR

The leader is the only node that observes follower progress, so it computes the watermark and proposes ISR changes. The controller is the only node that can serialize those changes against leader elections, so it validates and commits them. Splitting the two this way is Kafka's design, and it keeps the acknowledgement path free of a controller round trip: a leader acknowledges from its own `ReplicaState`, and only an ISR change waits on the quorum.

### Progress is keyed by the replica set, not the ISR

The alternative, a map keyed by the ISR, is simpler and wrong. Image churn reinstalls the ISR often, and each reinstall would erase a lagging follower's fetch history, so the follower could never satisfy the "recently caught up" rule and would never rejoin. Keying by the replica set costs nothing on the watermark, because `compute_hw` reads only ISR members.

### Truncation rewrites hold the replication-target lock

A follower task can outlive the leadership it was started against: the supervisor may hand the partition to a new target while a response from the old leader is still in flight. A reset or truncation applied on that stale response would destroy records the new leader considers committed. So every rewrite locks the partition's replication target and compares it with the task's own target first, and a mismatch stops the task instead of touching the log.

### The idempotent commit runs even when the `acks=all` gate times out

The batch is already in the leader's log when the gate expires. If the producer-state commit were skipped, the producer's retry would look out of order rather than duplicate, and it would be refused. Recording the commit on the timeout path is what makes `NOT_ENOUGH_REPLICAS_AFTER_APPEND` a safe, retryable answer.

### ELR members are elected before any unclean path is risked

An ELR member left the ISR while the partition still had `min.insync.replicas` members, so it holds every committed record. Electing it loses nothing, so the policy takes it ahead of any longer log that is in neither set, and it calls that election clean. The offset-aware recovery and the KIP-841 election are the last resort, and the break-glass rule in KFC-9 cannot gate the background path, because a death edge has no request and no principal to refuse.

## Integration

- **Partition writer.** Replicated appends, truncations, and resets all go through the partition's single writer task, in [`partition_writer.rs`](../src/partition_writer.rs), so a follower's `Replicate` message is ordered with every other mutation.
- **Fetch handler.** A fetch with `replica_id >= 0` is a follower fetch. [`handlers/fetch/plan.rs`](../src/handlers/fetch/plan.rs) records the follower's offset in `ReplicaState` and serves bytes up to the leader's log end; a consumer fetch is capped at the high watermark. KIP-73 follower throttling, in [`replicator/follower_throttle.rs`](../src/replicator/follower_throttle.rs), decides whether a fetch round may run and with what byte budget.
- **Producer state.** [`producer_state.rs`](../src/producer_state.rs) is truncated alongside the log on every reset and truncation, so idempotent dedup never references an offset the log no longer holds.
- **Log.** Verbatim append at a leader-assigned offset, truncation, reset, and the leader-epoch checkpoint come from [`krabka-log`](../../log/docs/design.md).
- **Diskless partitions.** For a diskless partition the WAL quorum's durable watermark, not the ISR, advances the high watermark, and the produce gate checks the partition's installed leader and epoch instead of the metadata leader. The ISR and failover machinery still runs, for leadership and metadata only.
- **Configuration.** `replica_lag_time_max` (default 30 s) bounds both recency rules. `heartbeat_interval`, `heartbeat_timeout`, and `liveness_tick_interval` (default 1 s) set the heartbeat cadence, the session length, and the controller's scan period.

## Kafka / KIP Compliance

- **Replication protocol.** Followers use the public `Fetch` API with `replica_id` set. The leader serves them past the high watermark, exactly as a Kafka leader does, and caps consumers at it.
- **[KIP-101](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+Alter+Replication+Protocol+to+use+Leader+Epoch+rather+than+High+Watermark+for+Truncation)** and **[KIP-320](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation).** `OffsetForLeaderEpoch` answers `UNKNOWN_LEADER_EPOCH` above the current epoch, the log end for the current epoch, and the checkpoint's next-epoch start below it. The in-band `diverging_epoch` hint is honoured on the follower.
- **[KIP-903](https://cwiki.apache.org/confluence/display/KAFKA/KIP-903%3A+Replicas+with+stale+broker+epoch+should+not+be+allowed+to+join+the+ISR).** Proposed ISR members carry their broker epoch, and the controller refuses a member whose stamped epoch disagrees with its registration.
- **[KIP-841](https://cwiki.apache.org/confluence/display/KAFKA/KIP-841%3A+Fenced+replicas+should+not+be+allowed+to+join+the+ISR+in+KRaft)** and **[KIP-966](https://cwiki.apache.org/confluence/display/KAFKA/KIP-966%3A+Eligible+Leader+Replicas).** Unclean election stays off by default. ELR and last-known ELR are reported on `DescribeTopicPartitions` only, because `MetadataResponsePartition` has no ELR field in any Kafka schema version. The ELR election rule was read out of `kafka-metadata-4.3.1.jar`.
- **[KIP-112](https://cwiki.apache.org/confluence/display/KAFKA/KIP-112%3A+Handle+disk+failure+for+JBOD)** and **[KIP-460](https://cwiki.apache.org/confluence/display/KAFKA/KIP-460%3A+Admin+Leader+Election+RPC).** Offline log directories move leadership through the heartbeat, and `ElectLeaders` serves preferred and unclean election types.
- **[KIP-73](https://cwiki.apache.org/confluence/display/KAFKA/KIP-73+Replication+Quotas).** `follower.replication.throttled.replicas` is honoured on the follower side of the fetch.
- **Krabka extensions.** The witness role and the three-site stretch profile are [KFC-2](../../../docs/KFCs/KFC-2-witness-broker-stretch-cluster.md). Fencing and ELR travel as controller-managed configs because the pinned `krabka-metadata` records have no field for them; the wire projections a client reads are unchanged.

## Testing

- The Stateright models in the [inventory](../../../docs/verification.md#stateright-model-check-tier): [`replica_state_model`](../src/replica_state_model.rs) drives `install_isr`, `update_follower_leo`, and the watermark; [`leader_failover_model`](../src/leader_failover_model.rs) drives the real per-partition failover policy; [`stretch_cluster_model`](../src/stretch_cluster_model.rs) checks the witness profile; [`data_path_model`](../src/data_path_model.rs) composes the append, watermark, and fetch-visibility rules; and [`reassignment_model`](../src/reassignment_model.rs) drives the reassignment policy.
- The proved kernels in the [Creusot ledger](../../../docs/verification.md#creusot-proof-ledger): `isr_high_watermark`, `isr_maintenance_selected`, `isr_proposal_changed`, and `isr_admission` in [`crates/verified/src/isr.rs`](../../verified/src/isr.rs); `failover_action` and `select_best_recovery_replica` in [`crates/verified/src/consensus.rs`](../../verified/src/consensus.rs); `epoch_and_offset_for_entries` in [`crates/verified/src/leader_epoch.rs`](../../verified/src/leader_epoch.rs); and `exact_epoch_successor` in [`crates/verified/src/epoch.rs`](../../verified/src/epoch.rs).
- [`tests/replication.rs`](../tests/replication.rs) runs a three-broker cluster and checks that every follower converges to the leader's log end. [`tests/offline_replicas.rs`](../tests/offline_replicas.rs) covers the KIP-112 `offlineReplicas` reporting that the admin tools read. The container suites `jvm_kip320_divergence`, `jvm_acceptance_durability`, and `unavailable_partitions_jvm` compare the divergence, `acks=all`, and unavailable-partition behaviour with live Kafka.
