# KRaft Consensus and Controller Design

The metadata quorum that every krabka node reads cluster state from and writes cluster state through, and the controller logic that runs on its leader.

This document follows the [design document style guide](../../../docs/style_guides/design_doc_style_guide.md). It covers two crates and one part of a third. [`krabka-kraft-core`](../../kraft-core/README.md) is the sans-IO consensus state machine. [`krabka-raft`](../README.md) wraps it in an async engine, a durable log, the KIP-595 wire, and the controller listener. The controller policy that decides leaders, fences brokers, and maintains ISRs lives in the broker crate, and the last section describes how it plugs in.

## Design Goals

- **Speak KIP-595 on the wire, byte for byte.** A JVM Kafka controller must be able to vote, fetch, and follow a krabka leader, and a JVM `kafka-metadata-quorum` tool must be able to describe the quorum. The peer RPCs are `Vote`, `BeginQuorumEpoch`, `EndQuorumEpoch`, and `Fetch`, pinned at one captured version each in [`kraft/transport/wire.rs`](../src/kraft/transport/wire.rs).
- **Keep the consensus decision deterministic and testable without I/O.** The state machine in [`kraft-core/src/core.rs`](../../kraft-core/src/core.rs) reads the clock as an injected instant, reads the log through the `LogView` seam, and returns every side effect as an `Action`. A model checker and a deterministic simulator drive the same code that production runs.
- **One owner for all consensus state.** The engine in [`kraft/controller.rs`](../src/kraft/controller.rs) is a single tokio task that owns the state machine, the log, and the published image. Every other party talks to it over a command channel. There is no lock across an await anywhere in the consensus path.
- **Recover from disk alone.** A restarted node rebuilds its image from the newest KIP-630 checkpoint plus the committed log tail, and its quorum state from the node-local `quorum-state` file, in [`kraft/controller/recovery.rs`](../src/kraft/controller/recovery.rs) and [`kraft/controller/startup.rs`](../src/kraft/controller/startup.rs).
- **Serve brokers and admin tools from one implementation.** Requests that arrive on the controller listener but belong to the broker's handler registry are routed back to it through the `ControllerAdminRouter` seam, so `BrokerHeartbeat`, `AlterPartition`, and the other KIP-919 APIs have one implementation, not two.

## Architecture Overview

### The sans-IO core

`QuorumStateMachine::on_event(event, log, now) -> Vec<Action>` is the whole consensus algorithm. [`kraft-core/src/event.rs`](../../kraft-core/src/event.rs) lists the inputs: the two timers, an inbound `Vote` and its response, `BeginQuorumEpoch`, `EndQuorumEpoch`, and the leader and follower halves of `Fetch`. [`kraft-core/src/action.rs`](../../kraft-core/src/action.rs) lists the outputs: send a vote request, reply to one, announce an epoch, fetch from a leader, persist the quorum state, append the `LeaderChange` record, advance the high watermark, truncate to a diverging point, and arm a timer.

[`kraft-core/src/role.rs`](../../kraft-core/src/role.rs) holds the volatile role: `Unattached`, `Voted`, `Follower`, `Prospective`, `Candidate`, `Leader`, `Resigned`, and `Observer`. `Prospective` is the KIP-996 pre-vote round. A `Leader` remembers `epoch_start_offset`, the offset of its own `LeaderChange` record, and the high watermark may only advance past that offset once a current-epoch record is majority-replicated. That is the Raft leader-completeness rule.

[`kraft-core/src/types.rs`](../../kraft-core/src/types.rs) holds the durable `QuorumState`: the cluster id, the finalized `kraft.version`, the leader epoch, the leader id, the voted key, and the voter set. One submodule per event family holds the handlers: `membership` applies voter records, `vote_request` answers a vote, `election` runs the pre-vote and vote rounds, `leadership` reacts to an epoch announcement, and `replication` serves and answers `Fetch`.

The core builds for `wasm32-unknown-unknown`, and the `sim` module behind the `sim` feature is a deterministic multi-node simulator with a logical clock, a `BTreeMap`-ordered message bus, and recorded traces. The browser playground and the rendered failure-scenario slideshow both run it.

### The async engine

`KraftController` in [`kraft/controller.rs`](../src/kraft/controller.rs) spawns one task that owns an `Engine`: the core, the `KraftLog`, the KIP-853 control state, the published `MetadataImage`, and the parked commit waiters. The loop in [`kraft/controller/engine_loop.rs`](../src/kraft/controller/engine_loop.rs) selects over the command channel, an election timer, a fetch timer, and a leader heartbeat interval. Each `Send*` action is dispatched fire-and-forget: a spawned task calls `PeerSender::send`, decodes the response into the matching `Receive*Response` event, and posts it back on the command channel. The loop never awaits a peer inline, which is what keeps an in-process multi-node cluster from deadlocking when engines call each other reciprocally.

`KraftLog` in [`kraft/log.rs`](../src/kraft/log.rs) is a thin facade over `krabka_log::Log` at `<dir>/@metadata-0`. It adds the high watermark, which the log crate does not track, and the committed-read filter that a `Fetch` response is cut at. Records are Kafka record batches: metadata values encoded with the KIP-631 codecs, KIP-853 control records as typed control batches, and the KIP-595 `LeaderChange` marker, all built in [`kraft/controller/records.rs`](../src/kraft/controller/records.rs).

### Submission and commit

`ControllerHandle::submit_change` in [`controller/submit.rs`](../src/controller/submit.rs) submits a batch of metadata records. On the leader the engine's [`kraft/controller/submit.rs`](../src/kraft/controller/submit.rs) validates and encodes the batch in one pass against a scratch copy of the image, so a batch that mixes a topic record and its partition records validates as a sequence. It stamps a new broker registration's epoch with the offset the record will commit at (KIP-903), appends the batch, and parks a `CommitWaiter` keyed by the end offset the batch needs committed. [`kraft/controller/apply.rs`](../src/kraft/controller/apply.rs) resolves the waiter once the high watermark reaches that offset and the records have been applied to the published image. A leadership change fails every parked waiter rather than leaving it hung.

A follower that receives `submit_change` forwards it to the leader's controller listener over the Krabka-private `SubmitChange` RPC (api key 1003). Broker-only nodes never hold a log; the `MetadataObserver` in the broker crate keeps its image current by fetching the committed log over `MetadataFetch` (1004) and forwards its writes the same way.

### Snapshots, recovery, and reconfiguration

[`snapshot.rs`](../src/snapshot.rs) writes KIP-630 checkpoints as `<end_offset>-<epoch>.checkpoint` files with Kafka's header, data, and footer batch layout, and no `.meta` sidecar. [`kraft/controller/snapshotting.rs`](../src/kraft/controller/snapshotting.rs) writes one every `snapshot_interval_records` committed records and prunes the log below it. A committed `metadata.version` downgrade must be checkpointed at exactly its boundary (KIP-1155), and that write is retried until it succeeds. A follower that has fallen below the leader's log start fetches the checkpoint over `FetchSnapshot` (api key 59), reassembles it through the IO-free [`kraft-core/src/snapshot_fetch.rs`](../../kraft-core/src/snapshot_fetch.rs) state machine, and installs it.

KIP-853 voter changes go through [`kraft/controller/reconfiguration.rs`](../src/kraft/controller/reconfiguration.rs). At most one voter or `kraft.version` operation may be uncommitted at a time. The control state in [`kraft/controller/control_state.rs`](../src/kraft/controller/control_state.rs) keeps the uncommitted-through-committed history of voter sets, and a replica applies a `VotersRecord` as soon as it reads one, before it commits, so the core keeps both sides of the in-flight transition addressable. The `AddRaftVoter`, `RemoveRaftVoter`, `UpdateRaftVoter`, and `DescribeQuorum` handlers in [`server/voter_admin.rs`](../src/server/voter_admin.rs) and [`server/kip853.rs`](../src/server/kip853.rs) answer with the error codes the pinned `apache/kafka:4.3.1` image assigns.

The node-local `quorum-state` file, in [`kraft/controller/quorum_state_file.rs`](../src/kraft/controller/quorum_state_file.rs), keeps Kafka's `QuorumStateData` JSON field set and order for both schema versions, so the JVM tools can read it.

### The controller listener

[`server.rs`](../src/server.rs) is the accept loop. It frames requests as `len | RequestHeader | body`, runs the `ApiVersions` handshake with the advertised table in [`server/api_versions/table.rs`](../src/server/api_versions/table.rs), and dispatches on api key. The peer RPCs go to the engine. `BrokerRegistration` and `ControllerRegistration` are served here, in [`server/registration.rs`](../src/server/registration.rs). `DescribeCluster` answers the quorum's voter endpoints so an `AdminClient` with `--bootstrap-controller` can discover the controllers. Every other broker-owned API reaches the broker's handler registry through the injected `ControllerAdminRouter`, and a KIP-595 `Fetch` addressed to a diskless WAL shard is claimed by the injected `RaftShardRouter` before metadata dispatch sees it.

The inbound handshake and the outbound dial are trait seams, in [`handshake.rs`](../src/handshake.rs) and [`network/dialer.rs`](../src/network/dialer.rs), so the broker can terminate TLS and SASL on both sides without this crate depending on the broker or the security crate.

## Key Design Decisions

### A hand-rolled engine rather than a generic Raft library

Kafka's KRaft is Raft with a pull-based replication verb, a pre-vote round, a `LeaderChange` control record, and a byte-exact wire format that a JVM peer checks. A generic library would own the RPC shapes, the election timing, and the log encoding, and each of those would have to be translated at the boundary. The core is instead written against KIP-595 and KIP-996 directly, and the wire codecs in [`kraft/transport/wire`](../src/kraft/transport/wire.rs) map the core's flat request and response enums onto the generated Kafka message bodies at the captured versions. Some older comments in the tree still mention `openraft`, which an earlier engine used; the current engine has no such dependency.

### Pull replication and the `Fetch` codec

KIP-595 has one replication verb, `Fetch`, and the follower drives it. The leader serves batch bytes up to its log end, not only up to the high watermark, because a follower must hold a record before the leader can count it toward a majority. The high watermark travels in the response and gates apply on the follower. A follower that asks with a `last_fetched_epoch` the leader does not hold receives a diverging-epoch hint and truncates to it, which is the KIP-320 rule reused for the metadata log.

### The single-owner actor and fire-and-forget sends

An engine that awaited a peer RPC inline would hold consensus state across the await and would deadlock an in-process cluster whose engines call each other. Sending on a spawned task and posting the decoded response back as an event keeps the loop single-threaded over all state and keeps every transition a pure `on_event` call. The cost is that a response is applied one loop turn later than it arrived, which the model checker and the simulator both cover.

### The core reads the log through `LogView`

The core needs three facts from the log: its end offset, its last epoch, and the end offset of a given epoch for the divergence hint. Exposing exactly those through a trait lets the model checker and the simulator supply an in-memory log, lets `KraftLog` supply the real one, and lets the diskless WAL reuse the same seam over a partition log.

### Deterministic election jitter

Randomized backoff is what breaks a split vote, but a random source would make the simulator and the model non-reproducible. `election_jitter_ms` derives the jitter from the node id and the epoch, so different nodes and different re-election rounds get different spreads, and the same run replays the same way. The kernel is proved in `krabka-verified`.

### Bootstrap modes

A fresh cluster needs one node to seed the voter set and the others to wait. `BootstrapMode` in [`config.rs`](../src/config.rs) names the three cases: `Bootstrap` seeds the initial voters and elects itself, `Join` starts as an observer and, with `auto_join`, sends `AddRaftVoter` for itself once it has caught up, and `Rejoin` recovers everything from the on-disk log, checkpoint, and quorum-state file. The handle-level `add_learner` and `change_membership` methods from the earlier engine return `RaftError::Unsupported`.

## Integration

### What the broker builds on top

The broker reads and writes metadata through the `MetadataSource` seam in [`crates/broker/src/metadata_source.rs`](../../broker/src/metadata_source.rs). A combined or controller node backs it with a `ControllerHandle`. A broker-only node backs it with the observer in [`crates/broker/src/metadata_observer.rs`](../../broker/src/metadata_observer.rs), which fetches the committed `__cluster_metadata` log and applies each record exactly as the engine would.

The controller policy runs in the broker crate, on whichever node currently leads the quorum:

- **Liveness and fencing.** [`crates/broker/src/heartbeat/controller_state.rs`](../../broker/src/heartbeat/controller_state.rs) tracks every broker's last heartbeat and emits alive-to-dead transitions. [`crates/broker/src/heartbeat/fencing.rs`](../../broker/src/heartbeat/fencing.rs) publishes the fencing decision into the metadata log, level-triggered, so every node sees it.
- **Leader failover.** [`crates/broker/src/leader_election`](../../broker/src/leader_election.rs) runs the dead-broker and offline-log-dir scans over the image and applies the pure failover policy, which the [replication and ISR design](../../broker/docs/replication-isr-design.md) describes.
- **ISR changes.** `AlterPartition` in [`crates/broker/src/handlers/alter_partition.rs`](../../broker/src/handlers/alter_partition.rs) validates a leader's proposal and submits the new partition record.
- **Registration and heartbeat.** `BrokerHeartbeat` reaches the controller listener through the admin router, in [`crates/broker/src/handlers/broker_heartbeat.rs`](../../broker/src/handlers/broker_heartbeat.rs), because its answer also drives the liveness registry, the KIP-112 offline-dir failover, and the controlled-shutdown drain.
- **Reassignment and auto-join.** [`crates/broker/src/reassignment.rs`](../../broker/src/reassignment.rs) completes KIP-455 reassignments on the leader. [`crates/broker/src/auto_join.rs`](../../broker/src/auto_join.rs) drives the joiner side of KIP-853 auto-join over the real `AddRaftVoter` RPC.

### What the quorum depends on

- **Storage.** `KraftLog` sits on [`krabka-log`](../../log/docs/design.md), which owns the segment files, the leader-epoch checkpoint, the log-start-offset checkpoint that carries a prune inside the active segment across a restart, and fsync.
- **Transport.** Outbound peer RPCs use `krabka_client_core::Connection::raw_request` over one cached connection per voter, in [`network/peer_sender.rs`](../src/network/peer_sender.rs).
- **Formatting.** A node refuses to boot on an unformatted directory. [`krabka-format`](../../format/src/lib.rs) seeds `meta.properties.json`, the bootstrap records, and the singleton `VotersRecord`.
- **Diskless WAL shards.** The [diskless WAL](../../broker/docs/diskless-wal-design.md) reuses the KIP-595 `Fetch` envelope, the `LogView` seam, and the `RaftShardRouter` hook, and reserves its offsets through `submit_change`.

## Kafka / KIP Compliance

- **[KIP-595](https://cwiki.apache.org/confluence/display/KAFKA/KIP-595%3A+A+Raft+Protocol+for+the+Metadata+Quorum).** The peer RPCs are pinned at `Vote` v2, `BeginQuorumEpoch` v1, `EndQuorumEpoch` v1, and `Fetch` v17, and the advertised `ApiVersions` range for each is exactly that version, so a JVM peer never sends a version the codec would misread. The metadata log is the single topic `__cluster_metadata`, partition 0.
- **[KIP-996](https://cwiki.apache.org/confluence/display/KAFKA/KIP-996%3A+Pre-Vote).** A voter runs a non-binding pre-vote at the current epoch before it bumps the epoch. Kafka's `VoteResponse` carries no pre-vote flag, so the candidate infers the round from its own role.
- **[KIP-853](https://cwiki.apache.org/confluence/display/KAFKA/KIP-853%3A+KRaft+Controller+Membership+Changes).** Voter changes are `VotersRecord` control records, one in flight at a time, with directory ids compared exactly at `kraft.version` 1. `INVALID_VOTER_KEY`, `DUPLICATE_VOTER`, and `VOTER_NOT_FOUND` carry the codes from the pinned `apache/kafka:4.3.1` image, and a malformed update answers `INVALID_REQUEST` as `KafkaRaftClient.handleUpdateVoterRequest` does.
- **[KIP-630](https://cwiki.apache.org/confluence/display/KAFKA/KIP-630%3A+Kafka+Raft+Snapshot)** and **[KIP-1155](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1155%3A+Metadata+log+downgrade)**. Checkpoints use Kafka's filename grammar and batch layout. A `metadata.version` downgrade is checkpointed at its exact boundary.
- **[KIP-631](https://cwiki.apache.org/confluence/display/KAFKA/KIP-631%3A+The+Quorum-based+Kafka+Controller)** and **[KIP-903](https://cwiki.apache.org/confluence/display/KAFKA/KIP-903%3A+Replicas+with+stale+broker+epoch+should+not+be+allowed+to+join+the+ISR).** Metadata records are the KIP-631 value encodings, and a broker's epoch is the committed offset of its registration record.
- **[KIP-919](https://cwiki.apache.org/confluence/display/KAFKA/KIP-919%3A+Allow+AdminClient+to+Talk+Directly+with+the+KRaft+Controller+Quorum+and+add+Controller+Registration).** `DescribeCluster` and `ControllerRegistration` are served on the controller listener, and the admin surface routes to the broker's handlers.
- **Mixed quorums.** A JVM and krabka controller can form one static quorum at `kraft.version` 0, which [`crates/broker/tests/jvm_static_quorum_spike.rs`](../../broker/tests/jvm_static_quorum_spike.rs) exercises against `apache/kafka:4.0.0`. Mixed dynamic quorums are outside the compatibility target.

## Testing

- [`tests/kraft_model.rs`](../tests/kraft_model.rs) is the Stateright model. It holds the real `QuorumStateMachine` for each node, an unordered network with loss and duplication, and node crashes, and it checks election safety over three voters, committed-log linearizability over two, and leader completeness over three voters that take client appends. That last config, `three_voters_append`, is the one where a committed prefix can rest on a bare majority while the third voter falls behind, so it is what makes the KIP-595 log-recency test in `handle_vote_request` observable: drop that conjunct and a stale voter wins an election and `leader_completeness` fails.
- [`tests/kraft_sim.rs`](../tests/kraft_sim.rs) drives the core through the deterministic simulator over an in-memory log. [`tests/kraft_log_sim.rs`](../tests/kraft_log_sim.rs) runs the same scheduler over real on-disk `KraftLog`s. [`tests/kraft_engine_sim.rs`](../tests/kraft_engine_sim.rs) runs three real async engines over an in-memory `PeerSender`.
- [`tests/snapshot.rs`](../tests/snapshot.rs), [`tests/reconfig.rs`](../tests/reconfig.rs), and [`tests/single_node.rs`](../tests/single_node.rs) cover checkpoint recovery, KIP-853 changes, and the single-voter wiring. [`crates/broker/tests/quorum.rs`](../../broker/tests/quorum.rs) runs three in-process brokers through election, replication, and follower forwarding.
- The proved kernels are in the [Creusot ledger](../../../docs/verification.md#creusot-proof-ledger): `election_jitter_ms`, `log_is_up_to_date`, `majority_size`, `election_has_quorum`, and `recompute_high_watermark` in [`crates/verified/src/consensus.rs`](../../verified/src/consensus.rs); the vote, voter-set, quorum-state, snapshot, checkpoint, reconfiguration, recovery, and offset kernels named beside their raft callers in that table.
