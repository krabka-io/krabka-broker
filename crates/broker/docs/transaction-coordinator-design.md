# Transaction Coordinator Design

The per-broker coordinator that owns transactional ids, persists their state in `__transaction_state`, and drives the commit and abort markers that give consumers exactly-once reads.

This document follows the [design document style guide](../../../docs/style_guides/design_doc_style_guide.md). The subsystem lives under [`txn`](../src/txn/mod.rs). The idempotent-producer sequence tracking it relies on lives in [`producer_state.rs`](../src/producer_state.rs), and the last-stable-offset bookkeeping lives in [`krabka-log`](../../log/docs/design.md).

## Design Goals

- **Kafka's transaction state machine, persisted in Kafka's records.** The states, the transitions, and the `TransactionLogKey` and `TransactionLogValue` codecs match Apache Kafka 4.0 byte for byte, so `__transaction_state` can be read by the JVM tools and survives a coordinator move to any broker.
- **A transaction is finalized at most once, and never both committed and aborted.** The `EndTxn` path splits into a persisted prepare, a marker fan-out, and a persisted complete, and the completion revalidates that the entry it prepared is still the entry it is about to complete.
- **A `read_committed` consumer sees exactly the committed records.** The log holds the LSO at the first offset of the earliest open transaction, an abort records the transaction's range in the `.txnindex`, and the fetch path clamps visibility at `min(lso, hw)`.
- **KIP-939 two-phase commit is safe by construction.** A prepared 2PC transaction is never aborted by a timeout and never expired by the id sweep, because both decisions are pure functions the model checker enumerates.

## Architecture Overview

### Ownership

`TxnCoordinator` in [`txn/coordinator.rs`](../src/txn/coordinator.rs) is constructed once per broker. It coordinates every transactional id whose `__transaction_state` partition this broker leads. The partition is `String.hashCode(transactional_id) % num_partitions`, computed with Java's UTF-16 hash by the proved `java_string_hash_partition` kernel in [`txn/partitioner.rs`](../src/txn/partitioner.rs), so a client that finds its coordinator with `FindCoordinator` lands on the same broker a JVM cluster would pick. The topic has 50 partitions by default and is created lazily by [`txn/bootstrap.rs`](../src/txn/bootstrap.rs) on the first `FindCoordinator(TRANSACTION)`.

The coordinator's in-memory map is `transactional_id` to a locked `TxnEntry`. Every state change is persisted first, as one record in the matching `__transaction_state` partition, and published to the map only after the append succeeds, in [`txn/coordinator/persistence.rs`](../src/txn/coordinator/persistence.rs). Writes to one state partition are serialized by a per-partition lock, so the reaper's recheck-and-append is one operation. On `Broker::start` the coordinator replays every locally led state partition, tombstones included, to rebuild the map. A replay failure latches `recovery_valid` false, and the coordinator then refuses ownership of every partition rather than serve a partial image.

A reverse index from `producer_id` to `transactional_id`, in [`txn/coordinator/pid_index.rs`](../src/txn/coordinator/pid_index.rs), lets the produce handler verify a transactional batch under KIP-1319. A superseded producer id is evicted from it, so a fenced id cannot bypass coordinator validation.

### The state machine

[`txn/state.rs`](../src/txn/state.rs) holds `TxnState`: `Empty`, `Ongoing`, `PrepareCommit`, `PrepareAbort`, `CompleteCommit`, `CompleteAbort`, and `Dead`, with Kafka's `TransactionStatus` byte ids. `can_transition_to` is the whole transition relation. Kafka's transient `PrepareEpochFence` state is not modelled. The `TxnEntry` carries the producer identity, the timeout, the partition set, the KIP-890 staged next identity, and the timestamps the expiry sweeps read.

### The request path

1. **`InitProducerId`** in [`handlers/init_producer_id.rs`](../src/handlers/init_producer_id.rs). The non-transactional path hands out an idempotent identity from the broker's producer-id block, which [`producer_id_manager.rs`](../src/producer_id_manager.rs) reserves through the controller so no two brokers overlap. The transactional path, in [`handlers/init_producer_id/transactional.rs`](../src/handlers/init_producer_id/transactional.rs), allocates for a fresh id, recovers a prepared KIP-939 transaction for `keepPreparedTxn`, and otherwise aborts any ongoing transaction before it bumps the epoch. The epoch bump is what fences a zombie producer.
2. **`AddPartitionsToTxn`** in [`txn/handlers/add_partitions_to_txn.rs`](../src/txn/handlers/add_partitions_to_txn.rs) moves the entry to `Ongoing` and records the partitions the transaction writes. At `transaction.version` 2 and above the KIP-890 verify-only request answers from the partitions already registered. The KFC-9 write-freeze gate refuses a frozen topic here, before the transaction can reach it.
3. **`AddOffsetsToTxn`** and **`TxnOffsetCommit`** register a consumer group's offsets partition with the transaction and append the offsets to `__consumer_offsets` as a transactional batch under the producer's identity, so they stay below the LSO until the marker lands.
4. **`EndTxn`** in [`txn/handlers/end_txn.rs`](../src/txn/handlers/end_txn.rs) runs three phases. Phase 1 validates coordinator ownership, the producer identity, and the state, stages the identity the client continues with, and persists `Prepare{Commit,Abort}`. Phase 2 fans `WriteTxnMarkers` out to every partition the transaction touched, appending directly to the partitions this broker leads and sending the RPC to remote leaders over the inter-broker client. Phase 3 re-reads the entry, checks through the proved `transaction_completion_decision` kernel that it is still the entry that was prepared, and persists `Complete{Commit,Abort}`.
5. **`WriteTxnMarkers`** in [`txn/handlers/write_txn_markers.rs`](../src/txn/handlers/write_txn_markers.rs) appends the control batch to each locally led partition. Every marker append, local or remote, goes through [`txn/handlers/write_txn_markers/materialize.rs`](../src/txn/handlers/write_txn_markers/materialize.rs), which also resolves a `__consumer_offsets` marker against the group actors: a commit publishes the offsets and an abort discards them.

The control record itself, in [`txn/marker.rs`](../src/txn/marker.rs), is a single-record batch with `is_control_batch` and `is_transactional` set, Kafka's `EndTransactionMarker` key, and the `EndTxnMarker` value that carries the coordinator epoch.

### The two sweeps

Two background tasks run on every broker and act only on the ids that broker coordinates. The idle-transaction reaper, in [`txn/expiration.rs`](../src/txn/expiration.rs) and [`txn/coordinator/reaper.rs`](../src/txn/coordinator/reaper.rs), aborts an `Ongoing` transaction whose timeout has elapsed, through the same prepare, fan-out, and complete phases behind a mockable backend seam. The transactional-id expiry, in [`txn/id_expiration.rs`](../src/txn/id_expiration.rs) and [`txn/coordinator/expiry.rs`](../src/txn/coordinator/expiry.rs), tombstones a terminal or idle id after `transactional.id.expiration.ms`, so compaction reclaims it and the replay at start stays bounded.

### Versions and two-phase commit

[`txn/version.rs`](../src/txn/version.rs) reads the finalized `transaction.version` feature from the image. `TV_0` writes non-flexible records, `TV_1` flexible ones, `TV_2` adds the KIP-890 epoch bump on completion and server-side verification, and `TV_3` opts in to KIP-939. An unfinalized version resolves to classic behaviour.

A 2PC transaction is not a new persisted field. Exactly as Kafka's `isDistributedTwoPhaseCommitTxn`, it is a `TransactionTimeoutMs` of `i32::MAX`, resolved by `resolve_txn_timeout` in [`txn/two_pc.rs`](../src/txn/two_pc.rs) when `enable2Pc` is set. Because the timeout round-trips through `TransactionLogValue`, the property survives failover and replay without a schema change. The reaper's `should_abort_idle_txn` refuses the sentinel, and the expiry sweep's `should_expire_transactional_id` refuses every `Prepare*` state, so neither reaper can take the commit-or-abort decision away from the external transaction manager.

## Key Design Decisions

### Persist before publish, and lock per state partition

An entry that appeared in memory before its record was durable could be observed, acted on, and then lost on restart. So every transition appends first and publishes second. The per-partition write lock exists for the reaper: its post-marker recheck and its completion append must be one serialized step, or a concurrent `EndTxn` could slip between them and complete the same generation twice.

### `EndTxn` releases the entry lock across the marker fan-out

Holding the lock across network I/O to remote leaders would serialize every transaction on the coordinator behind the slowest broker and could deadlock two coordinators fanning out to each other. Releasing it opens a window in which an `InitProducerId` can bump the epoch. Phase 3 closes that window: the reacquisition guard in [`txn/handlers/end_txn/reacquire.rs`](../src/txn/handlers/end_txn/reacquire.rs) compares the current entry with the prepared snapshot, and a fenced producer can never finalize.

### KIP-890 identity rotation at the marker-epoch boundary

`i16::MAX` is reserved for the transaction marker. When a completion would push the epoch to that value, the coordinator allocates a fresh producer id at epoch 0 and records the old id as `prev_producer_id`, in [`txn/handlers/end_txn/producer_identity.rs`](../src/txn/handlers/end_txn/producer_identity.rs), so the client receives a usable pair and the marker keeps its reserved epoch.

### One marker path for local and remote appends

A marker that lands in a `__consumer_offsets` log but is never resolved against the group actor leaves offsets that are durable yet invisible, or permanently unstable after an abort. Routing both the inter-broker handler and the coordinator's direct local append through `append_marker_and_materialize` is what keeps the durable marker and the in-memory publication from drifting apart.

### The sweeps skip what they cannot decide

The reaper refuses every state except `Ongoing`, and the id expiry refuses every `Prepare*` state, whatever its age. A prepared transaction is either mid-completion by the coordinator or owned by an external 2PC manager, and in both cases a timeout has no authority over it.

### Pure decision cores under the handlers

`decide_phase1_transition` and `decide_end_txn_completion` in [`txn/decision.rs`](../src/txn/decision.rs), `should_abort_idle_txn`, and `should_expire_transactional_id` are pure. The live handlers call them, and the Stateright models enumerate them, so the models' guarantees bind the production path.

## Integration

- **Produce.** [`handlers/produce/producer_checks.rs`](../src/handlers/produce/producer_checks.rs) verifies a transactional batch against the coordinator's reverse index under KIP-1319 and runs the idempotent dedup that [`producer_state.rs`](../src/producer_state.rs) keeps per partition. A transactional batch is appended with its producer identity, and the log opens a pending transaction at its base offset.
- **Fetch.** A `read_committed` fetch is clamped at `effective_lso = min(lso, hw)` and carries the aborted-transaction ranges from the `.txnindex` files, in [`handlers/fetch/read.rs`](../src/handlers/fetch/read.rs). The broker does no server-side batch filtering; the client filters aborted batches from the ranges it is given, as Kafka clients do.
- **Group coordinator.** A `__consumer_offsets` marker resolves the transaction's pending offset commits against the group actors, which is how KIP-447's `UNSTABLE_OFFSET_COMMIT` answer clears.
- **Inter-broker client.** Remote marker fan-out and the KIP-890 routing of an offsets-partition enrollment to the coordinating broker both run over the shared `InterBrokerClient`, with whatever TLS and SASL the inter-broker listener demands.
- **Admin.** `ListTransactions` and `DescribeTransactions` (KIP-664) read a snapshot of the in-memory map, consistent per id but not across the batch, as the JVM coordinator's are.
- **Log.** The LSO, the pending-transaction map, the `.txnindex`, and marker recovery on open are the log's, in [`crates/log/src/log/transaction.rs`](../../log/src/log/transaction.rs).

## Kafka / KIP Compliance

- **[KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging).** The state machine, the coordinator partitioner, the transaction log records, the control markers, and the two sweeps follow the KIP and the 4.0 codebase. The codec module [`txn/log_record.rs`](../src/txn/log_record.rs) documents the v0 and v1 wire layouts field by field.
- **[KIP-360](https://cwiki.apache.org/confluence/display/KAFKA/KIP-360%3A+Improve+reliability+of+idempotent%2Ftransactional+producer)** and **[KIP-890](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense).** The epoch bump on completion, the verify-only `AddPartitionsToTxn`, and the identity rotation at the marker-epoch boundary apply at `transaction.version` 2 and above.
- **[KIP-939](https://cwiki.apache.org/confluence/display/KAFKA/KIP-939%3A+Support+Participation+in+2PC).** `enable2Pc` and `keepPreparedTxn` on `InitProducerId` v6, the `i32::MAX` timeout sentinel, and the reaper exemption apply at `transaction.version` 3. `enable2Pc` is refused with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` when `transaction.two.phase.commit.enable` is off.
- **[KIP-447](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics)** and **[KIP-664](https://cwiki.apache.org/confluence/display/KAFKA/KIP-664%3A+Provide+tooling+to+detect+and+abort+hanging+transactions).** `TxnOffsetCommit` validates group metadata at v3 and above, and the admin listing and describe APIs read the coordinator's map.
- **Interpretation decisions.** `PrepareEpochFence` is not modelled; an `InitProducerId` that must fence an ongoing transaction aborts it through the ordinary prepare and complete path instead. The broker handles the single-transaction request shape a producer client sends and processes a multi-transaction v4+ request in sequence.

## Testing

- The Stateright models in the [inventory](../../../docs/verification.md#stateright-model-check-tier): [`decision_model.rs`](../src/txn/decision_model.rs) drives the real `EndTxn` decision cores through the prepare, marker-window, and complete split with a concurrent `InitProducerId`; [`two_pc_model.rs`](../src/txn/two_pc_model.rs) interleaves the reaper with the lifecycle and proves that a 2PC transaction is never reaped; and [`eos_composition_model.rs`](../src/txn/eos_composition_model.rs) composes the decision cores with the LSO rule and the `read_committed` clamp to show that the visible set is exactly the committed records.
- The proved kernels in the [Creusot ledger](../../../docs/verification.md#creusot-proof-ledger): `transaction_completion_decision` in [`crates/verified/src/transaction.rs`](../../verified/src/transaction.rs), `java_string_hash_partition` in [`crates/verified/src/broker.rs`](../../verified/src/broker.rs), `producer_decision` in [`crates/verified/src/producer.rs`](../../verified/src/producer.rs), and `producer_id_block_allocation` in [`crates/verified/src/producer_id.rs`](../../verified/src/producer_id.rs).
- [`tests/transactions.rs`](../tests/transactions.rs) runs init, begin, send, commit or abort, and consumer isolation in process. [`tests/transactions_2pc.rs`](../tests/transactions_2pc.rs) pins the KIP-939 wire behaviour. [`tests/transaction_version.rs`](../tests/transaction_version.rs), [`tests/txn_secured_marker_fanout.rs`](../tests/txn_secured_marker_fanout.rs), [`tests/txn_offset_commit_materialize.rs`](../tests/txn_offset_commit_materialize.rs), and [`tests/list_describe_transactions.rs`](../tests/list_describe_transactions.rs) cover the version split, the secured remote fan-out, offset materialization, and the admin APIs. The container suite `jvm_acceptance_durability` runs a JVM producer's `acks=all` transactional writes against the broker.
