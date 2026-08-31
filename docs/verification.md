# Verification

Krabka uses two formal test tiers. Creusot proves contracts for small,
executable kernels. Stateright checks all reachable states inside each model's
stated bounds. Neither tier proves the whole broker.

## Creusot Proof Ledger

The contract attributes beside each function are the source of truth. A proof
applies only when its caller establishes every listed precondition. An empty
precondition cell means that the contract accepts all values of the Rust input
types.

| Kernel and contract | Host caller | Proof session | Caller preconditions |
| :--- | :--- | :--- | :--- |
| [`acl_decision`](../crates/verified/src/authz.rs) applies super-user bypass, explicit deny precedence, and default deny to matching-ACL state. | [`krabka-authz`](../crates/authz/src/simple.rs) | [`proof.json`](../verif/krabka_verified_rlib/authz/acl_decision/proof.json) | The host sets the allow/deny flags from ACLs matching resource, principal, host, and operation; matching remains outside Creusot. |
| [`spool_append_decision`](../crates/verified/src/audit.rs) admits an audit frame without overflowing the byte cap and advances the fsync cadence without wrapping. | [`krabka-audit`](../crates/audit/src/spool.rs) | [`proof.json`](../verif/krabka_verified_rlib/audit/spool_append_decision/proof.json) | `sync_every > 0`; `unsynced < sync_every`. |
| [`fetch_visibility`](../crates/verified/src/broker.rs) computes Fetch bounds and response watermarks. | [`krabka-broker`](../crates/broker/src/handlers/fetch.rs) | [`proof.json`](../verif/krabka_verified_rlib/broker/fetch_visibility/proof.json) | `0 <= log_start <= hw <= log_end`; `log_start <= deliverable <= hw`. |
| [`delete_records_target`](../crates/verified/src/broker.rs) resolves the `-1` sentinel to the high watermark. | [`krabka-broker`](../crates/broker/src/handlers/delete_records/offsets.rs) | [`proof.json`](../verif/krabka_verified_rlib/broker/delete_records_target/proof.json) | None. |
| [`delete_records_offset_out_of_range`](../crates/verified/src/broker.rs) classifies targets below zero or above the log end. | [`krabka-broker`](../crates/broker/src/handlers/delete_records/offsets.rs) | [`proof.json`](../verif/krabka_verified_rlib/broker/delete_records_offset_out_of_range/proof.json) | None. |
| [`effective_share_backlog`](../crates/verified/src/broker.rs) computes a nonnegative, saturating backlog. | [`krabka-broker`](../crates/broker/src/share_partition/backlog_poller.rs) | [`proof.json`](../verif/krabka_verified_rlib/broker/effective_share_backlog/proof.json) | None. |
| [`chain_step`](../crates/verified/src/chain.rs) accepts exactly the expected sequence and opaque previous head, advances by one, and rejects sequence exhaustion instead of wrapping. | [`krabka-audit`](../crates/audit/src/verify/walk.rs); the [`krabka-remote-storage`](../crates/remote-storage/src/worm/verify/walk.rs) WORM verifier | [`proof.json`](../verif/krabka_verified_rlib/chain/chain_step/proof.json) | The host computes `head_matches` by exact comparison with its independently recomputed chain head; hashing and signatures remain outside Creusot. |
| [`select_chain_tip`](../crates/verified/src/chain.rs) selects the eligible WORM receipt with the greatest `(start offset, sequence)` rank; `chain_step` rejects writer-side sequence exhaustion instead of repeating `u64::MAX`. | The [`krabka-remote-storage`](../crates/remote-storage/src/worm/chain.rs) WORM writer and the broker [archive copy loop](../crates/broker/src/remote_log_manager/copy.rs) | [`proof.json`](../verif/krabka_verified_rlib/chain/select_chain_tip/proof.json) | The host marks a receipt eligible only after excluding delete states, decoding its metadata, and confirming that it carries a produced head; opaque heads remain outside Creusot. |
| [`compute_horizon`](../crates/verified/src/compaction.rs) adds retention time with saturation at the `i64` bounds. | `krabka-verified` calls it from `retain_decision`; [`krabka-log`](../crates/log/src/compact/decision.rs) also calls it in tests. | [`proof.json`](../verif/krabka_verified_rlib/compaction/compute_horizon/proof.json) | None. |
| [`retain_decision`](../crates/verified/src/compaction.rs) implements the KIP-534 record and control-marker retention rules. | [`krabka-log`](../crates/log/src/compact/decision.rs) | [`proof.json`](../verif/krabka_verified_rlib/compaction/retain_decision/proof.json) | None. |
| [`election_jitter_ms`](../crates/verified/src/consensus.rs) keeps deterministic jitter in `[0, base_ms)`. | [`krabka-kraft-core`](../crates/kraft-core/src/core.rs) | [`proof.json`](../verif/krabka_verified_rlib/consensus/election_jitter_ms/proof.json) | None. |
| [`log_is_up_to_date`](../crates/verified/src/consensus.rs) implements the KIP-595 candidate-log ordering. | [`krabka-kraft-core`](../crates/kraft-core/src/core/vote_request.rs) | [`proof.json`](../verif/krabka_verified_rlib/consensus/log_is_up_to_date/proof.json) | None. |
| [`vote_wire_decision`](../crates/verified/src/vote.rs) rejects negative KIP-595 voter, candidate, and epoch fields; [`vote_encode_decision`](../crates/verified/src/vote.rs) rejects unsigned values above the signed wire maximum; [`vote_admission_decision`](../crates/verified/src/vote.rs) requires the exact target directory, matching cluster, and both voter memberships. | The raft [Vote codec](../crates/raft/src/kraft/transport/wire/request.rs) and kraft-core [Vote handler](../crates/kraft-core/src/core/vote_request.rs) | [`decode proof`](../verif/krabka_verified_rlib/vote/vote_wire_decision/proof.json), [`encode proof`](../verif/krabka_verified_rlib/vote/vote_encode_decision/proof.json), [`admission proof`](../verif/krabka_verified_rlib/vote/vote_admission_decision/proof.json) | The decoder also requires exactly one `__cluster_metadata` partition-0 row and full frame consumption. The host compares KIP-853 directory IDs exactly at kraft.version 1 and retains the version-0 node-ID compatibility rule; later epoch/log and double-vote checks remain in the state machine. |
| [`majority_size`](../crates/verified/src/consensus.rs) computes `floor(voters / 2) + 1`; [`election_has_quorum`](../crates/verified/src/consensus.rs) admits only that many unique grants from the current voter set. | [`krabka-kraft-core`](../crates/kraft-core/src/core/election.rs) | [`majority_size/proof.json`](../verif/krabka_verified_rlib/consensus/majority_size/proof.json), [`election_has_quorum/proof.json`](../verif/krabka_verified_rlib/consensus/election_has_quorum/proof.json) | The host filters the grant set to unique current voters before passing its count. |
| [`recompute_high_watermark`](../crates/verified/src/consensus.rs) computes the majority offset, applies the epoch gate, and does not regress. | [`krabka-kraft-core`](../crates/kraft-core/src/core/replication.rs); the `krabka-broker` WAL [live engine](../crates/broker/src/wal/quorum/engine/distributed.rs); the [diskless crash model](../crates/broker/src/diskless_crash_model.rs) | [`proof.json`](../verif/krabka_verified_rlib/consensus/recompute_high_watermark/proof.json) | `1 <= majority <= followers + 1`; `current_hwm <= log_end`; each follower offset is at most `log_end`. |
| [`diskless_trim_decision`](../crates/verified/src/diskless.rs) advances local log start only behind both the committed object projection and the lagged high watermark, without subtraction overflow or regression. | The diskless WAL [object flusher](../crates/broker/src/diskless/flusher/object_flush.rs) | [`proof.json`](../verif/krabka_verified_rlib/diskless/diskless_trim_decision/proof.json) | None; negative offsets and a safety lag beyond the high watermark fail closed, while a negative configured lag preserves the host's existing zero-lag behavior. |
| [`create_token_deadlines`](../crates/verified/src/delegation_token.rs), [`renew_token_expiry`](../crates/verified/src/delegation_token.rs), [`expire_token_deadline`](../crates/verified/src/delegation_token.rs), and [`token_is_active`](../crates/verified/src/delegation_token.rs) implement KIP-48 deadline selection and authentication admission without signed overflow or resurrection of expired tokens. | The broker's [create](../crates/broker/src/handlers/create_delegation_token/lifetime.rs), [renew](../crates/broker/src/handlers/renew_delegation_token.rs), [expire](../crates/broker/src/handlers/expire_delegation_token.rs), and [SCRAM](../crates/broker/src/network/auth/scram.rs) paths | [`create proof`](../verif/krabka_verified_rlib/delegation_token/create_token_deadlines/proof.json), [`renew proof`](../verif/krabka_verified_rlib/delegation_token/renew_token_expiry/proof.json), [`expire proof`](../verif/krabka_verified_rlib/delegation_token/expire_token_deadline/proof.json), [`authentication proof`](../verif/krabka_verified_rlib/delegation_token/token_is_active/proof.json) | Owner/renewer authorization and controller persistence remain in the host handlers; renew/expire still depend on an image snapshot followed by an unconditional controller write. |
| [`handoff_high_watermark`](../crates/verified/src/consensus.rs) returns the maximum of the old and new frontiers. | No host caller. | [`proof.json`](../verif/krabka_verified_rlib/consensus/handoff_high_watermark/proof.json) | None. |
| [`epoch_and_offset_for_entries`](../crates/verified/src/leader_epoch.rs) returns the KIP-101 leader epoch and offset where a follower truncates its divergent suffix. | [`krabka-log`](../crates/log/src/leader_epoch_checkpoint/lookup.rs) | [`proof.json`](../verif/krabka_verified_rlib/leader_epoch/epoch_and_offset_for_entries/proof.json) | Entries have strictly increasing leader epochs and start offsets. |
| [`offset_index_lookup`](../crates/verified/src/log_index.rs) returns the position for the greatest relative offset at or below the target. | [`krabka-log`](../crates/log/src/index/offset.rs) | [`proof.json`](../verif/krabka_verified_rlib/log_index/offset_index_lookup/proof.json) | Entries have strictly increasing relative offsets. |
| [`offset_index_position_at_or_after`](../crates/verified/src/log_index.rs) returns the position for the least relative offset at or above the target. | [`krabka-log`](../crates/log/src/index/offset.rs) | [`proof.json`](../verif/krabka_verified_rlib/log_index/offset_index_position_at_or_after/proof.json) | Entries have strictly increasing relative offsets. |
| [`time_index_lookup`](../crates/verified/src/log_index.rs) returns the relative offset for the last timestamp at or below the target. | [`krabka-log`](../crates/log/src/index/time.rs) | [`proof.json`](../verif/krabka_verified_rlib/log_index/time_index_lookup/proof.json) | Entries have nondecreasing timestamps. |
| [`isr_high_watermark`](../crates/verified/src/isr.rs) computes the minimum log end represented by a classic Kafka leader and its ISR followers. | [`krabka-broker`](../crates/broker/src/replica_state.rs) | [`proof.json`](../verif/krabka_verified_rlib/isr/isr_high_watermark/proof.json) | The host identifies the current leader, includes every other ISR member, and substitutes zero for missing follower progress during the leadership-change reseed window. |
| [`reserve_offsets`](../crates/verified/src/offset_allocator.rs) returns the base and advances the next offset by the count. | [`krabka-raft`](../crates/raft/src/kraft/controller/submit.rs) | [`proof.json`](../verif/krabka_verified_rlib/offset_allocator/reserve_offsets/proof.json) | `count >= 0`; `next + count <= i64::MAX`. |
| [`producer_decision`](../crates/verified/src/producer.rs) classifies an idempotent-producer batch as append, duplicate, out of order, or fenced. | [`krabka-broker`](../crates/broker/src/producer_state/decision.rs); [`increment_sequence`](../crates/verified/src/producer.rs) is also called by [`krabka-log`](../crates/log/src/log/open.rs). | [`proof.json`](../verif/krabka_verified_rlib/producer/producer_decision/proof.json) | None. |
| [`advance_high_watermark`](../crates/verified/src/raft.rs) advances a KRaft HWM monotonically without passing log end; [`in_half_open_window`](../crates/verified/src/raft.rs) and [`frontier_reaches`](../crates/verified/src/raft.rs) define commit, fetch, apply, and waiter boundaries. | [`krabka-raft`](../crates/raft/src/kraft/log.rs) and its controller [offset rules](../crates/raft/src/kraft/controller/offsets.rs) | [`advance_high_watermark/proof.json`](../verif/krabka_verified_rlib/raft/advance_high_watermark/proof.json), [`in_half_open_window/proof.json`](../verif/krabka_verified_rlib/raft/in_half_open_window/proof.json), [`frontier_reaches/proof.json`](../verif/krabka_verified_rlib/raft/frontier_reaches/proof.json) | The HWM bound requires the host invariant `previous <= log_end`; the predicates accept all integers. |
| [`remote_txn_overlap_decision`](../crates/verified/src/remote_txn.rs) classifies inclusive aborted-transaction and Fetch ranges exactly, rejecting either inverted interval. | The remote-storage [transaction-index reader](../crates/remote-storage/src/index/txn.rs), which also rejects payloads not divisible by the 24-byte entry size | [`proof.json`](../verif/krabka_verified_rlib/remote_txn/remote_txn_overlap_decision/proof.json) | The host decodes each big-endian entry field exactly and rejects a torn partial entry before calling the kernel. |
| [`restore_batch_step`](../crates/verified/src/restore.rs) accepts only a non-overlapping archived batch base and computes its exclusive end without overflow, while permitting gaps preserved by Kafka compaction; [`restore_record_coordinates`](../crates/verified/src/restore.rs) validates each record delta evaluated by an applicable bound/filter predicate and derives its absolute offset and timestamp without overflow. | The restore verifier's [record-batch walk](../crates/restore/src/verify/log_walk.rs) and the materializer's [bound/filter path](../crates/restore/src/materialize/prepare.rs) | [`batch proof`](../verif/krabka_verified_rlib/restore/restore_batch_step/proof.json), [`record proof`](../verif/krabka_verified_rlib/restore/restore_record_coordinates/proof.json) | Batch framing and CRC are validated first; record parsing and coordinate failures in the filtering path are surfaced as archive-integrity errors before selection or rewriting. |
| [`schema_failure_decision`](../crates/verified/src/schema.rs) permits unvalidated records only when fail-open is configured and the registry failure is transient. | The broker schema validator's [registry classifier](../crates/broker/src/schema_validation/validator/cache.rs) and [admission path](../crates/broker/src/schema_validation/validator/check.rs) | [`proof.json`](../verif/krabka_verified_rlib/schema/schema_failure_decision/proof.json) | The host classifies transport errors, HTTP 408/429, and 5xx as transient; unknown IDs, other statuses, and malformed successful responses fail closed. |
| [`site_loss_survivors`](../crates/verified/src/stretch.rs) computes the replica count after one site fails. | `krabka-verified` calls it from `min_insync_is_site_loss_safe`; the [`krabka-broker` stretch model](../crates/broker/src/stretch_cluster_model/config.rs) calls it directly. | [`proof.json`](../verif/krabka_verified_rlib/stretch/site_loss_survivors/proof.json) | `1 <= rf <= 1024`; `1 <= sites <= 1024`. |
| [`min_insync_is_site_loss_safe`](../crates/verified/src/stretch.rs) checks the lower and upper durability bounds for `min.insync.replicas`. | [`krabka-broker`](../crates/broker/src/config/stretch.rs) | [`proof.json`](../verif/krabka_verified_rlib/stretch/min_insync_is_site_loss_safe/proof.json) | `1 <= rf <= 1024`; `1 <= sites <= 1024`. |
| [`quorum_survives_any_single_site_loss`](../crates/verified/src/stretch.rs) checks that each one-site loss leaves a strict voter majority. | The [`krabka-broker` stretch model](../crates/broker/src/stretch_cluster_model/config.rs); no production caller. | [`proof.json`](../verif/krabka_verified_rlib/stretch/quorum_survives_any_single_site_loss/proof.json) | At most 1024 sites; each site has between 0 and 1024 voters. |
| [`plan_consume`](../crates/verified/src/throttle.rs) caps a refill, grants at most the request, and conserves the capped tokens. | [`krabka-throttle`](../crates/throttle/src/runtime/consume.rs) | [`proof.json`](../verif/krabka_verified_rlib/throttle/plan_consume/proof.json) | None. |
| [`transaction_completion_decision`](../crates/verified/src/transaction.rs) permits `EndTxn` completion only for the prepared producer identity and state, treats only the intended completed identity as idempotent, and rejects stale or conflicting state. | The broker's [`EndTxn` reacquisition guard](../crates/broker/src/txn/handlers/end_txn/reacquire.rs) and [completion decision](../crates/broker/src/txn/decision.rs) | [`proof.json`](../verif/krabka_verified_rlib/transaction/transaction_completion_decision/proof.json) | Prepare and complete state tags are distinct; the host maps Kafka transaction states and producer identities exactly into primitives. |
| [`wal_fetch_admission`](../crates/verified/src/wal.rs) fails closed unless authenticated and claimed nodes agree, the local node leads the placement, the claimant is a voter, and the requested epoch is current or unset. | [`krabka-broker`](../crates/broker/src/wal/quorum/registry.rs) | [`proof.json`](../verif/krabka_verified_rlib/wal/wal_fetch_admission/proof.json) | The host maps the authenticated identity and current placement exactly into primitive node IDs and voter order. |

Creusot also creates sessions for logic functions, lemmas, private helper
functions, and generated `Clone` implementations. These sessions support the
contracts in the ledger. They are not additional production kernels.

## Run the Creusot Proofs

The [`.creusot-version`](../.creusot-version) file pins the Creusot release.
That release pins the Rust nightly that `creusot-rustc` needs. Its
`creusot-setup/src/tools_versions_urls.rs` file pins the prover binaries and
checksums. Its `creusot-deps.opam` file pins Why3 and Why3find. The local
[`why3find.json`](../why3find.json) file selects the provers and sets the time
limits, search depth, tactics, and proof-search profile. It does not set a
memory limit.

Install the pinned Creusot release and its provers as the
[`proofs` job](../.github/workflows/ci.yml) does. Then run these commands from
the repository root:

```sh
export PATH="${HOME}/.local/share/creusot/bin:${PATH}"
python3 tools/test_creusot_packages.py
packages=(krabka-verified)
python3 tools/creusot_packages.py "${packages[@]}"
for package in "${packages[@]}"; do
  cargo creusot --package "${package}"
done
```

The package check fails if a workspace package that depends on `creusot-std`
or contains a Creusot contract is absent from the explicit proof list. Each
`cargo creusot` command updates its package directory under `verif/`. A
nonzero exit or an unproved goal fails the CI job. CI checks the proof result,
not a clean Git diff. Generated `.coma` files contain an absolute source path,
so another checkout can change them without a change to the proof result.

## Stateright Model-Check Tier

Stateright enumerates reachable states within explicit bounds. It can find a
counterexample in that state space. It does not prove behavior outside the
bounds. Each model states its bounds and properties in its module comments.

The current model entry points are:

| Area | Model entry points |
| :--- | :--- |
| Broker data and Fetch | [`data_path_model`](../crates/broker/src/data_path_model.rs), [`producer_state_model`](../crates/broker/src/producer_state_model.rs), [`replica_state_model`](../crates/broker/src/replica_state_model.rs), [`fetch_session_model`](../crates/broker/src/fetch_session_model.rs), and [`fetch_visibility_model`](../crates/broker/src/handlers/fetch_visibility_model.rs). |
| Broker failover and placement | [`leader_failover_model`](../crates/broker/src/leader_failover_model.rs), [`reassignment_model`](../crates/broker/src/reassignment_model.rs), [`stretch_cluster_model`](../crates/broker/src/stretch_cluster_model.rs), [`diskless_crash_model`](../crates/broker/src/diskless_crash_model.rs), and [`client_server_failover_model`](../crates/broker/src/client_server_failover_model.rs). |
| Broker groups and transactions | [`classic_state_model`](../crates/broker/src/coordinator/unified/classic_state_model.rs), [`consumer_group_composition_model`](../crates/broker/src/coordinator/unified/consumer_group_composition_model.rs), [`reconciler_model`](../crates/broker/src/coordinator/unified/reconciler_model.rs), [`decision_model`](../crates/broker/src/txn/decision_model.rs), [`eos_composition_model`](../crates/broker/src/txn/eos_composition_model.rs), and [`two_pc_model`](../crates/broker/src/txn/two_pc_model.rs). |
| Broker share and break-glass | [`share_partition/state_model`](../crates/broker/src/share_partition/state_model.rs), [`break_glass/state_model`](../crates/broker/src/break_glass/state_model.rs), and [`break_glass/cross_spend_model`](../crates/broker/src/break_glass/cross_spend_model.rs). |
| Log | [`compact_model`](../crates/log/src/compact_model.rs) and [`leader_epoch_model`](../crates/log/src/leader_epoch_model.rs). |
| KRaft | [`kraft_model`](../crates/raft/tests/kraft_model.rs). |
| Throttle | [`bucket_model`](../crates/throttle/tests/bucket_model.rs). |

Some models drive a production kernel directly. Other models compose real
decision functions with a small environment. Read the model's `DRIVEN` and
`MODELED` notes before you treat its property as a production guarantee.

## Outside Both Tiers

Code that is not in the Creusot ledger or the Stateright inventory is in
neither tier. This includes most protocol encoding and decoding, authorization,
network and async orchestration, file and object-store I/O, and operational
configuration. Unit, property, integration, differential, and container tests
cover those areas. Those tests are evidence, but they are not a Creusot proof
or a Stateright model check.

A proved kernel does not prove its caller. The proof starts after the caller
establishes the preconditions in the ledger. A Stateright model does not prove
the I/O around the decisions that it drives.
