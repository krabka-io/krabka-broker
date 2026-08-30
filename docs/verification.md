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
| [`fetch_visibility`](../crates/verified/src/broker.rs) computes Fetch bounds and response watermarks. | [`krabka-broker`](../crates/broker/src/handlers/fetch.rs) | [`proof.json`](../verif/krabka_verified_rlib/broker/fetch_visibility/proof.json) | `0 <= log_start <= hw <= log_end`; `log_start <= deliverable <= hw`. |
| [`delete_records_target`](../crates/verified/src/broker.rs) resolves the `-1` sentinel to the high watermark. | [`krabka-broker`](../crates/broker/src/handlers/delete_records/offsets.rs) | [`proof.json`](../verif/krabka_verified_rlib/broker/delete_records_target/proof.json) | None. |
| [`delete_records_offset_out_of_range`](../crates/verified/src/broker.rs) classifies targets below zero or above the log end. | [`krabka-broker`](../crates/broker/src/handlers/delete_records/offsets.rs) | [`proof.json`](../verif/krabka_verified_rlib/broker/delete_records_offset_out_of_range/proof.json) | None. |
| [`effective_share_backlog`](../crates/verified/src/broker.rs) computes a nonnegative, saturating backlog. | [`krabka-broker`](../crates/broker/src/share_partition/backlog_poller.rs) | [`proof.json`](../verif/krabka_verified_rlib/broker/effective_share_backlog/proof.json) | None. |
| [`compute_horizon`](../crates/verified/src/compaction.rs) adds retention time with saturation at the `i64` bounds. | `krabka-verified` calls it from `retain_decision`; [`krabka-log`](../crates/log/src/compact/decision.rs) also calls it in tests. | [`proof.json`](../verif/krabka_verified_rlib/compaction/compute_horizon/proof.json) | None. |
| [`retain_decision`](../crates/verified/src/compaction.rs) implements the KIP-534 record and control-marker retention rules. | [`krabka-log`](../crates/log/src/compact/decision.rs) | [`proof.json`](../verif/krabka_verified_rlib/compaction/retain_decision/proof.json) | None. |
| [`election_jitter_ms`](../crates/verified/src/consensus.rs) keeps deterministic jitter in `[0, base_ms)`. | [`krabka-kraft-core`](../crates/kraft-core/src/core.rs) | [`proof.json`](../verif/krabka_verified_rlib/consensus/election_jitter_ms/proof.json) | None. |
| [`log_is_up_to_date`](../crates/verified/src/consensus.rs) implements the KIP-595 candidate-log ordering. | [`krabka-kraft-core`](../crates/kraft-core/src/core/vote_request.rs) | [`proof.json`](../verif/krabka_verified_rlib/consensus/log_is_up_to_date/proof.json) | None. |
| [`recompute_high_watermark`](../crates/verified/src/consensus.rs) computes the majority offset, applies the epoch gate, and does not regress. | [`krabka-kraft-core`](../crates/kraft-core/src/core/replication.rs); the `krabka-broker` WAL [live engine](../crates/broker/src/wal/quorum/engine/distributed.rs) and [recovery path](../crates/broker/src/wal/quorum/engine/recovery.rs); the [diskless crash model](../crates/broker/src/diskless_crash_model.rs) | [`proof.json`](../verif/krabka_verified_rlib/consensus/recompute_high_watermark/proof.json) | `1 <= majority <= followers + 1`; `current_hwm <= log_end`; each follower offset is at most `log_end`. |
| [`handoff_high_watermark`](../crates/verified/src/consensus.rs) returns the maximum of the old and new frontiers. | No host caller. | [`proof.json`](../verif/krabka_verified_rlib/consensus/handoff_high_watermark/proof.json) | None. |
| [`offset_index_lookup`](../crates/verified/src/log_index.rs) returns the position for the greatest relative offset at or below the target. | [`krabka-log`](../crates/log/src/index/offset.rs) | [`proof.json`](../verif/krabka_verified_rlib/log_index/offset_index_lookup/proof.json) | Entries have strictly increasing relative offsets. |
| [`offset_index_position_at_or_after`](../crates/verified/src/log_index.rs) returns the position for the least relative offset at or above the target. | [`krabka-log`](../crates/log/src/index/offset.rs) | [`proof.json`](../verif/krabka_verified_rlib/log_index/offset_index_position_at_or_after/proof.json) | Entries have strictly increasing relative offsets. |
| [`time_index_lookup`](../crates/verified/src/log_index.rs) returns the relative offset for the last timestamp at or below the target. | [`krabka-log`](../crates/log/src/index/time.rs) | [`proof.json`](../verif/krabka_verified_rlib/log_index/time_index_lookup/proof.json) | Entries have nondecreasing timestamps. |
| [`reserve_offsets`](../crates/verified/src/offset_allocator.rs) returns the base and advances the next offset by the count. | [`krabka-raft`](../crates/raft/src/kraft/controller/submit.rs) | [`proof.json`](../verif/krabka_verified_rlib/offset_allocator/reserve_offsets/proof.json) | `count >= 0`; `next + count <= i64::MAX`. |
| [`producer_decision`](../crates/verified/src/producer.rs) classifies an idempotent-producer batch as append, duplicate, out of order, or fenced. | [`krabka-broker`](../crates/broker/src/producer_state/decision.rs); [`increment_sequence`](../crates/verified/src/producer.rs) is also called by [`krabka-log`](../crates/log/src/log/open.rs). | [`proof.json`](../verif/krabka_verified_rlib/producer/producer_decision/proof.json) | None. |
| [`site_loss_survivors`](../crates/verified/src/stretch.rs) computes the replica count after one site fails. | `krabka-verified` calls it from `min_insync_is_site_loss_safe`; the [`krabka-broker` stretch model](../crates/broker/src/stretch_cluster_model/config.rs) calls it directly. | [`proof.json`](../verif/krabka_verified_rlib/stretch/site_loss_survivors/proof.json) | `1 <= rf <= 1024`; `1 <= sites <= 1024`. |
| [`min_insync_is_site_loss_safe`](../crates/verified/src/stretch.rs) checks the lower and upper durability bounds for `min.insync.replicas`. | [`krabka-broker`](../crates/broker/src/config/stretch.rs) | [`proof.json`](../verif/krabka_verified_rlib/stretch/min_insync_is_site_loss_safe/proof.json) | `1 <= rf <= 1024`; `1 <= sites <= 1024`. |
| [`quorum_survives_any_single_site_loss`](../crates/verified/src/stretch.rs) checks that each one-site loss leaves a strict voter majority. | The [`krabka-broker` stretch model](../crates/broker/src/stretch_cluster_model/config.rs); no production caller. | [`proof.json`](../verif/krabka_verified_rlib/stretch/quorum_survives_any_single_site_loss/proof.json) | At most 1024 sites; each site has between 0 and 1024 voters. |
| [`plan_consume`](../crates/verified/src/throttle.rs) caps a refill, grants at most the request, and conserves the capped tokens. | [`krabka-throttle`](../crates/throttle/src/runtime/consume.rs) | [`proof.json`](../verif/krabka_verified_rlib/throttle/plan_consume/proof.json) | None. |

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
