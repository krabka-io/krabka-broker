//! Exhaustive stateright model of the KIP-98/EOS `EndTxn` decision core.
//!
//! The model runs one transactional-id through every interleaving of an
//! `EndTxn` handler split at its marker window (Phase 1, then Phase 3 after the
//! lock is dropped for the fan-out), `InitProducerId`, `AddPartitionsToTxn`,
//! and the completion task that finishes a durable `Prepare*` record. Design:
//! `crates/broker/docs/transaction-coordinator-design.md`.
//!
//! Headline safety, each an `always` property with a counterexample trace:
//!
//! - `fenced_end_txn_never_finalizes`: an `EndTxn` whose Phase 3 finds the
//!   entry no longer holding the identity, state and generation its Phase 1
//!   prepared never writes `Complete*`.
//! - `finalized_at_most_once`: each transaction generation reaches `Complete*`
//!   at most once, so it is never both committed and aborted, however the
//!   `EndTxn` Phase 3 and the completion task race.
//! - `init_never_overwrites_prepared`: `InitProducerId` never moves a
//!   transaction out of `Prepare*`. Kafka's `prepareInitProducerIdTransit`
//!   answers `CONCURRENT_TRANSACTIONS` there, because the markers for that
//!   decision may already sit on some partitions.
//!
//! A producer is fenced inside the window only through legal transitions: the
//! completion task finishes the prepared transaction, and then an
//! `InitProducerId` bump, a fence-abort, or a new transaction moves the entry
//! on while the original `EndTxn` still waits for its Phase 3. The
//! `fence_in_window` witness shows that this state is reached.
//!
//! What is DRIVEN (production code on every transition):
//!
//! - `EndTxn` Phase 1: `decide_phase1_transition` and
//!   `prepare_completion_identities_with_fresh`, and the completion identity
//!   from `completion_producer_identity`.
//! - `EndTxn` Phase 3: `decide_end_txn_completion`.
//! - `InitProducerId`: the `Prepare*` gate of `pending_completion_response`,
//!   which is `completion_for(entry.state)`; the epoch bump
//!   `krabka_verified::transaction::next_producer_identity`; and, for the
//!   fence of an `Ongoing` transaction, `prepare_server_abort_identities_with_fresh`
//!   at the cluster's transaction version, the one function the handler and the
//!   reaper prepare that abort with. Its epoch handling is the production one:
//!   one bump, at completion at `TV_2` and in the fence below it.
//! - Completion task: `completion_for`, `completion_decision` and
//!   `apply_completion` with `completion_producer_identity`.
//! - `AddPartitionsToTxn`: `TxnState::can_transition_to(Ongoing)`.
//!
//! What is MODELED (hand-written, mirroring the handler):
//!
//! - The `InitProducerId` request names no producer identity, so the KIP-360
//!   `is_fenced` / retry classification is not exercised, and a `Prepare*`
//!   entry always answers `CONCURRENT_TRANSACTIONS`, never `PRODUCER_FENCED`.
//!   The `keepPreparedTxn` (KIP-939 recovery) branch is not modeled.
//! - The fence of an `Ongoing` transaction moves the state to `PrepareAbort`
//!   in the handler's inline code, and the model runs it at `TV_2` and below
//!   it (`TxnModel::fence_version`). Its `CompleteAbort` is the same completion
//!   the completion task performs, so the model finishes it through the
//!   `Complete` action.
//! - The `EndTxn` state table (`end_txn_decision`), including the
//!   transaction-version-2 abort of a transaction with no partition, is not
//!   modeled: Phase 1 runs only from `Ongoing` at the entry's live identity.
//! - One `EndTxn` handler is in flight at a time; partitions and timestamps
//!   are omitted because no decision here reads them. The producer ID is fixed
//!   and the epoch cap stays far below the rotation boundary, so no staged
//!   identity arises and the projection loses nothing.
//!
//! Terminal outcomes are ghost records keyed by generation, the producer epoch
//! at which `AddPartitionsToTxn` opened the transaction. So a tid that commits
//! one generation and aborts the next is not a false violation.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! this module fences each run with `within_boundary` + `target_state_count`.
//! You MUST run each config under the host memory watchdog while you tune the
//! bounds.

use std::collections::BTreeSet;

use krabka_log::ProducerId;
use krabka_verified::transaction::TransactionReaperCompletionDecision;
use stateright::{Checker, Model, Property};

use super::{
    super::{
        coordinator::completion::{apply_completion, completion_decision, completion_for},
        handlers::end_txn::{
            completion_producer_identity, prepare_completion_identities_with_fresh,
            prepare_server_abort_identities_with_fresh,
        },
        state::{TxnEntry, TxnState},
        version::TxnVersion,
    },
    CompletionDecision, decide_end_txn_completion, decide_phase1_transition,
};

const MAX_STATES: usize = 200_000;

const MAX_DEPTH: usize = 60;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// `TV_2` and the versions below it reach the same states, since the epoch moves
// once at every version and the projection holds no field that the versions
// stamp differently. They differ in what the fence records, which
// `fence_matches_kafka` checks against Kafka's rule for each version.
const PINNED_UNIQUE_STATES_BASIC: usize = 231;

const PINNED_UNIQUE_STATES_WIDE: usize = 7_744;

const PID: ProducerId = ProducerId(1000);

struct TxnModel {
    max_epoch: i16,
    /// The cluster's transaction version, which the `InitProducerId` fence
    /// aborts at: `TV_2` bumps the epoch at completion, and the versions below
    /// it bump it in the fence (`TV_1` fences as `TV_0` does).
    fence_version: TxnVersion,
}

/// In-flight `EndTxn` captured at Phase 1. It waits for Phase 3 across the
/// marker window.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct PendingEnd {
    /// Ghost: the generation Phase 1 prepared.
    generation: i16,
    /// The identity of the persisted `Prepare*` snapshot.
    expected_pid: i64,
    expected_epoch: i16,
    /// The completion identity persisted with the `Prepare*` record.
    completion_pid: i64,
    completion_epoch: i16,
    prepare: i8, // TxnState::to_kafka_status()
    complete: i8,
}

/// One ghost terminal outcome.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
struct Finalized {
    generation: i16,
    committed: bool,
}

/// Ghost violations. The transition that commits one records it, and an
/// `always` property requires it to be absent, so the checker reports the
/// trace that led there.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Violation {
    /// Phase 3 wrote `Complete*` although the entry was not the snapshot its
    /// Phase 1 prepared.
    FencedEndTxnFinalized,
    /// Phase 3 rejected although the entry was still exactly its snapshot.
    UnjustifiedReject,
    /// A transition lowered the producer epoch.
    EpochRegressed,
    /// `InitProducerId` moved a `Prepare*` transaction.
    InitOverwrotePrepared,
    /// The fence of an `Ongoing` transaction left an entry that Kafka's
    /// `prepareFenceProducerEpoch` and server abort would not.
    FenceDiverged,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct TxnProj {
    pid: i64,
    epoch: i16,
    state: i8, // TxnState::to_kafka_status()
    /// Ghost: the producer epoch at which the live transaction was opened.
    generation: i16,
    pending: Option<PendingEnd>,
    /// Ghost: every finalization, sorted, duplicates kept so that
    /// `finalized_at_most_once` can see a second one.
    finalized: Vec<Finalized>,
    violations: BTreeSet<Violation>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum TxnAction {
    /// `InitProducerId` naming no producer identity.
    Init,
    /// `AddPartitionsToTxn`: → `Ongoing`.
    BeginTxn,
    /// `EndTxn` Phase 1 (`committed?`): → `Prepare*`, opens the window.
    EndTxnPhase1(bool),
    /// `EndTxn` Phase 3: revalidate, then `Complete*` or reject.
    EndTxnPhase3,
    /// The completion task (or the inline completion of an `InitProducerId`
    /// fence) finishes a durable `Prepare*` record.
    Complete,
}

#[path = "decision_model/helpers.rs"]
mod helpers;
use helpers::{fenced_as_kafka, finalize, is_prepared, project, rebuild, st};

#[path = "decision_model/transitions.rs"]
mod transitions;

#[path = "decision_model/checker.rs"]
mod checker;

#[path = "decision_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "decision_model/tests.rs"]
mod tests;
