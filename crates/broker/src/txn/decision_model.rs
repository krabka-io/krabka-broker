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

fn st(id: i8) -> TxnState {
    TxnState::from_kafka_status(id).expect("valid TxnState id in model")
}

/// Reconstructs the real `TxnEntry` from the projection, so the real decision
/// fns behave as in a live run.
fn rebuild(s: &TxnProj) -> TxnEntry {
    let mut e = TxnEntry::new_empty("tid".to_string(), ProducerId(s.pid), s.epoch, 60_000, 1);
    e.state = st(s.state);
    e
}

/// Writes the persisted fields of `entry` back into the projection.
fn project(s: &mut TxnProj, entry: &TxnEntry) {
    s.pid = entry.producer_id.get();
    s.epoch = entry.producer_epoch;
    s.state = entry.state.to_kafka_status();
}

fn finalize(s: &mut TxnProj, generation: i16, complete: TxnState) {
    s.finalized.push(Finalized {
        generation,
        committed: complete == TxnState::CompleteCommit,
    });
    s.finalized.sort_unstable();
}

fn is_prepared(state: TxnState) -> bool {
    matches!(state, TxnState::PrepareCommit | TxnState::PrepareAbort)
}

/// Kafka's fence of an `Ongoing` transaction, restated from
/// `TransactionMetadata` (`prepareFenceProducerEpoch`, then `prepareAbortOrCommit`
/// from `endTransaction(isFromClient = false)` at the cluster's version)
/// instead of read back from the function under test. The producer epoch moves
/// from `held` to `held + 1` once, and the producer continues at that epoch.
/// `TV_2` records the epoch it held as the last epoch, and stamps `TV_2`. Below
/// it the last epoch is cleared and the record carries `TV_0`.
fn fenced_as_kafka(entry: &TxnEntry, held: i16, version: TxnVersion) -> bool {
    let verified = version == TxnVersion::Verified;
    entry.producer_epoch == held + 1
        && entry.last_producer_epoch == if verified { held } else { -1 }
        && entry.client_transaction_version == if verified { 2 } else { 0 }
        && completion_producer_identity(entry) == (entry.producer_id, held + 1)
}

impl TxnModel {
    /// `InitProducerId` over the live entry, in the handler's order.
    fn init(&self, s: &mut TxnProj) -> Option<()> {
        let mut entry = rebuild(s);
        // `pending_completion_response`: a durable `Prepare*` answers
        // `CONCURRENT_TRANSACTIONS` to a request that names no identity, and
        // nothing is written.
        if completion_for(entry.state).is_some() {
            return None;
        }
        if entry.state == TxnState::Ongoing {
            // `prepareFenceProducerEpoch` and the server's abort at the
            // cluster's version: the epoch moves once, whichever version. The
            // client is answered `CONCURRENT_TRANSACTIONS` and retries after
            // the completion.
            let held = entry.producer_epoch;
            entry.state = TxnState::PrepareAbort;
            prepare_server_abort_identities_with_fresh(&mut entry, self.fence_version, None)
                .expect("model epochs never reach the rotation boundary");
            if !fenced_as_kafka(&entry, held, self.fence_version) {
                s.violations.insert(Violation::FenceDiverged);
            }
        } else {
            // `prepareIncrementProducerEpoch` on a terminal or empty entry.
            let (pid, epoch) = krabka_verified::transaction::next_producer_identity(
                true,
                false,
                entry.producer_id.get(),
                entry.producer_epoch,
                None,
            )
            .expect("model epochs never reach the rotation boundary");
            entry = TxnEntry::new_empty("tid".to_string(), ProducerId(pid), epoch, 60_000, 1);
        }
        project(s, &entry);
        Some(())
    }

    fn begin(s: &mut TxnProj) -> Option<()> {
        let prior = st(s.state);
        if !prior.can_transition_to(TxnState::Ongoing) {
            return None;
        }
        if prior != TxnState::Ongoing {
            s.generation = s.epoch;
        }
        s.state = TxnState::Ongoing.to_kafka_status();
        Some(())
    }

    fn end_txn_phase1(s: &mut TxnProj, committed: bool) -> Option<()> {
        if s.pending.is_some() {
            return None;
        }
        let mut entry = rebuild(s);
        let (prepare, complete) = decide_phase1_transition(&mut entry, committed).ok()?;
        prepare_completion_identities_with_fresh(&mut entry, TxnVersion::Verified, None)
            .expect("model epochs never reach the rotation boundary");
        let (completion_pid, completion_epoch) = completion_producer_identity(&entry);
        s.pending = Some(PendingEnd {
            generation: s.generation,
            expected_pid: entry.producer_id.get(),
            expected_epoch: entry.producer_epoch,
            completion_pid: completion_pid.get(),
            completion_epoch,
            prepare: prepare.to_kafka_status(),
            complete: complete.to_kafka_status(),
        });
        project(s, &entry);
        Some(())
    }

    fn end_txn_phase3(s: &mut TxnProj) -> Option<()> {
        let p = s.pending.take()?;
        let entry = rebuild(s);
        // Independent of the decision core: is the entry still exactly the
        // snapshot Phase 1 persisted, for the generation it prepared?
        let still_prepared = s.pid == p.expected_pid
            && s.epoch == p.expected_epoch
            && s.state == p.prepare
            && s.generation == p.generation;
        match decide_end_txn_completion(
            &entry,
            ProducerId(p.expected_pid),
            p.expected_epoch,
            ProducerId(p.completion_pid),
            p.completion_epoch,
            st(p.prepare),
            st(p.complete),
        ) {
            CompletionDecision::Proceed {
                next_state,
                response_pid,
                response_epoch,
            } => {
                if !still_prepared {
                    s.violations.insert(Violation::FencedEndTxnFinalized);
                }
                finalize(s, p.generation, next_state);
                s.pid = response_pid.get();
                s.epoch = response_epoch;
                s.state = next_state.to_kafka_status();
            }
            // Idempotent retry or a lost race with the completion task: the
            // handler answers success and writes nothing.
            CompletionDecision::AlreadyComplete { .. } => {}
            CompletionDecision::Reject(_) => {
                if still_prepared {
                    s.violations.insert(Violation::UnjustifiedReject);
                }
            }
        }
        Some(())
    }

    fn complete(s: &mut TxnProj) -> Option<()> {
        let entry = rebuild(s);
        let (_, complete) = completion_for(entry.state)?;
        // The model's completion runs atomically, so the prepared snapshot is
        // the live entry.
        match completion_decision(&entry, &entry, (entry.state, complete)) {
            TransactionReaperCompletionDecision::Proceed => {
                let mut completed = entry.clone();
                let identity = completion_producer_identity(&completed);
                apply_completion(&mut completed, complete, identity, 1);
                finalize(s, s.generation, complete);
                project(s, &completed);
                Some(())
            }
            TransactionReaperCompletionDecision::AlreadyComplete
            | TransactionReaperCompletionDecision::RejectMalformed
            | TransactionReaperCompletionDecision::RejectStaleIdentity
            | TransactionReaperCompletionDecision::RejectChangedPreparedState => None,
        }
    }
}

impl Model for TxnModel {
    type State = TxnProj;
    type Action = TxnAction;

    fn init_states(&self) -> Vec<Self::State> {
        // A tid that has completed its first InitProducerId: epoch 0, Empty.
        vec![TxnProj {
            pid: PID.get(),
            epoch: 0,
            state: TxnState::Empty.to_kafka_status(),
            generation: 0,
            pending: None,
            finalized: vec![],
            violations: BTreeSet::new(),
        }]
    }

    fn actions(&self, _: &Self::State, actions: &mut Vec<Self::Action>) {
        // Every action is offered in every state; the production decisions
        // in `next_state` refuse the ones that do not apply.
        actions.extend([
            TxnAction::Init,
            TxnAction::BeginTxn,
            TxnAction::EndTxnPhase1(true),
            TxnAction::EndTxnPhase1(false),
            TxnAction::EndTxnPhase3,
            TxnAction::Complete,
        ]);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            TxnAction::Init => self.init(&mut s)?,
            TxnAction::BeginTxn => Self::begin(&mut s)?,
            TxnAction::EndTxnPhase1(committed) => Self::end_txn_phase1(&mut s, committed)?,
            TxnAction::EndTxnPhase3 => Self::end_txn_phase3(&mut s)?,
            TxnAction::Complete => Self::complete(&mut s)?,
        }
        if action == TxnAction::Init && is_prepared(st(last.state)) {
            s.violations.insert(Violation::InitOverwrotePrepared);
        }
        if s.epoch < last.epoch {
            s.violations.insert(Violation::EpochRegressed);
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE: a fenced or overtaken EndTxn never writes Complete*.
            Property::always("fenced_end_txn_never_finalizes", |_, s: &TxnProj| {
                !s.violations.contains(&Violation::FencedEndTxnFinalized)
            }),
            // HEADLINE: each generation finalizes at most once, so it is never
            // both committed and aborted.
            Property::always("finalized_at_most_once", |_, s: &TxnProj| {
                s.finalized
                    .windows(2)
                    .all(|pair| pair[0].generation != pair[1].generation)
            }),
            // HEADLINE: InitProducerId answers CONCURRENT_TRANSACTIONS to a
            // prepared transaction instead of moving it.
            Property::always("init_never_overwrites_prepared", |_, s: &TxnProj| {
                !s.violations.contains(&Violation::InitOverwrotePrepared)
            }),
            // The fence of an `Ongoing` transaction raises the epoch once and
            // stamps what Kafka does at the cluster's version.
            Property::always("fence_matches_kafka", |_, s: &TxnProj| {
                !s.violations.contains(&Violation::FenceDiverged)
            }),
            // Phase 3 rejects only an entry that moved underneath it.
            Property::always("reject_is_justified", |_, s: &TxnProj| {
                !s.violations.contains(&Violation::UnjustifiedReject)
            }),
            Property::always("epoch_never_regresses", |_, s: &TxnProj| {
                !s.violations.contains(&Violation::EpochRegressed)
            }),
            // Non-vacuity: an EndTxn commit and an InitProducerId fence-abort
            // both finalize.
            Property::sometimes("can_commit", |_, s: &TxnProj| {
                s.finalized.iter().any(|f| f.committed)
            }),
            Property::sometimes("can_abort", |_, s: &TxnProj| {
                s.finalized.iter().any(|f| !f.committed)
            }),
            // Non-vacuity: the entry's epoch moves past a pending EndTxn's
            // prepared epoch while it waits for Phase 3 -- the zombie window.
            Property::sometimes("fence_in_window", |_, s: &TxnProj| {
                s.pending.is_some_and(|p| p.expected_epoch < s.epoch)
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.epoch <= self.max_epoch
    }
}

fn run(model: TxnModel, label: &str, pinned_unique_states: usize) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert2::assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    checker.assert_properties();
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
}

#[test]
fn txn_basic() {
    // One tid, epoch 0..=3: every interleaving of Init / BeginTxn / EndTxn
    // Phase1 / Phase3 / Complete, including an overtaken EndTxn in the window.
    run(
        TxnModel {
            max_epoch: 3,
            fence_version: TxnVersion::Verified,
        },
        "txn_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn txn_wide() {
    // More producer-epoch generations → deeper commit/abort/fence interleavings.
    run(
        TxnModel {
            max_epoch: 6,
            fence_version: TxnVersion::Verified,
        },
        "txn_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}

#[test]
fn txn_basic_below_tv2_fence() {
    // The same interleavings with a cluster below `TV_2`: the fence of an
    // `Ongoing` transaction raises the epoch itself.
    run(
        TxnModel {
            max_epoch: 3,
            fence_version: TxnVersion::Classic,
        },
        "txn_basic_below_tv2_fence",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn txn_wide_below_tv2_fence() {
    run(
        TxnModel {
            max_epoch: 6,
            fence_version: TxnVersion::Classic,
        },
        "txn_wide_below_tv2_fence",
        PINNED_UNIQUE_STATES_WIDE,
    );
}

/// `InitProducerId` against each state of the live entry: the `Prepare*`
/// states refuse (Kafka's `CONCURRENT_TRANSACTIONS`), `Ongoing` is fenced and
/// prepared for abort, and the rest bump the epoch into `Empty`. The fence
/// raises the epoch once, at every cluster version.
#[test]
fn init_producer_id_by_state() {
    let at = |state: TxnState| TxnProj {
        pid: PID.get(),
        epoch: 2,
        state: state.to_kafka_status(),
        generation: 2,
        pending: None,
        finalized: vec![],
        violations: BTreeSet::new(),
    };
    let rows = [
        (TxnState::PrepareCommit, None),
        (TxnState::PrepareAbort, None),
        (
            TxnState::Ongoing,
            Some(TxnProj {
                // Kafka bumps the epoch of a fenced producer once: 2 → 3, in
                // the completion at `TV_2` and in the fence below it.
                epoch: 3,
                state: TxnState::PrepareAbort.to_kafka_status(),
                ..at(TxnState::Ongoing)
            }),
        ),
        (
            TxnState::Empty,
            Some(TxnProj {
                epoch: 3,
                ..at(TxnState::Empty)
            }),
        ),
        (
            TxnState::CompleteCommit,
            Some(TxnProj {
                epoch: 3,
                state: TxnState::Empty.to_kafka_status(),
                ..at(TxnState::CompleteCommit)
            }),
        ),
        (
            TxnState::CompleteAbort,
            Some(TxnProj {
                epoch: 3,
                state: TxnState::Empty.to_kafka_status(),
                ..at(TxnState::CompleteAbort)
            }),
        ),
    ];
    for fence_version in [TxnVersion::Verified, TxnVersion::Classic] {
        let model = TxnModel {
            max_epoch: 6,
            fence_version,
        };
        for (state, expected) in &rows {
            assert2::assert!(
                model.next_state(&at(*state), TxnAction::Init) == *expected,
                "{fence_version:?} {state:?}"
            );
        }
    }
}
