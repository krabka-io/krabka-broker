//! Exhaustive stateright model of the KIP-939 2PC timeout-safety property.
//!
//! The model runs one transactional-id through every interleaving of
//! `InitProducerId` (with and without `enable2Pc`, and with valid and invalid
//! requested timeouts), `AddPartitionsToTxn`, `EndTxn`, the completion of a
//! durable `Prepare*` record, the idle-transaction reaper, and a monotonic
//! clock that visits each timeout's boundary: `start + timeout` and
//! `start + timeout + 1`.
//!
//! Headline safety (KIP-939), `two_pc_never_reaped`: **the timeout reaper
//! never aborts a transaction opened by a producer that asked for 2PC.** The
//! ghost that records "this generation asked for 2PC" is the request flag, not
//! the persisted timeout, so a wrong encoding in `resolve_txn_timeout` fails
//! the property instead of being assumed.
//!
//! The reaper's rule is also checked against Kafka's
//! `TransactionStateManager.timedOutTransactions` restated independently from
//! the request (`Ongoing`, not 2PC, and `start + requested < now` in exact
//! arithmetic): `reaper_never_early` and `reaper_never_misses` fail on any
//! sweep where the production decision differs.
//!
//! Secondary safety: generations finalize in strictly increasing order, so a
//! generation is finalized at most once and never both committed and aborted,
//! with the reaper, `EndTxn`, the `InitProducerId` fence and the completion
//! task interleaved.
//!
//! What is DRIVEN (production code on every transition):
//!
//! - The timeout encoder `resolve_txn_timeout`, against
//!   `transaction.max.timeout.ms` = [`MAX_TIMEOUT_MS`].
//! - The reaper decision `should_abort_idle_txn` over the persisted entry and
//!   the model clock, and the prepared abort's
//!   `prepare_completion_identities_with_fresh`.
//! - `EndTxn` Phase 1: `decide_phase1_transition` and
//!   `prepare_completion_identities_with_fresh`.
//! - Completion: `completion_for`, `completion_decision`, `apply_completion`
//!   and `completion_producer_identity`, so every epoch bump on completion is
//!   the production one.
//! - `InitProducerId`: the `Prepare*` gate (`completion_for`, as
//!   `pending_completion_response` uses it) and the epoch bump
//!   `krabka_verified::transaction::next_producer_identity`.
//! - `AddPartitionsToTxn`: `TxnState::can_transition_to(Ongoing)`.
//!
//! What is MODELED (hand-written, mirroring the handlers):
//!
//! - The reaper's `apply_prepare_abort` (the state becomes `PrepareAbort`),
//!   the `InitProducerId` fence of an `Ongoing` transaction (epoch `+ 1`,
//!   `PrepareAbort`), and `AddPartitionsToTxn` stamping `start_ms` when the
//!   transaction opens.
//! - Each `Prepare*` completes atomically in one `Complete` action that
//!   stands for the `EndTxn` Phase 3, the reaper's `complete_abort`, and the
//!   completion task alike. `decision_model` covers the marker window.
//! - The `InitProducerId` request names no producer identity, and the
//!   `keepPreparedTxn` recovery branch is not modeled.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! each run is fenced with `within_boundary` and `target_state_count`.

use std::collections::BTreeSet;

use krabka_log::ProducerId;
use krabka_verified::transaction::TransactionReaperCompletionDecision;
use stateright::{Checker, Model, Property};

use super::{
    coordinator::completion::{apply_completion, completion_decision, completion_for},
    decision::decide_phase1_transition,
    handlers::end_txn::{completion_producer_identity, prepare_completion_identities_with_fresh},
    state::{TxnEntry, TxnState},
    two_pc::{resolve_txn_timeout, should_abort_idle_txn},
    version::TxnVersion,
};

const MAX_STATES: usize = 1_000_000;
const MAX_DEPTH: usize = 80;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_BASIC: usize = 23_124;
const PINNED_UNIQUE_STATES_WIDE: usize = 93_754;

const PID: ProducerId = ProducerId(1000);
/// `transaction.max.timeout.ms`, Kafka's default.
const MAX_TIMEOUT_MS: i32 = 900_000;
/// The classic timeout the tid's first `InitProducerId` asked for.
const FIRST_TIMEOUT_MS: i32 = 60_000;

/// The requested `TransactionTimeoutMs` values: two valid timeouts, the
/// largest valid one, and the invalid values on either side of the range.
const REQUESTS: [i32; 5] = [1, FIRST_TIMEOUT_MS, MAX_TIMEOUT_MS, MAX_TIMEOUT_MS + 1, 0];

/// The clock's instants. A transaction opened at 0 meets each requested
/// timeout's `start + timeout` and `start + timeout + 1`, and the 2PC
/// sentinel's too, before the clock ends at `i64::MAX`.
const CLOCK: [i64; 10] = [
    0,
    1,
    2,
    60_000,
    60_001,
    900_000,
    900_001,
    2_147_483_647,
    2_147_483_648,
    i64::MAX,
];

struct TwoPcModel {
    max_epoch: i16,
}

/// Ghost violations. The transition that commits one records it, and an
/// `always` property requires it to be absent, so the checker reports the
/// trace that led there.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Violation {
    /// The reaper aborted a transaction whose producer asked for 2PC.
    ReapedTwoPc,
    /// The reaper aborted a transaction Kafka's rule does not time out.
    ReapedEarly,
    /// The reaper spared a transaction Kafka's rule times out.
    MissedReap,
    /// A generation finalized at or below one that already finalized.
    FinalizedOutOfOrder,
    /// A transition lowered the producer epoch.
    EpochRegressed,
}

/// Ghost non-vacuity witnesses.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Witness {
    Committed,
    ReapedClassic,
    /// The reaper aborted at exactly `start + requested + 1`.
    ReapedOnePastTimeout,
    /// The reaper swept a classic transaction at exactly
    /// `start + requested` and spared it.
    SparedAtTimeout,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct TwoPcProj {
    pid: i64,
    epoch: i16,
    state: i8, // TxnState::to_kafka_status()
    /// The persisted `TransactionTimeoutMs`.
    timeout_ms: i32,
    /// The persisted transaction start.
    start_ms: i64,
    /// Index into [`CLOCK`].
    clock: usize,
    /// Ghost: the last successful `InitProducerId` asked for 2PC.
    enable_2pc: bool,
    /// Ghost: the timeout that `InitProducerId` asked for.
    requested_ms: i32,
    /// Ghost: the producer epoch at which the live transaction was opened.
    generation: i16,
    /// Ghost: the last generation that finalized.
    last_finalized: Option<i16>,
    violations: BTreeSet<Violation>,
    witnesses: BTreeSet<Witness>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum TwoPcAction {
    /// `InitProducerId(enable2Pc, TransactionTimeoutMs)` naming no identity.
    Init(bool, i32),
    /// `AddPartitionsToTxn`: → `Ongoing`.
    BeginTxn,
    /// `EndTxn` Phase 1 (`committed?`): → `Prepare*`.
    EndTxn(bool),
    /// A durable `Prepare*` completes.
    Complete,
    /// The idle-transaction reaper sweeps at the current clock.
    TimeoutSweep,
    /// The clock advances to its next instant.
    Tick,
}

fn st(id: i8) -> TxnState {
    TxnState::from_kafka_status(id).expect("valid TxnState id in model")
}

/// Reconstructs the real `TxnEntry` so the real decision functions behave as
/// in a live run. Partitions do not change these decisions.
fn rebuild(s: &TwoPcProj) -> TxnEntry {
    let mut e = TxnEntry::new_empty(
        "tid".to_string(),
        ProducerId(s.pid),
        s.epoch,
        s.timeout_ms,
        1,
    );
    e.state = st(s.state);
    e.start_ms = s.start_ms;
    e
}

/// Writes the persisted fields of `entry` back into the projection.
fn project(s: &mut TwoPcProj, entry: &TxnEntry) {
    s.pid = entry.producer_id.get();
    s.epoch = entry.producer_epoch;
    s.state = entry.state.to_kafka_status();
    s.timeout_ms = entry.txn_timeout_ms;
    s.start_ms = entry.start_ms;
}

/// Kafka's `timedOutTransactions` rule, restated from the request rather than
/// the persisted timeout: an `Ongoing` transaction of a producer that did not
/// ask for 2PC times out once `start + requested < now`, in exact arithmetic.
fn kafka_timed_out(s: &TwoPcProj, now_ms: i64) -> bool {
    st(s.state) == TxnState::Ongoing
        && !s.enable_2pc
        && i128::from(s.start_ms) + i128::from(s.requested_ms) < i128::from(now_ms)
}

impl TwoPcModel {
    fn init(s: &mut TwoPcProj, enable_2pc: bool, requested_ms: i32) -> Option<()> {
        // The handler resolves the timeout before it reads the entry, and
        // answers INVALID_TRANSACTION_TIMEOUT when Kafka refuses it.
        let timeout_ms = resolve_txn_timeout(enable_2pc, requested_ms, MAX_TIMEOUT_MS).ok()?;
        let mut entry = rebuild(s);
        // `pending_completion_response`: CONCURRENT_TRANSACTIONS, no write.
        if completion_for(entry.state).is_some() {
            return None;
        }
        if entry.state == TxnState::Ongoing {
            // `prepareFenceProducerEpoch`, then the abort's completion
            // identity. The client retries after the completion, so this call
            // does not apply its own timeout.
            entry.state = TxnState::PrepareAbort;
            entry.producer_epoch += 1;
            prepare_completion_identities_with_fresh(&mut entry, TxnVersion::Verified, None)
                .expect("model epochs never reach the rotation boundary");
        } else {
            let (pid, epoch) = krabka_verified::transaction::next_producer_identity(
                true,
                false,
                entry.producer_id.get(),
                entry.producer_epoch,
                None,
            )
            .expect("model epochs never reach the rotation boundary");
            entry = TxnEntry::new_empty(
                "tid".to_string(),
                ProducerId(pid),
                epoch,
                timeout_ms,
                CLOCK[s.clock],
            );
            s.enable_2pc = enable_2pc;
            s.requested_ms = requested_ms;
        }
        project(s, &entry);
        Some(())
    }

    fn begin(s: &mut TwoPcProj) -> Option<()> {
        let prior = st(s.state);
        if !prior.can_transition_to(TxnState::Ongoing) {
            return None;
        }
        if prior != TxnState::Ongoing {
            s.start_ms = CLOCK[s.clock];
            s.generation = s.epoch;
        }
        s.state = TxnState::Ongoing.to_kafka_status();
        Some(())
    }

    fn end_txn(s: &mut TwoPcProj, committed: bool) -> Option<()> {
        let mut entry = rebuild(s);
        decide_phase1_transition(&mut entry, committed).ok()?;
        prepare_completion_identities_with_fresh(&mut entry, TxnVersion::Verified, None)
            .expect("model epochs never reach the rotation boundary");
        project(s, &entry);
        Some(())
    }

    fn complete(s: &mut TwoPcProj) -> Option<()> {
        let entry = rebuild(s);
        let (_, complete) = completion_for(entry.state)?;
        match completion_decision(&entry, &entry, (entry.state, complete)) {
            TransactionReaperCompletionDecision::Proceed => {}
            TransactionReaperCompletionDecision::AlreadyComplete
            | TransactionReaperCompletionDecision::RejectMalformed
            | TransactionReaperCompletionDecision::RejectStaleIdentity
            | TransactionReaperCompletionDecision::RejectChangedPreparedState => return None,
        }
        let mut completed = entry.clone();
        let identity = completion_producer_identity(&completed);
        apply_completion(&mut completed, complete, identity, CLOCK[s.clock]);
        if s.last_finalized.is_some_and(|last| s.generation <= last) {
            s.violations.insert(Violation::FinalizedOutOfOrder);
        }
        s.last_finalized = Some(s.generation);
        if complete == TxnState::CompleteCommit {
            s.witnesses.insert(Witness::Committed);
        }
        project(s, &completed);
        Some(())
    }

    fn sweep(s: &mut TwoPcProj) -> Option<()> {
        let now_ms = CLOCK[s.clock];
        let mut entry = rebuild(s);
        let reaps =
            should_abort_idle_txn(entry.state, entry.txn_timeout_ms, entry.start_ms, now_ms);
        let kafka = kafka_timed_out(s, now_ms);
        if !reaps {
            if kafka {
                s.violations.insert(Violation::MissedReap);
                return Some(());
            }
            if st(s.state) == TxnState::Ongoing
                && !s.enable_2pc
                && i128::from(s.start_ms) + i128::from(s.requested_ms) == i128::from(now_ms)
                && !s.witnesses.contains(&Witness::SparedAtTimeout)
            {
                s.witnesses.insert(Witness::SparedAtTimeout);
                return Some(());
            }
            return None;
        }
        if s.enable_2pc {
            s.violations.insert(Violation::ReapedTwoPc);
        }
        if !kafka {
            s.violations.insert(Violation::ReapedEarly);
        }
        if !s.enable_2pc {
            s.witnesses.insert(Witness::ReapedClassic);
        }
        if i128::from(s.start_ms) + i128::from(s.requested_ms) + 1 == i128::from(now_ms) {
            s.witnesses.insert(Witness::ReapedOnePastTimeout);
        }
        // `apply_prepare_abort`, then the abort's completion identity.
        entry.state = TxnState::PrepareAbort;
        prepare_completion_identities_with_fresh(&mut entry, TxnVersion::Verified, None)
            .expect("model epochs never reach the rotation boundary");
        project(s, &entry);
        Some(())
    }
}

impl Model for TwoPcModel {
    type State = TwoPcProj;
    type Action = TwoPcAction;

    fn init_states(&self) -> Vec<Self::State> {
        // A tid that completed its first, classic InitProducerId at time 0.
        let timeout_ms = resolve_txn_timeout(false, FIRST_TIMEOUT_MS, MAX_TIMEOUT_MS)
            .expect("the first timeout is valid");
        let entry = TxnEntry::new_empty("tid".to_string(), PID, 0, timeout_ms, CLOCK[0]);
        let mut s = TwoPcProj {
            pid: 0,
            epoch: 0,
            state: 0,
            timeout_ms: 0,
            start_ms: 0,
            clock: 0,
            enable_2pc: false,
            requested_ms: FIRST_TIMEOUT_MS,
            generation: 0,
            last_finalized: None,
            violations: BTreeSet::new(),
            witnesses: BTreeSet::new(),
        };
        project(&mut s, &entry);
        vec![s]
    }

    fn actions(&self, _: &Self::State, actions: &mut Vec<Self::Action>) {
        // Every action is offered in every state; the production decisions
        // in `next_state` refuse the ones that do not apply.
        for requested in REQUESTS {
            actions.push(TwoPcAction::Init(false, requested));
        }
        // The requested timeout is not read under 2PC, valid or not.
        actions.push(TwoPcAction::Init(true, FIRST_TIMEOUT_MS));
        actions.push(TwoPcAction::Init(true, 0));
        actions.extend([
            TwoPcAction::BeginTxn,
            TwoPcAction::EndTxn(true),
            TwoPcAction::EndTxn(false),
            TwoPcAction::Complete,
            TwoPcAction::TimeoutSweep,
            TwoPcAction::Tick,
        ]);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut s = last.clone();
        match action {
            TwoPcAction::Init(enable_2pc, requested) => Self::init(&mut s, enable_2pc, requested)?,
            TwoPcAction::BeginTxn => Self::begin(&mut s)?,
            TwoPcAction::EndTxn(committed) => Self::end_txn(&mut s, committed)?,
            TwoPcAction::Complete => Self::complete(&mut s)?,
            TwoPcAction::TimeoutSweep => Self::sweep(&mut s)?,
            TwoPcAction::Tick => {
                if s.clock + 1 >= CLOCK.len() {
                    return None;
                }
                s.clock += 1;
            }
        }
        if s.epoch < last.epoch {
            s.violations.insert(Violation::EpochRegressed);
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // HEADLINE (KIP-939): the timeout reaper never aborts a 2PC txn.
            Property::always("two_pc_never_reaped", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::ReapedTwoPc)
            }),
            // The reaper aborts exactly when Kafka's rule times out.
            Property::always("reaper_never_early", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::ReapedEarly)
            }),
            Property::always("reaper_never_misses", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::MissedReap)
            }),
            // Generations finalize once each, in order.
            Property::always("finalized_at_most_once", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::FinalizedOutOfOrder)
            }),
            Property::always("epoch_never_regresses", |_, s: &TwoPcProj| {
                !s.violations.contains(&Violation::EpochRegressed)
            }),
            // Non-vacuity: the reaper aborts classic transactions, and does so
            // one millisecond past the timeout but not at it.
            Property::sometimes("reaper_aborts_classic", |_, s: &TwoPcProj| {
                s.witnesses.contains(&Witness::ReapedClassic)
            }),
            Property::sometimes("reaped_one_past_timeout", |_, s: &TwoPcProj| {
                s.witnesses.contains(&Witness::ReapedOnePastTimeout)
            }),
            Property::sometimes("spared_at_timeout", |_, s: &TwoPcProj| {
                s.witnesses.contains(&Witness::SparedAtTimeout)
            }),
            // Non-vacuity: a 2PC transaction stays open past the instant its
            // sentinel timeout would have expired.
            Property::sometimes("two_pc_open_past_timeout", |_, s: &TwoPcProj| {
                s.enable_2pc
                    && st(s.state) == TxnState::Ongoing
                    && i128::from(s.start_ms) + i128::from(i32::MAX) < i128::from(CLOCK[s.clock])
            }),
            Property::sometimes("can_commit", |_, s: &TwoPcProj| {
                s.witnesses.contains(&Witness::Committed)
            }),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.epoch <= self.max_epoch
    }
}

fn run(model: TwoPcModel, label: &str, pinned_unique_states: usize) {
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
fn two_pc_basic() {
    run(
        TwoPcModel { max_epoch: 3 },
        "two_pc_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn two_pc_wide() {
    // More generations → deeper classic↔2PC alternations and reaper interleaves.
    run(
        TwoPcModel { max_epoch: 5 },
        "two_pc_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
