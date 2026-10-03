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
//!   `prepare_server_abort_identities_with_fresh` at the cluster's transaction
//!   version, the one function the reaper and the `InitProducerId` fence
//!   prepare that abort with.
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
//!   the `InitProducerId` fence of an `Ongoing` transaction (the state becomes
//!   `PrepareAbort`), and `AddPartitionsToTxn` stamping `start_ms` when the
//!   transaction opens. The cluster's transaction version is a model
//!   parameter, `TV_2` and below it.
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
    handlers::end_txn::{
        completion_producer_identity, prepare_completion_identities_with_fresh,
        prepare_server_abort_identities_with_fresh,
    },
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
//
// `TV_2` and the versions below it reach the same states, since the epoch moves
// once at every version and the projection holds no field that the versions
// stamp differently. They differ in what the abort records, which
// `fence_matches_kafka` checks against Kafka's rule for each version.
const PINNED_UNIQUE_STATES_BASIC: usize = 21_876;

const PINNED_UNIQUE_STATES_WIDE: usize = 80_914;

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
    /// The cluster's transaction version, which the `InitProducerId` fence and
    /// the reaper abort at: `TV_2` bumps the epoch at completion, and the
    /// versions below it bump it in the fence.
    fence_version: TxnVersion,
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
    /// The abort the coordinator ran on an `Ongoing` transaction, in the
    /// `InitProducerId` fence or the reaper, left an entry that Kafka's
    /// `prepareFenceProducerEpoch` and server abort would not.
    FenceDiverged,
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

#[path = "two_pc_model/helpers.rs"]
mod helpers;
use helpers::{fenced_as_kafka, kafka_timed_out, project, rebuild, st};

#[path = "two_pc_model/transitions.rs"]
mod transitions;

#[path = "two_pc_model/checker.rs"]
mod checker;

#[path = "two_pc_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "two_pc_model/tests.rs"]
mod tests;
