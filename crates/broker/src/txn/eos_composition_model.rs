//! COMPOSITIONAL model of the exactly-once READ visibility algebra.
//!
//! This is the second end-to-end model, after the data-path composition. It
//! runs over a single partition with >= 2 interleaving transactional producers
//! and an advancing HWM. It verifies that what a `read_committed` consumer may
//! see is EXACTLY the committed records below `min(lso, hw)`:
//!
//! - `only_committed_visible`: every visible batch belongs to a committed
//!   transaction, so no open and no aborted record is visible.
//! - `window_is_min_lso_hw`: the window the real `compute_visibility_window`
//!   returns equals `min(first unstable offset, hw)`, with the first unstable
//!   offset recomputed from each producer's control markers, not from `lso()`.
//! - `committed_prefix_complete`: every committed Data batch below that
//!   independent window is visible.
//! - `nothing_visible_above_hw`: no visible offset is at or above the HWM.
//! - `no_visible_aborted`: no visible offset lies in an aborted range as
//!   Kafka's aborted-transaction index derives it from the Abort markers,
//!   independently of the abort filter in `visible()`.
//! - `hw_within_log` and `no_transition_violation`: the HWM stays within the
//!   log, the HWM and the LSO never regress, and every `End` Proceeds.
//!
//! Concurrent producers' batches interleave at the offset level. So a
//! committed txn can sit partly above the LSO, behind an older open txn, or
//! above the HWM. The guarantee is prefix-correctness, not whole-txn snapshot
//! atomicity.
//!
//! Scope: what is DRIVEN and what is MODELED.
//!
//! - DRIVEN (real code): the EndTxn decision cores `decide_phase1_transition`,
//!   `prepare_completion_identities_with_fresh` and
//!   `decide_end_txn_completion` on their Proceed path. `decision_model.rs`
//!   exercises the fencing and retry arms; a ghost flag fails
//!   `no_transition_violation` if they ever fire here. Also DRIVEN: the real
//!   `read_committed` clamp of `compute_visibility_window`
//!   (`effective_lso = lso.min(hw)`). That clamp bites non-trivially when an
//!   open txn's records sit above the HWM. The witness is `hwm_clamp_active`.
//! - MODELED (faithful abstraction, NOT driving real code): the LSO rule
//!   `lso()` and the abort filter in `visible()`. The LSO rule is Kafka's
//!   first-unstable-offset; `Log::lso()`'s incremental maintenance is stored
//!   state, not a pure fn. The filter hides a Data batch if and only if its
//!   txn aborted, which matches the consumer's range filtering only under the
//!   one-in-flight-txn-per-producer invariant this model enforces. The
//!   properties check both against oracles derived by producer and marker
//!   rather than by generation tag.
//! - NOT covered, and left to the per-slice log / txn-index / fetch models:
//!   the `Log::lso()` maintenance internals, `TxnIndex::aborted_in_range`
//!   overlap arithmetic, and the consumer `aborted_pids` state machine.
//!
//! See the design spec.

use krabka_log::{Offset, ProducerId};
use stateright::{Checker, Model, Property};

use super::{
    decision::{CompletionDecision, decide_end_txn_completion, decide_phase1_transition},
    state::{TxnEntry, TxnState},
    version::TxnVersion,
};
use crate::handlers::fetch::{FetchWatermarks, compute_visibility_window};

const TARGET_STATE_COUNT: usize = 20_000_000;

const MAX_UNIQUE_STATES: usize = 2_000_000;

const MAX_DEPTH: usize = 50;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_BASIC: usize = 1_228;

const PINNED_UNIQUE_STATES_WIDE: usize = 58_524;

const PID0: i64 = 1000;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Kind {
    Data,
    Commit,
    Abort,
}

/// One appended batch (offset = index in the log).
///
/// A transaction is a producer's run of `Data` batches in one `generation`. A
/// Commit or Abort marker ends the transaction.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Batch {
    producer: u8,
    generation: u8,
    kind: Kind,
}

/// Per-producer coordinator projection (drives the real `TxnEntry`, hashably).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Prod {
    state: i8, // TxnState::to_kafka_status()
    epoch: i16,
    generation: u8,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct EosState {
    log: Vec<Batch>,
    prod: Vec<Prod>, // index = producer
    /// High watermark = offsets replicated/durable so far (`hw <= log_end`).
    /// `Ack` advances it. An OPEN transaction's not-yet-replicated records can
    /// push the LSO ABOVE the HWM. The real `compute_visibility_window` clamp
    /// `effective_lso = lso.min(hw)` then bites and returns `hw`.
    hw: Offset,
    violations: Violations,
}

/// Ghost transition-violation flags. The transition that commits a violation
/// sets its flag, and `no_transition_violation` requires every flag to stay
/// `false`, so the checker reports the trace instead of panicking mid-search.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct Violations {
    /// The HWM moved backwards.
    hw_regressed: bool,
    /// The LSO moved backwards.
    lso_regressed: bool,
    /// An `End` did not Proceed. The no-window `End` path never has its epoch
    /// bumped underneath it, so only a changed decision core reaches this;
    /// `decision_model` exercises the fencing and retry arms.
    end_not_proceed: bool,
}

struct EosModel {
    producers: u8,
    max_gen: u8,
    max_data_per_txn: usize,
    max_log: usize,
}

// ----- model -----

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Act {
    Begin(u8),     // producer p: -> Ongoing (new generation)
    Append(u8),    // producer p: append a Data batch to its open txn
    End(u8, bool), // producer p: commit? -> drive decision cores + append marker
    Ack,           // a follower replicates one more offset: hw += 1
}

#[path = "eos_composition_model/helpers.rs"]
mod helpers;
use helpers::{
    aborted_by_markers, effective_lso, first_unstable_offset, lso, model_index, model_offset,
    rebuild, tstate, txn_outcome, visible,
};

#[path = "eos_composition_model/checker.rs"]
mod checker;

#[path = "eos_composition_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "eos_composition_model/tests.rs"]
mod tests;
