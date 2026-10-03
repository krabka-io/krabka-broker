//! Exhaustive stateright enumeration of the fetch read-path visibility decision
//! (`super::compute_visibility_window`).
//!
//! The state is the advancing partition watermarks
//! `{log_start, hw, lso, deliverable, log_end}`, with the Kafka invariant
//! `0 <= log_start <= lso <= hw <= log_end` and KFC-1's
//! `log_start <= deliverable <= hw`. `Advance*` actions raise them
//! monotonically: appends raise LEO, ISR catch-up raises HW, txn commits raise
//! LSO, retention raises `log_start`, and a batch coming due raises the
//! delivery watermark. `Fetch` probes drive the real decision.
//!
//! For each `Fetch` the model asserts the clamp contract. A consumer fetch
//! never exposes an offset beyond the high-watermark, so there is no dirty
//! read, and it never exposes one at or beyond the delivery watermark, so it
//! never delivers a record before it is due. The broker clamps a
//! `read_committed` consumer at `lso.min(hw)`, and it serves a follower up to
//! the log-end whatever the delivery watermark says, because replication is not
//! gated. The model also asserts the single-source-of-truth response-field
//! contract, the de-dup'd hazard from `do_read`: every fetcher, follower or
//! consumer, is told the partition's high watermark and `lso.min(hw)`, as
//! Kafka's `Partition.readRecords` reports them, so a KIP-392 follower never
//! adopts an offset the leader has not committed. For each `Advance*` it
//! asserts KIP-227 monotonicity: the reported HW/LSO never regress as the log
//! progresses, and the delivery watermark does not enter those two formulas at
//! all. See the design spec
//! `docs/superpowers/specs/2026-06-14-krabka-fetch-hwm-visibility-model-design.md`.

use stateright::{Checker, Model, Property};

use super::{FetchWatermarks, Offset, compute_visibility_window};

const MAX_STATES: usize = 200_000;

const MAX_DEPTH: usize = 40;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_BASIC: usize = 182;

const PINNED_UNIQUE_STATES_WIDE: usize = 1_254;

struct VisModel {
    max_offset: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct VisState {
    log_start: i64,
    hw: i64,
    lso: i64,
    /// KFC-1's delivery watermark: the first offset that is not due yet.
    deliverable: i64,
    log_end: i64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum VisAction {
    AdvanceLogEnd,
    AdvanceHw,
    AdvanceLso,
    AdvanceLogStart,
    /// A scheduled batch reached its activation time.
    AdvanceDeliverable,
    /// `(is_follower, read_committed, fetch_offset)`. `read_committed` implies
    /// `!is_follower`.
    Fetch(bool, bool, i64),
}

#[path = "fetch_visibility_model/helpers.rs"]
mod helpers;
use helpers::{assert_fetch_contract, assert_monotonic};

#[path = "fetch_visibility_model/checker.rs"]
mod checker;

#[path = "fetch_visibility_model/checks.rs"]
mod checks;
use checks::run;

#[cfg(test)]
#[path = "fetch_visibility_model/tests.rs"]
mod tests;
