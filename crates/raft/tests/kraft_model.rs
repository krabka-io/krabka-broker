//! Exhaustive stateright checks of the `KRaft` consensus core. See
//! `model/mod.rs`.
//!
//! Memory safety: the stateright BFS keeps every visited unique state resident.
//! Each checker run is therefore fenced with a `target_state_count` backstop,
//! on top of the model's tight `within_boundary`, so a runaway space cannot
//! exhaust the host RAM.
//!
//! The configs differ in their bounds because the linearizability tester keeps
//! its history in the fingerprinted state and so blows the space up by about
//! 30x wherever client appends are enabled:
//! - `three_voters_election_safety`: 3 voters and NO client appends. This covers
//!   election and log-matching safety over the small, fast space.
//! - `two_voters_linearizable`: 2 voters with client appends. This covers
//!   committed-log linearizability over a tightly-bounded space.
//! - `three_voters_faults`: 3 voters, no appends, message loss and duplication,
//!   and one crash at a time.
//! - `three_voters_append`: 3 voters WITH client appends and one crash at a
//!   time, which is the only config in which a committed entry can rest on a
//!   bare majority while the third voter falls behind. That is what makes the
//!   KIP-595 log-recency test in `handle_vote_request` observable: without it a
//!   stale voter wins an election and `leader_completeness` fails.
//! - `two_voters_append_via_linearizable`: 2 voters and stateless appenders.
mod model;

use krabka_ids::NodeId;
use model::ConsensusModel;
use stateright::{Checker, Model};

/// Hard backstop on the explored, that is generated, states. It bounds memory
/// even if `within_boundary` is looser than intended. It is set well above the
/// true bounded count of each config, so it never truncates a real check. Such
/// a truncation would spuriously fail a `sometimes` witness, or leave an
/// `always` only partially verified.
const MAX_STATES: usize = 6_000_000;
/// Depth backstop. It must exceed the reachable-graph diameter of each config,
/// or the search is depth-truncated and therefore incomplete. The configs below
/// are bounded, so their diameter sits well under this value.
const MAX_DEPTH: usize = 60;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
const PINNED_UNIQUE_STATES_THREE_VOTERS_ELECTION_SAFETY: usize = 10_834;
const PINNED_UNIQUE_STATES_TWO_VOTERS_LINEARIZABLE: usize = 43_811;
const PINNED_UNIQUE_STATES_THREE_VOTERS_FAULTS: usize = 779_078;
const PINNED_UNIQUE_STATES_THREE_VOTERS_APPEND: usize = 445_169;
const PINNED_UNIQUE_STATES_TWO_VOTERS_APPEND_VIA: usize = 230_591;

fn run(model: ConsensusModel, label: &str, pinned_unique_states: usize) {
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
    // Guard against silent incompleteness: if we hit the depth or state cap, the
    // `always` properties were only partially verified — fail loudly so the
    // bounds get retuned rather than passing a non-exhaustive check.
    assert2::assert!(checker.max_depth() < MAX_DEPTH);
    assert2::assert!(checker.state_count() < MAX_STATES);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

#[test]
fn three_voters_election_safety() {
    run(
        ConsensusModel::elections(&[NodeId(1), NodeId(2), NodeId(3)]),
        "three_voters_election_safety",
        PINNED_UNIQUE_STATES_THREE_VOTERS_ELECTION_SAFETY,
    );
}

#[test]
fn two_voters_linearizable() {
    run(
        ConsensusModel::linearizable(&[NodeId(1), NodeId(2)], 2),
        "two_voters_linearizable",
        PINNED_UNIQUE_STATES_TWO_VOTERS_LINEARIZABLE,
    );
}

#[test]
fn three_voters_faults() {
    // Election + log-matching safety under an adversarial network: message
    // loss, duplication, and a single crash/recover. 3 voters so a crash leaves
    // a majority that can still make progress.
    run(
        ConsensusModel::faults(&[NodeId(1), NodeId(2), NodeId(3)]),
        "three_voters_faults",
        PINNED_UNIQUE_STATES_THREE_VOTERS_FAULTS,
    );
}

#[test]
fn three_voters_append() {
    // Leader completeness under a stale majority: three voters, client appends,
    // and one crash at a time, so a committed prefix can live on a bare
    // majority while the crashed voter misses it. The `leader_completeness`
    // property then holds every elected leader to that prefix, and the
    // `stale_candidate_refused` witness proves the refusal is actually reached.
    run(
        ConsensusModel::three_voters_append(&[NodeId(1), NodeId(2), NodeId(3)], 1),
        "three_voters_append",
        PINNED_UNIQUE_STATES_THREE_VOTERS_APPEND,
    );
}

#[test]
fn two_voters_append_via_linearizable() {
    run(
        // The diskless linearizability leg deliberately uses the design's
        // exhaustive tiny bound: two voters, two stateless appenders, and two
        // appends. The separate crash model and 3-broker black-box gate cover
        // minority WAL-node loss.
        ConsensusModel::append_via(&[NodeId(1), NodeId(2)], 2),
        "two_voters_append_via_linearizable",
        PINNED_UNIQUE_STATES_TWO_VOTERS_APPEND_VIA,
    );
}
