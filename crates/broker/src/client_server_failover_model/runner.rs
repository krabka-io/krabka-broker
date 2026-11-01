//! The checker bounds and the one entry point that the model test calls.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.
//!
//! # Known failure
//!
//! `no_duplicate_append` fails on the current code, at depth 2:
//! `ClientSend(AppendedUnacked)`, `ClientRetry(AppendedUnacked)`. The leader
//! appends the first send, whose commit into the tracker waits for the
//! `acks=all` gate. The client's request times out on its side and it sends
//! the batch again. The leader's dedup check finds no tracked entry and
//! appends the batch a second time. This is Jepsen's
//! "Duplicate records" known failure (krabka-io/krabka-broker#1932), the same
//! defect `producer_state::inflight_model` reproduces with two producers. The
//! fix is being made separately; when it merges, it deletes the
//! [`KnownFailure`] entry and re-pins the count.

use {
    super::model::ClientServerFailoverModel,
    crate::model_check::{KnownFailure, check_model_with_known_failures},
};

const MAX_DEPTH: usize = 48;
const MAX_STATES: usize = 4_000_000;

// The exact unique-state count of the exhaustive single-threaded BFS over this
// model. A changed count is a changed model, not a retuning knob: a dropped
// action, a `next_state` arm that starts returning `None`, or a field that the
// derived `Hash` stops reading. The count grew from 38,131 when the dedup check
// started to read a tracker that commits apart from the append, and the log
// grew a second slot that a second copy of the batch can take.
const PINNED_UNIQUE_STATES: usize = 187_373;

pub fn run_model() {
    check_model_with_known_failures(
        ClientServerFailoverModel,
        "client_server_failover",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES,
        &[KnownFailure {
            property: "no_duplicate_append",
            depth: 2,
        }],
    );
}
