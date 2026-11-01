//! The checker bounds and the one entry point that the model test calls.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.
//!
//! # Without the writer's lookup
//!
//! [`WriterLookup::Off`] restates the broker before #1947, whose writer
//! appended every batch the dedup check let through. `no_duplicate_append`
//! then fails at depth 2: `ClientSend(AppendedUnacked)`,
//! `ClientRetry(AppendedUnacked)`. The leader appends the first send, whose
//! commit into the tracker has not run yet. The client's request times out on
//! its side and it sends the batch again. The leader's dedup check finds no
//! tracked entry and appends the batch a second time. This is Jepsen's
//! "Duplicate records" failure (krabka-io/krabka-broker#1932), the same defect
//! `producer_state::inflight_model` reproduces with two producers. It stays
//! pinned as a [`KnownFailure`] so the model keeps reaching it.

use {
    super::{model::ClientServerFailoverModel, produce::WriterLookup},
    crate::model_check::{KnownFailure, check_model_with_known_failures},
};

const MAX_DEPTH: usize = 48;
const MAX_STATES: usize = 4_000_000;

// The exact unique-state count of each exhaustive single-threaded BFS over
// this model. A changed count is a changed model, not a retuning knob: a
// dropped action, a `next_state` arm that starts returning `None`, or a field
// that the derived `Hash` stops reading.
const PINNED_UNIQUE_STATES: usize = 66_587;
const PINNED_UNIQUE_STATES_WITHOUT_LOOKUP: usize = 187_373;

pub fn run_model() {
    check_model_with_known_failures(
        ClientServerFailoverModel {
            lookup: WriterLookup::UnderLock,
        },
        "client_server_failover",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES,
        &[],
    );
}

pub fn run_model_without_lookup() {
    check_model_with_known_failures(
        ClientServerFailoverModel {
            lookup: WriterLookup::Off,
        },
        "client_server_failover_without_lookup",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES_WITHOUT_LOOKUP,
        &[KnownFailure {
            property: "no_duplicate_append",
            depth: 2,
        }],
    );
}
