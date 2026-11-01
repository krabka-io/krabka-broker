//! The checker bounds and the entry points that the model tests call.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.
//!
//! # RED regression
//!
//! [`Orchestration::GateOnly`] is the leader before #1947, whose writer
//! appended every batch the dedup check passed. `no_duplicate_append` fails
//! there at depth 2: `ClientSend(AppendedUnacked)`,
//! `ClientRetry(AppendedUnacked)`. The leader appends the first send, whose
//! commit into the tracker is still owed. The client's request times out on
//! its side and it sends the batch again. The dedup check finds no tracked
//! entry, and the writer appends the batch a second time. This is Jepsen's
//! "Duplicate records" failure (krabka-io/krabka-broker#1932). The test pins
//! that counterexample, so a leader that loses the writer's lookup, or a model
//! that stops reaching the failure, fails it.

use {
    super::model::{ClientServerFailoverModel, Orchestration},
    crate::model_check::{KnownFailure, check_model, check_model_with_known_failures},
};

const MAX_DEPTH: usize = 48;
const MAX_STATES: usize = 4_000_000;

// The exact unique-state counts of the exhaustive single-threaded BFS over this
// model, one per orchestration. A changed count is a changed model, not a
// retuning knob: a dropped action, a `next_state` arm that starts returning
// `None`, or a field that the derived `Hash` stops reading.
const PINNED_UNIQUE_STATES: usize = 85_084;
const PINNED_UNIQUE_STATES_GATE_ONLY: usize = 187_373;

pub fn run_model() {
    check_model(
        ClientServerFailoverModel {
            orchestration: Orchestration::WriterLookup,
        },
        "client_server_failover",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES,
    );
}

pub fn run_gate_only() {
    check_model_with_known_failures(
        ClientServerFailoverModel {
            orchestration: Orchestration::GateOnly,
        },
        "client_server_failover_gate_only",
        (MAX_DEPTH, MAX_STATES, MAX_STATES),
        PINNED_UNIQUE_STATES_GATE_ONLY,
        &[KnownFailure {
            property: "no_duplicate_append",
            depth: 2,
        }],
    );
}
