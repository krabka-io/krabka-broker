//! The checker bounds and the one entry point that the model test calls.
//!
//! The bounds sit next to the assertions that prove a run was exhaustive,
//! because a truncated search proves nothing and the two must move together.

use stateright::{Checker, Model};

use super::model::ClientServerFailoverModel;

const MAX_DEPTH: usize = 36;
const MAX_STATES: usize = 120_000;

pub fn run_model() {
    let checker = ClientServerFailoverModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "[client_server_failover] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(
        checker.max_depth() < MAX_DEPTH,
        "client_server_failover depth cap hit"
    );
    assert2::assert!(
        checker.state_count() < MAX_STATES,
        "client_server_failover truncated"
    );
    checker.assert_properties();
}
