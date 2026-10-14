// docs:begin quorum_state
use {
    assert2::assert,
    krabka_metadata::VoterSet,
    krabka_raft::kraft::{NodeId, QuorumState, QuorumStateMachine},
    krabka_units::prelude::millis,
    uuid::Uuid,
};

#[path = "../tests/support/voter.rs"]
mod voter_fixture;

fn main() {
    let node = NodeId(1);
    let voters = VoterSet::from_voters([voter_fixture::voter(1, Uuid::new_v4())]);
    let state = QuorumState::bootstrap(Uuid::new_v4(), voters);
    let machine = QuorumStateMachine::new(node, state, millis(1_000));
    assert!(machine.is_voter());
}
// docs:end quorum_state
