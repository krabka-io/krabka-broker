use {
    krabka_metadata::Voter,
    krabka_raft::{
        NodeId,
        reconfig::{AddVoter, RemoveVoter, UpdateVoter, VoterChange},
    },
};

#[path = "support/voter.rs"]
mod voter_fixture;

fn voter(id: u64) -> Voter { voter_fixture::voter(id, uuid::Uuid::from_u128(id.into())) }

#[test]
fn voter_changes_preserve_exact_request_identity() {
    let add = VoterChange::Add(AddVoter {
        voter: voter(2),
        ack_when_committed: false,
    });
    let remove = VoterChange::Remove(RemoveVoter {
        id: NodeId(2),
        directory_id: uuid::Uuid::from_u128(2),
    });
    let update = VoterChange::Update(UpdateVoter { voter: voter(2) });

    assert2::assert!(let VoterChange::Add(add_request) = add);
    assert2::assert!(!add_request.ack_when_committed);
    assert2::assert!(let VoterChange::Remove(remove_request) = remove);
    assert2::assert!(remove_request.id == NodeId(2));
    assert2::assert!(let VoterChange::Update(update_request) = update);
    assert2::assert!(update_request.voter.directory_id == uuid::Uuid::from_u128(2));
}
