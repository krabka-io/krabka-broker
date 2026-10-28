#![cfg(test)]

use {
    assert2::assert,
    krabka_metadata::Voter,
    krabka_raft::{
        NodeId,
        reconfig::{AddVoter, RemoveVoter, UpdateVoter, VoterChange},
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
};

#[path = "support/voter.rs"]
mod voter_fixture;

struct VoterIdentity {
    voter: Option<Voter>,
    change: Option<VoterChange>,
}

#[fixture]
fn identity() -> VoterIdentity {
    // Start this scenario with fresh inputs and observations.

    VoterIdentity {
        voter: None,
        change: None,
    }
}

#[given("quorum voter {id} with its directory identity derived from the same integer")]
fn quorum_voter(identity: &mut VoterIdentity, id: u64) {
    identity.voter = Some(voter_fixture::voter(
        id,
        uuid::Uuid::from_u128(u128::from(id)),
    ));
}

#[when("a {operation} voter change is constructed")]
fn construct_voter_change(identity: &mut VoterIdentity, operation: String) {
    let voter = identity.voter.take().expect("a quorum voter was given");
    identity.change = Some(match operation.as_str() {
        "add" => VoterChange::Add(AddVoter {
            voter,
            ack_when_committed: false,
        }),
        "remove" => VoterChange::Remove(RemoveVoter {
            id: voter.id,
            directory_id: voter.directory_id,
        }),
        "update" => VoterChange::Update(UpdateVoter { voter }),
        _ => panic!("unknown voter operation: {operation}"),
    });
}

#[then(
    "the {operation} change preserves voter ID {id}, directory integer {directory}, and local-append acknowledgement for adds"
)]
fn preserved_voter_identity(identity: &VoterIdentity, operation: String, id: u64, directory: u128) {
    let actual = match (
        operation.as_str(),
        identity.change.as_ref().expect("a change was constructed"),
    ) {
        ("add", VoterChange::Add(request)) => (
            request.voter.id,
            request.voter.directory_id,
            Some(request.ack_when_committed),
        ),
        ("remove", VoterChange::Remove(request)) => (request.id, request.directory_id, None),
        ("update", VoterChange::Update(request)) => {
            (request.voter.id, request.voter.directory_id, None)
        }
        _ => panic!("voter change variant did not match its requested operation"),
    };
    let ack_when_committed = (operation == "add").then_some(false);
    assert!(
        actual
            == (
                NodeId(id),
                uuid::Uuid::from_u128(directory),
                ack_when_committed
            )
    );
}

scenarios!("tests/features/migrated/tests/reconfig.feature", fixtures = [identity: VoterIdentity]);
