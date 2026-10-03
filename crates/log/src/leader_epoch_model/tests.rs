use super::*;

#[test]
fn reconciliation_two_replicas() {
    run(
        ReconcileModel {
            replicas: 2,
            max_epoch: 5,
            max_log: 3,
            assign_on_election: true,
        },
        "reconciliation_two_replicas",
        PINNED_UNIQUE_STATES_TWO,
    )
    .assert_properties();
}

#[test]
fn reconciliation_three_replicas() {
    run(
        ReconcileModel {
            replicas: 3,
            max_epoch: 3,
            max_log: 2,
            assign_on_election: true,
        },
        "reconciliation_three_replicas",
        PINNED_UNIQUE_STATES_THREE,
    )
    .assert_properties();
}

/// Without Kafka's assign-at-election a new leader that has not written yet
/// cannot place a follower's newer epoch, and answers `OFFSET_OUT_OF_RANGE`.
/// The safety properties still hold: the lookup never licenses a wrong
/// truncation, it only stops answering.
#[test]
fn without_assign_on_election_the_leader_cannot_place_newer_epochs() {
    let checker = run(
        ReconcileModel {
            replicas: 2,
            max_epoch: 4,
            max_log: 3,
            assign_on_election: false,
        },
        "without_assign_on_election",
        PINNED_UNIQUE_STATES_NO_ASSIGN,
    );
    checker.assert_any_discovery("leader_places_every_follower_epoch");
    for property in [
        "reconciled_follower_is_leader_prefix",
        "no_agreed_record_truncated",
        "checkpoints_strictly_increasing",
    ] {
        checker.assert_no_discovery(property);
    }
}
