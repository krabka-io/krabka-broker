use super::*;

#[test]
fn reassign_basic() {
    // Leader not removed: catch-up then completion to the target replica set.
    run(
        ReassignModel::basic(),
        "reassign_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn reassign_leader_handoff() {
    // Leader in `removing`: catch-up, leader handoff, then completion.
    run(
        ReassignModel::leader_handoff(),
        "reassign_leader_handoff",
        PINNED_UNIQUE_STATES_LEADER_HANDOFF,
    );
}

#[test]
fn reassign_rf_decrease() {
    // Nothing added, one replica removed: complete only once every target
    // replica is in the ISR.
    run(
        ReassignModel::rf_decrease(),
        "reassign_rf_decrease",
        PINNED_UNIQUE_STATES_RF_DECREASE,
    );
}

#[test]
fn reassign_rf_decrease_leader_removed() {
    // Two removed, one added, leader removed: the handoff also waits for every
    // target replica.
    run(
        ReassignModel::rf_decrease_leader_removed(),
        "reassign_rf_decrease_leader_removed",
        PINNED_UNIQUE_STATES_RF_DECREASE_LEADER_REMOVED,
    );
}

#[test]
fn reassign_wide() {
    // 5 replicas, add 2 + remove 2, leader removed → handoff then completion.
    run(
        ReassignModel::wide(),
        "reassign_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}
