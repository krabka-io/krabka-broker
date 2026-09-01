//! Tests for the KIP-853 control records: the control state's
//! apply/commit/truncate behaviour, the `LeaderChange` batch the leader
//! appends, and the reasons a reconfiguration is refused before it is proposed.

use assert2::{assert, check};

use super::*;
use crate::kraft::controller::{
    control_state::voter_set_to_wire,
    records::leader_change_batch,
    test_support::{build_engine_only, elect_single_voter_engine, voter_set},
};

#[test]
fn control_state_applies_before_commit_and_restores_on_truncation() {
    let initial = voter_set(&[NodeId(1)]);
    let two_voters = voter_set(&[NodeId(1), NodeId(2)]);
    let replacement = voter_set(&[NodeId(1), NodeId(3)]);
    let mut controls = KraftControlState::new(initial.clone(), 0);

    controls
        .apply(5, &ControlRecord::Voters(voter_set_to_wire(&two_voters)))
        .unwrap();
    assert!(controls.latest_voters() == &two_voters);
    assert!(controls.committed_voters == initial);
    assert!(controls.commit_to(6));
    assert!(controls.committed_voters == two_voters);

    controls
        .apply(7, &ControlRecord::Voters(voter_set_to_wire(&replacement)))
        .unwrap();
    assert!(controls.latest_voters() == &replacement);
    controls.truncate_to(7);
    assert!(controls.latest_voters() == &two_voters);
    assert!(!controls.commit_to(8));
    assert!(controls.committed_voters == two_voters);
}

#[test]
fn control_history_frontiers_handle_empty_exact_repeated_and_moving_states() {
    let initial = voter_set(&[NodeId(1)]);
    let mut controls = KraftControlState::new(initial.clone(), 0);
    controls.voter_history.clear();
    controls.version_history.clear();

    check!(controls.voters_at(Offset(10)) == initial);
    check!(controls.version_at(Offset(10)) == 0);

    controls
        .version_history
        .extend([(2, 0), (4, 1), (6, 1), (8, 0)]);
    check!(controls.version_at(Offset(2)) == 0);
    check!(controls.version_at(Offset(4)) == 0);
    check!(controls.version_at(Offset(5)) == 1);
    check!(controls.version_at(Offset(8)) == 1);
    check!(controls.version_at(Offset(9)) == 0);

    check!(!controls.commit_to(4));
    check!(controls.commit_to(5));
    check!(!controls.commit_to(7));
    controls.truncate_to(6);
    check!(controls.version_history.keys().copied().collect::<Vec<_>>() == vec![2, 4]);
    check!(controls.version_at(Offset(i64::MAX)) == 1);
    check!(!controls.commit_to(i64::MAX));
}

#[test]
fn execute_local_only_appends_leader_change_batch_to_log() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let start = engine.log.log_end_offset();

    engine.execute_local_only(vec![Action::AppendLeaderChange { epoch: 4 }]);

    assert2::assert!(engine.log.log_end_offset() == start + 1);
    let batches = engine
        .log
        .read_decoded(start, DEFAULT_METADATA_RAFT_FETCH_MAX)
        .expect("read appended leader-change");
    assert2::assert!(batches.len() == 1);
    let batch = &batches[0];
    check!(
        (
            batch.base_offset,
            batch.partition_leader_epoch,
            batch.attributes.is_control_batch(),
            batch.records.len(),
        ) == (start.0, 4, true, 1)
    );
}

#[test]
fn leader_change_batch_encodes_control_record_payload() {
    use krabka_protocol::{
        Decode,
        owned::leader_change_message::LeaderChangeMessage,
        records::metadata::control::{ControlRecordType, control_record_key},
    };

    let voters = voter_set(&[NodeId(1), NodeId(2), NodeId(3)]);
    let batch = leader_change_batch(7, NodeId(2), &voters, 0);

    check!(
        (
            batch.partition_leader_epoch,
            batch.attributes.is_control_batch(),
            batch.last_offset_delta,
            batch.records.len(),
        ) == (7, true, 0, 1)
    );
    let record = &batch.records[0];
    check!(record.offset_delta == 0);
    check!(record.key.as_ref() == Some(&control_record_key(ControlRecordType::LeaderChange)));
    let value = record.value.as_ref().expect("leader change value");
    let mut cur: &[u8] = value;
    let decoded = LeaderChangeMessage::decode(&mut cur, 0).expect("decode leader change");
    check!(cur.is_empty());
    check!((decoded.version, decoded.leader_id) == (0, 2));
    let voters: Vec<i32> = decoded.voters.iter().map(|v| v.voter_id).collect();
    let granting_voters: Vec<i32> = decoded.granting_voters.iter().map(|v| v.voter_id).collect();
    assert2::assert!(voters == vec![1, 2, 3]);
    assert2::assert!(granting_voters == vec![1, 2, 3]);
}

/// A reconfiguration is refused before it is proposed when this node is
/// not the leader, or when the quorum cannot support the change.
///
/// Each refusal names a different cause, and the caller acts on which one:
/// `NotLeader` says where to go instead, while the rest say the request
/// itself will not do. Collapsing them loses the redirect.
#[test]
fn a_reconfiguration_is_refused_with_the_reason_it_was_refused_for() {
    use crate::reconfig::{AddVoter, ReconfigOutcome, VoterChange};

    fn add_of(id: u64) -> VoterChange {
        VoterChange::Add(AddVoter {
            voter: krabka_metadata::Voter {
                id: NodeId(id),
                directory_id: uuid::Uuid::nil(),
                endpoints: vec![],
                kraft_version: krabka_metadata::KRaftVersionRange::default(),
            },
            ack_when_committed: true,
        })
    }

    // A follower redirects rather than refusing outright: it knows the
    // request is legitimate, just not addressed to it.
    let (mut follower, _dir) = build_engine_only(NodeId(1), &[NodeId(1), NodeId(2)]);
    let (reply, mut rx) = oneshot::channel();
    follower.on_reconfigure(add_of(3), reply);
    check!(
        matches!(rx.try_recv(), Ok(Ok(ReconfigOutcome::NotLeader { .. }))),
        "a non-leader redirects"
    );

    // A leader whose quorum is still at kraft.version 0 has no mechanism to
    // add a voter with: dynamic membership is what version 1 introduces.
    let (mut leader, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut leader);
    check!(
        leader.controls.committed_version == 0,
        "a fresh quorum starts at version 0"
    );
    let (reply, mut rx) = oneshot::channel();
    leader.on_reconfigure(add_of(2), reply);
    check!(
        matches!(
            rx.try_recv(),
            Ok(Err(RaftError::UnsupportedKraftVersion(0)))
        ),
        "adding a voter at version 0 is refused as unsupported"
    );
}
