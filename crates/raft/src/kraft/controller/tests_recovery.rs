//! Tests for restart recovery: replay of the committed metadata and control
//! prefixes, recovery of an image from a checkpoint plus log, and the
//! round-trip of the durable quorum-state file at both schema versions.

use std::time::Duration as StdDuration;

use assert2::{assert, check};

use super::*;
use crate::kraft::{
    controller::{
        control_state::voter_set_to_wire,
        quorum_state_file::{load_quorum_state, save_quorum_state},
        records::typed_control_batch,
        recovery::{replay_committed, replay_control_records},
        test_support::{
            TEST_ELECTION_TIMEOUT, await_leader, build_engine_only, elect_single_voter_engine,
            submit_change_with_timeout, topic_record, voter_set,
        },
    },
    transport::NullPeerSender,
};

#[test]
fn restart_replays_control_records_only_through_persisted_high_watermark() {
    let dir = tempfile::tempdir().expect("tempdir");
    let initial = voter_set(&[NodeId(1)]);
    let committed = voter_set(&[NodeId(1), NodeId(2)]);
    let uncommitted = voter_set(&[NodeId(1), NodeId(3)]);
    {
        let mut log = KraftLog::open(dir.path()).expect("open log");
        let mut batch =
            typed_control_batch(1, &[ControlRecord::Voters(voter_set_to_wire(&committed))])
                .expect("committed voter batch");
        log.append(&mut batch)
            .expect("append committed voter batch");
        log.advance_hwm(log.log_end_offset());
        let mut batch =
            typed_control_batch(1, &[ControlRecord::Voters(voter_set_to_wire(&uncommitted))])
                .expect("uncommitted voter batch");
        log.append(&mut batch)
            .expect("append uncommitted voter batch");
    }

    let log = KraftLog::open(dir.path()).expect("reopen log");
    assert2::assert!(log.hwm() < log.log_end_offset());
    let mut state = QuorumState::bootstrap(uuid::Uuid::nil(), initial);
    replay_control_records(&log, &mut state, MetadataRaftFetchMax::default());
    assert2::assert!(state.voters == committed);
}

#[test]
fn control_replay_stops_inside_a_partially_committed_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let initial = voter_set(&[NodeId(1)]);
    let committed = voter_set(&[NodeId(1), NodeId(2)]);
    let uncommitted = voter_set(&[NodeId(1), NodeId(3)]);
    {
        let mut log = KraftLog::open(dir.path()).expect("open log");
        let mut batch = typed_control_batch(
            1,
            &[
                ControlRecord::Voters(voter_set_to_wire(&committed)),
                ControlRecord::Voters(voter_set_to_wire(&uncommitted)),
            ],
        )
        .expect("mixed-commit voter batch");
        log.append(&mut batch).expect("append voter batch");
        log.advance_hwm(Offset(1));
    }

    let log = KraftLog::open(dir.path()).expect("reopen log");
    let mut state = QuorumState::bootstrap(uuid::Uuid::nil(), initial);
    replay_control_records(&log, &mut state, MetadataRaftFetchMax::default());

    assert2::assert!(state.voters == committed);
}

#[test]
fn replay_committed_rebuilds_image_from_log_records() {
    let (mut engine, _dir) = build_engine_only(NodeId(1), &[NodeId(1)]);
    elect_single_voter_engine(&mut engine);
    let (reply, mut rx) = oneshot::channel();
    engine.on_submit_change(&topic_record("replayed"), reply);
    assert2::assert!(matches!(rx.try_recv(), Ok(Ok(_))));

    let mut recovered = MetadataImage::new(uuid::Uuid::nil());
    replay_committed(
        &engine.log,
        &mut recovered,
        Offset(0),
        MetadataRaftFetchMax::default(),
    )
    .expect("replay");

    assert2::assert!(recovered.topic("replayed").is_some());
}

#[tokio::test]
async fn snapshot_then_restart_recovers_image() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_dir = dir.path().to_path_buf();
    let cluster_id = uuid::Uuid::from_u128(7);
    let voters = voter_set(&[NodeId(1)]);

    {
        let log = KraftLog::open(&data_dir).expect("open log");
        let ctrl = KraftController::spawn(
            KraftConfig {
                me: NodeId(1),
                cluster_id,
                initial_state: QuorumState::bootstrap(cluster_id, voters.clone()),
                election_timeout: TEST_ELECTION_TIMEOUT,
                heartbeat_interval: None,
                controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
                metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::default(),
                metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
                peers: Arc::new(NullPeerSender),
                snapshot_interval_records: 0,
                metadata_snapshot_fetch_max: MetadataSnapshotFetchMax::default(),
            },
            log,
            data_dir.clone(),
        );
        ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
        await_leader(&ctrl, Some(NodeId(1))).await;
        submit_change_with_timeout(&ctrl, topic_record("recovered"), "recovery seed")
            .await
            .unwrap();
        assert2::assert!(ctrl.current_image().topic("recovered").is_some());
        ctrl.trigger_snapshot().await.unwrap();
        ctrl.shutdown().await;
        // Give the loop a moment to fully drain.
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }

    // Reopen over the same dir: the image is rebuilt from checkpoint+log.
    let ctrl2 = KraftController::open(
        data_dir.clone(),
        NodeId(1),
        cluster_id,
        voters,
        TEST_ELECTION_TIMEOUT,
        None,
        ControllerFetchMissLimit::default(),
        MetadataRaftCommandQueueCapacity::default(),
        MetadataRaftFetchMax::default(),
        Arc::new(NullPeerSender),
        0,
        MetadataSnapshotFetchMax::default(),
    )
    .expect("reopen");
    assert2::assert!(ctrl2.current_image().topic("recovered").is_some());
    ctrl2.shutdown().await;
}

#[tokio::test]
async fn quorum_state_file_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cid = uuid::Uuid::from_u128(9);
    let mut state = QuorumState::bootstrap(cid, voter_set(&[NodeId(1), NodeId(2), NodeId(3)]));
    state.leader_epoch = 5;
    state.leader_id = Some(NodeId(2));
    state.voted_key = Some(ReplicaKey {
        id: NodeId(3),
        directory_id: uuid::Uuid::from_u128(3),
    });
    save_quorum_state(dir.path(), &state).unwrap();

    // The JVM tools read this file, so the schema-v0 field set and its
    // Kafka field order are part of the contract, not an implementation
    // detail of whatever serializer writes it.
    let json = std::fs::read_to_string(dir.path().join(QUORUM_STATE_FILE)).unwrap();
    check!(
        json == format!(
            "{{\"clusterId\":\"{cid}\",\"leaderId\":2,\"leaderEpoch\":5,\"votedId\":3,\
             \"appliedOffset\":0,\"currentVoters\":[{{\"voterId\":1}},{{\"voterId\":2}},\
             {{\"voterId\":3}}],\"data_version\":0}}"
        )
    );

    let loaded = load_quorum_state(
        dir.path(),
        cid,
        &voter_set(&[NodeId(1), NodeId(2), NodeId(3)]),
    )
    .unwrap()
    .expect("present");
    // Leadership is volatile (Raft persists only currentTerm + votedFor):
    // `leader_id` is deliberately cleared on load so a restarted ex-leader
    // re-discovers the current leader instead of trusting stale state.
    check!(
        (
            loaded.leader_epoch,
            loaded.leader_id,
            loaded.voted_key.map(|k| k.id),
            loaded.cluster_id,
        ) == (5, None, Some(NodeId(3)), cid)
    );
    let json = std::fs::read_to_string(dir.path().join(QUORUM_STATE_FILE)).unwrap();
    assert2::assert!(json.contains("\"currentVoters\""));
    assert2::assert!(json.contains("\"data_version\":0"));
    assert2::assert!(!json.contains("votedDirectoryId"));
}

#[test]
fn quorum_state_level_one_uses_voted_directory_and_omits_static_voters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let voters = voter_set(&[NodeId(1), NodeId(2)]);
    let mut state = QuorumState::bootstrap(uuid::Uuid::from_u128(9), voters.clone());
    state.kraft_version = 1;
    state.voted_key = Some(ReplicaKey {
        id: NodeId(2),
        directory_id: uuid::Uuid::from_u128(0x0102_0304),
    });
    save_quorum_state(dir.path(), &state).unwrap();

    let json = std::fs::read_to_string(dir.path().join(QUORUM_STATE_FILE)).unwrap();
    assert2::assert!(json.contains("\"data_version\":1"));
    assert2::assert!(json.contains("\"votedDirectoryId\""));
    assert2::assert!(!json.contains("currentVoters"));
    let loaded = load_quorum_state(dir.path(), state.cluster_id, &voters)
        .unwrap()
        .unwrap();
    assert2::assert!(loaded.kraft_version == 1);
    assert2::assert!(loaded.voted_key == state.voted_key);
}

#[test]
fn load_quorum_state_reports_unreadable_non_missing_file_errors() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir(dir.path().join(QUORUM_STATE_FILE)).expect("mkdir quorum-state path");

    let loaded = load_quorum_state(dir.path(), uuid::Uuid::nil(), &voter_set(&[NodeId(1)]));

    assert2::assert!(matches!(loaded, Err(RaftError::Storage(_))));
}

#[test]
fn load_quorum_state_ignores_truncated_file_without_panicking() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join(QUORUM_STATE_FILE), [0u8; 53]).expect("write short state");

    let loaded = load_quorum_state(dir.path(), uuid::Uuid::nil(), &voter_set(&[NodeId(1)]))
        .expect("short file is ignored");

    assert2::assert!(loaded.is_none());
}
