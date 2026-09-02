//! Fixtures shared by the controller's unit tests: engine and controller
//! builders over a temporary data directory, a recording [`PeerSender`], and
//! the election and submission helpers the behaviour tests drive them with.

use std::time::Duration as StdDuration;

use krabka_units::prelude::{TimeExt as _, secs};

use super::*;
use crate::kraft::transport::NullPeerSender;

/// Deadline every test-side channel receive is bounded by.
pub const TEST_RECV_TIMEOUT: Time = secs(1);

/// Default election timeout for engines built by [`build`].
pub const TEST_ELECTION_TIMEOUT: Time = secs(1);

pub fn voter_set(ids: &[NodeId]) -> krabka_metadata::voters::VoterSet {
    krabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id| {
        krabka_metadata::voters::Voter {
            id,
            directory_id: uuid::Uuid::nil(),
            endpoints: vec![krabka_metadata::voters::VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "127.0.0.1".into(),
                port: 9_093,
            }],
            kraft_version: krabka_metadata::voters::KRaftVersionRange::default(),
        }
    }))
}

pub fn build(me: NodeId, ids: &[NodeId]) -> (KraftController, tempfile::TempDir) {
    build_with_timeout(me, ids, TEST_ELECTION_TIMEOUT)
}

pub fn build_with_timeout(
    me: NodeId,
    ids: &[NodeId],
    election_timeout: Time,
) -> (KraftController, tempfile::TempDir) {
    build_full(me, ids, election_timeout, 0)
}

pub fn build_with_snapshot_interval(
    me: NodeId,
    ids: &[NodeId],
    snapshot_interval_records: u64,
) -> (KraftController, tempfile::TempDir) {
    build_full(me, ids, TEST_ELECTION_TIMEOUT, snapshot_interval_records)
}

pub fn build_full(
    me: NodeId,
    ids: &[NodeId],
    election_timeout: Time,
    snapshot_interval_records: u64,
) -> (KraftController, tempfile::TempDir) {
    build_full_with_policy(
        me,
        ids,
        election_timeout,
        snapshot_interval_records,
        None,
        ControllerFetchMissLimit::default(),
        MetadataRaftCommandQueueCapacity::default(),
        MetadataRaftFetchMax::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_full_with_policy(
    me: NodeId,
    ids: &[NodeId],
    election_timeout: Time,
    snapshot_interval_records: u64,
    heartbeat_interval: Option<Time>,
    controller_fetch_miss_limit: ControllerFetchMissLimit,
    metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity,
    metadata_raft_fetch_max: MetadataRaftFetchMax,
) -> (KraftController, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = KraftLog::open(dir.path()).expect("open log");
    let state = QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(ids));
    let ctrl = KraftController::spawn(
        KraftConfig {
            me,
            cluster_id: uuid::Uuid::nil(),
            initial_state: state,
            election_timeout,
            heartbeat_interval,
            controller_fetch_miss_limit,
            metadata_raft_command_queue_capacity,
            metadata_raft_fetch_max,
            peers: Arc::new(NullPeerSender),
            snapshot_interval_records,
            metadata_snapshot_fetch_max: MetadataSnapshotFetchMax::default(),
        },
        log,
        dir.path().to_path_buf(),
    );
    (ctrl, dir)
}

pub fn build_engine_only(me: NodeId, ids: &[NodeId]) -> (Engine, tempfile::TempDir) {
    build_engine_only_with_policy(
        me,
        ids,
        ControllerFetchMissLimit::default(),
        MetadataRaftFetchMax::default(),
    )
}

pub fn build_engine_only_with_policy(
    me: NodeId,
    ids: &[NodeId],
    controller_fetch_miss_limit: ControllerFetchMissLimit,
    metadata_raft_fetch_max: MetadataRaftFetchMax,
) -> (Engine, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = KraftLog::open(dir.path()).expect("open log");
    let core = QuorumStateMachine::new(
        me,
        QuorumState::bootstrap(uuid::Uuid::nil(), voter_set(ids)),
        TEST_ELECTION_TIMEOUT,
    );
    let image = MetadataImage::new(uuid::Uuid::nil());
    let (image_tx, _image_rx) = watch::channel(Arc::new(image.clone()));
    let (leader_tx, _leader_rx) = watch::channel(core.quorum_state().leader_id);
    let log_hwm_at_open = log.hwm().0;
    let initial_snapshot = QuorumStateSnapshot {
        leader_id: core.quorum_state().leader_id,
        leader_epoch: core.quorum_state().leader_epoch,
        high_watermark: log_hwm_at_open,
        quorum_high_watermark: log_hwm_at_open,
        log_end_offset: log.log_end_offset().0,
        log_start_offset: log.log_start_offset().0,
        voters: core.quorum_state().voters.clone(),
        voted_directory_id: core
            .quorum_state()
            .voted_key
            .as_ref()
            .map(|key| key.directory_id),
        observers: Vec::new(),
        per_replica_fetch_offset: std::collections::BTreeMap::new(),
    };
    let (quorum_tx, _quorum_rx) = watch::channel(initial_snapshot);
    let (cmd_tx, _cmd_rx) = mpsc::channel(1);
    let held_epoch = core.quorum_state().leader_epoch;
    let was_leader = core.role().is_leader();
    let controls = KraftControlState::new(core.quorum_state().voters.clone(), 0);
    let clock_base = Instant::now();
    (
        Engine {
            me,
            core,
            log,
            image,
            peers: Arc::new(NullPeerSender),
            image_tx,
            leader_tx,
            quorum_tx,
            cmd_tx,
            data_dir: dir.path().to_path_buf(),
            clock_base,
            election_timeout: TEST_ELECTION_TIMEOUT,
            heartbeat_interval: None,
            controller_fetch_miss_limit,
            metadata_raft_fetch_max,
            election_at: None,
            fetch_at: None,
            fetch_misses: 0,
            commit_waiters: Vec::new(),
            was_leader,
            held_epoch,
            snapshot_interval_records: 0,
            metadata_snapshot_fetch_max: MetadataSnapshotFetchMax::default(),
            last_snapshot_end_offset: Offset(0),
            downgrade_snapshot_pending: None,
            downgrade_snapshot_failures_remaining: 0,
            snapshot_fetch: None,
            installed_snapshot_epoch: None,
            controls,
            replica_fetch_offsets: BTreeMap::new(),
            leader_reported_hwm: log_hwm_at_open,
            pending_reconfig: None,
        },
        dir,
    )
}

#[derive(Debug)]
pub struct CapturedPeerSend {
    pub peer: NodeId,
    pub api_key: i16,
    pub body: bytes::Bytes,
}

struct RecordingPeerSender {
    sends: mpsc::UnboundedSender<CapturedPeerSend>,
    response: bytes::Bytes,
}

#[async_trait::async_trait]
impl PeerSender for RecordingPeerSender {
    async fn send(
        &self,
        peer: NodeId,
        api_key: i16,
        body: bytes::Bytes,
    ) -> Result<bytes::Bytes, RaftError> {
        self.sends
            .send(CapturedPeerSend {
                peer,
                api_key,
                body,
            })
            .expect("record peer send");
        Ok(self.response.clone())
    }
}

pub fn record_peer_sends(
    engine: &mut Engine,
    response: bytes::Bytes,
) -> mpsc::UnboundedReceiver<CapturedPeerSend> {
    let (sends, rx) = mpsc::unbounded_channel();
    engine.peers = Arc::new(RecordingPeerSender { sends, response });
    rx
}

pub async fn recv_peer_send(
    rx: &mut mpsc::UnboundedReceiver<CapturedPeerSend>,
) -> CapturedPeerSend {
    tokio::time::timeout(TEST_RECV_TIMEOUT.to_std(), rx.recv())
        .await
        .expect("peer send timed out")
        .expect("peer send channel closed")
}

pub async fn recv_peer_send_with_api(
    rx: &mut mpsc::UnboundedReceiver<CapturedPeerSend>,
    api_key: i16,
) -> CapturedPeerSend {
    let deadline = tokio::time::Instant::now() + TEST_RECV_TIMEOUT.to_std();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let send = tokio::time::timeout(remaining, rx.recv())
            .await
            .expect("peer send with api timed out")
            .expect("peer send channel closed");
        if send.api_key == api_key {
            return send;
        }
    }
}

pub fn one_offset_batch(base_offset: i64, epoch: i32, value: &[u8]) -> RecordBatch {
    RecordBatch {
        base_offset,
        partition_leader_epoch: epoch,
        last_offset_delta: 0,
        records: vec![Record {
            value: Some(bytes::Bytes::copy_from_slice(value)),
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub fn elect_single_voter_engine(engine: &mut Engine) {
    engine.on_event(Event::ElectionTimeout);
    assert2::assert!(engine.core.role().is_leader());
}

/// A realistic single-partition create batch: a `V1Topic` plus its one
/// `V1Partition`. KIP-631 framing derives the topic's partition count from
/// the partition records (the `TopicRecord` wire shape carries no count), so
/// a bare `V1Topic` would round-trip back to zero partitions and fail
/// validation on apply.
pub fn topic_record(name: &str) -> Vec<krabka_metadata::MetadataRecord> {
    topic_record_named(name, 1)
}

pub fn topic_record_named(name: &str, id: u128) -> Vec<krabka_metadata::MetadataRecord> {
    vec![
        krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: name.to_string(),
            topic_id: uuid::Uuid::from_u128(id),
            partitions: 1,
            replication_factor: 1,
        }),
        krabka_metadata::MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
            topic: name.to_string(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1)],
            isr: vec![NodeId(1)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }),
    ]
}

/// Drive a voter to leadership in a multi-voter cluster under `NullPeerSender`
/// by injecting the vote responses it would have received: `ElectionTimeout`
/// starts a pre-vote round (epoch unchanged), a granted pre-vote from `helper`
/// promotes to `Candidate` (epoch +1) and broadcasts a real vote, and a
/// granted real vote from `helper` reaches majority and promotes to `Leader`.
pub async fn elect_leader_with_helper(ctrl: &KraftController, me: NodeId, helper: NodeId) {
    ctrl.inject_event(Event::ElectionTimeout).await.unwrap();
    // Pre-vote round runs at the current (pre-bump) epoch 0.
    ctrl.inject_event(Event::ReceiveVoteResponse {
        from: helper,
        epoch: 0,
        vote_granted: true,
    })
    .await
    .unwrap();
    // Candidate round runs at the bumped epoch 1.
    ctrl.inject_event(Event::ReceiveVoteResponse {
        from: helper,
        epoch: 1,
        vote_granted: true,
    })
    .await
    .unwrap();
    await_leader(ctrl, Some(me)).await;
}

pub async fn await_leader(ctrl: &KraftController, want: Option<NodeId>) {
    let result = tokio::time::timeout(StdDuration::from_secs(2), async {
        let mut rx = ctrl.watch_leader();
        loop {
            if *rx.borrow() == want {
                return;
            }
            rx.changed().await.expect("leader watch closed");
        }
    })
    .await;
    assert2::assert!(result.is_ok());
}

pub async fn submit_change_with_timeout(
    ctrl: &KraftController,
    records: Vec<krabka_metadata::MetadataRecord>,
    context: &str,
) -> Result<(), RaftError> {
    tokio::time::timeout(StdDuration::from_secs(2), ctrl.submit_change(records))
        .await
        .unwrap_or_else(|_| panic!("{context} submit_change timed out"))
        .map(|_| ())
}
