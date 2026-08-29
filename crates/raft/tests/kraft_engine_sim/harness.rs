//! The simulation harness: building an engine over a fresh tempdir log, the
//! metadata values the acceptances submit, and the polling helpers that wait
//! for the quorum to converge.
//!
//! Every acceptance in this suite starts by standing up engines and ends by
//! waiting for agreement, so both halves of that shape are kept here rather
//! than repeated per test module.

use std::{sync::Arc, time::Duration};

use krabka_raft::{
    ControllerFetchMissLimit, MetadataRaftCommandQueueCapacity, MetadataRaftFetchMax,
    kraft::{
        KraftConfig, KraftController, KraftLog, NodeId, QuorumState,
        snapshot_fetch::MetadataSnapshotFetchMax,
    },
};
use krabka_units::prelude::{Time, millis};

use crate::sim_net::SimNet;

/// Per-node election timeouts, staggered so one node reliably wins the first
/// round rather than splitting the vote.
pub(crate) const STAGGERED_TIMEOUTS: [Time; 3] = [millis(150), millis(300), millis(450)];

pub(crate) fn voter_set(ids: &[NodeId]) -> krabka_metadata::voters::VoterSet {
    krabka_metadata::voters::VoterSet::from_voters(ids.iter().map(|&id| {
        krabka_metadata::voters::Voter {
            id,
            directory_id: uuid::Uuid::nil(),
            endpoints: Vec::new(),
            kraft_version: krabka_metadata::voters::KRaftVersionRange::default(),
        }
    }))
}

pub(crate) fn topic_record(name: &str, id: u128) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: name.to_string(),
        topic_id: uuid::Uuid::from_u128(id),
        partitions: 1,
        replication_factor: 1,
    })
}

/// Builds a single engine over a fresh tempdir log, and does not register it.
pub(crate) fn build_engine(
    me: NodeId,
    ids: &[NodeId],
    cluster_id: uuid::Uuid,
    election_timeout: Time,
    net: &SimNet,
) -> (KraftController, tempfile::TempDir) {
    build_engine_with_snapshot_interval(me, ids, cluster_id, election_timeout, net, 0)
}

/// Works like [`build_engine`], but with a caller-chosen
/// `snapshot_interval_records`, where `0` disables snapshots. The snapshot
/// catch-up acceptance uses a small interval, so the leader snapshots and prunes
/// its log after a short burst.
pub(crate) fn build_engine_with_snapshot_interval(
    me: NodeId,
    ids: &[NodeId],
    cluster_id: uuid::Uuid,
    election_timeout: Time,
    net: &SimNet,
    snapshot_interval_records: u64,
) -> (KraftController, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let log = KraftLog::open(dir.path()).expect("open log");
    let ctrl = KraftController::spawn(
        KraftConfig {
            me,
            cluster_id,
            initial_state: QuorumState::bootstrap(cluster_id, voter_set(ids)),
            election_timeout,
            heartbeat_interval: None,
            controller_fetch_miss_limit: ControllerFetchMissLimit::default(),
            metadata_raft_command_queue_capacity: MetadataRaftCommandQueueCapacity::default(),
            metadata_raft_fetch_max: MetadataRaftFetchMax::default(),
            peers: Arc::new(net.clone()),
            snapshot_interval_records,
            metadata_snapshot_fetch_max: MetadataSnapshotFetchMax::default(),
        },
        log,
        dir.path().to_path_buf(),
    );
    (ctrl, dir)
}

/// Polls `f` until it returns `Some`, bounded by `timeout`. The helper yields
/// between polls, so the engine loops make progress. It returns the value, or
/// panics on a timeout.
pub(crate) async fn await_until<T, F>(timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Option<T>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert2::assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met before timeout"
        );
        tokio::task::yield_now().await;
    }
}

/// The set of `(node, leader_id, leader_epoch)` that each live engine currently
/// believes, read through the non-mutating `quorum_state` handle op.
async fn leaders(net: &SimNet, ids: &[NodeId]) -> Vec<(NodeId, Option<NodeId>, u32)> {
    let mut out = Vec::new();
    for &id in ids {
        if let Some(ctrl) = net.get(id)
            && let Ok(qs) = ctrl.quorum_state().await
        {
            out.push((id, qs.leader_id, qs.leader_epoch));
        }
    }
    out
}

/// Waits until exactly one node believes it is the leader and every live node
/// agrees on that leader id and epoch. Returns `(leader_id, epoch)`.
pub(crate) async fn await_single_leader(
    net: &SimNet,
    ids: &[NodeId],
    timeout: Duration,
) -> (NodeId, u32) {
    let net = net.clone();
    let ids = ids.to_vec();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let snap = leaders(&net, &ids).await;
        // All live nodes must report the same Some(leader) and same epoch, and
        // that leader must be one of the live nodes.
        if !snap.is_empty() {
            let first_leader = snap[0].1;
            let first_epoch = snap[0].2;
            if let Some(leader) = first_leader {
                let agree = snap
                    .iter()
                    .all(|(_, l, e)| *l == Some(leader) && *e == first_epoch);
                let live = ids.contains(&leader);
                if agree && live {
                    return (leader, first_epoch);
                }
            }
        }
        assert2::assert!(
            tokio::time::Instant::now() < deadline,
            "no single leader before timeout; states={snap:?}"
        );
        tokio::task::yield_now().await;
    }
}
