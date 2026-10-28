//! KIP-630 snapshot catch-up: a voter that joins with an empty log, far behind
//! the leader's pruned `log_start`, converges on the leader's metadata image
//! through `FetchSnapshot` rather than through log replication, and keeps that
//! transfer alive when the node serving it rolls to a newer checkpoint.

use {
    crate::{
        harness::{
            await_single_leader, await_until, build_engine, shutdown_nodes, three_voter_network,
            topic_record, uniform,
        },
        sim_net::SimNet,
    },
    krabka_protocol::{
        Decode, owned::fetch_snapshot_response::FetchSnapshotResponse, records::RecordBatch,
    },
    krabka_raft::kraft::{
        NodeId, PeerSender,
        transport::{api_key, wire},
    },
    krabka_units::prelude::{Time, secs},
    std::{
        collections::{BTreeSet, HashMap},
        fs,
        path::Path,
        sync::Arc,
        time::Duration,
    },
    tokio::time,
};

/// Per-node election timeouts for these tests, staggered as the harness's
/// `STAGGERED_TIMEOUTS` are, but wide enough that a scheduling stall on a
/// loaded runner does not depose the leader mid-transfer.
///
/// None of these tests is about an election: each needs the leader it elected
/// to stay the leader. A voter elects after one fetch timeout without a
/// successful Fetch, as Kafka's does, and a leader resigns after 1.5 of its
/// own without hearing from a majority, so the harness's 150 ms to 450 ms
/// leave no room for a pause of a few hundred milliseconds.
const SNAPSHOT_TIMEOUTS: [Time; 3] = [secs(1), secs(2), secs(3)];

fn start_snapshot_pair(
    net: &SimNet,
    voters: &[NodeId],
    cid: uuid::Uuid,
    interval: u64,
) -> HashMap<NodeId, tempfile::TempDir> {
    [NodeId(1), NodeId(2)]
        .into_iter()
        .map(|id| {
            let idx = usize::try_from(id.0 - 1).unwrap();
            let (ctrl, dir) = build_engine()
                .net(net)
                .me(id)
                .ids(voters)
                .cluster_id(cid)
                .timeouts(uniform(SNAPSHOT_TIMEOUTS[idx]))
                .snapshot_interval_records(interval)
                .call();
            net.register(id, ctrl);
            (id, dir)
        })
        .collect()
}

/// An elected pair: the data directories that must outlive it, the leader and
/// the epoch it leads.
struct ElectedSnapshotPair {
    dirs: HashMap<NodeId, tempfile::TempDir>,
    leader: NodeId,
    epoch: u32,
}

async fn elected_snapshot_pair(
    net: &SimNet,
    ids: &[NodeId],
    cluster_id: uuid::Uuid,
    interval: u64,
) -> ElectedSnapshotPair {
    let dirs = start_snapshot_pair(net, ids, cluster_id, interval);
    let (leader, epoch) =
        await_single_leader(net, &[NodeId(1), NodeId(2)], Duration::from_secs(10)).await;
    ElectedSnapshotPair {
        dirs,
        leader,
        epoch,
    }
}

fn fetch_from_zero(epoch: u32) -> bytes::Bytes {
    wire::PeerRequest::Fetch {
        cluster_id: None,
        max_wait_ms: 0,
        high_watermark: -1,
        from: NodeId(3),
        current_leader_epoch: i32::try_from(epoch).unwrap(),
        fetch_epoch: 0,
        fetch_offset: 0,
        replica_directory_id: uuid::Uuid::nil(),
    }
    .try_encode()
    .unwrap()
}

async fn submit_burst(net: &SimNet, leader: NodeId, count: usize, prefix: &str, first_id: u128) {
    for i in 0..count {
        time::timeout(
            Duration::from_secs(10),
            net.get(leader).unwrap().submit_change(vec![topic_record(
                &format!("{prefix}{i}"),
                first_id + i as u128,
            )]),
        )
        .await
        .expect("burst submit did not hang")
        .expect("burst submit ok");
    }
}

/// The `.checkpoint` artifacts a node currently holds, by file name. The
/// checkpoint directory is also the metadata log's own segment directory, so
/// the extension filter is what separates checkpoints from `.log` / `.index`.
fn checkpoint_names(dir: &Path) -> BTreeSet<String> {
    fs::read_dir(dir)
        .expect("read checkpoint dir")
        .flatten()
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|ext| ext == "checkpoint")
        })
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect()
}

use rstest_bdd_macros::{given, then, when};
struct SnapshotCluster {
    net: SimNet,
    ids: [NodeId; 3],
    cluster: uuid::Uuid,
    pair: Option<ElectedSnapshotPair>,
    leader_image: Option<Arc<krabka_metadata::MetadataImage>>,
    checkpoint_names: BTreeSet<String>,
    after_roll: BTreeSet<String>,
    follower_start: Option<i64>,
    fetch_answer: Option<wire::FetchAnswer>,
    snapshot_response: Option<FetchSnapshotResponse>,
    snapshot_id: Option<(i64, i32)>,
    assembled: Vec<u8>,
    total: Option<usize>,
    chunk_results: Vec<((i64, i32), i16)>,
}
#[rstest::fixture]
fn snapshots() -> SnapshotCluster {
    let (net, ids) = three_voter_network();
    SnapshotCluster {
        net,
        ids,
        cluster: uuid::Uuid::nil(),
        pair: None,
        leader_image: None,
        checkpoint_names: BTreeSet::new(),
        after_roll: BTreeSet::new(),
        follower_start: None,
        fetch_answer: None,
        snapshot_response: None,
        snapshot_id: None,
        assembled: vec![],
        total: None,
        chunk_results: vec![],
    }
}
#[given("two voters in cluster {cluster} have elected a leader and snapshot every five records")]
async fn snapshot_pair(snapshots: &mut SnapshotCluster, cluster: u128) {
    snapshots.cluster = uuid::Uuid::from_u128(cluster);
    snapshots.pair =
        Some(elected_snapshot_pair(&snapshots.net, &snapshots.ids, snapshots.cluster, 5).await);
}
#[when("the leader commits fifteen topics prefixed {prefix} beginning at identity {identity}")]
async fn snapshot_burst(snapshots: &mut SnapshotCluster, prefix: String, identity: u128) {
    let pair = snapshots.pair.as_ref().expect("elected pair");
    submit_burst(&snapshots.net, pair.leader, 15, &prefix, identity).await;
    let leader = snapshots.net.get(pair.leader).expect("leader");
    await_until(Duration::from_secs(10), || {
        (leader.quorum_snapshot().log_start_offset > 0).then_some(())
    })
    .await;
    snapshots.leader_image = Some(leader.current_image());
    snapshots.checkpoint_names = checkpoint_names(pair.dirs[&pair.leader].path());
}
#[then("the leader has pruned its log and contains all fifteen topics prefixed {prefix}")]
fn leader_snapshot_image(snapshots: &SnapshotCluster, prefix: String) {
    let pair = snapshots.pair.as_ref().expect("elected pair");
    assert2::assert!(
        snapshots
            .net
            .get(pair.leader)
            .expect("leader")
            .quorum_snapshot()
            .log_start_offset
            > 0
    );
    let image = snapshots
        .leader_image
        .as_ref()
        .expect("committed leader image");
    for index in 0..15 {
        assert2::assert!(image.topic(&format!("{prefix}{index}")).is_some());
    }
}
#[when("voter 3 joins with an empty log and catches up to the leader image")]
async fn empty_voter_catches_up(snapshots: &mut SnapshotCluster) {
    let (controller, directory) = build_engine()
        .net(&snapshots.net)
        .me(3)
        .ids(&snapshots.ids)
        .cluster_id(snapshots.cluster)
        .timeouts(uniform(SNAPSHOT_TIMEOUTS[2]))
        .snapshot_interval_records(5)
        .call();
    snapshots.net.register(NodeId(3), controller);
    snapshots
        .pair
        .as_mut()
        .expect("elected pair")
        .dirs
        .insert(NodeId(3), directory);
    let lag = snapshots.net.get(NodeId(3)).expect("new voter");
    let image = snapshots.leader_image.as_ref().expect("leader image");
    await_until(Duration::from_secs(10), || {
        (*lag.current_image() == **image).then_some(())
    })
    .await;
}
#[then("voter 3 has the complete leader image and a nonzero snapshot log boundary")]
fn installed_snapshot_image(snapshots: &SnapshotCluster) {
    let lag = snapshots.net.get(NodeId(3)).expect("new voter");
    assert2::assert!(
        *lag.current_image() == **snapshots.leader_image.as_ref().expect("leader image")
    );
    assert2::assert!(lag.quorum_snapshot().log_start_offset > 0);
}
#[when("the follower finishes independent pruning and receives a Fetch from offset zero")]
async fn fetch_pruned_follower(snapshots: &mut SnapshotCluster) {
    let pair = snapshots.pair.as_ref().expect("elected pair");
    let follower = NodeId(2);
    let committed = snapshots
        .net
        .get(pair.leader)
        .expect("leader")
        .quorum_state()
        .await
        .expect("leader state")
        .high_watermark;
    let controller = snapshots.net.get(follower).expect("follower");
    let directory = pair.dirs[&follower].path();
    let (names, start) = await_until(Duration::from_secs(10), || {
        let state = controller.quorum_snapshot();
        let names = checkpoint_names(directory);
        (state.high_watermark == committed
            && state.log_start_offset > 0
            && committed - state.log_start_offset < 5
            && (1..=2).contains(&names.len()))
        .then_some((names, state.log_start_offset))
    })
    .await;
    snapshots.checkpoint_names = names;
    snapshots.follower_start = Some(start);
    let body = snapshots
        .net
        .send(follower, api_key::FETCH, fetch_from_zero(pair.epoch))
        .await
        .expect("Fetch answered");
    let Some(wire::PeerResponse::Fetch(answer)) = wire::PeerResponse::decode_fetch(&body) else {
        panic!("decodable Fetch response required")
    };
    snapshots.fetch_answer = Some(answer);
}
#[then(
    "the follower retains one or two checkpoints and refuses Fetch with the current leader and its own log start"
)]
fn follower_fetch_refusal(snapshots: &SnapshotCluster) {
    let pair = snapshots.pair.as_ref().expect("elected pair");
    assert2::assert!(pair.leader == NodeId(1));
    assert2::assert!((1..=2).contains(&snapshots.checkpoint_names.len()));
    assert2::assert!(
        snapshots.fetch_answer.as_ref()
            == Some(&wire::FetchAnswer {
                error_code: 6,
                leader: wire::QuorumLeader {
                    leader_id: Some(pair.leader),
                    epoch: pair.epoch,
                    endpoint: None
                },
                diverging: None,
                snapshot_id: None,
                hwm: -1,
                log_start_offset: snapshots.follower_start.expect("pruned follower boundary"),
                records: bytes::Bytes::new(),
            })
    );
}
#[when("the lagging peer asks the follower for its latest checkpoint")]
async fn follower_snapshot_request(snapshots: &mut SnapshotCluster) {
    let pair = snapshots.pair.as_ref().expect("elected pair");
    let latest = snapshots
        .checkpoint_names
        .last()
        .expect("checkpoint exists");
    let (offset, epoch) = latest
        .trim_end_matches(".checkpoint")
        .split_once('-')
        .expect("checkpoint name");
    let request = wire::PeerRequest::FetchSnapshot {
        cluster_id: Some(snapshots.cluster),
        from: NodeId(3),
        current_leader_epoch: i32::try_from(pair.epoch).expect("wire epoch"),
        snapshot_id: (
            offset.parse().expect("snapshot offset"),
            epoch.parse().expect("snapshot epoch"),
        ),
        position: 0,
        max_bytes: i32::MAX,
    }
    .try_encode()
    .expect("encode FetchSnapshot");
    let body = snapshots
        .net
        .send(NodeId(2), api_key::FETCH_SNAPSHOT, request)
        .await
        .expect("snapshot refusal answered");
    snapshots.snapshot_response = Some(
        FetchSnapshotResponse::decode(&mut &body[..], wire::FETCH_SNAPSHOT_VERSION)
            .expect("decode FetchSnapshot"),
    );
}
#[then(
    "the follower refuses FetchSnapshot with NotLeaderOrFollower and leader 1 at the current epoch"
)]
fn follower_snapshot_refusal(snapshots: &SnapshotCluster) {
    let response = snapshots
        .snapshot_response
        .as_ref()
        .expect("snapshot response");
    let partition = &response.topics[0].partitions[0];
    let epoch = snapshots.pair.as_ref().expect("elected pair").epoch;
    assert2::assert!(
        (
            partition.error_code,
            partition.current_leader.leader_id,
            partition.current_leader.leader_epoch
        ) == (6, 1, i32::try_from(epoch).expect("wire epoch"))
    );
}
async fn request_snapshot_chunk(snapshots: &mut SnapshotCluster, maximum: i32) -> bytes::Bytes {
    let pair = snapshots.pair.as_ref().expect("elected pair");
    let request = wire::PeerRequest::FetchSnapshot {
        cluster_id: Some(snapshots.cluster),
        from: NodeId(3),
        current_leader_epoch: i32::try_from(pair.epoch).expect("wire epoch"),
        snapshot_id: snapshots.snapshot_id.expect("transfer identity"),
        position: i64::try_from(snapshots.assembled.len()).expect("chunk position"),
        max_bytes: maximum,
    }
    .try_encode()
    .expect("encode chunk request");
    let body = snapshots
        .net
        .send(pair.leader, api_key::FETCH_SNAPSHOT, request)
        .await
        .expect("chunk answered");
    let Some(wire::PeerResponse::FetchSnapshot {
        snapshot_id,
        size,
        bytes,
        error_code,
        ..
    }) = wire::PeerResponse::decode_fetch_snapshot(&body)
    else {
        panic!("decodable snapshot chunk required")
    };
    snapshots.chunk_results.push((snapshot_id, error_code));
    snapshots.total = Some(usize::try_from(size).expect("snapshot size"));
    bytes
}
#[when("a lagging peer starts a snapshot transfer with only the header batch")]
async fn start_snapshot_transfer(snapshots: &mut SnapshotCluster) {
    let pair = snapshots.pair.as_ref().expect("elected pair");
    let body = snapshots
        .net
        .send(pair.leader, api_key::FETCH, fetch_from_zero(pair.epoch))
        .await
        .expect("Fetch answered");
    let Some(wire::PeerResponse::Fetch(answer)) = wire::PeerResponse::decode_fetch(&body) else {
        panic!("decodable Fetch required")
    };
    snapshots.snapshot_id = Some(
        answer
            .snapshot_id
            .expect("pruned Fetch points to checkpoint"),
    );
    let directory = pair.dirs[&pair.leader].path();
    let checkpoint = directory.join(
        snapshots
            .checkpoint_names
            .last()
            .expect("latest checkpoint"),
    );
    let bytes = fs::read(checkpoint).expect("read checkpoint");
    let mut cursor = &bytes[..];
    RecordBatch::decode(&mut cursor).expect("decode header batch");
    let maximum = i32::try_from(bytes.len() - cursor.len()).expect("header batch wire size");
    let chunk = request_snapshot_chunk(snapshots, maximum).await;
    snapshots.assembled.extend_from_slice(&chunk);
}
#[then(
    "the snapshot transfer is incomplete and every chunk retains its original identity without error"
)]
fn partial_snapshot_transfer(snapshots: &SnapshotCluster) {
    assert2::assert!(snapshots.assembled.len() < snapshots.total.expect("snapshot size"));
    for &(identity, error) in &snapshots.chunk_results {
        assert2::assert!((Some(identity), error) == (snapshots.snapshot_id, 0));
    }
}
#[when("newly committed topics roll the leader onto a newer checkpoint")]
async fn roll_checkpoint(snapshots: &mut SnapshotCluster) {
    let pair = snapshots.pair.as_ref().expect("elected pair");
    let directory = pair.dirs[&pair.leader].path();
    for index in 0_u128..10 {
        time::timeout(
            Duration::from_secs(10),
            snapshots
                .net
                .get(pair.leader)
                .expect("leader")
                .submit_change(vec![topic_record(&format!("s{index}"), 4000 + index)]),
        )
        .await
        .expect("submit did not hang")
        .expect("submit committed");
        if checkpoint_names(directory) != snapshots.checkpoint_names {
            break;
        }
    }
    snapshots.after_roll = checkpoint_names(directory);
}
#[then("the leader checkpoint catalogue has changed")]
fn checkpoint_rolled(snapshots: &SnapshotCluster) {
    assert2::assert!(snapshots.after_roll != snapshots.checkpoint_names);
}
#[when("the peer finishes the original snapshot transfer against the rolled leader")]
async fn finish_snapshot_transfer(snapshots: &mut SnapshotCluster) {
    while snapshots.assembled.len() < snapshots.total.expect("snapshot size") {
        let chunk = request_snapshot_chunk(snapshots, i32::MAX).await;
        assert2::assert!(!chunk.is_empty(), "snapshot transfer must make progress");
        snapshots.assembled.extend_from_slice(&chunk);
    }
}
#[then("all chunks preserve the original snapshot identity and decode only the pre-roll topics")]
fn original_snapshot_topics(snapshots: &SnapshotCluster) {
    for &(identity, error) in &snapshots.chunk_results {
        assert2::assert!((Some(identity), error) == (snapshots.snapshot_id, 0));
    }
    let records = krabka_raft::deserialize_metadata_snapshot(&snapshots.assembled)
        .expect("reassembled snapshot decodes");
    let topics = records
        .iter()
        .filter_map(|record| match record {
            krabka_metadata::MetadataRecord::V1Topic(topic) => Some(topic.name.clone()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert2::assert!(topics.contains("r0"));
    assert2::assert!(topics.len() >= 5);
    assert2::assert!(topics.iter().all(|name| name.starts_with('r')));
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/snapshot_catchup.feature",
    name = "An empty voter installs a snapshot and converges"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_follower_catches_up_via_snapshot(snapshots: SnapshotCluster) {
    shutdown_nodes(&snapshots.net, &snapshots.ids).await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/snapshot_catchup.feature",
    name = "A pruned follower redirects Fetch and FetchSnapshot to the leader"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_that_pruned_independently_redirects_a_lagging_fetch_to_the_leader(
    snapshots: SnapshotCluster,
) {
    shutdown_nodes(&snapshots.net, &snapshots.ids).await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kraft_engine_sim/snapshot_catchup.feature",
    name = "A snapshot transfer survives a leader checkpoint roll"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_fetch_in_flight_survives_the_leader_rolling_to_a_new_checkpoint(
    snapshots: SnapshotCluster,
) {
    shutdown_nodes(&snapshots.net, &snapshots.ids).await;
}
