//! `FetchSnapshot` (`api_key` 59, KIP-630), end to end.
//!
//! The test boots one in-process broker, creates a topic so that the metadata
//! image is non-empty, and triggers a controller snapshot. It then fetches the
//! `__cluster_metadata` snapshot byte range over the wire and asserts that the
//! broker serves the page. The broker listener answers as Kafka's
//! `KafkaRaftClient.handleFetchSnapshotRequest` does, so a request names the
//! snapshot it wants and the leader epoch it believes in.
#![cfg(test)]

use {
    crate::support::topics::{creatable_topic, create_topic_request},
    assert2::{assert, check},
    krabka_protocol::owned::fetch_snapshot_request::{
        FetchSnapshotRequest, PartitionSnapshot, SnapshotId, TopicSnapshot,
    },
    std::{
        fs,
        path::Path,
        time::{Duration, Instant},
    },
    tokio::time,
};

mod support;

const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";
/// The `(end_offset, epoch)` of one snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CheckpointId {
    end_offset: i64,
    epoch: i32,
}

fn fetch(snapshot: CheckpointId, leader_epoch: i32, position: i64) -> FetchSnapshotRequest {
    FetchSnapshotRequest {
        replica_id: -1,
        max_bytes: 1 << 20,
        topics: vec![TopicSnapshot {
            name: CLUSTER_METADATA_TOPIC.into(),
            partitions: vec![PartitionSnapshot {
                partition: 0,
                current_leader_epoch: leader_epoch,
                snapshot_id: SnapshotId {
                    end_offset: snapshot.end_offset,
                    epoch: snapshot.epoch,
                    ..Default::default()
                },
                position,
                ..Default::default()
            }],
            ..Default::default()
        }],
        cluster_id: None,
        ..Default::default()
    }
}

/// The `(end_offset, epoch)` of every `<end_offset>-<epoch>.checkpoint` file
/// under `dir`, which is how the controller names the snapshots it wrote.
fn checkpoint_ids(dir: &Path) -> Vec<CheckpointId> {
    let mut ids = Vec::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return ids;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            ids.extend(checkpoint_ids(&path));
        } else if let Some((end_offset, epoch)) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_suffix(".checkpoint"))
            .and_then(|name| name.split_once('-'))
            && let (Ok(end_offset), Ok(epoch)) = (end_offset.parse(), epoch.parse())
        {
            ids.push(CheckpointId { end_offset, epoch });
        }
    }
    ids
}

use {
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::fetch_snapshot_response::FetchSnapshotResponse,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct SnapshotFetch {
    broker: Option<BrokerHandle>,
    client: Option<Client>,
    directory: Option<TempDir>,
    snapshot: Option<CheckpointId>,
    leader_epoch: Option<i32>,
    size: Option<i64>,
    response: Option<FetchSnapshotResponse>,
}

#[fixture]
fn snapshot_fetch() -> SnapshotFetch {
    // Start this scenario with fresh inputs and observations.

    SnapshotFetch {
        broker: None,
        client: None,
        directory: None,
        snapshot: None,
        leader_epoch: None,
        size: None,
        response: None,
    }
}

#[given("a broker with an owned metadata data directory")]
async fn broker(snapshot_fetch: &mut SnapshotFetch) {
    let directory = tempfile::tempdir().expect("tempdir");
    let (broker, client) = support::start_with_dir(directory.path()).await;
    snapshot_fetch.broker = Some(broker);
    snapshot_fetch.client = Some(client);
    snapshot_fetch.directory = Some(directory);
}

#[given("the metadata image contains topic {topic}")]
async fn metadata(snapshot_fetch: &SnapshotFetch, topic: String) {
    let response = snapshot_fetch
        .client
        .as_ref()
        .unwrap()
        .send(create_topic_request(creatable_topic().name(topic).call()))
        .await
        .unwrap();
    assert!(response.topics[0].error_code == 0);
}

#[when("a controller snapshot is requested")]
async fn trigger(snapshot_fetch: &SnapshotFetch) {
    snapshot_fetch
        .broker
        .as_ref()
        .unwrap()
        .trigger_snapshot_for_test()
        .await
        .expect("trigger snapshot");
}

#[when(
    "FetchSnapshot requests topic {topic}, snapshot offset {offset}, snapshot epoch {epoch}, leader epoch {leader_epoch}, and position {position}"
)]
async fn explicit_fetch(
    snapshot_fetch: &mut SnapshotFetch,
    topic: String,
    offset: i64,
    epoch: i32,
    leader_epoch: i32,
    position: i64,
) {
    let mut request = fetch(
        CheckpointId {
            end_offset: offset,
            epoch,
        },
        leader_epoch,
        position,
    );
    request.topics[0].name = topic;
    snapshot_fetch.response = Some(
        snapshot_fetch
            .client
            .as_ref()
            .unwrap()
            .send(request)
            .await
            .unwrap(),
    );
}

#[then(
    "the partition refuses stale epoch with code {error} and advertises leader {leader} with a positive epoch"
)]
fn leader_refusal(snapshot_fetch: &mut SnapshotFetch, error: i16, leader: i32) {
    let part = &snapshot_fetch.response.as_ref().unwrap().topics[0].partitions[0];
    assert!(part.error_code == error);
    check!(part.current_leader.leader_id == leader);
    assert!(part.current_leader.leader_epoch > 0);
    snapshot_fetch.leader_epoch = Some(part.current_leader.leader_epoch);
}

#[when(
    "the checkpoint file appears within {seconds} seconds and its first page is requested using the advertised epoch"
)]
async fn first_page(snapshot_fetch: &mut SnapshotFetch, seconds: u64) {
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let snapshot = loop {
        if let Some(id) = checkpoint_ids(snapshot_fetch.directory.as_ref().unwrap().path())
            .into_iter()
            .max()
        {
            break id;
        }
        assert!(
            Instant::now() <= deadline,
            "snapshot not written within {seconds}s"
        );
        time::sleep(Duration::from_millis(50)).await;
    };
    snapshot_fetch.response = Some(
        snapshot_fetch
            .client
            .as_ref()
            .unwrap()
            .send(fetch(snapshot, snapshot_fetch.leader_epoch.unwrap(), 0))
            .await
            .unwrap(),
    );
    snapshot_fetch.snapshot = Some(snapshot);
}

#[then(
    "the exact named snapshot succeeds on partition zero with a positive total size and nonempty page"
)]
fn named_page(snapshot_fetch: &mut SnapshotFetch) {
    let snapshot = snapshot_fetch.snapshot.unwrap();
    let out = snapshot_fetch.response.as_ref().unwrap();
    assert!(out.error_code == 0);
    let part = &out.topics[0].partitions[0];
    assert!(part.error_code == 0);
    check!(part.index == 0);
    check!(part.snapshot_id.end_offset == snapshot.end_offset);
    check!(part.snapshot_id.epoch == snapshot.epoch);
    check!(part.size > 0);
    check!(part.unaligned_records.payload_len() > 0);
    snapshot_fetch.size = Some(part.size);
}

#[when("the same epoch requests a snapshot with offset increased by {delta} at position zero")]
async fn absent_snapshot(snapshot_fetch: &mut SnapshotFetch, delta: i64) {
    let mut snapshot = snapshot_fetch.snapshot.unwrap();
    snapshot.end_offset += delta;
    snapshot_fetch.response = Some(
        snapshot_fetch
            .client
            .as_ref()
            .unwrap()
            .send(fetch(snapshot, snapshot_fetch.leader_epoch.unwrap(), 0))
            .await
            .unwrap(),
    );
}

#[when("the named snapshot is requested at its reported total size")]
async fn past_end(snapshot_fetch: &mut SnapshotFetch) {
    snapshot_fetch.response = Some(
        snapshot_fetch
            .client
            .as_ref()
            .unwrap()
            .send(fetch(
                snapshot_fetch.snapshot.unwrap(),
                snapshot_fetch.leader_epoch.unwrap(),
                snapshot_fetch.size.unwrap(),
            ))
            .await
            .unwrap(),
    );
}

#[then("FetchSnapshot has top-level error {top} and partition error {partition}")]
fn errors(snapshot_fetch: &SnapshotFetch, top: i16, partition: i16) {
    let response = snapshot_fetch.response.as_ref().unwrap();
    check!(response.error_code == top);
    check!(response.topics[0].partitions[0].error_code == partition);
}

#[scenario(
    path = "tests/features/migrated/tests/fetch_snapshot.feature",
    name = "fetch snapshot serves the named metadata snapshot"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_snapshot_serves_the_named_metadata_snapshot(snapshot_fetch: SnapshotFetch) {
    drop(snapshot_fetch.client);
    snapshot_fetch.broker.unwrap().shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/fetch_snapshot.feature",
    name = "fetch snapshot rejects non metadata topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_snapshot_rejects_non_metadata_topic(snapshot_fetch: SnapshotFetch) {
    drop(snapshot_fetch.client);
    snapshot_fetch.broker.unwrap().shutdown().await;
}
