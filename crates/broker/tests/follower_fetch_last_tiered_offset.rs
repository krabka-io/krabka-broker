//! KIP-1023 over three in-process brokers: a new replica of a tiered
//! partition starts at the leader's earliest pending upload offset when
//! `follower.fetch.last.tiered.offset.enable` is set, and at the leader's local
//! log start when it is not.
//!
//! Each partition is assigned to broker 1 alone, archived, and evicted locally
//! only in part: local retention keeps a few sealed segments that are already
//! in the remote tier. That is the band the flag decides about. Broker 3 is
//! then added as a replica. Its empty log fetches offset 0, which lies below
//! the leader's local log start, so the leader answers
//! `OFFSET_MOVED_TO_TIERED_STORAGE`:
//!
//! - Without the flag, the replica restarts at the leader's local log start
//!   and replicates every local segment, the tiered ones included.
//! - With the flag, set dynamically at the cluster default or for broker 3
//!   alone, it restarts at the leader's earliest pending upload offset and
//!   replicates only what is still waiting for an upload.
//!
//! The flag is changed between partitions on the running cluster, so the test
//! also shows that the follower reads it per decision, with no restart.
//!
//! The measurement is the new replica's cumulative `replication_bytes_in`,
//! against the bytes of the sealed segments the leader still holds locally:
//! the flag is what decides whether the replica pulls them. The replica's
//! first local segment backs it up for the flagged cases. It can only move up
//! afterwards, so it cannot flatter a replica that started low.
#![cfg(test)]

mod support;

// The shared three-broker boot of the multi-broker tiered suite. This binary
// uses only part of it, as every binary uses only part of `support`, whose
// module carries the same allow.
#[allow(dead_code)]
#[path = "tiered_storage_multi_broker/multi_cluster.rs"]
mod multi_cluster;

use {
    crate::{
        multi_cluster::{
            TieredCluster, controller_leader_addr, local_log_files, ready_admin,
            start_tiered_cluster_with,
        },
        support::{
            client::connect_owned,
            configs::{ConfigMutation, ConfigOperation, broker_alter_request},
            offsets::{list_offset_partition, single_partition_list_offsets},
            tiered::produce_records,
            topic_id_for,
        },
    },
    assert2::{assert, check},
    krabka_broker::{BrokerHandle, metrics::PartitionLabel},
    krabka_client_admin::{AdminClient, CreateTopicSpec, TopicMutationOptions},
    krabka_client_core::Client,
    krabka_protocol::owned::alter_partition_reassignments_request::{
        AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
    },
    std::{
        collections::BTreeMap,
        path::Path,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::time,
};

/// The dynamic broker config under test.
const KEY: &str = "follower.fetch.last.tiered.offset.enable";
/// Kafka's `ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP`.
const EARLIEST_PENDING_UPLOAD_TIMESTAMP: i64 = -6;
/// The broker added as a replica.
const NEW_REPLICA: i32 = 3;

/// Where the flag is set when broker 3 joins a partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flag {
    /// Never published: Kafka's default `false`.
    Unset,
    /// The cluster-wide default broker config.
    ClusterDefault,
    /// Broker 3's own dynamic config, with the cluster default removed.
    NewReplicaOnly,
}

/// The base offsets of the `*.log` files one [`local_log_files`] listing
/// holds, ascending.
fn segment_bases(files: &[(i64, u64)]) -> Vec<i64> { files.iter().map(|(base, _)| *base).collect() }

/// The total size of every `*.log` file in one [`local_log_files`] listing
/// but the active (last) one.
fn sealed_bytes(files: &[(i64, u64)]) -> u64 {
    files
        .split_last()
        .map_or(0, |(_, sealed)| sealed.iter().map(|(_, len)| len).sum())
}

/// Whether local retention has evicted all it can from one [`local_log_files`]
/// listing. Kafka's size pass drops the oldest sealed segment only while the
/// local log's bytes over `local.retention.bytes` still cover that whole
/// segment, so it is done once the oldest sealed segment holds more bytes than
/// the log has over the budget. Nothing is produced after the setup, so a log
/// in that state keeps its segments.
fn local_retention_done(files: &[(i64, u64)], local_retention_bytes: u64) -> bool {
    let total: u64 = files.iter().map(|(_, len)| len).sum();
    match files {
        [(_, oldest), _, ..] => *oldest > total.saturating_sub(local_retention_bytes),
        _ => true,
    }
}

/// Creates `topic` on broker 1 alone, tiered, with partial local retention.
async fn create_tiered_topic(
    admin: &mut AdminClient,
    leader: &BrokerHandle,
    topic: &str,
    local_retention: u64,
) {
    let response = admin
        .create_topics(
            &[CreateTopicSpec::builder()
                .name(topic)
                .configs(BTreeMap::from([
                    ("remote.storage.enable".into(), "true".into()),
                    ("local.retention.bytes".into(), local_retention.to_string()),
                    ("retention.bytes".into(), "-1".into()),
                    ("retention.ms".into(), "-1".into()),
                ]))
                .replica_assignments(BTreeMap::from([(0, vec![1])]))
                .build()],
            TopicMutationOptions::with_timeout(krabka_units::secs(10)),
        )
        .await
        .expect("CreateTopics");
    assert!(
        response[0].error.is_none(),
        "CreateTopics failed: {response:?}"
    );
    leader
        .wait_for_image(|img| img.partition(topic, 0).is_some())
        .await;
}

/// The leader's answer to `ListOffsets(EARLIEST_PENDING_UPLOAD_TIMESTAMP)`.
async fn earliest_pending_upload(admin: &Client, topic: &str) -> i64 {
    let response = admin
        .send(single_partition_list_offsets(
            topic,
            list_offset_partition(0, EARLIEST_PENDING_UPLOAD_TIMESTAMP),
        ))
        .await
        .expect("ListOffsets");
    let row = &response.topics[0].partitions[0];
    assert!(row.error_code == 0, "ListOffsets failed: {response:?}");
    row.offset
}

/// What the leader holds once the copy pass and local retention have settled.
#[derive(Debug)]
struct SettledLeader {
    local_log_start: i64,
    pending_upload: i64,
    sealed_local_bytes: u64,
}

/// Waits until every sealed segment of `topic` is in the remote tier, local
/// retention has evicted all it can, and at least one tiered segment is still
/// on the leader's disk.
///
/// The eviction must have ended, not only begun. The copy pass uploads the
/// sealed segments faster than the retention passes that follow it drop them,
/// so a leader whose earliest pending upload is already the active segment can
/// still be evicting. A measurement taken then counted sealed bytes the new
/// replica never saw, and the case without the flag failed its check that the
/// replica pulled them all (#1831).
async fn await_settled_leader(
    admin: &Client,
    leader_dir: &Path,
    topic: &str,
    local_retention_bytes: u64,
) -> SettledLeader {
    let deadline = Instant::now() + Duration::from_mins(3);
    loop {
        let files = local_log_files(leader_dir);
        let pending_upload = earliest_pending_upload(admin, topic).await;
        // Uploaded up to the active segment, evicted from below as far as the
        // budget allows, and still holding a sealed segment that is already
        // tiered.
        if let [local_log_start, .., active] = segment_bases(&files)[..]
            && local_log_start > 0
            && pending_upload == active
            && local_retention_done(&files, local_retention_bytes)
        {
            return SettledLeader {
                local_log_start,
                pending_upload,
                sealed_local_bytes: sealed_bytes(&files),
            };
        }
        assert!(
            Instant::now() <= deadline,
            "{topic}: the leader never settled; files={files:?} pending={pending_upload}"
        );
        // intentional: the copy pass and local retention both run on the
        // broker's own 1s interval, and the object store is a directory.
        time::sleep(Duration::from_millis(300)).await;
    }
}

/// Sends one `IncrementalAlterConfigs` of a `BROKER` resource through
/// `client` and asserts it succeeded.
async fn alter_flag(client: &Client, resource_name: &str, operation: ConfigOperation) {
    let value = matches!(operation, ConfigOperation::Set).then_some("true");
    let response = client
        .send(broker_alter_request(
            resource_name.to_owned(),
            vec![(KEY, operation, value)],
            ConfigMutation::Apply,
        ))
        .await
        .expect("IncrementalAlterConfigs");
    assert!(
        response.responses[0].error_code == 0,
        "IncrementalAlterConfigs failed: {response:?}"
    );
}

/// Publishes `flag` for broker 3 and waits until broker 3 sees it.
async fn apply_flag(admin: &Client, new_replica_client: &Client, b3: &BrokerHandle, flag: Flag) {
    match flag {
        Flag::Unset => {}
        Flag::ClusterDefault => alter_flag(admin, "", ConfigOperation::Set).await,
        Flag::NewReplicaOnly => {
            alter_flag(admin, "", ConfigOperation::Delete).await;
            alter_flag(
                new_replica_client,
                &NEW_REPLICA.to_string(),
                ConfigOperation::Set,
            )
            .await;
        }
    }
    let node = krabka_metadata::NodeId(u64::from(NEW_REPLICA.unsigned_abs()));
    b3.wait_for_image(|img| {
        let cluster = img
            .default_broker_config()
            .and_then(|configs| configs.get(KEY))
            .map(String::as_str);
        let own = img
            .broker_config(node)
            .and_then(|configs| configs.get(KEY))
            .map(String::as_str);
        (cluster, own)
            == match flag {
                Flag::Unset => (None, None),
                Flag::ClusterDefault => (Some("true"), None),
                Flag::NewReplicaOnly => (None, Some("true")),
            }
    })
    .await;
}

/// Sends `AlterPartitionReassignments` adding broker 3 to `topic`-0.
async fn add_new_replica(controller: &Client, topic: &str) {
    let response = controller
        .send(AlterPartitionReassignmentsRequest {
            timeout_ms: 30_000,
            allow_replication_factor_change: true,
            topics: vec![ReassignableTopic {
                name: topic.into(),
                partitions: vec![ReassignablePartition {
                    partition_index: 0,
                    replicas: Some(vec![1, NEW_REPLICA]),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("AlterPartitionReassignments");
    assert!(
        response.error_code == 0 && response.responses[0].partitions[0].error_code == 0,
        "AlterPartitionReassignments failed: {response:?}"
    );
}

/// Waits until broker 3's log of `topic`-0 reaches `leader_end`.
async fn await_caught_up(b3: &BrokerHandle, topic: &str, leader_end: i64) {
    let deadline = Instant::now() + Duration::from_mins(3);
    while b3
        .local_log_end_offset(topic, 0)
        .is_none_or(|end| end < leader_end)
    {
        assert!(
            Instant::now() <= deadline,
            "{topic}: the new replica reached {:?} of the leader's {leader_end}",
            b3.local_log_end_offset(topic, 0),
        );
        // intentional: replication has no in-process completion signal here.
        time::sleep(Duration::from_millis(300)).await;
    }
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct TieredFollower {
    brokers: Option<[BrokerHandle; 3]>,
    log_dirs: Vec<TempDir>,
    remote_dir: Option<TempDir>,
    admin: Option<Client>,
    new_replica_client: Option<Client>,
    controller: Option<Client>,
    settled: BTreeMap<String, SettledLeader>,
    topic: String,
    new_replica_start: Option<i64>,
    replicated: Option<u64>,
}

#[fixture]
fn tiered() -> TieredFollower {
    // Start this scenario with fresh inputs and observations.

    TieredFollower {
        brokers: None,
        log_dirs: Vec::new(),
        remote_dir: None,
        admin: None,
        new_replica_client: None,
        controller: None,
        settled: BTreeMap::new(),
        topic: String::new(),
        new_replica_start: None,
        replicated: None,
    }
}

#[given("three tiered brokers with one-KiB segments and a thirty-second heartbeat timeout")]
async fn cluster(tiered: &mut TieredFollower) {
    let TieredCluster {
        brokers,
        log_dirs,
        remote_dir,
        ..
    } = start_tiered_cluster_with([krabka_units::kibibytes(1); 3], |_, config| {
        config.heartbeat_timeout = krabka_units::secs(30);
    })
    .await;
    let [b1, b2, b3] = &brokers;
    tiered.admin = Some(ready_admin([b1, b2, b3], "kip1023-admin").await);
    tiered.new_replica_client = Some(
        connect_owned(
            &format!("127.0.0.1:{}", b3.listen_addr().port()),
            "kip1023-new-replica",
            "new replica client",
        )
        .await,
    );
    let controller_addr = controller_leader_addr([b1, b2, b3]).await;
    tiered.controller =
        Some(connect_owned(&controller_addr, "kip1023-controller", "controller").await);
    tiered.brokers = Some(brokers);
    tiered.log_dirs = log_dirs;
    tiered.remote_dir = Some(remote_dir);
}

#[given(
    "topics {topics} on broker one each archive {records} records of {payload} bytes with local retention {retention} bytes"
)]
async fn archived_topics(
    tiered: &mut TieredFollower,
    topics: String,
    records: usize,
    payload: usize,
    retention: u64,
) {
    let leader = &tiered.brokers.as_ref().unwrap()[0];
    let admin = tiered.admin.as_ref().unwrap();
    let mut topic_admin = AdminClient::connect(&[leader.listen_addr().to_string()])
        .await
        .expect("topic admin client");
    for topic in topics.split(',') {
        create_tiered_topic(&mut topic_admin, leader, topic, retention).await;
        let topic_id = topic_id_for(admin, topic).await;
        produce_records()
            .client(admin)
            .value(|index| {
                let mut value = format!("record-{index}-").into_bytes();
                value.resize(payload, b'x');
                bytes::Bytes::from(value)
            })
            .topic(topic)
            .topic_id(topic_id)
            .count(records)
            .call()
            .await;
    }
    for topic in topics.split(',') {
        let leader_dir = tiered.log_dirs[0].path().join(format!("{topic}-0"));
        tiered.settled.insert(
            topic.to_owned(),
            await_settled_leader(admin, &leader_dir, topic, retention).await,
        );
    }
}

#[when("broker three joins topic {topic} with last-tiered-offset config scope {scope}")]
async fn join_replica(tiered: &mut TieredFollower, topic: String, scope: String) {
    let flag = match scope.as_str() {
        "unset" => Flag::Unset,
        "cluster-default" => Flag::ClusterDefault,
        "broker-three-only" => Flag::NewReplicaOnly,
        _ => panic!("unknown dynamic config scope {scope}"),
    };
    let [b1, _, b3] = tiered.brokers.as_ref().unwrap();
    apply_flag(
        tiered.admin.as_ref().unwrap(),
        tiered.new_replica_client.as_ref().unwrap(),
        b3,
        flag,
    )
    .await;
    add_new_replica(tiered.controller.as_ref().unwrap(), &topic).await;
    let leader_end = b1
        .local_log_end_offset(&topic, 0)
        .expect("leader holds partition");
    await_caught_up(b3, &topic, leader_end).await;
    let new_replica_dir = tiered.log_dirs[2].path().join(format!("{topic}-0"));
    let new_replica_files = local_log_files(&new_replica_dir);
    tiered.new_replica_start = Some(
        segment_bases(&new_replica_files)
            .first()
            .copied()
            .expect("new replica holds a segment"),
    );
    tiered.replicated = Some(
        b3.metrics()
            .replication_bytes_in
            .get_or_create(&PartitionLabel {
                topic: Arc::from(topic.as_str()),
                partition: 0,
            })
            .get(),
    );
    tiered.topic = topic;
}

#[then("the leader retains sealed tiered bytes below its pending upload offset")]
fn discriminatory_band(tiered: &TieredFollower) {
    let settled = &tiered.settled[&tiered.topic];
    assert!(settled.local_log_start < settled.pending_upload);
    assert!(settled.sealed_local_bytes > 0);
}

#[then("the new replica pulls at least the leader's sealed local byte count")]
fn includes_tiered(tiered: &TieredFollower) {
    let settled = &tiered.settled[&tiered.topic];
    let replicated = tiered.replicated.unwrap();
    check!(
        replicated >= settled.sealed_local_bytes,
        "unflagged follower pulled {replicated} bytes instead of at least {}",
        settled.sealed_local_bytes
    );
}

#[then(
    "the new replica starts at or above pending upload and pulls fewer bytes than the leader's sealed local byte count"
)]
fn skips_tiered(tiered: &TieredFollower) {
    let settled = &tiered.settled[&tiered.topic];
    let start = tiered.new_replica_start.unwrap();
    let replicated = tiered.replicated.unwrap();
    check!(
        start >= settled.pending_upload,
        "replica starts at {start}, below pending upload {}",
        settled.pending_upload
    );
    check!(
        replicated < settled.sealed_local_bytes,
        "replica pulled {replicated} bytes; sealed local bytes {}",
        settled.sealed_local_bytes
    );
}

#[scenario(
    path = "tests/features/migrated/tests/follower_fetch_last_tiered_offset.feature",
    name = "a new replica starts at the last tiered offset only with the flag"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_replica_starts_at_the_last_tiered_offset_only_with_the_flag(tiered: TieredFollower) {
    drop(tiered.admin);
    drop(tiered.new_replica_client);
    drop(tiered.controller);
    for broker in tiered.brokers.unwrap() {
        broker.shutdown().await;
    }
}
