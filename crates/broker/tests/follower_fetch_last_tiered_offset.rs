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
        time::{Duration, Instant},
    },
};

/// The dynamic broker config under test.
const KEY: &str = "follower.fetch.last.tiered.offset.enable";
/// Kafka's `ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP`.
const EARLIEST_PENDING_UPLOAD_TIMESTAMP: i64 = -6;
/// Records produced into each partition, one batch each, so the 1 KiB segments
/// roll many times.
const RECORDS: usize = 60;
/// The payload each record carries.
const PAYLOAD: usize = 256;
/// Local retention wide enough to keep a few sealed, already-tiered segments
/// on the leader's disk, and narrow enough to evict the oldest ones.
const LOCAL_RETENTION_BYTES: &str = "4096";
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

/// One partition, the flag setting broker 3 joins it under, and whether that
/// setting turns KIP-1023 on.
struct Case {
    topic: &'static str,
    flag: Flag,
    from_last_tiered: bool,
}

const CASES: [Case; 3] = [
    Case {
        topic: "kip1023-flag-unset",
        flag: Flag::Unset,
        from_last_tiered: false,
    },
    Case {
        topic: "kip1023-cluster-default",
        flag: Flag::ClusterDefault,
        from_last_tiered: true,
    },
    Case {
        topic: "kip1023-new-replica-only",
        flag: Flag::NewReplicaOnly,
        from_last_tiered: true,
    },
];

/// The base offsets of the `*.log` files in one replica's partition directory,
/// ascending.
fn segment_bases(partition_dir: &Path) -> Vec<i64> {
    local_log_files(partition_dir)
        .into_iter()
        .map(|(base, _)| base)
        .collect()
}

/// The total size of every `*.log` file but the active (last) one.
fn sealed_bytes(partition_dir: &Path) -> u64 {
    let files = local_log_files(partition_dir);
    files
        .split_last()
        .map_or(0, |(_, sealed)| sealed.iter().map(|(_, len)| len).sum())
}

/// Creates `topic` on broker 1 alone, tiered, with partial local retention.
async fn create_tiered_topic(admin: &mut AdminClient, leader: &BrokerHandle, topic: &str) {
    let response = admin
        .create_topics(
            &[CreateTopicSpec::builder()
                .name(topic)
                .configs(BTreeMap::from([
                    ("remote.storage.enable".into(), "true".into()),
                    ("local.retention.bytes".into(), LOCAL_RETENTION_BYTES.into()),
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
/// retention has evicted the oldest of them, and at least one tiered segment is
/// still on the leader's disk.
async fn await_settled_leader(admin: &Client, leader_dir: &Path, topic: &str) -> SettledLeader {
    let deadline = Instant::now() + Duration::from_mins(3);
    loop {
        let bases = segment_bases(leader_dir);
        let pending_upload = earliest_pending_upload(admin, topic).await;
        // Uploaded up to the active segment, evicted from below, and still
        // holding a sealed segment that is already tiered.
        if let [local_log_start, .., active] = bases[..]
            && local_log_start > 0
            && pending_upload == active
        {
            return SettledLeader {
                local_log_start,
                pending_upload,
                sealed_local_bytes: sealed_bytes(leader_dir),
            };
        }
        assert!(
            Instant::now() <= deadline,
            "{topic}: the leader never settled; segments={bases:?} pending={pending_upload}"
        );
        // intentional: the copy pass and local retention both run on the
        // broker's own 1s interval, and the object store is a directory.
        tokio::time::sleep(Duration::from_millis(300)).await;
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
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// A new replica of a tiered partition skips the leader's tiered local
/// segments exactly when `follower.fetch.last.tiered.offset.enable` is set for
/// it, at the cluster default or for that broker alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_replica_starts_at_the_last_tiered_offset_only_with_the_flag() {
    let TieredCluster {
        brokers: [b1, b2, b3],
        log_dirs,
        ..
    } = start_tiered_cluster_with([krabka_units::kibibytes(1); 3], |_, config| {
        // Every sole replica stays alive in this case. Tolerate delayed
        // heartbeats while parallel suites exercise disk I/O on the runner.
        config.heartbeat_timeout = krabka_units::secs(30);
    })
    .await;
    let admin = ready_admin([&b1, &b2, &b3], "kip1023-admin").await;
    let mut topic_admin = AdminClient::connect(&[b1.listen_addr().to_string()])
        .await
        .expect("topic admin client");
    let new_replica_client = connect_owned(
        &format!("127.0.0.1:{}", b3.listen_addr().port()),
        "kip1023-new-replica",
        "new replica client",
    )
    .await;

    // Every partition is archived before the first replica joins, so the
    // cases share one wait for the copy pass.
    for case in &CASES {
        create_tiered_topic(&mut topic_admin, &b1, case.topic).await;
        let topic_id = topic_id_for(&admin, case.topic).await;
        produce_records()
            .client(&admin)
            .value(|index| {
                let mut value = format!("record-{index}-").into_bytes();
                value.resize(PAYLOAD, b'x');
                bytes::Bytes::from(value)
            })
            .topic(case.topic)
            .topic_id(topic_id)
            .count(RECORDS)
            .call()
            .await;
    }

    let controller_addr = controller_leader_addr([&b1, &b2, &b3]).await;
    let controller = connect_owned(&controller_addr, "kip1023-controller", "controller").await;

    for case in &CASES {
        let leader_dir = log_dirs[0].path().join(format!("{}-0", case.topic));
        let new_replica_dir = log_dirs[2].path().join(format!("{}-0", case.topic));
        let settled = await_settled_leader(&admin, &leader_dir, case.topic).await;
        eprintln!("ITEST: {}: the leader settled at {settled:?}", case.topic);
        // The band the flag decides about must exist: tiered segments the
        // leader still holds locally.
        assert!(settled.local_log_start < settled.pending_upload);
        assert!(settled.sealed_local_bytes > 0);

        apply_flag(&admin, &new_replica_client, &b3, case.flag).await;
        add_new_replica(&controller, case.topic).await;
        let leader_end = b1
            .local_log_end_offset(case.topic, 0)
            .expect("the leader holds the partition");
        await_caught_up(&b3, case.topic, leader_end).await;

        let new_replica_start = segment_bases(&new_replica_dir)
            .first()
            .copied()
            .expect("the new replica holds a segment");
        let replicated = b3
            .metrics()
            .replication_bytes_in
            .get_or_create(&PartitionLabel {
                topic: std::sync::Arc::from(case.topic),
                partition: 0,
            })
            .get();
        eprintln!(
            "ITEST: {}: the new replica starts at {new_replica_start} and pulled {replicated} \
             bytes; its files are {:?}",
            case.topic,
            local_log_files(&new_replica_dir),
        );
        if case.from_last_tiered {
            check!(
                new_replica_start >= settled.pending_upload,
                "{}: the new replica starts at {new_replica_start}, below the leader's earliest \
                 pending upload {}",
                case.topic,
                settled.pending_upload,
            );
            check!(
                replicated < settled.sealed_local_bytes,
                "{}: the new replica pulled {replicated} bytes, the leader's tiered local \
                 segments among them ({} bytes)",
                case.topic,
                settled.sealed_local_bytes,
            );
        } else {
            check!(
                replicated >= settled.sealed_local_bytes,
                "{}: without the flag the new replica pulled only {replicated} bytes, not the \
                 leader's {} bytes of tiered local segments",
                case.topic,
                settled.sealed_local_bytes,
            );
        }
    }

    drop(admin);
    drop(topic_admin);
    drop(new_replica_client);
    drop(controller);
    b1.shutdown().await;
    b2.shutdown().await;
    b3.shutdown().await;
}
