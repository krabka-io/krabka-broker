//! Cluster, topic, and membership plumbing that every KIP-932 consume test in
//! this binary shares. It starts an in-process broker whose share-coordinator
//! state topic has a single partition, creates a data topic and resolves its
//! id, produces records into it, joins a share group, and waits until the
//! group lifecycle has durably initialized the share state a consume needs.

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use assert2::assert;
use krabka_broker::BrokerConfig;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        find_coordinator_request::FindCoordinatorRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

pub async fn connect(bootstrap: &str) -> Arc<Client> {
    Arc::new(
        Client::builder()
            .bootstrap(bootstrap)
            .client_id("c1")
            .build()
            .await
            .unwrap(),
    )
}

/// Create `topic` with `partitions` partitions and wait until this broker has
/// materialized (and leads) partition 0, so a subsequent produce won't race the
/// replicator supervisor.
pub async fn create_topic(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    topic: &str,
    partitions: i32,
) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.into(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(
        resp.topics[0].error_code == 0,
        "topic create failed: {resp:?}"
    );
    broker.wait_until_partition_present(topic, 0).await;
}

/// Resolve a created topic's id from this broker's metadata image.
pub fn topic_id(broker: &krabka_broker::BrokerHandle, topic: &str) -> uuid::Uuid {
    let image = broker.controller_image_for_test();
    image
        .topic(topic)
        .map(|t| *t.topic_id.as_bytes())
        .map(uuid::Uuid::from_bytes)
        .expect("topic present in image")
}

pub fn wire(tid: uuid::Uuid) -> WireUuid {
    WireUuid(*tid.as_bytes())
}

/// Bootstrap `__share_group_state` and wait until this broker has materialized
/// the state partition that owns `key`. `FindCoordinator(SHARE)` creates the
/// topic lazily, exactly as a KIP-932 client does. Until this broker leads that
/// partition, the share-partition manager's persist would route to a
/// not-yet-present leader, and the SPSO advance would only live in memory. A
/// restart would then lose it. This is the share-state analogue of waiting for
/// the data partition.
const SHARE_STATE_TOPIC: &str = "__share_group_state";
// These single-broker tests only need one state partition. Keeping the test
// geometry small also prevents the parallel test runner from exhausting its
// process-wide file-descriptor limit while eleven brokers run concurrently.
const SHARE_STATE_PARTITIONS: i32 = 1;
const MAX_CONCURRENT_TEST_BROKERS: usize = 3;

pub async fn broker_test_permit() -> tokio::sync::OwnedSemaphorePermit {
    static GATE: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

    Arc::clone(
        GATE.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_TEST_BROKERS))),
    )
    .acquire_owned()
    .await
    .expect("broker test concurrency gate remains open")
}

pub fn broker_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut config = BrokerConfig::for_tests(log_dir);
    config.share_coordinator.state_topic_num_partitions = SHARE_STATE_PARTITIONS;
    config
}

pub async fn bootstrap_share_state(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    key: &str,
) {
    let resp = client
        .send(FindCoordinatorRequest {
            key_type: 2, // SHARE
            coordinator_keys: vec![key.to_string()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator(SHARE)");
    assert!(
        resp.coordinators[0].error_code == 0,
        "FindCoordinator(SHARE) error: {}",
        resp.coordinators[0].error_code
    );
    // Wait until every state partition this single broker should lead is local,
    // so the share-state writes land durably.
    for p in 0..SHARE_STATE_PARTITIONS {
        broker
            .wait_until_partition_present(SHARE_STATE_TOPIC, p)
            .await;
    }
}

/// Wait until the group-coordinator lifecycle hook has durably initialized the
/// share state for `(group, topic, partition)`. The persister summary then
/// becomes present. Until that happens the share coordinator is not yet
/// write-ready, and a consume's SPSO advance would not persist.
///
/// The lifecycle hook fires on each heartbeat, so this helper drives
/// steady-state heartbeats inside the wait loop rather than sleeping. It mirrors
/// the `lifecycle_initializes_share_state` pattern in `share_groups.rs`.
pub async fn wait_for_share_init(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    member_id: &str,
    member_epoch: i32,
    tid: uuid::Uuid,
) {
    let res = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            // Send a steady-state heartbeat to trigger the lifecycle hook.
            let _ = client
                .send(ShareGroupHeartbeatRequest {
                    group_id: "g1".into(),
                    member_id: member_id.into(),
                    member_epoch,
                    subscribed_topic_names: Some(vec!["t".into()]),
                    ..Default::default()
                })
                .await;
            if broker
                .share_state_summary_for_test("g1", tid, 0)
                .await
                .is_some()
            {
                return;
            }
        }
    })
    .await;
    assert!(
        res.is_ok(),
        "share state for g1:{tid}:0 never initialized within 30s"
    );
}

/// Produce `n` records into `(topic, partition)` in a single batch. Each record
/// carries a tiny distinct value so the bytes are non-empty.
///
/// This helper retries while the freshly-created partition is still
/// materializing its leader (`UNKNOWN_TOPIC_OR_PARTITION` /
/// `NOT_LEADER_OR_FOLLOWER`), exactly as a real producer would.
pub async fn produce_n(client: &Client, topic: &str, tid: uuid::Uuid, partition: i32, n: i64) {
    for _ in 0..40 {
        let records: Vec<Record> = (0..n)
            .map(|i| Record {
                offset_delta: i32::try_from(i).unwrap(),
                value: Some(bytes::Bytes::copy_from_slice(format!("v{i}").as_bytes())),
                ..Default::default()
            })
            .collect();
        let resp = client
            .send(ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.to_string(),
                    // Produce negotiates v13, which carries topic_id (not name)
                    // on the wire; the broker resolves the topic by id.
                    topic_id: wire(tid),
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(
                            RecordBatch {
                                last_offset_delta: i32::try_from(n - 1).unwrap(),
                                records,
                                ..Default::default()
                            }
                            .into(),
                        ),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        let p = &resp.responses[0].partition_responses[0];
        // 3 = UNKNOWN_TOPIC_OR_PARTITION, 6 = NOT_LEADER_OR_FOLLOWER.
        if p.error_code == 0 {
            return;
        }
        if p.error_code == 3 || p.error_code == 6 {
            // intentional: bounded produce-retry backoff while the partition
            // leader materializes; this helper has no BrokerHandle to await on
            // and mirrors a real producer's retry.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed: {p:?}");
    }
    panic!("partition never became produceable for {topic}:{partition}");
}

/// Join `group` as a fresh member subscribed to `topic` so the share actor
/// knows the member. The `ShareFetch` membership check needs this. Returns
/// `(member_id, member_epoch)` so the caller can drive heartbeats inside the
/// `wait_for_share_init` lifecycle loop.
pub async fn join(client: &Client, group: &str, topic: &str) -> (String, i32) {
    let resp = client
        .send(ShareGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![topic.into()]),
            ..Default::default()
        })
        .await
        .expect("ShareGroupHeartbeat");
    assert!(resp.error_code == 0, "join failed: {:?}", resp.error_code);
    let member_id = resp.member_id.expect("broker must mint a member id");
    let member_epoch = resp.member_epoch;
    (member_id, member_epoch)
}
