//! Shared cluster and share-consume helpers for the KIP-932 admin offset suite.
//!
//! The helpers start a single-broker test cluster, create a topic, produce
//! records, drive `ShareGroupHeartbeat` membership, and run `ShareFetch` and
//! `ShareAcknowledge`. They are a copy of the harness in
//! `tests/share_consume.rs`, because each integration test is its own
//! compilation unit and carries its own helper copies. The error-code and
//! acknowledgement-type constants live here as well, since every admin surface
//! in this suite asserts on them.

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
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AckAckBatch,
            ShareAcknowledgeRequest,
        },
        share_acknowledge_response::ShareAcknowledgeResponse,
        share_fetch_request::{
            AcknowledgementBatch as FetchAckBatch, FetchPartition, FetchTopic, ShareFetchRequest,
        },
        share_fetch_response::ShareFetchResponse,
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

pub const NONE: i16 = 0;
pub const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const UNSUPPORTED_VERSION: i16 = 35;
pub const NON_EMPTY_GROUP: i16 = 68;

pub const ACCEPT: i8 = 1;
pub const ONE_MB: i32 = 1 << 20;

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

pub fn topic_id(broker: &krabka_broker::BrokerHandle, topic: &str) -> uuid::Uuid {
    let image = broker.controller_image_for_test();
    image
        .topic(topic)
        .map(|t| *t.topic_id.as_bytes())
        .map(uuid::Uuid::from_bytes)
        .expect("topic present in image")
}

fn wire(tid: uuid::Uuid) -> WireUuid {
    WireUuid(*tid.as_bytes())
}

const SHARE_STATE_TOPIC: &str = "__share_group_state";
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

pub async fn wait_for_share_init(
    broker: &krabka_broker::BrokerHandle,
    group: &str,
    tid: uuid::Uuid,
    partition: i32,
) {
    // Delegates to the broker-handle awaiter (30s timeout, 25ms poll interval).
    // `join()` drives the steady-state heartbeats that trigger the lifecycle hook
    // before this is called, so no repeated heartbeats are needed here.
    broker
        .wait_for_share_state_summary(group, tid, partition)
        .await;
}

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
        if p.error_code == 0 {
            return;
        }
        if p.error_code == 3 || p.error_code == 6 {
            // intentional: bounded produce-RPC retry on UNKNOWN_TOPIC_OR_PARTITION /
            // NOT_LEADER_OR_FOLLOWER while the partition's local writer materializes;
            // this helper has no BrokerHandle to await on and returns via the RPC response.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed: {p:?}");
    }
    panic!("partition never became produceable for {topic}:{partition}");
}

/// Joins `group` with a subscription to `topic`.
///
/// The function drives steady-state heartbeats, so the lifecycle hook
/// initializes the share state of the subscribed partitions. It returns
/// `(member_id, member_epoch)`, so the caller can leave with the live epoch.
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
    let mut epoch = resp.member_epoch;

    for _ in 0..3 {
        let hb = client
            .send(ShareGroupHeartbeatRequest {
                group_id: group.into(),
                member_id: member_id.clone(),
                member_epoch: epoch,
                subscribed_topic_names: Some(vec![topic.into()]),
                ..Default::default()
            })
            .await
            .expect("ShareGroupHeartbeat steady-state");
        epoch = hb.member_epoch;
        // intentional: paces steady-state heartbeats to drive the membership
        // reconciliation / lifecycle hook forward; this drives the protocol rather
        // than waiting on a single observable state (share init is awaited separately).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    (member_id, epoch)
}

/// Leaves the group with `member_epoch == -1`.
///
/// The broker keeps the group but reports zero members, in state "Empty".
pub async fn leave(client: &Client, group: &str, member_id: &str) {
    let resp = client
        .send(ShareGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: member_id.into(),
            member_epoch: -1,
            ..Default::default()
        })
        .await
        .expect("ShareGroupHeartbeat leave");
    assert!(resp.error_code == 0, "leave failed: {:?}", resp.error_code);
}

fn share_fetch_req(
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
    max_wait_ms: i32,
    acks: Vec<FetchAckBatch>,
) -> ShareFetchRequest {
    ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        max_wait_ms,
        min_bytes: 1,
        max_bytes: ONE_MB,
        max_records: 500,
        batch_size: 500,
        share_acquire_mode: 0,
        is_renew_ack: false,
        topics: vec![FetchTopic {
            topic_id: wire(tid),
            partitions: vec![FetchPartition {
                partition_index: partition,
                partition_max_bytes: ONE_MB,
                acknowledgement_batches: acks,
                ..Default::default()
            }],
            ..Default::default()
        }],
        forgotten_topics_data: vec![],
        ..Default::default()
    }
}

async fn share_fetch(
    client: &Client,
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
    max_wait_ms: i32,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    let req = share_fetch_req(group, member, tid, partition, epoch, max_wait_ms, vec![]);
    let resp: ShareFetchResponse = client.send(req).await.expect("ShareFetch");
    assert!(
        resp.error_code == NONE,
        "ShareFetch top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

pub struct ShareAck<'a> {
    pub group: &'a str,
    pub member: &'a str,
    pub topic_id: uuid::Uuid,
    pub partition: i32,
    pub epoch: i32,
    pub first: i64,
    pub last: i64,
    pub ack_type: i8,
}

pub async fn share_ack(
    client: &Client,
    ack: ShareAck<'_>,
) -> krabka_protocol::owned::share_acknowledge_response::PartitionData {
    let count = usize::try_from(ack.last - ack.first + 1).unwrap();
    let req = ShareAcknowledgeRequest {
        group_id: Some(ack.group.into()),
        member_id: Some(ack.member.into()),
        share_session_epoch: ack.epoch,
        is_renew_ack: false,
        topics: vec![AcknowledgeTopic {
            topic_id: wire(ack.topic_id),
            partitions: vec![AcknowledgePartition {
                partition_index: ack.partition,
                acknowledgement_batches: vec![AckAckBatch {
                    first_offset: ack.first,
                    last_offset: ack.last,
                    acknowledge_types: vec![ack.ack_type; count],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp: ShareAcknowledgeResponse = client.send(req).await.expect("ShareAcknowledge");
    assert!(
        resp.error_code == NONE,
        "ShareAcknowledge top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

pub fn acquired_count(p: &krabka_protocol::owned::share_fetch_response::PartitionData) -> i64 {
    p.acquired_records
        .iter()
        .map(|r| r.last_offset - r.first_offset + 1)
        .sum()
}

pub async fn fetch_until_acquired(
    client: &Client,
    group: &str,
    member: &str,
    tid: uuid::Uuid,
    partition: i32,
    epoch: i32,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    for _ in 0..40 {
        let row = share_fetch(client, group, member, tid, partition, epoch, 0).await;
        if row.error_code == NONE && acquired_count(&row) > 0 {
            return row;
        }
        // intentional: bounded ShareFetch-RPC poll — the fetch IS the acquiring action
        // and its response row is returned for assertions, so an awaiter can't replace it.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("share fetch never acquired any records for {group}:{tid}:{partition}");
}
