//! Membership and record fixtures for share-group integration scenarios.
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use assert2::assert;
use krabka_broker::BrokerConfig;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        incremental_alter_configs_request::IncrementalAlterConfigsRequest,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AckAckBatch,
            ShareAcknowledgeRequest,
        },
        share_acknowledge_response::ShareAcknowledgeResponse,
        share_fetch_request::{
            AcknowledgementBatch as FetchAckBatch, FetchPartition, FetchTopic, ShareFetchRequest,
        },
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch},
};

use crate::support::{
    configs::{incremental_config, incremental_request, incremental_resource},
    records::{batch_from_records, value_record},
};
const SHARE_STATE_PARTITIONS: i32 = 1;
const MAX_CONCURRENT_TEST_BROKERS: usize = 3;
const RESOURCE_TYPE_GROUP: i8 = 32;
const CONFIG_OP_SET: i8 = 0;
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
    group: &str,
) {
    broker.wait_until_group_coordinator_ready().await;
    broker.wait_until_share_coordinator_ready().await;
    set_auto_offset_reset_earliest(client, group).await;
}

pub async fn set_auto_offset_reset_earliest(client: &Client, group: &str) {
    let resp = client
        .send(incremental_request(
            vec![incremental_resource(
                RESOURCE_TYPE_GROUP,
                group,
                vec![incremental_config(
                    "share.auto.offset.reset",
                    Some("earliest".into()),
                    CONFIG_OP_SET,
                )],
            )],
            IncrementalAlterConfigsRequest::default().validate_only,
        ))
        .await
        .expect("IncrementalAlterConfigs(group)");
    assert!(
        resp.responses[0].error_code == 0,
        "share.auto.offset.reset=earliest rejected: {:?}",
        resp.responses[0].error_message
    );
}

pub async fn produce_n(client: &Client, topic: &str, tid: uuid::Uuid, partition: i32, n: i64) {
    let values = (0..n)
        .map(|i| bytes::Bytes::from(format!("v{i}")))
        .collect();
    produce_values(client, topic, tid, partition, values).await;
}

pub async fn produce_values(
    client: &Client,
    topic: &str,
    tid: uuid::Uuid,
    partition: i32,
    values: Vec<bytes::Bytes>,
) {
    let n = i64::try_from(values.len()).unwrap();
    let records: Vec<Record> = values
        .iter()
        .enumerate()
        .map(|(i, value)| value_record(i32::try_from(i).unwrap(), Some(value.clone())))
        .collect();
    produce_batch(
        client,
        topic,
        tid,
        partition,
        RecordBatch {
            last_offset_delta: i32::try_from(n - 1).unwrap(),
            ..batch_from_records(records)
        },
    )
    .await;
}
pub async fn join(client: &Client, group: &str, topic: &str) -> (String, i32) {
    let resp = client
        .send(ShareGroupHeartbeatRequest {
            group_id: group.into(),
            member_id: uuid::Uuid::new_v4().to_string(),
            member_epoch: 0,
            subscribed_topic_names: Some(vec![topic.into()]),
            ..Default::default()
        })
        .await
        .expect("ShareGroupHeartbeat");
    assert!(resp.error_code == 0, "join failed: {:?}", resp.error_code);
    let member_id = resp.member_id.expect("the broker echoes the member id");
    let member_epoch = resp.member_epoch;
    (member_id, member_epoch)
}

/// Produce a supplied batch, preserving its timestamps and record geometry on retries.
pub async fn produce_batch(
    client: &Client,
    topic: &str,
    tid: uuid::Uuid,
    partition: i32,
    batch: RecordBatch,
) {
    for _ in 0..40 {
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
                        records: Some(batch.clone().into()),
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

/// A broker, client and materialized data topic, retaining the fixture directory.
pub async fn topic_fixture(
    topic: &str,
    partitions: i32,
    customize: impl FnOnce(&mut BrokerConfig),
) -> (
    krabka_broker::BrokerHandle,
    Arc<Client>,
    tempfile::TempDir,
    uuid::Uuid,
) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = broker_config(dir.path().to_path_buf());
    customize(&mut config);
    let (broker, client, tid) = start_topic(config, topic, partitions).await;
    (broker, client, dir, tid)
}

/// A topic fixture that holds this binary's broker permit for the test's lifetime.
pub async fn permitted_topic_fixture(
    topic: &str,
    partitions: i32,
    customize: impl FnOnce(&mut BrokerConfig),
) -> (
    tokio::sync::OwnedSemaphorePermit,
    krabka_broker::BrokerHandle,
    Arc<Client>,
    tempfile::TempDir,
    uuid::Uuid,
) {
    let permit = broker_test_permit().await;
    let (broker, client, dir, tid) = topic_fixture(topic, partitions, customize).await;
    (permit, broker, client, dir, tid)
}

pub async fn connect(bootstrap: &str) -> Arc<Client> {
    crate::support::client::connect(bootstrap, "c1").await
}
pub async fn create_topic(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    topic: &str,
    partitions: i32,
) {
    crate::support::client::create_topic(client, topic, partitions).await;
    broker.wait_until_partition_present(topic, 0).await;
}

pub fn share_fetch_req(
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
        max_bytes: 1 << 20,
        max_records: 500,
        batch_size: 500,
        share_acquire_mode: 0,
        is_renew_ack: false,
        topics: vec![FetchTopic {
            topic_id: wire(tid),
            partitions: vec![FetchPartition {
                partition_index: partition,
                partition_max_bytes: 1 << 20,
                acknowledgement_batches: acks,
                ..Default::default()
            }],
            ..Default::default()
        }],
        forgotten_topics_data: vec![],
        ..Default::default()
    }
}
pub fn acquired_count(p: &krabka_protocol::owned::share_fetch_response::PartitionData) -> i64 {
    p.acquired_records
        .iter()
        .map(|r| r.last_offset - r.first_offset + 1)
        .sum()
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
    let req = acknowledge_request(
        Some(ack.group.into()),
        Some(ack.member.into()),
        ack.epoch,
        Some(false),
        vec![acknowledge_topic(
            wire(ack.topic_id),
            vec![acknowledge_partition(
                ack.partition,
                vec![acknowledgement(
                    ack.first,
                    ack.last,
                    vec![ack.ack_type; count],
                )],
            )],
        )],
    );
    let resp: ShareAcknowledgeResponse = client.send(req).await.expect("ShareAcknowledge");
    assert!(
        resp.error_code == 0,
        "ShareAcknowledge top-level error: {}",
        resp.error_code
    );
    resp.responses[0].partitions[0].clone()
}

/// Fetch one row, allowing omitted empty partitions only for incremental sessions.
pub async fn fetch_row(
    client: &Client,
    req: ShareFetchRequest,
    incremental: bool,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    let partition = req.topics[0].partitions[0].partition_index;
    let resp: krabka_protocol::owned::share_fetch_response::ShareFetchResponse =
        client.send(req).await.expect("ShareFetch");
    assert!(
        resp.error_code == 0,
        "ShareFetch top-level error: {}",
        resp.error_code
    );
    if incremental {
        resp.responses
            .first()
            .and_then(|t| t.partitions.first())
            .cloned()
            .unwrap_or_else(
                || krabka_protocol::owned::share_fetch_response::PartitionData {
                    partition_index: partition,
                    ..Default::default()
                },
            )
    } else {
        resp.responses[0].partitions[0].clone()
    }
}

/// The fetch itself acquires records, so poll the same session-opening RPC.
pub async fn fetch_until_acquired(
    client: &Client,
    req: ShareFetchRequest,
    incremental: bool,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    for _ in 0..40 {
        let row = fetch_row(client, req.clone(), incremental).await;
        if row.error_code == 0 && acquired_count(&row) > 0 {
            return row;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("share fetch never acquired any records: {req:?}");
}

/// Start a data topic on a caller-owned broker config (including restart fixtures).
pub async fn start_topic(
    cfg: BrokerConfig,
    topic: &str,
    partitions: i32,
) -> (krabka_broker::BrokerHandle, Arc<Client>, uuid::Uuid) {
    // Keep the growing broker config inside a boxed startup future.
    let broker = Box::pin(krabka_broker::Broker::start(cfg)).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, topic, partitions).await;
    let tid = topic_id(&broker, topic);
    (broker, client, tid)
}

pub async fn crash_follower(
    cluster: &mut Vec<(krabka_broker::BrokerHandle, BrokerConfig, tempfile::TempDir)>,
    coordinator: u64,
) -> tempfile::TempDir {
    let raft_leader = cluster[0].0.wait_until_controller_leader().await.0;
    let follower = cluster
        .iter()
        .position(|(h, _, _)| h.node_id() != coordinator && h.node_id() != raft_leader)
        .unwrap_or_else(|| {
            cluster
                .iter()
                .position(|(h, _, _)| h.node_id() != coordinator)
                .expect("a follower")
        });
    let (stopped, _, dir) = cluster.remove(follower);
    stopped.crash_for_test().await;
    dir
}

/// Rejoin a persisted share fixture and wait for its group/coordinator bootstrap.
///
/// # Panics
/// Panics if the broker or client cannot restart from the supplied directory.
pub async fn rejoin_group(
    log_dir: std::path::PathBuf,
    group: &str,
) -> (krabka_broker::BrokerHandle, Arc<Client>) {
    let mut cfg = broker_config(log_dir);
    cfg.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let broker = krabka_broker::Broker::start(cfg).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    bootstrap_share_state(&broker, &client, group).await;
    (broker, client)
}

/// The URL-safe, unpadded coordinator key for a share partition.
pub fn coordinator_key(group: &str, tid: uuid::Uuid, partition: i32) -> String {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    format!(
        "{group}:{}:{partition}",
        URL_SAFE_NO_PAD.encode(tid.as_bytes())
    )
}

/// Present record values in batch order, preserving the share fixtures' lossy UTF-8 decoding.
pub fn record_values(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| batch.records.iter())
        .filter_map(|record| record.value.as_ref())
        .map(|value| String::from_utf8_lossy(value).into_owned())
        .collect()
}

/// Retry empty incremental acquisitions using the case's explicit session-epoch bounds.
/// The initial row is supplied by the caller; every retry retains its original 100ms spacing.
///
/// # Panics
/// Panics if the fetch fails or answers a nonzero top-level code.
pub async fn refetch_while_empty(
    client: &Client,
    (group, member, tid, partition): (&str, &str, uuid::Uuid, i32),
    mut row: krabka_protocol::owned::share_fetch_response::PartitionData,
    epochs: std::ops::Range<i32>,
) -> krabka_protocol::owned::share_fetch_response::PartitionData {
    for epoch in epochs {
        if acquired_count(&row) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        row = fetch_row(
            client,
            share_fetch_req(group, member, tid, partition, epoch, 0, vec![]),
            true,
        )
        .await;
    }
    row
}

/// An empty incremental session request with explicit forgotten topics and byte budget.
pub fn empty_session_request(
    group: String,
    member: String,
    epoch: i32,
    max_bytes: i32,
    forgotten_topics_data: Vec<krabka_protocol::owned::share_fetch_request::ForgottenTopic>,
) -> ShareFetchRequest {
    ShareFetchRequest {
        group_id: Some(group),
        member_id: Some(member),
        share_session_epoch: epoch,
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes,
        max_records: 500,
        batch_size: 500,
        topics: vec![],
        forgotten_topics_data,
        ..Default::default()
    }
}

/// Generate a typed first-acquisition driver with explicit incremental-session behavior.
#[macro_export]
macro_rules! share_first_fetch_fixture {
    ($name:ident, $incremental:literal) => {
        pub async fn $name(
            client: &::krabka_client_core::Client,
            group: &str,
            member: &str,
            tid: ::uuid::Uuid,
            partition: i32,
            epoch: i32,
        ) -> ::krabka_protocol::owned::share_fetch_response::PartitionData {
            $crate::support::share::fetch_until_acquired(
                client,
                $crate::support::share::share_fetch_req(
                    group,
                    member,
                    tid,
                    partition,
                    epoch,
                    0,
                    vec![],
                ),
                $incremental,
            )
            .await
        }
    };
}

pub fn acknowledgement(
    first_offset: i64,
    last_offset: i64,
    acknowledge_types: Vec<i8>,
) -> AckAckBatch {
    AckAckBatch {
        first_offset,
        last_offset,
        acknowledge_types,
        ..Default::default()
    }
}

pub fn acknowledge_partition(
    partition_index: i32,
    acknowledgement_batches: Vec<AckAckBatch>,
) -> AcknowledgePartition {
    AcknowledgePartition {
        partition_index,
        acknowledgement_batches,
        ..Default::default()
    }
}

pub fn acknowledge_topic(
    topic_id: WireUuid,
    partitions: Vec<AcknowledgePartition>,
) -> AcknowledgeTopic {
    AcknowledgeTopic {
        topic_id,
        partitions,
        ..Default::default()
    }
}

pub fn acknowledge_request(
    group_id: Option<String>,
    member_id: Option<String>,
    share_session_epoch: i32,
    is_renew_ack: Option<bool>,
    topics: Vec<AcknowledgeTopic>,
) -> ShareAcknowledgeRequest {
    let mut request = ShareAcknowledgeRequest {
        group_id,
        member_id,
        share_session_epoch,
        topics,
        ..Default::default()
    };
    if let Some(renew) = is_renew_ack {
        request.is_renew_ack = renew;
    }
    request
}

/// Drive the g1/t consume fixture lifecycle until its persisted share state is ready.
///
/// # Panics
/// Panics if repeated member heartbeats do not initialize the state within 30 seconds.
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
                .send(krabka_protocol::owned::share_group_heartbeat_request::ShareGroupHeartbeatRequest {
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

/// Join the consume fixture's g1/t member and finish its heartbeat-driven initialization.
pub async fn join_consume_member(
    broker: &krabka_broker::BrokerHandle,
    client: &Client,
    tid: uuid::Uuid,
) -> (String, i32) {
    let (member, epoch) = join(client, "g1", "t").await;
    wait_for_share_init(broker, client, &member, epoch, tid).await;
    (member, epoch)
}

/// Generate the common g1/t setup while retaining each suite's join and readiness policy.
#[macro_export]
macro_rules! share_consumption_fixture {
    ($(#[$attributes:meta])* $name:ident, $join:path,
        |$broker:ident, $client:ident, $member:ident, $epoch:ident, $tid:ident| $wait:expr
    ) => {
        $(#[$attributes])*
        pub async fn $name(
            $broker: &::krabka_broker::BrokerHandle,
            $client: &::krabka_client_core::Client,
            $tid: ::uuid::Uuid,
            records: i64,
        ) -> (String, i32) {
            $crate::support::share::bootstrap_share_state($broker, $client, "g1").await;
            $crate::support::share::produce_n($client, "t", $tid, 0, records).await;
            let ($member, $epoch) = ($join)($client, "g1", "t").await;
            ($wait).await;
            ($member, $epoch)
        }
    };
}
