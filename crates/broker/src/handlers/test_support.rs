//! Shared live-broker and local-partition fixtures for handler tests.

use std::{path::Path, sync::Arc};

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::MetadataRecord;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};

use crate::{broker::BrokerHandle, codes};

pub(crate) fn acl(
    resource_type: krabka_metadata::ResourceType,
    resource_name: &str,
    pattern_type: krabka_metadata::PatternType,
    operation: krabka_metadata::AclOperation,
) -> krabka_metadata::AclEntry {
    krabka_metadata::AclEntry {
        resource_type,
        resource_name: resource_name.into(),
        pattern_type,
        principal: "User:alice".into(),
        host: "*".into(),
        operation,
        permission_type: krabka_metadata::PermissionType::Allow,
    }
}

pub(crate) async fn start_broker() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|_| {}).await
}

pub(crate) async fn start_broker_with(
    configure: impl FnOnce(&mut crate::config::BrokerConfig),
) -> (BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(configure).await
}

pub(crate) async fn produce_wire(
    broker: &BrokerHandle,
    version: i16,
    request: &ProduceRequest,
) -> ProduceResponse {
    let shared = broker.broker_arc_for_test();
    let user = crate::test_support::principal("producer");
    let peer = crate::test_support::peer();
    let context = crate::test_support::request_context(&user, &peer, "producer-client");
    let request_bytes = crate::test_support::encode_request(request, version);
    let response_bytes = crate::handlers::produce::handle(
        &shared,
        version,
        &request_bytes,
        request_bytes.clone(),
        &context,
    )
    .await
    .expect("handle produce");
    crate::test_support::decode_response(&response_bytes, version)
}

pub(crate) async fn share_acknowledge_wire(
    broker: &BrokerHandle,
    version: i16,
    request: &krabka_protocol::owned::share_acknowledge_request::ShareAcknowledgeRequest,
) -> krabka_protocol::owned::share_acknowledge_response::ShareAcknowledgeResponse {
    let user = crate::test_support::principal("share-consumer");
    let peer = crate::test_support::peer();
    let context = crate::test_support::request_context(&user, &peer, "share-client");
    crate::test_support::dispatch_wire(
        &broker.broker_arc_for_test(),
        krabka_protocol::owned::share_acknowledge_request::API_KEY,
        version,
        request,
        &context,
    )
    .await
}

/// Compare each table case's complete actual and expected outcome.
pub(crate) async fn check_cases<C, T: std::fmt::Debug + PartialEq>(
    cases: impl IntoIterator<Item = C>,
    mut drive: impl AsyncFnMut(C) -> (T, T),
) {
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases {
        let (got, want) = drive(case).await;
        actual.push(got);
        expected.push(want);
    }
    assert!(actual == expected);
}

/// Publish the two metadata records of a two-replica topic separately, as
/// the replication and epoch handler fixtures originally seeded them.
pub(crate) async fn seed_replicated_topic(
    broker: &BrokerHandle,
    topic: &str,
    topic_id: u128,
    leader: u64,
) {
    use krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1Topic(TopicRecord {
            name: topic.to_owned(),
            topic_id: uuid::Uuid::from_u128(topic_id),
            partitions: 1,
            replication_factor: 2,
        }))
        .await
        .expect("submit topic record");
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.to_owned(),
            partition: 0,
            leader: krabka_audit::NodeId(leader),
            replicas: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            isr: vec![krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: vec![uuid::Uuid::nil(); 2],
            partition_epoch: 0,
        }))
        .await
        .expect("submit partition record");
}

pub(crate) fn acknowledge_request(
    group: &str,
    member: &str,
    epoch: i32,
    topic_id: WireUuid,
    (first_offset, last_offset): (i64, i64),
    acknowledge_type: i8,
) -> krabka_protocol::owned::share_acknowledge_request::ShareAcknowledgeRequest {
    use krabka_protocol::owned::share_acknowledge_request::{
        AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch, ShareAcknowledgeRequest,
    };
    ShareAcknowledgeRequest {
        group_id: Some(group.into()),
        member_id: Some(member.into()),
        share_session_epoch: epoch,
        topics: vec![AcknowledgeTopic {
            topic_id,
            partitions: vec![AcknowledgePartition {
                partition_index: 0,
                acknowledgement_batches: vec![AcknowledgementBatch {
                    first_offset,
                    last_offset,
                    acknowledge_types: vec![acknowledge_type],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(crate) async fn share_fetch_wire(
    broker: &BrokerHandle,
    version: i16,
    request: &krabka_protocol::owned::share_fetch_request::ShareFetchRequest,
) -> krabka_protocol::owned::share_fetch_response::ShareFetchResponse {
    let user = crate::test_support::principal("share-consumer");
    let peer = crate::test_support::peer();
    let context = crate::test_support::request_context(&user, &peer, "share-client");
    crate::test_support::dispatch_wire(
        &broker.broker_arc_for_test(),
        krabka_protocol::owned::share_fetch_request::API_KEY,
        version,
        request,
        &context,
    )
    .await
}

pub(crate) async fn create_topic(
    broker: &BrokerHandle,
    client_id: &str,
    name: &str,
    partitions: i32,
) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id(client_id)
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions: partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    for partition in 0..partitions {
        broker.wait_until_partition_present(name, partition).await;
    }
    WireUuid(
        broker
            .controller_image_for_test()
            .topic(name)
            .expect("created topic")
            .topic_id
            .into_bytes(),
    )
}

/// Appends one v2 batch of `count` records through the v12 Produce handler.
pub(crate) async fn produce_records(
    broker: &BrokerHandle,
    topic: &str,
    partition_index: i32,
    count: i32,
) {
    let request = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition_index,
                records: Some(RecordsPayload::V2(vec![record_batch(count)])),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let response = produce_wire(broker, 12, &request).await;
    assert!(
        response.responses[0].partition_responses[0].error_code == codes::NONE,
        "{response:?}"
    );
}

pub(crate) fn record_batch(count: i32) -> RecordBatch {
    RecordBatch {
        last_offset_delta: count - 1,
        records: (0..count)
            .map(|offset_delta| Record {
                offset_delta,
                value: Some(Bytes::from_static(b"v")),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub(crate) fn local_partition(
    broker: &crate::broker::Broker,
    root: &Path,
    topic: &str,
) -> Arc<crate::partition::Partition> {
    partition(broker, root, topic, false)
}

pub(crate) fn partition(
    broker: &crate::broker::Broker,
    root: &Path,
    topic: &str,
    diskless: bool,
) -> Arc<crate::partition::Partition> {
    spawn_partition(
        root,
        topic,
        0,
        (
            broker.log_dir_status.clone(),
            Arc::clone(&broker.producer_state),
        ),
        diskless,
        krabka_log::LogConfig::default(),
    )
}

pub(crate) fn spawn_partition(
    root: &Path,
    topic: &str,
    index: i32,
    (log_dir_status, producer_state): (
        crate::log_dir_status::LogDirRegistry,
        Arc<crate::producer_state::ProducerState>,
    ),
    diskless: bool,
    log_config: krabka_log::LogConfig,
) -> Arc<crate::partition::Partition> {
    let partition_dir = crate::log_dir::partition_dir(root, topic, index);
    std::fs::create_dir_all(&partition_dir).expect("partition directory");
    crate::broker::spawn_partition(
        topic.to_string(),
        krabka_ids::PartitionIndex(index),
        root.to_path_buf(),
        krabka_log::Log::open(&partition_dir, log_config).expect("open partition log"),
        log_dir_status,
        producer_state,
        diskless,
    )
}

pub(crate) async fn seed_controller_quota(handle: &BrokerHandle, rate: f64) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1ClientQuota(
            krabka_metadata::ClientQuotaRecord {
                entity: vec![
                    krabka_metadata::QuotaEntity {
                        entity_type: "user".into(),
                        entity_name: Some("admin".into()),
                    },
                    krabka_metadata::QuotaEntity {
                        entity_type: "client-id".into(),
                        entity_name: Some("admin-client".into()),
                    },
                ],
                config_key: "controller_mutation_rate".into(),
                config_value: Some(rate),
            },
        )])
        .await
        .expect("seed quota");
}
