//! End-to-end tests for the `DeleteRecords` handler: the ACL denial rows, the
//! unknown-partition row, and the KFC-1 trim bound on a scheduled topic.
//!
//! These drive `handle` against a live broker, so they live apart from the
//! unit tests that each sibling module keeps beside its own helpers.

use std::{net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use krabka_protocol::owned::{
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic},
    delete_records_response::{
        DeleteRecordsPartitionResult, DeleteRecordsResponse, DeleteRecordsTopicResult,
    },
};
use krabka_security::Principal;

use super::*;
use crate::{
    broker::Broker,
    codes,
    handlers::delete_records::test_support::gated_config,
    test_support::{DenyAll, peer, principal},
};

const VERSION: i16 = 2;

fn request(topic: &str, partitions: &[(i32, i64)]) -> DeleteRecordsRequest {
    DeleteRecordsRequest {
        topics: vec![DeleteRecordsTopic {
            name: topic.into(),
            partitions: partitions
                .iter()
                .map(|(partition_index, offset)| DeleteRecordsPartition {
                    partition_index: *partition_index,
                    offset: *offset,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    }
}

crate::test_support::wire_helpers!(
    DeleteRecordsRequest,
    DeleteRecordsResponse,
    version = VERSION,
    client_id = "admin-client"
);

use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

async fn drive(
    broker: &Broker,
    req: &DeleteRecordsRequest,
    principal: &Principal,
    peer: &SocketAddr,
) -> DeleteRecordsResponse {
    let ctx = test_context(principal, peer);
    let req_bytes = encode_request(req);
    let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    decode_response(&bytes)
}

#[tokio::test]
async fn handle_denied_topic_returns_topic_auth_rows() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("alice");
    let peer = peer();
    let req = request("secret", &[(0, 3), (2, -1)]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = DeleteRecordsResponse {
        throttle_time_ms: 0,
        topics: vec![DeleteRecordsTopicResult {
            name: "secret".into(),
            partitions: vec![
                DeleteRecordsPartitionResult {
                    partition_index: 0,
                    low_watermark: -1,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                },
                DeleteRecordsPartitionResult {
                    partition_index: 2,
                    low_watermark: -1,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                },
            ],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_unknown_partition_preserves_requested_index() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request("missing", &[(4, 0)]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = DeleteRecordsResponse {
        throttle_time_ms: 0,
        topics: vec![DeleteRecordsTopicResult {
            name: "missing".into(),
            partitions: vec![DeleteRecordsPartitionResult {
                partition_index: 4,
                low_watermark: -1,
                error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

// Activation time of a batch that has long since come due.
const DELIVERED_MS: i64 = 1_700_000_000_000;
// Activation time of a batch that still waits. It sits far enough ahead
// that every clock this test can read calls it pending, so the schedule
// holds without a mock timeline.
const PENDING_MS: i64 = 4_100_000_000_000;

// A two-record batch that activates at `activation_ms`, stamped with the
// epoch the partition writer expects from a leader append.
fn batch_at(activation_ms: i64, leader_epoch: i32) -> krabka_protocol::records::RecordBatch {
    krabka_protocol::records::RecordBatch {
        partition_leader_epoch: leader_epoch,
        ..crate::delivery::test_support::batch_at(activation_ms)
    }
}

// Create `topic` with the given `delivery.mode`, then append one batch
// that has come due and one that has not. Two records per batch puts the
// log end offset at 4, and a scheduled topic's delivery watermark at 2.
async fn topic_holding_a_pending_batch(
    broker_handle: &crate::broker::BrokerHandle,
    broker: &Broker,
    topic: &str,
    delivery_mode: Option<&str>,
    ctx: &crate::handlers::RequestContext<'_>,
) {
    use krabka_protocol::owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        create_topics_response::{self, CreateTopicsResponse},
    };

    let version = create_topics_response::MAX_VERSION;
    let create = crate::test_support::encode_request(
        &CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.to_owned(),
                num_partitions: 1,
                replication_factor: 1,
                configs: delivery_mode
                    .map(|mode| CreatableTopicConfig {
                        name: crate::config_keys::DELIVERY_MODE.to_owned(),
                        value: Some(mode.to_owned()),
                        ..Default::default()
                    })
                    .into_iter()
                    .collect(),
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        },
        version,
    );
    let bytes = crate::handlers::create_topics::handle(broker, version, 1, &create, ctx)
        .await
        .expect("CreateTopics");
    let created: CreateTopicsResponse = crate::test_support::decode_response(&bytes, version);
    assert!(created.topics[0].error_code == codes::NONE, "{created:?}");
    broker_handle.wait_until_partition_present(topic, 0).await;

    let expected_policy = if delivery_mode == Some(crate::config_keys::DELIVERY_MODE_SCHEDULED) {
        krabka_log::DeliveryPolicy::Scheduled
    } else {
        krabka_log::DeliveryPolicy::Immediate
    };
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if broker_handle
                .partition_log_config_for_test(topic, 0)
                .is_some_and(|config| config.delivery_policy == expected_policy)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the delivery mode reaches the partition log");

    let part = broker
        .partitions
        .get(topic, krabka_ids::PartitionIndex(0))
        .expect("the partition is local");
    let leader_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    for activation_ms in [DELIVERED_MS, PENDING_MS] {
        part.produce_batch(batch_at(activation_ms, leader_epoch))
            .await
            .expect("append a batch");
    }
}

#[tokio::test]
async fn a_trim_stops_at_the_delivery_watermark_of_a_scheduled_topic() {
    // The `-1` sentinel resolves to the high watermark, which is 4 on both
    // topics: replication is never gated on delivery. The scheduled topic
    // keeps the batch that has not come due.
    let cases = [
        ("delete-records-immediate-delivery", None, 4),
        (
            "delete-records-scheduled-delivery",
            Some(crate::config_keys::DELIVERY_MODE_SCHEDULED),
            2,
        ),
    ];

    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let admin = principal("admin");
    let peer = peer();
    let ctx = test_context(&admin, &peer);

    for (topic, delivery_mode, expected_low_watermark) in cases {
        topic_holding_a_pending_batch(&broker_handle, &broker, topic, delivery_mode, &ctx).await;

        let resp = drive(&broker, &request(topic, &[(0, -1)]), &admin, &peer).await;

        let expected = DeleteRecordsResponse {
            throttle_time_ms: 0,
            topics: vec![DeleteRecordsTopicResult {
                name: topic.into(),
                partitions: vec![DeleteRecordsPartitionResult {
                    partition_index: 0,
                    low_watermark: expected_low_watermark,
                    error_code: codes::NONE,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        };
        check!(resp == expected, "{topic}");
    }

    broker_handle.shutdown().await;
}

// ── KFC-9: the break-glass gate over a trim ─────────────────────────

#[tokio::test]
async fn a_refused_trim_deletes_nothing() {
    let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        cfg.break_glass = gated_config();
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = principal("admin");
    let peer = peer();
    let ctx = test_context(&principal, &peer);
    topic_holding_a_pending_batch(&broker_handle, &broker, "orders", None, &ctx).await;
    let part = broker
        .partitions
        .get("orders", krabka_ids::PartitionIndex(0))
        .expect("the partition is local");
    let before = part.log_start_offset();

    let resp = drive(&broker, &request("orders", &[(0, -1)]), &principal, &peer).await;

    let expected = vec![DeleteRecordsTopicResult {
        name: "orders".to_owned(),
        partitions: vec![DeleteRecordsPartitionResult {
            partition_index: 0,
            low_watermark: -1,
            error_code: codes::POLICY_VIOLATION,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    }];
    assert!(resp.topics == expected, "{resp:?}");
    // The refusal refused: the log start offset did not move.
    check!(part.log_start_offset() == before);
    broker_handle.shutdown().await;
}

// ── The trims that must not move the deletion frontier ───────────────

/// The one row a `DeleteRecords` response carries for `topic-0`.
fn one_row(topic: &str, low_watermark: i64, error_code: i16) -> Vec<DeleteRecordsTopicResult> {
    vec![DeleteRecordsTopicResult {
        name: topic.to_owned(),
        partitions: vec![DeleteRecordsPartitionResult {
            partition_index: 0,
            low_watermark,
            error_code,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
    }]
}

/// A trim the partition has already satisfied answers success and deletes
/// nothing, and one past the log end is refused outright.
///
/// Both rows are load-bearing. A stale request that answered
/// `OFFSET_OUT_OF_RANGE` would make an ordinary retry look like an error, and
/// a request past the log end that answered success would claim records were
/// deleted that the partition never held.
#[tokio::test]
async fn a_trim_that_deletes_nothing_leaves_the_log_start_alone() {
    // `topic_holding_a_pending_batch` leaves the log start at 0 and the log
    // end at 4, so offset 0 is the retry case and offset 100 is past the end.
    let cases = [
        ("delete-records-stale", 0_i64, 0_i64, codes::NONE),
        (
            "delete-records-beyond-end",
            100,
            -1,
            codes::OFFSET_OUT_OF_RANGE,
        ),
    ];

    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let admin = principal("admin");
    let peer = peer();
    let ctx = test_context(&admin, &peer);

    for (topic, offset, low_watermark, error_code) in cases {
        topic_holding_a_pending_batch(&broker_handle, &broker, topic, None, &ctx).await;
        let part = broker
            .partitions
            .get(topic, krabka_ids::PartitionIndex(0))
            .expect("the partition is local");
        let before = part.log_start_offset();

        let resp = drive(&broker, &request(topic, &[(0, offset)]), &admin, &peer).await;

        check!(
            resp.topics == one_row(topic, low_watermark, error_code),
            "{topic}"
        );
        check!(part.log_start_offset() == before, "{topic}");
    }

    broker_handle.shutdown().await;
}

/// KFC-9: a write freeze over the topic refuses the trim, and the records
/// stay. The freeze answers ahead of every other check the trim makes, so a
/// caller who could otherwise trim still cannot.
#[tokio::test]
async fn a_frozen_topic_refuses_a_trim() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let admin = principal("admin");
    let peer = peer();
    let ctx = test_context(&admin, &peer);
    let topic = "delete-records-frozen";
    topic_holding_a_pending_batch(&broker_handle, &broker, topic, None, &ctx).await;
    let part = broker
        .partitions
        .get(topic, krabka_ids::PartitionIndex(0))
        .expect("the partition is local");
    let before = part.log_start_offset();

    broker_handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1TopicFreeze(
            krabka_metadata::TopicFreezeRecord {
                scope: topic.to_owned(),
                pattern_type: krabka_metadata::PatternType::Literal,
                frozen: true,
                reason: "a compliance hold".to_owned(),
                set_by: "User:carol".to_owned(),
                set_at_ms: 1_770_000_000_000,
                proposal_id: uuid::Uuid::nil(),
                key_id: String::new(),
                signature: Vec::new(),
            },
        ))
        .await
        .expect("the freeze record commits");
    broker_handle
        .wait_for_image(|image| {
            crate::freeze::resolve::resolve_topic_freeze(image, topic).is_some()
        })
        .await;

    let resp = drive(&broker, &request(topic, &[(0, -1)]), &admin, &peer).await;

    assert!(
        resp.topics == one_row(topic, -1, codes::POLICY_VIOLATION),
        "{resp:?}"
    );
    check!(part.log_start_offset() == before);
    broker_handle.shutdown().await;
}

/// Only the leader trims its local segments. A replica that has lost the
/// leadership answers `NOT_LEADER_OR_FOLLOWER` and keeps its records, so the
/// client re-resolves the leader rather than deleting on a stale replica.
#[tokio::test]
async fn a_replica_that_is_not_the_leader_refuses_a_trim() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let admin = principal("admin");
    let peer = peer();
    let ctx = test_context(&admin, &peer);
    let topic = "delete-records-follower";
    topic_holding_a_pending_batch(&broker_handle, &broker, topic, None, &ctx).await;
    let part = broker
        .partitions
        .get(topic, krabka_ids::PartitionIndex(0))
        .expect("the partition is local");
    let before = part.log_start_offset();
    part.current_leader.store(
        broker.config.node_id.0 + 1,
        std::sync::atomic::Ordering::Release,
    );

    let resp = drive(&broker, &request(topic, &[(0, -1)]), &admin, &peer).await;

    assert!(
        resp.topics == one_row(topic, -1, codes::NOT_LEADER_OR_FOLLOWER),
        "{resp:?}"
    );
    check!(part.log_start_offset() == before);
    broker_handle.shutdown().await;
}

/// Kafka refuses `DeleteRecords` on every partition of an internal topic with
/// `INVALID_TOPIC_EXCEPTION` and a low watermark of -1, and trims an ordinary
/// topic. Each topic holds two records first, so a trim to the high watermark
/// would move the log start.
#[tokio::test]
async fn an_internal_topic_refuses_a_trim() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let admin = principal("admin");
    let peer = peer();
    let ctx = test_context(&admin, &peer);

    crate::txn::bootstrap::ensure_topic(
        &broker.controller,
        1,
        1,
        &crate::txn::bootstrap::topic_configs(
            broker.config.transaction_state_segment_bytes,
            broker.config.transaction_state_min_isr,
        ),
    )
    .await
    .expect("create the transaction-state topic");
    crate::share_coordinator::bootstrap::ensure_topic(
        &broker.controller,
        1,
        1,
        &crate::share_coordinator::bootstrap::topic_configs(&broker.config.share_coordinator),
    )
    .await
    .expect("create the share-state topic");
    topic_holding_a_pending_batch(&broker_handle, &broker, "orders", None, &ctx).await;

    let cases = [
        (
            crate::coordinator::bootstrap::OFFSETS_TOPIC,
            -1_i64,
            codes::INVALID_TOPIC_EXCEPTION,
            0_i64,
        ),
        (
            crate::txn::bootstrap::TOPIC,
            -1,
            codes::INVALID_TOPIC_EXCEPTION,
            0,
        ),
        (
            crate::share_coordinator::bootstrap::TOPIC,
            -1,
            codes::INVALID_TOPIC_EXCEPTION,
            0,
        ),
        ("orders", 4, codes::NONE, 4),
    ];
    for (topic, low_watermark, error_code, log_start) in cases {
        broker_handle.wait_until_partition_present(topic, 0).await;
        let part = broker
            .partitions
            .get(topic, krabka_ids::PartitionIndex(0))
            .expect("the partition is local");
        if topic != "orders" {
            let leader_epoch = part
                .current_leader_epoch
                .load(std::sync::atomic::Ordering::Acquire);
            part.produce_batch(batch_at(DELIVERED_MS, leader_epoch))
                .await
                .expect("append a batch");
        }

        let resp = drive(&broker, &request(topic, &[(0, -1)]), &admin, &peer).await;

        check!(
            resp.topics == one_row(topic, low_watermark, error_code),
            "{topic}"
        );
        check!(
            part.log_start_offset() == krabka_log::Offset(log_start),
            "{topic}"
        );
    }

    // A partition of an internal topic that the metadata does not hold is
    // unknown, as `KafkaApis` answers it before the internal-topic check.
    let missing = 10_000;
    let resp = drive(
        &broker,
        &request(
            crate::coordinator::bootstrap::OFFSETS_TOPIC,
            &[(missing, -1)],
        ),
        &admin,
        &peer,
    )
    .await;
    check!(
        resp.topics[0].partitions
            == vec![error_partition_result(
                missing,
                codes::UNKNOWN_TOPIC_OR_PARTITION
            )]
    );

    broker_handle.shutdown().await;
}

// ── KIP-107: the trim reaches the followers ─────────────────────────

/// The follower that [`replicated_topic`] assigns beside this broker.
const FOLLOWER: u64 = 2;

/// Records [`replicated_topic`] appends to the leader log.
const APPENDED: i64 = 10;

/// Create `topic` with one partition that this broker leads and that the live,
/// registered broker [`FOLLOWER`] follows, append [`APPENDED`] records, and
/// fetch them as the follower so the high watermark reaches the log end.
async fn replicated_topic(
    broker_handle: &crate::broker::BrokerHandle,
    topic: &str,
) -> Arc<crate::partition::Partition> {
    use krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};

    let broker = broker_handle.broker_arc_for_test();
    register_follower(broker_handle).await;
    broker.liveness.record_heartbeat(FOLLOWER).await;
    let replicas = vec![krabka_audit::NodeId(1), krabka_audit::NodeId(FOLLOWER)];
    for record in [
        MetadataRecord::V1Topic(TopicRecord {
            name: topic.to_owned(),
            topic_id: uuid::Uuid::new_v4(),
            partitions: 1,
            replication_factor: 2,
        }),
        MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.to_owned(),
            partition: 0,
            leader: krabka_audit::NodeId(1),
            replicas: replicas.clone(),
            isr: replicas,
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: vec![uuid::Uuid::nil(); 2],
            partition_epoch: 0,
        }),
    ] {
        broker_handle
            .submit_metadata_record_for_test(record)
            .await
            .expect("submit topic metadata");
    }
    let partition = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if let Some(partition) = broker.partitions.get(topic, krabka_ids::PartitionIndex(0))
                && partition
                    .replica_state
                    .lock()
                    .await
                    .isr
                    .contains(&krabka_raft::NodeId(FOLLOWER))
            {
                return partition;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the broker leads the partition with the follower in the ISR");
    let mut batch = krabka_protocol::records::RecordBatch {
        last_offset_delta: i32::try_from(APPENDED - 1).expect("delta"),
        records: (0..APPENDED)
            .map(|offset| krabka_protocol::records::Record {
                offset_delta: i32::try_from(offset).expect("delta"),
                value: Some(bytes::Bytes::from_static(b"v")),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    partition
        .log
        .lock()
        .expect("partition log lock")
        .append(&mut batch)
        .expect("append the records");
    follower_fetch(&broker, topic, APPENDED, 0).await;
    assert!(partition.high_watermark().await == krabka_log::Offset(APPENDED));
    partition
}

/// Register [`FOLLOWER`] as a broker in the controller's image.
async fn register_follower(broker_handle: &crate::broker::BrokerHandle) {
    broker_handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                node_id: krabka_raft::NodeId(FOLLOWER),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: "127.0.0.1".into(),
                port: 9092,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            },
        ))
        .await
        .expect("register the follower");
}

/// Fence [`FOLLOWER`] the way the controller does: its heartbeat session is
/// fenced, and the replicated `broker.fenced` config says so.
async fn fence_follower(broker_handle: &crate::broker::BrokerHandle) {
    let broker = broker_handle.broker_arc_for_test();
    broker.liveness.apply_fencing(FOLLOWER, true, true).await;
    broker_handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_raft::NodeId(FOLLOWER),
                config_name: crate::config_keys::BROKER_FENCED.to_string(),
                config_value: Some(crate::config_keys::FENCED_TRUE.to_string()),
            },
        ))
        .await
        .expect("fence the follower");
}

/// One Fetch from [`FOLLOWER`] at `fetch_offset`, reporting `log_start_offset`
/// as the follower's own log start.
async fn follower_fetch(broker: &Broker, topic: &str, fetch_offset: i64, log_start_offset: i64) {
    use krabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};

    const VERSION: i16 = 12;
    let request = FetchRequest {
        replica_id: i32::try_from(FOLLOWER).expect("replica id"),
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: 1_048_576,
        session_id: crate::fetch_session::INVALID_SESSION_ID,
        session_epoch: crate::fetch_session::FINAL_EPOCH,
        topics: vec![FetchTopic {
            topic: topic.to_owned(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset,
                log_start_offset,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let replicator = principal("replicator");
    let peer = peer();
    let ctx = crate::test_support::request_context(&replicator, &peer, "replica-2");
    let (response, _) = crate::handlers::fetch::handle(
        broker,
        VERSION,
        1,
        &crate::test_support::encode_request(&request, VERSION),
        &ctx,
    )
    .await
    .expect("follower fetch");
    assert!(
        response.responses[0].partitions[0].error_code == codes::NONE,
        "{response:?}"
    );
}

/// The row a `DeleteRecords` of `topic-0` to `offset` answers, with
/// `timeout_ms`.
async fn delete_to(
    broker: &Broker,
    topic: &str,
    offset: i64,
    timeout_ms: i32,
) -> Vec<DeleteRecordsTopicResult> {
    let admin = principal("admin");
    let peer = peer();
    let request = DeleteRecordsRequest {
        timeout_ms,
        ..request(topic, &[(0, offset)])
    };
    drive(broker, &request, &admin, &peer).await.topics
}

/// Kafka's `DeleteRecords` purgatory (#746): a row answers once the lowest log
/// start of the leader and every live follower reaches the trim point. A
/// follower that has not reported it holds the row until `timeout_ms`, which
/// answers `REQUEST_TIMED_OUT` with the low watermark the trim saw. A follower
/// on a fenced broker is not live and does not hold the row.
#[tokio::test]
async fn a_trim_waits_for_every_live_follower_to_reach_the_trim_point() {
    let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        // The follower never fetches on its own. Keep it in the ISR and alive
        // for the whole test, so only the fetches below move its state.
        cfg.replica_lag_time_max = krabka_units::secs(600);
        cfg.heartbeat_timeout = krabka_units::secs(600);
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let topic = "delete-records-followers";
    let partition = replicated_topic(&broker_handle, topic).await;

    // The follower still reports log start 0, so the row times out with that
    // low watermark, and the leader log is trimmed all the same.
    let timed_out = delete_to(&broker, topic, 4, 200).await;
    check!(timed_out == one_row(topic, 0, codes::REQUEST_TIMED_OUT));
    check!(partition.log_start_offset() == krabka_log::Offset(4));

    // The follower catches up while a second request waits.
    let waiting = {
        let broker = broker.clone();
        tokio::spawn(async move { delete_to(&broker, topic, 6, 10_000).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    check!(!waiting.is_finished(), "the row waits for the follower");
    follower_fetch(&broker, topic, APPENDED, 6).await;
    let completed = tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
        .await
        .expect("the row completes once the follower reports")
        .expect("delete task");
    check!(completed == one_row(topic, 6, codes::NONE));

    // A retry at a point every replica already passed answers at once.
    let retry = delete_to(&broker, topic, 5, 0).await;
    check!(retry == one_row(topic, 6, codes::NONE));

    // A fenced follower is not live, so its old log start does not count.
    fence_follower(&broker_handle).await;
    let fenced = delete_to(&broker, topic, 8, 10_000).await;
    check!(fenced == one_row(topic, 8, codes::NONE));

    broker_handle.shutdown().await;
}
