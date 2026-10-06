//! Handler tests for the order of the per-partition acknowledge checks of
//! `ShareFetch` and `ShareAcknowledge`, and for the partition that the
//! metadata does not hold.
//!
//! Kafka's `KafkaApis.handleAcknowledgements` runs
//! `validateAcknowledgementBatches` first (`INVALID_REQUEST`), then the topic
//! `Read` check (`TOPIC_AUTHORIZATION_FAILED`), then the metadata check
//! (`UNKNOWN_TOPIC_OR_PARTITION`). Only a partition that passes all three
//! reaches the share partition. The fetch half of a `ShareFetch` row runs the
//! `Read` check and the metadata check on its own, so a row can carry a fetch
//! error and a different acknowledge error.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{AclOperation, GroupConfigRecord, MetadataRecord, ResourceType};
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch as AcknowledgeBatch,
            ShareAcknowledgeRequest,
        },
        share_acknowledge_response::ShareAcknowledgeResponse,
        share_fetch_request::{
            AcknowledgementBatch as FetchAcknowledgeBatch, FetchPartition, FetchTopic,
            ShareFetchRequest,
        },
        share_fetch_response::ShareFetchResponse,
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};

use crate::{
    authorizer::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer},
    broker::BrokerHandle,
    codes,
    share_partition::state::RecordState::{self, Acquired},
    test_support::{
        decode_response, encode_request, peer, principal, request_context,
        start_broker_no_audit_with,
    },
};

const TOPIC: &str = "ack-order";
const PRODUCE_VERSION: i16 = 12;
const VERSION: i16 = 2;

/// The principal that the topic `Read` check refuses.
const DENIED: &str = "denied";
/// The principal that may read everything.
const READER: &str = "reader";

/// A partition that the one-partition topic does not have.
const MISSING_PARTITION: i32 = 3;

const ACCEPT: i8 = 1;
const RENEW: i8 = 4;

/// One acknowledgement batch: `(first_offset, last_offset, types)`.
type Batch = (i64, i64, &'static [i8]);

/// Denies topic `Read` to [`DENIED`] and allows everything else.
#[derive(Debug)]
struct DenyOnePrincipal;

impl Authorizer for DenyOnePrincipal {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        request: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        if request.principal.name == DENIED
            && request.resource_type == ResourceType::Topic
            && request.operation == AclOperation::Read
        {
            AuthorizationResult::Deny
        } else {
            AuthorizationResult::Allow
        }
    }
}

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| cfg.authorizer = Arc::new(DenyOnePrincipal)).await
}

async fn create_topic(broker: &BrokerHandle) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("ack-order-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.to_string(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    broker.wait_until_partition_present(TOPIC, 0).await;
    let image = broker.controller_image_for_test();
    WireUuid(
        image
            .topic(TOPIC)
            .expect("created topic")
            .topic_id
            .into_bytes(),
    )
}

/// Appends one batch of three records to partition 0.
async fn produce(broker: &BrokerHandle) {
    let request = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: TOPIC.to_string(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::V2(vec![RecordBatch {
                    last_offset_delta: 2,
                    records: (0..3)
                        .map(|offset_delta| Record {
                            offset_delta,
                            value: Some(Bytes::from_static(b"v")),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }])),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let shared = broker.broker_arc_for_test();
    let user = principal("producer");
    let address = peer();
    let ctx = request_context(&user, &address, "producer-client");
    let bytes = encode_request(&request, PRODUCE_VERSION);
    let response =
        crate::handlers::produce::handle(&shared, PRODUCE_VERSION, &bytes, bytes.clone(), &ctx)
            .await
            .expect("handle produce");
    let response: ProduceResponse = decode_response(&response, PRODUCE_VERSION);
    assert!(response.responses[0].partition_responses[0].error_code == codes::NONE);
}

/// Starts `group` at the earliest offset and lets [`READER`] acquire offsets
/// 0 to 2 at epoch 0.
async fn acquire_all(broker: &BrokerHandle, group: &str, topic_id: WireUuid) {
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: group.to_string(),
            configs: maplit::btreemap! {
                "share.auto.offset.reset".to_owned() => "earliest".to_owned()
            },
        })])
        .await
        .expect("set the group config");
    crate::test_support::initialize_share_state(
        broker,
        group,
        uuid::Uuid::from_bytes(topic_id.0),
        0,
    )
    .await;
    let response = share_fetch(broker, READER, group, 0, topic_id, &[(0, &[])], false).await;
    let acquired: Vec<_> = response.responses[0].partitions[0]
        .acquired_records
        .iter()
        .map(|range| (range.first_offset, range.last_offset))
        .collect();
    assert!(acquired == vec![(0, 2)], "{response:?}");
}

/// A `ShareFetch` that names each `(partition, batches)` row of `rows`.
async fn share_fetch(
    broker: &BrokerHandle,
    user: &str,
    group: &str,
    epoch: i32,
    topic_id: WireUuid,
    rows: &[(i32, &[Batch])],
    is_renew_ack: bool,
) -> ShareFetchResponse {
    let request = ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some("member".into()),
        share_session_epoch: epoch,
        max_bytes: 1 << 20,
        max_records: 500,
        batch_size: 500,
        is_renew_ack,
        topics: vec![FetchTopic {
            topic_id,
            partitions: rows
                .iter()
                .map(|&(partition_index, batches)| FetchPartition {
                    partition_index,
                    acknowledgement_batches: batches
                        .iter()
                        .map(
                            |&(first_offset, last_offset, types)| FetchAcknowledgeBatch {
                                first_offset,
                                last_offset,
                                acknowledge_types: types.to_vec(),
                                ..Default::default()
                            },
                        )
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let shared = broker.broker_arc_for_test();
    let user = principal(user);
    let address = peer();
    let ctx = request_context(&user, &address, "share-client");
    let bytes = encode_request(&request, VERSION);
    let response = crate::test_support::try_dispatch_context(
        &shared,
        krabka_protocol::owned::share_fetch_request::API_KEY,
        VERSION,
        &bytes,
        &ctx,
    )
    .await
    .expect("handle share fetch");
    decode_response(&response, VERSION)
}

async fn share_acknowledge(
    broker: &BrokerHandle,
    user: &str,
    group: &str,
    topic_id: WireUuid,
    (partition_index, batches): (i32, &[Batch]),
) -> ShareAcknowledgeResponse {
    let request = ShareAcknowledgeRequest {
        group_id: Some(group.into()),
        member_id: Some("member".into()),
        share_session_epoch: 1,
        topics: vec![AcknowledgeTopic {
            topic_id,
            partitions: vec![AcknowledgePartition {
                partition_index,
                acknowledgement_batches: batches
                    .iter()
                    .map(|&(first_offset, last_offset, types)| AcknowledgeBatch {
                        first_offset,
                        last_offset,
                        acknowledge_types: types.to_vec(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let shared = broker.broker_arc_for_test();
    let user = principal(user);
    let address = peer();
    let ctx = request_context(&user, &address, "share-client");
    let bytes = encode_request(&request, VERSION);
    let response = crate::test_support::try_dispatch_context(
        &shared,
        krabka_protocol::owned::share_acknowledge_request::API_KEY,
        VERSION,
        &bytes,
        &ctx,
    )
    .await
    .expect("handle share acknowledge");
    decode_response(&response, VERSION)
}

async fn record_states(broker: &BrokerHandle, group: &str, topic_id: WireUuid) -> Vec<RecordState> {
    let cell = broker
        .broker_arc_for_test()
        .share_partition_leaders
        .peek_for_test(group, uuid::Uuid::from_bytes(topic_id.0), 0)
        .expect("a loaded share partition");
    let state = cell.lock().await;
    state
        .record_states()
        .into_iter()
        .map(|(_, state)| state)
        .collect()
}

/// One scenario for both APIs.
struct Case {
    name: &'static str,
    user: &'static str,
    partition: i32,
    batches: &'static [Batch],
    /// `ShareFetch`: the row's `(error_code, acknowledge_error_code)`.
    fetch: (i16, i16),
    /// `ShareAcknowledge`: the row's error code.
    acknowledge: i16,
    /// The state of each offset left in the window of partition 0 after the
    /// request.
    states: &'static [RecordState],
}

const HELD: &[RecordState] = &[Acquired, Acquired, Acquired];

fn cases() -> Vec<Case> {
    let invalid = |name, batches| Case {
        name,
        user: READER,
        partition: 0,
        batches,
        fetch: (codes::NONE, codes::INVALID_REQUEST),
        acknowledge: codes::INVALID_REQUEST,
        states: HELD,
    };
    vec![
        Case {
            name: "allowed and valid",
            user: READER,
            partition: 0,
            batches: &[(0, 2, &[ACCEPT])],
            fetch: (codes::NONE, codes::NONE),
            acknowledge: codes::NONE,
            // The accepted prefix leaves the window with the SPSO.
            states: &[],
        },
        Case {
            name: "denied and valid",
            user: DENIED,
            partition: 0,
            batches: &[(0, 2, &[ACCEPT])],
            fetch: (
                codes::TOPIC_AUTHORIZATION_FAILED,
                codes::TOPIC_AUTHORIZATION_FAILED,
            ),
            acknowledge: codes::TOPIC_AUTHORIZATION_FAILED,
            states: HELD,
        },
        Case {
            name: "denied and invalid",
            user: DENIED,
            partition: 0,
            batches: &[(2, 0, &[ACCEPT])],
            fetch: (codes::TOPIC_AUTHORIZATION_FAILED, codes::INVALID_REQUEST),
            acknowledge: codes::INVALID_REQUEST,
            states: HELD,
        },
        invalid("first offset past last", &[(2, 0, &[ACCEPT])]),
        invalid(
            "overlapping batches",
            &[(0, 1, &[ACCEPT]), (0, 2, &[ACCEPT])],
        ),
        invalid("no acknowledge type", &[(0, 2, &[])]),
        invalid("type count not the range", &[(0, 2, &[ACCEPT, ACCEPT])]),
        invalid("type out of range", &[(0, 2, &[5])]),
        invalid("renew without IsRenewAck", &[(0, 2, &[RENEW])]),
        Case {
            name: "partition the metadata does not hold",
            user: READER,
            partition: MISSING_PARTITION,
            batches: &[(0, 0, &[ACCEPT])],
            fetch: (
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                codes::UNKNOWN_TOPIC_OR_PARTITION,
            ),
            acknowledge: codes::UNKNOWN_TOPIC_OR_PARTITION,
            states: HELD,
        },
    ]
}

#[tokio::test]
async fn acknowledge_checks_run_in_kafka_order() {
    let (broker, _dir) = start().await;
    let topic_id = create_topic(&broker).await;
    produce(&broker).await;

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (index, case) in cases().into_iter().enumerate() {
        let group = format!("fetch-{index}");
        acquire_all(&broker, &group, topic_id).await;
        let response = share_fetch(
            &broker,
            case.user,
            &group,
            1,
            topic_id,
            &[(case.partition, case.batches)],
            false,
        )
        .await;
        let row = &response.responses[0].partitions[0];
        actual.push((
            "ShareFetch",
            case.name,
            (
                row.partition_index,
                row.error_code,
                row.acknowledge_error_code,
            ),
            record_states(&broker, &group, topic_id).await,
        ));
        expected.push((
            "ShareFetch",
            case.name,
            (case.partition, case.fetch.0, case.fetch.1),
            case.states.to_vec(),
        ));

        let group = format!("acknowledge-{index}");
        acquire_all(&broker, &group, topic_id).await;
        let response = share_acknowledge(
            &broker,
            case.user,
            &group,
            topic_id,
            (case.partition, case.batches),
        )
        .await;
        let row = &response.responses[0].partitions[0];
        actual.push((
            "ShareAcknowledge",
            case.name,
            (row.partition_index, row.error_code, codes::NONE),
            record_states(&broker, &group, topic_id).await,
        ));
        expected.push((
            "ShareAcknowledge",
            case.name,
            (case.partition, case.acknowledge, codes::NONE),
            case.states.to_vec(),
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// A denied topic row without acknowledgements in a request without any
/// acknowledgement answers only the fetch error.
#[tokio::test]
async fn a_denied_row_without_acknowledgements_has_no_acknowledge_error() {
    let (broker, _dir) = start().await;
    let topic_id = create_topic(&broker).await;
    produce(&broker).await;
    acquire_all(&broker, "no-acks", topic_id).await;

    let response = share_fetch(&broker, DENIED, "no-acks", 1, topic_id, &[(0, &[])], false).await;
    let row = &response.responses[0].partitions[0];

    assert!(
        (row.error_code, row.acknowledge_error_code)
            == (codes::TOPIC_AUTHORIZATION_FAILED, codes::NONE)
    );
    broker.shutdown().await;
}
