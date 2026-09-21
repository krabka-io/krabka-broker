//! Handler tests for KIP-1222 renew acknowledgements on `ShareFetch` and
//! `ShareAcknowledge`.
//!
//! The Java client sets `IsRenewAck` for a whole request when any batch holds
//! the type `Renew` (4). Kafka's `SharePartition` renews only the offsets of
//! type 4 and applies the other types as usual. `share.renew.acknowledge.enable`
//! set to `false` refuses a renewal with `INVALID_RECORD_STATE`.
//! `KafkaApis.handleShareFetchRequest` refuses a renew-ack fetch whose
//! `MaxBytes`, `MinBytes`, `MaxRecords` or `MaxWaitMs` is not 0 with a
//! top-level `INVALID_REQUEST`, and runs no fetch for a valid one.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{GroupConfigRecord, MetadataRecord};
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

use super::handle;
use crate::{
    authorizer::AllowAllAuthorizer,
    broker::BrokerHandle,
    codes,
    share_partition::state::RecordState::{self, Acknowledged, Acquired, Available},
    test_support::{
        decode_response, encode_request, peer, principal, request_context, start_broker_with,
    },
};

/// Produce v12 names the topic.
const PRODUCE_VERSION: i16 = 12;

/// The request version that carries `IsRenewAck`.
const VERSION: i16 = 2;

const ACCEPT: i8 = 1;
const RELEASE: i8 = 2;
const RENEW: i8 = 4;

/// One acknowledgement batch: `(first_offset, last_offset, types)`.
type Batch = (i64, i64, &'static [i8]);

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_group.enable = true;
    })
    .await
}

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("share-renew-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
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
    broker.wait_until_partition_present(name, 0).await;
    let image = broker.controller_image_for_test();
    let topic = image.topic(name).expect("created topic in the image");
    WireUuid(topic.topic_id.into_bytes())
}

/// Appends one batch of `count` records to partition 0 of `topic`.
async fn produce(broker: &BrokerHandle, topic: &str, count: i32) {
    let request = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(RecordsPayload::V2(vec![RecordBatch {
                    last_offset_delta: count - 1,
                    records: (0..count)
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
    let request_bytes = encode_request(&request, PRODUCE_VERSION);
    let response_bytes = crate::handlers::produce::handle(
        &shared,
        PRODUCE_VERSION,
        7,
        &request_bytes,
        request_bytes.clone(),
        &ctx,
    )
    .await
    .expect("handle produce");
    let response: ProduceResponse = decode_response(&response_bytes, PRODUCE_VERSION);
    assert!(
        response.responses[0].partition_responses[0].error_code == codes::NONE,
        "{response:?}"
    );
}

/// The fetch limits of a `ShareFetch`.
#[derive(Debug, Clone, Copy)]
struct Limits {
    max_bytes: i32,
    max_records: i32,
}

const FETCH: Limits = Limits {
    max_bytes: 1 << 20,
    max_records: 500,
};

const NO_FETCH: Limits = Limits {
    max_bytes: 0,
    max_records: 0,
};

async fn share_fetch(
    broker: &BrokerHandle,
    group: &str,
    epoch: i32,
    topic_id: WireUuid,
    is_renew_ack: bool,
    limits: Limits,
    batches: &[Batch],
) -> ShareFetchResponse {
    let request = ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some("member".into()),
        share_session_epoch: epoch,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: limits.max_bytes,
        max_records: limits.max_records,
        batch_size: limits.max_records,
        is_renew_ack,
        topics: vec![FetchTopic {
            topic_id,
            partitions: vec![FetchPartition {
                partition_index: 0,
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
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    share_fetch_as(broker, "share-consumer", &request).await
}

async fn share_fetch_as(
    broker: &BrokerHandle,
    user: &str,
    request: &ShareFetchRequest,
) -> ShareFetchResponse {
    let shared = broker.broker_arc_for_test();
    let user = principal(user);
    let address = peer();
    let ctx = request_context(&user, &address, "share-client");
    let request_bytes = encode_request(request, VERSION);
    let response = handle(&shared, VERSION, 7, &request_bytes, &ctx)
        .await
        .expect("handle share fetch");
    decode_response(&response, VERSION)
}

async fn share_acknowledge(
    broker: &BrokerHandle,
    group: &str,
    epoch: i32,
    topic_id: WireUuid,
    batches: &[Batch],
) -> ShareAcknowledgeResponse {
    let request = ShareAcknowledgeRequest {
        group_id: Some(group.into()),
        member_id: Some("member".into()),
        share_session_epoch: epoch,
        is_renew_ack: true,
        topics: vec![AcknowledgeTopic {
            topic_id,
            partitions: vec![AcknowledgePartition {
                partition_index: 0,
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
    let user = principal("share-consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "share-client");
    let request_bytes = encode_request(&request, VERSION);
    let response =
        crate::handlers::share_acknowledge::handle(&shared, VERSION, 7, &request_bytes, &ctx)
            .await
            .expect("handle share acknowledge");
    decode_response(&response, VERSION)
}

/// Sets `share.renew.acknowledge.enable=false` on `group`.
async fn disable_renew(broker: &BrokerHandle, group: &str) {
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: group.to_string(),
            configs: maplit::btreemap! {
                "share.renew.acknowledge.enable".to_owned() => "false".to_owned()
            },
        })])
        .await
        .expect("set the group config");
}

/// The request under test.
#[derive(Debug, Clone, Copy)]
enum Call {
    ShareAcknowledge,
    ShareFetch(Limits),
}

/// One scenario.
struct Case {
    name: &'static str,
    call: Call,
    renew_enabled: bool,
    batches: &'static [Batch],
    expected: Outcome,
}

/// What a scenario observes: the top-level error, the partition error
/// (`ShareAcknowledge`) or acknowledge error (`ShareFetch`), the offsets that
/// the request acquired, and the state of each offset afterwards.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    error: i16,
    acknowledge_error: Option<i16>,
    acquired: Vec<(i64, i64)>,
    states: Vec<(i64, RecordState)>,
}

fn cases() -> Vec<Case> {
    vec![
        Case {
            name: "renew-only",
            call: Call::ShareAcknowledge,
            renew_enabled: true,
            batches: &[(0, 2, &[RENEW])],
            expected: Outcome {
                error: codes::NONE,
                acknowledge_error: Some(codes::NONE),
                acquired: Vec::new(),
                states: vec![(0, Acquired), (1, Acquired), (2, Acquired)],
            },
        },
        Case {
            name: "renew-and-accept-in-another-batch",
            call: Call::ShareAcknowledge,
            renew_enabled: true,
            batches: &[(0, 0, &[RENEW]), (1, 2, &[ACCEPT])],
            expected: Outcome {
                error: codes::NONE,
                acknowledge_error: Some(codes::NONE),
                acquired: Vec::new(),
                states: vec![(0, Acquired), (1, Acknowledged), (2, Acknowledged)],
            },
        },
        Case {
            name: "per-offset-renew-accept-release",
            call: Call::ShareAcknowledge,
            renew_enabled: true,
            batches: &[(0, 2, &[RENEW, ACCEPT, RELEASE])],
            expected: Outcome {
                error: codes::NONE,
                acknowledge_error: Some(codes::NONE),
                acquired: Vec::new(),
                states: vec![(0, Acquired), (1, Acknowledged), (2, Available)],
            },
        },
        Case {
            name: "renew-disabled",
            call: Call::ShareAcknowledge,
            renew_enabled: false,
            batches: &[(0, 2, &[RENEW])],
            expected: Outcome {
                error: codes::NONE,
                acknowledge_error: Some(codes::INVALID_RECORD_STATE),
                acquired: Vec::new(),
                states: vec![(0, Acquired), (1, Acquired), (2, Acquired)],
            },
        },
        Case {
            name: "fetch-renew-with-max-records",
            call: Call::ShareFetch(Limits {
                max_bytes: 0,
                max_records: 500,
            }),
            renew_enabled: true,
            batches: &[(0, 0, &[RENEW]), (1, 2, &[ACCEPT])],
            expected: Outcome {
                error: codes::INVALID_REQUEST,
                acknowledge_error: None,
                acquired: Vec::new(),
                states: vec![(0, Acquired), (1, Acquired), (2, Acquired)],
            },
        },
        Case {
            name: "fetch-renew-with-zero-limits",
            call: Call::ShareFetch(NO_FETCH),
            renew_enabled: true,
            batches: &[(0, 0, &[RENEW]), (1, 2, &[ACCEPT])],
            expected: Outcome {
                error: codes::NONE,
                acknowledge_error: Some(codes::NONE),
                acquired: Vec::new(),
                states: vec![(0, Acquired), (1, Acknowledged), (2, Acknowledged)],
            },
        },
    ]
}

/// Each row acquires offsets 0-2 as one member, produces offsets 3-4 so that
/// a fetch could acquire more, and then sends the request under test.
#[tokio::test]
async fn renew_acknowledgements_renew_only_the_renew_offsets() {
    let (broker, _dir) = start().await;
    let shared = broker.broker_arc_for_test();

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for case in cases() {
        let group = format!("renew-{}", case.name);
        let topic_id = create_topic(&broker, &group).await;
        crate::test_support::initialize_share_state(
            &broker,
            &group,
            uuid::Uuid::from_bytes(topic_id.0),
            0,
        )
        .await;
        if !case.renew_enabled {
            disable_renew(&broker, &group).await;
        }
        let opened = share_fetch(&broker, &group, 0, topic_id, false, FETCH, &[]).await;
        assert!(opened.error_code == codes::NONE, "{opened:?}");
        produce(&broker, &group, 3).await;
        let fetched = share_fetch(&broker, &group, 1, topic_id, false, FETCH, &[]).await;
        assert!(
            fetched.responses[0].partitions[0].acquired_records.len() == 1,
            "{fetched:?}"
        );
        produce(&broker, &group, 2).await;

        let outcome = match case.call {
            Call::ShareAcknowledge => {
                let response = share_acknowledge(&broker, &group, 2, topic_id, case.batches).await;
                Outcome {
                    error: response.error_code,
                    acknowledge_error: response
                        .responses
                        .first()
                        .map(|topic| topic.partitions[0].error_code),
                    acquired: Vec::new(),
                    states: Vec::new(),
                }
            }
            Call::ShareFetch(limits) => {
                let response =
                    share_fetch(&broker, &group, 2, topic_id, true, limits, case.batches).await;
                let row = response.responses.first().map(|topic| &topic.partitions[0]);
                Outcome {
                    error: response.error_code,
                    acknowledge_error: row.map(|row| row.acknowledge_error_code),
                    acquired: row
                        .map(|row| {
                            row.acquired_records
                                .iter()
                                .map(|range| (range.first_offset, range.last_offset))
                                .collect()
                        })
                        .unwrap_or_default(),
                    states: Vec::new(),
                }
            }
        };
        let states = shared
            .share_partition_leaders
            .peek_for_test(&group, uuid::Uuid::from_bytes(topic_id.0), 0)
            .expect("the fetch cached the share partition")
            .lock()
            .await
            .record_states();

        actual.push((case.name, Outcome { states, ..outcome }));
        expected.push((case.name, case.expected));
    }

    assert!(actual == expected);
    broker.shutdown().await;
}

/// The principal that [`DenyTopicReadToOne`] refuses.
const NO_TOPIC_READ: &str = "no-topic-read";

/// Denies topic `Read` to [`NO_TOPIC_READ`] and allows everything else.
#[derive(Debug)]
struct DenyTopicReadToOne;

impl crate::authorizer::Authorizer for DenyTopicReadToOne {
    fn authorize(
        &self,
        _source: &dyn crate::authorizer::AclSource,
        request: &crate::authorizer::AuthorizationRequest<'_>,
    ) -> crate::authorizer::AuthorizationResult {
        if request.principal.name == NO_TOPIC_READ
            && request.resource_type == krabka_metadata::ResourceType::Topic
            && request.operation == krabka_metadata::AclOperation::Read
        {
            crate::authorizer::AuthorizationResult::Deny
        } else {
            crate::authorizer::AuthorizationResult::Allow
        }
    }
}

/// A renew-ack fetch runs only the acknowledgement path, so a denied topic
/// `Read` is the acknowledge error of the row, and the fetch error stays
/// `NONE`.
#[tokio::test]
async fn a_renew_fetch_answers_a_denied_topic_as_an_acknowledge_error() {
    let (broker, _dir) = start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(DenyTopicReadToOne);
        cfg.share_group.enable = true;
    })
    .await;
    let topic_id = create_topic(&broker, "renew-denied").await;
    crate::test_support::initialize_share_state(
        &broker,
        "renew-denied",
        uuid::Uuid::from_bytes(topic_id.0),
        0,
    )
    .await;
    let request = |epoch, is_renew_ack, limits: Limits, batches: &[Batch]| ShareFetchRequest {
        group_id: Some("renew-denied".into()),
        member_id: Some("member".into()),
        share_session_epoch: epoch,
        max_bytes: limits.max_bytes,
        max_records: limits.max_records,
        batch_size: limits.max_records,
        is_renew_ack,
        topics: vec![FetchTopic {
            topic_id,
            partitions: vec![FetchPartition {
                partition_index: 0,
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
            }],
            ..Default::default()
        }],
        ..Default::default()
    };

    let opened = share_fetch_as(&broker, NO_TOPIC_READ, &request(0, false, FETCH, &[])).await;
    let renewed = share_fetch_as(
        &broker,
        NO_TOPIC_READ,
        &request(1, true, NO_FETCH, &[(0, 0, &[RENEW])]),
    )
    .await;

    let row = |response: &ShareFetchResponse| {
        let partition = &response.responses[0].partitions[0];
        (partition.error_code, partition.acknowledge_error_code)
    };
    assert!(
        (row(&opened), row(&renewed))
            == (
                (codes::TOPIC_AUTHORIZATION_FAILED, codes::NONE),
                (codes::NONE, codes::TOPIC_AUTHORIZATION_FAILED)
            )
    );
    broker.shutdown().await;
}
