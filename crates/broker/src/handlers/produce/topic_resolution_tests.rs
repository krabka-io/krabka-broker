//! Handler tests for how `Produce` answers a topic row by request version and
//! by the kind of topic reference that the row carries.
//!
//! Kafka's `KafkaApis.handleProduceRequest` answers `UNKNOWN_TOPIC_ID` on
//! every partition row when an id-only version (v13 and later) names a topic
//! whose id does not resolve, the zero id included. It does this before the
//! topic `Write` authorization. Versions 12 and earlier name the topic, and a
//! name that does not resolve goes to the authorization gate and then to the
//! partition gate.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::{PartitionProduceResponse, ProduceResponse, TopicProduceResponse},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};

use super::{FIRST_TOPIC_ID_VERSION, handle};
use crate::{
    authorizer::{AllowAllAuthorizer, Authorizer},
    broker::BrokerHandle,
    codes,
    test_support::{
        DenyAll, decode_response, encode_request, peer, principal, request_context,
        start_broker_with_authorizer_no_audit,
    },
};

/// An id that no topic in these tests has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// A name that no topic in these tests has.
const UNKNOWN_NAME: &str = "no-such-topic";

/// The topic reference that one request row carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TopicRef {
    /// The name of a topic that exists, with the zero id (v12 and earlier).
    KnownName,
    /// A name that no topic has, with the zero id (v12 and earlier).
    UnknownName,
    /// The id of a topic that exists, with an empty name (v13 and later).
    KnownId,
    /// A non-zero id that no topic has, with an empty name (v13 and later).
    UnknownId,
    /// The zero id, with an empty name (v13 and later).
    ZeroId,
}

/// One row of a table: the request version, the topic reference, and the
/// error code that Kafka puts on the partition row.
#[derive(Debug, Clone, Copy)]
struct Case {
    version: i16,
    topic: TopicRef,
    error_code: i16,
}

/// The actual or the expected outcome of one [`Case`].
type Outcome = (i16, TopicRef, ProduceResponse);

/// One v2 batch with one record. A leader of a fresh topic appends it at
/// offset 0.
fn one_record_batch() -> RecordsPayload {
    RecordsPayload::V2(vec![RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    }])
}

/// The partition row that Kafka's `PartitionResponse(error)` constructor
/// builds for a row that failed before the append.
fn refused_partition(error_code: i16) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code,
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: -1,
        ..Default::default()
    }
}

/// The partition row for a one-record append to a fresh topic, as it decodes
/// at `version`. The wire carries `log_start_offset` from v5 on. An older
/// version decodes the field's default, -1.
fn appended_partition(version: i16) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code: codes::NONE,
        base_offset: 0,
        log_append_time_ms: -1,
        log_start_offset: if version >= 5 { 0 } else { -1 },
        ..Default::default()
    }
}

async fn start(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with_authorizer_no_audit(authorizer).await
}

async fn create_topic(broker: &BrokerHandle, name: &str) {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("produce-resolution-test")
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
}

/// Send one single-row `Produce` at `case.version` for `case.topic`. Return
/// the decoded response and the response that the case expects.
///
/// `known` is the name of a topic that exists and that no earlier case wrote
/// to. The expected topic row carries the name and the id as the wire carries
/// them at `case.version`: the name at v12 and earlier, the id at v13 and
/// later.
async fn drive(broker: &BrokerHandle, known: Option<&str>, case: Case) -> (Outcome, Outcome) {
    let known_id = known.map_or(WireUuid::ZERO, |name| {
        let image = broker.controller_image_for_test();
        let topic = image.topic(name).expect("known topic in the image");
        WireUuid(topic.topic_id.into_bytes())
    });
    let (name, topic_id) = match case.topic {
        TopicRef::KnownName => (known.unwrap_or_default(), WireUuid::ZERO),
        TopicRef::UnknownName => (UNKNOWN_NAME, WireUuid::ZERO),
        TopicRef::KnownId => ("", known_id),
        TopicRef::UnknownId => ("", UNKNOWN_ID),
        TopicRef::ZeroId => ("", WireUuid::ZERO),
    };
    let request = ProduceRequest {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: name.to_string(),
            topic_id,
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(one_record_batch()),
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
    let request_bytes = encode_request(&request, case.version);
    let response_bytes = handle(
        &shared,
        case.version,
        7,
        &request_bytes,
        request_bytes.clone(),
        &ctx,
    )
    .await
    .expect("handle produce");
    let actual: ProduceResponse = decode_response(&response_bytes, case.version);

    let partition = if case.error_code == codes::NONE {
        appended_partition(case.version)
    } else {
        refused_partition(case.error_code)
    };
    let id_only = case.version >= FIRST_TOPIC_ID_VERSION;
    let expected = ProduceResponse {
        responses: vec![TopicProduceResponse {
            name: if id_only {
                String::new()
            } else {
                name.to_string()
            },
            topic_id: if id_only { topic_id } else { WireUuid::ZERO },
            partition_responses: vec![partition],
            ..Default::default()
        }],
        ..Default::default()
    };
    (
        (case.version, case.topic, actual),
        (case.version, case.topic, expected),
    )
}

#[tokio::test]
async fn topic_row_error_follows_version_and_topic_reference() {
    let cases = [
        Case {
            version: 3,
            topic: TopicRef::KnownName,
            error_code: codes::NONE,
        },
        Case {
            version: 3,
            topic: TopicRef::UnknownName,
            error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
        },
        Case {
            version: 12,
            topic: TopicRef::KnownName,
            error_code: codes::NONE,
        },
        Case {
            version: 12,
            topic: TopicRef::UnknownName,
            error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
        },
        Case {
            version: 13,
            topic: TopicRef::KnownId,
            error_code: codes::NONE,
        },
        Case {
            version: 13,
            topic: TopicRef::UnknownId,
            error_code: codes::UNKNOWN_TOPIC_ID,
        },
        Case {
            version: 13,
            topic: TopicRef::ZeroId,
            error_code: codes::UNKNOWN_TOPIC_ID,
        },
    ];
    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for (row, case) in cases.into_iter().enumerate() {
        // Every row gets its own topic, so an appended row starts at offset 0.
        let known = format!("resolution-{row}");
        create_topic(&broker, &known).await;
        let (got, want) = drive(&broker, Some(&known), case).await;
        actual.push(got);
        expected.push(want);
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic. A
/// principal with no `Write` grant sees 100 for an id that does not resolve,
/// and 29 for a name that does not resolve.
#[tokio::test]
async fn unresolved_id_answers_before_topic_authorization() {
    let cases = [
        Case {
            version: 12,
            topic: TopicRef::UnknownName,
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
        },
        Case {
            version: 13,
            topic: TopicRef::UnknownId,
            error_code: codes::UNKNOWN_TOPIC_ID,
        },
        Case {
            version: 13,
            topic: TopicRef::ZeroId,
            error_code: codes::UNKNOWN_TOPIC_ID,
        },
    ];
    let (broker, _dir) = start(Arc::new(DenyAll)).await;

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for case in cases {
        let (got, want) = drive(&broker, None, case).await;
        actual.push(got);
        expected.push(want);
    }
    assert!(actual == expected);
    broker.shutdown().await;
}
