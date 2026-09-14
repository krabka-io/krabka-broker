//! Handler tests for how `Fetch` answers a topic row by request version and by
//! the kind of topic reference that the row carries.
//!
//! Kafka's `KafkaApis.handleFetchRequest` resolves the topic of every row
//! through `metadataCache.topicIdsToNames()` at version 13 and later. A row
//! whose id gives no name, the zero id included, gets `UNKNOWN_TOPIC_ID` on
//! every partition row, before the topic `Read` authorization. Versions 12 and
//! earlier name the topic, and a name that does not resolve goes to the
//! authorization gate and then to the partition gate.
//!
//! The tests encode each response the way the wire carries it and decode it
//! again, so the expected structs hold only what a client sees.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::{FetchResponse, FetchableTopicResponse, PartitionData},
    },
    primitives::uuid::Uuid as WireUuid,
    records::RecordsPayload,
};

use super::{FIRST_TOPIC_ID_VERSION, encode_fetch_response, handle};
use crate::{
    authorizer::{AllowAllAuthorizer, Authorizer},
    broker::BrokerHandle,
    codes,
    fetch_session::{FINAL_EPOCH, INITIAL_EPOCH, INVALID_SESSION_ID},
    test_support::{
        DenyAll, encode_request, peer, principal, request_context,
        start_broker_with_authorizer_no_audit,
    },
};

/// An id that no topic in these tests has.
const UNKNOWN_ID: WireUuid = WireUuid([0x0b; 16]);

/// A second id that no topic in these tests has.
const OTHER_UNKNOWN_ID: WireUuid = WireUuid([0x0c; 16]);

/// A name that no topic in these tests has.
const UNKNOWN_NAME: &str = "no-such-topic";

/// The highest `Fetch` version that the broker serves.
const MAX_VERSION: i16 = krabka_protocol::owned::fetch_request::MAX_VERSION;

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
type Outcome = (i16, TopicRef, FetchResponse);

/// The empty record set, as a client decodes it.
fn no_records() -> RecordsPayload {
    RecordsPayload::Legacy(Bytes::new())
}

/// The partition row of Kafka's `FetchResponse.partitionResponse`, which
/// `KafkaApis.handleFetchRequest` builds for a row that it refuses before the
/// read.
fn refused_partition(partition_index: i32, error_code: i16) -> PartitionData {
    PartitionData {
        partition_index,
        error_code,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: Some(Vec::new()),
        preferred_read_replica: -1,
        records: Some(no_records()),
        ..Default::default()
    }
}

/// The partition row of an empty topic that the consumer may read, as it
/// decodes at `version`. The wire carries `log_start_offset` from v5 on. An
/// older version decodes the field's default, -1.
fn empty_partition(version: i16) -> PartitionData {
    PartitionData {
        partition_index: 0,
        error_code: codes::NONE,
        high_watermark: 0,
        last_stable_offset: 0,
        log_start_offset: if version >= 5 { 0 } else { -1 },
        aborted_transactions: None,
        preferred_read_replica: -1,
        records: Some(no_records()),
        ..Default::default()
    }
}

/// One request row for partition 0 of the topic that `name` and `topic_id`
/// name.
fn fetch_topic(name: &str, topic_id: WireUuid) -> FetchTopic {
    FetchTopic {
        topic: name.to_owned(),
        topic_id,
        partitions: vec![FetchPartition {
            partition: 0,
            fetch_offset: 0,
            partition_max_bytes: 1_048_576,
            ..Default::default()
        }],
        ..Default::default()
    }
}

async fn start(authorizer: Arc<dyn Authorizer>) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with_authorizer_no_audit(authorizer).await
}

async fn create_topic(broker: &BrokerHandle, name: &str) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("fetch-resolution-test")
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

/// Send one `Fetch` at `version` and return the response as a client decodes
/// it.
async fn fetch(broker: &BrokerHandle, version: i16, request: &FetchRequest) -> FetchResponse {
    let shared = broker.broker_arc_for_test();
    let user = principal("consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "consumer-client");
    let request_bytes = encode_request(request, version);
    let (response, response_version) = handle(&shared, version, 7, &request_bytes, &ctx)
        .await
        .expect("handle fetch");
    let wire = encode_fetch_response(response, response_version).expect("encode response");
    let mut cursor: &[u8] = wire.as_ref();
    let decoded = FetchResponse::decode(&mut cursor, version).expect("decode response");
    assert!(cursor.is_empty(), "the decoder consumed every byte");
    decoded
}

/// Send one sessionless single-row `Fetch` at `case.version` for `case.topic`.
/// Return the decoded response and the response that the case expects.
///
/// `known` names a topic that exists, with its id. The expected topic row
/// carries the name and the id as the wire carries them at `case.version`: the
/// name at v12 and earlier, the id at v13 and later.
async fn drive(
    broker: &BrokerHandle,
    known: Option<(&str, WireUuid)>,
    case: Case,
) -> (Outcome, Outcome) {
    let (known_name, known_id) = known.unwrap_or(("", WireUuid::ZERO));
    let (name, topic_id) = match case.topic {
        TopicRef::KnownName => (known_name, WireUuid::ZERO),
        TopicRef::UnknownName => (UNKNOWN_NAME, WireUuid::ZERO),
        TopicRef::KnownId => ("", known_id),
        TopicRef::UnknownId => ("", UNKNOWN_ID),
        TopicRef::ZeroId => ("", WireUuid::ZERO),
    };
    let request = FetchRequest {
        max_wait_ms: 0,
        min_bytes: 0,
        session_id: INVALID_SESSION_ID,
        session_epoch: FINAL_EPOCH,
        topics: vec![fetch_topic(name, topic_id)],
        ..Default::default()
    };
    let actual = fetch(broker, case.version, &request).await;

    let partition = if case.error_code == codes::NONE {
        empty_partition(case.version)
    } else {
        refused_partition(0, case.error_code)
    };
    let id_only = case.version >= FIRST_TOPIC_ID_VERSION;
    let expected = FetchResponse {
        error_code: codes::NONE,
        session_id: INVALID_SESSION_ID,
        responses: vec![FetchableTopicResponse {
            topic: if id_only {
                String::new()
            } else {
                name.to_owned()
            },
            topic_id: if id_only { topic_id } else { WireUuid::ZERO },
            partitions: vec![partition],
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
    let mut cases = vec![
        Case {
            version: 4,
            topic: TopicRef::KnownName,
            error_code: codes::NONE,
        },
        Case {
            version: 4,
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
    ];
    for version in [FIRST_TOPIC_ID_VERSION, MAX_VERSION] {
        cases.extend([
            Case {
                version,
                topic: TopicRef::KnownId,
                error_code: codes::NONE,
            },
            Case {
                version,
                topic: TopicRef::UnknownId,
                error_code: codes::UNKNOWN_TOPIC_ID,
            },
            Case {
                version,
                topic: TopicRef::ZeroId,
                error_code: codes::UNKNOWN_TOPIC_ID,
            },
        ]);
    }
    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;
    let known_id = create_topic(&broker, "resolution").await;

    let mut actual = Vec::with_capacity(cases.len());
    let mut expected = Vec::with_capacity(cases.len());
    for case in cases {
        let (got, want) = drive(&broker, Some(("resolution", known_id)), case).await;
        actual.push(got);
        expected.push(want);
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic. A
/// principal with no `Read` grant sees 100 for an id that does not resolve,
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
            version: FIRST_TOPIC_ID_VERSION,
            topic: TopicRef::UnknownId,
            error_code: codes::UNKNOWN_TOPIC_ID,
        },
        Case {
            version: FIRST_TOPIC_ID_VERSION,
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

/// Each topic whose id does not resolve keeps its own topic row. Kafka's
/// `FetchResponse.toMessage` fails with a `NullPointerException` when two such
/// rows are adjacent and answers `UNKNOWN_SERVER_ERROR` for the whole request.
/// The broker answers every row instead, as Kafka does for one such row.
#[tokio::test]
async fn every_unresolved_id_keeps_its_own_topic_row() {
    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;
    let request = FetchRequest {
        max_wait_ms: 0,
        min_bytes: 0,
        topics: vec![
            fetch_topic("", UNKNOWN_ID),
            fetch_topic("", WireUuid::ZERO),
            fetch_topic("", OTHER_UNKNOWN_ID),
        ],
        ..Default::default()
    };

    let actual = fetch(&broker, MAX_VERSION, &request).await;

    let refused_topic = |topic_id| FetchableTopicResponse {
        topic: String::new(),
        topic_id,
        partitions: vec![refused_partition(0, codes::UNKNOWN_TOPIC_ID)],
        ..Default::default()
    };
    let expected = FetchResponse {
        responses: vec![
            refused_topic(UNKNOWN_ID),
            refused_topic(WireUuid::ZERO),
            refused_topic(OTHER_UNKNOWN_ID),
        ],
        ..Default::default()
    };
    assert!(actual == expected);
    broker.shutdown().await;
}

/// A fetch session keeps a partition whose id does not resolve. Kafka's
/// `FullFetchContext` caches it, and `CachedPartition.maybeUpdateResponseData`
/// puts every partition with an error into each incremental response. So the
/// first response and every incremental response carry 100 for it.
#[tokio::test]
async fn a_fetch_session_repeats_unknown_topic_id() {
    let (broker, _dir) = start(Arc::new(AllowAllAuthorizer)).await;
    let opened = fetch(
        &broker,
        MAX_VERSION,
        &FetchRequest {
            max_wait_ms: 0,
            min_bytes: 0,
            session_id: INVALID_SESSION_ID,
            session_epoch: INITIAL_EPOCH,
            topics: vec![fetch_topic("", UNKNOWN_ID)],
            ..Default::default()
        },
    )
    .await;
    let session_id = opened.session_id;
    assert!(session_id != INVALID_SESSION_ID, "{opened:?}");

    let mut actual = vec![(INITIAL_EPOCH, opened)];
    for epoch in [1, 2] {
        let incremental = FetchRequest {
            max_wait_ms: 0,
            min_bytes: 0,
            session_id,
            session_epoch: epoch,
            ..Default::default()
        };
        actual.push((epoch, fetch(&broker, MAX_VERSION, &incremental).await));
    }

    let expected_response = FetchResponse {
        session_id,
        responses: vec![FetchableTopicResponse {
            topic: String::new(),
            topic_id: UNKNOWN_ID,
            partitions: vec![refused_partition(0, codes::UNKNOWN_TOPIC_ID)],
            ..Default::default()
        }],
        ..Default::default()
    };
    let expected: Vec<_> = [INITIAL_EPOCH, 1, 2]
        .into_iter()
        .map(|epoch| (epoch, expected_response.clone()))
        .collect();
    assert!(actual == expected);
    broker.shutdown().await;
}
