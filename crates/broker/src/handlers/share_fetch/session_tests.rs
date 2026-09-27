//! Handler tests for the share-session rules of `ShareFetch`, Kafka's
//! `SharePartitionManager.newContext` and `ShareSessionContext`.
//!
//! An initial fetch that replaces a live session keeps the member's acquired
//! records. A final fetch may name and forget partitions, fetches nothing,
//! and releases the member's records. An initial fetch ignores forgotten
//! partitions. A full session cache answers `SHARE_SESSION_LIMIT_REACHED`
//! only after `MaxWaitMs`. An incremental response leaves out the session
//! partitions that have nothing new.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{GroupConfigRecord, MetadataRecord};
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
        share_fetch_request::{
            AcknowledgementBatch, FetchPartition, FetchTopic, ForgottenTopic, ShareFetchRequest,
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
    share_partition::state::RecordState::{self, Acquired, Available},
    test_support::{
        decode_response, encode_request, peer, principal, request_context, start_broker_with,
    },
};

const PRODUCE_VERSION: i16 = 12;
const VERSION: i16 = 2;
const ACCEPT: i8 = 1;

async fn start(session_max: usize) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(move |cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_group.enable = true;
        cfg.share_session_cache_max_when_unlimited = session_max;
    })
    .await
}

/// Creates `topic` with `partitions` partitions and returns its id.
async fn create_topic(broker: &BrokerHandle, topic: &str, partitions: i32) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("share-session-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.to_string(),
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
        broker.wait_until_partition_present(topic, partition).await;
    }
    let image = broker.controller_image_for_test();
    WireUuid(
        image
            .topic(topic)
            .expect("created topic")
            .topic_id
            .into_bytes(),
    )
}

/// Appends three records to `partition` of `topic`.
async fn produce(broker: &BrokerHandle, topic: &str, partition: i32) {
    let request = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition,
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
        crate::handlers::produce::handle(&shared, PRODUCE_VERSION, 7, &bytes, bytes.clone(), &ctx)
            .await
            .expect("handle produce");
    let response: ProduceResponse = decode_response(&response, PRODUCE_VERSION);
    assert!(response.responses[0].partition_responses[0].error_code == codes::NONE);
}

/// Starts `group` at the earliest offset of each partition of `topic_id`.
async fn earliest(broker: &BrokerHandle, group: &str, topic_id: WireUuid, partitions: i32) {
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
    for partition in 0..partitions {
        crate::test_support::initialize_share_state(
            broker,
            group,
            uuid::Uuid::from_bytes(topic_id.0),
            partition,
        )
        .await;
    }
}

/// One request row: `(partition, acknowledgement batches)`.
type Row = (i32, &'static [(i64, i64, &'static [i8])]);

struct Fetch<'a> {
    group: &'a str,
    epoch: i32,
    topic_id: WireUuid,
    rows: &'a [Row],
    forgotten: &'a [i32],
    max_wait_ms: i32,
}

async fn share_fetch(broker: &BrokerHandle, fetch: &Fetch<'_>) -> ShareFetchResponse {
    let request = ShareFetchRequest {
        group_id: Some(fetch.group.into()),
        member_id: Some("member".into()),
        share_session_epoch: fetch.epoch,
        max_wait_ms: fetch.max_wait_ms,
        max_bytes: 1 << 20,
        max_records: 500,
        batch_size: 500,
        topics: if fetch.rows.is_empty() {
            Vec::new()
        } else {
            vec![FetchTopic {
                topic_id: fetch.topic_id,
                partitions: fetch
                    .rows
                    .iter()
                    .map(|&(partition_index, batches)| FetchPartition {
                        partition_index,
                        acknowledgement_batches: batches
                            .iter()
                            .map(|&(first_offset, last_offset, types)| AcknowledgementBatch {
                                first_offset,
                                last_offset,
                                acknowledge_types: types.to_vec(),
                                ..Default::default()
                            })
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }]
        },
        forgotten_topics_data: if fetch.forgotten.is_empty() {
            Vec::new()
        } else {
            vec![ForgottenTopic {
                topic_id: fetch.topic_id,
                partitions: fetch.forgotten.to_vec(),
                ..Default::default()
            }]
        },
        ..Default::default()
    };
    let shared = broker.broker_arc_for_test();
    let user = principal("share-consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "share-client");
    let bytes = encode_request(&request, VERSION);
    let response = handle(&shared, VERSION, 7, &bytes, &ctx)
        .await
        .expect("handle share fetch");
    decode_response(&response, VERSION)
}

async fn states(broker: &BrokerHandle, group: &str, topic_id: WireUuid) -> Vec<RecordState> {
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

/// `(partition, error_code, acknowledge_error_code, acquired ranges)` of
/// every row of `response`.
fn rows(response: &ShareFetchResponse) -> Vec<(i32, i16, i16, Vec<(i64, i64)>)> {
    response
        .responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|row| {
            (
                row.partition_index,
                row.error_code,
                row.acknowledge_error_code,
                row.acquired_records
                    .iter()
                    .map(|range| (range.first_offset, range.last_offset))
                    .collect(),
            )
        })
        .collect()
}

const HELD: &[RecordState] = &[Acquired, Acquired, Acquired];

/// Re-opening at epoch 0 keeps the records the member holds, and the member
/// can still acknowledge them. A final fetch that names a partition fetches
/// nothing and releases what the member holds.
#[tokio::test]
async fn reopening_keeps_records_and_a_final_fetch_fetches_nothing() {
    let (broker, _dir) = start(10_000).await;
    let topic_id = create_topic(&broker, "reopen", 1).await;
    produce(&broker, "reopen", 0).await;
    earliest(&broker, "g", topic_id, 1).await;
    let fetch = |epoch, rows, forgotten| Fetch {
        group: "g",
        epoch,
        topic_id,
        rows,
        forgotten,
        max_wait_ms: 0,
    };

    let opened = share_fetch(&broker, &fetch(0, &[(0, &[])], &[])).await;
    // Epoch 0 again, forgetting the partition it names: Kafka ignores the
    // forgotten partitions of an initial request.
    let reopened = share_fetch(&broker, &fetch(0, &[(0, &[])], &[0])).await;
    let after_reopen = states(&broker, "g", topic_id).await;
    let accepted = share_fetch(&broker, &fetch(1, &[(0, &[(0, 0, &[ACCEPT])])], &[])).await;
    let closed = share_fetch(&broker, &fetch(-1, &[(0, &[])], &[0])).await;
    let after_close = states(&broker, "g", topic_id).await;

    assert!(
        (
            (opened.error_code, rows(&opened)),
            (reopened.error_code, rows(&reopened)),
            after_reopen,
            (accepted.error_code, rows(&accepted)),
            (closed.error_code, rows(&closed)),
            after_close,
        ) == (
            (
                codes::NONE,
                vec![(0, codes::NONE, codes::NONE, vec![(0, 2)])]
            ),
            (codes::NONE, vec![(0, codes::NONE, codes::NONE, Vec::new())]),
            HELD.to_vec(),
            (codes::NONE, vec![(0, codes::NONE, codes::NONE, Vec::new())]),
            (codes::NONE, Vec::new()),
            vec![Available, Available],
        )
    );
    broker.shutdown().await;
}

/// A full session cache answers `SHARE_SESSION_LIMIT_REACHED` after
/// `MaxWaitMs`, not at once.
#[tokio::test]
async fn a_full_session_cache_answers_after_max_wait() {
    let (broker, _dir) = start(1).await;
    let topic_id = create_topic(&broker, "full", 1).await;
    let first = share_fetch(
        &broker,
        &Fetch {
            group: "first",
            epoch: 0,
            topic_id,
            rows: &[],
            forgotten: &[],
            max_wait_ms: 0,
        },
    )
    .await;
    let started = Instant::now();
    let refused = share_fetch(
        &broker,
        &Fetch {
            group: "second",
            epoch: 0,
            topic_id,
            rows: &[],
            forgotten: &[],
            max_wait_ms: 300,
        },
    )
    .await;
    let waited = started.elapsed();

    assert!(
        (
            first.error_code,
            refused.error_code,
            waited >= Duration::from_millis(300)
        ) == (codes::NONE, codes::SHARE_SESSION_LIMIT_REACHED, true)
    );
    broker.shutdown().await;
}

/// An incremental response carries only the partition that has records.
#[tokio::test]
async fn an_incremental_response_leaves_out_partitions_without_news() {
    let (broker, _dir) = start(10_000).await;
    let topic_id = create_topic(&broker, "incremental", 3).await;
    earliest(&broker, "g", topic_id, 3).await;
    let fetch = |epoch, rows| Fetch {
        group: "g",
        epoch,
        topic_id,
        rows,
        forgotten: &[],
        max_wait_ms: 0,
    };

    let opened = share_fetch(&broker, &fetch(0, &[(0, &[]), (1, &[]), (2, &[])])).await;
    produce(&broker, "incremental", 1).await;
    let continued = share_fetch(&broker, &fetch(1, &[])).await;
    let quiet = share_fetch(&broker, &fetch(2, &[])).await;

    assert!(
        (rows(&opened).len(), rows(&continued), rows(&quiet))
            == (
                3,
                vec![(1, codes::NONE, codes::NONE, vec![(0, 2)])],
                Vec::new()
            )
    );
    broker.shutdown().await;
}
