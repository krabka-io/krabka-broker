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
        decode_response, encode_request, peer, principal, request_context,
        start_broker_no_audit_with,
    },
};

const PRODUCE_VERSION: i16 = 12;
const VERSION: i16 = 2;
const ACCEPT: i8 = 1;

async fn start(session_max: usize) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(move |cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
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

/// `(partition, error_code, acknowledge_error_code, acquired ranges)` of one
/// response row.
type RowOutcome = (i32, i16, i16, Vec<(i64, i64)>);

/// The [`RowOutcome`] of every row of `response`.
fn rows(response: &ShareFetchResponse) -> Vec<RowOutcome> {
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

/// The per-group `share.*` overrides reach the share partition: the lock
/// duration that `AcquisitionLockTimeoutMs` reports, the record lock limit of
/// the window, and the delivery count limit at which a release archives.
#[tokio::test]
async fn group_share_settings_override_the_broker_defaults() {
    const RELEASE: i8 = 2;
    // The broker's bound admits the record lock limit of 2 that the group
    // asks for, which is below Kafka's default minimum and would be capped to
    // it.
    let (broker, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_session_cache_max_when_unlimited = 10_000;
        cfg.share_group.min_partition_max_record_locks = 2;
    })
    .await;
    let topic_id = create_topic(&broker, "overrides", 1).await;
    produce(&broker, "overrides", 0).await;
    earliest(&broker, "g", topic_id, 1).await;
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: "g".to_string(),
            configs: maplit::btreemap! {
                "share.auto.offset.reset".to_owned() => "earliest".to_owned(),
                "share.record.lock.duration.ms".to_owned() => "60000".to_owned(),
                "share.delivery.count.limit".to_owned() => "2".to_owned(),
                "share.partition.max.record.locks".to_owned() => "2".to_owned(),
            },
        })])
        .await
        .expect("set the group config");
    let fetch = |epoch, rows| Fetch {
        group: "g",
        epoch,
        topic_id,
        rows,
        forgotten: &[],
        max_wait_ms: 0,
    };
    let delivered = |response: &ShareFetchResponse| {
        response
            .responses
            .iter()
            .flat_map(|topic| &topic.partitions)
            .flat_map(|row| &row.acquired_records)
            .map(|range| (range.first_offset, range.last_offset, range.delivery_count))
            .collect::<Vec<_>>()
    };

    let first = share_fetch(&broker, &fetch(0, &[(0, &[])])).await;
    let second = share_fetch(&broker, &fetch(1, &[(0, &[(0, 1, &[RELEASE])])])).await;
    let third = share_fetch(&broker, &fetch(2, &[(0, &[(0, 1, &[RELEASE])])])).await;

    assert!(
        (
            (first.acquisition_lock_timeout_ms, delivered(&first)),
            delivered(&second),
            delivered(&third),
        ) == (
            (60_000, vec![(0, 1, 1)]),
            vec![(0, 1, 2)],
            // The second release reaches the limit of 2 and archives 0 and 1
            // at once, so the window moves on to offset 2.
            vec![(2, 2, 1)],
        )
    );
    broker.shutdown().await;
}

/// `(version, ShareAcquireMode, MaxRecords, BatchSize, acquired rows)`.
type ShapeCase = (i16, i8, i32, i32, Vec<(i64, i64)>);

/// Kafka's `ShareAcquireMode` and `BatchSize` over two three-record log
/// batches at offsets 0 and 3, each case in a group of its own.
#[tokio::test]
async fn the_acquire_mode_and_batch_size_shape_the_acquired_rows() {
    let (broker, _dir) = start(10_000).await;
    let topic_id = create_topic(&broker, "shaped", 1).await;
    produce(&broker, "shaped", 0).await;
    produce(&broker, "shaped", 0).await;
    // (version, ShareAcquireMode, MaxRecords, BatchSize, acquired rows)
    let cases: [ShapeCase; 4] = [
        // batch_optimized, the only mode at v1: a whole log batch.
        (1, 0, 2, 500, vec![(0, 2)]),
        // record_limit: exactly MaxRecords.
        (2, 1, 2, 500, vec![(0, 1)]),
        // BatchSize splits new records on log batch boundaries.
        (2, 0, 500, 3, vec![(0, 2), (3, 5)]),
        (2, 1, 500, 3, vec![(0, 5)]),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (index, (version, mode, max_records, batch_size, rows)) in cases.into_iter().enumerate() {
        let group = format!("shaped-{index}");
        earliest(&broker, &group, topic_id, 1).await;
        let request = ShareFetchRequest {
            group_id: Some(group),
            member_id: Some("member".into()),
            max_bytes: 1 << 20,
            max_records,
            batch_size,
            share_acquire_mode: mode,
            topics: vec![FetchTopic {
                topic_id,
                partitions: vec![FetchPartition::default()],
                ..Default::default()
            }],
            ..Default::default()
        };
        let shared = broker.broker_arc_for_test();
        let user = principal("share-consumer");
        let address = peer();
        let ctx = request_context(&user, &address, "share-client");
        let response = handle(
            &shared,
            version,
            7,
            &encode_request(&request, version),
            &ctx,
        )
        .await
        .expect("handle share fetch");
        let response: ShareFetchResponse = decode_response(&response, version);
        let acquired: Vec<(i64, i64)> = response
            .responses
            .iter()
            .flat_map(|topic| &topic.partitions)
            .flat_map(|row| &row.acquired_records)
            .map(|range| (range.first_offset, range.last_offset))
            .collect();
        actual.push((index, acquired));
        expected.push((index, rows));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// A `ShareFetch` with the limits of `(max_bytes, min_bytes, max_wait_ms)`
/// over `partitions` of `topic_id`, at session epoch 0 of `group`. It returns
/// the acquired ranges per partition, in partition order, and the time the
/// response took.
async fn fetch_with_limits(
    broker: &BrokerHandle,
    group: &str,
    topic_id: WireUuid,
    partitions: &[i32],
    (max_bytes, min_bytes, max_wait_ms): (i32, i32, i32),
) -> (Vec<(i32, Vec<(i64, i64)>)>, Duration) {
    let request = ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some("member".into()),
        max_bytes,
        min_bytes,
        max_wait_ms,
        max_records: 500,
        batch_size: 500,
        topics: vec![FetchTopic {
            topic_id,
            partitions: partitions
                .iter()
                .map(|&partition_index| FetchPartition {
                    partition_index,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let shared = broker.broker_arc_for_test();
    let user = principal("share-consumer");
    let address = peer();
    let ctx = request_context(&user, &address, "share-client");
    let started = Instant::now();
    let response = handle(
        &shared,
        VERSION,
        7,
        &encode_request(&request, VERSION),
        &ctx,
    )
    .await
    .expect("handle share fetch");
    let took = started.elapsed();
    let response: ShareFetchResponse = decode_response(&response, VERSION);
    let mut rows: Vec<(i32, Vec<(i64, i64)>)> = response
        .responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|row| {
            (
                row.partition_index,
                row.acquired_records
                    .iter()
                    .map(|range| (range.first_offset, range.last_offset))
                    .collect(),
            )
        })
        .collect();
    rows.sort_unstable();
    (rows, took)
}

/// Kafka's `PartitionMaxBytesStrategy.UNIFORM`: `MaxBytes` is the budget of
/// the whole response, split across the partitions. A budget of one byte
/// reaches one of two partitions, which still gets its first batch whole.
#[tokio::test]
async fn max_bytes_is_split_across_the_partitions() {
    let (broker, _dir) = start(10_000).await;
    let topic_id = create_topic(&broker, "split", 2).await;
    for partition in [0, 1] {
        produce(&broker, "split", partition).await;
        produce(&broker, "split", partition).await;
    }
    earliest(&broker, "tight", topic_id, 2).await;
    earliest(&broker, "roomy", topic_id, 2).await;

    let (tight, _) = fetch_with_limits(&broker, "tight", topic_id, &[0, 1], (1, 0, 0)).await;
    let (roomy, _) = fetch_with_limits(&broker, "roomy", topic_id, &[0, 1], (1 << 20, 0, 0)).await;

    assert!(
        (tight, roomy)
            == (
                vec![(0, vec![(0, 2)]), (1, Vec::new())],
                vec![(0, vec![(0, 5)]), (1, vec![(0, 5)])],
            )
    );
    broker.shutdown().await;
}

/// Kafka's `DelayedShareFetch.isMinBytesSatisfied`: a response that holds
/// fewer than `MinBytes` waits out `MaxWaitMs`, and one that holds enough
/// answers at once.
#[tokio::test]
async fn min_bytes_holds_the_response_until_max_wait() {
    let (broker, _dir) = start(10_000).await;
    let topic_id = create_topic(&broker, "min-bytes", 1).await;
    produce(&broker, "min-bytes", 0).await;
    earliest(&broker, "short", topic_id, 1).await;
    earliest(&broker, "enough", topic_id, 1).await;

    let (short, short_took) =
        fetch_with_limits(&broker, "short", topic_id, &[0], (1 << 20, 1 << 20, 300)).await;
    let (enough, enough_took) =
        fetch_with_limits(&broker, "enough", topic_id, &[0], (1 << 20, 1, 30_000)).await;

    assert!(
        (
            short,
            short_took >= Duration::from_millis(300),
            enough,
            enough_took < Duration::from_secs(10),
        ) == (vec![(0, vec![(0, 2)])], true, vec![(0, vec![(0, 2)])], true)
    );
    broker.shutdown().await;
}
