//! End-to-end tests for issue #948: `ShareFetch` used to close the connection
//! once the log start offset moved past the share-partition start offset
//! (SPSO), because the log read that materialization and acquisition both
//! start from answered `OffsetTooLow` and the handler propagated it.
//!
//! Kafka's `ShareFetchUtils.processFetchResponse` catches the equivalent
//! `OFFSET_OUT_OF_RANGE` from the log read, calls
//! `SharePartition.updateCacheAndOffsets` to archive the `Available` records
//! below the new log start and move the SPSO and SPEO, and answers the
//! partition with `NONE`, retrying the fetch so a record already readable at
//! or above the new log start is not held back for another round trip. These
//! tests drive that same scenario against a live broker: produce records,
//! move the log start with a real `DeleteRecords`, and fetch again.
//!
//! The same fixtures serve two more end-to-end tests of the acquire pass: the
//! record lock limit that bounds the window, and a partition whose log read
//! fails beside a healthy one.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_records_request::{
            DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
        },
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
        share_fetch_request::{FetchPartition, FetchTopic, ShareFetchRequest},
        share_fetch_response::{PartitionData, ShareFetchResponse},
    },
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordBatch, RecordsPayload},
};

use crate::{
    authorizer::AllowAllAuthorizer,
    broker::BrokerHandle,
    codes,
    test_support::{
        decode_response, encode_request, initialize_share_state, peer, principal, request_context,
        start_broker_no_audit_with,
    },
};

/// One partition row of a multi-partition `ShareFetch`: its
/// `(partition_index, error_code, acquired ranges)`.
type PartitionOutcome = (i32, i16, Vec<(i64, i64)>);

/// Produce v12 names the topic.
const PRODUCE_VERSION: i16 = 12;

/// `DeleteRecords` v2, matching the other handler tests.
const DELETE_RECORDS_VERSION: i16 = 2;

async fn start() -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| cfg.authorizer = Arc::new(AllowAllAuthorizer)).await
}

async fn create_topic(broker: &BrokerHandle, name: &str, num_partitions: i32) -> WireUuid {
    let client = krabka_client_core::Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("share-log-start-lockout-test")
        .build()
        .await
        .expect("client build");
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.to_string(),
                num_partitions,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
    for partition in 0..num_partitions {
        broker.wait_until_partition_present(name, partition).await;
    }
    let image = broker.controller_image_for_test();
    let topic = image.topic(name).expect("created topic in the image");
    WireUuid(topic.topic_id.into_bytes())
}

/// Appends `count` one-record batches to `partition_index` of `topic`.
async fn produce_records(broker: &BrokerHandle, topic: &str, partition_index: i32, count: i32) {
    let request = ProduceRequest {
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition_index,
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

/// Trims `partition_index` of `topic` to `new_log_start` with a real
/// `DeleteRecords`, and returns the resulting error code.
async fn delete_records(
    broker: &BrokerHandle,
    topic: &str,
    partition_index: i32,
    new_log_start: i64,
) -> i16 {
    let request = DeleteRecordsRequest {
        topics: vec![DeleteRecordsTopic {
            name: topic.to_string(),
            partitions: vec![DeleteRecordsPartition {
                partition_index,
                offset: new_log_start,
                ..Default::default()
            }],
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let shared = broker.broker_arc_for_test();
    let user = principal("admin");
    let address = peer();
    let ctx = request_context(&user, &address, "admin-client");
    let response =
        crate::handlers::delete_records::handle(&shared, request, DELETE_RECORDS_VERSION, &ctx)
            .await
            .expect("handle delete records");
    response.topics[0].partitions[0].error_code
}

/// Sends a `ShareFetch` naming every partition index of `topic_id` with
/// `max_records`, and returns each row's `PartitionData` in request order.
async fn share_fetch_rows_with_max_records(
    broker: &BrokerHandle,
    group: &str,
    epoch: i32,
    topic_id: WireUuid,
    partitions: &[i32],
    max_records: i32,
) -> Vec<PartitionData> {
    let version = krabka_protocol::owned::share_fetch_request::MAX_VERSION;
    let request = ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some("member".into()),
        share_session_epoch: epoch,
        max_wait_ms: 0,
        max_bytes: 1 << 20,
        max_records,
        batch_size: max_records.max(1),
        // `record_limit`, so a priming fetch takes exactly `max_records` of
        // the one five-record batch.
        share_acquire_mode: 1,
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
    let request_bytes = encode_request(&request, version);
    let response = crate::test_support::try_dispatch_context(
        &shared,
        krabka_protocol::owned::share_fetch_request::API_KEY,
        version,
        &request_bytes,
        &ctx,
    )
    .await
    .expect("handle share fetch");
    let response: ShareFetchResponse = decode_response(&response, version);
    // An incremental response leaves out a partition with nothing new.
    response
        .responses
        .first()
        .map(|topic| topic.partitions.clone())
        .unwrap_or_default()
}

async fn share_fetch_rows(
    broker: &BrokerHandle,
    group: &str,
    epoch: i32,
    topic_id: WireUuid,
    partitions: &[i32],
) -> Vec<PartitionData> {
    share_fetch_rows_with_max_records(broker, group, epoch, topic_id, partitions, 500).await
}

async fn share_fetch_one(
    broker: &BrokerHandle,
    group: &str,
    epoch: i32,
    topic_id: WireUuid,
    partition_index: i32,
) -> PartitionData {
    share_fetch_rows(broker, group, epoch, topic_id, &[partition_index])
        .await
        .into_iter()
        .next()
        .unwrap_or(PartitionData {
            partition_index,
            ..Default::default()
        })
}

fn acquired(row: &PartitionData) -> Vec<(i64, i64)> {
    row.acquired_records
        .iter()
        .map(|range| (range.first_offset, range.last_offset))
        .collect()
}

fn topic_uuid(topic_id: WireUuid) -> uuid::Uuid {
    uuid::Uuid::from_bytes(topic_id.0)
}

async fn spso(broker: &BrokerHandle, group: &str, topic_id: WireUuid, partition_index: i32) -> i64 {
    let shared = broker.broker_arc_for_test();
    shared
        .share_partition_leaders
        .peek_for_test(group, topic_uuid(topic_id), partition_index)
        .expect("a cached share-partition leader")
        .lock()
        .await
        .start_offset
        .0
}

/// One row of the table: what is below the new log start when it moves past
/// the SPSO, and what the handler must do about it.
struct Row {
    name: &'static str,
    /// Records to acquire on the priming fetch, out of the 5 produced, so
    /// that fewer than 5 are handed out and the rest stay `Available`.
    prime_max_records: i32,
    /// The `DeleteRecords` offset: the new log start.
    new_log_start: i64,
    /// The lockout fetch's partition error.
    expected_error: i16,
    /// The lockout fetch's acquired ranges. The handler retries once in
    /// place after repairing the SPSO, so any record already readable at or
    /// above the new log start is handed out on this same pass -- not on a
    /// later fetch.
    expected_acquired: Vec<(i64, i64)>,
    /// The SPSO right after the lockout fetch.
    expected_spso: i64,
    /// What a follow-up fetch acquires: empty whenever the lockout fetch
    /// already handed out everything currently acquirable (the in-place
    /// retry means there is usually nothing left), and still empty for the
    /// "acquired stays locked" row, whose lock the lockout fetch cannot and
    /// must not touch.
    expected_next_acquired: Vec<(i64, i64)>,
}

/// The table from issue #948: nothing in flight, `Available` records below
/// the new start (archived), `Acquired` records below the new start (kept
/// locked until the lock ends), and a log start at the SPSO (no change).
#[tokio::test]
async fn share_fetch_survives_the_log_start_moving_past_the_spso() {
    let (broker, _dir) = start().await;

    let rows = [
        Row {
            name: "nothing in flight",
            prime_max_records: 0,
            new_log_start: 3,
            expected_error: codes::NONE,
            // The lockout fetch repairs the SPSO and retries in place, so it
            // already hands out [3,4] itself.
            expected_acquired: vec![(3, 4)],
            expected_spso: 3,
            expected_next_acquired: vec![],
        },
        Row {
            name: "available records below the new start are archived, acquired stay locked",
            prime_max_records: 2,
            new_log_start: 3,
            expected_error: codes::NONE,
            // [0,1] is Acquired by the priming fetch's member and blocks the
            // SPSO from following the log start; offset 2, which was
            // Available, is archived. Offsets [3,4] are unaffected by either
            // and are handed out on this same lockout fetch, in place.
            expected_acquired: vec![(3, 4)],
            expected_spso: 0,
            // The [0,1] lock is still held, and [3,4] were already handed
            // out above, so nothing new is left for a follow-up fetch.
            expected_next_acquired: vec![],
        },
        Row {
            name: "log start equal to the SPSO changes nothing",
            prime_max_records: 0,
            new_log_start: 0,
            expected_error: codes::NONE,
            expected_acquired: vec![(0, 4)],
            expected_spso: 0,
            expected_next_acquired: vec![],
        },
    ];

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (index, row) in rows.into_iter().enumerate() {
        let topic = format!("lockout-row-{index}");
        let group = format!("g-lockout-row-{index}");
        let topic_id = create_topic(&broker, &topic, 1).await;
        initialize_share_state(&broker, &group, topic_uuid(topic_id), 0).await;

        // Prime the leader cell at SPSO 0 before any record exists, so a
        // later `DeleteRecords` moves the log start out from under a cell
        // that is already cached, exactly as issue #948 describes.
        let mut epoch = 0;
        let opened = share_fetch_one(&broker, &group, epoch, topic_id, 0).await;
        assert!(opened.error_code == codes::NONE, "{}: {opened:?}", row.name);

        produce_records(&broker, &topic, 0, 5).await;

        // With `prime_max_records > 0`, this acquires that many of the 5
        // produced records, leaving the rest `Available`. It is skipped
        // entirely for a row that wants the window still unmaterialized
        // (`prime_max_records == 0`), so the leader cell's window stays
        // exactly as `initialize_share_state` left it.
        if row.prime_max_records > 0 {
            epoch += 1;
            let priming = share_fetch_rows_with_max_records(
                &broker,
                &group,
                epoch,
                topic_id,
                &[0],
                row.prime_max_records,
            )
            .await;
            assert!(
                priming[0].error_code == codes::NONE,
                "{}: priming fetch {:?}",
                row.name,
                priming[0]
            );
        }

        let delete_error = delete_records(&broker, &topic, 0, row.new_log_start).await;
        assert!(delete_error == codes::NONE, "{}: {delete_error}", row.name);

        epoch += 1;
        let lockout = share_fetch_one(&broker, &group, epoch, topic_id, 0).await;
        let after_lockout_spso = spso(&broker, &group, topic_id, 0).await;
        epoch += 1;
        let next = share_fetch_one(&broker, &group, epoch, topic_id, 0).await;

        actual.push((
            row.name,
            lockout.error_code,
            acquired(&lockout),
            after_lockout_spso,
            acquired(&next),
        ));
        expected.push((
            row.name,
            row.expected_error,
            row.expected_acquired,
            row.expected_spso,
            row.expected_next_acquired,
        ));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// Kafka's `ShareFetchUtils.processFetchResponse` turns the log read error of
/// one partition into that partition's error code with no records, and the
/// other partitions of the request still return theirs. A batch that does not
/// check out is `CORRUPT_MESSAGE`. The request must not fail as a whole, which
/// closes the connection.
#[tokio::test]
async fn an_unreadable_partition_fails_alone() {
    let (broker, dir) = start().await;
    let topic = "unreadable-and-healthy";
    let group = "g-unreadable-and-healthy";
    let topic_id = create_topic(&broker, topic, 2).await;
    initialize_share_state(&broker, group, topic_uuid(topic_id), 0).await;
    initialize_share_state(&broker, group, topic_uuid(topic_id), 1).await;
    let opened = share_fetch_rows(&broker, group, 0, topic_id, &[0, 1]).await;
    assert!(
        opened.iter().all(|row| row.error_code == codes::NONE),
        "{opened:?}"
    );
    produce_records(&broker, topic, 0, 5).await;
    produce_records(&broker, topic, 1, 5).await;

    // Give the only batch of partition 0 a length of zero, which is shorter
    // than a batch header: bytes 8 to 11 of a record batch are its
    // `batch_length`.
    let segment = std::fs::read_dir(dir.path().join(format!("{topic}-0")))
        .expect("the partition directory")
        .map(|entry| entry.expect("a directory entry").path())
        .find(|path| path.extension().is_some_and(|ext| ext == "log"))
        .expect("a segment file");
    let mut bytes = std::fs::read(&segment).expect("read the segment");
    bytes[8..12].copy_from_slice(&0_i32.to_be_bytes());
    std::fs::write(&segment, bytes).expect("corrupt the segment");

    let rows = share_fetch_rows(&broker, group, 1, topic_id, &[0, 1]).await;
    let mut by_partition: Vec<PartitionOutcome> = rows
        .iter()
        .map(|row| (row.partition_index, row.error_code, acquired(row)))
        .collect();
    by_partition.sort_unstable();

    assert!(
        by_partition
            == vec![
                (0, codes::CORRUPT_MESSAGE, vec![]),
                (1, codes::NONE, vec![(0, 4)]),
            ]
    );
    broker.shutdown().await;
}

/// A second, healthy partition in the same `ShareFetch` request must still
/// get its records when the first partition's log start has moved past its
/// SPSO: the recovery is per-partition, not per-request.
#[tokio::test]
async fn a_healthy_partition_in_the_same_request_is_unaffected() {
    let (broker, _dir) = start().await;
    let topic = "lockout-and-healthy";
    let group = "g-lockout-and-healthy";
    let topic_id = create_topic(&broker, topic, 2).await;
    initialize_share_state(&broker, group, topic_uuid(topic_id), 0).await;
    initialize_share_state(&broker, group, topic_uuid(topic_id), 1).await;

    // Prime both leader cells at SPSO 0 before any record exists. Both
    // partitions ride the same share session, so this is one request rather
    // than two: a second `share_session_epoch: 0` would reopen the session
    // and drop the first partition's subscription.
    let opened = share_fetch_rows(&broker, group, 0, topic_id, &[0, 1]).await;
    assert!(
        opened.iter().all(|row| row.error_code == codes::NONE),
        "{opened:?}"
    );
    produce_records(&broker, topic, 0, 5).await;
    produce_records(&broker, topic, 1, 5).await;

    // Only partition 0's log start moves past its SPSO.
    let delete_error = delete_records(&broker, topic, 0, 3).await;
    assert!(delete_error == codes::NONE, "{delete_error}");

    let rows = share_fetch_rows(&broker, group, 1, topic_id, &[0, 1]).await;
    // The partitions rotate by session epoch, so the rows come in either
    // order.
    let mut by_partition: Vec<PartitionOutcome> = rows
        .iter()
        .map(|row| (row.partition_index, row.error_code, acquired(row)))
        .collect();
    by_partition.sort_unstable();

    assert!(
        by_partition
            == vec![
                (0, codes::NONE, vec![(3, 4)]),
                (1, codes::NONE, vec![(0, 4)]),
            ]
    );
    broker.shutdown().await;
}

/// Kafka's `SharePartition` stops taking records once the window from the SPSO
/// to the SPEO reaches `group.share.partition.max.record.locks`, until an
/// acknowledgement, a release or a lock timeout moves the SPSO. A member that
/// fetches again without acknowledging must not get another limit's worth of
/// records each time.
#[tokio::test]
async fn a_member_that_does_not_acknowledge_gets_no_more_than_the_record_lock_limit() {
    let (broker, _dir) = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_group.max_inflight_records = 100;
    })
    .await;
    let topic = "record-lock-limit";
    let group = "g-record-lock-limit";
    let topic_id = create_topic(&broker, topic, 1).await;
    initialize_share_state(&broker, group, topic_uuid(topic_id), 0).await;
    let opened = share_fetch_one(&broker, group, 0, topic_id, 0).await;
    assert!(opened.error_code == codes::NONE, "{opened:?}");
    produce_records(&broker, topic, 0, 250).await;

    let mut per_fetch = Vec::new();
    for epoch in 1..=3 {
        let row = share_fetch_one(&broker, group, epoch, topic_id, 0).await;
        per_fetch.push((row.error_code, acquired(&row)));
    }
    let end_offset = broker
        .broker_arc_for_test()
        .share_partition_leaders
        .peek_for_test(group, topic_uuid(topic_id), 0)
        .expect("a cached share-partition leader")
        .lock()
        .await
        .end_offset
        .0;

    assert!(
        (per_fetch, end_offset)
            == (
                vec![
                    (codes::NONE, vec![(0, 99)]),
                    (codes::NONE, vec![]),
                    (codes::NONE, vec![]),
                ],
                100
            )
    );
    broker.shutdown().await;
}
