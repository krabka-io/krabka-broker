//! Handler tests for the order in which an incremental fetch session reads
//! its partitions.
//!
//! The leader spends a fetch's response budget on the partitions in the order
//! it reads them. Kafka's `IncrementalFetchContext` moves a partition that
//! returned records to the end of the session, so the next fetch reads the
//! other partitions first. Without the move, a partition that always has a
//! full batch waiting takes every response of the session, and the partitions
//! behind it get none until it runs dry. A follower that shares its fetcher
//! between a busy partition and `__consumer_offsets` then falls behind on the
//! offsets partition, and the group coordinator's write times out waiting for
//! the high watermark (issue #1352).

use {
    super::super::test_support::fetch_wire,
    crate::{
        fetch_session::INITIAL_EPOCH,
        handlers::test_support::{create_topic, produce_records, start_broker},
    },
    krabka_protocol::owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
    },
};

/// The last `Fetch` version that names topics, which keeps the request rows
/// readable.
const VERSION: i16 = 12;

/// The records in each batch that `produce_records` appends.
const BATCH_RECORDS: i64 = 3;

/// The incremental fetches of a case, after the full fetch that opens the
/// session.
const INCREMENTAL_FETCHES: i32 = 4;

/// A topic partition.
type Row = (&'static str, i32);

/// A partition that sent records, and the offset it sent them from.
type Served = (&'static str, i32, i64);

/// A fetch of `rows` that asks for one byte across the response. One byte fits
/// no batch, so the first partition read takes one batch under Kafka's
/// progress exception and leaves no budget for the rest. Consecutive rows of
/// one topic share a `FetchTopic`, in the order given.
fn fetch(session_id: i32, session_epoch: i32, rows: &[(Row, i64)]) -> FetchRequest {
    let mut topics: Vec<FetchTopic> = Vec::new();
    for &((topic, partition), fetch_offset) in rows {
        let row = FetchPartition {
            partition,
            fetch_offset,
            current_leader_epoch: -1,
            last_fetched_epoch: -1,
            log_start_offset: -1,
            partition_max_bytes: 1_048_576,
            ..FetchPartition::default()
        };
        match topics.last_mut() {
            Some(last) if last.topic == topic => last.partitions.push(row),
            _ => topics.push(FetchTopic {
                topic: topic.to_owned(),
                partitions: vec![row],
                ..FetchTopic::default()
            }),
        }
    }
    FetchRequest {
        replica_id: -1,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: 1,
        session_id,
        session_epoch,
        topics,
        ..FetchRequest::default()
    }
}

/// The rows whose records were not empty.
fn served(response: &FetchResponse, rows: &[Row]) -> Vec<Row> {
    response
        .responses
        .iter()
        .flat_map(|topic| {
            topic
                .partitions
                .iter()
                .filter(|row| {
                    row.records
                        .as_ref()
                        .is_some_and(|records| records.payload_len() > 0)
                })
                .map(|row| {
                    *rows
                        .iter()
                        .find(|&&(name, partition)| {
                            name == topic.topic && partition == row.partition_index
                        })
                        .expect("a requested partition")
                })
        })
        .collect()
}

/// One case: the partitions in the order the full fetch names them, with the
/// batches each holds, and what each fetch serves.
struct Case {
    partitions: &'static [(Row, usize)],
    /// The partition served by the full fetch, then by each incremental fetch.
    expected: [Served; 5],
}

/// A consumer opens a session and then fetches incrementally, moving the
/// fetch offset of each partition that sent a batch, as a Kafka client does.
/// Each response has room for one batch.
///
/// Kafka's `FullFetchContext` caches the session in the order the full fetch
/// read it, and moves nothing. Each incremental fetch then moves the partition
/// that sent its batch to the end, and reads the session in its order, so a
/// partition with a batch waiting is never passed over twice in a row.
///
/// - `two_partitions`: `[0, 1]` serves 0 at offset 3, leaving `[1, 0]`; 1 at
///   offset 0, leaving `[0, 1]`; and so on.
/// - `idle_partition_between`: `orders-1` never has records. After `orders-0`
///   sends at offset 3 the session is `[orders-1, audit-0, orders-0]`, and
///   `audit-0` is read before `orders-0`. Reading `orders` as one topic would
///   put `orders-0` back ahead of `audit-0` on every fetch.
#[yare::parameterized(
    two_partitions = { Case {
        partitions: &[(("orders", 0), 4), (("orders", 1), 4)],
        expected: [
            ("orders", 0, 0),
            ("orders", 0, 3),
            ("orders", 1, 0),
            ("orders", 0, 6),
            ("orders", 1, 3),
        ],
    } },
    idle_partition_between = { Case {
        partitions: &[(("orders", 0), 4), (("orders", 1), 0), (("audit", 0), 4)],
        expected: [
            ("orders", 0, 0),
            ("orders", 0, 3),
            ("audit", 0, 0),
            ("orders", 0, 6),
            ("audit", 0, 3),
        ],
    } },
)]
#[test_macro(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn an_incremental_session_serves_its_partitions_in_turn(case: Case) {
    let started = start_broker().await;
    let broker = &started.broker;
    let mut topics: Vec<(&str, i32)> = Vec::new();
    for &((topic, _), _) in case.partitions {
        match topics.iter_mut().find(|(name, _)| *name == topic) {
            Some((_, count)) => *count += 1,
            None => topics.push((topic, 1)),
        }
    }
    for (topic, partitions) in topics {
        create_topic()
            .broker(broker)
            .name(topic)
            .partitions(partitions)
            .call()
            .await;
    }
    for &((topic, partition), batches) in case.partitions {
        for _ in 0..batches {
            produce_records()
                .broker(broker)
                .topic(topic)
                .partition_index(partition)
                .call()
                .await;
        }
    }

    let rows: Vec<Row> = case.partitions.iter().map(|&(row, _)| row).collect();
    let mut offsets: Vec<(Row, i64)> = rows.iter().map(|&row| (row, 0)).collect();
    let mut changed = offsets.clone();
    let mut session_id = 0;
    let mut rounds: Vec<Vec<Served>> = Vec::new();
    for session_epoch in INITIAL_EPOCH..=INCREMENTAL_FETCHES {
        let response = fetch_wire()
            .broker(broker)
            .request(&fetch(session_id, session_epoch, &changed))
            .version(VERSION)
            .call()
            .await;
        assert2::assert!(response.error_code == 0, "{response:?}");
        if session_epoch == INITIAL_EPOCH {
            session_id = response.session_id;
            assert2::assert!(session_id != 0, "the full fetch opened a session");
        }
        changed = Vec::new();
        let mut round = Vec::new();
        for row in served(&response, &rows) {
            let (_, offset) = offsets
                .iter_mut()
                .find(|(cached, _)| *cached == row)
                .expect("a fetched partition");
            round.push((row.0, row.1, *offset));
            *offset += BATCH_RECORDS;
            changed.push((row, *offset));
        }
        rounds.push(round);
    }

    let expected: Vec<Vec<Served>> = case.expected.iter().map(|&row| vec![row]).collect();
    assert2::assert!(rounds == expected);
}
