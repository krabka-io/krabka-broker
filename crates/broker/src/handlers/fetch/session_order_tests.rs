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
    assert2::assert,
    krabka_protocol::owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
    },
    std::collections::BTreeMap,
};

const TOPIC: &str = "orders";

/// The last `Fetch` version that names topics, which keeps the request rows
/// readable.
const VERSION: i16 = 12;

/// The records in each batch that `produce_records` appends.
const BATCH_RECORDS: i64 = 3;

/// The batches appended to each partition: more than the fetches of a case
/// read from it, so every partition always has a batch waiting.
const BATCHES: usize = 4;

/// A fetch that reads one partition and asks for `max_bytes` across the
/// response. One byte fits no batch, so the first partition read takes one
/// batch under Kafka's progress exception and leaves no budget for the rest.
fn fetch(session_id: i32, session_epoch: i32, rows: &BTreeMap<i32, i64>) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms: 0,
        min_bytes: 0,
        max_bytes: 1,
        session_id,
        session_epoch,
        topics: if rows.is_empty() {
            Vec::new()
        } else {
            vec![FetchTopic {
                topic: TOPIC.to_owned(),
                partitions: rows
                    .iter()
                    .map(|(&partition, &fetch_offset)| FetchPartition {
                        partition,
                        fetch_offset,
                        current_leader_epoch: -1,
                        last_fetched_epoch: -1,
                        log_start_offset: -1,
                        partition_max_bytes: 1_048_576,
                        ..FetchPartition::default()
                    })
                    .collect(),
                ..FetchTopic::default()
            }]
        },
        ..FetchRequest::default()
    }
}

/// The partitions whose rows carried records.
fn served(response: &FetchResponse) -> Vec<i32> {
    response
        .responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .filter(|row| {
            row.records
                .as_ref()
                .is_some_and(|records| records.payload_len() > 0)
        })
        .map(|row| row.partition_index)
        .collect()
}

/// A consumer opens a session over two partitions that both always have a
/// batch waiting, and then fetches incrementally, moving the fetch offset of
/// each partition that sent a batch, as a Kafka client does. Each response has
/// room for one batch.
///
/// Kafka's `FullFetchContext` caches the session in the order the full fetch
/// read it, `[0, 1]`, and moves nothing. From then on each incremental fetch
/// moves the partition that sent its batch to the end: partition 0 sends at
/// offset 3 and leaves `[1, 0]`, partition 1 sends at offset 0 and leaves
/// `[0, 1]`, and so on. Neither partition gets two incremental responses in a
/// row while the other has a batch waiting.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incremental_session_serves_its_partitions_in_turn() {
    let started = start_broker().await;
    let broker = &started.broker;
    create_topic()
        .broker(broker)
        .name(TOPIC)
        .partitions(2)
        .call()
        .await;
    for partition in [0, 1] {
        for _ in 0..BATCHES {
            produce_records()
                .broker(broker)
                .topic(TOPIC)
                .partition_index(partition)
                .call()
                .await;
        }
    }

    let mut offsets: BTreeMap<i32, i64> = [(0, 0), (1, 0)].into_iter().collect();
    // Each round: (the partition that sent records, its fetch offset).
    let mut rounds: Vec<Vec<(i32, i64)>> = Vec::new();
    let mut changed = offsets.clone();
    let mut session_id = 0;
    for session_epoch in INITIAL_EPOCH..5 {
        let response = fetch_wire()
            .broker(broker)
            .request(&fetch(session_id, session_epoch, &changed))
            .version(VERSION)
            .call()
            .await;
        assert!(response.error_code == 0, "{response:?}");
        if session_epoch == INITIAL_EPOCH {
            session_id = response.session_id;
            assert!(session_id != 0, "the full fetch opened a session");
        }
        let round: Vec<(i32, i64)> = served(&response)
            .into_iter()
            .map(|partition| (partition, offsets[&partition]))
            .collect();
        changed = round
            .iter()
            .map(|&(partition, offset)| (partition, offset + BATCH_RECORDS))
            .collect();
        offsets.extend(&changed);
        rounds.push(round);
    }

    assert!(
        rounds
            == vec![
                vec![(0, 0)],
                vec![(0, 3)],
                vec![(1, 0)],
                vec![(0, 6)],
                vec![(1, 3)]
            ]
    );
}
