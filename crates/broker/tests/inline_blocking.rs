//! The broker with its blocking work inline, the way `wasm32-wasip1` runs it,
//! on a native target.
//!
//! `wasm32-wasip1` has no threads, so there the broker runs on one
//! current-thread runtime and does its blocking work -- the log appends and
//! reads, the fsyncs, the internal-topic replays -- on that runtime's own
//! thread. This suite gets the same arrangement from a native thread: that
//! thread holds `inline_blocking_on_this_thread` while it drives a
//! current-thread runtime, and the runtime counts every thread it starts. A
//! node boots, creates a topic, takes a produce, serves it back to a fetch and
//! shuts down without starting one. The control case, the same run without
//! the guard, shows that the count does see the blocking pool.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use assert2::{assert, check};
use bytes::Bytes;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    records::{Record, RecordBatch},
};

mod support;

const TOPIC: &str = "inline-blocking";

/// One batch of `values`, one record each.
fn batch_of(values: &[&str]) -> RecordBatch {
    let count = i32::try_from(values.len()).expect("a small batch");
    RecordBatch {
        last_offset_delta: count - 1,
        records: values
            .iter()
            .zip(0..)
            .map(|(value, offset_delta)| Record {
                offset_delta,
                value: Some(Bytes::from(value.to_string())),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
}

/// Create the topic, produce three records to it and fetch them back.
/// Returns how many records the fetch served.
async fn produce_and_fetch(client: &krabka_client_core::Client) -> usize {
    let created = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: TOPIC.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..CreatableTopic::default()
            }],
            timeout_ms: 5_000,
            ..CreateTopicsRequest::default()
        })
        .await
        .expect("CreateTopics");
    assert!(created.topics[0].error_code == 0);
    let topic_id = support::topic_id_for(client, TOPIC).await;

    let produced = client
        .send(ProduceRequest {
            acks: -1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: TOPIC.into(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch_of(&["a", "b", "c"]).into()),
                    ..PartitionProduceData::default()
                }],
                ..TopicProduceData::default()
            }],
            ..ProduceRequest::default()
        })
        .await
        .expect("Produce");
    assert!(produced.responses[0].partition_responses[0].error_code == 0);

    let fetched = client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: TOPIC.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("Fetch");
    let partition = &fetched.responses[0].partitions[0];
    assert!(partition.error_code == 0);
    partition
        .records
        .as_ref()
        .and_then(|records| records.as_v2())
        .map_or(0, |batches| {
            batches.iter().map(|batch| batch.records.len()).sum()
        })
}

/// Boot a node on a current-thread runtime of its own thread, run
/// [`produce_and_fetch`] against it and shut it down, with the blocking work
/// inline when `inline` holds. Returns the records the fetch served and the
/// threads the runtime started.
fn run_node(inline: bool) -> (usize, usize) {
    let started = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&started);
    let served = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .on_thread_start(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            })
            .build()
            .expect("build a current-thread runtime");
        // Every task of this runtime, the broker's included, runs on this
        // thread, so the guard covers all of them.
        let _inline = inline.then(krabka_broker::inline_blocking_on_this_thread);
        runtime.block_on(async {
            let node = support::start().await;
            let served = produce_and_fetch(&node.client).await;
            node.broker.shutdown().await;
            served
        })
    })
    .join()
    .expect("the node's thread");
    (served, started.load(Ordering::SeqCst))
}

#[test]
fn a_node_with_its_blocking_work_inline_starts_no_thread() {
    // (inline, whether the runtime starts a thread)
    let cases = [(true, false), (false, true)];
    for (inline, starts_a_thread) in cases {
        let (served, started) = run_node(inline);
        check!(served == 3, "inline={inline}");
        check!(
            (started > 0) == starts_a_thread,
            "inline={inline}: the runtime started {started} thread(s)"
        );
    }
}
