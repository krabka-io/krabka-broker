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
#![cfg(test)]

use {
    crate::support::{
        fetch::{FetchLimits, RequestWaitMillis, single_partition_fetch},
        produce::{SinglePartitionProduceSetup, send_batch},
        records::{batch_from_records, value_record},
    },
    assert2::check,
    bytes::Bytes,
    krabka_protocol::{
        owned::create_topics_request::{CreatableTopic, CreateTopicsRequest},
        records::RecordBatch,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
    std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
    },
    tokio::runtime::Builder,
};

mod support;

const TOPIC: &str = "inline-blocking";

/// One batch of `values`, one record each.
fn batch_of(values: &[&str]) -> RecordBatch {
    let count = i32::try_from(values.len()).expect("a small batch");
    RecordBatch {
        last_offset_delta: count - 1,
        ..batch_from_records(
            values
                .iter()
                .zip(0..)
                .map(|(value, offset_delta)| {
                    value_record(offset_delta, Some(Bytes::from(value.to_string())))
                })
                .collect(),
        )
    }
}

/// Create the topic, produce three records to it and fetch them back.
/// Returns the three RPC error codes and how many records the fetch served.
struct ProtocolRun {
    served: usize,
    reply_codes: [i16; 3],
}

async fn produce_and_fetch(client: &krabka_client_core::Client) -> ProtocolRun {
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
    let topic_id = support::topic_id_for(client, TOPIC).await;

    let produced = send_batch(
        &client,
        batch_of(&["a", "b", "c"]),
        SinglePartitionProduceSetup {
            topic: (TOPIC).into(),
            topic_id,
            ..SinglePartitionProduceSetup::replicated()
        },
    )
    .await;

    let fetched = client
        .send(
            single_partition_fetch()
                .topic(TOPIC.into())
                .topic_id(topic_id)
                .limits(FetchLimits::one_mebibyte(RequestWaitMillis(100)))
                .call(),
        )
        .await
        .expect("Fetch");
    let partition = &fetched.responses[0].partitions[0];
    let mut total_records = 0;
    if let Some(v2_batches) = partition.records.as_ref().and_then(|r| r.as_v2()) {
        for b in v2_batches {
            total_records += b.records.len();
        }
    }
    ProtocolRun {
        served: total_records,
        reply_codes: [
            created.topics[0].error_code,
            produced.responses[0].partition_responses[0].error_code,
            partition.error_code,
        ],
    }
}

/// The records a fetch served and the threads the runtime started.
struct NodeRun {
    protocol: ProtocolRun,
    started: usize,
}

/// Boot a node on a current-thread runtime of its own thread, run
/// [`produce_and_fetch`] against it and shut it down, with the blocking work
/// inline when `inline` holds. Returns the records the fetch served and the
/// threads the runtime started.
fn run_node(inline: bool) -> NodeRun {
    let started = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&started);
    let served = thread::spawn(move || {
        let runtime = Builder::new_current_thread()
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
    NodeRun {
        protocol: served,
        started: started.load(Ordering::SeqCst),
    }
}

struct BlockingScenario {
    inline: Option<bool>,
    observed: Option<NodeRun>,
}

#[fixture]
fn blocking() -> BlockingScenario {
    // Start this scenario with fresh inputs and observations.

    BlockingScenario {
        inline: None,
        observed: None,
    }
}

#[given("blocking work uses {mode}")]
fn blocking_executor(blocking: &mut BlockingScenario, mode: String) {
    blocking.inline = Some(match mode.as_str() {
        "the runtime thread" => true,
        "the blocking pool" => false,
        _ => panic!("unknown blocking executor: {mode}"),
    });
}

#[when("a current-thread broker creates produces and fetches records")]
fn exercise_runtime(blocking: &mut BlockingScenario) {
    blocking.observed = Some(run_node(
        blocking.inline.expect("a blocking executor was selected"),
    ));
}

#[then("the broker serves three records and starts {threads} extra threads")]
fn runtime_observations(blocking: &BlockingScenario, threads: String) {
    let run = blocking
        .observed
        .as_ref()
        .expect("the current-thread broker was exercised");
    check!(
        run.protocol.reply_codes == [0, 0, 0],
        "create, produce and fetch must all succeed"
    );
    check!(run.protocol.served == 3);
    match threads.as_str() {
        "no" => {
            check!(
                run.started == 0,
                "the runtime started {} extra threads",
                run.started
            );
        }
        "some" => {
            check!(run.started > 0, "the blocking pool should start a worker");
        }
        _ => panic!("unknown thread expectation: {threads}"),
    }
}

scenarios!("tests/features/migrated/tests/inline_blocking.feature", fixtures = [blocking: BlockingScenario]);
