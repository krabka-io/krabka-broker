//! Increment D end-to-end validation of a large-message fetch.
//!
//! The test produces a records run larger than 64 KiB. It then consumes the
//! run over the real loopback TCP socket and asserts that the record values
//! round-trip **byte-for-byte**, and that the broker drained them on the path
//! this target is supposed to take.
//!
//! On Linux this fetch is far above the `sendfile_min` threshold on a
//! plaintext `TcpStream`, so the kernel sends the records region through the
//! `sendfile(2)` zero-copy path. The consumer's CRC check and the value
//! comparison below fail if sendfile sends the wrong file range, or if a
//! partial-write loop bug drops or duplicates bytes. On Windows and on TLS
//! the same test exercises the portable vectored Increment C fallback. The
//! wire bytes are identical either way, so those assertions hold on every
//! platform — which is exactly why they cannot tell the paths apart. The
//! `fetch_response_drain_total` assertion can, and it is the one that fails if
//! a regression quietly routes every plaintext fetch onto the copy path.
#![cfg(test)]

use {
    crate::support::{
        client::create_topic,
        fetch::{
            FetchByteLimit, FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch,
        },
        produce::{SinglePartitionProduceSetup, send_batch},
        records::value_record,
    },
    assert2::assert,
    krabka_protocol::records::RecordsPayload,
};
mod support;

use {
    bytes::Bytes,
    krabka_broker::metrics::{BrokerMetrics, FetchDrainPath, FetchDrainPathLabel},
    krabka_protocol::{
        owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
        records::{Record, RecordBatch},
    },
    support::topic_id_for,
};

/// A batch of large records, and the values it carries.
struct LargeRecords {
    batch: RecordBatch,
    expected: Vec<Bytes>,
}

/// Build `n` records whose values are distinct, large, and content-addressed
/// by index. Any misplaced byte is then detectable.
fn large_records(n: i32, value_len: usize) -> LargeRecords {
    let mut batch = RecordBatch {
        last_offset_delta: (n - 1).max(0),
        ..RecordBatch::default()
    };
    let mut expected = Vec::with_capacity(usize::try_from(n.max(0)).unwrap_or(0));
    for i in 0..n {
        // Fill each value with a per-record byte pattern so a swapped/duplicated
        // range is caught, not just a length mismatch.
        let mut v = vec![0u8; value_len];
        let tag = (u8::try_from(i & 0xff).unwrap_or(0))
            .wrapping_mul(31)
            .wrapping_add(7);
        for (j, b) in v.iter_mut().enumerate() {
            *b = tag ^ u8::try_from(j & 0xff).unwrap_or(0);
        }
        let value = Bytes::from(v);
        expected.push(value.clone());
        batch.records.push(Record {
            key: Some(Bytes::from(format!("key-{i}"))),
            ..value_record(i, Some(value))
        });
    }
    LargeRecords { batch, expected }
}

/// The path this target must drain a large plaintext fetch on.
///
/// A platform with a file-to-socket `sendfile(2)` and a plaintext listener
/// takes the kernel path for a run this far above `sendfile_min`. Windows has
/// no such call, so its fetch is `vectored` — and neither target may reach
/// `pread`, which is the drain's own fallback for a stream that promises a
/// socket and then withholds it.
#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
))]
const EXPECTED_DRAIN_PATH: FetchDrainPath = FetchDrainPath::Sendfile;
#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
)))]
const EXPECTED_DRAIN_PATH: FetchDrainPath = FetchDrainPath::Vectored;

/// The drain counts of all three paths, in `FetchDrainPath::ALL` order.
fn drain_counts(metrics: &BrokerMetrics) -> [u64; 3] {
    FetchDrainPath::ALL.map(|path| {
        metrics
            .fetch_response_drain
            .get_or_create(&FetchDrainPathLabel { path })
            .get()
    })
}

/// The counts one drain on `path` adds, as a whole three-path split.
fn one_drain_on(path: FetchDrainPath) -> [u64; 3] {
    FetchDrainPath::ALL.map(|p| u64::from(p == path))
}

#[derive(Default)]
struct LargeFetch {
    broker: Option<support::InProcess>,
    topic: String,
    batch: Option<RecordBatch>,
    expected: Vec<Bytes>,
    produced: Option<i16>,
    response: Option<FetchResponse>,
    before: [u64; 3],
}

#[rstest::fixture]
fn large_fetch() -> LargeFetch {
    // Start this scenario with fresh inputs and observations.

    LargeFetch {
        broker: None,
        topic: String::new(),
        batch: None,
        expected: Vec::new(),
        produced: None,
        response: None,
        before: [0; 3],
    }
}

#[rstest_bdd_macros::given(
    "a plaintext topic {topic} and {count} distinct records of {value_len} bytes"
)]
async fn large_payload(large_fetch: &mut LargeFetch, topic: String, count: i32, value_len: usize) {
    let broker = support::start().await;
    create_topic(&broker.client, &topic, 1).await;
    let LargeRecords { batch, expected } = large_records(count, value_len);
    large_fetch.topic = topic;
    large_fetch.batch = Some(batch);
    large_fetch.expected = expected;
    large_fetch.broker = Some(broker);
}

#[rstest_bdd_macros::when(
    "the records are produced and fetched with byte budget {budget} and wait {wait} ms"
)]
async fn fetch_large(large_fetch: &mut LargeFetch, budget: i32, wait: i32) {
    let p = large_fetch.broker.as_ref().unwrap();
    let tid = topic_id_for(&p.client, &large_fetch.topic).await;
    let prod = send_batch(
        &p.client,
        large_fetch.batch.take().unwrap(),
        SinglePartitionProduceSetup {
            topic: large_fetch.topic.clone(),
            topic_id: tid,
            ..Default::default()
        },
    )
    .await;
    large_fetch.produced = Some(prod.responses[0].partition_responses[0].error_code);
    large_fetch.before = drain_counts(p.broker.metrics());
    large_fetch.response = Some(
        p.client
            .send(FetchRequest {
                session_id: 0,
                session_epoch: -1,
                ..single_partition_fetch()
                    .topic(large_fetch.topic.clone())
                    .topic_id(tid)
                    .partition(fetch_partition().maximum(budget).call())
                    .limits(FetchLimits::wait_for_data_with_maximum(
                        RequestWaitMillis(wait),
                        FetchByteLimit(budget),
                    ))
                    .call()
            })
            .await
            .expect("Fetch"),
    );
}

#[rstest_bdd_macros::then(
    "Produce and Fetch succeed and every record value matches its original bytes"
)]
fn exact_values(large_fetch: &LargeFetch) {
    assert!(large_fetch.produced == Some(0));
    let r = large_fetch.response.as_ref().unwrap();
    assert!(r.error_code == 0);
    assert!(r.responses.len() == 1);
    let batches = r.responses[0].partitions[0]
        .records
        .as_ref()
        .and_then(RecordsPayload::as_v2)
        .expect("v2 records decoded from the fetch response");
    let got_values: Vec<&Bytes> = batches
        .iter()
        .flat_map(|b| b.records.iter())
        .filter_map(|rec| rec.value.as_ref())
        .collect();
    let expected = &large_fetch.expected;
    assert!(
        got_values.len() == expected.len(),
        "expected {} records, got {}",
        expected.len(),
        got_values.len()
    );
    for (i, (got, want)) in got_values.iter().zip(expected.iter()).enumerate() {
        assert!(
            got.as_ref() == &want[..],
            "record {i} value mismatch (sendfile byte corruption?)"
        );
    }
}

#[rstest_bdd_macros::then(
    "exactly one response uses the platform plaintext drain path and no fallback path"
)]
fn drain_path(large_fetch: &LargeFetch) {
    let after = drain_counts(large_fetch.broker.as_ref().unwrap().broker.metrics());
    let before = large_fetch.before;
    let drained = [
        after[0] - before[0],
        after[1] - before[1],
        after[2] - before[2],
    ];
    assert!(
        drained == one_drain_on(EXPECTED_DRAIN_PATH),
        "the fetch must drain once on the {} path",
        EXPECTED_DRAIN_PATH.as_str()
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/fetch_large_message_sendfile.feature",
    name = "large message fetch round trips byte exact"
)]
#[tokio::test]
async fn large_message_fetch_round_trips_byte_exact(large_fetch: LargeFetch) {
    large_fetch.broker.unwrap().broker.shutdown().await;
}
