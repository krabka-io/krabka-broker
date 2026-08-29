//! The record bytes a `ShareFetch` returns for the offsets it acquired. An
//! acquired offset that begins a later on-disk batch must still carry its
//! payload, and a fetch whose acquired window is fragmented must carry every
//! acquired offset and nothing from the gap between them.

use std::time::Duration;

use assert2::assert;
use krabka_broker::Broker;
use krabka_client_core::Client;
use krabka_protocol::{
    owned::produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    records::{Record, RecordBatch},
};

use crate::{
    ACCEPT, NONE, REJECT, RELEASE,
    harness::{
        bootstrap_share_state, broker_config, broker_test_permit, connect, create_topic, join,
        produce_n, topic_id, wait_for_share_init, wire,
    },
    share_rpc::{acquired_count, fetch_until_acquired, share_ack, share_fetch},
};

/// Regression: a `ShareFetch` whose acquired offset begins a *later* record
/// batch must still return that offset's record bytes, not an empty payload.
/// The broker already consumed or archived a leading multi-record batch.
///
/// `ShareFetch.partition_max_bytes` is a v0-only field. At the supported
/// versions (v1+) it is absent and decodes to 0. The read path must not use
/// that 0 as the log-read byte budget. A 0 budget reads only one batch header,
/// which cannot skip the leading batch to reach the acquired offset. The broker
/// then returns the acquired record with no bytes, and the record stays locked.
/// The read must fall back to the request-level `max_bytes`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acquire_past_leading_batch_returns_bytes() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    // One 3-record batch at offsets 0..2.
    produce_n(&client, "t", tid, 0, 3).await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Acquire 0..2 and Reject them → archived, SPSO advances to 3.
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 3, "acquire all 3");
    let ack = share_ack(&client, &member, tid, 1, 0, 2, REJECT).await;
    assert!(ack.error_code == NONE, "reject error: {}", ack.error_code);

    // A separate single-record batch at offset 3 (this starts a new batch; the
    // acquired range 3..3 begins past the leading 0..2 batch).
    produce_n(&client, "t", tid, 0, 1).await;

    // Acquire offset 3 — the payload must carry the record bytes.
    let mut row3 = share_fetch(&client, "g1", &member, tid, 0, 2, 0).await;
    for epoch in 3..18 {
        if acquired_count(&row3) > 0 {
            break;
        }
        // intentional: bounded RPC poll — acquiring the freshly produced offset
        // 3 requires re-fetching; no image/metric signals when it becomes
        // acquirable.
        tokio::time::sleep(Duration::from_millis(100)).await;
        row3 = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
    }
    assert!(
        acquired_count(&row3) == 1,
        "offset 3 must be acquired, got {:?}",
        row3.acquired_records
    );
    assert!(
        row3.acquired_records[0].first_offset == 3,
        "acquired offset must be 3, got {:?}",
        row3.acquired_records
    );
    let batches = row3
        .records
        .as_ref()
        .and_then(|r| r.as_v2())
        .expect("acquired offset 3 must carry decodable v2 record bytes");
    let values: Vec<String> = batches
        .iter()
        .flat_map(|b| b.records.iter())
        .filter_map(|r| r.value.as_ref())
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .collect();
    assert!(
        values == vec!["v0"],
        "offset 3's record bytes must be returned, got {values:?}"
    );
}

/// Produce a single record carrying `value` into `(topic, partition)` as its OWN
/// batch, so each offset is a distinct on-disk batch. This matters for the
/// fragmented-window read test: the share-fetch read path reads verbatim bytes
/// at *batch* granularity, so each offset must be its own batch to surface
/// byte-exact disjoint offsets. This helper retries while the partition is still
/// materializing.
async fn produce_one(client: &Client, topic: &str, tid: uuid::Uuid, partition: i32, value: &str) {
    for _ in 0..40 {
        let resp = client
            .send(ProduceRequest {
                transactional_id: None,
                acks: -1,
                timeout_ms: 5_000,
                topic_data: vec![TopicProduceData {
                    name: topic.to_string(),
                    topic_id: wire(tid),
                    partition_data: vec![PartitionProduceData {
                        index: partition,
                        records: Some(
                            RecordBatch {
                                last_offset_delta: 0,
                                records: vec![Record {
                                    offset_delta: 0,
                                    value: Some(bytes::Bytes::copy_from_slice(value.as_bytes())),
                                    ..Default::default()
                                }],
                                ..Default::default()
                            }
                            .into(),
                        ),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("Produce");
        let p = &resp.responses[0].partition_responses[0];
        if p.error_code == 0 {
            return;
        }
        if p.error_code == 3 || p.error_code == 6 {
            // intentional: bounded produce-retry backoff while the partition
            // leader materializes; this helper has no BrokerHandle to await on.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        panic!("produce failed: {p:?}");
    }
    panic!("partition never became produceable for {topic}:{partition}");
}

/// F5 (fragmented window): a single share fetch that returns DISJOINT acquired
/// ranges must carry record bytes for exactly the acquired offsets. The gap
/// offset's value must not appear.
///
/// Scenario: produce 3 records as THREE separate single-record batches, so
/// offsets 0, 1, 2 are each their own on-disk batch. The share-fetch read is
/// batch-granular, so byte-exact disjoint reads need separate batches. Acquire
/// 0..2, then Accept the MIDDLE offset (1) only and Release the outer offsets 0
/// and 2. The SPSO stays at 0 because offset 0 is not accepted. The broker
/// acknowledges offset 1, and offsets 0, 2 return to Available, and that leaves
/// a gap at offset 1.
///
/// The re-fetch acquires the DISJOINT set {0, 2}. The read concatenates the
/// per-range bytes, so the payload decodes to exactly offsets {0, 2} (values
/// v0, v2), never the gap offset 1's value v1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_window_records_match_acquired_offsets() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, &format!("g1:{tid}:0")).await;
    // Three separate single-record batches: offset 0=v0, 1=v1, 2=v2.
    produce_one(&client, "t", tid, 0, "v0").await;
    produce_one(&client, "t", tid, 0, "v1").await;
    produce_one(&client, "t", tid, 0, "v2").await;
    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // Acquire 0..2 (epoch 0 opens; stored epoch is now 1).
    let row = fetch_until_acquired(&client, "g1", &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == 3, "acquire all 3 offsets");

    // Accept the MIDDLE offset (1) only; Release the outer offsets 0 and 2.
    // SPSO stays at 0 (offset 0 is not accepted), offset 1 becomes Acknowledged,
    // offsets 0 and 2 return to Available — a gap at offset 1 between them.
    let a1 = share_ack(&client, &member, tid, 1, 1, 1, ACCEPT).await;
    assert!(a1.error_code == NONE, "accept 1 error: {}", a1.error_code);
    let a0 = share_ack(&client, &member, tid, 2, 0, 0, RELEASE).await;
    assert!(a0.error_code == NONE, "release 0 error: {}", a0.error_code);
    let a2 = share_ack(&client, &member, tid, 3, 2, 2, RELEASE).await;
    assert!(a2.error_code == NONE, "release 2 error: {}", a2.error_code);

    // Re-fetch: the acquired set is the DISJOINT {0, 2} (offset 1 is gone). The
    // returned records payload must decode to exactly offsets {0, 2}.
    let mut row2 = share_fetch(&client, "g1", &member, tid, 0, 4, 0).await;
    for epoch in 5..20 {
        if acquired_count(&row2) >= 2 {
            break;
        }
        // intentional: bounded RPC poll — re-acquiring the released disjoint set
        // {0, 2} happens only via this ShareFetch; no image/metric reflects it.
        tokio::time::sleep(Duration::from_millis(100)).await;
        row2 = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
    }
    // The authoritative acquired offset set.
    let acquired_offsets: std::collections::BTreeSet<i64> = row2
        .acquired_records
        .iter()
        .flat_map(|r| r.first_offset..=r.last_offset)
        .collect();
    assert!(
        acquired_offsets == std::collections::BTreeSet::from([0, 2]),
        "must re-acquire the disjoint set {{0, 2}}, got {acquired_offsets:?}"
    );

    // Decode the records payload and collect the absolute offsets it carries.
    let batches = row2
        .records
        .as_ref()
        .and_then(|r| r.as_v2())
        .expect("disjoint acquired ranges must carry decodable v2 record bytes");
    let record_offsets: std::collections::BTreeSet<i64> = batches
        .iter()
        .flat_map(|b| {
            let base = b.base_offset;
            b.records
                .iter()
                .map(move |r| base + i64::from(r.offset_delta))
        })
        .collect();
    assert!(
        record_offsets == acquired_offsets,
        "records payload offsets {record_offsets:?} must equal acquired offsets \
         {acquired_offsets:?} (gap offset 1 must be excluded)"
    );
    // Belt-and-suspenders: the gap offset's value (v1) must never appear.
    let values: Vec<String> = batches
        .iter()
        .flat_map(|b| b.records.iter())
        .filter_map(|r| r.value.as_ref())
        .map(|v| String::from_utf8_lossy(v).into_owned())
        .collect();
    assert!(
        !values.contains(&"v1".to_string()),
        "the gap offset's value v1 must be excluded, got {values:?}"
    );
}
