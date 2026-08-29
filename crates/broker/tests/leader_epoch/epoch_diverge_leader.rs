//! The leader side of KIP-320: a follower `Fetch` that advertises a
//! `last_fetched_epoch` whose epoch ended before the requested `fetch_offset`
//! is answered with a `diverging_epoch` and no records.
//!
//! The leader-side answer is computed from the epoch cache alone, so this test
//! needs only the single-broker fixture. The follower that acts on the answer
//! is covered in `epoch_diverge_follower`.

use assert2::{assert, check};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
};

use crate::epoch_harness::{boot_single, create_topic, record, topic_id_for};

/// KIP-320 leader side. A follower-style Fetch, with `replica_id >= 0`, that
/// advertises a stale `last_fetched_epoch` whose epoch ends *before* the
/// requested `fetch_offset` must get a `diverging_epoch` that points at the
/// epoch boundary, and NO records.
///
/// The test builds the leader's epoch history deterministically:
///   * produce `k = 2` records at epoch 0, which gives checkpoint `0 -> 0`,
///   * bump the leader epoch to 1, with the split-brain shim that the fence
///     test uses, then produce 2 more, which gives checkpoint `1 -> 2`.
///
/// The cache is then `e0 -> [0, 2)` and `e1 -> [2, 4)`, and the log end is 4.
///
/// A follower fetch at `fetch_offset = 4` with `last_fetched_epoch = 0` says
/// "my last record was in epoch 0, give me offset 4". That is divergent,
/// because epoch 0 on this leader ends at offset 2, not 4. The leader's
/// `epoch_and_offset_for(0, 4)` returns `(0, 2)`. The recorded end, 2, is below
/// the fetch offset, 4, so the handler answers with
/// `diverging_epoch { epoch: 0, end_offset: 2 }` and serves nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diverging_epoch_returned_on_stale_last_fetched_epoch() {
    let (broker, bootstrap, _dir) = boot_single().await;
    create_topic(&broker, &bootstrap, "diverge").await;

    let client = Client::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .unwrap();
    let topic_id = topic_id_for(&client, "diverge").await;

    // Helper: produce one single-record batch, stamped with whatever leader
    // epoch the partition currently holds.
    let produce_one = |value: &'static str| {
        let client = &client;
        async move {
            client
                .send(ProduceRequest {
                    acks: 1,
                    timeout_ms: 5_000,
                    topic_data: vec![TopicProduceData {
                        name: "diverge".into(),
                        topic_id,
                        partition_data: vec![PartitionProduceData {
                            index: 0,
                            records: Some(record(value).into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .await
                .expect("produce");
        }
    };

    // Epoch 0: produce k = 2 records → checkpoint row `0 0`, LEO = 2.
    let e0: i32 = 0;
    let k: i64 = 2;
    produce_one("e0-a").await;
    produce_one("e0-b").await;

    // Bump leader epoch to 1, produce 2 more → checkpoint row `1 2`, LEO = 4.
    broker.test_set_leader_epoch("diverge", 0, 1);
    produce_one("e1-a").await;
    produce_one("e1-b").await;
    let n: i64 = 4;

    // Sanity: the leader really advanced to LEO == n.
    let leo = broker
        .local_log_end_offset("diverge", 0)
        .expect("local leo");
    assert!(leo == n, "expected leader LEO == {n}, got {leo}");

    // Follower Fetch at offset n claiming last_fetched_epoch == e0. Leave
    // `current_leader_epoch` at its -1 default so we don't trip the KIP-101
    // fence and actually reach the KIP-320 divergence check.
    let resp = client
        .send(FetchRequest {
            replica_id: 7,
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: "diverge".into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: n,
                    last_fetched_epoch: e0,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("fetch");

    let part = &resp.responses[0].partitions[0];
    // NONE error code: divergence is reported in-band, not as an error.
    check!(
        part.error_code == 0,
        "expected NONE, got {}",
        part.error_code
    );
    check!(
        part.diverging_epoch.end_offset == k,
        "diverging_epoch.end_offset should be the epoch-0 boundary {k}, got {}",
        part.diverging_epoch.end_offset
    );
    check!(
        part.diverging_epoch.epoch == e0,
        "diverging_epoch.epoch should be {e0}, got {}",
        part.diverging_epoch.epoch
    );
    // No records are served alongside a divergence signal.
    check!(
        part.records.is_none()
            || part
                .records
                .as_ref()
                .and_then(|r| r.as_v2())
                .is_none_or(<[_]>::is_empty),
        "diverging fetch must serve no records"
    );

    broker.shutdown().await;
}
