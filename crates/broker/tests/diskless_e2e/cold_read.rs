//! Flush, trim, then read a trimmed offset back out of the object store.
//!
//! The WAL quorum keeps a committed prefix on local disks only until the
//! flusher has moved it into the object store and the index record naming it
//! has been committed and projected. After that the local logs are trimmed and
//! the only copy of those offsets is the flushed object.
//!
//! What makes this a real cold read, and not an accidental local hit, is the
//! assertion that the partition's log start has actually passed the offset
//! being fetched. Below the log start there is nothing left to serve locally,
//! so `OFFSET_OUT_OF_RANGE` is the only answer the log path can give and the
//! object store is the only thing that can rescue it.

use std::time::Duration;

use assert2::assert;

use crate::{
    CLIENT_PRINCIPAL, PASSWORD, RECORDS, TOPIC,
    cluster::start_diskless_cluster,
    support,
    topic::{await_wal_quorum, create_diskless_topic},
    wire::{assert_matches_produced, fetch_log, produce_all, value_at},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trimmed_diskless_offsets_are_served_from_the_object_store() {
    // Flush often, and keep no safety lag behind the committed index frontier,
    // so the trim reaches every offset the flush covered.
    let cluster = start_diskless_cluster(|config| {
        config.diskless_wal_flush_interval = krabka_units::millis(100);
        config.diskless_wal_trim_safety_lag = 0;
    })
    .await;
    cluster.await_ready().await;

    let admin = support::sasl_client(
        &cluster.bootstrap_for_node(cluster.node_ids()[0]),
        CLIENT_PRINCIPAL,
        PASSWORD,
    )
    .await;
    let topic_id = create_diskless_topic(&admin).await;
    let leader = await_wal_quorum(&cluster).await;

    let values: Vec<bytes::Bytes> = (0..RECORDS).map(value_at).collect();
    let producer = support::sasl_client(
        &cluster.bootstrap_for_node(leader),
        CLIENT_PRINCIPAL,
        PASSWORD,
    )
    .await;
    produce_all(&producer, topic_id, &values).await;

    // Wait for the flusher to publish an index record covering the whole
    // committed prefix and for the trim behind it to move the log start off
    // zero.
    let leader_broker = cluster
        .handle_for_node(leader)
        .expect("the diskless leader is up");
    let committed = i64::try_from(RECORDS).expect("small count");
    let (log_start, high_watermark, frontier) = await_trimmed_flush(leader_broker, committed).await;
    assert!(frontier == Some(committed));
    assert!(high_watermark == committed);
    assert!(
        log_start > 0,
        "offset 0 is still on local disk, so a fetch of it would not be a cold read"
    );

    // Offset 0 is now below the partition's log start. Only the flushed object
    // can answer for it.
    let cold = fetch_log(
        &cluster.bootstrap_for_node(leader),
        topic_id,
        0,
        RECORDS,
        Duration::from_secs(90),
    )
    .await;
    assert_matches_produced(&cold, 0, RECORDS);

    drop(producer);
    drop(admin);
    cluster.shutdown().await;
}

/// Poll the leader's own flush inputs until the committed prefix is both
/// flushed and trimmed, and return `(log start, high watermark, projected
/// flush frontier)`.
///
/// `diskless_flush_state_for_test` reports the same values the flusher itself
/// reads, so this waits on the real flush pipeline rather than on a proxy for
/// it. The `diskless` flag is asserted here because every other assertion in
/// this case would also hold for an ordinary topic that simply never trimmed.
async fn await_trimmed_flush(
    leader: &krabka_broker::BrokerHandle,
    committed: i64,
) -> (i64, i64, Option<i64>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let state = leader.diskless_flush_state_for_test(TOPIC, 0).await;
        if let Some((diskless, _, log_start, _, high_watermark, frontier)) = state {
            assert!(
                diskless,
                "the topic must be diskless for this case to mean anything"
            );
            if log_start > 0 && frontier == Some(committed) {
                return (log_start, high_watermark, frontier);
            }
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "the diskless flusher did not flush and trim the committed prefix; last state \
             was {state:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
