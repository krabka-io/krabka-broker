//! The closed half of the role: a client `Produce` or consumer `Fetch` that
//! reaches the witness is refused with `NOT_LEADER_OR_FOLLOWER`, while the
//! witness's own follower fetch keeps replicating.
//!
//! The two halves have to be asserted together. A witness that refused
//! everything would be no ISR member at all, and it is the ISR seat that keeps
//! `acks=all` writable after a site loss, so the test checks the local log and
//! the ISR right after the refusals.

use std::collections::BTreeSet;

use assert2::check;
use krabka_broker::codes;

use crate::{
    N_RECORDS, SITE_C, TOPIC, cluster_lock, within,
    witness_cluster::{client_at, shutdown, start_stretch_cluster},
    witness_wire::{consumer_fetch, create_topic, produce_error},
};

/// A client `Produce` and a consumer `Fetch` that reach the witness are
/// refused, while replication to that same witness keeps advancing: its local
/// log reaches the produced offset and it stays in the ISR.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn witness_refuses_client_traffic_while_replication_advances() {
    let _guard = cluster_lock().lock().await;
    let cluster = start_stretch_cluster().await;

    let leader = client_at(&cluster[0].1.listen_addr.to_string()).await;
    let topic_id = create_topic(&leader).await;
    for (handle, _, _) in &cluster {
        within(
            "partition present on every node",
            handle.wait_until_partition_present(TOPIC, 0),
        )
        .await;
    }
    within(
        "the witness joins the ISR",
        cluster[0].0.wait_until_isr_len(TOPIC, 0, 3),
    )
    .await;

    check!(
        produce_error(&leader, topic_id, N_RECORDS).await == codes::NONE,
        "acks=all commits on the leader with all three sites up"
    );

    // Replication to the witness advances: it holds the records. This is a
    // FOLLOWER fetch the witness itself issued, so it is the half of the fetch
    // path that must keep working.
    let witness_handle = &cluster[2].0;
    within(
        "the witness replicates every record",
        witness_handle.wait_until_local_log_end_offset(TOPIC, 0, i64::from(N_RECORDS)),
    )
    .await;

    let witness = client_at(&cluster[2].1.listen_addr.to_string()).await;
    check!(
        produce_error(&witness, topic_id, N_RECORDS).await == codes::NOT_LEADER_OR_FOLLOWER,
        "a client Produce to the witness is refused"
    );
    let fetched = witness
        .send(consumer_fetch(topic_id, SITE_C))
        .await
        .expect("consumer Fetch to the witness");
    check!(
        fetched.responses[0].partitions[0].error_code == codes::NOT_LEADER_OR_FOLLOWER,
        "a client Fetch to the witness is refused"
    );

    // The refusals are client-facing only. The witness is still a full ISR
    // member, which is what keeps `min.insync.replicas=2` satisfiable when a
    // data site is lost.
    let isr: BTreeSet<u64> = cluster[0]
        .0
        .partition_isr_for_test(TOPIC, 0)
        .expect("the leader knows the partition")
        .into_iter()
        .collect();
    check!(
        isr == maplit::btreeset! {1, 2, 3},
        "the witness stays in the ISR after refusing client traffic"
    );

    shutdown(cluster).await;
}
