//! KIP-392 end-to-end fetch-from-follower (rack-aware reads) integration
//! test. The test boots a 2-broker cluster in two different racks. Both
//! brokers use the rack-aware replica selector. The test asserts the full
//! consumer redirect path:
//!
//!   1. A consumer Fetch to the LEADER carrying the follower's rack gets a
//!      `preferred_read_replica` that points at the same-rack follower.
//!   2. A consumer Fetch sent to that FOLLOWER returns the committed
//!      records. The follower tracks the leader-reported high watermark.
//!   3. A consumer Fetch to the LEADER carrying the leader's own rack gets
//!      `preferred_read_replica == -1` and reads from the leader.
//!
//! The shared `support` harness has no per-broker config customizer, so
//! this test inlines the harness's bootstrap-then-join start loop to set a
//! distinct `rack` and the `RackAware` selector on each broker. This is
//! option (b) in the task plan. The change stays inside this file.
#![cfg(test)]

use {
    crate::support::{
        client::connect_client,
        fetch::fetch_topic_row,
        produce::{SinglePartitionProduceSetup, send_batch},
        shutdown_cluster,
    },
    assert2::assert,
    krabka_broker::{BrokerConfig, BrokerHandle, replica_selector::ReplicaSelectorKind},
    krabka_protocol::{
        owned::{
            create_topics_request::CreateTopicsRequest,
            fetch_request::{FetchPartition, FetchRequest},
            fetch_response::FetchResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{
        collections::BTreeSet,
        mem,
        time::{Duration, Instant},
    },
    support::cluster_lock,
    tempfile::TempDir,
    tokio::{sync::MutexGuard, time},
};

mod support;

const RACK_A: &str = "rack-a"; // broker 1 (leader)
const RACK_B: &str = "rack-b"; // broker 2 (follower)

/// Boot an `n`-broker cluster where each broker carries a distinct `rack` and
/// the `RackAware` replica selector. The boot uses KIP-595 static-quorum
/// multi-voter bootstrap. All brokers boot in `Bootstrap` mode with the same
/// static voter set and elect among themselves, with no `add_learner` and no
/// `change_membership`. This function injects the KIP-392 config into each
/// broker before it starts. `racks[i]` is broker `i+1`'s rack.
async fn start_rack_aware_cluster(racks: &[&str]) -> Vec<(BrokerHandle, BrokerConfig, TempDir)> {
    // Reuse the shared race-free static-voter bootstrap (it holds the ephemeral
    // listeners live until each broker adopts them, so there is no
    // bind-and-drop TOCTOU window for a concurrently running test binary to
    // steal a just-released port — the `AddrInUse` flake). Layer the KIP-392
    // per-broker config (distinct rack + `RackAware` selector) via the
    // customizer hook.
    let racks: Vec<String> = racks.iter().map(|r| (*r).to_owned()).collect();
    support::start_n_node_with(u64::try_from(racks.len()).unwrap(), |i, cfg| {
        cfg.rack = Some(racks[i].clone());
        cfg.replica_selector = ReplicaSelectorKind::RackAware;
    })
    .await
    .expect("rack-aware static cluster start")
}

async fn wait_for_partition_on_all(
    cluster: &[(BrokerHandle, BrokerConfig, TempDir)],
    topic: &str,
    partition: i32,
) {
    for (h, _, _) in cluster {
        h.wait_until_partition_present(topic, partition).await;
    }
}

/// Poll until `(topic, partition)` has `leader` as leader and an ISR equal to
/// `expected_isr` as a set. This function observes the state through `handle`.
async fn wait_leader_and_isr(
    handle: &BrokerHandle,
    topic: &str,
    partition: i32,
    leader: u64,
    expected_isr: &[u64],
) {
    let want: BTreeSet<u64> = expected_isr.iter().copied().collect();
    handle
        .wait_for_image(|img| {
            img.partition(topic, partition).is_some_and(|p| {
                p.leader == leader
                    && p.isr.iter().map(|n| n.get()).collect::<BTreeSet<u64>>() == want
            })
        })
        .await;
}

use crate::support::client::value_batch as record_batch;

/// Build a consumer Fetch (`replica_id` = -1) for a single (topic, partition)
/// at `offset`, with `rack_id`. The shared `Client` negotiates the broker's
/// max Fetch version (>= 11). So the client serializes `rack_id` on the wire,
/// and the decoded response carries `preferred_read_replica`.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ConsumerFetchSetup<'a> {
    #[default("t")]
    topic: &'a str,
    #[default(WireUuid::ZERO)]
    topic_id: WireUuid,
    offset: krabka_ids::Offset,
    #[default(RACK_B)]
    rack: &'a str,
}

#[bon::builder]
fn consumer_fetch(
    #[builder(default = "t")] topic: &'_ str,
    #[builder(default = WireUuid :: ZERO)] topic_id: WireUuid,
    #[builder(into, default)] offset: krabka_ids::Offset,
    #[builder(default = RACK_B)] rack: &'_ str,
) -> FetchRequest {
    let setup = ConsumerFetchSetup {
        topic,
        topic_id,
        offset,
        rack,
    };
    let ConsumerFetchSetup {
        topic,
        topic_id,
        offset,
        rack,
    } = setup;
    FetchRequest {
        replica_id: -1,
        max_wait_ms: 800,
        min_bytes: 0,
        max_bytes: 10_485_760,
        session_id: 0,
        session_epoch: -1, // sessionless full fetch
        rack_id: rack.to_owned(),
        topics: vec![fetch_topic_row(topic, topic_id, vec![FetchPartition {
            partition: 0,
            fetch_offset: offset.0,
            current_leader_epoch: -1,
            partition_max_bytes: 1_048_576,
            ..Default::default()
        }])],
        ..Default::default()
    }
}

type RackCluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

struct RackReads {
    guard: Option<MutexGuard<'static, ()>>,
    cluster: RackCluster,
    topic_id: WireUuid,
    records: i32,
    leader_fetch: Option<FetchResponse>,
    follower_reads: Vec<(i16, usize)>,
}

#[rstest::fixture]
fn rack_reads() -> RackReads {
    // Start this scenario with fresh inputs and observations.

    RackReads {
        guard: None,
        cluster: Vec::new(),
        topic_id: WireUuid::ZERO,
        records: 0,
        leader_fetch: None,
        follower_reads: Vec::new(),
    }
}

#[rstest_bdd_macros::given("a rack-aware leader in rack-a and in-sync follower in rack-b")]
async fn racks(rack_reads: &mut RackReads) {
    rack_reads.guard = Some(cluster_lock().lock().await);
    rack_reads.cluster = start_rack_aware_cluster(&[RACK_A, RACK_B]).await;
    support::wait_for_all_brokers_registered(&rack_reads.cluster, 2).await;
    let admin = connect_client(rack_reads.cluster[0].1.listen_addr.to_string(), None).await;
    let response = admin
        .send(CreateTopicsRequest {
            topics: vec![support::topic_on("t", &[&[1, 2]])],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == 0);
    rack_reads.topic_id = response.topics[0].topic_id;
    wait_for_partition_on_all(&rack_reads.cluster, "t", 0).await;
    wait_leader_and_isr(&rack_reads.cluster[0].0, "t", 0, 1, &[1, 2]).await;
    assert!(rack_reads.cluster[0].0.partition_leader_for_test("t", 0) == Some(1));
    let isr = rack_reads.cluster[0]
        .0
        .partition_isr_for_test("t", 0)
        .unwrap();
    assert!(isr.iter().copied().find(|&id| id != 1) == Some(2));
}

#[rstest_bdd_macros::given("{records} records committed with all replica acknowledgements")]
async fn committed(rack_reads: &mut RackReads, records: i32) {
    rack_reads.records = records;
    let producer = connect_client(rack_reads.cluster[0].1.listen_addr.to_string(), None).await;
    let response = send_batch(
        &producer,
        record_batch().records(records).call(),
        SinglePartitionProduceSetup {
            topic: "t".into(),
            topic_id: rack_reads.topic_id,
            ..SinglePartitionProduceSetup::replicated()
        },
    )
    .await;
    assert!(response.responses[0].partition_responses[0].error_code == 0);
    rack_reads.cluster[1]
        .0
        .wait_until_local_log_end_offset("t", 0, i64::from(records))
        .await;
}

#[rstest_bdd_macros::when("the consumer fetches from the leader using rack {rack}")]
async fn leader_read(rack_reads: &mut RackReads, rack: String) {
    let client = connect_client(rack_reads.cluster[0].1.listen_addr.to_string(), None).await;
    rack_reads.leader_fetch = Some(
        client
            .send(
                consumer_fetch()
                    .topic_id(rack_reads.topic_id)
                    .rack(&rack)
                    .call(),
            )
            .await
            .expect("leader fetch"),
    );
}

#[rstest_bdd_macros::then("partition 0 recommends read replica {replica}")]
fn redirect(rack_reads: &RackReads, replica: i32) {
    let response = rack_reads
        .leader_fetch
        .as_ref()
        .expect("leader fetch performed");
    let partition = &response.responses[0].partitions[0];
    assert!(partition.partition_index == 0);
    assert!(partition.preferred_read_replica == replica);
}

#[rstest_bdd_macros::when(
    "the rack-b consumer fetches from follower 2 until its committed watermark catches up"
)]
async fn follower_read(rack_reads: &mut RackReads) {
    let client = connect_client(rack_reads.cluster[1].1.listen_addr.to_string(), None).await;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response = client
            .send(consumer_fetch().topic_id(rack_reads.topic_id).call())
            .await
            .expect("follower fetch");
        let partition = &response.responses[0].partitions[0];
        let count = partition
            .records
            .as_ref()
            .and_then(|r| r.as_v2())
            .map_or(0, |batches| batches.iter().map(|b| b.records.len()).sum());
        rack_reads
            .follower_reads
            .push((partition.error_code, count));
        if count >= usize::try_from(rack_reads.records).unwrap() {
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "follower watermark did not catch up"
        );
        time::sleep(Duration::from_millis(50)).await;
    }
}

#[rstest_bdd_macros::then(
    "the follower returns exactly {count} committed records without fetch errors"
)]
fn follower_records(rack_reads: &RackReads, count: usize) {
    assert!(
        rack_reads
            .follower_reads
            .iter()
            .all(|(error, _)| *error == 0)
    );
    assert!(
        rack_reads
            .follower_reads
            .last()
            .expect("follower fetch performed")
            .1
            == count
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/kip_392_fetch_from_follower.feature",
    name = "Rack-aware consumers follow the same-rack replica recommendation"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rack_aware_consumer_is_redirected_to_same_rack_follower(rack_reads: RackReads) {
    shutdown_cluster(mem::take(&mut rack_reads.cluster)).await;
    drop(rack_reads);
}
