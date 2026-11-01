//! A transaction that is open when brokers die is still aborted on timeout.
//!
//! Kafka's coordinator aborts a transaction once `transaction.timeout.ms` has
//! passed, and it retries the abort markers until every partition that the
//! transaction touched has one. A broker kill moves the coordinator, a
//! partition leader, or both, and none of that may leave a transactional batch
//! without its marker: the partition's last stable offset would stop at the
//! batch for good, and a `read_committed` consumer would read nothing after it.
//!
//! Each case opens a transaction-version-2 transaction on two partitions led
//! by two brokers, with Jepsen's one-second transaction timeout. Each
//! partition joins at produce time, as `Produce` v12 adds it. The case then
//! crashes brokers, and requires an ABORT marker of the producer after its
//! batch on both partitions, and a `read_committed` reader to read a plain
//! record appended after the batch.

use {
    crate::support::{
        self,
        client::{connect_owned, raw_connection},
        relayed_cluster::RelayedCluster,
        topics::AssignedTopicSetup,
        transaction_wire::{
            coordinator_loading, create_assigned_topic, initialized_identity, partition_leader,
            produce_fixture, read_committed_through,
        },
        transactions::{ProducerIdentity, init_producer_request},
    },
    assert2::assert,
    krabka_broker::NodeId,
    krabka_client_core::Connection,
    krabka_protocol::{primitives::uuid::Uuid as WireUuid, records::RecordBatch},
    std::{
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tokio::time,
};

const TID: &str = "txn-timeout-abort-kill";
const STATE_TOPIC: &str = "__transaction_state";
/// The topic the case's data leader leads.
const LED_TOPIC: &str = "txn-timeout-abort-kill-led";
/// The topic the remaining broker leads.
const OTHER_TOPIC: &str = "txn-timeout-abort-kill-other";

/// The `transaction.timeout.ms` of the Jepsen workload that found the hang.
const TRANSACTION_TIMEOUT_MS: i32 = 1_000;

/// The time the cluster may take to settle after a crash.
const SETTLE: Duration = Duration::from_secs(60);

/// How long a stalled coordinator keeps failing to commit its state writes
/// before it is crashed: longer than the transaction timeout, the reaper
/// interval, and the one-second commit wait of each write together.
const STALL: Duration = Duration::from_secs(3);

const CONCURRENT_TRANSACTIONS: i16 = 51;

fn retriable(code: i16) -> bool { coordinator_loading(code) || code == CONCURRENT_TRANSACTIONS }

async fn connect(address: SocketAddr) -> Connection {
    raw_connection(address, "txn-timeout-abort-kill").await
}

/// Which brokers a case crashes while the transaction is open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Crash {
    /// The transaction coordinator, which leads neither data partition.
    Coordinator,
    /// The coordinator, after its followers stopped fetching from it, so
    /// that every `__transaction_state` write it tries, the timeout abort
    /// included, fails to commit first.
    StalledCoordinator,
    /// The leader of one data partition, which is not the coordinator.
    DataLeader,
    /// The coordinator, which also leads one data partition.
    CoordinatorLeadingData,
}

/// The ABORT or COMMIT marker that ends the transaction of a producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Marker {
    Abort,
    Commit,
}

/// The marker that `producer_id` wrote after its first transactional data
/// batch in `batches`, or `None` when its transaction is still open there.
fn marker_after_data(batches: &[RecordBatch], producer_id: i64) -> Option<Marker> {
    let mut data_seen = false;
    for batch in batches
        .iter()
        .filter(|batch| batch.producer_id == producer_id)
    {
        if !batch.attributes.is_control_batch() {
            data_seen = true;
            continue;
        }
        if !data_seen {
            continue;
        }
        let key = batch
            .records
            .first()
            .and_then(|record| record.key.as_deref());
        return match key {
            Some([0, 0, 0, 0]) => Some(Marker::Abort),
            Some([0, 0, 0, 1]) => Some(Marker::Commit),
            _ => None,
        };
    }
    None
}

async fn init_producer(connection: &Connection) -> ProducerIdentity {
    ProducerIdentity::from_wire(
        initialized_identity(
            || async {
                connection
                    .send(
                        init_producer_request()
                            .transactional_id(TID.into())
                            .timeout(TRANSACTION_TIMEOUT_MS)
                            .call(),
                    )
                    .await
                    .expect("InitProducerId")
            },
            retriable,
            SETTLE,
        )
        .await,
    )
}

async fn produce(
    connection: &Connection,
    (topic, topic_id): (&str, WireUuid),
    producer: Option<ProducerIdentity>,
    values: &[&'static str],
) {
    produce_fixture()
        .transactional_id(TID)
        .topic(topic)
        .topic_id(topic_id)
        .maybe_producer(producer)
        .values(values)
        .send(|request| connection.send(request))
        .call()
        .await;
}

/// The marker of `producer_id` after its batch on `topic-0`, read from the
/// partition's current leader, once one appears or [`SETTLE`] has passed.
async fn settled_marker(
    cluster: &RelayedCluster,
    survivor: u64,
    topic: &str,
    producer_id: i64,
) -> Option<Marker> {
    let deadline = Instant::now() + SETTLE;
    loop {
        let marker = cluster
            .handle(survivor)
            .partition_leader_for_test(topic, 0)
            .filter(|leader| *leader != 0)
            .and_then(|leader| cluster.running(leader))
            .and_then(|leader| leader.local_batches_for_test(topic, 0))
            .and_then(|batches| marker_after_data(&batches, producer_id));
        if marker.is_some() || Instant::now() >= deadline {
            return marker;
        }
        // intentional: the timeout abort has no awaiter; the log is the signal.
        time::sleep(Duration::from_millis(100)).await;
    }
}

async fn run_case(crash: Crash) {
    let mut cluster = RelayedCluster::start(|config| {
        config.transaction_state_num_partitions = 1;
        config.transaction_state_replication_factor = 3;
        config.transaction_state_min_isr = 2;
        config.txn_abort_cleanup_interval = krabka_units::millis(200);
    })
    .await;
    for node in 1..=3 {
        cluster
            .handle(node)
            .wait_until_group_coordinator_ready()
            .await;
    }
    let admin = connect_owned(
        cluster.address(1).to_string(),
        "txn-timeout-abort-kill",
        "admin",
    )
    .await;
    let coordinator = support::find_coordinator(&admin, support::KEY_TYPE_TRANSACTION, TID)
        .await
        .node_id;
    let coordinator = u64::try_from(coordinator).expect("a node id");
    let others: Vec<u64> = (1..=3).filter(|node| *node != coordinator).collect();
    let (led_leader, other_leader) = match crash {
        Crash::CoordinatorLeadingData => (coordinator, others[0]),
        Crash::Coordinator | Crash::StalledCoordinator | Crash::DataLeader => {
            (others[0], others[1])
        }
    };
    let mut topics = Vec::new();
    for (topic, leader) in [(LED_TOPIC, led_leader), (OTHER_TOPIC, other_leader)] {
        let topic_id = create_assigned_topic()
            .client(&admin)
            .topic(AssignedTopicSetup::leader_first(
                topic,
                krabka_metadata::NodeId(leader),
            ))
            .call()
            .await;
        cluster.handle(leader).wait_until_isr_len(topic, 0, 3).await;
        cluster
            .handle(leader)
            .wait_until_local_partition_leader(topic, 0, NodeId(leader))
            .await;
        topics.push((topic, topic_id, leader));
    }
    cluster
        .handle(coordinator)
        .wait_until_isr_len(STATE_TOPIC, 0, 3)
        .await;
    admin.close();

    let to_coordinator = connect(cluster.address(coordinator)).await;
    let producer = init_producer(&to_coordinator).await;
    to_coordinator.close();
    // No AddPartitionsToTxn: each Produce (v12 and later) adds its partition.
    for (topic, topic_id, leader) in &topics {
        let to_leader = connect(cluster.address(*leader)).await;
        produce(&to_leader, (topic, *topic_id), Some(producer), &[
            "a", "b", "c",
        ])
        .await;
        to_leader.close();
    }

    if crash == Crash::StalledCoordinator {
        cluster.relay(coordinator).cut();
        time::sleep(STALL).await;
    }
    let crashed = match crash {
        Crash::Coordinator | Crash::StalledCoordinator | Crash::CoordinatorLeadingData => {
            coordinator
        }
        Crash::DataLeader => led_leader,
    };
    cluster.take(crashed).crash_for_test().await;
    let survivor = others
        .iter()
        .copied()
        .find(|node| *node != crashed)
        .expect("a surviving broker");
    for (topic, old) in [(STATE_TOPIC, coordinator), (LED_TOPIC, led_leader)] {
        if old == crashed {
            cluster
                .handle(survivor)
                .wait_until_partition_leader_changed(topic, 0, NodeId(old))
                .await;
        }
    }

    let mut markers = Vec::new();
    for (topic, _, _) in &topics {
        markers.push((
            *topic,
            settled_marker(&cluster, survivor, topic, producer.id.0).await,
        ));
    }
    assert!(
        markers
            == [
                (LED_TOPIC, Some(Marker::Abort)),
                (OTHER_TOPIC, Some(Marker::Abort))
            ],
        "{crash:?}"
    );

    let (topic, topic_id, _) = topics[0];
    let leader = partition_leader(cluster.handle(survivor), topic, SETTLE).await;
    cluster
        .handle(leader)
        .wait_until_local_partition_leader(topic, 0, NodeId(leader))
        .await;
    let to_leader = connect(cluster.address(leader)).await;
    produce(&to_leader, (topic, topic_id), None, &["z"]).await;
    to_leader.close();
    let seen =
        read_committed_through(&cluster.address(survivor).to_string(), topic, "z", SETTLE).await;
    assert!(seen == ["z"], "{crash:?}");

    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_transaction_is_aborted_after_its_coordinator_crashes() {
    run_case(Crash::Coordinator).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_transaction_is_aborted_after_a_stalled_coordinator_crashes() {
    run_case(Crash::StalledCoordinator).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_transaction_is_aborted_after_a_data_leader_crashes() {
    run_case(Crash::DataLeader).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_transaction_is_aborted_after_a_coordinator_leading_data_crashes() {
    run_case(Crash::CoordinatorLeadingData).await;
}
