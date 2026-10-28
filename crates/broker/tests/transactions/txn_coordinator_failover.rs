//! The transaction coordinator moves with the leadership of
//! `__transaction_state`.
//!
//! Kafka's `TransactionCoordinator.onElection` loads a state partition on the
//! broker that becomes its leader, so a transaction that the old leader opened
//! commits through the new one. The test opens a transaction on the
//! coordinator of a three-broker cluster, stops that broker, commits the
//! transaction through the broker that is elected next, and reads the records
//! with `read_committed`.

use {
    crate::{
        support::{
            client::connect_owned,
            discovery::{coordinator_lookup_request, topic_metadata_request},
            shutdown_cluster,
            topics::{AssignedTopicSetup, metadata_topic},
            transaction_wire::{
                add_partition_request, coordinator_loading, create_assigned_topic,
                init_producer_request, initialized_identity, partition_added, partition_leader,
                produce_fixture, read_committed_through, retry_coordinator,
            },
            transactions::{ProducerIdentity, end_transaction_request},
        },
        txn_harness::registered_transaction_cluster,
    },
    assert2::assert,
    krabka_broker::{BrokerConfig, BrokerHandle, NodeId},
    krabka_client_core::Client,
    krabka_protocol::{owned::end_txn_response::EndTxnResponse, primitives::uuid::Uuid},
    std::time::{Duration, Instant},
    tempfile::TempDir,
    tokio::time,
};

const TID: &str = "txn-coordinator-failover";
const TOPIC: &str = "txn-coordinator-failover";
const STATE_TOPIC: &str = "__transaction_state";

const CONCURRENT_TRANSACTIONS: i16 = 51;

/// The time a retried request may take to see the cluster settle.
const SETTLE: Duration = Duration::from_secs(60);

type Cluster = Vec<(BrokerHandle, BrokerConfig, TempDir)>;

fn retriable(code: i16) -> bool { coordinator_loading(code) }

async fn client(address: &str) -> Client {
    connect_owned(address, "txn-coordinator-failover", "client").await
}

fn address_of(cluster: &Cluster, node: u64) -> String {
    cluster
        .iter()
        .find(|(handle, _, _)| handle.node_id() == node)
        .map(|(handle, _, _)| handle.listen_addr().to_string())
        .expect("a broker of the cluster")
}

/// The leader of `topic-0` in the image of `handle`.
async fn leader_of(handle: &BrokerHandle, topic: &str) -> u64 {
    partition_leader(handle, topic, SETTLE).await
}

/// Metadata leadership can precede installation of the Produce readiness gate.
async fn data_leader_client(cluster: &Cluster) -> Client {
    let leader = leader_of(&cluster[0].0, TOPIC).await;
    let (handle, _, _) = cluster
        .iter()
        .find(|(handle, _, _)| handle.node_id() == leader)
        .expect("the data leader is a cluster member");
    handle
        .wait_until_local_partition_leader(TOPIC, 0, NodeId(leader))
        .await;
    client(&handle.listen_addr().to_string()).await
}

/// Create `__transaction_state` with `FindCoordinator`.
async fn find_coordinator(client: &Client) {
    let deadline = Instant::now() + SETTLE;
    loop {
        let found = client
            .send(coordinator_lookup_request(TID, 1, vec![TID.into()]))
            .await
            .expect("FindCoordinator");
        if found
            .coordinators
            .first()
            .is_some_and(|row| row.error_code == 0)
        {
            return;
        }
        assert!(Instant::now() < deadline, "FindCoordinator: {found:?}");
        // intentional: the topic creation has no awaiter from this client.
        time::sleep(Duration::from_millis(100)).await;
    }
}

async fn init_producer(client: &Client) -> ProducerIdentity {
    ProducerIdentity::from_wire(
        initialized_identity(
            || async {
                client
                    .send(init_producer_request(TID))
                    .await
                    .expect("InitProducerId")
            },
            retriable,
            SETTLE,
        )
        .await,
    )
}

async fn add_partition(client: &Client, producer: ProducerIdentity) {
    partition_added(client.send(add_partition_request(
        TID,
        TOPIC,
        (producer.id.0, producer.epoch.0),
    )))
    .await;
}

async fn produce(client: &Client, producer: Option<ProducerIdentity>, values: &[&'static str]) {
    let topic_id = client
        .send(topic_metadata_request(Some(vec![metadata_topic(
            Some(TOPIC.into()),
            Uuid::default(),
        )])))
        .await
        .expect("Metadata")
        .topics
        .iter()
        .find(|row| row.name.as_deref() == Some(TOPIC))
        .map(|row| row.topic_id)
        .expect("topic in metadata");
    produce_fixture()
        .transactional_id(TID)
        .topic(TOPIC)
        .topic_id(topic_id)
        .maybe_producer(producer)
        .values(values)
        .send(|request| client.send(request))
        .call()
        .await;
}

async fn end_txn(client: &Client, producer: ProducerIdentity) -> EndTxnResponse {
    retry_coordinator(
        || async {
            client
                .send(
                    end_transaction_request()
                        .transactional_id(TID)
                        .producer(producer)
                        .call(),
                )
                .await
                .expect("EndTxn")
        },
        |response| response.error_code,
        |code| retriable(code) || code == CONCURRENT_TRANSACTIONS,
        SETTLE,
    )
    .await
}

async fn read_committed(bootstrap: &str, last: &str) -> Vec<String> {
    read_committed_through(bootstrap, TOPIC, last, SETTLE).await
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct CoordinatorRecovery {
    cluster: Cluster,
    coordinator: u64,
    producer: Option<ProducerIdentity>,
    retired_directory: Option<TempDir>,
    committed: Option<EndTxnResponse>,
    seen: Option<Vec<String>>,
}

#[fixture]
fn coordinator_recovery() -> CoordinatorRecovery {
    // Keep the original transaction identity across coordinator replacement.
    CoordinatorRecovery::default()
}

#[given("a three-replica transaction coordinator with an acknowledged open a,b,c transaction")]
async fn recoverable_transaction(coordinator_recovery: &mut CoordinatorRecovery) {
    coordinator_recovery.cluster = registered_transaction_cluster(|_, config| {
        config.transaction_state_num_partitions = 1;
        config.transaction_state_replication_factor = 3;
    })
    .await;
    let admin = client(&coordinator_recovery.cluster[0].0.listen_addr().to_string()).await;
    find_coordinator(&admin).await;
    coordinator_recovery.coordinator =
        leader_of(&coordinator_recovery.cluster[0].0, STATE_TOPIC).await;
    create_assigned_topic()
        .client(&admin)
        .topic(AssignedTopicSetup::leader_first(
            TOPIC,
            krabka_metadata::NodeId(coordinator_recovery.coordinator),
        ))
        .maybe_context(Some("CreateTopics"))
        .call()
        .await;
    coordinator_recovery.cluster[0]
        .0
        .wait_until_isr_len(STATE_TOPIC, 0, 3)
        .await;
    coordinator_recovery.cluster[0]
        .0
        .wait_until_isr_len(TOPIC, 0, 3)
        .await;
    let first = client(&address_of(
        &coordinator_recovery.cluster,
        coordinator_recovery.coordinator,
    ))
    .await;
    let producer = init_producer(&first).await;
    add_partition(&first, producer).await;
    let data_leader = data_leader_client(&coordinator_recovery.cluster).await;
    produce(&data_leader, Some(producer), &["a", "b", "c"]).await;
    first.close();
    data_leader.close();
    admin.close();
    coordinator_recovery.producer = Some(producer);
}

#[when("the transaction coordinator stops and surviving replicas elect new state and data leaders")]
async fn replace_transaction_coordinator(coordinator_recovery: &mut CoordinatorRecovery) {
    let coordinator = coordinator_recovery.coordinator;
    let position = coordinator_recovery
        .cluster
        .iter()
        .position(|(broker, _, _)| broker.node_id() == coordinator)
        .expect("coordinator member");
    let (broker, _, directory) = coordinator_recovery.cluster.remove(position);
    coordinator_recovery.retired_directory = Some(directory);
    broker.shutdown().await;
    coordinator_recovery.cluster[0]
        .0
        .wait_until_partition_leader_changed(STATE_TOPIC, 0, NodeId(coordinator))
        .await;
    coordinator_recovery.cluster[0]
        .0
        .wait_until_partition_leader_changed(TOPIC, 0, NodeId(coordinator))
        .await;
}

#[when("the producer retries its commit through the new transaction coordinator")]
async fn commit_recovered_transaction(coordinator_recovery: &mut CoordinatorRecovery) {
    let next = leader_of(&coordinator_recovery.cluster[0].0, STATE_TOPIC).await;
    let second = client(&address_of(&coordinator_recovery.cluster, next)).await;
    coordinator_recovery.committed = Some(
        end_txn(
            &second,
            coordinator_recovery
                .producer
                .expect("open producer identity"),
        )
        .await,
    );
    second.close();
}

#[then("the recovered commit succeeds and advances the producer epoch by one")]
fn recovered_commit(coordinator_recovery: &CoordinatorRecovery) {
    let producer = coordinator_recovery
        .producer
        .expect("open producer identity");
    assert!(
        coordinator_recovery
            .committed
            .as_ref()
            .expect("commit answered")
            == &EndTxnResponse {
                producer_id: producer.id.0,
                producer_epoch: producer.epoch.0 + 1,
                ..Default::default()
            },
        "EndTxn through the next coordinator"
    );
}

#[when("a nontransactional z is appended and a committed reader reads through it")]
async fn read_recovered_records(coordinator_recovery: &mut CoordinatorRecovery) {
    let data_leader = data_leader_client(&coordinator_recovery.cluster).await;
    produce(&data_leader, None, &["z"]).await;
    data_leader.close();
    coordinator_recovery.seen = Some(
        read_committed(
            &coordinator_recovery.cluster[0].0.listen_addr().to_string(),
            "z",
        )
        .await,
    );
}

#[then("the committed reader sees a,b,c,z in order after coordinator recovery")]
fn recovered_records(coordinator_recovery: &CoordinatorRecovery) {
    assert!(
        coordinator_recovery
            .seen
            .as_ref()
            .expect("committed records read")
            == &["a", "b", "c", "z"]
    );
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_coordinator_failover.feature",
    name = "a transaction commits through the next coordinator"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_commits_through_the_next_coordinator(
    coordinator_recovery: CoordinatorRecovery,
) {
    shutdown_cluster(take(&mut coordinator_recovery.cluster)).await;
    drop(coordinator_recovery);
}

use std::mem::take;
