//! A coordinator answers a transactional write only once it is committed.
//!
//! Kafka's `TransactionStateManager.appendTransactionToLog` appends each
//! `__transaction_state` record with `acks=-1`, and its group coordinator
//! completes a `TxnOffsetCommit` or an `OffsetDelete` write once the high
//! watermark passes it. Each answers the client only then. A coordinator that
//! answered at its local append could acknowledge a commit decision, or the
//! offsets of a transaction, that the next leader of the partition never gets.
//! Kafka's transactions system test bounces brokers under transactional
//! copiers, and such an answer breaks exactly-once on the failover after it.
//!
//! The same rule holds for a transaction marker. Kafka's `WriteTxnMarkers`
//! handler appends each marker with `acks=-1`, and the coordinator records the
//! transaction complete only after every marker is committed.

use {
    crate::{
        support::{
            self,
            client::connect_owned,
            offsets::{
                offset_commit_partition, offset_commit_topic, offset_delete_partition,
                offset_delete_request, offset_delete_topic,
            },
            relayed_cluster::RelayedCluster,
            shutdown_cluster,
            topics::AssignedTopicSetup,
            transaction_wire::{
                add_partition_request, coordinator_loading, create_assigned_topic,
                init_producer_request, initialized_identity, partition_added, partition_leader,
                produce_fixture, read_committed_through, retry_coordinator,
            },
            transactions::{
                ProducerIdentity, end_transaction_request, txn_offset_partition, txn_offset_topic,
            },
        },
        txn_harness::registered_transaction_cluster,
    },
    assert2::assert,
    krabka_broker::{BrokerConfig, BrokerHandle, NodeId},
    krabka_client_core::{Client, Connection, ConnectionOptions},
    krabka_protocol::{
        owned::{
            add_offsets_to_txn_request::AddOffsetsToTxnRequest,
            describe_transactions_request::DescribeTransactionsRequest,
            end_txn_request::EndTxnRequest, end_txn_response::EndTxnResponse,
            offset_commit_request::OffsetCommitRequest,
            offset_delete_response::OffsetDeleteResponse,
            txn_offset_commit_request::TxnOffsetCommitRequest,
            txn_offset_commit_response::TxnOffsetCommitResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::{task::JoinHandle, time},
};

const TID: &str = "txn-uncommitted-writes";
const TOPIC: &str = "txn-uncommitted-writes";
const GROUP: &str = "txn-uncommitted-writes";
const STATE_TOPIC: &str = "__transaction_state";
const OFFSETS_TOPIC: &str = "__consumer_offsets";

const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const CONCURRENT_TRANSACTIONS: i16 = 51;

/// The time a retried request may take to see the cluster settle.
const SETTLE: Duration = Duration::from_secs(60);

fn retriable(code: i16) -> bool { coordinator_loading(code) || code == CONCURRENT_TRANSACTIONS }

/// One connection to the broker that binds `address`, past any relay, so a
/// request reaches that broker whatever the cluster's metadata advertises.
async fn connect(address: SocketAddr) -> Connection {
    Connection::connect(address, ConnectionOptions {
        client_id: "txn-uncommitted-writes".to_owned(),
        ..ConnectionOptions::default()
    })
    .await
    .expect("connect")
}

async fn client(address: SocketAddr) -> Client {
    connect_owned(address.to_string(), "txn-uncommitted-writes", "client").await
}

/// The leader of `topic-0` in the image of `handle`.
async fn leader_of(handle: &BrokerHandle, topic: &str) -> u64 {
    partition_leader(handle, topic, SETTLE).await
}

async fn create_topic(client: &Client, topic: AssignedTopicSetup) -> WireUuid {
    create_assigned_topic()
        .client(client)
        .topic(topic)
        .call()
        .await
}

fn other_node(coordinator: u64) -> u64 {
    (1..=3)
        .find(|node| *node != coordinator)
        .expect("another broker")
}

async fn init_producer(connection: &Connection) -> ProducerIdentity {
    ProducerIdentity::from_wire(
        initialized_identity(
            || async {
                connection
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

async fn add_partition(connection: &Connection, producer: ProducerIdentity) {
    partition_added(connection.send(add_partition_request(
        TID,
        TOPIC,
        (producer.id.0, producer.epoch.0),
    )))
    .await;
}

async fn produce(
    connection: &Connection,
    topic_id: WireUuid,
    producer: Option<ProducerIdentity>,
    values: &[&'static str],
) {
    produce_fixture()
        .transactional_id(TID)
        .topic(TOPIC)
        .topic_id(topic_id)
        .maybe_producer(producer)
        .values(values)
        .send(|request| connection.send(request))
        .call()
        .await;
}

fn end_txn_request(producer: ProducerIdentity) -> EndTxnRequest {
    end_transaction_request()
        .transactional_id(TID)
        .producer(producer)
        .call()
}

/// Commit, and retry while the coordinator answers a retriable error.
async fn end_txn(connection: &Connection, producer: ProducerIdentity) -> EndTxnResponse {
    retry_coordinator(
        || async {
            connection
                .send(end_txn_request(producer))
                .await
                .expect("EndTxn")
        },
        |response| response.error_code,
        retriable,
        SETTLE,
    )
    .await
}

/// The code a response answers for one partition.
#[derive(Debug, PartialEq, Eq)]
struct PartitionCode {
    partition: i32,
    code: i16,
}

/// The state of a transaction as its coordinator describes it, and the
/// partitions whose markers are outstanding.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Described {
    state: String,
    topics: Vec<DescribedTopic>,
}

/// A topic of a described transaction, and its partitions.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DescribedTopic {
    topic: String,
    partitions: Vec<i32>,
}

/// Describe the transaction until its coordinator answers `expected`, and
/// return the last answer. A coordinator that loads its partition answers a
/// retriable error first.
async fn describe_until(connection: &Connection, expected: &Described) -> Option<Described> {
    let deadline = Instant::now() + SETTLE;
    loop {
        let described = connection
            .send(DescribeTransactionsRequest {
                transactional_ids: vec![TID.into()],
                ..Default::default()
            })
            .await
            .expect("DescribeTransactions");
        let row = &described.transaction_states[0];
        assert!(
            row.error_code == 0 || retriable(row.error_code),
            "DescribeTransactions: {described:?}"
        );
        let answer = (row.error_code == 0).then(|| Described {
            state: row.transaction_state.clone(),
            topics: row
                .topics
                .iter()
                .map(|topic| DescribedTopic {
                    topic: topic.topic.clone(),
                    partitions: topic.partitions.clone(),
                })
                .collect(),
        });
        if answer.as_ref() == Some(expected) || Instant::now() >= deadline {
            return answer;
        }
        // intentional: the completion has no awaiter; the answer is the signal.
        time::sleep(Duration::from_millis(100)).await;
    }
}

async fn read_committed(bootstrap: SocketAddr, last: &str) -> Vec<String> {
    read_committed_through(&bootstrap.to_string(), TOPIC, last, SETTLE).await
}

/// A connection to a coordinator, and the producer whose transaction it holds.
struct ConnectedTransaction {
    connection: Connection,
    producer: ProducerIdentity,
}

async fn connected_transaction(address: SocketAddr) -> ConnectedTransaction {
    let connection = connect(address).await;
    let producer = init_producer(&connection).await;
    add_partition(&connection, producer).await;
    ConnectedTransaction {
        connection,
        producer,
    }
}

async fn cut_and_append_commit(
    cluster: &RelayedCluster,
    coordinator: u64,
    connection: Connection,
    producer: ProducerIdentity,
    topic: &str,
    offset: i64,
) -> JoinHandle<Result<EndTxnResponse, krabka_client_core::ClientError>> {
    cluster.relay(coordinator).cut();
    let commit = tokio::spawn(async move { connection.send(end_txn_request(producer)).await });
    cluster
        .handle(coordinator)
        .wait_until_local_log_end_offset(topic, 0, offset)
        .await;
    commit
}

/// A relayed cluster, an admin client of it, and the node that coordinates
/// the transaction.
struct RelayedTransaction {
    cluster: RelayedCluster,
    admin: Client,
    coordinator: u64,
}

async fn relayed_transaction_cluster(configure: impl Fn(&mut BrokerConfig)) -> RelayedTransaction {
    let cluster = RelayedCluster::start(configure).await;
    // __consumer_offsets needs three brokers for replication, including readers after a broker dies.
    for node in 1..=3 {
        cluster
            .handle(node)
            .wait_until_group_coordinator_ready()
            .await;
    }
    let admin = client(cluster.address(1)).await;
    let coordinator = support::find_coordinator(&admin, support::KEY_TYPE_TRANSACTION, TID)
        .await
        .node_id;
    let coordinator = u64::try_from(coordinator).expect("a node id");
    RelayedTransaction {
        cluster,
        admin,
        coordinator,
    }
}

#[derive(Default)]
struct ReplicatedCommit {
    session: Option<RelayedTransaction>,
    connection: Option<Connection>,
    producer: Option<ProducerIdentity>,
    topic_id: Option<WireUuid>,
    data_node: u64,
    state_end: Option<i64>,
    commit: Option<JoinHandle<Result<EndTxnResponse, krabka_client_core::ClientError>>>,
    completed: Option<EndTxnResponse>,
    records: Option<Vec<String>>,
}

#[rstest::fixture]
fn replicated_commit() -> ReplicatedCommit {
    // The held request remains live across relay cuts and coordinator crashes.
    ReplicatedCommit::default()
}

impl ReplicatedCommit {
    fn session(&self) -> &RelayedTransaction { self.session.as_ref().expect("relayed transaction") }

    fn producer(&self) -> ProducerIdentity { self.producer.expect("transaction identity") }

    fn topic_id(&self) -> WireUuid { self.topic_id.expect("transaction data topic") }

    async fn setup(&mut self, state_replicas: i16) {
        self.session = Some(
            relayed_transaction_cluster(|config| {
                config.transaction_state_num_partitions = 1;
                config.transaction_state_replication_factor = state_replicas;
                config.transaction_state_min_isr = i32::from(state_replicas - 1).max(1);
                config.isr_scan_interval = krabka_units::hours(1);
            })
            .await,
        );
        self.data_node = other_node(self.session().coordinator);
    }

    async fn initialize(&mut self) {
        let ConnectedTransaction {
            connection,
            producer,
        } = connected_transaction(self.session().cluster.address(self.session().coordinator)).await;
        self.connection = Some(connection);
        self.producer = Some(producer);
    }

    async fn shutdown(self) {
        let RelayedTransaction { cluster, admin, .. } = self.session.expect("relayed transaction");
        admin.close();
        if let Some(connection) = self.connection {
            connection.close();
        }
        cluster.shutdown().await;
    }
}

#[rstest_bdd_macros::given(
    "a three-replica transaction-state partition holds the decision while a different broker leads an RF1 data partition containing transactional a,b,c"
)]
async fn replicated_decision_workload(replicated_commit: &mut ReplicatedCommit) {
    replicated_commit.setup(3).await;
    replicated_commit
        .session()
        .cluster
        .handle(replicated_commit.session().coordinator)
        .wait_until_isr_len(STATE_TOPIC, 0, 3)
        .await;
    replicated_commit.topic_id = Some(
        create_topic(
            &replicated_commit.session().admin,
            AssignedTopicSetup::on_replicas(TOPIC, vec![krabka_metadata::NodeId(
                replicated_commit.data_node,
            )]),
        )
        .await,
    );
    replicated_commit
        .session()
        .cluster
        .handle(replicated_commit.data_node)
        .wait_until_local_partition_leader(TOPIC, 0, NodeId(replicated_commit.data_node))
        .await;
    replicated_commit.initialize().await;
    let to_data = connect(
        replicated_commit
            .session()
            .cluster
            .address(replicated_commit.data_node),
    )
    .await;
    produce(
        &to_data,
        replicated_commit.topic_id(),
        Some(replicated_commit.producer()),
        &["a", "b", "c"],
    )
    .await;
    to_data.close();
}

#[rstest_bdd_macros::given(
    "the coordinator alone replicates transaction state and leads an RF2 data partition whose follower has transactional a,b,c"
)]
async fn replicated_marker_workload(replicated_commit: &mut ReplicatedCommit) {
    replicated_commit.setup(1).await;
    replicated_commit.topic_id = Some(
        create_topic(
            &replicated_commit.session().admin,
            AssignedTopicSetup::on_replicas(TOPIC, vec![
                krabka_metadata::NodeId(replicated_commit.session().coordinator),
                krabka_metadata::NodeId(replicated_commit.data_node),
            ]),
        )
        .await,
    );
    replicated_commit
        .session()
        .cluster
        .handle(replicated_commit.session().coordinator)
        .wait_until_local_partition_leader(
            TOPIC,
            0,
            NodeId(replicated_commit.session().coordinator),
        )
        .await;
    replicated_commit
        .session()
        .cluster
        .handle(replicated_commit.session().coordinator)
        .wait_until_isr_len(TOPIC, 0, 2)
        .await;
    replicated_commit.initialize().await;
    produce(
        replicated_commit
            .connection
            .as_ref()
            .expect("transaction connection"),
        replicated_commit.topic_id(),
        Some(replicated_commit.producer()),
        &["a", "b", "c"],
    )
    .await;
}

#[rstest_bdd_macros::when(
    "the coordinator relay is cut and EndTxn appends one local PrepareCommit state record"
)]
async fn unreplicated_commit_decision(replicated_commit: &mut ReplicatedCommit) {
    replicated_commit.state_end = Some(
        replicated_commit
            .session()
            .cluster
            .handle(replicated_commit.session().coordinator)
            .local_log_end_offset(STATE_TOPIC, 0)
            .expect("state partition"),
    );
    let connection = replicated_commit
        .connection
        .take()
        .expect("transaction connection");
    replicated_commit.commit = Some(
        cut_and_append_commit(
            &replicated_commit.session().cluster,
            replicated_commit.session().coordinator,
            connection,
            replicated_commit.producer(),
            STATE_TOPIC,
            replicated_commit.state_end.expect("old log end") + 1,
        )
        .await,
    );
}

#[rstest_bdd_macros::when(
    "the coordinator relay is cut and EndTxn appends a local data COMMIT marker at offset 3"
)]
async fn unreplicated_commit_marker(replicated_commit: &mut ReplicatedCommit) {
    let connection = replicated_commit
        .connection
        .take()
        .expect("transaction connection");
    replicated_commit.commit = Some(
        cut_and_append_commit(
            &replicated_commit.session().cluster,
            replicated_commit.session().coordinator,
            connection,
            replicated_commit.producer(),
            TOPIC,
            4,
        )
        .await,
    );
}

#[rstest_bdd_macros::then("EndTxn remains unanswered for two seconds")]
async fn pending_unreplicated_commit(replicated_commit: &mut ReplicatedCommit) {
    let answered = time::timeout(
        Duration::from_secs(2),
        replicated_commit.commit.as_mut().expect("pending EndTxn"),
    )
    .await;
    assert!(
        answered.is_err(),
        "EndTxn answered for an unreplicated commit: {answered:?}"
    );
}

#[rstest_bdd_macros::then("the transaction-state high watermark remains at its previous log end")]
async fn unreplicated_decision_watermark(replicated_commit: &ReplicatedCommit) {
    let high_watermark = replicated_commit
        .session()
        .cluster
        .handle(replicated_commit.session().coordinator)
        .high_watermark_for_test(STATE_TOPIC, 0)
        .await
        .expect("state partition");
    assert!(Some(high_watermark) == replicated_commit.state_end);
}

#[rstest_bdd_macros::then(
    "DescribeTransactions remains PrepareCommit with its data partition 0 awaiting its marker"
)]
async fn pending_replicated_marker(replicated_commit: &ReplicatedCommit) {
    let connection = connect(
        replicated_commit
            .session()
            .cluster
            .address(replicated_commit.session().coordinator),
    )
    .await;
    let preparing = Described {
        state: "PrepareCommit".to_owned(),
        topics: vec![DescribedTopic {
            topic: TOPIC.to_owned(),
            partitions: vec![0],
        }],
    };
    assert!(describe_until(&connection, &preparing).await == Some(preparing));
    connection.close();
}

#[rstest_bdd_macros::when("the coordinator crashes with the unreplicated commit")]
async fn crash_unreplicated_coordinator(replicated_commit: &mut ReplicatedCommit) {
    let coordinator = replicated_commit.session().coordinator;
    replicated_commit
        .session
        .as_mut()
        .expect("relayed transaction")
        .cluster
        .take(coordinator)
        .crash_for_test()
        .await;
}

#[rstest_bdd_macros::then("the pending EndTxn request is lost without a response")]
async fn lost_unreplicated_commit(replicated_commit: &mut ReplicatedCommit) {
    let lost = replicated_commit
        .commit
        .take()
        .expect("pending EndTxn")
        .await
        .expect("the EndTxn task");
    assert!(lost.is_err(), "EndTxn has no answer: {lost:?}");
}

#[rstest_bdd_macros::when(
    "a follower becomes transaction coordinator and the producer retries EndTxn"
)]
async fn retry_replicated_decision(replicated_commit: &mut ReplicatedCommit) {
    replicated_commit
        .session()
        .cluster
        .handle(replicated_commit.data_node)
        .wait_until_partition_leader_changed(
            STATE_TOPIC,
            0,
            NodeId(replicated_commit.session().coordinator),
        )
        .await;
    let next = leader_of(
        replicated_commit
            .session()
            .cluster
            .handle(replicated_commit.data_node),
        STATE_TOPIC,
    )
    .await;
    let connection = connect(replicated_commit.session().cluster.address(next)).await;
    replicated_commit.completed = Some(end_txn(&connection, replicated_commit.producer()).await);
    connection.close();
}

#[rstest_bdd_macros::then(
    "EndTxn succeeds with the same producer id and an epoch increment of one"
)]
fn replicated_decision_completed(replicated_commit: &ReplicatedCommit) {
    let producer = replicated_commit.producer();
    assert!(
        replicated_commit.completed.as_ref()
            == Some(&EndTxnResponse {
                producer_id: producer.id.0,
                producer_epoch: producer.epoch.0 + 1,
                ..Default::default()
            })
    );
}

#[rstest_bdd_macros::when(
    "the data follower takes leadership and the healed coordinator restarts to retry its marker"
)]
async fn replay_replicated_marker(replicated_commit: &mut ReplicatedCommit) {
    replicated_commit
        .session()
        .cluster
        .handle(replicated_commit.data_node)
        .wait_until_partition_leader_changed(
            TOPIC,
            0,
            NodeId(replicated_commit.session().coordinator),
        )
        .await;
    let coordinator = replicated_commit.session().coordinator;
    replicated_commit
        .session()
        .cluster
        .relay(coordinator)
        .heal();
    replicated_commit
        .session
        .as_mut()
        .expect("relayed transaction")
        .cluster
        .restart(coordinator)
        .await;
}

#[rstest_bdd_macros::when(
    "the data leader appends plain z and a committed consumer reads through it"
)]
async fn read_replicated_commit(replicated_commit: &mut ReplicatedCommit) {
    let connection = connect(
        replicated_commit
            .session()
            .cluster
            .address(replicated_commit.data_node),
    )
    .await;
    produce(&connection, replicated_commit.topic_id(), None, &["z"]).await;
    connection.close();
    replicated_commit.records = Some(
        read_committed(
            replicated_commit
                .session()
                .cluster
                .address(replicated_commit.data_node),
            "z",
        )
        .await,
    );
}

#[rstest_bdd_macros::then("the committed consumer sees exactly a,b,c,z")]
fn visible_replicated_commit(replicated_commit: &ReplicatedCommit) {
    assert!(replicated_commit.records.as_ref().expect("committed read") == &["a", "b", "c", "z"]);
}

#[rstest_bdd_macros::then(
    "DescribeTransactions reports CompleteCommit with no outstanding partitions"
)]
async fn completed_replicated_marker(replicated_commit: &ReplicatedCommit) {
    let connection = connect(
        replicated_commit
            .session()
            .cluster
            .address(replicated_commit.session().coordinator),
    )
    .await;
    let completed = Described {
        state: "CompleteCommit".to_owned(),
        topics: vec![],
    };
    assert!(describe_until(&connection, &completed).await == Some(completed));
    connection.close();
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/transactions/txn_uncommitted_writes.feature",
    name = "an end txn is answered only once its commit decision is replicated"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_end_txn_is_answered_only_once_its_commit_decision_is_replicated(
    replicated_commit: ReplicatedCommit,
) {
    replicated_commit.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/transactions/txn_uncommitted_writes.feature",
    name = "a transaction completes only once its markers are committed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_completes_only_once_its_markers_are_committed(
    replicated_commit: ReplicatedCommit,
) {
    replicated_commit.shutdown().await;
}

#[derive(Default)]
struct UncommittedOffsets {
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    to_group: Option<Connection>,
    to_txn: Option<Connection>,
    group_node: u64,
    txn_node: u64,
    topic_id: Option<WireUuid>,
    producer: Option<ProducerIdentity>,
    stopped_dir: Option<TempDir>,
    offsets: Option<TxnOffsetCommitResponse>,
    deleted: Option<OffsetDeleteResponse>,
}

#[rstest::fixture]
fn uncommitted_offsets() -> UncommittedOffsets {
    // The failed replica stays in ISR for both coordinator writes.
    UncommittedOffsets::default()
}

impl UncommittedOffsets {
    fn address(&self, node: u64) -> SocketAddr {
        self.cluster
            .iter()
            .find(|(broker, _, _)| broker.node_id() == node)
            .map(|(_, config, _)| config.listen_addr)
            .expect("a cluster member")
    }

    fn group(&self) -> &Connection {
        self.to_group
            .as_ref()
            .expect("group coordinator connection")
    }

    fn transaction(&self) -> &Connection {
        self.to_txn
            .as_ref()
            .expect("transaction coordinator connection")
    }

    fn producer(&self) -> ProducerIdentity { self.producer.expect("transaction identity") }

    async fn shutdown(self) {
        let Self {
            cluster,
            to_group,
            to_txn,
            stopped_dir,
            ..
        } = self;
        to_group.expect("group coordinator connection").close();
        to_txn.expect("transaction coordinator connection").close();
        shutdown_cluster(cluster).await;
        drop(stopped_dir);
    }
}

#[rstest_bdd_macros::given(
    "three-replica offsets and transaction state hold stable group offset 5 and an open transaction registered for that group, with ISR scan one hour and heartbeat timeout ten minutes"
)]
async fn replicated_group_offsets(uncommitted_offsets: &mut UncommittedOffsets) {
    uncommitted_offsets.cluster = registered_transaction_cluster(|_, config| {
        config.offsets_topic_num_partitions = 1;
        config.transaction_state_num_partitions = 1;
        // A stopped follower stays in the ISR: the leader does not shrink it
        // on lag, and the controller does not fence it, while the test runs.
        config.isr_scan_interval = krabka_units::hours(1);
        config.heartbeat_timeout = krabka_units::minutes(10);
    })
    .await;
    let admin = client(uncommitted_offsets.cluster[0].1.listen_addr).await;
    let group_node = u64::try_from(
        support::find_coordinator(&admin, support::KEY_TYPE_GROUP, GROUP)
            .await
            .node_id,
    )
    .expect("a node id");
    let txn_node = u64::try_from(
        support::find_coordinator(&admin, support::KEY_TYPE_TRANSACTION, TID)
            .await
            .node_id,
    )
    .expect("a node id");
    let topic_id = create_topic(
        &admin,
        AssignedTopicSetup::on_replicas(TOPIC, vec![krabka_metadata::NodeId(group_node)]),
    )
    .await;
    admin.close();
    // The group coordinator resolves the offsets below by topic id, so its
    // broker has to hold the topic first.
    for (handle, _, _) in &uncommitted_offsets.cluster {
        handle.wait_until_partition_present(TOPIC, 0).await;
    }
    uncommitted_offsets.cluster[0]
        .0
        .wait_until_isr_len(OFFSETS_TOPIC, 0, 3)
        .await;
    uncommitted_offsets.cluster[0]
        .0
        .wait_until_isr_len(STATE_TOPIC, 0, 3)
        .await;
    assert!(leader_of(&uncommitted_offsets.cluster[0].0, OFFSETS_TOPIC).await == group_node);

    // A committed offset, for the delete below, and an open transaction that
    // holds the group's offsets partition.
    uncommitted_offsets.to_group = Some(connect(uncommitted_offsets.address(group_node)).await);
    let committed = uncommitted_offsets
        .group()
        .send(OffsetCommitRequest {
            group_id: GROUP.into(),
            generation_id_or_member_epoch: -1,
            topics: vec![offset_commit_topic(TOPIC, topic_id, vec![
                offset_commit_partition(0, 5, None),
            ])],
            ..Default::default()
        })
        .await
        .expect("OffsetCommit");
    assert!(
        committed.topics[0].partitions[0].error_code == 0,
        "{committed:?}"
    );
    uncommitted_offsets.to_txn = Some(connect(uncommitted_offsets.address(txn_node)).await);
    let producer = init_producer(uncommitted_offsets.transaction()).await;
    let added = uncommitted_offsets
        .transaction()
        .send(AddOffsetsToTxnRequest {
            transactional_id: TID.into(),
            producer_id: producer.id.0,
            producer_epoch: producer.epoch.0,
            group_id: GROUP.into(),
            ..Default::default()
        })
        .await
        .expect("AddOffsetsToTxn");
    assert!(added.error_code == 0, "{added:?}");

    uncommitted_offsets.group_node = group_node;
    uncommitted_offsets.txn_node = txn_node;
    uncommitted_offsets.topic_id = Some(topic_id);
    uncommitted_offsets.producer = Some(producer);
}

#[rstest_bdd_macros::when(
    "an offsets follower that is neither coordinator crashes while remaining in ISR"
)]
async fn stop_offsets_replica(uncommitted_offsets: &mut UncommittedOffsets) {
    // Stop a follower of the offsets partition that is neither coordinator.
    let follower = uncommitted_offsets
        .cluster
        .iter()
        .position(|(handle, _, _)| {
            ![uncommitted_offsets.group_node, uncommitted_offsets.txn_node]
                .contains(&handle.node_id())
        })
        .expect("a follower");
    let (stopped, _, stopped_dir) = uncommitted_offsets.cluster.remove(follower);
    uncommitted_offsets.stopped_dir = Some(stopped_dir);
    stopped.crash_for_test().await;
}

#[rstest_bdd_macros::when(
    "TxnOffsetCommit proposes partition 0 offset 7 with generation -1 and an empty member id"
)]
async fn unreplicated_txn_offsets(uncommitted_offsets: &mut UncommittedOffsets) {
    uncommitted_offsets.offsets = Some(
        uncommitted_offsets
            .group()
            .send(TxnOffsetCommitRequest {
                transactional_id: TID.into(),
                group_id: GROUP.into(),
                producer_id: uncommitted_offsets.producer().id.0,
                producer_epoch: uncommitted_offsets.producer().epoch.0,
                generation_id_or_member_epoch: -1,
                member_id: String::new(),
                topics: vec![txn_offset_topic(
                    TOPIC,
                    uncommitted_offsets.topic_id.expect("source topic"),
                    vec![txn_offset_partition(
                        krabka_ids::PartitionIndex(0),
                        krabka_ids::Offset(7),
                    )],
                )],
                ..Default::default()
            })
            .await
            .expect("TxnOffsetCommit"),
    );
}

#[rstest_bdd_macros::then(
    "the transactional offset response contains only partition 0 with COORDINATOR_NOT_AVAILABLE 15"
)]
fn rejected_unreplicated_offsets(uncommitted_offsets: &UncommittedOffsets) {
    let offsets = uncommitted_offsets
        .offsets
        .as_ref()
        .expect("transactional offsets requested");
    let codes: Vec<PartitionCode> = offsets
        .topics
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|row| PartitionCode {
            partition: row.partition_index,
            code: row.error_code,
        })
        .collect();
    assert!(
        codes
            == [PartitionCode {
                partition: 0,
                code: COORDINATOR_NOT_AVAILABLE
            }],
        "{offsets:?}"
    );
}

#[rstest_bdd_macros::when("OffsetDelete requests the group's partition 0 offset")]
async fn unreplicated_offset_delete(uncommitted_offsets: &mut UncommittedOffsets) {
    uncommitted_offsets.deleted = Some(
        uncommitted_offsets
            .group()
            .send(offset_delete_request(GROUP, vec![offset_delete_topic(
                TOPIC,
                vec![offset_delete_partition(0)],
            )]))
            .await
            .expect("OffsetDelete"),
    );
}

#[rstest_bdd_macros::then(
    "the full OffsetDelete response is COORDINATOR_NOT_AVAILABLE 15 with default remaining fields"
)]
fn rejected_unreplicated_delete(uncommitted_offsets: &UncommittedOffsets) {
    assert!(
        uncommitted_offsets.deleted.as_ref()
            == Some(&OffsetDeleteResponse {
                error_code: COORDINATOR_NOT_AVAILABLE,
                ..Default::default()
            })
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/transactions/txn_uncommitted_writes.feature",
    name = "group coordinator writes are answered only once committed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_coordinator_writes_are_answered_only_once_committed(
    uncommitted_offsets: UncommittedOffsets,
) {
    uncommitted_offsets.shutdown().await;
}
