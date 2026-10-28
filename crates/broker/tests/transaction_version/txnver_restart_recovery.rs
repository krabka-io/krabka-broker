//! Restart recovery of `__transaction_state`: the decode / recover-from-disk
//! path that a live broker never exercises.
//!
//! Each case persists an `Ongoing` entry, restarts the broker on the same data
//! directory, and commits the recovered transaction through `EndTxn`. A commit
//! succeeds only if `TxnCoordinator::recover` decoded the persisted record with
//! the original producer identity, so the `EndTxn` response is the proof.

use {
    crate::{
        support::{
            client::start_client,
            heartbeat_config,
            topics::TopicPartitionCount,
            transaction_wire::{partitions_request, transaction_topic},
            transactions::{ProducerIdentity, end_transaction_request, init_producer_request},
        },
        txnver_harness::{
            NONE, create_topic, downgrade_transaction_version, expected_partitions, partition_code,
        },
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, BrokerConfig},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        end_txn_response::EndTxnResponse, find_coordinator_request::FindCoordinatorRequest,
    },
    std::{
        path::PathBuf,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::time,
};

/// Whether `error_code` is Kafka's answer from a transaction coordinator that
/// is not ready yet: `COORDINATOR_LOAD_IN_PROGRESS` (14) while it loads its
/// partition, `COORDINATOR_NOT_AVAILABLE` (15) or `NOT_COORDINATOR` (16) while
/// the partition's leadership settles. Kafka clients retry all three. A
/// restarted single node meets every one of them: its registration retires the
/// previous incarnation's leaderships, as Kafka's `registerBroker` does, and
/// the coordinator loads again once the node leads its partition anew.
const fn coordinator_not_ready(error_code: i16) -> bool { matches!(error_code, 14..=16) }

/// Re-open the broker on the SAME data dir. A populated dir replays the raft
/// log and checkpoint instead of a re-bootstrap, so the restart uses
/// `BootstrapMode::Rejoin`. This is the same pattern as
/// `consumer_group_next_gen_persistence.rs`.
fn recovery_config(log_dir: PathBuf) -> BrokerConfig {
    let mut cfg = heartbeat_config(log_dir);
    // Recovery fails closed if any locally led state partition is missing.
    // This single-transaction fixture materializes and replays one partition.
    cfg.transaction_state_num_partitions = 1;
    cfg.transaction_state_replication_factor = 1;
    cfg
}

fn rejoin_config(log_dir: PathBuf) -> BrokerConfig {
    let mut cfg = recovery_config(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}

async fn find_ready_coordinator(client: &Client, tid: &str) {
    let fc = client
        .send(FindCoordinatorRequest {
            key: tid.into(),
            key_type: 1, // TRANSACTION
            coordinator_keys: vec![tid.into()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");
    assert!(
        fc.error_code == 0 || fc.coordinators.iter().all(|c| c.error_code == 0),
        "FindCoordinator: {fc:?}"
    );
}

/// `InitProducerId` for `tid`. It retries while the coordinator is still
/// loading, that is, on `COORDINATOR_NOT_AVAILABLE(15)` or
/// `NOT_COORDINATOR(16)`. Returns the assigned
/// producer identity.
async fn init_producer_id(client: &Client, tid: &str) -> ProducerIdentity {
    // FindCoordinator locates and triggers loading of the coordinator for tid;
    // on a single-broker cluster the coordinator load can lag broker boot.
    find_ready_coordinator(client, tid).await;

    let mut init = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        let resp = client
            .send(
                init_producer_request()
                    .maybe_transactional_id(Some(tid.into()))
                    .call(),
            )
            .await
            .expect("InitProducerId");
        if resp.error_code == 0 {
            init = Some(resp);
            break;
        }
        assert!(
            coordinator_not_ready(resp.error_code),
            "InitProducerId failed: {resp:?}"
        );
        // intentional: txn-coordinator load state is not in the metadata image and
        // has no metric/awaiter; only InitProducerId's error code signals it.
        time::sleep(Duration::from_millis(100)).await;
    }
    let init = init.expect("InitProducerId did not become ready within 10s");
    ProducerIdentity::from_wire((init.producer_id, init.producer_epoch))
}

/// `AddPartitionsToTxn` to add `(topic, partition)` to the ongoing txn for
/// `tid`/`pid`/`epoch`. This transitions the coordinator entry to `Ongoing`
/// and PERSISTS a `TransactionLogValue` record to `__transaction_state`. It
/// does not commit the record. Asserts success.
async fn add_partition_ongoing(
    client: &Client,
    tid: &str,
    producer: ProducerIdentity,
    topic: &str,
    partition: i32,
) {
    let added_topic = transaction_topic(topic, vec![partition]);
    let add = client
        .send(
            partitions_request()
                .transactional_id(tid)
                .producer(producer)
                .topics(vec![added_topic])
                .call(),
        )
        .await
        .expect("AddPartitionsToTxn add");
    let expected = expected_partitions(tid, topic, &[partition_code(partition, NONE)]);
    assert!(
        add == expected,
        "adding ({topic},{partition}) returned an unexpected response: {add:?}"
    );
}

/// Wait for the transaction coordinator for `tid` to finish loading after a
/// (re)boot, then commit the in-flight transaction through `EndTxn`. The
/// commit succeeds only if the coordinator already holds an `Ongoing` entry
/// whose `(producer_id, producer_epoch)` match. On a freshly-rebooted broker
/// that entry can come only from a decode of the persisted
/// `__transaction_state` record. Returns the complete `EndTxn` response.
async fn commit_via_end_txn(
    client: &Client,
    tid: &str,
    producer: ProducerIdentity,
) -> EndTxnResponse {
    // FindCoordinator both locates and triggers loading of the coordinator.
    find_ready_coordinator(client, tid).await;

    // Retry while the coordinator is still loading state from disk.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let resp = client
            .send(
                end_transaction_request()
                    .transactional_id(tid)
                    .producer(producer)
                    .call(),
            )
            .await
            .expect("EndTxn");
        // The coordinator is not ready yet: keep retrying until the deadline.
        if coordinator_not_ready(resp.error_code) && Instant::now() < deadline {
            // intentional: coordinator recover/load state after restart is not in the
            // metadata image and has no metric/awaiter; only EndTxn's error code
            // signals it. Bounded RPC-response poll, not a materialization wait.
            time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return resp;
    }
}

use {
    krabka_broker::BrokerHandle,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct TransactionLogRecovery {
    directory: Option<TempDir>,
    broker: Option<BrokerHandle>,
    client: Option<Client>,
    transactional_id: String,
    producer: Option<ProducerIdentity>,
    response: Option<EndTxnResponse>,
}

#[fixture]
fn transaction_log_recovery() -> TransactionLogRecovery {
    // The durable directory and original identity outlive the first broker incarnation.
    TransactionLogRecovery::default()
}

#[given(
    "an ongoing transaction {transactional_id} on topic {topic} persisted at transaction version {level}"
)]
async fn persisted_transaction(
    transaction_log_recovery: &mut TransactionLogRecovery,
    transactional_id: String,
    topic: String,
    level: i16,
) {
    let directory = TempDir::new().unwrap();
    let (broker, _, client) = start_client(
        recovery_config(directory.path().to_path_buf()),
        Some("krabka-txnv-test"),
    )
    .await;
    create_topic(&client, &topic, TopicPartitionCount(1)).await;
    if level == 0 {
        downgrade_transaction_version(&client, level).await;
    }
    let producer = init_producer_id(&client, &transactional_id).await;
    add_partition_ongoing(&client, &transactional_id, producer, &topic, 0).await;
    transaction_log_recovery.directory = Some(directory);
    transaction_log_recovery.broker = Some(broker);
    transaction_log_recovery.client = Some(client);
    transaction_log_recovery.transactional_id = transactional_id;
    transaction_log_recovery.producer = Some(producer);
}

#[when("the transaction broker restarts and decodes the same durable state directory")]
async fn decode_transaction_log(transaction_log_recovery: &mut TransactionLogRecovery) {
    transaction_log_recovery
        .broker
        .take()
        .expect("original broker")
        .shutdown()
        .await;
    let (broker, _, client) = start_client(
        rejoin_config(
            transaction_log_recovery
                .directory
                .as_ref()
                .expect("durable directory")
                .path()
                .to_path_buf(),
        ),
        Some("krabka-txnv-test"),
    )
    .await;
    transaction_log_recovery.broker = Some(broker);
    transaction_log_recovery.client = Some(client);
}

#[when("the original producer commits the recovered ongoing transaction")]
async fn commit_decoded_transaction(transaction_log_recovery: &mut TransactionLogRecovery) {
    transaction_log_recovery.response = Some(
        commit_via_end_txn(
            transaction_log_recovery
                .client
                .as_ref()
                .expect("recovered coordinator client"),
            &transaction_log_recovery.transactional_id,
            transaction_log_recovery
                .producer
                .expect("original identity"),
        )
        .await,
    );
}

#[then("the recovered EndTxn response succeeds with the original producer ID and its next epoch")]
fn decoded_transaction_committed(transaction_log_recovery: &TransactionLogRecovery) {
    let producer = transaction_log_recovery
        .producer
        .expect("original identity");
    assert!(
        transaction_log_recovery
            .response
            .as_ref()
            .expect("recovered EndTxn answered")
            == &EndTxnResponse {
                producer_id: producer.id.0,
                producer_epoch: producer.epoch.0 + 1,
                ..Default::default()
            }
    );
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_restart_recovery.feature",
    name = "versioned ongoing transactions survive restart and decode recovery"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versioned_ongoing_transactions_survive_restart_and_decode_recovery(
    transaction_log_recovery: TransactionLogRecovery,
    transactional_id: String,
    topic: String,
    level: i16,
) {
    let _ = (transactional_id, topic, level);
    transaction_log_recovery
        .broker
        .take()
        .expect("recovered broker")
        .shutdown()
        .await;
    drop(transaction_log_recovery);
}
