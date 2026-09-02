//! Restart recovery of `__transaction_state`: the decode / recover-from-disk
//! path that a live broker never exercises.
//!
//! Each case persists an `Ongoing` entry, restarts the broker on the same data
//! directory, and commits the recovered transaction through `EndTxn`. A commit
//! succeeds only if `TxnCoordinator::recover` decoded the persisted record with
//! the original producer identity, so the `EndTxn` response is the proof.

use std::time::Duration;

use assert2::assert;
use krabka_broker::{BootstrapMode, Broker, BrokerConfig};
use krabka_client_core::Client;
use krabka_protocol::owned::{
    add_partitions_to_txn_request::{AddPartitionsToTxnRequest, AddPartitionsToTxnTransaction},
    add_partitions_to_txn_response::{AddPartitionsToTxnResponse, AddPartitionsToTxnResult},
    common::{
        add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
        add_partitions_to_txn_response::{
            add_partitions_to_txn_partition_result::AddPartitionsToTxnPartitionResult,
            add_partitions_to_txn_topic_result::AddPartitionsToTxnTopicResult,
        },
    },
    end_txn_request::EndTxnRequest,
    end_txn_response::EndTxnResponse,
    find_coordinator_request::FindCoordinatorRequest,
    init_producer_id_request::InitProducerIdRequest,
};
use tempfile::TempDir;

use crate::txnver_harness::{NONE, admin_client, create_topic, downgrade_transaction_version};

/// Re-open the broker on the SAME data dir. A populated dir replays the raft
/// log and checkpoint instead of a re-bootstrap, so the restart uses
/// `BootstrapMode::Rejoin`. This is the same pattern as
/// `consumer_group_next_gen_persistence.rs`.
fn recovery_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir);
    // Recovery fails closed if any locally led state partition is missing.
    // This single-transaction fixture materializes and replays one partition.
    cfg.transaction_state_num_partitions = 1;
    cfg.transaction_state_replication_factor = 1;
    cfg
}

fn rejoin_config(log_dir: std::path::PathBuf) -> BrokerConfig {
    let mut cfg = recovery_config(log_dir);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    cfg
}

/// `InitProducerId` for `tid`. It retries while the coordinator is still
/// loading, that is, on `COORDINATOR_NOT_AVAILABLE(15)` or
/// `NOT_COORDINATOR(16)`. Returns the assigned
/// `(producer_id, producer_epoch)`.
async fn init_producer_id(client: &Client, tid: &str) -> (i64, i16) {
    // FindCoordinator locates and triggers loading of the coordinator for tid;
    // on a single-broker cluster the coordinator load can lag broker boot.
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

    let mut init = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let resp = client
            .send(InitProducerIdRequest {
                transactional_id: Some(tid.into()),
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            })
            .await
            .expect("InitProducerId");
        if resp.error_code == 0 {
            init = Some(resp);
            break;
        }
        assert!(
            resp.error_code == 15 || resp.error_code == 16,
            "InitProducerId failed: {resp:?}"
        );
        // intentional: txn-coordinator load state is not in the metadata image and
        // has no metric/awaiter; only InitProducerId's 15/16 code signals it.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let init = init.expect("InitProducerId did not become ready within 10s");
    (init.producer_id, init.producer_epoch)
}

/// `AddPartitionsToTxn` to add `(topic, partition)` to the ongoing txn for
/// `tid`/`pid`/`epoch`. This transitions the coordinator entry to `Ongoing`
/// and PERSISTS a `TransactionLogValue` record to `__transaction_state`. It
/// does not commit the record. Asserts success.
async fn add_partition_ongoing(
    client: &Client,
    tid: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
) {
    let added_topic = AddPartitionsToTxnTopic {
        name: topic.into(),
        partitions: vec![partition],
        ..Default::default()
    };
    let add = client
        .send(AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: tid.into(),
                producer_id: pid,
                producer_epoch: epoch,
                verify_only: false,
                topics: vec![added_topic.clone()],
                ..Default::default()
            }],
            v3_and_below_transactional_id: tid.into(),
            v3_and_below_producer_id: pid,
            v3_and_below_producer_epoch: epoch,
            v3_and_below_topics: vec![added_topic],
            ..Default::default()
        })
        .await
        .expect("AddPartitionsToTxn add");
    let expected = AddPartitionsToTxnResponse {
        results_by_transaction: vec![AddPartitionsToTxnResult {
            transactional_id: tid.into(),
            topic_results: vec![AddPartitionsToTxnTopicResult {
                name: topic.into(),
                results_by_partition: vec![AddPartitionsToTxnPartitionResult {
                    partition_index: partition,
                    partition_error_code: NONE,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
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
async fn commit_via_end_txn(client: &Client, tid: &str, pid: i64, epoch: i16) -> EndTxnResponse {
    // FindCoordinator both locates and triggers loading of the coordinator.
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

    // Retry while the coordinator is still loading state from disk.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let resp = client
            .send(EndTxnRequest {
                transactional_id: tid.into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
                ..Default::default()
            })
            .await
            .expect("EndTxn");
        // 15/16: coordinator still loading — keep retrying until the deadline.
        if (resp.error_code == 15 || resp.error_code == 16) && std::time::Instant::now() < deadline
        {
            // intentional: coordinator recover/load state after restart is not in the
            // metadata image and has no metric/awaiter; only EndTxn's 15/16 code
            // signals it. Bounded RPC-response poll, not a materialization wait.
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        return resp;
    }
}

struct RecoveryCase {
    name: &'static str,
    topic: &'static str,
    tid: &'static str,
    downgrade_to: Option<i16>,
    completion_epoch_delta: i16,
}

/// Persist an `Ongoing` transaction, restart on the same data directory, and
/// compare the complete `EndTxn` response after recovery. Success proves that
/// the broker decoded the selected transaction-log codec with the original
/// producer identity. The expected completion epoch also checks the feature
/// level's KIP-890 behavior.
async fn assert_ongoing_txn_survives_restart(case: &RecoveryCase) {
    let dir = TempDir::new().unwrap();
    let log_dir = dir.path().to_path_buf();

    let (pid, epoch);
    {
        let broker = Broker::start(recovery_config(log_dir.clone()))
            .await
            .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = admin_client(&bootstrap).await;
        create_topic(&client, case.topic, 1).await;
        if let Some(level) = case.downgrade_to {
            downgrade_transaction_version(&client, level).await;
        }

        (pid, epoch) = init_producer_id(&client, case.tid).await;
        add_partition_ongoing(&client, case.tid, pid, epoch, case.topic, 0).await;
        // Deliberately do NOT commit: the entry stays Ongoing on disk.

        broker.shutdown().await;
    }

    // Re-boot on the same dir: triggers TxnCoordinator::recover + decode.
    {
        let broker = Broker::start(rejoin_config(log_dir)).await.unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let client = admin_client(&bootstrap).await;

        let response = commit_via_end_txn(&client, case.tid, pid, epoch).await;
        let expected = EndTxnResponse {
            producer_id: pid,
            producer_epoch: epoch + case.completion_epoch_delta,
            ..Default::default()
        };
        assert!(
            response == expected,
            "{} recovery returned an unexpected EndTxn response: {response:?}",
            case.name
        );

        broker.shutdown().await;
    }
}

/// Primary durability matrix for the v1 (flexible, `TV_2` default) and v0
/// (classic, `TV_0`) transaction-log codecs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versioned_ongoing_transactions_survive_restart_and_decode_recovery() {
    let cases = [
        RecoveryCase {
            name: "v1/TV_2",
            topic: "rec1",
            tid: "recover-v1-tid",
            downgrade_to: None,
            completion_epoch_delta: 1,
        },
        RecoveryCase {
            name: "v0/TV_0",
            topic: "rec0",
            tid: "recover-v0-tid",
            downgrade_to: Some(0),
            completion_epoch_delta: 0,
        },
    ];

    for case in &cases {
        assert_ongoing_txn_survives_restart(case).await;
    }
}
