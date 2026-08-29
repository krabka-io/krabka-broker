//! The KIP-890 verify-only `AddPartitionsToTxn` path at the default `TV_2`.
//!
//! A verify-only request must report per-partition status without mutating the
//! transaction, so this module owns both the case and the
//! `InitProducerId` readiness loop it needs to reach a loaded transaction
//! coordinator.

use std::time::Duration;

use assert2::assert;
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
    find_coordinator_request::FindCoordinatorRequest,
    init_producer_id_request::InitProducerIdRequest,
};

use crate::txnver_harness::{NONE, TRANSACTION_ABORTABLE, admin_client, boot_single, create_topic};

const VERIFY_TID: &str = "verify-tid";

async fn await_transaction_coordinator(client: &Client) -> (i64, i16) {
    let coordinator = client
        .send(FindCoordinatorRequest {
            key: VERIFY_TID.into(),
            key_type: 1,
            coordinator_keys: vec![VERIFY_TID.into()],
            ..Default::default()
        })
        .await
        .expect("FindCoordinator");
    assert!(
        coordinator.error_code == 0
            || coordinator
                .coordinators
                .iter()
                .all(|row| row.error_code == 0),
        "FindCoordinator: {coordinator:?}"
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let response = client
            .send(InitProducerIdRequest {
                transactional_id: Some(VERIFY_TID.into()),
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
                ..Default::default()
            })
            .await
            .expect("InitProducerId");
        if response.error_code == 0 {
            return (response.producer_id, response.producer_epoch);
        }
        assert!(
            response.error_code == 15 || response.error_code == 16,
            "InitProducerId: {response:?}"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "transaction coordinator did not become ready"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// At the default `TV_2`, verify-only `AddPartitionsToTxn` (KIP-890) returns
/// per-partition `NONE (0)` for a partition already in the ongoing txn and
/// `TRANSACTION_ABORTABLE (120)` for one that was never added.
///
/// Flow: `InitProducerId` → `AddPartitionsToTxn` (`verify_only=false`) adding
/// `(t,0)` → `AddPartitionsToTxn` (`verify_only=true`) querying both `(t,0)`
/// (added → NONE) and `(t,1)` (never added → `TRANSACTION_ABORTABLE`).
///
/// The test sends this over a single connection to the in-process broker,
/// which is its own transaction coordinator. `Client::send` negotiates the
/// highest mutually supported version. The broker advertises v5, which carries
/// the same batched `transactions` array and `verify_only` field as v4 and
/// routes through the identical `handle_v4` verify path, so the assertions
/// hold at either version.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tv2_verify_only_add_partitions_reports_per_partition_codes() {
    let (broker, bootstrap, _dir) = boot_single().await;
    let client = admin_client(&bootstrap).await;
    // Two partitions so (t,1) is a real partition that simply isn't in the txn.
    create_topic(&client, "t", 2).await;

    // Locate (and trigger loading of) the transaction coordinator for TID.
    // On a single-broker cluster the coordinator is this same node, but the
    // `__transaction_state` partition's coordinator load can lag broker boot,
    // so `InitProducerId` may transiently return NOT_COORDINATOR (16) until it
    // settles — retry until the coordinator is ready.
    let (pid, epoch) = await_transaction_coordinator(&client).await;

    // Normal add of (t, 0): transitions the entry to Ongoing and registers the
    // partition. verify_only=false.
    let added_topic = AddPartitionsToTxnTopic {
        name: "t".into(),
        partitions: vec![0],
        ..Default::default()
    };
    let add = client
        .send(AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: VERIFY_TID.into(),
                producer_id: pid,
                producer_epoch: epoch,
                verify_only: false,
                topics: vec![added_topic.clone()],
                ..Default::default()
            }],
            v3_and_below_transactional_id: VERIFY_TID.into(),
            v3_and_below_producer_id: pid,
            v3_and_below_producer_epoch: epoch,
            v3_and_below_topics: vec![added_topic],
            ..Default::default()
        })
        .await
        .expect("AddPartitionsToTxn add");
    let expected_add = AddPartitionsToTxnResponse {
        results_by_transaction: vec![AddPartitionsToTxnResult {
            transactional_id: VERIFY_TID.into(),
            topic_results: vec![AddPartitionsToTxnTopicResult {
                name: "t".into(),
                results_by_partition: vec![AddPartitionsToTxnPartitionResult {
                    partition_index: 0,
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
        add == expected_add,
        "adding (t,0) returned an unexpected response: {add:?}"
    );

    // Verify-only query for BOTH (t,0) (added → NONE) and (t,1) (not added →
    // TRANSACTION_ABORTABLE). verify_only=true must never mutate state.
    let verify_topic = AddPartitionsToTxnTopic {
        name: "t".into(),
        partitions: vec![0, 1],
        ..Default::default()
    };
    let verify = client
        .send(AddPartitionsToTxnRequest {
            transactions: vec![AddPartitionsToTxnTransaction {
                transactional_id: VERIFY_TID.into(),
                producer_id: pid,
                producer_epoch: epoch,
                verify_only: true,
                topics: vec![verify_topic.clone()],
                ..Default::default()
            }],
            v3_and_below_transactional_id: VERIFY_TID.into(),
            v3_and_below_producer_id: pid,
            v3_and_below_producer_epoch: epoch,
            v3_and_below_topics: vec![verify_topic],
            ..Default::default()
        })
        .await
        .expect("AddPartitionsToTxn verify-only");
    let expected_verify = AddPartitionsToTxnResponse {
        results_by_transaction: vec![AddPartitionsToTxnResult {
            transactional_id: VERIFY_TID.into(),
            topic_results: vec![AddPartitionsToTxnTopicResult {
                name: "t".into(),
                results_by_partition: vec![
                    AddPartitionsToTxnPartitionResult {
                        partition_index: 0,
                        partition_error_code: NONE,
                        ..Default::default()
                    },
                    AddPartitionsToTxnPartitionResult {
                        partition_index: 1,
                        partition_error_code: TRANSACTION_ABORTABLE,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(
        verify == expected_verify,
        "verify-only response did not match the partition result table: {verify:?}"
    );

    broker.shutdown().await;
}
