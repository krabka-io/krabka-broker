// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! KIP-939 two-phase-commit (2PC) participation — `InitProducerId` v6
//! coordinator semantics:
//!  - `enable2Pc` is rejected with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` when
//!    the cluster has `transaction.two.phase.commit.enable=false`;
//!  - `keepPreparedTxn` succeeds with the no-ongoing sentinels when there is
//!    no transaction to recover;
//!  - with 2PC enabled, an `enable2Pc` transaction is persisted with the
//!    no-timeout sentinel (`i32::MAX`), so it is exempt from the idle reaper;
//!  - none of it needs a `transaction.version` above `TV_2`, which is the
//!    highest level Kafka defines, and an upgrade to 3 is refused as Kafka
//!    refuses it (krabka-io/krabka-broker#784).
//!
//! `txn::two_pc_model` proves the reaper's *decision*, that it never aborts a
//! 2PC transaction, exhaustively. These tests pin the wire and handler
//! behaviour end to end.

mod support;

use std::time::Duration;

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use krabka_client_producer::Producer;
use krabka_protocol::owned::{
    describe_transactions_request::DescribeTransactionsRequest,
    init_producer_id_request::{self, InitProducerIdRequest},
    update_features_request::UpdateFeaturesRequest,
};
use tempfile::TempDir;

use crate::support::{
    client::connect_client, configs::feature_update, discovery::api_versions_request_for,
    transactions::init_producer_request,
};

/// An `InitProducerId` request that keeps the two-phase commit fields.
///
/// Kafka marks `InitProducerId` v6 `latestVersionUnstable`, so a client
/// negotiates v5 at the most and leaves `enable2Pc` and `keepPreparedTxn` off
/// the wire. These tests drive the v6 semantics, so they pin v6, on a broker
/// that enables unstable api versions.
fn two_phase_init(
    request: InitProducerIdRequest,
) -> crate::support::wire::At<InitProducerIdRequest, { init_producer_id_request::MAX_VERSION }> {
    crate::support::wire::At(request)
}

// Kafka error codes (see crates/broker/src/codes.rs).
const NONE: i16 = 0;
const TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;

#[derive(Clone, Copy)]
enum TwoPhaseCommitSupport {
    Enabled,
    Disabled,
}

async fn boot(support: TwoPhaseCommitSupport) -> (BrokerHandle, String, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.features.transaction_two_phase_commit_enable =
        matches!(support, TwoPhaseCommitSupport::Enabled);
    // v6 is `latestVersionUnstable`: a broker accepts it only with Kafka's
    // `unstable.api.versions.enable`, and closes the connection otherwise.
    cfg.features.unstable_api_versions = krabka_broker::api_catalog::UnstableApiVersions::Enabled;
    let broker = Broker::start(cfg).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    (broker, bootstrap, dir)
}

async fn client(bootstrap: &str) -> krabka_client_core::Client {
    connect_client(bootstrap, None).await
}

/// The broker rejects `enable2Pc=true` against a cluster with 2PC disabled,
/// with `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`. It does so before any
/// coordinator lookup, so the result does not depend on a bootstrapped
/// `__transaction_state`, and a client cannot probe the cluster flag with an
/// `UNSUPPORTED_*` code.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_2pc_rejected_when_cluster_disabled() {
    let (broker, bootstrap, _dir) = boot(TwoPhaseCommitSupport::Disabled).await;
    let client = client(&bootstrap).await;

    let resp = client
        .send(two_phase_init(InitProducerIdRequest {
            enable2_pc: true,
            keep_prepared_txn: false,
            ..init_producer_request(crate::support::transactions::InitProducerSetup {
                transactional_id: Some("tid-2pc".into()),
                timeout: crate::support::transactions::TransactionTimeoutMillis(30_000),
                ..Default::default()
            })
        }))
        .await
        .expect("InitProducerId");

    assert!(
        resp.error_code == TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        "expected 53 (TRANSACTIONAL_ID_AUTHORIZATION_FAILED), got {}",
        resp.error_code
    );
    broker.shutdown().await;
}

/// `keepPreparedTxn=true` with no ongoing transaction succeeds and reports the
/// no-ongoing sentinels. A recovery client can therefore treat completion as a
/// no-op without a separate describe round trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keep_prepared_txn_without_ongoing_transaction_is_a_noop() {
    let (broker, bootstrap, _dir) = boot(TwoPhaseCommitSupport::Enabled).await;
    // The producer does not retry COORDINATOR_NOT_AVAILABLE from
    // FindCoordinator. Bring the transaction coordinator up first.
    broker.wait_until_transaction_coordinator_ready().await;
    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("tid-keep")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let client = client(&bootstrap).await;

    let resp = client
        .send(two_phase_init(InitProducerIdRequest {
            enable2_pc: true,
            keep_prepared_txn: true,
            ..init_producer_request(crate::support::transactions::InitProducerSetup {
                transactional_id: Some("tid-keep".into()),
                timeout: crate::support::transactions::TransactionTimeoutMillis(30_000),
                ..Default::default()
            })
        }))
        .await
        .expect("InitProducerId");

    assert!(
        resp.error_code == NONE,
        "keepPreparedTxn without an ongoing transaction failed: {}",
        resp.error_code
    );
    assert!(resp.ongoing_txn_producer_id == -1);
    assert!(resp.ongoing_txn_producer_epoch == -1);
    producer.close().await.ok();
    broker.shutdown().await;
}

/// With 2PC enabled, an `enable2Pc` `InitProducerId` succeeds, and the broker
/// persists the transaction with the no-timeout sentinel `i32::MAX`. That is
/// how the coordinator marks a transaction that the timeout reaper must skip.
///
/// The test bootstraps the coordinator with a normal transactional producer,
/// which creates `__transaction_state` and the tid's entry. It then
/// re-initializes the same tid with `enable2Pc=true`, and reads the persisted
/// timeout back through `DescribeTransactions`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_2pc_persists_no_timeout_sentinel() {
    let (broker, bootstrap, _dir) = boot(TwoPhaseCommitSupport::Enabled).await;
    // The producer does not retry COORDINATOR_NOT_AVAILABLE from
    // FindCoordinator. Bring the transaction coordinator up first.
    broker.wait_until_transaction_coordinator_ready().await;

    // Bootstrap the txn coordinator + this tid's entry via a normal producer.
    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("tid-2pc-ok")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();

    // Re-init the SAME tid with enable2Pc → flips it to a no-timeout 2PC txn.
    let client = client(&bootstrap).await;
    let resp = client
        .send(two_phase_init(InitProducerIdRequest {
            enable2_pc: true,
            keep_prepared_txn: false,
            ..init_producer_request(crate::support::transactions::InitProducerSetup {
                transactional_id: Some("tid-2pc-ok".into()),
                timeout: crate::support::transactions::TransactionTimeoutMillis(30_000),
                ..Default::default()
            })
        }))
        .await
        .expect("InitProducerId(enable2Pc)");
    assert!(
        resp.error_code == NONE,
        "enable2Pc init should succeed once the cluster enables 2PC, got {}",
        resp.error_code
    );
    assert!(resp.producer_id >= 0);

    // The persisted transaction timeout must be the 2PC no-timeout sentinel.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let timeout_ms = loop {
        let r = client
            .send(DescribeTransactionsRequest {
                transactional_ids: vec!["tid-2pc-ok".into()],
                ..Default::default()
            })
            .await
            .expect("DescribeTransactions");
        let row = &r.transaction_states[0];
        if row.error_code == NONE {
            break row.transaction_timeout_ms;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "DescribeTransactions never returned the tid: {row:?}"
        );
        // intentional: transaction-coordinator state (persisted txn timeout) is
        // read via a DescribeTransactions RPC and is not in the metadata image
        // nor exposed as a metric — bounded RPC-response poll, no awaiter exists.
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        timeout_ms == i32::MAX,
        "2PC transaction must persist the no-timeout sentinel i32::MAX, got {timeout_ms}"
    );

    producer.close().await.ok();
    broker.shutdown().await;
}

/// krabka-io/krabka-broker#784: KIP-939 runs at the bootstrapped
/// `transaction.version` 2, and an upgrade to 3 fails as it does on Kafka
/// 4.3.1, whose `TransactionVersion` stops at `TV_2`: `INVALID_UPDATE_VERSION`
/// (95) with `QuorumFeatures.reasonNotSupported`'s message and no rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_phase_commit_needs_no_transaction_version_3_and_3_is_refused() {
    use krabka_protocol::owned::update_features_response::UpdateFeaturesResponse;

    let (broker, bootstrap, _dir) = boot(TwoPhaseCommitSupport::Enabled).await;
    let client = client(&bootstrap).await;

    let api_versions = client
        .send(api_versions_request_for("krabka-test", "0.0.0"))
        .await
        .expect("ApiVersions");
    let finalized = api_versions
        .finalized_features
        .iter()
        .find(|feature| feature.name == "transaction.version")
        .map(|feature| feature.max_version_level);
    assert!(finalized == Some(2), "{api_versions:?}");

    let response = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![feature_update("transaction.version", 3, 1)],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(
        response
            == UpdateFeaturesResponse {
                error_code: 95,
                error_message: Some(
                    "The update failed for all features since the following feature had an \
                     error: Invalid update version 3 for feature transaction.version. Local \
                     controller 1 only supports versions 0-2"
                        .into(),
                ),
                ..Default::default()
            },
        "{response:?}"
    );
    broker.shutdown().await;
}
