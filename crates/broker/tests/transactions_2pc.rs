#![cfg(test)]

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

use {
    crate::support::{
        client::connect_client, configs::feature_update, discovery::api_versions_request_for,
        transactions::init_producer_request, wire::At,
    },
    assert2::assert,
    krabka_broker::{Broker, BrokerConfig, BrokerHandle, api_catalog::UnstableApiVersions},
    krabka_client_core::Client,
    krabka_client_producer::Producer,
    krabka_protocol::owned::{
        describe_transactions_request::DescribeTransactionsRequest,
        init_producer_id_request::{self, InitProducerIdRequest},
        init_producer_id_response::InitProducerIdResponse,
        update_features_request::UpdateFeaturesRequest,
        update_features_response::UpdateFeaturesResponse,
    },
    std::{
        mem,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::time,
};

#[derive(Clone, Copy, Default)]
enum PreparedTransactionRecovery {
    #[default]
    Disabled,
    Keep,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct TwoPhaseInitSetup<'a> {
    #[default("tid-2pc")]
    transactional_id: &'a str,
    recovery: PreparedTransactionRecovery,
}

/// Pin the unstable v6 wire version and preserve both two-phase commit flags.
#[bon::builder]
fn two_phase_init(
    #[builder(default = "tid-2pc")] transactional_id: &'_ str,
    #[builder(default)] recovery: PreparedTransactionRecovery,
) -> At<InitProducerIdRequest, { init_producer_id_request::MAX_VERSION }> {
    let setup = TwoPhaseInitSetup {
        transactional_id,
        recovery,
    };
    At(InitProducerIdRequest {
        enable2_pc: true,
        keep_prepared_txn: matches!(setup.recovery, PreparedTransactionRecovery::Keep),
        ..init_producer_request()
            .maybe_transactional_id(Some(setup.transactional_id.into()))
            .timeout(30_000)
            .call()
    })
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct InitializedProducerSetup<'a> {
    #[default("tid-keep")]
    transactional_id: &'a str,
}

/// Bootstrap the coordinator before initializing a transactional producer.
#[bon::builder]
async fn initialized_producer(
    broker: &BrokerHandle,
    bootstrap: &str,
    #[builder(default = "tid-keep")] transactional_id: &'_ str,
) -> Producer {
    let setup = InitializedProducerSetup { transactional_id };
    broker.wait_until_transaction_coordinator_ready().await;
    let producer = Producer::builder()
        .bootstrap(bootstrap.to_owned())
        .transactional_id(setup.transactional_id)
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    producer
}

// Kafka error codes (see crates/broker/src/codes.rs).
const NONE: i16 = 0;

#[derive(Clone, Copy)]
enum TwoPhaseCommitSupport {
    Enabled,
    Disabled,
}

/// A started broker, its bootstrap address, and the data directory that must
/// outlive it.
struct Booted {
    broker: BrokerHandle,
    bootstrap: String,
    dir: TempDir,
}

async fn boot(support: TwoPhaseCommitSupport) -> Booted {
    let dir = TempDir::new().unwrap();
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.features.transaction_two_phase_commit_enable =
        matches!(support, TwoPhaseCommitSupport::Enabled);
    // v6 is `latestVersionUnstable`: a broker accepts it only with Kafka's
    // `unstable.api.versions.enable`, and closes the connection otherwise.
    cfg.features.unstable_api_versions = UnstableApiVersions::Enabled;
    let broker = Broker::start(cfg).await.unwrap();
    let bootstrap = broker.listen_addr().to_string();
    Booted {
        broker,
        bootstrap,
        dir,
    }
}

async fn client(bootstrap: &str) -> krabka_client_core::Client {
    connect_client(bootstrap, None).await
}

#[derive(Default)]
struct TwoPhaseScenario {
    booted: Option<Booted>,
    client: Option<Client>,
    producer: Option<Producer>,
    tid: String,
    initialized: Option<InitProducerIdResponse>,
    timeout_ms: Option<i32>,
    finalized: rstest_bdd::Slot<Option<i16>>,
    feature_update: Option<UpdateFeaturesResponse>,
}

#[rstest::fixture]
fn two_phase_scenario() -> TwoPhaseScenario {
    // Every feature flag and transaction belongs to a fresh coordinator.
    TwoPhaseScenario::default()
}

impl TwoPhaseScenario {
    fn booted(&self) -> &Booted { self.booted.as_ref().expect("two-phase broker") }

    fn client(&self) -> &Client { self.client.as_ref().expect("two-phase client") }

    async fn shutdown(&mut self) {
        if let Some(producer) = self.producer.take() {
            let _ = producer.close().await;
        }
        let Self { booted, client, .. } = mem::take(self);
        client.expect("two-phase client").close();
        let Booted { broker, dir, .. } = booted.expect("two-phase broker");
        broker.shutdown().await;
        drop(dir);
    }
}

#[rstest_bdd_macros::given(
    "transaction.two.phase.commit.enable is {enabled} and unstable API versions are enabled"
)]
async fn two_phase_support(two_phase_scenario: &mut TwoPhaseScenario, enabled: bool) {
    let support = if enabled {
        TwoPhaseCommitSupport::Enabled
    } else {
        TwoPhaseCommitSupport::Disabled
    };
    two_phase_scenario.booted = Some(boot(support).await);
    two_phase_scenario.client = Some(client(&two_phase_scenario.booted().bootstrap).await);
}

#[rstest_bdd_macros::given(
    "transactional producer {tid} is initialized without an ongoing transaction"
)]
async fn initialize_two_phase_producer(two_phase_scenario: &mut TwoPhaseScenario, tid: String) {
    two_phase_scenario.tid = tid;
    two_phase_scenario.producer = Some(
        initialized_producer()
            .broker(&two_phase_scenario.booted().broker)
            .bootstrap(&two_phase_scenario.booted().bootstrap)
            .transactional_id(&two_phase_scenario.tid)
            .call()
            .await,
    );
}

#[rstest_bdd_macros::when("InitProducerId v6 enables 2PC for {tid} with keepPreparedTxn {keep}")]
async fn request_two_phase_identity(
    two_phase_scenario: &mut TwoPhaseScenario,
    tid: String,
    keep: bool,
) {
    two_phase_scenario.tid = tid;
    two_phase_scenario.initialized = Some(
        two_phase_scenario
            .client()
            .send(
                two_phase_init()
                    .transactional_id(&two_phase_scenario.tid)
                    .recovery(if keep {
                        PreparedTransactionRecovery::Keep
                    } else {
                        PreparedTransactionRecovery::Disabled
                    })
                    .call(),
            )
            .await
            .expect("InitProducerId"),
    );
}

#[rstest_bdd_macros::then("InitProducerId returns error code {code}")]
fn two_phase_init_code(two_phase_scenario: &TwoPhaseScenario, code: i16) {
    assert!(
        two_phase_scenario
            .initialized
            .as_ref()
            .expect("identity requested")
            .error_code
            == code
    );
}

#[rstest_bdd_macros::then("the response reports no ongoing producer id or epoch, both -1")]
fn no_prepared_two_phase_transaction(two_phase_scenario: &TwoPhaseScenario) {
    let response = two_phase_scenario
        .initialized
        .as_ref()
        .expect("identity requested");
    assert!(response.ongoing_txn_producer_id == -1);
    assert!(response.ongoing_txn_producer_epoch == -1);
}

#[rstest_bdd_macros::then("the assigned producer id is nonnegative")]
fn valid_two_phase_identity(two_phase_scenario: &TwoPhaseScenario) {
    assert!(
        two_phase_scenario
            .initialized
            .as_ref()
            .expect("identity requested")
            .producer_id
            >= 0
    );
}

#[rstest_bdd_macros::when(
    "DescribeTransactions reads the persisted timeout for that transactional id"
)]
async fn describe_two_phase_timeout(two_phase_scenario: &mut TwoPhaseScenario) {
    let deadline = Instant::now() + Duration::from_secs(30);
    two_phase_scenario.timeout_ms = Some(loop {
        let response = two_phase_scenario
            .client()
            .send(DescribeTransactionsRequest {
                transactional_ids: vec![two_phase_scenario.tid.clone()],
                ..Default::default()
            })
            .await
            .expect("DescribeTransactions");
        let row = &response.transaction_states[0];
        if row.error_code == NONE {
            break row.transaction_timeout_ms;
        }
        assert!(
            Instant::now() < deadline,
            "DescribeTransactions never returned the tid: {row:?}"
        );
        time::sleep(Duration::from_millis(100)).await;
    });
}

#[rstest_bdd_macros::then("the persisted timeout is the no-timeout sentinel 2147483647")]
fn infinite_two_phase_timeout(two_phase_scenario: &TwoPhaseScenario) {
    assert!(two_phase_scenario.timeout_ms == Some(i32::MAX));
}

#[rstest_bdd_macros::when("ApiVersions reads the finalized transaction.version")]
async fn describe_two_phase_feature(two_phase_scenario: &TwoPhaseScenario) {
    let response = two_phase_scenario
        .client()
        .send(api_versions_request_for("krabka-test", "0.0.0"))
        .await
        .expect("ApiVersions");
    two_phase_scenario.finalized.set(
        response
            .finalized_features
            .iter()
            .find(|feature| feature.name == "transaction.version")
            .map(|feature| feature.max_version_level),
    );
}

#[rstest_bdd_macros::then("the finalized transaction.version is 2")]
fn two_phase_feature_version(two_phase_scenario: &TwoPhaseScenario) {
    assert!(
        two_phase_scenario
            .finalized
            .get()
            .expect("ApiVersions read")
            == Some(2)
    );
}

#[rstest_bdd_macros::when("UpdateFeatures tries to upgrade transaction.version to 3")]
async fn upgrade_two_phase_feature(two_phase_scenario: &mut TwoPhaseScenario) {
    two_phase_scenario.feature_update = Some(
        two_phase_scenario
            .client()
            .send(UpdateFeaturesRequest {
                feature_updates: vec![feature_update("transaction.version", 3, 1)],
                ..Default::default()
            })
            .await
            .expect("UpdateFeatures"),
    );
}

#[rstest_bdd_macros::then(
    "the full UpdateFeatures response is INVALID_UPDATE_VERSION 95 without feature rows and with message"
)]
fn rejected_two_phase_feature(two_phase_scenario: &TwoPhaseScenario, docstring: String) {
    assert!(
        two_phase_scenario.feature_update.as_ref()
            == Some(&UpdateFeaturesResponse {
                error_code: 95,
                error_message: Some(docstring.trim().into()),
                ..Default::default()
            })
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/transactions_2pc.feature",
    name = "enable 2pc rejected when cluster disabled"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_2pc_rejected_when_cluster_disabled(two_phase_scenario: TwoPhaseScenario) {
    two_phase_scenario.shutdown().await;
    drop(two_phase_scenario);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/transactions_2pc.feature",
    name = "keep prepared txn without ongoing transaction is a noop"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keep_prepared_txn_without_ongoing_transaction_is_a_noop(
    two_phase_scenario: TwoPhaseScenario,
) {
    two_phase_scenario.shutdown().await;
    drop(two_phase_scenario);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/transactions_2pc.feature",
    name = "enable 2pc persists no timeout sentinel"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn enable_2pc_persists_no_timeout_sentinel(two_phase_scenario: TwoPhaseScenario) {
    two_phase_scenario.shutdown().await;
    drop(two_phase_scenario);
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/transactions_2pc.feature",
    name = "two phase commit needs no transaction version 3 and 3 is refused"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_phase_commit_needs_no_transaction_version_3_and_3_is_refused(
    two_phase_scenario: TwoPhaseScenario,
) {
    two_phase_scenario.shutdown().await;
    drop(two_phase_scenario);
}
