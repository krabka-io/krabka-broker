//! Producer identities and transaction-coordinator discovery over the Kafka wire.

use {
    crate::support::{
        self, InProcess,
        discovery::{
            expected_coordinator, expected_coordinator_response, expected_local_coordinator,
        },
        transactions::{claim_idempotent_producer, init_producer_request},
    },
    assert2::{assert, check},
    krabka_protocol::owned::{
        find_coordinator_request::FindCoordinatorRequest,
        find_coordinator_response::{Coordinator, FindCoordinatorResponse},
        init_producer_id_response::InitProducerIdResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct ProducerIdentityScenario {
    broker: Option<InProcess>,
    allocations: Vec<InitProducerIdResponse>,
    first_lookup: Option<FindCoordinatorResponse>,
    retried_lookup: Option<Coordinator>,
}

impl ProducerIdentityScenario {
    fn broker(&self) -> &InProcess {
        self.broker
            .as_ref()
            .expect("the producer identity broker is running")
    }

    async fn shutdown(self) {
        self.broker
            .expect("the producer identity broker is running")
            .broker
            .shutdown()
            .await;
    }
}

#[fixture]
fn identity() -> ProducerIdentityScenario {
    // Each scenario owns fresh services and observations.
    ProducerIdentityScenario {
        broker: None,
        ..Default::default()
    }
}

#[given("a single broker with no transaction coordinator topic")]
async fn identity_broker(identity: &mut ProducerIdentityScenario) {
    identity.broker = Some(support::start().await);
}

#[given("the transaction coordinator for {transactional_id} is ready")]
async fn ready_transaction_coordinator(
    identity: &ProducerIdentityScenario,
    transactional_id: String,
) {
    support::find_coordinator(
        &identity.broker().client,
        support::KEY_TYPE_TRANSACTION,
        &transactional_id,
    )
    .await;
}

#[when("a client requests an idempotent producer identity")]
async fn allocate_idempotent(identity: &mut ProducerIdentityScenario) {
    identity
        .allocations
        .push(claim_idempotent_producer(&identity.broker().client).await);
}

#[when("a client initializes transactional producer {transactional_id}")]
async fn allocate_transactional(identity: &mut ProducerIdentityScenario, transactional_id: String) {
    let response = identity
        .broker()
        .client
        .send(
            init_producer_request()
                .maybe_transactional_id(Some(transactional_id))
                .call(),
        )
        .await
        .expect("InitProducerId");
    identity.allocations.push(response);
}

#[then("the fresh idempotent identity has producer id zero and epoch zero")]
fn fresh_idempotent_identity(identity: &ProducerIdentityScenario) {
    let response = identity
        .allocations
        .last()
        .expect("an identity was requested");
    check!(response.error_code == 0);
    check!(response.producer_id == 0);
    check!(response.producer_epoch == 0);
}

#[then("producer initialization returns NOT_COORDINATOR")]
fn missing_transaction_coordinator(identity: &ProducerIdentityScenario) {
    assert!(
        identity
            .allocations
            .last()
            .expect("an identity was requested")
            .error_code
            == 16
    );
}

#[then("the transaction coordinator allocates a real producer id at epoch zero")]
fn fresh_transactional_identity(identity: &ProducerIdentityScenario) {
    let response = identity
        .allocations
        .last()
        .expect("an identity was requested");
    check!(response.error_code == 0, "error_code should be NONE");
    check!(
        response.producer_id >= 0,
        "producer_id should come from txn coordinator's pool"
    );
    check!(response.producer_epoch == 0, "first allocation → epoch 0");
}

#[then("the first transactional identity allocation succeeds")]
fn first_allocation_succeeds(identity: &ProducerIdentityScenario) {
    assert!(identity.allocations[0].error_code == 0, "r1 error_code");
}

#[then("the same transaction id keeps its producer id and increments the epoch")]
fn epoch_increments(identity: &ProducerIdentityScenario) {
    let first = &identity.allocations[0];
    let second = &identity.allocations[1];
    check!(second.error_code == 0, "r2 error_code");
    check!(
        first.producer_id == second.producer_id,
        "same pid for same tid"
    );
    check!(
        second.producer_epoch == first.producer_epoch + 1,
        "second call bumps epoch by 1"
    );
}

#[when("the client first looks up the transaction coordinator for my-tid")]
async fn first_transaction_lookup(identity: &mut ProducerIdentityScenario) {
    identity.first_lookup = Some(
        identity
            .broker()
            .client
            .send(FindCoordinatorRequest {
                coordinator_keys: vec!["my-tid".into()],
                key_type: 1,
                ..Default::default()
            })
            .await
            .expect("FindCoordinator(TRANSACTION)"),
    );
}

#[then("the transaction coordinator is initially unavailable with no broker address")]
fn transaction_coordinator_unavailable(identity: &ProducerIdentityScenario) {
    check!(
        identity
            .first_lookup
            .as_ref()
            .expect("the first lookup completed")
            == &expected_coordinator_response(expected_coordinator(
                "my-tid",
                -1,
                String::new(),
                -1,
                15,
                None
            ))
    );
}

#[when("the client retries transaction coordinator discovery until it is ready")]
async fn retry_transaction_lookup(identity: &mut ProducerIdentityScenario) {
    identity.retried_lookup = Some(
        support::find_coordinator(
            &identity.broker().client,
            support::KEY_TYPE_TRANSACTION,
            "my-tid",
        )
        .await,
    );
}

#[then("the transaction coordinator names this broker at its bound listener")]
fn transaction_coordinator_local(identity: &ProducerIdentityScenario) {
    check!(
        identity
            .retried_lookup
            .as_ref()
            .expect("the retried lookup completed")
            == &expected_local_coordinator("my-tid", identity.broker().broker.listen_addr())
    );
}

#[scenario(
    path = "tests/features/migrated/tests/unit/producer_id.feature",
    name = "init producer id returns fresh pid"
)]
#[tokio::test]
async fn init_producer_id_returns_fresh_pid(identity: ProducerIdentityScenario) {
    identity.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/producer_id.feature",
    name = "init producer id without coordinator bootstrap returns not coordinator"
)]
#[tokio::test]
async fn init_producer_id_without_coordinator_bootstrap_returns_not_coordinator(
    identity: ProducerIdentityScenario,
) {
    identity.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/producer_id.feature",
    name = "find coordinator txn creates topic then returns local broker"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn find_coordinator_txn_creates_topic_then_returns_local_broker(
    identity: ProducerIdentityScenario,
) {
    identity.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/producer_id.feature",
    name = "init producer id with transactional id returns real pid"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_with_transactional_id_returns_real_pid(
    identity: ProducerIdentityScenario,
) {
    identity.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/producer_id.feature",
    name = "init producer id with same tid bumps epoch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_with_same_tid_bumps_epoch(identity: ProducerIdentityScenario) {
    identity.shutdown().await;
}
