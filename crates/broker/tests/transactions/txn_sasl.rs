//! Authentication on the producer's transaction and group coordinator connections.

use {
    crate::{
        support::{producer::enqueue_string_values, transaction_wire::committed_values},
        txn_harness::{boot_single_sasl, create_topic_sasl, sasl_plain_security},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_consumer::Consumer,
    krabka_client_producer::{ConsumerGroupMetadata, Producer},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::env,
    tempfile::TempDir,
};

#[derive(Default)]
struct AuthenticatedTransaction {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    bootstrap: String,
    producer: Option<Producer>,
    verifier: Option<Consumer>,
    visible: Vec<String>,
}

#[fixture]
fn authenticated() -> AuthenticatedTransaction {
    // Each scenario owns fresh services and observations.
    AuthenticatedTransaction {
        broker: None,
        ..Default::default()
    }
}

#[given("a SASL PLAIN broker authenticates alice and hosts sasl-txn")]
async fn sasl_transaction_broker(authenticated: &mut AuthenticatedTransaction) {
    let (broker, bootstrap, directory) = boot_single_sasl(&[("alice", "alice-secret")]).await;
    create_topic_sasl(
        &bootstrap,
        "sasl-txn",
        sasl_plain_security("alice", "alice-secret"),
    )
    .await;
    authenticated.broker = Some(broker);
    authenticated.directory = Some(directory);
    authenticated.bootstrap = bootstrap;
}

#[when("Alice initializes a transactional producer and commits records a,b,c with group offsets")]
async fn authenticated_commit(authenticated: &mut AuthenticatedTransaction) {
    authenticated.producer = Some(
        Producer::builder()
            .bootstrap(authenticated.bootstrap.clone())
            .transactional_id("sasl-tid")
            .security(sasl_plain_security("alice", "alice-secret"))
            .build()
            .await
            .unwrap(),
    );
    authenticated
        .producer
        .as_ref()
        .expect("Alice's producer was built")
        .init_transactions()
        .await
        .unwrap();
    let transaction = authenticated
        .producer
        .as_ref()
        .expect("Alice's producer was built")
        .begin_transaction()
        .await
        .unwrap();
    enqueue_string_values(
        authenticated
            .producer
            .as_ref()
            .expect("Alice's producer was built"),
        "sasl-txn",
        &["a", "b", "c"],
    )
    .await;
    authenticated
        .producer
        .as_ref()
        .expect("Alice's producer was built")
        .send_offsets_to_transaction(
            [(("sasl-txn".to_owned(), 0), 3i64)],
            &ConsumerGroupMetadata::for_group("sasl-cpp-g"),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    authenticated
        .broker
        .as_ref()
        .expect("the SASL broker is running")
        .wait_until_local_log_end_offset("sasl-txn", 0, 3)
        .await;
}

#[when("an authenticated read_committed verifier polls the committed transaction")]
async fn authenticated_readback(authenticated: &mut AuthenticatedTransaction) {
    // Preserve the original coverage-only exception: the authenticated coordinator
    // flow above always runs, but instrumentation can stall the final poll.
    if env::var_os("LLVM_PROFILE_FILE").is_some() {
        return;
    }
    let (consumer, seen) = committed_values(
        authenticated.bootstrap.clone(),
        "sasl-verify",
        "sasl-txn",
        Some(sasl_plain_security("alice", "alice-secret")),
    )
    .await;
    authenticated.verifier = Some(consumer);
    authenticated.visible = seen;
}

#[then("the authenticated transaction exposes records a,b,c to read_committed")]
fn authenticated_records_visible(authenticated: &AuthenticatedTransaction) {
    // This is the same limited visibility exemption as the original test, not an
    // exemption for producer initialization, offset enrollment, or commit.
    if env::var_os("LLVM_PROFILE_FILE").is_some() {
        return;
    }
    assert!(
        authenticated.visible == vec!["a", "b", "c"],
        "seen={:?}",
        authenticated.visible
    );
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_sasl.feature",
    name = "sasl authenticated transactional flow commits"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sasl_authenticated_transactional_flow_commits(authenticated: AuthenticatedTransaction) {
    authenticated
        .producer
        .take()
        .expect("Alice's producer committed")
        .close()
        .await
        .unwrap();
    if let Some(verifier) = authenticated.verifier.take() {
        verifier.close().await.unwrap();
    }
    authenticated
        .broker
        .take()
        .expect("the SASL broker is running")
        .shutdown()
        .await;
    drop(authenticated.directory.take());
    drop(authenticated);
}
