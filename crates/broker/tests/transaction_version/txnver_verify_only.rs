//! The KIP-890 verify-only `AddPartitionsToTxn` path at the default `TV_2`.
//!
//! A verify-only request must report per-partition status without mutating the
//! transaction, so this module owns both the case and the
//! `InitProducerId` readiness loop it needs to reach a loaded transaction
//! coordinator.

use {
    crate::{
        support::{
            discovery::coordinator_lookup_request,
            topics::TopicPartitionCount,
            transaction_wire::partitions_request,
            transactions::{PartitionRegistration, ProducerIdentity, init_producer_request},
        },
        txnver_harness::{
            NONE, TRANSACTION_ABORTABLE, admin_client, boot_single, create_topic,
            expected_partitions, partition_code,
        },
    },
    assert2::assert,
    krabka_client_core::Client,
    krabka_protocol::owned::common::add_partitions_to_txn_request::add_partitions_to_txn_topic::AddPartitionsToTxnTopic,
    std::time::{Duration, Instant},
    tokio::time,
};

const VERIFY_TID: &str = "verify-tid";

async fn await_transaction_coordinator(client: &Client) -> ProducerIdentity {
    let coordinator = client
        .send(coordinator_lookup_request(VERIFY_TID, 1, vec![
            VERIFY_TID.into(),
        ]))
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

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = client
            .send(
                init_producer_request()
                    .maybe_transactional_id(Some(VERIFY_TID.into()))
                    .call(),
            )
            .await
            .expect("InitProducerId");
        if response.error_code == 0 {
            return ProducerIdentity::from_wire((response.producer_id, response.producer_epoch));
        }
        assert!(
            response.error_code == 15 || response.error_code == 16,
            "InitProducerId: {response:?}"
        );
        assert!(
            Instant::now() < deadline,
            "transaction coordinator did not become ready"
        );
        time::sleep(Duration::from_millis(100)).await;
    }
}

use {
    krabka_broker::BrokerHandle,
    krabka_protocol::owned::add_partitions_to_txn_response::AddPartitionsToTxnResponse,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct PartitionVerification {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Client>,
    producer: Option<ProducerIdentity>,
    added: Option<AddPartitionsToTxnResponse>,
    verified: Option<AddPartitionsToTxnResponse>,
}

#[fixture]
fn partition_verification() -> PartitionVerification {
    // Added and queried partitions are separate protocol observations.
    PartitionVerification::default()
}

#[given("a TV2 coordinator with producer verify-tid and two-partition topic t")]
async fn verification_coordinator(partition_verification: &mut PartitionVerification) {
    let (broker, bootstrap, directory) = boot_single().await;
    let client = admin_client(&bootstrap).await;
    create_topic(&client, "t", TopicPartitionCount(2)).await;
    partition_verification.producer = Some(await_transaction_coordinator(&client).await);
    partition_verification.client = Some(client);
    partition_verification.broker = Some(broker);
    partition_verification.directory = Some(directory);
}

#[when("the producer adds t partition 0 to its transaction")]
async fn add_verified_partition(partition_verification: &mut PartitionVerification) {
    partition_verification.added = Some(
        partition_verification
            .client
            .as_ref()
            .expect("coordinator client")
            .send(
                partitions_request()
                    .transactional_id(VERIFY_TID)
                    .producer(
                        partition_verification
                            .producer
                            .expect("initialized identity"),
                    )
                    .topics(vec![AddPartitionsToTxnTopic {
                        name: "t".into(),
                        partitions: vec![0],
                        ..Default::default()
                    }])
                    .call(),
            )
            .await
            .expect("AddPartitionsToTxn add"),
    );
}

#[then("the add response contains exactly t partition 0 with NONE")]
fn partition_added_response(partition_verification: &PartitionVerification) {
    assert!(
        partition_verification.added.as_ref().expect("add answered")
            == &expected_partitions(VERIFY_TID, "t", &[partition_code(0, NONE)])
    );
}

#[when("the producer verifies t partitions 0 and 1 without adding them")]
async fn verify_registered_partitions(partition_verification: &mut PartitionVerification) {
    partition_verification.verified = Some(
        partition_verification
            .client
            .as_ref()
            .expect("coordinator client")
            .send(
                partitions_request()
                    .transactional_id(VERIFY_TID)
                    .producer(
                        partition_verification
                            .producer
                            .expect("initialized identity"),
                    )
                    .registration(PartitionRegistration::VerifyOnly)
                    .topics(vec![AddPartitionsToTxnTopic {
                        name: "t".into(),
                        partitions: vec![0, 1],
                        ..Default::default()
                    }])
                    .call(),
            )
            .await
            .expect("AddPartitionsToTxn verify-only"),
    );
}

#[then(
    "verification reports NONE for added partition 0 and TRANSACTION_ABORTABLE for unregistered partition 1"
)]
fn partition_verification_response(partition_verification: &PartitionVerification) {
    assert!(
        partition_verification
            .verified
            .as_ref()
            .expect("verification answered")
            == &expected_partitions(VERIFY_TID, "t", &[
                partition_code(0, NONE),
                partition_code(1, TRANSACTION_ABORTABLE),
            ])
    );
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_verify_only.feature",
    name = "tv2 verify only add partitions reports per partition codes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tv2_verify_only_add_partitions_reports_per_partition_codes(
    partition_verification: PartitionVerification,
) {
    partition_verification
        .broker
        .take()
        .expect("coordinator broker")
        .shutdown()
        .await;
    drop(partition_verification);
}
