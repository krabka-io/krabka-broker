//! The full transactional cycle run at each downgraded `transaction.version`
//! level.
//!
//! A produce → commit → `read_committed` consume at `TV_1` and at `TV_0` is
//! what proves that the coordinator's encode path for the resolved level runs
//! and that the transaction commits and reads end to end.

use {
    crate::{
        support::{
            self,
            producer::{string_record as rec, transactional_producer},
            topics::TopicPartitionCount,
            transaction_wire::committed_values,
        },
        txnver_harness::{admin_client, boot_single, create_topic, downgrade_transaction_version},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_consumer::Consumer,
    krabka_client_producer::Producer,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct VersionedCycle {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    bootstrap: String,
    topic: String,
    transactional_id: String,
    producer: Option<Producer>,
    consumer: Option<Consumer>,
    seen: Option<Vec<String>>,
}

#[fixture]
fn versioned_cycle() -> VersionedCycle {
    // Observe the same producer-to-reader cycle at each finalized feature level.
    VersionedCycle::default()
}

#[given(
    "transaction.version is finalized at {level} with topic {topic} and producer {transactional_id}"
)]
async fn downgraded_cycle(
    versioned_cycle: &mut VersionedCycle,
    level: i16,
    topic: String,
    transactional_id: String,
) {
    let (broker, bootstrap, directory) = boot_single().await;
    let admin = admin_client(&bootstrap).await;
    create_topic(&admin, &topic, TopicPartitionCount(1)).await;
    downgrade_transaction_version(&admin, level).await;
    support::find_coordinator(&admin, support::KEY_TYPE_TRANSACTION, &transactional_id).await;
    versioned_cycle.broker = Some(broker);
    versioned_cycle.directory = Some(directory);
    versioned_cycle.bootstrap = bootstrap;
    versioned_cycle.topic = topic;
    versioned_cycle.transactional_id = transactional_id;
}

#[when("the versioned producer commits a transaction containing a,b,c")]
async fn commit_versioned_cycle(versioned_cycle: &mut VersionedCycle) {
    versioned_cycle.producer = Some(
        transactional_producer(
            versioned_cycle.bootstrap.clone(),
            &versioned_cycle.transactional_id,
        )
        .await,
    );

    let transaction = versioned_cycle
        .producer
        .as_ref()
        .expect("initialized producer")
        .begin_transaction()
        .await
        .unwrap();
    for value in ["a", "b", "c"] {
        versioned_cycle
            .producer
            .as_ref()
            .expect("initialized producer")
            .send(rec(&versioned_cycle.topic, value))
            .await
            .expect("produce acknowledged");
    }
    transaction.commit().await.unwrap();
}

#[when("a fresh read-committed group {group} consumes the versioned topic")]
async fn read_versioned_cycle(versioned_cycle: &mut VersionedCycle, group: String) {
    let (consumer, seen) = committed_values(
        versioned_cycle.bootstrap.clone(),
        &group,
        &versioned_cycle.topic,
        None,
    )
    .await;
    versioned_cycle.consumer = Some(consumer);
    versioned_cycle.seen = Some(seen);
}

#[then("the versioned committed read contains exactly a,b,c")]
fn versioned_records(versioned_cycle: &VersionedCycle) {
    assert!(
        versioned_cycle
            .seen
            .as_ref()
            .expect("committed topic consumed")
            == &["a", "b", "c"]
    );
}

#[scenario(
    path = "tests/features/migrated/tests/transaction_version/txnver_full_cycle.feature",
    name = "versioned full cycles commit and read"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn versioned_full_cycles_commit_and_read(
    versioned_cycle: VersionedCycle,
    level: i16,
    topic: String,
    transactional_id: String,
    group: String,
) {
    let _ = (level, topic, transactional_id, group);
    versioned_cycle
        .producer
        .take()
        .expect("producer")
        .close()
        .await
        .unwrap();
    versioned_cycle
        .consumer
        .take()
        .expect("consumer")
        .close()
        .await
        .unwrap();
    versioned_cycle
        .broker
        .take()
        .expect("broker")
        .shutdown()
        .await;
    drop(versioned_cycle);
}
