//! `send_offsets_to_transaction` over `TxnOffsetCommit` v6 (KIP-1319, #867).
//!
//! krabka-client-rs' producer names each topic by its id and offers v6 when
//! the cluster finalizes `transaction.version` 2 and its metadata holds an id
//! for every topic of the request, as Kafka's `TransactionManager` picks
//! `TxnOffsetCommitRequest.Builder.forTopicIdsOrNames`. The in-process broker
//! finalizes `transaction.version` 2 and, under Kafka's
//! `unstable.api.versions.enable`, advertises `TxnOffsetCommit` 0-6, so the
//! producer commits by topic id alone. By default it advertises 4.3.1's 0-5.

use {
    crate::{
        support::producer::transactional_producer,
        txn_harness::{boot_single_trunk, create_topic},
    },
    assert2::assert,
    krabka_client_admin::AdminClient,
    krabka_client_producer::{ConsumerGroupMetadata, ProducerError},
    std::{collections::BTreeMap, slice},
};

/// `GROUP_ID_NOT_FOUND`. Kafka's `validateTransactionalOffsetCommit` answers
/// it at v6 for a generation of a group the coordinator does not hold, and
/// answers `ILLEGAL_GENERATION (22)` below v6.
const GROUP_ID_NOT_FOUND: i16 = 69;

use {
    krabka_broker::BrokerHandle,
    krabka_client_producer::Producer,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct TopicIdOffsets {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    bootstrap: String,
    producer: Option<Producer>,
    refused: Option<ProducerError>,
    committed: Option<BTreeMap<(String, i32), i64>>,
    absent: Option<BTreeMap<(String, i32), i64>>,
}

#[fixture]
fn topic_id_offsets() -> TopicIdOffsets {
    // Both transactional attempts share the same negotiated topic identity.
    TopicIdOffsets::default()
}

#[given("a transaction-version-2 broker with input topic v6-in and a transactional producer")]
async fn topic_id_broker(topic_id_offsets: &mut TopicIdOffsets) {
    let (broker, bootstrap, directory) = boot_single_trunk().await;
    create_topic(&bootstrap, "v6-in").await;
    topic_id_offsets.producer = Some(transactional_producer(bootstrap.clone(), "v6-tid").await);
    topic_id_offsets.broker = Some(broker);
    topic_id_offsets.bootstrap = bootstrap;
    topic_id_offsets.directory = Some(directory);
}

#[when("the producer enrolls offset 4 for generation 3 of missing group v6-gone and aborts")]
async fn missing_group_offset(topic_id_offsets: &mut TopicIdOffsets) {
    let transaction = topic_id_offsets
        .producer
        .as_ref()
        .expect("initialized transactional producer")
        .begin_transaction()
        .await
        .unwrap();
    let gone = ConsumerGroupMetadata {
        group_id: "v6-gone".into(),
        generation_id: 3,
        member_id: "member".into(),
        group_instance_id: None,
    };
    topic_id_offsets.refused = Some(
        topic_id_offsets
            .producer
            .as_ref()
            .expect("initialized transactional producer")
            .send_offsets_to_transaction([(("v6-in".to_owned(), 0), 4)], &gone)
            .await
            .expect_err("a generation of a missing group is refused"),
    );
    transaction.abort().await.unwrap();
}

#[then("the topic-ID offset request reports GROUP_ID_NOT_FOUND")]
fn topic_id_negotiated(topic_id_offsets: &TopicIdOffsets) {
    assert!(matches!(
        topic_id_offsets
            .refused
            .as_ref()
            .expect("offset enrollment attempted"),
        ProducerError::Server(GROUP_ID_NOT_FOUND)
    ));
}

#[when("the producer commits offset 7 for simple group v6-g through the same input topic")]
async fn simple_group_offset(topic_id_offsets: &mut TopicIdOffsets) {
    let transaction = topic_id_offsets
        .producer
        .as_ref()
        .expect("initialized transactional producer")
        .begin_transaction()
        .await
        .unwrap();
    topic_id_offsets
        .producer
        .as_ref()
        .expect("initialized transactional producer")
        .send_offsets_to_transaction(
            [(("v6-in".to_owned(), 0), 7)],
            &ConsumerGroupMetadata::for_group("v6-g"),
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    topic_id_offsets
        .producer
        .take()
        .expect("producer")
        .close()
        .await
        .unwrap();
}

#[when("an administrator reads both consumer groups' committed offsets")]
async fn inspect_topic_id_offsets(topic_id_offsets: &mut TopicIdOffsets) {
    let mut admin = AdminClient::connect(slice::from_ref(&topic_id_offsets.bootstrap))
        .await
        .unwrap();
    topic_id_offsets.committed = Some(admin.list_consumer_group_offsets("v6-g").await.unwrap());
    topic_id_offsets.absent = Some(admin.list_consumer_group_offsets("v6-gone").await.unwrap());
}

#[then("v6-g holds offset 7 for v6-in partition 0 and v6-gone holds no offsets")]
fn committed_topic_id_offsets(topic_id_offsets: &TopicIdOffsets) {
    assert!(
        topic_id_offsets
            .committed
            .as_ref()
            .expect("committed offsets read")
            == &BTreeMap::from([(("v6-in".to_owned(), 0), 7)])
    );
    assert!(
        topic_id_offsets
            .absent
            .as_ref()
            .expect("missing group offsets read")
            .is_empty()
    );
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_offset_commit_topic_ids.feature",
    name = "send offsets to transaction commits by topic id"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_offsets_to_transaction_commits_by_topic_id(topic_id_offsets: TopicIdOffsets) {
    topic_id_offsets
        .broker
        .take()
        .expect("broker running")
        .shutdown()
        .await;
    drop(topic_id_offsets);
}
