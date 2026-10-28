#![cfg(test)]

mod support;

use {
    crate::support::client_admin::*,
    krabka_client_admin::groups::{GroupListing, ListGroupsOptions},
    krabka_client_consumer::{AutoOffsetReset, Consumer},
    std::time::Duration,
    tokio::time,
};

struct GroupAdministration {
    directory: Option<tempfile::TempDir>,
    broker: Option<krabka_broker::BrokerHandle>,
    admin: Option<krabka_client_admin::AdminClient>,
    consumer: Option<Consumer>,
    records: Option<Vec<krabka_client_consumer::ConsumerRecord>>,
    groups: Option<Vec<GroupListing>>,
    committed: rstest_bdd::Slot<Option<i64>>,
}
#[rstest::fixture]
fn group_admin() -> GroupAdministration {
    // Start this scenario with fresh inputs and observations.

    GroupAdministration {
        directory: None,
        broker: None,
        admin: None,
        consumer: None,
        records: None,
        groups: None,
        committed: rstest_bdd::Slot::default(),
    }
}
#[rstest_bdd_macros::given("t1 has one record v and an Earliest consumer in group g1")]
async fn group_offset_inputs(group_admin: &mut GroupAdministration) {
    // `ListGroups` goes to every broker of the metadata, as Kafka's
    // `KafkaAdminClient.listGroups` does, so it depends on the broker
    // advertising a real, dialable port for itself.
    let (directory, broker, bootstrap, mut admin) = standalone_admin().await;
    admin
        .create_topics(
            &[topic_spec("t1", 1, 1)],
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await
        .unwrap();

    let producer = default_producer(&bootstrap).await;
    producer
        .send(
            producer_record()
                .topic(("t1").into())
                .value(Some(bytes::Bytes::from_static(b"v")))
                .call(),
        )
        .await
        .unwrap();
    producer.flush().await.unwrap();
    drop(producer);

    let consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .group_id("g1")
        .subscribe(vec!["t1".to_owned()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    group_admin.directory = Some(directory);
    group_admin.broker = Some(broker);
    group_admin.admin = Some(admin);
    group_admin.consumer = Some(consumer);
}
#[rstest_bdd_macros::when("group g1 polls its first record within 30 seconds")]
async fn group_reads_first_record(group_admin: &mut GroupAdministration) {
    let consumer = group_admin.consumer.as_mut().expect("consumer configured");
    // A timed-out poll can be empty while the group is still joining. Commit
    // only after the produced record has actually advanced the position.
    let records = time::timeout(Duration::from_secs(30), async {
        loop {
            let records = consumer.poll(krabka_units::millis(500)).await.unwrap();
            if !records.is_empty() {
                break records;
            }
        }
    })
    .await
    .expect("the group must receive its produced record before committing");
    group_admin.records = Some(records);
}
#[rstest_bdd_macros::then("exactly t1 partition 0 offset 0 with value v was consumed")]
fn group_first_record(group_admin: &GroupAdministration) {
    let records = group_admin.records.as_ref().expect("consumer polled");
    assert2::assert!(records.len() == 1);
    assert2::assert!(records[0].topic == "t1");
    assert2::assert!(records[0].partition == 0);
    assert2::assert!(records[0].offset == 0);
    assert2::assert!(records[0].value.as_deref() == Some(b"v".as_slice()));
}
#[rstest_bdd_macros::when("group g1 commits its consumed position synchronously")]
async fn group_commits_position(group_admin: &mut GroupAdministration) {
    group_admin
        .consumer
        .as_mut()
        .expect("consumer configured")
        .commit_sync()
        .await
        .unwrap();
}
#[rstest_bdd_macros::when("the admin lists consumer groups across the cluster")]
async fn list_consumer_groups(group_admin: &mut GroupAdministration) {
    group_admin.groups = Some(
        group_admin
            .admin
            .as_mut()
            .expect("admin connected")
            .list_groups(&ListGroupsOptions::default())
            .await
            .unwrap()
            .all()
            .unwrap(),
    );
}
#[rstest_bdd_macros::then("the group listing contains g1")]
fn administered_group_visible(group_admin: &GroupAdministration) {
    assert2::assert!(
        group_admin
            .groups
            .as_ref()
            .expect("groups listed")
            .iter()
            .any(|group| group.group_id == "g1")
    );
}
#[rstest_bdd_macros::when("the admin reads g1 committed offsets")]
async fn list_group_offsets(group_admin: &mut GroupAdministration) {
    let offsets = group_admin
        .admin
        .as_mut()
        .expect("admin connected")
        .list_consumer_group_offsets("g1")
        .await
        .unwrap();
    group_admin
        .committed
        .set(offsets.get(&("t1".to_owned(), 0)).copied());
}
#[rstest_bdd_macros::then("g1 has committed next offset 1 for t1 partition 0")]
fn group_committed_next_offset(group_admin: &GroupAdministration) {
    assert2::assert!(group_admin.committed.get().expect("offsets queried") == Some(1));
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_admin_groups.feature",
    name = "lists groups and committed offsets"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lists_groups_and_committed_offsets(group_admin: GroupAdministration) {
    group_admin
        .consumer
        .expect("consumer configured")
        .close()
        .await
        .unwrap();
    group_admin.broker.expect("broker started").shutdown().await;
}
