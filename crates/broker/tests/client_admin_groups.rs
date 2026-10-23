mod support;

use {
    crate::support::client_admin::*,
    krabka_client_consumer::{AutoOffsetReset, Consumer},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lists_groups_and_committed_offsets() {
    // `ListGroups` goes to every broker of the metadata, as Kafka's
    // `KafkaAdminClient.listGroups` does, so it depends on the broker
    // advertising a real, dialable port for itself.
    let (_dir, _broker, bootstrap, mut admin) = standalone_admin().await;
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

    let mut consumer = Consumer::builder()
        .bootstrap(&bootstrap)
        .group_id("g1")
        .subscribe(vec!["t1".to_string()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    // A timed-out poll can be empty while the group is still joining. Commit
    // only after the produced record has actually advanced the position.
    let records = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let records = consumer.poll(krabka_units::millis(500)).await.unwrap();
            if !records.is_empty() {
                break records;
            }
        }
    })
    .await
    .expect("the group must receive its produced record before committing");
    assert2::assert!(records.len() == 1);
    assert2::assert!(records[0].topic == "t1");
    assert2::assert!(records[0].partition == 0);
    assert2::assert!(records[0].offset == 0);
    assert2::assert!(records[0].value.as_deref() == Some(b"v".as_slice()));
    consumer.commit_sync().await.unwrap();

    let groups = admin
        .list_groups(&krabka_client_admin::groups::ListGroupsOptions::default())
        .await
        .unwrap()
        .all()
        .unwrap();
    assert2::assert!(groups.iter().any(|g| g.group_id == "g1"));

    let offsets = admin.list_consumer_group_offsets("g1").await.unwrap();
    let committed = offsets.get(&("t1".to_string(), 0)).copied();
    assert2::assert!(committed == Some(1));
}
