//! `DescribeProducers` snapshots actual empty and idempotent append history.
use {
    crate::producers_harness::{ProducerSnapshot, snapshot},
    assert2::{assert, check},
    std::collections::HashSet,
};
#[rstest_bdd_macros::then("fresh partition 0 succeeds with no active producer rows")]
fn fresh_producer_snapshot(snapshot: &ProducerSnapshot) {
    let resp = snapshot.response.as_ref().expect("producers described");
    assert!(resp.topics.len() == 1);
    check!(resp.topics[0].name == "fresh");
    assert!(resp.topics[0].partitions.len() == 1);
    let part = &resp.topics[0].partitions[0];
    check!(
        part.error_code == 0,
        "fresh partition must succeed: {part:?}"
    );
    check!(part.partition_index == 0);
    check!(
        part.active_producers.is_empty(),
        "no produce has happened — list must be empty: {:?}",
        part.active_producers,
    );
}
#[rstest_bdd_macros::then(
    "the single producer snapshot matches its identity epoch and sequence 2 without transaction fields"
)]
fn single_producer_snapshot(snapshot: &ProducerSnapshot) {
    let resp = snapshot.response.as_ref().expect("producers described");
    let (pid, epoch) = snapshot.identities[0];
    let [topic] = &resp.topics[..] else {
        panic!("expected 1 topic")
    };
    let [part] = &topic.partitions[..] else {
        panic!("expected 1 partition")
    };
    assert!(part.error_code == 0, "{part:?}");
    assert!(
        part.active_producers.len() == 1,
        "expected exactly one tracked producer, got {:?}",
        part.active_producers
    );
    let producer = &part.active_producers[0];
    check!(producer.producer_id == pid);
    check!(producer.producer_epoch == i32::from(epoch));
    // base_seq=0, last_offset_delta=n-1=2 → last_sequence = 2.
    check!(producer.last_sequence == 2);
    // An idempotent (non-transactional) producer has no transaction fields.
    check!(producer.coordinator_epoch == -1);
    check!(producer.current_txn_start_offset == -1);
}
#[rstest_bdd_macros::then(
    "the shared partition snapshot contains exactly both claimed producer identities"
)]
fn multiple_producer_snapshot(snapshot: &ProducerSnapshot) {
    let resp = snapshot.response.as_ref().expect("producers described");
    let (pid_a, _) = snapshot.identities[0];
    let (pid_b, _) = snapshot.identities[1];
    let producers = &resp.topics[0].partitions[0].active_producers;
    assert!(
        producers.len() == 2,
        "expected both producers: {producers:?}"
    );
    let seen: HashSet<i64> = producers.iter().map(|p| p.producer_id).collect();
    assert!(seen.contains(&pid_a) && seen.contains(&pid_b));
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_producers/producers_idempotent.feature",
    name = "empty partition returns no active producers"
)]
#[tokio::test]
async fn empty_partition_returns_no_active_producers(snapshot: ProducerSnapshot) {
    snapshot.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_producers/producers_idempotent.feature",
    name = "after idempotent produce describe returns the producer"
)]
#[tokio::test]
async fn after_idempotent_produce_describe_returns_the_producer(snapshot: ProducerSnapshot) {
    snapshot.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_producers/producers_idempotent.feature",
    name = "multiple producers on same partition all surfaced"
)]
#[tokio::test]
async fn multiple_producers_on_same_partition_all_surfaced(snapshot: ProducerSnapshot) {
    snapshot.shutdown().await;
}
