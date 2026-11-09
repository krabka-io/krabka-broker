//! Scenarios for `tests/features/metadata_partitioning`: which partition of
//! `__remote_log_metadata` carries the metadata of a user partition.

#![cfg(test)]

use {
    krabka_bdd::{Attempt, Given},
    krabka_remote_storage::TopicIdPartition,
    krabka_remote_storage_topic::{error::MetadataLogError, partitioning::metadata_partition_for},
    uuid::Uuid,
};

/// The topic a scenario places metadata for, unless it names another ID.
const TOPIC_ID: Uuid = Uuid::from_u128(0x1234_5678_9ABC_DEF0_0011_2233_4455_6677);

type Placement = Attempt<i32, MetadataLogError>;

struct PartitioningScenario {
    topic_id: Uuid,
    /// The metadata partition count of the metadata topic a `Given` placed
    /// metadata in.
    partition_count: Given<i32>,
    before: Given<i32>,
    placed: Given<Vec<i32>>,
}

impl PartitioningScenario {
    fn place(
        &self,
        topic: &str,
        partition: i32,
        partition_count: i32,
    ) -> Result<i32, MetadataLogError> {
        metadata_partition_for(
            &TopicIdPartition::new(self.topic_id, topic, partition),
            partition_count,
        )
    }
}

#[rstest::fixture]
fn partitioning() -> PartitioningScenario {
    PartitioningScenario {
        topic_id: TOPIC_ID,
        partition_count: Given::new(
            "partition_count",
            "Given the metadata of orders partition 3 has been placed in a metadata topic of 50 partitions",
        ),
        before: Given::new(
            "before",
            "Given the metadata of orders partition 3 has been placed in a metadata topic of 50 partitions",
        ),
        placed: Given::new(
            "placed",
            "When a broker places the metadata of partition 0 in a metadata topic of 50 partitions",
        ),
    }
}

#[rstest::fixture]
fn placement() -> Placement {
    // The When step replaces it with what the placement returned.
    Placement::NotExecuted
}

#[krabka_bdd::given("a topic with topic ID {topic_id}")]
fn topic_with_id(partitioning: &mut PartitioningScenario, topic_id: Uuid) {
    partitioning.topic_id = topic_id;
}

#[krabka_bdd::given(
    "the metadata of {topic} partition {partition} has been placed in a metadata topic of {count} partitions"
)]
fn placed_before(
    partitioning: &mut PartitioningScenario,
    topic: String,
    partition: i32,
    count: i32,
) {
    let placed = partitioning.place(&topic, partition, count).unwrap();
    partitioning.before.set(placed);
    partitioning.partition_count.set(count);
}

#[krabka_bdd::when(
    "a broker places the metadata of partition {partition} in a metadata topic of {count} partitions"
)]
fn places(partitioning: &mut PartitioningScenario, partition: i32, count: i32) {
    let placed = partitioning.place("orders", partition, count).unwrap();
    partitioning.placed.set(vec![placed]);
}

#[krabka_bdd::when(
    "a broker places the metadata of partition {partition} of the topic renamed to {topic}"
)]
fn places_renamed(partitioning: &mut PartitioningScenario, partition: i32, topic: String) {
    let count = *partitioning.partition_count.get();
    let placed = partitioning.place(&topic, partition, count).unwrap();
    partitioning.placed.set(vec![placed]);
}

#[krabka_bdd::when(
    "a broker places the metadata of partitions {first} to {last} in a metadata topic of {count} partition"
)]
fn places_many(partitioning: &mut PartitioningScenario, first: i32, last: i32, count: i32) {
    let placed = (first..=last)
        .map(|partition| partitioning.place("orders", partition, count).unwrap())
        .collect();
    partitioning.placed.set(placed);
}

#[krabka_bdd::when(
    "a broker attempts to place the metadata of partition {partition} in a metadata topic of {count} partitions"
)]
fn attempts_to_place(
    partitioning: &PartitioningScenario,
    partition: i32,
    count: i32,
) -> Attempt<i32, MetadataLogError> {
    Placement::of(|| partitioning.place("orders", partition, count))
}

#[krabka_bdd::then("the metadata lands in metadata partition {expected}")]
fn lands_in(partitioning: &PartitioningScenario, expected: i32) {
    assert2::assert!(*partitioning.placed.get() == vec![expected]);
}

#[krabka_bdd::then("the metadata lands in the same metadata partition as before")]
fn lands_as_before(partitioning: &PartitioningScenario) {
    assert2::assert!(*partitioning.placed.get() == vec![*partitioning.before.get()]);
}

#[krabka_bdd::then("every partition's metadata lands in metadata partition {expected}")]
fn every_lands_in(partitioning: &PartitioningScenario, expected: i32) {
    let placed = partitioning.placed.get();
    assert2::assert!(*placed == vec![expected; placed.len()]);
}

#[krabka_bdd::then("the placement is refused because the metadata topic has {count} partitions")]
fn refused(placement: &Placement, count: i32) {
    // MetadataLogError has no PartialEq, so the matcher spells out the whole value.
    placement.assert_failed_with(|error| {
        matches!(error, MetadataLogError::NonPositivePartitionCount { count: refused } if *refused == count)
    });
}

krabka_bdd::scenarios!(
    "tests/features/metadata_partitioning",
    fixtures = [partitioning: PartitioningScenario, placement: Placement],
);
