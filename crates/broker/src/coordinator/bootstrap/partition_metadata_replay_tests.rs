//! Replay of the deprecated `ConsumerGroupPartitionMetadata` record (key v4),
//! as Kafka 4.3.1's loader and `GroupMetadataManager.replay` handle it.
//!
//! A value creates the consumer group when the log has none and marks it as
//! holding the record. A tombstone clears the mark, and is ignored for a group
//! the log does not hold. A value that does not decode, or whose version is
//! not 0, fails the load.

use {
    super::test_support::{LogRecord, ReplayedCoordinator, replay_log},
    crate::coordinator::unified::{
        GroupSeed, ReplaySeed,
        persistence_next_gen::{
            GroupMetadataValue, NextGenKey, PartitionMetadataValue, SubscribedTopicMetadata,
            encode_key,
        },
    },
    assert2::check,
    bytes::Bytes,
};

/// What a replay leaves for group `g`.
#[derive(Debug, PartialEq)]
enum Outcome {
    /// The replay failed.
    Failed,
    /// The replay holds no group `g`.
    NoGroup,
    /// The replay holds group `g` with this seed.
    Group(Box<GroupSeed>),
}

fn key(kind: fn(String) -> NextGenKey) -> Bytes { encode_key(&kind("g".into())).unwrap() }

fn partition_metadata(group_id: String) -> NextGenKey { NextGenKey::PartitionMetadata { group_id } }

fn group_metadata(group_id: String) -> NextGenKey { NextGenKey::GroupMetadata { group_id } }

fn value() -> Bytes {
    PartitionMetadataValue {
        topics: vec![SubscribedTopicMetadata {
            topic_id: uuid::Uuid::from_u128(7),
            topic_name: "t".into(),
            num_partitions: 2,
            partition_metadata: vec![],
        }],
    }
    .encode()
    .unwrap()
}

/// Replays one batch per record and returns what it left for group `g`.
fn replay(records: Vec<LogRecord>) -> Outcome {
    let ReplayedCoordinator {
        coordinator,
        replayed,
    } = replay_log(records);
    if replayed.is_err() {
        return Outcome::Failed;
    }
    coordinator.seeds.get("g").map_or(Outcome::NoGroup, |seed| {
        Outcome::Group(Box::new(seed.clone()))
    })
}

use {
    rstest_bdd_macros::{given, scenarios, then, when},
    std::mem,
};
#[derive(Default)]
struct PartitionMetadataLoad {
    records: Vec<LogRecord>,
    outcome: Option<Outcome>,
}
#[rstest::fixture]
fn metadata_load() -> PartitionMetadataLoad {
    // Start this scenario with fresh inputs and observations.

    PartitionMetadataLoad::default()
}
#[given("the persisted consumer partition metadata log contains {records}")]
fn persisted_partition_metadata(metadata_load: &mut PartitionMetadataLoad, records: String) {
    metadata_load.records = records
        .split(',')
        .map(|record| match record {
            "group-epoch-4" => LogRecord::put(
                key(group_metadata),
                GroupMetadataValue {
                    epoch: 4,
                    metadata_hash: 0,
                }
                .encode()
                .unwrap(),
            ),
            "metadata" => LogRecord::put(key(partition_metadata), value()),
            "metadata-tombstone" => LogRecord::tombstone(key(partition_metadata)),
            "unknown-version" => {
                let mut bytes = value().to_vec();
                bytes[1] = 1;
                LogRecord::put(key(partition_metadata), Bytes::from(bytes))
            }
            "truncated" => LogRecord::put(key(partition_metadata), value().slice(..5)),
            _ => panic!("unknown subscription metadata log record"),
        })
        .collect();
}
#[when("bootstrap loads the deprecated consumer partition metadata records")]
fn load_partition_metadata(metadata_load: &mut PartitionMetadataLoad) {
    metadata_load.outcome = Some(replay(mem::take(&mut metadata_load.records)));
}
#[then(
    "the consumer metadata load is {outcome} at group epoch {epoch} with persisted subscription marker {marked}"
)]
fn loaded_partition_metadata(
    metadata_load: &PartitionMetadataLoad,
    outcome: String,
    epoch: i32,
    marked: bool,
) {
    let expected = match outcome.as_str() {
        "group" => Outcome::Group(Box::new(GroupSeed {
            group_epoch: epoch,
            has_subscription_metadata_record: marked,
            ..GroupSeed::new_group()
        })),
        "absent" => Outcome::NoGroup,
        "failed" => Outcome::Failed,
        _ => panic!("unknown metadata load outcome"),
    };
    check!(metadata_load.outcome.as_ref() == Some(&expected));
}
scenarios!("tests/features/migrated/src/coordinator/bootstrap/partition_metadata_replay_tests.feature",fixtures=[metadata_load:PartitionMetadataLoad]);
