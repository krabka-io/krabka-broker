//! Which group a group id holds as the log replays, as Kafka 4.3.1's
//! `GroupMetadataManager` keeps one group per id whatever its type.
//!
//! An offset commit of an id with no group creates a simple classic group. A
//! consumer or streams record replaces a simple classic group, and fails the
//! load over any other classic group or a group of another modern type; a
//! share record fails over any classic group. A classic `GroupMetadata` value
//! puts a classic group in place of whatever held the id, and its tombstone
//! removes the group of any type.

use {
    super::test_support::{LogRecord, ReplayedCoordinator, classic_group_record, replay_log},
    crate::{
        coordinator::{
            persistence::{GroupMetadataValue as ClassicGroupMetadataValue, OffsetCommitValue},
            unified::{
                persistence_next_gen::{GroupMetadataValue, NextGenKey, encode_key},
                share::persistence::{ShareGroupKey, ShareGroupMetadataValue, encode_share_key},
            },
        },
        error::BrokerError,
    },
    assert2::check,
};

/// What the replay holds under group id `g`.
#[derive(Debug, PartialEq, Eq)]
enum Held {
    Failed(String),
    Nothing,
    Classic,
    Consumer,
    Share,
}

fn consumer_epoch() -> LogRecord {
    LogRecord::put(
        encode_key(&NextGenKey::GroupMetadata {
            group_id: "g".into(),
        })
        .unwrap(),
        GroupMetadataValue {
            epoch: 2,
            metadata_hash: 0,
        }
        .encode()
        .unwrap(),
    )
}

fn share_epoch() -> LogRecord {
    LogRecord::put(
        encode_share_key(&ShareGroupKey::GroupMetadata {
            group_id: "g".into(),
        })
        .unwrap(),
        ShareGroupMetadataValue {
            epoch: 2,
            metadata_hash: 0,
        }
        .encode()
        .unwrap(),
    )
}

fn offset_commit() -> LogRecord {
    LogRecord::put(
        OffsetCommitValue::encode_key("g", "t", 0).unwrap(),
        OffsetCommitValue {
            offset: krabka_log::Offset(5),
            leader_epoch: -1,
            metadata: String::new(),
            commit_timestamp_ms: 1,
            expire_timestamp_ms: None,
            topic_id: None,
        }
        .encode_value()
        .unwrap(),
    )
}

fn classic_group() -> LogRecord {
    let (key, value) = classic_group_record("g", "m1");
    LogRecord::put(key, value)
}

fn classic_tombstone() -> LogRecord {
    LogRecord::tombstone(ClassicGroupMetadataValue::encode_key("g").unwrap())
}

fn replay(records: Vec<LogRecord>) -> Held {
    let ReplayedCoordinator {
        coordinator,
        replayed,
    } = replay_log(records);
    let replayed = match replayed {
        Ok(replayed) => replayed,
        Err(BrokerError::Startup(message)) => return Held::Failed(message),
        Err(other) => return Held::Failed(other.to_string()),
    };
    if coordinator.seeds.contains_key("g") {
        Held::Consumer
    } else if coordinator.share_seeds.contains_key("g") {
        Held::Share
    } else if replayed.classic.contains_key("g") || replayed.simple.contains("g") {
        Held::Classic
    } else {
        Held::Nothing
    }
}

use {
    rstest_bdd_macros::{given, scenarios, then, when},
    std::mem,
};
#[derive(Default)]
struct GroupTypeLoad {
    records: Vec<LogRecord>,
    held: Option<Held>,
}
#[rstest::fixture]
fn group_type_load() -> GroupTypeLoad {
    // Start this scenario with fresh inputs and observations.

    GroupTypeLoad::default()
}
#[given("the coordinator log contains group g records {records}")]
fn persisted_group_types(group_type_load: &mut GroupTypeLoad, records: String) {
    group_type_load.records = records
        .split(',')
        .map(|record| match record {
            "offset" => offset_commit(),
            "consumer" => consumer_epoch(),
            "classic" => classic_group(),
            "classic-tombstone" => classic_tombstone(),
            "share" => share_epoch(),
            _ => panic!("unknown persisted group record"),
        })
        .collect();
}
#[when("bootstrap replays the records under the single group identifier")]
fn load_group_types(group_type_load: &mut GroupTypeLoad) {
    group_type_load.held = Some(replay(mem::take(&mut group_type_load.records)));
}
#[then("group g holds {kind} with load error {message}")]
fn loaded_group_type(group_type_load: &GroupTypeLoad, kind: String, message: String) {
    let expected = match kind.as_str() {
        "classic" => Held::Classic,
        "consumer" => Held::Consumer,
        "nothing" => Held::Nothing,
        "failed" => Held::Failed(message),
        _ => panic!("unknown group replay outcome"),
    };
    check!(group_type_load.held.as_ref() == Some(&expected));
}
scenarios!("tests/features/migrated/src/coordinator/bootstrap/group_type_replay_tests.feature",fixtures=[group_type_load:GroupTypeLoad]);
