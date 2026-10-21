//! `DeleteOffsets`: the group actor tombstones only the offsets it holds.

use {
    crate::{
        codes,
        coordinator::{
            test_support::{OffsetsRecord, offset_entry, parsed_offset_records},
            unified::{
                actor::{
                    GroupActorMessage,
                    test_support::{make_coordinator, rpc},
                },
                classic_state::ClassicGroup,
                group::{CoordinatorGroup, GroupKind},
                persistence::Key,
            },
        },
    },
    assert2::check,
    std::collections::{HashMap, HashSet},
};

fn key(topic: &str, partition: i32) -> (String, i32) { (topic.to_string(), partition) }

fn tombstone(topic: &str, partition: i32) -> OffsetsRecord {
    OffsetsRecord {
        key: Key::OffsetCommit {
            group_id: "g".into(),
            topic: topic.into(),
            partition,
        },
        value: None,
    }
}

/// One `DeleteOffsets` against a group that holds committed offsets for
/// `t-0` and `t-2`, and a pending transactional offset for `u-0`.
struct Row {
    name: &'static str,
    requested: Vec<(String, i32)>,
    fail_append: bool,
    want_reply: Result<(), i16>,
    want_records: Vec<OffsetsRecord>,
    want_committed: HashSet<(String, i32)>,
    want_pending: HashSet<(String, i32)>,
}

/// The whole outcome of a row: the reply, what reached the offsets log, and
/// the offsets the group still holds.
#[derive(Debug, PartialEq)]
struct Outcome {
    reply: Result<(), i16>,
    records: Vec<OffsetsRecord>,
    committed: HashSet<(String, i32)>,
    pending: HashSet<(String, i32)>,
}

/// Kafka's `OffsetMetadataManager.deleteOffsets` writes a tombstone only when
/// `hasCommittedOffset` or `hasPendingTransactionalOffsets` holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_offsets_tombstones_only_offsets_the_group_holds() {
    let all_committed = HashSet::from([key("t", 0), key("t", 2)]);
    let all_pending = HashSet::from([key("u", 0)]);
    let rows = [
        Row {
            name: "a committed offset",
            requested: vec![key("t", 0)],
            fail_append: false,
            want_reply: Ok(()),
            want_records: vec![tombstone("t", 0)],
            want_committed: HashSet::from([key("t", 2)]),
            want_pending: all_pending.clone(),
        },
        Row {
            name: "no offset",
            requested: vec![key("t", 1)],
            fail_append: false,
            want_reply: Ok(()),
            want_records: vec![],
            want_committed: all_committed.clone(),
            want_pending: all_pending.clone(),
        },
        Row {
            name: "only a pending transactional offset",
            requested: vec![key("u", 0)],
            fail_append: false,
            want_reply: Ok(()),
            want_records: vec![tombstone("u", 0)],
            want_committed: all_committed.clone(),
            want_pending: HashSet::new(),
        },
        Row {
            name: "mixed, in request order",
            requested: vec![key("u", 0), key("t", 1), key("t", 2), key("v", 0)],
            fail_append: false,
            want_reply: Ok(()),
            want_records: vec![tombstone("u", 0), tombstone("t", 2)],
            want_committed: HashSet::from([key("t", 0)]),
            want_pending: HashSet::new(),
        },
        Row {
            name: "the append fails",
            requested: vec![key("t", 0), key("u", 0)],
            fail_append: true,
            want_reply: Err(codes::NOT_LEADER_OR_FOLLOWER),
            want_records: vec![],
            want_committed: all_committed.clone(),
            want_pending: all_pending.clone(),
        },
    ];

    for row in rows {
        let (coordinator, log) = make_coordinator();
        let mut group = CoordinatorGroup::seeded(
            "g",
            GroupKind::Classic(ClassicGroup::new("g")),
            HashMap::from([
                (key("t", 0), offset_entry(10)),
                (key("t", 2), offset_entry(12)),
            ]),
        );
        group.add_pending_txn_offsets(7, 100, [key("u", 0)]);
        coordinator.seed_classic("g", Box::new(group));
        let handle = coordinator.find("g").unwrap();
        if row.fail_append {
            // `PartitionWriterDied`: `NOT_LEADER_OR_FOLLOWER`.
            log.fail_next_append(None);
        }

        let reply = rpc::begin(&handle, |reply| GroupActorMessage::DeleteOffsets {
            keys: row.requested,
            reply,
        })
        .await
        .await
        .unwrap();
        let offsets = rpc::fetch_offsets(&handle).await;
        let got = Outcome {
            reply,
            records: parsed_offset_records(&log.batches().await),
            committed: offsets.committed.into_keys().collect(),
            pending: offsets.pending_txn,
        };

        check!(
            got == Outcome {
                reply: row.want_reply,
                records: row.want_records,
                committed: row.want_committed,
                pending: row.want_pending,
            },
            "{}",
            row.name
        );
    }
}
