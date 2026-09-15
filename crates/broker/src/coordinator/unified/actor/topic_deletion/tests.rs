//! Unit tests for tombstoning the offsets of deleted topics.

use std::collections::{HashMap, HashSet};

use assert2::check;
use bytes::Bytes;
use krabka_log::Offset;
use tokio::sync::oneshot;

use crate::coordinator::unified::{
    actor::{GroupActorMessage, test_support::make_coordinator},
    classic_state::{ClassicGroup, OffsetEntry},
    group::{CoordinatorGroup, GroupKind},
    persistence::{Key, parse_key},
};

/// The id of the deleted `orders` topic.
const OLD_ORDERS: uuid::Uuid = uuid::Uuid::from_u128(1);
/// The id of the `orders` topic created again with the same name.
const NEW_ORDERS: uuid::Uuid = uuid::Uuid::from_u128(2);

fn entry(offset: i64, topic_id: Option<uuid::Uuid>) -> OffsetEntry {
    OffsetEntry {
        offset: Offset(offset),
        leader_epoch: -1,
        metadata: String::new(),
        commit_timestamp_ms: 0,
        expire_timestamp_ms: None,
        topic_id,
    }
}

fn key(topic: &str, partition: i32) -> (String, i32) {
    (topic.to_string(), partition)
}

fn tombstone(topic: &str, partition: i32) -> (Key, Option<Bytes>) {
    (
        Key::OffsetCommit {
            group_id: "g".into(),
            topic: topic.into(),
            partition,
        },
        None,
    )
}

struct Row {
    name: &'static str,
    deleted: Vec<&'static str>,
    fail_append: bool,
    expected_reply: Vec<(String, i32)>,
    expected_records: Vec<(Key, Option<Bytes>)>,
    expected_committed: HashSet<(String, i32)>,
    expected_pending: HashSet<(String, i32)>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_topic_tombstones_its_offsets_in_the_group() {
    let all_committed = HashSet::from([
        key("orders", 0),
        key("orders", 1),
        key("orders", 3),
        key("payments", 0),
    ]);
    let rows = [
        Row {
            name: "one of two topics, and the offset of the new topic stays",
            deleted: vec!["orders"],
            fail_append: false,
            expected_reply: vec![key("orders", 0), key("orders", 1), key("orders", 2)],
            expected_records: vec![
                tombstone("orders", 0),
                tombstone("orders", 1),
                tombstone("orders", 2),
            ],
            expected_committed: HashSet::from([key("orders", 3), key("payments", 0)]),
            expected_pending: HashSet::from([key("payments", 1)]),
        },
        Row {
            name: "a topic the group never committed",
            deleted: vec!["unknown"],
            fail_append: false,
            expected_reply: vec![],
            expected_records: vec![],
            expected_committed: all_committed.clone(),
            expected_pending: HashSet::from([key("orders", 2), key("payments", 1)]),
        },
        Row {
            name: "the append fails",
            deleted: vec!["orders", "payments"],
            fail_append: true,
            expected_reply: vec![],
            expected_records: vec![],
            expected_committed: all_committed.clone(),
            expected_pending: HashSet::from([key("orders", 2), key("payments", 1)]),
        },
    ];

    for row in rows {
        let (coordinator, log) = make_coordinator();
        let mut group = CoordinatorGroup::seeded(
            "g",
            GroupKind::Classic(ClassicGroup::new("g")),
            HashMap::from([
                // Replayed from the log, so no topic id.
                (key("orders", 0), entry(10, None)),
                (key("orders", 1), entry(11, Some(OLD_ORDERS))),
                // Committed to the topic created again with the same name.
                (key("orders", 3), entry(13, Some(NEW_ORDERS))),
                (key("payments", 0), entry(20, None)),
            ]),
        );
        group.add_pending_txn_offsets(7, 100, [key("orders", 2)]);
        group.add_pending_txn_offsets(8, 101, [key("payments", 1)]);
        coordinator.seed_classic("g", Box::new(group));
        let handle = coordinator.find("g").unwrap();
        log.fail_next
            .store(row.fail_append, std::sync::atomic::Ordering::SeqCst);

        let (reply, deleted) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::DeleteTopicOffsets {
                topics: row
                    .deleted
                    .iter()
                    .map(|name| ((*name).to_string(), OLD_ORDERS))
                    .collect(),
                reply,
            })
            .await
            .unwrap();
        check!(deleted.await.unwrap() == row.expected_reply, "{}", row.name);

        let records: Vec<(Key, Option<Bytes>)> = log
            .batches()
            .await
            .iter()
            .flat_map(|batch| &batch.records)
            .map(|record| {
                (
                    parse_key(record.key.as_ref().unwrap()).unwrap(),
                    record.value.clone(),
                )
            })
            .collect();
        check!(records == row.expected_records, "{}", row.name);

        let (reply, offsets) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchOffsets { reply })
            .await
            .unwrap();
        let offsets = offsets.await.unwrap();
        check!(
            offsets.committed.into_keys().collect::<HashSet<_>>() == row.expected_committed,
            "{}",
            row.name
        );
        check!(offsets.pending_txn == row.expected_pending, "{}", row.name);
    }
}
