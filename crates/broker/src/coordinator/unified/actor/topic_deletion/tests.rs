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

fn entry(offset: i64) -> OffsetEntry {
    OffsetEntry {
        offset: Offset(offset),
        leader_epoch: -1,
        metadata: String::new(),
        commit_timestamp_ms: 0,
        expire_timestamp_ms: None,
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
    let all_committed = HashSet::from([key("orders", 0), key("orders", 1), key("payments", 0)]);
    let rows = [
        Row {
            name: "one of two topics",
            deleted: vec!["orders"],
            fail_append: false,
            expected_reply: vec![key("orders", 0), key("orders", 1), key("orders", 2)],
            expected_records: vec![
                tombstone("orders", 0),
                tombstone("orders", 1),
                tombstone("orders", 2),
            ],
            expected_committed: HashSet::from([key("payments", 0)]),
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
                (key("orders", 0), entry(10)),
                (key("orders", 1), entry(11)),
                (key("payments", 0), entry(20)),
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
                topics: row.deleted.iter().map(ToString::to_string).collect(),
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
