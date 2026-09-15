//! `DeleteGroups` across a coordinator reload.
//!
//! A deleted group must stay deleted when its `__consumer_offsets` partition
//! replays again. Kafka's `GroupCoordinatorShard.deleteGroups` writes an
//! `OffsetCommit` tombstone for every committed offset and every open
//! transactional offset of the group, then the group tombstone, all in one
//! batch. These tests delete a replayed group, append the delete batch to the
//! same log, and replay the log into a new coordinator.

use std::sync::Arc;

use assert2::check;
use bytes::Bytes;
use krabka_log::{Offset, ProducerId};
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use tempfile::tempdir;

use super::replay::{finalize, replay_records};
use crate::{
    coordinator::{
        persistence::{Key, OffsetCommitValue, parse_key},
        unified::{
            GroupCoordinator, actor::MetadataProvider, offsets_log::fake::InMemoryOffsetsLog,
            reconciler::ReconcileInput,
        },
    },
    txn::marker::{MarkerType, build_marker_batch},
};

#[derive(Debug)]
struct EmptyMeta;

impl MetadataProvider for EmptyMeta {
    fn snapshot(&self) -> ReconcileInput {
        ReconcileInput::default()
    }
}

fn coordinator(log: Arc<InMemoryOffsetsLog>) -> Arc<GroupCoordinator> {
    Arc::new(GroupCoordinator::new(
        crate::coordinator::unified::config::NextGenConfig::default(),
        crate::coordinator::unified::share::config::ShareGroupConfig::default(),
        Arc::new(EmptyMeta),
        log,
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ))
}

fn commit(topic: &str, partition: i32, offset: i64) -> Record {
    Record {
        key: Some(OffsetCommitValue::encode_key("g", topic, partition)),
        value: Some(
            OffsetCommitValue {
                offset: Offset(offset),
                leader_epoch: -1,
                metadata: String::new(),
                commit_timestamp_ms: 0,
                expire_timestamp_ms: None,
            }
            .encode_value(),
        ),
        ..Default::default()
    }
}

fn batch(producer_id: Option<i64>, records: Vec<Record>) -> RecordBatch {
    let records: Vec<Record> = records
        .into_iter()
        .zip(0..)
        .map(|(record, offset_delta)| Record {
            offset_delta,
            ..record
        })
        .collect();
    RecordBatch {
        producer_id: producer_id.unwrap_or(-1),
        producer_epoch: if producer_id.is_some() { 0 } else { -1 },
        attributes: Attributes::default().with_transactional(producer_id.is_some()),
        last_offset_delta: i32::try_from(records.len()).unwrap() - 1,
        records,
        ..RecordBatch::default()
    }
}

fn offset_tombstone(topic: &str, partition: i32) -> (Key, Option<Bytes>) {
    (
        Key::OffsetCommit {
            group_id: "g".into(),
            topic: topic.into(),
            partition,
        },
        None,
    )
}

fn group_tombstone() -> (Key, Option<Bytes>) {
    (
        Key::GroupMetadata {
            group_id: "g".into(),
        },
        None,
    )
}

struct Row {
    name: &'static str,
    /// Plain offset commits of group `g` in the log before the delete.
    committed: Vec<Record>,
    /// Offset commits of an open transaction of producer 7.
    transactional: Vec<Record>,
    /// Append a commit marker for producer 7 after the delete batch.
    commit_after_delete: bool,
    expected_batch: Vec<(Key, Option<Bytes>)>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_group_and_its_offsets_stay_deleted_after_replay() {
    let rows = [
        Row {
            name: "no offsets",
            committed: vec![],
            transactional: vec![],
            commit_after_delete: false,
            expected_batch: vec![group_tombstone()],
        },
        Row {
            name: "two offsets on two topics",
            committed: vec![commit("orders", 0, 10), commit("payments", 3, 30)],
            transactional: vec![],
            commit_after_delete: false,
            expected_batch: vec![
                offset_tombstone("orders", 0),
                offset_tombstone("payments", 3),
                group_tombstone(),
            ],
        },
        Row {
            name: "one pending transactional offset",
            committed: vec![commit("orders", 0, 10)],
            transactional: vec![commit("orders", 1, 11)],
            commit_after_delete: false,
            expected_batch: vec![
                offset_tombstone("orders", 0),
                offset_tombstone("orders", 1),
                group_tombstone(),
            ],
        },
        Row {
            name: "transaction commits after the delete",
            committed: vec![commit("orders", 0, 10)],
            transactional: vec![commit("orders", 0, 12), commit("orders", 1, 11)],
            commit_after_delete: true,
            expected_batch: vec![
                offset_tombstone("orders", 0),
                offset_tombstone("orders", 1),
                group_tombstone(),
            ],
        },
    ];

    for row in rows {
        let dir = tempdir().unwrap();
        let mut log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
        // A classic GroupMetadata record makes the group exist even when it has
        // no offsets.
        let (group_key, group_value) = super::test_support::classic_group_record("g", "m1");
        let empty_group = crate::coordinator::persistence::GroupMetadataValue {
            members: vec![],
            leader: None,
            ..crate::coordinator::persistence::GroupMetadataValue::decode_value(&group_value)
                .unwrap()
        };
        log.append(&mut batch(
            None,
            vec![Record {
                key: Some(group_key),
                value: Some(empty_group.encode_value()),
                ..Default::default()
            }],
        ))
        .unwrap();
        if !row.committed.is_empty() {
            log.append(&mut batch(None, row.committed)).unwrap();
        }
        if !row.transactional.is_empty() {
            log.append(&mut batch(Some(7), row.transactional)).unwrap();
        }

        let offsets_log = Arc::new(InMemoryOffsetsLog::default());
        let before = coordinator(offsets_log.clone());
        finalize(&before, replay_records(&log, &before).unwrap()).await;
        check!(before.delete_group("g").await == Ok(()), "{}", row.name);

        let appended = offsets_log.batches().await;
        let written: Vec<(Key, Option<Bytes>)> = appended
            .iter()
            .flat_map(|batch| &batch.records)
            .map(|record| {
                (
                    parse_key(record.key.as_ref().unwrap()).unwrap(),
                    record.value.clone(),
                )
            })
            .collect();
        check!(appended.len() == 1, "{}", row.name);
        check!(written == row.expected_batch, "{}", row.name);

        for mut delete in appended {
            log.append(&mut delete).unwrap();
        }
        if row.commit_after_delete {
            log.append(&mut build_marker_batch(
                ProducerId(7),
                0,
                Offset(0),
                MarkerType::Commit,
                0,
            ))
            .unwrap();
        }

        let after = coordinator(Arc::new(InMemoryOffsetsLog::default()));
        let replayed = replay_records(&log, &after).unwrap();
        check!(!replayed.committed.contains_key("g"), "{}", row.name);
        check!(!replayed.pending_txn.contains_key("g"), "{}", row.name);
        finalize(&after, replayed).await;
        check!(after.find("g").is_none(), "{}", row.name);
    }
}
