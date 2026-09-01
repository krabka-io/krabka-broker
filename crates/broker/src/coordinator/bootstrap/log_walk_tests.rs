//! Tests for the log walk itself: the inter-batch cursor that
//! [`super::replay::replay_records`] advances, and the transactional records
//! it holds back until a commit marker arrives.

use std::sync::Arc;

use assert2::check;
use krabka_protocol::records::RecordBatch;
use tempfile::tempdir;

use super::replay::replay_records;
use crate::coordinator::persistence::OffsetCommitValue;

/// `replay_records` must walk EVERY batch in the log, not the first batch
/// only.
///
/// The cursor that advances between batches is
/// `base_offset + last_offset_delta + 1`. A two-record first batch, where
/// `last_offset_delta == 1`, followed by a second batch replays fully only
/// when that arithmetic is exact. Both offset-commit records from the
/// first batch AND the commit from the second batch must land in
/// `acc.committed`.
#[tokio::test]
async fn replay_records_walks_all_batches() {
    use krabka_log::Offset;
    use krabka_protocol::records::Record;

    use crate::coordinator::unified::{
        GroupCoordinator, offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
    };

    #[derive(Debug)]
    struct EmptyMeta;
    impl crate::coordinator::unified::actor::MetadataProvider for EmptyMeta {
        fn snapshot(&self) -> ReconcileInput {
            ReconcileInput::default()
        }
    }

    let coord = Arc::new(GroupCoordinator::new(
        crate::coordinator::unified::config::NextGenConfig::default(),
        crate::coordinator::unified::share::config::ShareGroupConfig::default(),
        Arc::new(EmptyMeta),
        Arc::new(InMemoryOffsetsLog::default()),
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ));

    // Build an offset-commit record with a distinct committed offset per
    // (topic, partition), so we can tell which batches were replayed.
    let commit_record = |partition: i32, offset: i64| Record {
        offset_delta: 0,
        key: Some(OffsetCommitValue::encode_key("g", "t", partition)),
        value: Some(
            OffsetCommitValue {
                offset: Offset(offset),
                leader_epoch: -1,
                metadata: String::new(),
                commit_timestamp_ms: 0,
            }
            .encode_value(),
        ),
        ..Default::default()
    };

    let dir = tempdir().unwrap();
    let mut log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();

    // First batch spans TWO offsets (last_offset_delta == 1): partitions 0
    // and 1 commit at offsets 100 and 101.
    let mut batch1 = RecordBatch {
        last_offset_delta: 1,
        ..RecordBatch::default()
    };
    batch1.records.push(commit_record(0, 100));
    batch1.records.push(Record {
        offset_delta: 1,
        ..commit_record(1, 101)
    });
    log.append(&mut batch1).unwrap();

    // Second batch: partition 2 commits at offset 202. Reaching it requires
    // the inter-batch cursor to have advanced past the first batch.
    let mut batch2 = RecordBatch::default();
    batch2.records.push(commit_record(2, 202));
    log.append(&mut batch2).unwrap();

    let replayed = replay_records(&log, &coord).unwrap();
    let committed = replayed
        .committed
        .get("g")
        .expect("group g has committed offsets");

    // All three commits present — the second batch is only reached when the
    // cursor arithmetic `base_offset + last_offset_delta + 1` is exact.
    check!(committed.len() == 3);
    check!(committed[&("t".to_string(), 0)].offset == 100);
    check!(committed[&("t".to_string(), 1)].offset == 101);
    check!(committed[&("t".to_string(), 2)].offset == 202);
}

#[test]
fn replay_applies_only_committed_transactional_offsets() {
    use krabka_log::{Offset, ProducerId};
    use krabka_protocol::records::{Attributes, Record};

    use crate::{
        coordinator::unified::{
            GroupCoordinator, offsets_log::fake::InMemoryOffsetsLog, reconciler::ReconcileInput,
        },
        txn::marker::{MarkerType, build_marker_batch},
    };

    #[derive(Debug)]
    struct EmptyMeta;
    impl crate::coordinator::unified::actor::MetadataProvider for EmptyMeta {
        fn snapshot(&self) -> ReconcileInput {
            ReconcileInput::default()
        }
    }

    let coordinator = Arc::new(GroupCoordinator::new(
        crate::coordinator::unified::config::NextGenConfig::default(),
        crate::coordinator::unified::share::config::ShareGroupConfig::default(),
        Arc::new(EmptyMeta),
        Arc::new(InMemoryOffsetsLog::default()),
        crate::coordinator::unified::streams::config::StreamsGroupConfig::default(),
    ));
    let record = |partition: i32, offset: i64| Record {
        key: Some(OffsetCommitValue::encode_key("g", "t", partition)),
        value: Some(
            OffsetCommitValue {
                offset: Offset(offset),
                leader_epoch: -1,
                metadata: String::new(),
                commit_timestamp_ms: 0,
            }
            .encode_value(),
        ),
        ..Default::default()
    };
    let transactional = |partition: i32, offset: i64| RecordBatch {
        producer_id: 7,
        producer_epoch: 0,
        attributes: Attributes::default().with_transactional(true),
        records: vec![record(partition, offset)],
        ..RecordBatch::default()
    };

    let dir = tempdir().unwrap();
    let mut log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
    log.append(&mut transactional(0, 111)).unwrap();
    log.append(&mut build_marker_batch(
        ProducerId(7),
        0,
        Offset(0),
        MarkerType::Commit,
        0,
    ))
    .unwrap();
    log.append(&mut transactional(1, 222)).unwrap();
    log.append(&mut build_marker_batch(
        ProducerId(7),
        0,
        Offset(0),
        MarkerType::Abort,
        0,
    ))
    .unwrap();
    log.append(&mut transactional(2, 333)).unwrap();

    let replayed = replay_records(&log, &coordinator).unwrap();
    let committed = replayed.committed.get("g").expect("committed transaction");
    check!(committed.len() == 1);
    check!(committed[&("t".to_string(), 0)].offset == 111);
    check!(!committed.contains_key(&("t".to_string(), 1)));
    check!(!committed.contains_key(&("t".to_string(), 2)));
}

/// KIP-447 across a coordinator reload. A transaction whose offset commits the
/// log ends without a marker for is still open: its marker can arrive after
/// the reload. Replay must not apply those offsets, and must not forget them
/// either -- the group has to keep answering `UNSTABLE_OFFSET_COMMIT` to a
/// `require_stable` `OffsetFetch` for exactly those keys.
#[tokio::test]
async fn replay_carries_an_open_transactions_offsets_forward_as_pending() {
    use krabka_log::Offset;
    use krabka_protocol::records::{Attributes, Record};
    use tokio::sync::oneshot;

    use super::{replay::finalize, test_support::bare_coordinator};
    use crate::coordinator::unified::actor::GroupActorMessage;

    let coordinator = bare_coordinator();
    let dir = tempdir().unwrap();
    let mut log = krabka_log::Log::open(dir.path(), krabka_log::LogConfig::default()).unwrap();
    log.append(&mut RecordBatch {
        producer_id: 7,
        producer_epoch: 0,
        attributes: Attributes::default().with_transactional(true),
        records: vec![Record {
            key: Some(OffsetCommitValue::encode_key("g", "t", 4)),
            value: Some(
                OffsetCommitValue {
                    offset: Offset(444),
                    leader_epoch: -1,
                    metadata: String::new(),
                    commit_timestamp_ms: 0,
                }
                .encode_value(),
            ),
            ..Default::default()
        }],
        ..RecordBatch::default()
    })
    .unwrap();

    let replayed = replay_records(&log, &coordinator).unwrap();
    check!(!replayed.committed.contains_key("g"));
    check!(
        replayed.pending_txn["g"][&7] == std::collections::HashSet::from([("t".to_string(), 4)])
    );

    // The group is offset-only and open-transaction-only, so `finalize` still
    // has to spawn an actor for it and hand it the marks.
    finalize(&coordinator, replayed);
    let handle = coordinator.find("g").expect("seeded classic actor");
    let (reply, offsets) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::FetchOffsets { reply })
        .await
        .unwrap();
    let offsets = offsets.await.unwrap();
    check!(offsets.committed.is_empty());
    check!(offsets.pending_txn == std::collections::HashSet::from([("t".to_string(), 4)]));
}
