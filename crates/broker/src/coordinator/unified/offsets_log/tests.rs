//! The commit rule of [`ProductionOffsetsLog`]: an append succeeds only when
//! this broker leads the group's `__consumer_offsets` partition, and only
//! once the high watermark covers the write under the same leadership term.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use krabka_ids::{LeaderEpoch, Offset, PartitionIndex};
use krabka_metadata::{MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicRecord};
use krabka_protocol::records::{Attributes, Record, RecordBatch};
use tokio::sync::Notify;

use super::{LedTerm, OFFSETS_TOPIC, OffsetsLog, ProductionOffsetsLog, await_committed, led_epoch};
use crate::{
    codes, error::BrokerError, metadata_source::MetadataSource,
    partition_registry::PartitionRegistry, test_support::FakeMetadataSource,
};

/// This broker.
const NODE: NodeId = NodeId(1);

/// An image whose `__consumer_offsets` has one partition, led by `leader` at
/// `epoch`.
fn offsets_image(leader: u64, epoch: i32) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: OFFSETS_TOPIC.into(),
        topic_id: uuid::Uuid::from_u128(7),
        partitions: 1,
        replication_factor: 3,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: OFFSETS_TOPIC.into(),
        partition: 0,
        leader: NodeId(leader),
        leader_epoch: LeaderEpoch(epoch),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: vec![NodeId(1), NodeId(2), NodeId(3)],
        ..PartitionRecord::default()
    }));
    image
}

/// A two-record batch as the group coordinator builds one: offset 0 and the
/// default leader epoch of zero.
fn group_batch() -> RecordBatch {
    RecordBatch {
        base_offset: 0,
        partition_leader_epoch: 0,
        attributes: Attributes::default(),
        last_offset_delta: 1,
        base_timestamp: 1_700_000_000_000,
        max_timestamp: 1_700_000_000_000,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: (0..2)
            .map(|delta| Record {
                attributes: 0,
                offset_delta: delta,
                timestamp_delta: 0,
                key: Some(bytes::Bytes::from_static(b"k")),
                value: Some(bytes::Bytes::from_static(b"v")),
                headers: vec![],
            })
            .collect(),
    }
}

/// The partition and code of an uncommitted write, or `Ok` for a committed
/// one.
fn outcome(result: Result<(), BrokerError>) -> Result<(), (i32, i16)> {
    match result {
        Ok(()) => Ok(()),
        Err(BrokerError::CoordinatorWriteUncommitted { partition, code }) => Err((partition, code)),
        Err(other) => panic!("not a coordinator write error: {other}"),
    }
}

#[test]
fn led_epoch_is_the_term_this_broker_leads_the_partition_under() {
    let cases = [
        (
            "led by this broker",
            offsets_image(1, 4),
            Some(LeaderEpoch(4)),
        ),
        ("led by another broker", offsets_image(2, 4), None),
        (
            "not in the image",
            MetadataImage::new(uuid::Uuid::nil()),
            None,
        ),
    ];
    for (what, image, expected) in cases {
        assert!(led_epoch(&image, 0, NODE) == expected, "{what}");
    }
}

/// Kafka's `CoordinatorRuntime` completes a write when the high watermark
/// passes it, and fails it with `NOT_COORDINATOR` when the broker loses the
/// partition first. `ShareConsumerTest.test_broker_failure` lost a share
/// group because a coordinator answered heartbeats whose records the next
/// leader of the partition never got.
#[tokio::test]
async fn a_write_completes_only_when_committed_under_its_term() {
    use crate::coordinator::test_support::{CommitWaitOutcome, commit_wait_cases};
    for case in commit_wait_cases() {
        let expected = match case.expected {
            CommitWaitOutcome::Committed => Ok(()),
            CommitWaitOutcome::NotLeader => Err((0, codes::NOT_COORDINATOR)),
            CommitWaitOutcome::TimedOut => Err((0, codes::COORDINATOR_NOT_AVAILABLE)),
        };
        let hw_notify = Arc::new(Notify::new());
        let (partition, _dir) =
            crate::partition::test_support::test_partition(Arc::clone(&hw_notify));
        let partition = Arc::new(partition);
        partition.replica_state.lock().await.hw = Offset(case.hw_now);
        let metadata = Arc::new(
            FakeMetadataSource::builder()
                .image(offsets_image(1, 0))
                .build(),
        );
        let mut images = metadata.watch_image();
        images.borrow_and_update();
        let term = LedTerm {
            partition: 0,
            node_id: NODE,
            epoch: LeaderEpoch(0),
        };

        let changes = {
            let metadata = Arc::clone(&metadata);
            let partition = Arc::clone(&partition);
            let (moves_to, hw_later) = (case.moves_to, case.hw_later);
            tokio::spawn(async move {
                // intentional: the wait has to start before the changes land.
                tokio::time::sleep(Duration::from_millis(20)).await;
                if let Some((leader, epoch)) = moves_to {
                    metadata.set_image(offsets_image(leader, epoch));
                }
                if let Some(hw) = hw_later {
                    partition.replica_state.lock().await.hw = Offset(hw);
                    hw_notify.notify_waiters();
                }
            })
        };
        let result = await_committed(&partition, &mut images, term, Offset(2), case.timeout).await;
        changes.await.expect("the changes land");

        assert!(outcome(result) == expected, "{}", case.what);
    }
}

/// The append path end to end: a broker that does not lead the partition
/// writes nothing and answers `NOT_COORDINATOR`, and the leader stamps its
/// leader epoch on the batch and completes once the write commits.
#[tokio::test]
async fn an_append_writes_only_as_the_partition_leader() {
    struct Case {
        what: &'static str,
        image: MetadataImage,
        expected: Result<(), (i32, i16)>,
        /// The leader epochs of the batches in the log afterwards.
        logged_epochs: Vec<i32>,
    }
    let cases = [
        Case {
            what: "this broker leads at epoch 3",
            image: offsets_image(1, 3),
            expected: Ok(()),
            logged_epochs: vec![3],
        },
        Case {
            what: "another broker leads",
            image: offsets_image(2, 3),
            expected: Err((0, codes::NOT_COORDINATOR)),
            logged_epochs: vec![],
        },
    ];
    for case in cases {
        let (partition, _dir) = crate::partition::test_support::test_partition_with_writer();
        let log = Arc::clone(&partition.log);
        let partitions = Arc::new(PartitionRegistry::new());
        partitions.insert(
            Arc::from(OFFSETS_TOPIC),
            PartitionIndex(0),
            Arc::new(partition),
        );
        let metadata: Arc<dyn MetadataSource> =
            Arc::new(FakeMetadataSource::builder().image(case.image).build());
        let offsets_log = ProductionOffsetsLog::new(partitions, metadata, NODE);

        let result = offsets_log.append("group", group_batch()).await;

        let logged_epochs = {
            let log = log.lock().expect("the log is not poisoned");
            let end = log.log_end_offset();
            if end == Offset(0) {
                Vec::new()
            } else {
                read_epochs(&log, end)
            }
        };
        assert!(outcome(result) == case.expected, "{}", case.what);
        assert!(logged_epochs == case.logged_epochs, "{}", case.what);
    }
}

/// The leader epoch of every batch in `log` below `end`.
fn read_epochs(log: &krabka_log::Log, end: Offset) -> Vec<i32> {
    let read = log
        .read_raw(Offset(0), end, krabka_units::mebibytes(1))
        .expect("read the log");
    let mut cursor: &[u8] = &read.bytes;
    let mut epochs = Vec::new();
    while !cursor.is_empty() {
        let batch = RecordBatch::decode(&mut cursor).expect("decode a batch");
        epochs.push(batch.partition_leader_epoch);
    }
    epochs
}

#[tokio::test]
async fn fake_records_in_order() {
    let log = super::fake::InMemoryOffsetsLog::default();
    let b1 = RecordBatch::default();
    let b2 = RecordBatch {
        max_timestamp: 42,
        ..Default::default()
    };
    log.append("g", b1.clone()).await.unwrap();
    log.append("g", b2.clone()).await.unwrap();
    let got = log.batches().await;
    assert!(got.len() == 2);
    assert!(got[1].max_timestamp == 42);
}

#[tokio::test]
async fn fake_fails_when_armed() {
    let log = super::fake::InMemoryOffsetsLog::default();
    log.fail_next
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(log.append("g", RecordBatch::default()).await.is_err());
    assert!(log.append("g", RecordBatch::default()).await.is_ok());
}
