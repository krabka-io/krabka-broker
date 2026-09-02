//! Appending one transaction marker to a partition, and the offset resolution
//! that a `__consumer_offsets` marker triggers.
//!
//! Both the inter-broker `WriteTxnMarkers` handler and `EndTxn`'s direct local
//! path go through this module rather than calling the partition themselves,
//! so that the durable marker and the in-memory publication can never drift
//! apart.

use std::{collections::HashMap, sync::Arc};

use krabka_verified::transaction::TransactionMarkerMaterializationDecision as Decision;

use super::offsets::{pending_offset_entries, resolve_pending_offsets};
use crate::{
    coordinator::{bootstrap::OFFSETS_TOPIC, unified::GroupCoordinator},
    error::BrokerError,
    txn::marker::{MarkerType, build_marker_batch},
};

/// Append a transaction marker and, on a `__consumer_offsets` partition,
/// resolve the offset commits the transaction wrote against the local group
/// actors: a commit publishes them, an abort discards them, and both drop the
/// KIP-447 pending marks that made them answer `UNSTABLE_OFFSET_COMMIT`.
///
/// Both the inter-broker `WriteTxnMarkers` handler and `EndTxn`'s direct local
/// path must use this function. Keeping marker append and actor resolution in
/// one path prevents local transactions from becoming durable in the log but
/// remaining invisible — or, after an abort, permanently unstable — until the
/// next coordinator replay.
///
/// The log scan runs before the append, because it starts from the producer's
/// first unresolved record and the marker itself ends that transaction.
///
/// A commit needs the group coordinator, because losing it would lose offsets
/// the transaction made durable. An abort publishes nothing, so a caller with
/// no coordinator — and therefore no group actors holding pending marks — has
/// nothing to resolve.
pub(crate) async fn append_marker_and_materialize(
    partition: &crate::partition::Partition,
    group_coordinator: Option<&Arc<GroupCoordinator>>,
    topic: &str,
    marker: MarkerAppend,
) -> Result<(), BrokerError> {
    let MarkerAppend {
        producer_id,
        producer_epoch,
        marker_type,
        coordinator_epoch,
        commit_stamp,
    } = marker;
    if commit_stamp.is_some() && marker_type != MarkerType::Commit {
        return Err(BrokerError::Txn(
            "a transaction commit stamp cannot be attached to an abort marker".into(),
        ));
    }

    // The guard spans admission, durable append, and offset publication. Two
    // conflicting marker requests therefore cannot both observe the same
    // pending transaction and publish different outcomes.
    let mut materialization = partition.marker_materialization.lock().await;
    if let Some((resolved_through, offsets)) = materialization.get(&producer_id).cloned() {
        let coordinator = group_coordinator.ok_or_else(|| {
            BrokerError::Txn("cannot retry committed offsets without a group coordinator".into())
        })?;
        resolve_pending_offsets(
            coordinator,
            producer_id,
            MarkerType::Commit,
            resolved_through,
            offsets,
        )
        .await?;
        materialization.remove(&producer_id);
    }
    let (current_producer_epoch, current_coordinator_epoch, has_pending_transaction) = partition
        .log
        .lock()
        .map_err(|_| BrokerError::Txn("transaction marker log lock poisoned".into()))?
        .transaction_marker_state(producer_id);
    let decision = krabka_verified::transaction_marker_materialization_decision(
        (producer_id.get(), producer_epoch, coordinator_epoch),
        (
            current_producer_epoch,
            current_coordinator_epoch,
            has_pending_transaction,
        ),
        (marker_type == MarkerType::Commit, topic == OFFSETS_TOPIC),
    );
    match decision {
        Decision::RejectMalformed => {
            return Err(BrokerError::Txn(
                "transaction marker contains a malformed producer or coordinator generation".into(),
            ));
        }
        Decision::RejectProducerEpoch => {
            return Err(BrokerError::ProducerEpochFenced {
                producer_id: producer_id.get(),
                current: current_producer_epoch,
                requested: producer_epoch,
            });
        }
        Decision::RejectCoordinatorEpoch => {
            return Err(BrokerError::CoordinatorEpochFenced {
                current: current_coordinator_epoch,
                requested: coordinator_epoch,
            });
        }
        Decision::Retry => return Ok(()),
        Decision::AppendAndPublishOffsets | Decision::AppendWithoutOffsetPublication => {}
    }
    let pending_offsets = if topic == OFFSETS_TOPIC {
        match (marker_type, group_coordinator) {
            (_, Some(coordinator)) => (
                Some(coordinator),
                pending_offset_entries(partition, producer_id)?,
            ),
            (MarkerType::Commit, None) => {
                return Err(BrokerError::Txn(
                    "cannot commit transactional offsets without a group coordinator".into(),
                ));
            }
            (MarkerType::Abort, None) => (None, HashMap::new()),
        }
    } else {
        (None, HashMap::new())
    };

    let mut marker = build_marker_batch(
        producer_id,
        producer_epoch,
        partition.log_end_offset(),
        marker_type,
        coordinator_epoch,
    );
    // The owned produce path stamps this field from the metadata image. A
    // marker does not travel that path, so it stamps its own, and a marker
    // that kept the default of zero would carry a false leader epoch.
    marker.partition_leader_epoch = partition
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    let marker_offset = if let Some(stamp) = commit_stamp {
        partition.produce_commit_marker(marker, stamp).await?
    } else {
        // A control batch takes the control append path, which applies no
        // compression rewrite. Kafka never compresses a control batch that
        // arrived uncompressed.
        partition.produce_control_batch(marker).await?
    };

    if let (Some(coordinator), offsets) = pending_offsets {
        if marker_type == MarkerType::Commit {
            // Retain the decoded publication until the actor acknowledges it.
            // An exact marker retry drains this entry without another append.
            materialization.insert(producer_id, (marker_offset.get(), offsets.clone()));
        }
        // The marker's own log position resolves the KIP-447 marks: it is what
        // tells a group actor that a mark still on its way, for records below
        // it, belongs to the transaction this marker ends.
        resolve_pending_offsets(
            coordinator,
            producer_id,
            marker_type,
            marker_offset.get(),
            offsets,
        )
        .await?;
        materialization.remove(&producer_id);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MarkerAppend {
    pub(crate) producer_id: krabka_log::ProducerId,
    pub(crate) producer_epoch: i16,
    pub(crate) marker_type: MarkerType,
    pub(crate) coordinator_epoch: i32,
    pub(crate) commit_stamp: Option<u64>,
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use krabka_ids::PartitionIndex;

    use super::*;
    use crate::{
        coordinator::{
            persistence::OffsetCommitValue,
            unified::{actor::GroupActorMessage, classic_state::OffsetEntry},
        },
        txn::handlers::write_txn_markers::{
            CommittedOffsets,
            test_support::{open_partition, start_broker},
        },
    };

    #[tokio::test]
    async fn aborted_offsets_are_not_published_by_the_offsets_partition_marker() {
        use krabka_log::Offset;
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "marker-abort-group";
        let offsets_partition = crate::coordinator::partitioner::partition_for_group(
            &broker.controller.current_image(),
            group_id,
        );
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
            .expect("local offsets partition");
        let producer_id = krabka_log::ProducerId(92);
        part.produce_batch(RecordBatch {
            producer_id: producer_id.get(),
            producer_epoch: 5,
            base_sequence: 0,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                key: Some(OffsetCommitValue::encode_key(group_id, "orders", 3)),
                value: Some(
                    OffsetCommitValue {
                        offset: Offset(99),
                        leader_epoch: 4,
                        metadata: "aborted".into(),
                        commit_timestamp_ms: 456,
                        expire_timestamp_ms: None,
                    }
                    .encode_value(),
                ),
                ..Record::default()
            }],
            ..RecordBatch::default()
        })
        .await
        .expect("append transactional offset");

        append_marker_and_materialize(
            &part,
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            MarkerAppend {
                producer_id,
                producer_epoch: 5,
                marker_type: MarkerType::Abort,
                coordinator_epoch: 0,
                commit_stamp: None,
            },
        )
        .await
        .expect("abort marker");

        assert!(broker.group_coordinator.find(group_id).is_none());
        {
            let log = part.log.lock().expect("offsets log lock");
            assert!(log.pending_transaction_start(producer_id).is_none());
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn internal_marker_path_records_supplied_commit_stamp() {
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        let (broker_handle, dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        open_partition(&broker, dir.path(), "stamped-orders", 0);
        let part = broker
            .partitions
            .get("stamped-orders", PartitionIndex(0))
            .expect("local partition");
        part.log
            .lock()
            .expect("partition log")
            .set_stamp_source(Arc::new(krabka_log::MonotonicStampSource::new(1, 1)))
            .expect("install stamp source");

        let producer_id = krabka_log::ProducerId(700);
        part.produce_batch(RecordBatch {
            producer_id: producer_id.get(),
            producer_epoch: 2,
            base_sequence: 0,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                value: Some(Bytes::from_static(b"event")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        })
        .await
        .expect("append transactional data");
        assert!(part.stamp_for_offset(krabka_log::Offset(0)).is_none());

        append_marker_and_materialize(
            &part,
            None,
            "stamped-orders",
            MarkerAppend {
                producer_id,
                producer_epoch: 2,
                marker_type: MarkerType::Commit,
                coordinator_epoch: 0,
                commit_stamp: Some(900),
            },
        )
        .await
        .expect("commit marker");

        assert!(part.stamp_for_offset(krabka_log::Offset(0)) == Some(900));
        assert!(part.stamp_for_offset(krabka_log::Offset(1)).is_none());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn marker_adapter_fences_generations_and_suppresses_exact_retries() {
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        let (broker_handle, dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        open_partition(&broker, dir.path(), "fenced-orders", 0);
        let part = broker
            .partitions
            .get("fenced-orders", PartitionIndex(0))
            .expect("local partition");
        let producer_id = krabka_log::ProducerId(701);
        let append_data = |epoch| RecordBatch {
            producer_id: producer_id.get(),
            producer_epoch: epoch,
            base_sequence: 0,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                value: Some(Bytes::from_static(b"event")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        part.produce_batch(append_data(5))
            .await
            .expect("append first transaction");
        let before_marker = part.log_end_offset();

        let malformed = append_marker_and_materialize(
            &part,
            None,
            "fenced-orders",
            MarkerAppend {
                producer_id: krabka_log::ProducerId(-1),
                producer_epoch: 0,
                marker_type: MarkerType::Commit,
                coordinator_epoch: 0,
                commit_stamp: None,
            },
        )
        .await;
        assert!(matches!(malformed, Err(BrokerError::Txn(_))));

        let stale_producer = append_marker_and_materialize(
            &part,
            None,
            "fenced-orders",
            MarkerAppend {
                producer_id,
                producer_epoch: 4,
                marker_type: MarkerType::Commit,
                coordinator_epoch: 10,
                commit_stamp: None,
            },
        )
        .await;
        assert!(matches!(
            stale_producer,
            Err(BrokerError::ProducerEpochFenced { .. })
        ));
        assert!(part.log_end_offset() == before_marker);

        let marker = MarkerAppend {
            producer_id,
            producer_epoch: 5,
            marker_type: MarkerType::Commit,
            coordinator_epoch: 10,
            commit_stamp: None,
        };
        let conflicting_marker = MarkerAppend {
            marker_type: MarkerType::Abort,
            ..marker
        };
        let (first, second) = tokio::join!(
            append_marker_and_materialize(&part, None, "fenced-orders", marker),
            append_marker_and_materialize(&part, None, "fenced-orders", conflicting_marker),
        );
        first.expect("append current marker");
        second.expect("suppress conflicting completed marker");
        let after_marker = part.log_end_offset();
        assert!(after_marker == before_marker + 1);

        append_marker_and_materialize(&part, None, "fenced-orders", marker)
            .await
            .expect("exact marker retry");
        assert!(part.log_end_offset() == after_marker);

        part.produce_batch(append_data(6))
            .await
            .expect("append next transaction");
        let before_stale_coordinator = part.log_end_offset();
        let stale_coordinator = append_marker_and_materialize(
            &part,
            None,
            "fenced-orders",
            MarkerAppend {
                producer_id,
                producer_epoch: 6,
                marker_type: MarkerType::Abort,
                coordinator_epoch: 9,
                commit_stamp: None,
            },
        )
        .await;
        assert!(matches!(
            stale_coordinator,
            Err(BrokerError::CoordinatorEpochFenced { .. })
        ));
        assert!(part.log_end_offset() == before_stale_coordinator);
        assert!(
            part.log
                .lock()
                .expect("partition log")
                .pending_transaction_start(producer_id)
                .is_some()
        );
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn failed_marker_append_never_publishes_transactional_offsets() {
        use krabka_log::Offset;
        use krabka_protocol::records::{Attributes, Record, RecordBatch};

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "failed-marker-group";
        let offsets_partition = crate::coordinator::partitioner::partition_for_group(
            &broker.controller.current_image(),
            group_id,
        );
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
            .expect("local offsets partition");
        let producer_id = krabka_log::ProducerId(702);
        part.produce_batch(RecordBatch {
            producer_id: producer_id.get(),
            producer_epoch: 1,
            base_sequence: 0,
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                key: Some(OffsetCommitValue::encode_key(group_id, "orders", 0)),
                value: Some(
                    OffsetCommitValue {
                        offset: Offset(55),
                        leader_epoch: 1,
                        metadata: "must-stay-hidden".into(),
                        commit_timestamp_ms: 1,
                        expire_timestamp_ms: None,
                    }
                    .encode_value(),
                ),
                ..Record::default()
            }],
            ..RecordBatch::default()
        })
        .await
        .expect("append transactional offset");

        let writer = part.take_writer_handle().expect("partition writer");
        writer.abort();
        let _ = writer.await;
        let result = append_marker_and_materialize(
            &part,
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            MarkerAppend {
                producer_id,
                producer_epoch: 1,
                marker_type: MarkerType::Commit,
                coordinator_epoch: 0,
                commit_stamp: None,
            },
        )
        .await;

        assert!(result.is_err());
        assert!(broker.group_coordinator.find(group_id).is_none());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn exact_marker_retry_drains_retained_offset_publication() {
        use krabka_log::Offset;

        let (broker_handle, _dir) = start_broker().await;
        let broker = broker_handle.broker_arc_for_test();
        let group_id = "retained-marker-group";
        let offsets_partition = crate::coordinator::partitioner::partition_for_group(
            &broker.controller.current_image(),
            group_id,
        );
        let part = broker
            .partitions
            .get(OFFSETS_TOPIC, PartitionIndex(offsets_partition))
            .expect("local offsets partition");
        let producer_id = krabka_log::ProducerId(703);
        let marker = MarkerAppend {
            producer_id,
            producer_epoch: 1,
            marker_type: MarkerType::Commit,
            coordinator_epoch: 4,
            commit_stamp: None,
        };
        append_marker_and_materialize(
            &part,
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            marker,
        )
        .await
        .expect("append marker generation");
        let after_marker = part.log_end_offset();
        let marker_count = || {
            part.log
                .lock()
                .expect("offsets log")
                .read(Offset(0), krabka_units::mebibytes(1))
                .expect("read offsets log")
                .batches
                .into_iter()
                .filter(|batch| {
                    batch.producer_id == producer_id.get() && batch.attributes.is_control_batch()
                })
                .count()
        };
        let markers_before_retry = marker_count();

        let mut retained = CommittedOffsets::new();
        retained.insert(
            group_id.into(),
            vec![(
                ("orders".into(), 3),
                OffsetEntry {
                    offset: Offset(88),
                    leader_epoch: 2,
                    metadata: "retained".into(),
                    commit_timestamp_ms: i64::MAX,
                    expire_timestamp_ms: None,
                },
            )],
        );
        part.marker_materialization
            .lock()
            .await
            .insert(producer_id, (after_marker.get() - 1, retained));

        append_marker_and_materialize(
            &part,
            Some(&broker.group_coordinator),
            OFFSETS_TOPIC,
            marker,
        )
        .await
        .expect("retry retained publication");
        assert!(marker_count() == markers_before_retry);
        assert!(
            !part
                .marker_materialization
                .lock()
                .await
                .contains_key(&producer_id)
        );

        let handle = broker
            .group_coordinator
            .find(group_id)
            .expect("offset home actor");
        let (reply, result) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchOffsets { reply })
            .await
            .expect("fetch committed request");
        let committed = result.await.expect("fetch committed response");
        assert!(
            committed
                .committed
                .get(&("orders".into(), 3))
                .is_some_and(|entry| { entry.offset == 88 && entry.metadata == "retained" })
        );
        broker_handle.shutdown().await;
    }
}
