//! Appending one transaction marker to a partition, and the offset resolution
//! that a `__consumer_offsets` marker triggers.
//!
//! Both the inter-broker `WriteTxnMarkers` handler and `EndTxn`'s direct local
//! path go through this module rather than calling the partition themselves,
//! so that the durable marker and the in-memory publication can never drift
//! apart.

use std::{collections::HashMap, sync::Arc};

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
    let committed_offsets = if topic == OFFSETS_TOPIC {
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
    if let Some(stamp) = commit_stamp {
        if marker_type != MarkerType::Commit {
            return Err(BrokerError::Txn(
                "a transaction commit stamp cannot be attached to an abort marker".into(),
            ));
        }
        partition.produce_commit_marker(marker, stamp).await?;
    } else {
        // A control batch takes the control append path, which applies no
        // compression rewrite. Kafka never compresses a control batch that
        // arrived uncompressed.
        partition.produce_control_batch(marker).await?;
    }

    if let (Some(coordinator), offsets) = committed_offsets {
        resolve_pending_offsets(coordinator, producer_id, marker_type, offsets).await?;
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
        coordinator::persistence::OffsetCommitValue,
        txn::handlers::write_txn_markers::test_support::{open_partition, start_broker},
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
            attributes: Attributes::default().with_transactional(true),
            records: vec![Record {
                key: Some(OffsetCommitValue::encode_key(group_id, "orders", 3)),
                value: Some(
                    OffsetCommitValue {
                        offset: Offset(99),
                        leader_epoch: 4,
                        metadata: "aborted".into(),
                        commit_timestamp_ms: 456,
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
}
