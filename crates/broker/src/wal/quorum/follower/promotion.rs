//! Promotion of a WAL follower to the canonical partition log. When this broker
//! takes over a diskless shard it copies its own checkpointed follower prefix
//! into the partition log, after it has checked that the two agree byte for byte
//! on the range they share.

use std::path::PathBuf;

use krabka_ids::Offset;
use krabka_log::{Log, LogConfig};
use krabka_raft::NodeId;

use super::log::{FollowerLog, voter_dir};
use crate::wal::quorum::{
    engine::{read_batches_exact, read_log_batches_exact},
    registry::ShardId,
};

/// Copy this broker's checkpointed follower prefix into a newly promoted
/// partition log. The follower directory remains intact so a crash during or
/// after hydration can retry from the same durable source.
pub(crate) fn hydrate_on_promotion(
    log_dirs: &[PathBuf],
    topic: &str,
    shard: ShardId,
    node_id: NodeId,
    storage: &LogConfig,
    destination: &mut Log,
) -> Result<Option<Offset>, crate::BrokerError> {
    let Some(dir) = log_dirs
        .iter()
        .map(|root| voter_dir(root, topic, shard, node_id))
        .find(|candidate| candidate.exists())
    else {
        return Ok(None);
    };
    let follower = FollowerLog::open_at(dir, storage)?;
    let source_start = follower.start_offset();
    let source_end = follower.end_offset();
    let destination_start = destination.log_start_offset();
    let destination_end = destination.log_end_offset();

    if destination_start == destination_end && destination_end < source_end {
        destination.reset_to(source_start)?;
    } else {
        let overlap_start = source_start.max(destination_start);
        let overlap_end = source_end.min(destination_end);
        if overlap_start < overlap_end {
            let source = read_batches_exact(&follower.log, overlap_start, overlap_end)?;
            let current = read_log_batches_exact(destination, overlap_start, overlap_end)?;
            if source.len() != current.len()
                || source.iter().zip(&current).any(|(source, current)| {
                    source.base_offset != current.base_offset
                        || source.last_offset != current.last_offset
                        || source.verbatim.bytes != current.verbatim.bytes
                })
            {
                return Err(crate::BrokerError::Replication(format!(
                    "promoted WAL follower diverges from canonical log in {}..{}",
                    overlap_start.0, overlap_end.0
                )));
            }
        } else if destination_end < source_start {
            return Err(crate::BrokerError::Replication(format!(
                "promoted WAL follower starts at {}, after canonical LEO {}",
                source_start.0, destination_end.0
            )));
        }
    }

    if destination.log_end_offset() < source_end {
        let batches = read_batches_exact(&follower.log, destination.log_end_offset(), source_end)?;
        for batch in batches {
            destination.append_verbatim_at(&batch.verbatim, batch.base_offset)?;
        }
    }
    if destination.log_end_offset() < source_end {
        return Err(crate::BrokerError::Replication(format!(
            "promoted WAL hydration ended at {}, before durable follower LEO {}",
            destination.log_end_offset().0,
            source_end.0
        )));
    }
    destination.sync()?;
    Ok(Some(source_end))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use krabka_ids::PartitionIndex;
    use krabka_protocol::records::{Record, RecordBatch};
    use krabka_units::mebibytes;

    use super::*;
    use crate::wal::quorum::follower::checkpoint::{
        DURABLE_OFFSET_FILE, DurableRange, write_durable_offset,
    };

    #[test]
    fn promotion_hydrates_exact_checkpointed_bytes_without_regression() {
        let root = tempfile::tempdir().unwrap();
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(101),
            partition: PartitionIndex(0),
        };
        let follower_dir = voter_dir(root.path(), "diskless", shard, NodeId(2));
        let mut follower = Log::open(&follower_dir, LogConfig::default()).unwrap();
        let mut durable = RecordBatch {
            records: vec![
                Record {
                    value: Some(Bytes::from_static(b"a")),
                    ..Record::default()
                },
                Record {
                    offset_delta: 1,
                    value: Some(Bytes::from_static(b"b")),
                    ..Record::default()
                },
            ],
            last_offset_delta: 1,
            ..RecordBatch::default()
        };
        follower.append(&mut durable).unwrap();
        follower.sync().unwrap();
        write_durable_offset(
            &follower_dir.join(DURABLE_OFFSET_FILE),
            DurableRange {
                start: Offset(0),
                end: Offset(2),
            },
        )
        .unwrap();
        let mut uncertain = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"uncertain")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        follower.append(&mut uncertain).unwrap();
        follower.sync().unwrap();
        drop(follower);

        let destination_dir = crate::log_dir::partition_dir(root.path(), "diskless", 0);
        let mut destination = Log::open(&destination_dir, LogConfig::default()).unwrap();
        assert!(
            hydrate_on_promotion(
                &[root.path().to_path_buf()],
                "diskless",
                shard,
                NodeId(2),
                &LogConfig::default(),
                &mut destination,
            )
            .unwrap()
                == Some(Offset(2))
        );
        assert!(destination.log_end_offset() == Offset(2));
        let source = Log::open(&follower_dir, LogConfig::default()).unwrap();
        assert!(source.log_end_offset() == Offset(2));
        assert!(
            source
                .read_raw(Offset(0), Offset(2), mebibytes(1))
                .unwrap()
                .bytes
                == destination
                    .read_raw(Offset(0), Offset(2), mebibytes(1))
                    .unwrap()
                    .bytes
        );

        let mut newer = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"newer")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        destination.append(&mut newer).unwrap();
        destination.sync().unwrap();
        assert!(
            hydrate_on_promotion(
                &[root.path().to_path_buf()],
                "diskless",
                shard,
                NodeId(2),
                &LogConfig::default(),
                &mut destination,
            )
            .unwrap()
                == Some(Offset(2))
        );
        assert!(destination.log_end_offset() == Offset(3));
        assert!(follower_dir.exists());
    }

    #[test]
    fn promotion_retries_after_reopening_a_partial_destination() {
        let root = tempfile::tempdir().unwrap();
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(102),
            partition: PartitionIndex(0),
        };
        let follower_dir = voter_dir(root.path(), "diskless", shard, NodeId(2));
        let mut follower = Log::open(&follower_dir, LogConfig::default()).unwrap();
        let mut first = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"first")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        let mut second = RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"second")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        follower.append(&mut first).unwrap();
        follower.append(&mut second).unwrap();
        follower.sync().unwrap();
        write_durable_offset(
            &follower_dir.join(DURABLE_OFFSET_FILE),
            DurableRange {
                start: Offset(0),
                end: Offset(2),
            },
        )
        .unwrap();

        let destination_dir = crate::log_dir::partition_dir(root.path(), "diskless", 0);
        {
            let mut partial = Log::open(&destination_dir, LogConfig::default()).unwrap();
            let prefix = read_log_batches_exact(&follower, Offset(0), Offset(1)).unwrap();
            partial
                .append_verbatim_at(&prefix[0].verbatim, prefix[0].base_offset)
                .unwrap();
            partial.sync().unwrap();
        }

        // Model a process restart after only the first durable batch was
        // adopted. Reopening the canonical directory and retrying hydration
        // must retain the exact prefix and append the missing durable tail.
        let mut reopened = Log::open(&destination_dir, LogConfig::default()).unwrap();
        assert!(
            hydrate_on_promotion(
                &[root.path().to_path_buf()],
                "diskless",
                shard,
                NodeId(2),
                &LogConfig::default(),
                &mut reopened,
            )
            .unwrap()
                == Some(Offset(2))
        );
        assert!(reopened.log_end_offset() == Offset(2));
        assert!(
            follower
                .read_raw(Offset(0), Offset(2), mebibytes(1))
                .unwrap()
                .bytes
                == reopened
                    .read_raw(Offset(0), Offset(2), mebibytes(1))
                    .unwrap()
                    .bytes
        );
    }
}
