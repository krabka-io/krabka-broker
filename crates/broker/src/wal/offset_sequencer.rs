//! Controller-backed offset assignment for diskless WAL partitions.

use std::sync::Arc;

use async_trait::async_trait;
use krabka_ids::{Offset, PartitionIndex};
use krabka_metadata::{MetadataRecord, PartitionOffsetAdvanceRecord};

use crate::{error::BrokerError, metadata_source::MetadataSource};

#[async_trait]
pub(crate) trait OffsetSequencer: Send + Sync {
    async fn assign(
        &self,
        topic: &str,
        partition: PartitionIndex,
        count: u32,
    ) -> Result<Offset, BrokerError>;
}

pub(crate) struct ControllerSequencer {
    metadata: Arc<dyn MetadataSource>,
}

impl ControllerSequencer {
    #[must_use]
    pub(crate) fn new(metadata: Arc<dyn MetadataSource>) -> Self {
        Self { metadata }
    }
}

#[async_trait]
impl OffsetSequencer for ControllerSequencer {
    async fn assign(
        &self,
        topic: &str,
        partition: PartitionIndex,
        count: u32,
    ) -> Result<Offset, BrokerError> {
        let request_epoch = controller_epoch_coordinate(self.metadata.current_controller_epoch());
        let result = self
            .metadata
            .submit_change(vec![MetadataRecord::V1PartitionOffsetAdvance(
                PartitionOffsetAdvanceRecord {
                    topic: topic.to_owned(),
                    partition: partition.0,
                    count: i64::from(count),
                },
            )])
            .await
            .map_err(|error| BrokerError::Replication(format!("offset sequencer: {error}")))?;

        let [reservation] = result.offset_reservations.as_slice() else {
            return Err(BrokerError::Replication(format!(
                "offset sequencer: expected one reservation, got {}",
                result.offset_reservations.len()
            )));
        };
        let request_matches = reservation.topic == topic
            && reservation.partition == partition.0
            && reservation.count == i64::from(count);
        let observed_epoch = controller_epoch_coordinate(self.metadata.current_controller_epoch());
        let response_epoch = i64::try_from(reservation.leader_epoch).unwrap_or(-2);
        let Some(base_offset) = krabka_verified::wal_reservation_response(
            request_matches,
            request_epoch,
            observed_epoch,
            response_epoch,
            reservation.base_offset,
            reservation.count,
        ) else {
            let detail = if request_matches {
                "reservation epoch or range is stale or invalid"
            } else {
                "reservation does not match request"
            };
            return Err(BrokerError::Replication(format!(
                "offset sequencer: {detail}"
            )));
        };
        Ok(Offset(base_offset))
    }
}

fn controller_epoch_coordinate(epoch: Option<u64>) -> i64 {
    match epoch {
        None => -1,
        Some(epoch) => i64::try_from(epoch).unwrap_or(-2),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use krabka_raft::{OffsetReservation, RaftError, SubmitChangeResult};

    use super::*;
    use crate::test_support::FakeMetadataSource;

    fn reservation(
        topic: &str,
        partition: i32,
        base_offset: i64,
        count: i64,
        leader_epoch: u64,
    ) -> OffsetReservation {
        OffsetReservation {
            topic: topic.to_string(),
            partition,
            base_offset,
            count,
            leader_epoch,
        }
    }

    /// The batches the sequencer must have submitted after one `assign` of
    /// three records on `topic-0`. Every case below asks for that same
    /// advance, so every case must submit exactly this -- whatever the
    /// controller then answers.
    fn expected_submissions() -> Vec<Vec<MetadataRecord>> {
        vec![vec![MetadataRecord::V1PartitionOffsetAdvance(
            PartitionOffsetAdvanceRecord {
                topic: "topic".to_string(),
                partition: 0,
                count: 3,
            },
        )]]
    }

    /// A controller in term 7 that answers every `submit_change` with
    /// `result`.
    fn source_answering(result: SubmitChangeResult) -> Arc<FakeMetadataSource> {
        Arc::new(
            FakeMetadataSource::builder()
                .term(7)
                .on_submit(move |_| Ok(result.clone()))
                .build(),
        )
    }

    #[tokio::test]
    async fn controller_sequencer_uses_returned_reservation_base() {
        let source = source_answering(SubmitChangeResult {
            offset_reservations: vec![reservation("topic", 0, 11, 3, 7)],
        });
        let sequencer = ControllerSequencer::new(Arc::clone(&source) as Arc<dyn MetadataSource>);

        let base = sequencer
            .assign("topic", PartitionIndex(0), 3)
            .await
            .unwrap();

        assert2::assert!((base) == (Offset(11)));
        // One batch, carrying exactly the advance the caller asked for.
        assert2::assert!(source.submitted() == expected_submissions());
    }

    #[tokio::test]
    async fn controller_sequencer_rejects_stale_malformed_and_failed_responses() {
        let cases = [
            SubmitChangeResult::default(),
            SubmitChangeResult {
                offset_reservations: vec![
                    reservation("topic", 0, 11, 3, 7),
                    reservation("topic", 0, 14, 3, 7),
                ],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("wrong", 0, 11, 3, 7)],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 1, 11, 3, 7)],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 0, 11, 4, 7)],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 0, 11, 3, 6)],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 0, -1, 3, 7)],
            },
            SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 0, i64::MAX, 3, 7)],
            },
        ];

        for result in cases {
            let source = source_answering(result);
            let sequencer =
                ControllerSequencer::new(Arc::clone(&source) as Arc<dyn MetadataSource>);
            assert2::assert!(
                sequencer
                    .assign("topic", PartitionIndex(0), 3)
                    .await
                    .is_err()
            );
            // The write went out as asked; only the answer was unusable.
            assert2::assert!(source.submitted() == expected_submissions());
        }

        let failing = Arc::new(
            FakeMetadataSource::builder()
                .term(7)
                .on_submit(|_| Err(RaftError::Shutdown))
                .build(),
        );
        let sequencer = ControllerSequencer::new(Arc::clone(&failing) as Arc<dyn MetadataSource>);
        assert2::assert!(
            sequencer
                .assign("topic", PartitionIndex(0), 3)
                .await
                .is_err()
        );
        assert2::assert!(failing.submitted() == expected_submissions());
    }

    #[tokio::test]
    async fn broker_only_sequencer_accepts_a_committed_response_epoch() {
        // A broker-only observer owns no term state, so it fences the write
        // against no epoch at all and must still take the controller's answer.
        let source = Arc::new(
            FakeMetadataSource::builder()
                .without_controller_epoch()
                .on_submit(|_| {
                    Ok(SubmitChangeResult {
                        offset_reservations: vec![reservation("topic", 0, 11, 3, 7)],
                    })
                })
                .build(),
        );
        let sequencer = ControllerSequencer::new(Arc::clone(&source) as Arc<dyn MetadataSource>);

        assert2::assert!(
            sequencer
                .assign("topic", PartitionIndex(0), 3)
                .await
                .unwrap()
                == Offset(11)
        );
        assert2::assert!(source.submitted() == expected_submissions());
    }
}
