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
        if reservation.topic != topic
            || reservation.partition != partition.0
            || reservation.count != i64::from(count)
        {
            return Err(BrokerError::Replication(
                "offset sequencer: reservation does not match request".to_string(),
            ));
        }
        Ok(Offset(reservation.base_offset))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use krabka_raft::{OffsetReservation, SubmitChangeResult};

    use super::*;
    use crate::test_support::FakeMetadataSource;

    #[tokio::test]
    async fn controller_sequencer_uses_returned_reservation_base() {
        let source = Arc::new(
            FakeMetadataSource::builder()
                .on_submit(|_| {
                    Ok(SubmitChangeResult {
                        offset_reservations: vec![OffsetReservation {
                            topic: "topic".to_string(),
                            partition: 0,
                            base_offset: 11,
                            count: 3,
                        }],
                    })
                })
                .build(),
        );
        let sequencer = ControllerSequencer::new(Arc::clone(&source) as Arc<dyn MetadataSource>);

        let base = sequencer
            .assign("topic", PartitionIndex(0), 3)
            .await
            .unwrap();

        assert2::assert!((base) == (Offset(11)));
        // One batch, carrying exactly the advance the caller asked for.
        assert2::assert!(
            source.submitted()
                == vec![vec![MetadataRecord::V1PartitionOffsetAdvance(
                    PartitionOffsetAdvanceRecord {
                        topic: "topic".to_string(),
                        partition: 0,
                        count: 3,
                    }
                )]]
        );
    }
}
