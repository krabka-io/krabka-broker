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
    use std::{collections::BTreeSet, net::SocketAddr, sync::Arc};

    use krabka_metadata::{MetadataImage, MetadataRecord};
    use krabka_raft::{
        AddVoter, Node, NodeId, OffsetReservation, QuorumState, RaftError, ReconfigOutcome,
        RemoveVoter, SnapshotRange, SubmitChangeResult, UpdateVoter,
    };
    use tokio::sync::watch;

    use super::*;

    struct FakeMetadataSource {
        result: SubmitChangeResult,
        term: u64,
        known_term: bool,
        fail: bool,
    }

    #[async_trait]
    impl MetadataSource for FakeMetadataSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            Arc::new(MetadataImage::default())
        }

        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            let (_tx, rx) = watch::channel(self.current_image());
            rx
        }

        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            let (_tx, rx) = watch::channel(None);
            rx
        }

        fn quorum_state(&self) -> QuorumState {
            QuorumState {
                current_term: self.term,
                last_applied_index: 0,
                current_leader: None,
                voters: Vec::new(),
                voter_nodes: std::collections::BTreeMap::new(),
                per_voter_matched_index: std::collections::BTreeMap::new(),
            }
        }

        fn current_controller_epoch(&self) -> Option<u64> {
            self.known_term.then_some(self.term)
        }

        async fn submit_change(
            &self,
            records: Vec<MetadataRecord>,
        ) -> Result<SubmitChangeResult, RaftError> {
            assert2::assert!(matches!(
                records.as_slice(),
                [MetadataRecord::V1PartitionOffsetAdvance(record)]
                    if record.topic == "topic" && record.partition == 0 && record.count == 3
            ));
            if self.fail {
                Err(RaftError::Shutdown)
            } else {
                Ok(self.result.clone())
            }
        }

        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        fn controller_bound_addr(&self) -> SocketAddr {
            "127.0.0.1:0".parse().unwrap()
        }

        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            SnapshotRange::NoSnapshot
        }

        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!("unused in offset sequencer tests")
        }

        async fn cancel(&self) {}
    }

    #[tokio::test]
    async fn controller_sequencer_uses_returned_reservation_base() {
        let sequencer = ControllerSequencer::new(Arc::new(FakeMetadataSource {
            result: SubmitChangeResult {
                offset_reservations: vec![OffsetReservation {
                    topic: "topic".to_string(),
                    partition: 0,
                    base_offset: 11,
                    count: 3,
                    leader_epoch: 7,
                }],
            },
            term: 7,
            known_term: true,
            fail: false,
        }));

        let base = sequencer
            .assign("topic", PartitionIndex(0), 3)
            .await
            .unwrap();

        assert2::assert!((base) == (Offset(11)));
    }

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
            let sequencer = ControllerSequencer::new(Arc::new(FakeMetadataSource {
                result,
                term: 7,
                known_term: true,
                fail: false,
            }));
            assert2::assert!(
                sequencer
                    .assign("topic", PartitionIndex(0), 3)
                    .await
                    .is_err()
            );
        }

        let failing = ControllerSequencer::new(Arc::new(FakeMetadataSource {
            result: SubmitChangeResult::default(),
            term: 7,
            known_term: true,
            fail: true,
        }));
        assert2::assert!(failing.assign("topic", PartitionIndex(0), 3).await.is_err());
    }

    #[tokio::test]
    async fn broker_only_sequencer_accepts_a_committed_response_epoch() {
        let sequencer = ControllerSequencer::new(Arc::new(FakeMetadataSource {
            result: SubmitChangeResult {
                offset_reservations: vec![reservation("topic", 0, 11, 3, 7)],
            },
            term: 0,
            known_term: false,
            fail: false,
        }));

        assert2::assert!(
            sequencer
                .assign("topic", PartitionIndex(0), 3)
                .await
                .unwrap()
                == Offset(11)
        );
    }
}
