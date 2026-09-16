//! Kafka's request checks for an inbound `Vote`, `BeginQuorumEpoch`,
//! `EndQuorumEpoch` and `FetchSnapshot`, and the leader view every response
//! carries.
//!
//! The checks and their order are those of `KafkaRaftClient.handleVoteRequest`,
//! `handleBeginQuorumEpochRequest`, `handleEndQuorumEpochRequest` and
//! `handleFetchSnapshotRequest`:
//!
//! 1. A `ClusterId` that names another cluster is a top-level
//!    `INCONSISTENT_CLUSTER_ID`, with no partition.
//! 2. A request that does not name exactly the `__cluster_metadata` partition 0
//!    is a top-level `INVALID_REQUEST`.
//! 3. The rest are partition errors beside the responder's leader id, epoch
//!    and leader endpoint.
//!
//! A body that does not decode gets no answer. Kafka's
//! `RequestContext.parseRequest` fails such a request, and the connection
//! closes.

use bytes::Bytes;

use super::{Engine, checkpoint::load_checkpoint_by_id, checkpoint_dir};
use crate::kraft::{
    event::{Event, LogEnd, SuccessorRank},
    transport::wire,
    types::{Epoch, NodeId},
};

const UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const NOT_LEADER_OR_FOLLOWER: i16 = 6;
const INVALID_REQUEST: i16 = 42;
const FENCED_LEADER_EPOCH: i16 = 74;
const UNKNOWN_LEADER_EPOCH: i16 = 75;
const SNAPSHOT_NOT_FOUND: i16 = 98;
const POSITION_OUT_OF_RANGE: i16 = 99;
const INCONSISTENT_CLUSTER_ID: i16 = 104;
const INVALID_VOTER_KEY: i16 = 125;

/// Kafka's `BOOTSTRAP_SNAPSHOT_ID`. The bootstrap checkpoint is not
/// replicated.
const BOOTSTRAP_SNAPSHOT_ID: (i64, i32) = (0, 0);

/// The listener name a controller advertises its peer RPCs on. A voter
/// endpoint with another name is used only when the voter has no such
/// endpoint, the convention of `controller_endpoint_addr`.
const CONTROLLER_LISTENER_NAME: &str = "CONTROLLER";

/// Kafka's `hasValidTopicPartition` for the quorum RPCs: one topic, named
/// `__cluster_metadata`, with one partition, index 0.
fn names_only_the_metadata_partition<'a>(
    mut topics: impl ExactSizeIterator<Item = (&'a str, Vec<i32>)>,
) -> bool {
    topics.len() == 1
        && topics.next().is_some_and(|(name, partitions)| {
            name == wire::METADATA_TOPIC && partitions == [wire::METADATA_PARTITION]
        })
}

impl Engine {
    /// The responder's leader id, epoch and leader endpoint.
    pub(super) fn quorum_leader(&self) -> wire::QuorumLeader {
        let state = self.core.quorum_state();
        let endpoint = state
            .leader_id
            .and_then(|leader| self.core.current_or_adjacent_voter_entry(leader))
            .and_then(|voter| {
                voter
                    .endpoints
                    .iter()
                    .find(|endpoint| endpoint.name.eq_ignore_ascii_case(CONTROLLER_LISTENER_NAME))
                    .or_else(|| voter.endpoints.first())
            })
            .map(|endpoint| (endpoint.host.clone(), endpoint.port));
        wire::QuorumLeader {
            leader_id: state.leader_id,
            epoch: state.leader_epoch,
            endpoint,
        }
    }

    /// Kafka's `hasValidClusterId`: an absent id is valid.
    fn has_valid_cluster_id(&self, cluster_id: Option<&str>) -> bool {
        cluster_id.is_none_or(|id| {
            wire::parse_cluster_id(id) == Some(self.core.quorum_state().cluster_id)
        })
    }

    /// Kafka's `validateVoterOnlyRequest`.
    fn voter_only_request_error(&self, remote_id: i32, request_epoch: i32) -> Option<i16> {
        if i64::from(request_epoch) < i64::from(self.core.quorum_state().leader_epoch) {
            Some(FENCED_LEADER_EPOCH)
        } else if remote_id < 0 {
            Some(INVALID_REQUEST)
        } else {
            None
        }
    }

    /// Kafka's `validateLeaderOnlyRequest`, without the shutdown check: the
    /// engine answers no request once it stops.
    fn leader_only_request_error(&self, request_epoch: i32) -> Option<i16> {
        let local_epoch = i64::from(self.core.quorum_state().leader_epoch);
        if i64::from(request_epoch) < local_epoch {
            Some(FENCED_LEADER_EPOCH)
        } else if i64::from(request_epoch) > local_epoch {
            Some(UNKNOWN_LEADER_EPOCH)
        } else if !self.core.role().is_leader() {
            Some(NOT_LEADER_OR_FOLLOWER)
        } else {
            None
        }
    }

    /// The directory id of this replica, when the voter set records one.
    fn local_directory_id(&self) -> Option<uuid::Uuid> {
        self.core
            .quorum_state()
            .voters
            .get(self.me)
            .map(|voter| voter.directory_id)
            .filter(|directory_id| !directory_id.is_nil())
    }

    /// Kafka's `isValidVoterKey`: a negative voter id names no voter key, and
    /// an empty directory id matches any directory.
    fn is_valid_voter_key(&self, voter_id: i32, voter_directory_id: uuid::Uuid) -> bool {
        if voter_id < 0 {
            return true;
        }
        u64::try_from(voter_id) == Ok(self.me.0)
            && (voter_directory_id.is_nil()
                || self
                    .local_directory_id()
                    .is_none_or(|local| local == voter_directory_id))
    }

    /// Where this replica stands in `PreferredCandidates`, as Kafka's
    /// `endEpochElectionBackoff` walks the list.
    fn successor_rank(
        &self,
        candidates: &[krabka_protocol::owned::end_quorum_epoch_request::ReplicaInfo],
    ) -> SuccessorRank {
        let local_directory_id = self.local_directory_id();
        let position = candidates
            .iter()
            .position(|candidate| {
                u64::try_from(candidate.candidate_id) == Ok(self.me.0) && {
                    let directory_id = uuid::Uuid::from_bytes(candidate.candidate_directory_id.0);
                    directory_id.is_nil() || local_directory_id.is_none_or(|l| l == directory_id)
                }
            })
            .unwrap_or(candidates.len());
        SuccessorRank {
            position: u32::try_from(position).unwrap_or(u32::MAX),
            successors: u32::try_from(candidates.len()).unwrap_or(u32::MAX),
        }
    }

    /// Answers a `Vote` request. `None` means the body did not decode.
    pub(super) fn answer_vote(&mut self, body: &[u8]) -> Option<Bytes> {
        let request = wire::decode_vote_request(body)?;
        let refuse = |engine: &Self, top: i16, partition: i16| {
            Some(wire::encode_vote_response(
                top,
                partition,
                false,
                &engine.quorum_leader(),
            ))
        };
        if !self.has_valid_cluster_id(request.cluster_id.as_deref()) {
            return refuse(self, INCONSISTENT_CLUSTER_ID, 0);
        }
        if !names_only_the_metadata_partition(request.topics.iter().map(|topic| {
            (
                topic.topic_name.as_str(),
                topic.partitions.iter().map(|p| p.partition_index).collect(),
            )
        })) {
            return refuse(self, INVALID_REQUEST, 0);
        }
        let partition = &request.topics[0].partitions[0];
        // A standard vote's candidate has bumped its epoch past its log, so its
        // last epoch must be below the replica epoch. A pre-vote does not bump.
        let illegal_epoch = if partition.pre_vote {
            partition.last_offset_epoch > partition.replica_epoch
        } else {
            partition.last_offset_epoch >= partition.replica_epoch
        };
        if partition.last_offset < 0 || partition.last_offset_epoch < 0 || illegal_epoch {
            return refuse(self, 0, INVALID_REQUEST);
        }
        if let Some(error) =
            self.voter_only_request_error(partition.replica_id, partition.replica_epoch)
        {
            return refuse(self, 0, error);
        }
        let voter_directory_id = uuid::Uuid::from_bytes(partition.voter_directory_id.0);
        if !self.is_valid_voter_key(request.voter_id, voter_directory_id) {
            return refuse(self, 0, INVALID_VOTER_KEY);
        }
        // Every field below passed the checks above, so the conversions hold.
        let (Ok(candidate_epoch), Ok(candidate), Ok(last_epoch)) = (
            Epoch::try_from(partition.replica_epoch),
            u64::try_from(partition.replica_id),
            Epoch::try_from(partition.last_offset_epoch),
        ) else {
            return refuse(self, 0, INVALID_REQUEST);
        };
        // The request is addressed to this replica, or to no voter key at all,
        // which Kafka also takes as this replica. The core sees its own key.
        let event = Event::ReceiveVoteRequest {
            from: NodeId(candidate),
            cluster_id: Some(self.core.quorum_state().cluster_id),
            voter_id: self.me,
            voter_directory_id: self
                .core
                .quorum_state()
                .voters
                .get(self.me)
                .map_or(uuid::Uuid::nil(), |voter| voter.directory_id),
            candidate_epoch,
            candidate: NodeId(candidate),
            candidate_directory_id: uuid::Uuid::from_bytes(partition.replica_directory_id.0),
            candidate_log_end: LogEnd {
                last_epoch,
                last_offset: partition.last_offset,
            },
            pre_vote: partition.pre_vote,
        };
        let granted = self.run_vote_request(event);
        Some(wire::encode_vote_response(
            0,
            0,
            granted,
            &self.quorum_leader(),
        ))
    }

    /// Answers a `BeginQuorumEpoch` request. `None` means the body did not
    /// decode.
    pub(super) fn answer_begin_quorum_epoch(&mut self, body: &[u8]) -> Option<Bytes> {
        let request = wire::decode_begin_quorum_epoch_request(body)?;
        let respond = |engine: &Self, top: i16, partition: i16| {
            Some(wire::encode_begin_quorum_epoch_response(
                top,
                partition,
                &engine.quorum_leader(),
            ))
        };
        if !self.has_valid_cluster_id(request.cluster_id.as_deref()) {
            return respond(self, INCONSISTENT_CLUSTER_ID, 0);
        }
        if !names_only_the_metadata_partition(request.topics.iter().map(|topic| {
            (
                topic.topic_name.as_str(),
                topic.partitions.iter().map(|p| p.partition_index).collect(),
            )
        })) {
            return respond(self, INVALID_REQUEST, 0);
        }
        let partition = &request.topics[0].partitions[0];
        if let Some(error) =
            self.voter_only_request_error(partition.leader_id, partition.leader_epoch)
        {
            return respond(self, 0, error);
        }
        let (Ok(leader_id), Ok(leader_epoch)) = (
            u64::try_from(partition.leader_id),
            Epoch::try_from(partition.leader_epoch),
        ) else {
            return respond(self, 0, INVALID_REQUEST);
        };
        self.on_event(Event::ReceiveBeginQuorumEpoch {
            leader_id: NodeId(leader_id),
            leader_epoch,
        });
        // Kafka transitions first, then checks that the request was meant for
        // this replica.
        let voter_directory_id = uuid::Uuid::from_bytes(partition.voter_directory_id.0);
        if !self.is_valid_voter_key(request.voter_id, voter_directory_id) {
            return respond(self, 0, INVALID_VOTER_KEY);
        }
        respond(self, 0, 0)
    }

    /// Answers an `EndQuorumEpoch` request. `None` means the body did not
    /// decode.
    pub(super) fn answer_end_quorum_epoch(&mut self, body: &[u8]) -> Option<Bytes> {
        let request = wire::decode_end_quorum_epoch_request(body)?;
        let respond = |engine: &Self, top: i16, partition: i16| {
            Some(wire::encode_end_quorum_epoch_response(
                top,
                partition,
                &engine.quorum_leader(),
            ))
        };
        if !self.has_valid_cluster_id(request.cluster_id.as_deref()) {
            return respond(self, INCONSISTENT_CLUSTER_ID, 0);
        }
        if !names_only_the_metadata_partition(request.topics.iter().map(|topic| {
            (
                topic.topic_name.as_str(),
                topic.partitions.iter().map(|p| p.partition_index).collect(),
            )
        })) {
            return respond(self, INVALID_REQUEST, 0);
        }
        let partition = &request.topics[0].partitions[0];
        if let Some(error) =
            self.voter_only_request_error(partition.leader_id, partition.leader_epoch)
        {
            return respond(self, 0, error);
        }
        let (Ok(leader_id), Ok(leader_epoch)) = (
            u64::try_from(partition.leader_id),
            Epoch::try_from(partition.leader_epoch),
        ) else {
            return respond(self, 0, INVALID_REQUEST);
        };
        let successor_rank = self.successor_rank(&partition.preferred_candidates);
        self.on_event(Event::ReceiveEndQuorumEpoch {
            leader_id: NodeId(leader_id),
            leader_epoch,
            successor_rank,
        });
        respond(self, 0, 0)
    }

    /// Answers a `FetchSnapshot` request. `None` means the body did not
    /// decode.
    pub(super) fn answer_fetch_snapshot(&mut self, body: &[u8]) -> Option<Bytes> {
        let request = wire::decode_fetch_snapshot_request(body)?;
        let top_level = |engine: &Self, error_code: i16| {
            Some(wire::encode_fetch_snapshot_answer(
                error_code,
                None,
                &engine.quorum_leader(),
            ))
        };
        let partition_error = |engine: &Self, error_code: i16| {
            Some(wire::encode_fetch_snapshot_answer(
                0,
                Some(wire::FetchSnapshotPartition {
                    topic: wire::METADATA_TOPIC.into(),
                    index: wire::METADATA_PARTITION,
                    error_code,
                    current_leader: true,
                    chunk: None,
                }),
                &engine.quorum_leader(),
            ))
        };
        if !self.has_valid_cluster_id(request.cluster_id.as_deref()) {
            return top_level(self, INCONSISTENT_CLUSTER_ID);
        }
        let [topic] = request.topics.as_slice() else {
            return top_level(self, INVALID_REQUEST);
        };
        let [partition] = topic.partitions.as_slice() else {
            return top_level(self, INVALID_REQUEST);
        };
        if topic.name != wire::METADATA_TOPIC || partition.partition != wire::METADATA_PARTITION {
            return Some(wire::encode_fetch_snapshot_answer(
                0,
                Some(wire::FetchSnapshotPartition {
                    topic: topic.name.clone(),
                    index: partition.partition,
                    error_code: UNKNOWN_TOPIC_OR_PARTITION,
                    current_leader: false,
                    chunk: None,
                }),
                &self.quorum_leader(),
            ));
        }
        if let Some(error) = self.leader_only_request_error(partition.current_leader_epoch) {
            return partition_error(self, error);
        }
        let snapshot_id = (
            partition.snapshot_id.end_offset,
            partition.snapshot_id.epoch,
        );
        let checkpoint = (snapshot_id != BOOTSTRAP_SNAPSHOT_ID)
            .then(|| {
                load_checkpoint_by_id(
                    &checkpoint_dir(&self.data_dir),
                    snapshot_id.0,
                    snapshot_id.1,
                )
            })
            .flatten();
        let Some(bytes) = checkpoint else {
            return partition_error(self, SNAPSHOT_NOT_FOUND);
        };
        let size = i64::try_from(bytes.len()).unwrap_or(i64::MAX);
        if partition.position < 0 || partition.position >= size {
            return partition_error(self, POSITION_OUT_OF_RANGE);
        }
        // A voter catching up through KIP-630 is in contact with the leader even
        // though it sends no Fetch, so score it for check-quorum. Kafka does
        // this once the request passed its checks.
        if let Ok(from) = u64::try_from(request.replica_id) {
            self.on_event(Event::ReceiveFetchSnapshot { from: NodeId(from) });
        }
        // Both fields are slice indices off the wire. The position is inside
        // the checkpoint, and a negative `MaxBytes` reads nothing.
        let max = usize::try_from(request.max_bytes.max(0)).unwrap_or(0);
        let position = usize::try_from(partition.position).unwrap_or(0);
        let chunk = crate::snapshot::SnapshotReader::byte_range(&bytes, position, max);
        Some(wire::encode_fetch_snapshot_answer(
            0,
            Some(wire::FetchSnapshotPartition {
                topic: wire::METADATA_TOPIC.into(),
                index: wire::METADATA_PARTITION,
                error_code: 0,
                current_leader: true,
                chunk: Some((
                    snapshot_id,
                    size,
                    partition.position,
                    Bytes::copy_from_slice(chunk),
                )),
            }),
            &self.quorum_leader(),
        ))
    }
}
