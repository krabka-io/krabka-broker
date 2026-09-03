//! The `PeerRequest` body codec.
//!
//! `PeerRequest` is the flat request vocabulary the engine reasons in. This
//! module maps each variant onto the generated KIP-595 request message at its
//! captured version, and decodes each one back.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        begin_quorum_epoch_request::{self as bqe_req, BeginQuorumEpochRequest},
        end_quorum_epoch_request::{self as eqe_req, EndQuorumEpochRequest},
        fetch_request::{self as fetch_req, FetchRequest},
        fetch_snapshot_request::{self as fs_req, FetchSnapshotRequest},
        vote_request::{self as vote_req, VoteRequest},
    },
};
use krabka_verified::{
    VoteWireDecision,
    vote::{VoteEncodeDecision, vote_encode_decision},
    vote_wire_decision,
};

use super::codec::{
    FETCH_SNAPSHOT_VERSION, FETCH_VERSION, METADATA_PARTITION, METADATA_TOPIC, METADATA_TOPIC_ID,
    QUORUM_EPOCH_VERSION, VOTE_VERSION, encode_body, epoch_from_wire, epoch_to_wire,
    node_from_wire, node_to_wire,
};
use crate::kraft::types::{Epoch, NodeId};

#[cfg(test)]
mod tests;

/// A peer RPC request body, as encoded by the sending engine and decoded by
/// the receiving engine's inbound dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerRequest {
    Vote {
        cluster_id: Option<uuid::Uuid>,
        /// The recipient voter this request is addressed to. This is the
        /// wire top-level `voterId`. The JVM validates that an incoming
        /// Vote is addressed to it before it considers the grant, and it
        /// silently rejects a stale `voterId` or a `voterId` of `-1`.
        /// `broadcast_vote` builds this field for each recipient.
        voter_id: NodeId,
        voter_directory_id: uuid::Uuid,
        candidate_epoch: Epoch,
        candidate: NodeId,
        candidate_directory_id: uuid::Uuid,
        last_epoch: Epoch,
        last_offset: i64,
        pre_vote: bool,
    },
    BeginQuorumEpoch {
        leader_id: NodeId,
        leader_epoch: Epoch,
    },
    EndQuorumEpoch {
        leader_id: NodeId,
        leader_epoch: Epoch,
    },
    Fetch {
        from: NodeId,
        fetch_epoch: Epoch,
        fetch_offset: i64,
        replica_directory_id: uuid::Uuid,
    },
    FetchSnapshot {
        from: NodeId,
        snapshot_id: (i64, i32),
        position: i64,
        max_bytes: i32,
    },
}

impl PeerRequest {
    /// Encode a request whose Vote fields have already passed host validation.
    ///
    /// # Panics
    ///
    /// Panics if a Vote identity or epoch exceeds Kafka's signed `int32`
    /// range. Production Vote sends use [`Self::try_encode`] and fail closed.
    #[must_use]
    pub fn encode(&self) -> Bytes {
        self.try_encode()
            .expect("Vote fields must fit Kafka signed int32 wire fields")
    }

    /// Encode after checking the Vote identity and epoch wire range.
    #[must_use]
    pub fn try_encode(&self) -> Option<Bytes> {
        match *self {
            PeerRequest::Vote {
                cluster_id,
                voter_id,
                voter_directory_id,
                candidate_epoch,
                candidate,
                candidate_directory_id,
                last_epoch,
                last_offset,
                pre_vote,
            } => {
                if vote_encode_decision(voter_id.0, candidate.0, candidate_epoch, last_epoch)
                    != VoteEncodeDecision::Accept
                {
                    return None;
                }
                let req = VoteRequest {
                    cluster_id: cluster_id.map(|id| URL_SAFE_NO_PAD.encode(id.as_bytes())),
                    voter_id: i32::try_from(voter_id.0).ok()?,
                    topics: vec![vote_req::TopicData {
                        topic_name: METADATA_TOPIC.to_string(),
                        partitions: vec![vote_req::PartitionData {
                            partition_index: METADATA_PARTITION,
                            replica_epoch: i32::try_from(candidate_epoch).ok()?,
                            replica_id: i32::try_from(candidate.0).ok()?,
                            replica_directory_id: krabka_protocol::primitives::uuid::Uuid(
                                *candidate_directory_id.as_bytes(),
                            ),
                            voter_directory_id: krabka_protocol::primitives::uuid::Uuid(
                                *voter_directory_id.as_bytes(),
                            ),
                            last_offset_epoch: i32::try_from(last_epoch).ok()?,
                            last_offset,
                            pre_vote,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                Some(encode_body(&req, VOTE_VERSION))
            }
            PeerRequest::BeginQuorumEpoch {
                leader_id,
                leader_epoch,
            } => {
                let req = BeginQuorumEpochRequest {
                    topics: vec![bqe_req::TopicData {
                        topic_name: METADATA_TOPIC.to_string(),
                        partitions: vec![bqe_req::PartitionData {
                            partition_index: METADATA_PARTITION,
                            leader_id: node_to_wire(leader_id),
                            leader_epoch: epoch_to_wire(leader_epoch),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                Some(encode_body(&req, QUORUM_EPOCH_VERSION))
            }
            PeerRequest::EndQuorumEpoch {
                leader_id,
                leader_epoch,
            } => {
                let req = EndQuorumEpochRequest {
                    topics: vec![eqe_req::TopicData {
                        topic_name: METADATA_TOPIC.to_string(),
                        partitions: vec![eqe_req::PartitionData {
                            partition_index: METADATA_PARTITION,
                            leader_id: node_to_wire(leader_id),
                            leader_epoch: epoch_to_wire(leader_epoch),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                Some(encode_body(&req, QUORUM_EPOCH_VERSION))
            }
            PeerRequest::Fetch {
                from,
                fetch_epoch,
                fetch_offset,
                replica_directory_id,
            } => {
                let req = FetchRequest {
                    max_wait_ms: 500,
                    min_bytes: 1,
                    max_bytes: 1024 * 1024,
                    isolation_level: 0,
                    session_id: 0,
                    session_epoch: -1,
                    replica_state: fetch_req::ReplicaState {
                        replica_id: node_to_wire(from),
                        replica_epoch: -1,
                        ..Default::default()
                    },
                    topics: vec![fetch_req::FetchTopic {
                        topic: METADATA_TOPIC.to_string(),
                        topic_id: METADATA_TOPIC_ID,
                        partitions: vec![fetch_req::FetchPartition {
                            partition: METADATA_PARTITION,
                            current_leader_epoch: epoch_to_wire(fetch_epoch),
                            fetch_offset,
                            last_fetched_epoch: epoch_to_wire(fetch_epoch),
                            replica_directory_id: krabka_protocol::primitives::uuid::Uuid(
                                *replica_directory_id.as_bytes(),
                            ),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                Some(encode_body(&req, FETCH_VERSION))
            }
            PeerRequest::FetchSnapshot {
                from,
                snapshot_id,
                position,
                max_bytes,
            } => Some(encode_fetch_snapshot_request(
                from,
                snapshot_id,
                position,
                max_bytes,
            )),
        }
    }

    /// Decodes a request body. Returns `None` on a malformed frame.
    #[must_use]
    pub fn decode(buf: &[u8]) -> Option<Self> {
        decode_vote(buf)
    }
}

/// Decodes a Vote request body (api 52).
#[must_use]
pub fn decode_vote(buf: &[u8]) -> Option<PeerRequest> {
    let mut cur = buf;
    let req = VoteRequest::decode(&mut cur, VOTE_VERSION).ok()?;
    if !cur.is_empty() || req.topics.len() != 1 {
        return None;
    }
    let topic = req.topics.first()?;
    if topic.topic_name != METADATA_TOPIC || topic.partitions.len() != 1 {
        return None;
    }
    let p = topic.partitions.first()?;
    if p.partition_index != METADATA_PARTITION
        || vote_wire_decision(
            req.voter_id,
            p.replica_id,
            p.replica_epoch,
            p.last_offset_epoch,
        ) != VoteWireDecision::Accept
    {
        return None;
    }
    let cluster_id = match req.cluster_id.as_deref() {
        Some(id) => Some(parse_cluster_id(id)?),
        None => None,
    };
    Some(PeerRequest::Vote {
        cluster_id,
        voter_id: NodeId(u64::try_from(req.voter_id).ok()?),
        voter_directory_id: uuid::Uuid::from_bytes(p.voter_directory_id.0),
        candidate_epoch: u32::try_from(p.replica_epoch).ok()?,
        candidate: NodeId(u64::try_from(p.replica_id).ok()?),
        candidate_directory_id: uuid::Uuid::from_bytes(p.replica_directory_id.0),
        last_epoch: u32::try_from(p.last_offset_epoch).ok()?,
        last_offset: p.last_offset,
        pre_vote: p.pre_vote,
    })
}

fn parse_cluster_id(value: &str) -> Option<uuid::Uuid> {
    uuid::Uuid::parse_str(value).ok().or_else(|| {
        let bytes: [u8; 16] = URL_SAFE_NO_PAD.decode(value).ok()?.try_into().ok()?;
        Some(uuid::Uuid::from_bytes(bytes))
    })
}

/// Decodes a `BeginQuorumEpoch` request body (api 53).
#[must_use]
pub fn decode_begin(buf: &[u8]) -> Option<PeerRequest> {
    let mut cur = buf;
    let req = BeginQuorumEpochRequest::decode(&mut cur, QUORUM_EPOCH_VERSION).ok()?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::BeginQuorumEpoch {
        leader_id: node_from_wire(p.leader_id),
        leader_epoch: epoch_from_wire(p.leader_epoch),
    })
}

/// Decodes an `EndQuorumEpoch` request body (api 54).
#[must_use]
pub fn decode_end(buf: &[u8]) -> Option<PeerRequest> {
    let mut cur = buf;
    let req = EndQuorumEpochRequest::decode(&mut cur, QUORUM_EPOCH_VERSION).ok()?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::EndQuorumEpoch {
        leader_id: node_from_wire(p.leader_id),
        leader_epoch: epoch_from_wire(p.leader_epoch),
    })
}

/// Decodes a Fetch request body (api 1).
#[must_use]
pub fn decode_fetch(buf: &[u8]) -> Option<PeerRequest> {
    let mut cur = buf;
    let req = FetchRequest::decode(&mut cur, FETCH_VERSION).ok()?;
    let from = node_from_wire(req.replica_state.replica_id);
    let p = req.topics.first()?.partitions.first()?;
    let replica_directory_id = uuid::Uuid::from_bytes(p.replica_directory_id.0);
    Some(PeerRequest::Fetch {
        from,
        fetch_epoch: epoch_from_wire(p.last_fetched_epoch),
        fetch_offset: p.fetch_offset,
        replica_directory_id,
    })
}

/// Encodes a `FetchSnapshot` request body (api 59).
fn encode_fetch_snapshot_request(
    from: NodeId,
    snapshot_id: (i64, i32),
    position: i64,
    max_bytes: i32,
) -> Bytes {
    let (end_offset, epoch) = snapshot_id;
    let req = FetchSnapshotRequest {
        replica_id: node_to_wire(from),
        max_bytes,
        topics: vec![fs_req::TopicSnapshot {
            name: METADATA_TOPIC.to_string(),
            partitions: vec![fs_req::PartitionSnapshot {
                partition: METADATA_PARTITION,
                current_leader_epoch: epoch,
                snapshot_id: fs_req::SnapshotId {
                    end_offset,
                    epoch,
                    ..Default::default()
                },
                position,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    encode_body(&req, FETCH_SNAPSHOT_VERSION)
}

/// Decodes a `FetchSnapshot` request body (api 59).
#[must_use]
pub fn decode_fetch_snapshot(buf: &[u8]) -> Option<PeerRequest> {
    let mut cur = buf;
    let req = FetchSnapshotRequest::decode(&mut cur, FETCH_SNAPSHOT_VERSION).ok()?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::FetchSnapshot {
        from: node_from_wire(req.replica_id),
        snapshot_id: (p.snapshot_id.end_offset, p.snapshot_id.epoch),
        position: p.position,
        max_bytes: req.max_bytes,
    })
}
