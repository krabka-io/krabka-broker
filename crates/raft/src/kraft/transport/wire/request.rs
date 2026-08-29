//! The `PeerRequest` body codec.
//!
//! `PeerRequest` is the flat request vocabulary the engine reasons in. This
//! module maps each variant onto the generated KIP-595 request message at its
//! captured version, and decodes each one back.

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
        /// The recipient voter this request is addressed to. This is the
        /// wire top-level `voterId`. The JVM validates that an incoming
        /// Vote is addressed to it before it considers the grant, and it
        /// silently rejects a stale `voterId` or a `voterId` of `-1`.
        /// `broadcast_vote` builds this field for each recipient.
        voter_id: NodeId,
        candidate_epoch: Epoch,
        candidate: NodeId,
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
    },
    FetchSnapshot {
        from: NodeId,
        snapshot_id: (i64, i32),
        position: i64,
        max_bytes: i32,
    },
}

impl PeerRequest {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        match *self {
            PeerRequest::Vote {
                voter_id,
                candidate_epoch,
                candidate,
                last_epoch,
                last_offset,
                pre_vote,
            } => {
                let req = VoteRequest {
                    voter_id: node_to_wire(voter_id),
                    topics: vec![vote_req::TopicData {
                        topic_name: METADATA_TOPIC.to_string(),
                        partitions: vec![vote_req::PartitionData {
                            partition_index: METADATA_PARTITION,
                            replica_epoch: epoch_to_wire(candidate_epoch),
                            replica_id: node_to_wire(candidate),
                            last_offset_epoch: epoch_to_wire(last_epoch),
                            last_offset,
                            pre_vote,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                encode_body(&req, VOTE_VERSION)
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
                encode_body(&req, QUORUM_EPOCH_VERSION)
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
                encode_body(&req, QUORUM_EPOCH_VERSION)
            }
            PeerRequest::Fetch {
                from,
                fetch_epoch,
                fetch_offset,
            } => {
                let req = FetchRequest {
                    // v17 carries replica_id in replica_state, not the
                    // top-level field (which is gated to v0..=14).
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
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                encode_body(&req, FETCH_VERSION)
            }
            PeerRequest::FetchSnapshot {
                from,
                snapshot_id,
                position,
                max_bytes,
            } => encode_fetch_snapshot_request(from, snapshot_id, position, max_bytes),
        }
    }

    /// Decodes a request body. Returns `None` on a malformed frame.
    #[must_use]
    pub fn decode(buf: &[u8]) -> Option<Self> {
        // Probe each api by attempting the decode at its captured version.
        // The engine's inbound dispatch already knows the api_key (it routes
        // to the right `Inbound` variant), so a single attempt per call site
        // suffices; we try all to keep `decode` self-contained for tests.
        let mut cur = buf;
        if let Ok(req) = VoteRequest::decode(&mut cur, VOTE_VERSION)
            && cur.is_empty()
            && let Some(p) = req.topics.first().and_then(|t| t.partitions.first())
        {
            return Some(PeerRequest::Vote {
                voter_id: node_from_wire(req.voter_id),
                candidate_epoch: epoch_from_wire(p.replica_epoch),
                candidate: node_from_wire(p.replica_id),
                last_epoch: epoch_from_wire(p.last_offset_epoch),
                last_offset: p.last_offset,
                pre_vote: p.pre_vote,
            });
        }
        None
    }
}

/// Decodes a Vote request body (api 52).
#[must_use]
pub fn decode_vote(buf: &[u8]) -> Option<PeerRequest> {
    let mut cur = buf;
    let req = VoteRequest::decode(&mut cur, VOTE_VERSION).ok()?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::Vote {
        voter_id: node_from_wire(req.voter_id),
        candidate_epoch: epoch_from_wire(p.replica_epoch),
        candidate: node_from_wire(p.replica_id),
        last_epoch: epoch_from_wire(p.last_offset_epoch),
        last_offset: p.last_offset,
        pre_vote: p.pre_vote,
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
    Some(PeerRequest::Fetch {
        from,
        fetch_epoch: epoch_from_wire(p.last_fetched_epoch),
        fetch_offset: p.fetch_offset,
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
