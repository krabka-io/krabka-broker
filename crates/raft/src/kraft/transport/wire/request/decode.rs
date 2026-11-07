//! The test-only decoders of the `PeerRequest` bodies this node encodes.
//!
//! The engine's inbound dispatch decodes the generated KIP-595 messages
//! itself, with Kafka's own sentinel checks. These decoders only let tests read
//! back what an engine sent, so they are built for tests alone.

use {
    super::{
        super::codec::{
            FETCH_SNAPSHOT_VERSION, FETCH_VERSION, METADATA_PARTITION, METADATA_TOPIC,
            QUORUM_EPOCH_VERSION, VOTE_VERSION, epoch_from_wire, node_from_wire, uuid_from_wire,
        },
        PeerRequest, PreferredCandidate, decode_request, parse_cluster_id,
    },
    crate::{
        kraft::{snapshot_fetch::SnapshotId, types::NodeId},
        voter_wire::endpoint as voter_endpoint,
    },
    krabka_protocol::{
        Decode,
        owned::{
            begin_quorum_epoch_request::BeginQuorumEpochRequest,
            end_quorum_epoch_request::EndQuorumEpochRequest, fetch_request::FetchRequest,
            fetch_snapshot_request::FetchSnapshotRequest, vote_request::VoteRequest,
        },
    },
    krabka_verified::{VoteWireDecision, vote_wire_decision},
};

impl PeerRequest {
    /// Decodes a request body. Returns `None` on a malformed frame.
    #[must_use]
    pub fn decode(buf: &[u8]) -> Option<Self> { decode_vote(buf) }
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
    Some(PeerRequest::Vote {
        cluster_id: request_cluster_id(req.cluster_id.as_deref()).ok()?,
        voter_id: NodeId(u64::try_from(req.voter_id).ok()?),
        voter_directory_id: uuid_from_wire(p.voter_directory_id),
        candidate_epoch: u32::try_from(p.replica_epoch).ok()?,
        candidate: NodeId(u64::try_from(p.replica_id).ok()?),
        candidate_directory_id: uuid_from_wire(p.replica_directory_id),
        last_epoch: u32::try_from(p.last_offset_epoch).ok()?,
        last_offset: p.last_offset,
        pre_vote: p.pre_vote,
    })
}

/// Parses an optional request `ClusterId`. An absent one is `Ok(None)`; one
/// that is present but does not parse is the error.
fn request_cluster_id(value: Option<&str>) -> Result<Option<uuid::Uuid>, ()> {
    value.map(|id| parse_cluster_id(id).ok_or(())).transpose()
}

/// Decodes a `BeginQuorumEpoch` request body (api 53).
#[must_use]
pub fn decode_begin(buf: &[u8]) -> Option<PeerRequest> {
    let req = decode_request::<BeginQuorumEpochRequest>(buf, QUORUM_EPOCH_VERSION)?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::BeginQuorumEpoch {
        cluster_id: request_cluster_id(req.cluster_id.as_deref()).ok()?,
        voter_id: node_from_wire(req.voter_id),
        voter_directory_id: uuid_from_wire(p.voter_directory_id),
        leader_id: node_from_wire(p.leader_id),
        leader_epoch: epoch_from_wire(p.leader_epoch),
        leader_endpoints: req
            .leader_endpoints
            .iter()
            .map(|endpoint| voter_endpoint(&endpoint.name, &endpoint.host, endpoint.port))
            .collect(),
    })
}

/// Decodes an `EndQuorumEpoch` request body (api 54).
#[must_use]
pub fn decode_end(buf: &[u8]) -> Option<PeerRequest> {
    let req = decode_request::<EndQuorumEpochRequest>(buf, QUORUM_EPOCH_VERSION)?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::EndQuorumEpoch {
        cluster_id: request_cluster_id(req.cluster_id.as_deref()).ok()?,
        leader_id: node_from_wire(p.leader_id),
        leader_epoch: epoch_from_wire(p.leader_epoch),
        preferred_candidates: p
            .preferred_candidates
            .iter()
            .map(|candidate| PreferredCandidate {
                id: node_from_wire(candidate.candidate_id),
                directory_id: uuid_from_wire(candidate.candidate_directory_id),
            })
            .collect(),
    })
}

/// Decodes a Fetch request body (api 1).
#[must_use]
pub fn decode_fetch(buf: &[u8]) -> Option<PeerRequest> {
    let req = decode_request::<FetchRequest>(buf, FETCH_VERSION)?;
    let from = node_from_wire(req.replica_state.replica_id);
    let p = req.topics.first()?.partitions.first()?;
    let replica_directory_id = uuid_from_wire(p.replica_directory_id);
    Some(PeerRequest::Fetch {
        cluster_id: request_cluster_id(req.cluster_id.as_deref()).ok()?,
        from,
        max_wait_ms: req.max_wait_ms,
        current_leader_epoch: p.current_leader_epoch,
        fetch_epoch: epoch_from_wire(p.last_fetched_epoch),
        fetch_offset: p.fetch_offset,
        replica_directory_id,
        high_watermark: p.high_watermark,
    })
}

/// Decodes a `FetchSnapshot` request body (api 59).
#[must_use]
pub fn decode_fetch_snapshot(buf: &[u8]) -> Option<PeerRequest> {
    let req = decode_request::<FetchSnapshotRequest>(buf, FETCH_SNAPSHOT_VERSION)?;
    let p = req.topics.first()?.partitions.first()?;
    Some(PeerRequest::FetchSnapshot {
        cluster_id: request_cluster_id(req.cluster_id.as_deref()).ok()?,
        from: node_from_wire(req.replica_id),
        current_leader_epoch: p.current_leader_epoch,
        snapshot_id: SnapshotId::new(p.snapshot_id.end_offset, p.snapshot_id.epoch),
        position: p.position,
        max_bytes: req.max_bytes,
    })
}
