//! The `PeerResponse` body codec.
//!
//! `PeerResponse` is the flat response vocabulary the engine reasons in. This
//! module maps each variant onto the generated KIP-595 response message at its
//! captured version, and decodes each one back into the variant the sending
//! engine feeds to the core.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        begin_quorum_epoch_response::BeginQuorumEpochResponse,
        fetch_response::{self as fetch_resp, FetchResponse},
        fetch_snapshot_response::{self as fs_resp, FetchSnapshotResponse},
        vote_response::{self as vote_resp, VoteResponse},
    },
    records::RecordsPayload,
};

use super::codec::{
    FETCH_SNAPSHOT_VERSION, FETCH_VERSION, METADATA_PARTITION, METADATA_TOPIC, METADATA_TOPIC_ID,
    QUORUM_EPOCH_VERSION, VOTE_VERSION, encode_body, epoch_from_wire, epoch_to_wire,
    node_from_wire, node_to_wire, records_payload_to_bytes,
};
use crate::kraft::types::{Epoch, LogOffsetMetadata, NodeId};

#[cfg(test)]
mod tests;

/// A peer RPC response body. The sending engine decodes it back into the
/// matching `Receive*Response` event, or applies it directly for Fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerResponse {
    Vote {
        epoch: Epoch,
        granted: bool,
    },
    /// `BeginQuorumEpoch` and `EndQuorumEpoch` acks carry the responder's
    /// epoch. They produce no core event.
    Ack {
        epoch: Epoch,
    },
    Fetch {
        leader_id: NodeId,
        leader_epoch: Epoch,
        diverging: Option<LogOffsetMetadata>,
        /// When set, the follower's fetch offset is below the leader's
        /// pruned log-start, and the follower must `FetchSnapshot` this
        /// snapshot instead. The tuple is `(end_offset, epoch)`.
        snapshot_id: Option<(i64, i32)>,
        /// Leader's high watermark at serve time.
        hwm: i64,
        /// Verbatim concatenated `RecordBatch` bytes for `[fetch_offset, log_end)`.
        records: Bytes,
    },
    /// Fetch could not identify a leader. The requester keeps its fetch
    /// watchdog armed instead of treating this as a successful heartbeat.
    FetchError {
        leader_epoch: Epoch,
        error_code: i16,
    },
    FetchSnapshot {
        snapshot_id: (i64, i32),
        size: i64,
        position: i64,
        bytes: Bytes,
        error_code: i16,
    },
}

/// Encodes a `FetchSnapshot` response body (api 59).
fn encode_fetch_snapshot_response(
    snapshot_id: (i64, i32),
    size: i64,
    position: i64,
    bytes: &Bytes,
    error_code: i16,
) -> Bytes {
    let (end_offset, epoch) = snapshot_id;
    let resp = FetchSnapshotResponse {
        topics: vec![fs_resp::TopicSnapshot {
            name: METADATA_TOPIC.to_string(),
            partitions: vec![fs_resp::PartitionSnapshot {
                index: METADATA_PARTITION,
                error_code,
                snapshot_id: fs_resp::SnapshotId {
                    end_offset,
                    epoch,
                    ..Default::default()
                },
                size,
                position,
                unaligned_records: RecordsPayload::Raw(bytes.clone()),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    encode_body(&resp, FETCH_SNAPSHOT_VERSION)
}

impl PeerResponse {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        match self {
            PeerResponse::Vote { epoch, granted } => {
                let resp = VoteResponse {
                    topics: vec![vote_resp::TopicData {
                        topic_name: METADATA_TOPIC.to_string(),
                        partitions: vec![vote_resp::PartitionData {
                            partition_index: METADATA_PARTITION,
                            leader_id: -1,
                            leader_epoch: epoch_to_wire(*epoch),
                            vote_granted: *granted,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                encode_body(&resp, VOTE_VERSION)
            }
            PeerResponse::Ack { epoch } => {
                // A Begin/End ack is encoded as the corresponding
                // BeginQuorumEpochResponse with the responder's leader_epoch.
                let resp = BeginQuorumEpochResponse {
                    topics: vec![
                        krabka_protocol::owned::begin_quorum_epoch_response::TopicData {
                            topic_name: METADATA_TOPIC.to_string(),
                            partitions: vec![
                                krabka_protocol::owned::begin_quorum_epoch_response::PartitionData {
                                    partition_index: METADATA_PARTITION,
                                    leader_id: -1,
                                    leader_epoch: epoch_to_wire(*epoch),
                                    ..Default::default()
                                },
                            ],
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                };
                encode_body(&resp, QUORUM_EPOCH_VERSION)
            }
            PeerResponse::Fetch {
                leader_id,
                leader_epoch,
                diverging,
                snapshot_id,
                hwm,
                records,
            } => {
                let mut partition = fetch_resp::PartitionData {
                    high_watermark: *hwm,
                    current_leader: fetch_resp::LeaderIdAndEpoch {
                        leader_id: node_to_wire(*leader_id),
                        leader_epoch: epoch_to_wire(*leader_epoch),
                        ..Default::default()
                    },
                    ..Default::default()
                };
                if let Some(point) = diverging {
                    partition.diverging_epoch = fetch_resp::EpochEndOffset {
                        epoch: epoch_to_wire(point.epoch),
                        end_offset: point.offset,
                        ..Default::default()
                    };
                }
                if let Some((end_offset, epoch)) = snapshot_id {
                    partition.snapshot_id = fetch_resp::SnapshotId {
                        end_offset: *end_offset,
                        epoch: *epoch,
                        ..Default::default()
                    };
                }
                if !records.is_empty() {
                    partition.records = Some(RecordsPayload::Raw(records.clone()));
                }
                let resp = FetchResponse {
                    responses: vec![fetch_resp::FetchableTopicResponse {
                        topic: METADATA_TOPIC.to_string(),
                        topic_id: METADATA_TOPIC_ID,
                        partitions: vec![partition],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                encode_body(&resp, FETCH_VERSION)
            }
            PeerResponse::FetchError {
                leader_epoch,
                error_code,
            } => {
                let resp = FetchResponse {
                    responses: vec![fetch_resp::FetchableTopicResponse {
                        topic: METADATA_TOPIC.to_string(),
                        topic_id: METADATA_TOPIC_ID,
                        partitions: vec![fetch_resp::PartitionData {
                            partition_index: METADATA_PARTITION,
                            error_code: *error_code,
                            high_watermark: -1,
                            current_leader: fetch_resp::LeaderIdAndEpoch {
                                leader_id: -1,
                                leader_epoch: epoch_to_wire(*leader_epoch),
                                ..Default::default()
                            },
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                };
                encode_body(&resp, FETCH_VERSION)
            }
            PeerResponse::FetchSnapshot {
                snapshot_id,
                size,
                position,
                bytes,
                error_code,
            } => encode_fetch_snapshot_response(*snapshot_id, *size, *position, bytes, *error_code),
        }
    }

    /// Decodes a Vote response body (api 52). The round, pre-vote or
    /// real, is not on the wire. The engine infers it from the candidate's
    /// role.
    #[must_use]
    pub fn decode_vote(buf: &[u8]) -> Option<Self> {
        let mut cur = buf;
        let resp = VoteResponse::decode(&mut cur, VOTE_VERSION).ok()?;
        let p = resp.topics.first()?.partitions.first()?;
        Some(PeerResponse::Vote {
            epoch: epoch_from_wire(p.leader_epoch),
            granted: p.vote_granted,
        })
    }

    /// Decodes a `BeginQuorumEpoch` or `EndQuorumEpoch` ack body
    /// (api 53 and api 54).
    #[must_use]
    pub fn decode_ack(buf: &[u8]) -> Option<Self> {
        let mut cur = buf;
        let resp = BeginQuorumEpochResponse::decode(&mut cur, QUORUM_EPOCH_VERSION).ok()?;
        let p = resp.topics.first()?.partitions.first()?;
        Some(PeerResponse::Ack {
            epoch: epoch_from_wire(p.leader_epoch),
        })
    }

    /// Decodes a Fetch response body (api 1).
    #[must_use]
    pub fn decode_fetch(buf: &[u8]) -> Option<Self> {
        let mut cur = buf;
        let resp = FetchResponse::decode(&mut cur, FETCH_VERSION).ok()?;
        let p = resp.responses.first()?.partitions.first()?;
        let leader_epoch = epoch_from_wire(p.current_leader.leader_epoch);
        if p.error_code != 0 && p.current_leader.leader_id < 0 {
            return Some(PeerResponse::FetchError {
                leader_epoch,
                error_code: p.error_code,
            });
        }
        let leader_id = node_from_wire(p.current_leader.leader_id);
        // diverging_epoch defaults to (-1, -1); a real divergence carries a
        // non-negative end_offset.
        let diverging = if p.diverging_epoch.end_offset >= 0 {
            Some(LogOffsetMetadata {
                offset: p.diverging_epoch.end_offset,
                epoch: epoch_from_wire(p.diverging_epoch.epoch),
            })
        } else {
            None
        };
        let snapshot_id = if p.snapshot_id.end_offset >= 0 {
            Some((p.snapshot_id.end_offset, p.snapshot_id.epoch))
        } else {
            None
        };
        let records = p
            .records
            .as_ref()
            .map_or_else(Bytes::new, records_payload_to_bytes);
        Some(PeerResponse::Fetch {
            leader_id,
            leader_epoch,
            diverging,
            snapshot_id,
            hwm: p.high_watermark,
            records,
        })
    }

    /// Decodes a `FetchSnapshot` response body (api 59).
    #[must_use]
    pub fn decode_fetch_snapshot(buf: &[u8]) -> Option<Self> {
        let mut cur = buf;
        let resp = FetchSnapshotResponse::decode(&mut cur, FETCH_SNAPSHOT_VERSION).ok()?;
        let p = resp.topics.first()?.partitions.first()?;
        let bytes = records_payload_to_bytes(&p.unaligned_records);
        Some(PeerResponse::FetchSnapshot {
            snapshot_id: (p.snapshot_id.end_offset, p.snapshot_id.epoch),
            size: p.size,
            position: p.position,
            bytes,
            error_code: p.error_code,
        })
    }
}
