//! Header-only framing of a `Produce` request, and the request decode that
//! builds it.
//!
//! The framing keeps each partition's records field as the bytes the producer
//! sent, so the hot path can decide verbatim passthrough per partition instead
//! of materializing owned records for the whole request.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::produce_request::ProduceRequest,
    primitives::uuid::Uuid as WireUuid,
    records::{RecordsPayload, count_records_in_v2_batches, produce_framing},
};

use crate::error::BrokerError;

/// One partition's records, as they arrived on the wire and BEFORE any owned
/// decode or decompression.
///
/// The verbatim hot path keeps the producer's exact bytes here. The owned
/// legacy path carries an already-decoded payload.
pub(super) enum PartitionPayload {
    /// v≥3 native records bytes captured zero-copy from the request frame.
    /// The value is a refcount view and not a copy. Nothing has validated or
    /// decompressed it yet. The per-partition dispatch validates the header
    /// and record structure, then decides between verbatim and owned.
    Slice(Bytes),
    /// Legacy v0-2 payload, or any pre-decoded payload. It always takes the
    /// owned path. The handler up-converts a v0/v1 `MessageSet` and never
    /// passes it through.
    Owned(RecordsPayload),
    /// Wire-null records field → `INVALID_REQUEST`.
    Null,
}

impl PartitionPayload {
    /// Records-field wire length in bytes.
    ///
    /// For the owned form it is `RecordsPayload::payload_len`. For the
    /// verbatim form it is the slice's own length. The KIP-13 bytes-in metrics
    /// and the producer byte-rate quota both use it.
    pub(super) fn payload_len(&self) -> usize {
        match self {
            Self::Slice(b) => b.len(),
            Self::Owned(p) => p.payload_len(),
            Self::Null => 0,
        }
    }

    /// Number of records across the field's batches, for `messages_in_total`.
    ///
    /// Verbatim slices read each v2 batch header's `records_count` WITHOUT
    /// decompression. Owned payloads sum `records.len()` over their v2
    /// batches.
    pub(super) fn message_count(&self) -> u64 {
        match self {
            Self::Slice(b) => count_records_in_v2_batches(b),
            Self::Owned(p) => p.as_v2().map_or(0, |batches| {
                batches.iter().map(|b| b.records.len() as u64).sum()
            }),
            Self::Null => 0,
        }
    }
}

/// Header-only framing of a `ProduceRequest`.
///
/// The field names match the owned struct's field names, so the handler body
/// differs only in the records form.
pub(super) struct ProduceFramed {
    pub(super) transactional_id: Option<String>,
    pub(super) acks: i16,
    pub(super) timeout_ms: i32,
    pub(super) topic_data: Vec<FramedTopic>,
}

pub(super) struct FramedTopic {
    pub(super) name: String,
    pub(super) topic_id: WireUuid,
    pub(super) partition_data: Vec<FramedPartition>,
}

pub(super) struct FramedPartition {
    pub(super) index: i32,
    pub(super) payload: PartitionPayload,
}

impl ProduceFramed {
    /// v≥3: build from the header-only `produce_framing` walk. This function
    /// decodes and decompresses no record body.
    fn from_framing(f: krabka_protocol::records::ProduceFraming) -> Self {
        Self {
            transactional_id: f.transactional_id,
            acks: f.acks,
            timeout_ms: f.timeout_ms,
            topic_data: f
                .topics
                .into_iter()
                .map(|t| FramedTopic {
                    name: t.name,
                    topic_id: WireUuid(t.topic_id.0),
                    partition_data: t
                        .partitions
                        .into_iter()
                        .map(|p| FramedPartition {
                            index: p.partition,
                            payload: match p.records {
                                Some(b) => PartitionPayload::Slice(b),
                                None => PartitionPayload::Null,
                            },
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// v0-2: wrap the fully-decoded legacy request. Every partition takes the
    /// owned path, because a legacy `MessageSet` up-conversion is never a
    /// passthrough.
    fn from_owned(req: ProduceRequest) -> Self {
        Self {
            transactional_id: req.transactional_id,
            acks: req.acks,
            timeout_ms: req.timeout_ms,
            topic_data: req
                .topic_data
                .into_iter()
                .map(|t| FramedTopic {
                    name: t.name,
                    topic_id: t.topic_id,
                    partition_data: t
                        .partition_data
                        .into_iter()
                        .map(|p| FramedPartition {
                            index: p.index,
                            payload: match p.records {
                                Some(rp) => PartitionPayload::Owned(rp),
                                None => PartitionPayload::Null,
                            },
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

pub(super) fn decode_produce_request(
    request_bytes: &[u8],
    body_bytes: Bytes,
    version: i16,
) -> Result<ProduceFramed, BrokerError> {
    if !(0..3).contains(&version) {
        return Ok(ProduceFramed::from_framing(produce_framing(
            body_bytes, version,
        )?));
    }
    let mut cursor = request_bytes;
    let owned: ProduceRequest =
        krabka_protocol::kafka_3_6_2::owned::produce_request::ProduceRequest::decode(
            &mut cursor,
            version,
        )?
        .into();
    Ok(ProduceFramed::from_owned(owned))
}
