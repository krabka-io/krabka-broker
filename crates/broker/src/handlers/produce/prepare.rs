//! The decision that turns one partition's records field into a
//! [`PreparedBatch`]: verbatim passthrough of the producer's own bytes, or the
//! owned-decode fallback, plus the client-origin batch-header invariants that
//! both paths apply.

use std::sync::Arc;

use bytes::Bytes;
use krabka_compression::RecordDecompressionPolicy;
use krabka_protocol::records::{
    Attributes, RecordBatch, RecordsPayload, TimestampType, ValidatedBatch, validate_one_v2_batch,
};
use krabka_verified::produce::{ProduceBatchAdmission, produce_batch_admission};

use super::{framing::PartitionPayload, owned_decode::decode_owned_batch};
use crate::codes;

/// All the per-batch HEADER fields that the broker's produce gates need.
///
/// The gates are the leadership epoch stamp, the transactional verify, the
/// idempotent dedup, and the `acks=-1` HW target. The struct holds these
/// fields without materializing owned records. On the verbatim path they come
/// from the v2 batch header through
/// [`validate_one_v2_batch`]. On the owned fallback they come from the decoded
/// [`RecordBatch`] header. The values are identical on both paths.
#[derive(Debug)]
pub(super) struct PreparedBatch {
    pub(super) attributes: Attributes,
    pub(super) last_offset_delta: i32,
    pub(super) max_timestamp: i64,
    pub(super) producer_id: i64,
    pub(super) producer_epoch: i16,
    pub(super) base_sequence: i32,
    /// The append source. It is either the producer's verbatim bytes on the
    /// passthrough path, or the decoded owned batch on the fallback path. On
    /// the verbatim path the writer stamps the leader epoch at append time. On
    /// the owned path the code below stamps it onto the owned batch.
    pub(super) source: PreparedSource,
}

#[derive(Debug)]
pub(super) enum PreparedSource {
    /// Validated, single, CRC-checked v2 batch. The writer appends the
    /// producer's exact bytes after every declared record was parsed.
    Verbatim(Bytes),
    /// Decoded owned batch. This is the complete fallback path. When the
    /// producer compressed the batch, `RecordBatch::decode` decompressed it
    /// here.
    Owned(RecordBatch),
}

impl PreparedBatch {
    fn from_header(header: ValidatedHeader, bytes: Bytes) -> Self {
        Self {
            attributes: header.attributes,
            last_offset_delta: header.last_offset_delta,
            max_timestamp: header.max_timestamp,
            producer_id: header.producer_id,
            producer_epoch: header.producer_epoch,
            base_sequence: header.base_sequence,
            source: PreparedSource::Verbatim(bytes),
        }
    }

    fn from_owned(batch: RecordBatch) -> Self {
        Self {
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            source: PreparedSource::Owned(batch),
        }
    }

    /// Wire length of this batch as the writer will store it, when storing it
    /// means encoding it afresh.
    ///
    /// `None` is the verbatim path. Those bytes are the producer's own, byte
    /// for byte, so the length that arrived is the length that lands and the
    /// `max.message.bytes` gate already measured it before `prepare_batch`
    /// ran.
    ///
    /// The owned path re-encodes, and re-encoding moves the number. A batch
    /// the producer compressed that the topic stores under a different
    /// `compression.type` changes by the whole ratio between the two codecs,
    /// and `uncompressed` is the direction that grows: a 2 KiB gzip batch of
    /// repeated bytes is hundreds of kilobytes once the writer expands it. A
    /// legacy `MessageSet` moves too, by the v2 up-conversion.
    ///
    /// Kafka measures exactly this, in the same place. `message.max.bytes` is
    /// documented in `ServerConfigs` as "The largest record batch size allowed
    /// by Kafka (after compression if compression is enabled)", and
    /// `UnifiedLog.append` re-runs its per-batch size check over the
    /// *validated* records whenever `LogValidator` reports
    /// `messageSizeMaybeChanged`, throwing the same `RecordTooLargeException`
    /// its pre-append check throws.
    ///
    /// `None` also answers an encode this measurement cannot perform, because
    /// that is an encode the writer cannot perform either: the append fails on
    /// its own and reports its own error rather than borrowing this gate's.
    pub(super) fn stored_len(
        &self,
        topic_compression: Option<krabka_compression::CompressionType>,
    ) -> Option<usize> {
        let PreparedSource::Owned(batch) = &self.source else {
            return None;
        };
        match topic_compression {
            Some(target) if target != batch.attributes.compression() => {
                let mut stored = batch.clone();
                stored.attributes = stored.attributes.with_compression(target);
                encoded_len(&stored)
            }
            _ => encoded_len(batch),
        }
    }
}

/// Bytes that [`RecordBatch::encode`] writes, which for a compressed batch
/// only an encode can answer.
fn encoded_len(batch: &RecordBatch) -> Option<usize> {
    let mut buf = bytes::BytesMut::with_capacity(batch.encoded_len());
    batch.encode(&mut buf).ok().map(|()| buf.len())
}

/// Decide the append shape for one partition's records and extract the header
/// fields that the gates need without materializing owned records on the
/// verbatim path.
///
/// The verbatim-passthrough predicate holds only when ALL of these hold. It
/// matches the writer's recompression gate exactly:
///   1. the records are a v≥3 native-v2 slice, not legacy and not a wire-null
///      field;
///   2. the slice is exactly one complete, CRC-valid v2 batch whose body
///      contains exactly the declared structurally valid records;
///   3. `timestamp_type == CreateTime`; a client-supplied log-append-time
///      batch is invalid;
///   4. there is no broker-side recompression. The topic's `compression.type`
///      is `producer` pass-through, which is `None`, OR it equals the batch's
///      own codec.
///
/// On any miss the function decodes the records into an owned `RecordBatch`.
/// That is the complete fallback. The verbatim path transiently decompresses
/// compressed bodies only to validate their record structure, then discards
/// that buffer and retains the original compressed wire bytes.
/// [`decode_owned_batch`] up-converts the legacy v0-2 payloads.
///
/// The function returns the response error *code* on a bad field, either
/// `INVALID_REQUEST` or `INVALID_RECORD`.
pub(super) fn prepare_batch(
    payload: PartitionPayload,
    topic_compression: Option<krabka_compression::CompressionType>,
    topic_name: &Arc<str>,
    metrics: &crate::metrics::BrokerMetrics,
    policy: RecordDecompressionPolicy,
) -> Result<PreparedBatch, i16> {
    let bytes = match payload {
        // Legacy / pre-decoded payload: always owned.
        PartitionPayload::Owned(rp) => {
            let batch = decode_owned_batch(rp, topic_name, metrics, policy)?;
            validate_owned_client_batch(&batch)?;
            return Ok(PreparedBatch::from_owned(batch));
        }
        PartitionPayload::Null => return Err(codes::INVALID_REQUEST),
        PartitionPayload::Slice(b) => b,
    };

    // Extract the header fields into owned values up front so the borrow of
    // `bytes` (via the `ValidatedBatch`) ends before any `owned_fallback(bytes)`
    // move or the final `Verbatim(bytes)` construction.
    let validated = match validate_one_v2_batch(&bytes) {
        Ok(batch) if batch.total_len == bytes.len() => batch,
        _ => return owned_fallback(bytes, topic_name, metrics, policy),
    };
    let header = ValidatedHeader::from(&validated);
    let attributes = header.attributes;
    validate_client_batch_header(header)?;

    // (4) No recompression: producer pass-through, or target == current codec.
    if let Some(target) = topic_compression
        && target != attributes.compression()
    {
        return owned_fallback(bytes, topic_name, metrics, policy);
    }
    validated
        .validate_records(policy)
        .map_err(|_| codes::INVALID_RECORD)?;

    Ok(PreparedBatch::from_header(header, bytes))
}

/// The owned-decode fallback for a v≥3 records slice that the verbatim
/// predicate rejects.
///
/// Routes the raw field bytes through `RecordsPayload::from_bytes` — which
/// dispatches v2 (parse every batch) vs legacy (v0/v1 `MessageSet`, kept
/// opaque) by the magic byte — then through [`decode_owned_batch`], the same
/// pipeline the request decoder used before the verbatim path existed. This is
/// what up-converts a v1 `MessageSet` carried over a v≥3 produce (older
/// message-format clients) and surfaces `INVALID_RECORD` on malformed bytes.
pub(super) fn owned_fallback(
    bytes: Bytes,
    topic_name: &Arc<str>,
    metrics: &crate::metrics::BrokerMetrics,
    policy: RecordDecompressionPolicy,
) -> Result<PreparedBatch, i16> {
    match RecordsPayload::from_bytes_with_policy(bytes, policy) {
        Ok(rp) => decode_owned_batch(rp, topic_name, metrics, policy).and_then(|batch| {
            validate_owned_client_batch(&batch)?;
            Ok(PreparedBatch::from_owned(batch))
        }),
        Err(_) => Err(codes::INVALID_RECORD),
    }
}

/// The v2 batch header fields that the gates need, copied out of a borrowed
/// [`ValidatedBatch`] so that the code can move the verbatim `Bytes`
/// afterward.
#[derive(Debug, Clone, Copy)]
struct ValidatedHeader {
    base_offset: i64,
    attributes: Attributes,
    last_offset_delta: i32,
    records_count: i32,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
}

impl From<&ValidatedBatch<'_>> for ValidatedHeader {
    fn from(v: &ValidatedBatch<'_>) -> Self {
        Self {
            base_offset: v.header.base_offset.get(),
            attributes: Attributes(v.header.attributes.get()),
            last_offset_delta: v.header.last_offset_delta.get(),
            records_count: v.header.records_count.get(),
            max_timestamp: v.header.max_timestamp.get(),
            producer_id: v.header.producer_id.get(),
            producer_epoch: v.header.producer_epoch.get(),
            base_sequence: v.header.base_sequence.get(),
        }
    }
}

/// Apply Kafka's client-origin v2 batch-header invariants without decoding
/// the record body. Every field is covered by the batch CRC that
/// [`validate_one_v2_batch`] checked before this function runs.
fn validate_client_batch_header(batch: ValidatedHeader) -> Result<(), i16> {
    validate_client_batch_fields(
        batch.attributes,
        batch.base_offset,
        batch.last_offset_delta,
        batch.records_count,
        batch.producer_id,
        batch.base_sequence,
    )
}

fn validate_owned_client_batch(batch: &RecordBatch) -> Result<(), i16> {
    let records_count = i32::try_from(batch.records.len()).map_err(|_| codes::INVALID_RECORD)?;
    validate_client_batch_fields(
        batch.attributes,
        batch.base_offset,
        batch.last_offset_delta,
        records_count,
        batch.producer_id,
        batch.base_sequence,
    )
}

fn validate_client_batch_fields(
    attributes: Attributes,
    base_offset: i64,
    last_offset_delta: i32,
    records_count: i32,
    producer_id: i64,
    base_sequence: i32,
) -> Result<(), i16> {
    match produce_batch_admission(
        base_offset,
        last_offset_delta,
        records_count,
        attributes.is_control_batch(),
        producer_id,
        base_sequence,
        attributes.timestamp_type() == TimestampType::CreateTime,
    ) {
        ProduceBatchAdmission::Admit => Ok(()),
        ProduceBatchAdmission::InvalidRecord => Err(codes::INVALID_RECORD),
        ProduceBatchAdmission::InvalidTimestamp => Err(codes::INVALID_TIMESTAMP),
    }
}

#[cfg(test)]
mod tests;
