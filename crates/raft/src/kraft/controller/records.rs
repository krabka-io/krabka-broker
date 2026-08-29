//! Construction, encoding and decoding of the record batches the engine writes
//! to and reads from the metadata log: metadata value batches, KIP-853 typed
//! control batches, and the KIP-595 `LeaderChange` marker.

use krabka_ids::Offset;
use krabka_metadata::VoterSet;
use krabka_protocol::{
    Decode, Encode,
    records::{Record, RecordBatch, metadata::control::ControlRecord},
};

use crate::{
    error::RaftError,
    kraft::types::{Epoch, NodeId},
};

pub fn metadata_record_batch(leader_epoch: Epoch, blobs: &[bytes::Bytes]) -> RecordBatch {
    let records: Vec<Record> = blobs
        .iter()
        .map(|blob| Record {
            value: Some(blob.clone()),
            ..Default::default()
        })
        .collect();

    RecordBatch {
        partition_leader_epoch: i32::try_from(leader_epoch).unwrap_or(i32::MAX),
        last_offset_delta: i32::try_from(blobs.len().saturating_sub(1)).unwrap_or(0),
        records,
        ..Default::default()
    }
}

pub fn typed_control_batch(
    leader_epoch: Epoch,
    controls: &[ControlRecord],
) -> Result<RecordBatch, RaftError> {
    let records = controls
        .iter()
        .enumerate()
        .map(|(index, control)| {
            let (key, value) = control.encode_key_value()?;
            Ok(Record {
                offset_delta: i32::try_from(index).unwrap_or(i32::MAX),
                key: Some(key),
                value: Some(value),
                ..Default::default()
            })
        })
        .collect::<Result<Vec<_>, krabka_protocol::ProtocolError>>()?;
    Ok(RecordBatch {
        partition_leader_epoch: i32::try_from(leader_epoch).unwrap_or(i32::MAX),
        attributes: krabka_protocol::records::Attributes::default().with_control(true),
        last_offset_delta: i32::try_from(controls.len().saturating_sub(1)).unwrap_or(i32::MAX),
        records,
        ..Default::default()
    })
}

pub fn decode_control_record(record: &Record) -> Result<Option<ControlRecord>, RaftError> {
    let (Some(key), Some(value)) = (&record.key, &record.value) else {
        return Ok(None);
    };
    Ok(Some(ControlRecord::decode(key, value)?))
}

/// Build the leader's `LeaderChange` control batch for `epoch`: a single
/// KIP-595 `LeaderChange` control record (control-batch attribute set), naming
/// the new leader and the current voter set. A real `KRaft` batch MUST contain at
/// least one record — an empty batch crashes a JVM follower
/// (`Batch must contain at least one record`) — so this carries the proper
/// `LeaderChangeMessage` rather than zero records. Krabka readers skip it via
/// `is_control_batch()`; it occupies exactly one log offset
/// (`last_offset_delta = 0`), unchanged from the prior empty batch.
// The `version: 0` field equals `LeaderChangeMessage`'s `Default` (i16 -> 0), so
// deleting it yields byte-identical encoding; it is not the wire schema version
// (that is the `0` passed to `msg.encode`). Equivalent mutant.
#[cfg_attr(test, mutants::skip)]
pub fn leader_change_batch(
    epoch: Epoch,
    leader_id: NodeId,
    voter_set: &VoterSet,
    kraft_version: u16,
) -> RecordBatch {
    use krabka_protocol::{
        Encode,
        owned::{
            common::leader_change_message::voter::Voter, leader_change_message::LeaderChangeMessage,
        },
        records::{
            header::Attributes,
            metadata::control::{ControlRecordType, control_record_key},
        },
    };

    let message_version = i16::from(kraft_version >= 1);
    let voters: Vec<Voter> = voter_set
        .iter()
        .map(|voter| Voter {
            voter_id: i32::try_from(voter.id.0).unwrap_or(i32::MAX),
            voter_directory_id: krabka_protocol::primitives::uuid::Uuid(
                *voter.directory_id.as_bytes(),
            ),
            ..Default::default()
        })
        .collect();
    let msg = LeaderChangeMessage {
        version: message_version,
        leader_id: i32::try_from(leader_id.0).unwrap_or(i32::MAX),
        voters: voters.clone(),
        granting_voters: voters,
        ..Default::default()
    };
    let mut value = bytes::BytesMut::new();
    // LeaderChangeMessage v0; encode is infallible for a well-formed message.
    let _ = msg.encode(&mut value, message_version);
    let key = control_record_key(ControlRecordType::LeaderChange);
    RecordBatch {
        partition_leader_epoch: i32::try_from(epoch).unwrap_or(i32::MAX),
        attributes: Attributes::default().with_control(true),
        last_offset_delta: 0,
        records: vec![Record {
            offset_delta: 0,
            key: Some(key),
            value: Some(value.freeze()),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Encode a run of `RecordBatch`es into one contiguous `Bytes` blob (each batch
/// is self-describing via its `batch_length` header, so they concatenate and
/// decode back in order — see [`decode_batches`]). Used by the leader's Fetch
/// serve path to ship replicated record bytes to a follower.
pub fn encode_batches(batches: &[RecordBatch]) -> bytes::Bytes {
    let mut out = bytes::BytesMut::new();
    for batch in batches {
        if let Err(e) = batch.encode(&mut out) {
            tracing::error!(?e, "kraft: encode batch for fetch serve failed");
        }
    }
    out.freeze()
}

/// Decode the contiguous `Bytes` blob produced by [`encode_batches`] back into a
/// `Vec<RecordBatch>` (each batch's `base_offset` is preserved). Used by the
/// follower's Fetch-response apply path.
pub fn decode_batches(mut buf: &[u8]) -> Result<Vec<RecordBatch>, RaftError> {
    let mut out = Vec::new();
    while !buf.is_empty() {
        match RecordBatch::decode(&mut buf) {
            Ok(batch) => out.push(batch),
            Err(e) => {
                return Err(RaftError::ChangeRejected(format!(
                    "decode replicated batch: {e}"
                )));
            }
        }
    }
    Ok(out)
}

pub fn next_batch_offset(batches: &[RecordBatch]) -> Option<Offset> {
    batches.last().map(|batch| {
        Offset(
            batch
                .base_offset
                .saturating_add(i64::from(batch.last_offset_delta))
                .saturating_add(1),
        )
    })
}
