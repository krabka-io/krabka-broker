//! Fetch response decoding that leaves record batches in their wire form.

use bytes::{Buf, BufMut};
use krabka_protocol::{
    Decode, Encode, ProtocolError, ProtocolRequest,
    owned::{
        fetch_request::FetchRequest,
        fetch_response::{
            AbortedTransaction, EpochEndOffset, FetchResponse, FetchableTopicResponse,
            LeaderIdAndEpoch, PartitionData, SnapshotId,
        },
    },
    primitives::{
        array::{get_array_len, get_nullable_array_len},
        fixed::{get_i16, get_i32, get_i64},
        string_bytes::{
            get_compact_nullable_bytes_owned, get_compact_string_owned, get_nullable_bytes_owned,
            get_string_owned,
        },
        uuid::get_uuid,
    },
    records::RecordsPayload,
    tagged_fields::read_tagged_fields,
};

pub(super) struct RawFetchRequest(pub(super) FetchRequest);

impl Encode for RawFetchRequest {
    fn encode<B: BufMut>(&self, buf: &mut B, version: i16) -> Result<(), ProtocolError> {
        self.0.encode(buf, version)
    }

    fn encoded_len(&self, version: i16) -> usize {
        self.0.encoded_len(version)
    }
}

impl ProtocolRequest for RawFetchRequest {
    const API_KEY: i16 = <FetchRequest as ProtocolRequest>::API_KEY;
    const MIN_VERSION: i16 = <FetchRequest as ProtocolRequest>::MIN_VERSION;
    const MAX_VERSION: i16 = <FetchRequest as ProtocolRequest>::MAX_VERSION;
    const FLEXIBLE_MIN: i16 = <FetchRequest as ProtocolRequest>::FLEXIBLE_MIN;
    type Response = RawFetchResponse;
}

pub(super) struct RawFetchResponse(pub(super) FetchResponse);

impl Decode<'_> for RawFetchResponse {
    fn decode<B: Buf>(buf: &mut B, version: i16) -> Result<Self, ProtocolError> {
        if !(<FetchRequest as ProtocolRequest>::MIN_VERSION
            ..=<FetchRequest as ProtocolRequest>::MAX_VERSION)
            .contains(&version)
        {
            return Err(ProtocolError::UnsupportedVersion {
                api_key: <FetchRequest as ProtocolRequest>::API_KEY,
                version,
            });
        }
        let flex = version >= <FetchRequest as ProtocolRequest>::FLEXIBLE_MIN;
        let mut out = FetchResponse {
            throttle_time_ms: get_i32(buf)?,
            ..FetchResponse::default()
        };
        if version >= 7 {
            out.error_code = get_i16(buf)?;
            out.session_id = get_i32(buf)?;
        }
        let count = get_array_len(buf, flex)?;
        out.responses.reserve(count);
        for _ in 0..count {
            out.responses.push(decode_topic(buf, version, flex)?);
        }
        if flex {
            out.unknown_tagged_fields = read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
        }
        Ok(Self(out))
    }
}

fn decode_topic<B: Buf>(
    buf: &mut B,
    version: i16,
    flex: bool,
) -> Result<FetchableTopicResponse, ProtocolError> {
    let mut out = FetchableTopicResponse::default();
    if version <= 12 {
        out.topic = if flex {
            get_compact_string_owned(buf)?
        } else {
            get_string_owned(buf)?
        };
    } else {
        out.topic_id = get_uuid(buf)?;
    }
    let count = get_array_len(buf, flex)?;
    out.partitions.reserve(count);
    for _ in 0..count {
        out.partitions.push(decode_partition(buf, version, flex)?);
    }
    if flex {
        out.unknown_tagged_fields = read_tagged_fields(buf, |_tag, _payload| Ok(false))?;
    }
    Ok(out)
}

fn decode_partition<B: Buf>(
    buf: &mut B,
    version: i16,
    flex: bool,
) -> Result<PartitionData, ProtocolError> {
    let mut out = PartitionData {
        partition_index: get_i32(buf)?,
        error_code: get_i16(buf)?,
        high_watermark: get_i64(buf)?,
        ..PartitionData::default()
    };
    if version >= 4 {
        out.last_stable_offset = get_i64(buf)?;
    }
    if version >= 5 {
        out.log_start_offset = get_i64(buf)?;
    }
    if version >= 4 {
        out.aborted_transactions = match get_nullable_array_len(buf, flex)? {
            None => None,
            Some(count) => {
                let mut transactions = Vec::with_capacity(count);
                for _ in 0..count {
                    transactions.push(AbortedTransaction::decode(buf, version)?);
                }
                Some(transactions)
            }
        };
    }
    if version >= 11 {
        out.preferred_read_replica = get_i32(buf)?;
    }
    let records = if flex {
        get_compact_nullable_bytes_owned(buf)?
    } else {
        get_nullable_bytes_owned(buf)?
    };
    out.records = records.map(RecordsPayload::Raw);
    if flex {
        let mut diverging_epoch = None;
        let mut current_leader = None;
        let mut snapshot_id = None;
        out.unknown_tagged_fields = read_tagged_fields(buf, |tag, payload| match tag {
            0 => {
                diverging_epoch = Some(EpochEndOffset::decode(payload, version)?);
                Ok(true)
            }
            1 => {
                current_leader = Some(LeaderIdAndEpoch::decode(payload, version)?);
                Ok(true)
            }
            2 => {
                snapshot_id = Some(SnapshotId::decode(payload, version)?);
                Ok(true)
            }
            _ => Ok(false),
        })?;
        if let Some(value) = diverging_epoch {
            out.diverging_epoch = value;
        }
        if let Some(value) = current_leader {
            out.current_leader = value;
        }
        if let Some(value) = snapshot_id {
            out.snapshot_id = value;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use krabka_protocol::{
        Decode as _, Encode as _,
        owned::fetch_response::{
            FetchResponse, FetchableTopicResponse, LeaderIdAndEpoch, PartitionData,
        },
        primitives::uuid::Uuid,
        records::RecordsPayload,
    };

    use super::RawFetchResponse;

    #[test]
    fn keeps_records_encoded() {
        let raw = Bytes::from_static(b"wire record bytes");
        for version in [12, 18] {
            let response = FetchResponse {
                responses: vec![FetchableTopicResponse {
                    topic: "t".into(),
                    topic_id: Uuid([7; 16]),
                    partitions: vec![PartitionData {
                        records: Some(RecordsPayload::Raw(raw.clone())),
                        current_leader: LeaderIdAndEpoch {
                            leader_id: 4,
                            leader_epoch: 9,
                            ..LeaderIdAndEpoch::default()
                        },
                        ..PartitionData::default()
                    }],
                    ..FetchableTopicResponse::default()
                }],
                ..FetchResponse::default()
            };
            let mut wire = BytesMut::new();
            response.encode(&mut wire, version).unwrap();
            let decoded = RawFetchResponse::decode(&mut wire.freeze(), version)
                .unwrap()
                .0;
            let partition = &decoded.responses[0].partitions[0];
            assert!(partition.records == Some(RecordsPayload::Raw(raw.clone())));
            assert!(partition.current_leader.leader_epoch == 9);
        }
    }
}
