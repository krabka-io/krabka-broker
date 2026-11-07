//! Constants and integer conversions shared by the peer request and response
//! codecs.
//!
//! The metadata topic identity, the captured wire versions, the `Epoch`,
//! `NodeId`, uuid and cluster id conversions to and from their wire forms, and the
//! `encode_body` helper are all needed by both [`request`](super::request) and
//! [`response`](super::response), so they sit in one place instead of being
//! duplicated.
//!
//! A generated encoder writes into its buffer as it goes, so the bytes of an
//! encode that failed are a truncated message. The helpers here never return
//! them.

use {
    crate::kraft::types::{Epoch, NodeId},
    base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD},
    bytes::{Bytes, BytesMut},
    krabka_protocol::{
        Encode, ProtocolError, primitives::uuid::Uuid as MetaUuid, records::RecordsPayload,
    },
    std::any::type_name,
};

/// `KRaft` metadata log topic name.
pub const METADATA_TOPIC: &str = "__cluster_metadata";
/// The single metadata partition.
pub const METADATA_PARTITION: i32 = 0;
/// The fixed `KRaft` `__cluster_metadata` topic id (KIP-595). Fetch v13 and
/// above key the topic by this id, not by name.
pub const METADATA_TOPIC_ID: MetaUuid = MetaUuid([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

/// Captured flexible wire versions, byte-validated against fixture frames.
pub const VOTE_VERSION: i16 = 2;
pub const QUORUM_EPOCH_VERSION: i16 = 1;
pub const FETCH_VERSION: i16 = 18;
pub const FETCH_SNAPSHOT_VERSION: i16 = 1;

/// The wire bytes of a decoded records payload, or `None` when a decoded
/// batch does not encode again. The caller treats `None` as a malformed
/// response.
pub fn records_payload_to_bytes(payload: &RecordsPayload) -> Option<Bytes> {
    match payload {
        RecordsPayload::Raw(bytes) => Some(bytes.clone()),
        other => {
            let mut out = BytesMut::new();
            match other.encode_to(&mut out) {
                Ok(()) => Some(out.freeze()),
                Err(error) => {
                    tracing::error!(%error, "a decoded records payload did not encode again");
                    None
                }
            }
        }
    }
}

/// Converts between the consensus `Epoch` (u32) and the wire `i32`.
/// `KRaft` uses an i32 `leaderEpoch`. The KIP-595 wire carries the leader
/// epoch as a raw `int32` and stays raw here. The consensus epoch is always
/// non-negative.
pub fn epoch_to_wire(e: Epoch) -> i32 { i32::try_from(e).unwrap_or(i32::MAX) }
pub fn epoch_from_wire(e: i32) -> Epoch { u32::try_from(e).unwrap_or(0) }
/// Converts between the `NodeId` (u64) and the wire `i32` replica id.
pub fn node_to_wire(n: NodeId) -> i32 { i32::try_from(n.0).unwrap_or(i32::MAX) }
pub fn node_from_wire(n: i32) -> NodeId { NodeId(u64::try_from(n).unwrap_or(0)) }
/// Converts between a `uuid::Uuid` and the wire `uuid` of the generated codecs.
pub fn uuid_to_wire(id: uuid::Uuid) -> MetaUuid { MetaUuid(id.into_bytes()) }
pub fn uuid_from_wire(id: MetaUuid) -> uuid::Uuid { uuid::Uuid::from_bytes(id.0) }
/// A request `ClusterId` in Kafka's base64 form.
pub fn cluster_id_to_wire(id: Option<uuid::Uuid>) -> Option<String> {
    id.map(|id| URL_SAFE_NO_PAD.encode(id.as_bytes()))
}

/// Encodes `msg` at `version`.
///
/// # Errors
/// Returns the encoder's error. The partly written bytes are dropped.
pub fn encode_message<T: Encode>(msg: &T, version: i16) -> Result<Bytes, ProtocolError> {
    let mut buf = BytesMut::with_capacity(msg.encoded_len(version));
    msg.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// Encodes a peer RPC body, or logs the encoder's error and returns `None`.
///
/// An answer of `None` drops the reply, which closes the connection, so a
/// peer never receives a truncated body.
pub fn encode_body<T: Encode>(msg: &T, version: i16) -> Option<Bytes> {
    encode_message(msg, version)
        .inspect_err(|error| {
            tracing::error!(message = type_name::<T>(), version, %error, "a peer RPC body did not encode");
        })
        .ok()
}

#[cfg(test)]
mod tests {
    use {
        super::{encode_body, encode_message},
        assert2::{assert, check},
        bytes::BufMut,
        krabka_protocol::{Encode, ProtocolError},
    };

    /// Writes one byte, then fails, as a generated encoder does when a field
    /// is out of range part way through a message.
    struct FailsAfterOneByte;

    impl Encode for FailsAfterOneByte {
        fn encode<B: BufMut>(&self, buf: &mut B, _version: i16) -> Result<(), ProtocolError> {
            buf.put_u8(1);
            Err(ProtocolError::InvalidValue("refused"))
        }

        fn encoded_len(&self, _version: i16) -> usize { 2 }
    }

    #[test]
    fn a_failed_encode_returns_the_error_not_the_truncated_bytes() {
        assert!(let Err(ProtocolError::InvalidValue("refused")) = encode_message(&FailsAfterOneByte, 0));
    }

    #[test]
    fn a_failed_peer_body_encode_returns_no_body() {
        check!(encode_body(&FailsAfterOneByte, 0) == None);
    }
}
