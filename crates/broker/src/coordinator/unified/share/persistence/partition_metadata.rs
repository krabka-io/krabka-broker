//! The KIP-932 share-group state partition metadata record at key version 14.
//!
//! This record is the only share-group value that encodes a topic id as sixteen
//! raw bytes rather than through the length-prefixed `put_bytes` leaf, so the
//! `get_uuid` reader lives here beside it.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::ProtocolError;

use crate::{
    coordinator::unified::persistence::{get_i16, get_i32},
    error::BrokerError,
};

/// KIP-932 `ShareGroupStatePartitionMetadata`, key v14.
///
/// The record tracks which `(topic_id, partition)` share-states a group has
/// initialized. It also holds a set of topic ids whose share-state the broker is
/// deleting. The record lets the group coordinator skip the re-initialization of
/// partitions across restarts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupStatePartitionMetadataValue {
    pub initialized: Vec<(uuid::Uuid, Vec<i32>)>,
    pub deleting: Vec<uuid::Uuid>,
}

impl ShareGroupStatePartitionMetadataValue {
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        let n = i32::try_from(self.initialized.len()).expect("fits");
        buf.put_i32(n);
        for (topic_id, partitions) in &self.initialized {
            buf.put_slice(topic_id.as_bytes());
            let pn = i32::try_from(partitions.len()).expect("fits");
            buf.put_i32(pn);
            for p in partitions {
                buf.put_i32(*p);
            }
        }
        let dn = i32::try_from(self.deleting.len()).expect("fits");
        buf.put_i32(dn);
        for topic_id in &self.deleting {
            buf.put_slice(topic_id.as_bytes());
        }
        buf.freeze()
    }

    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut initialized = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let topic_id = get_uuid(&mut buf)?;
            let pn = get_i32(&mut buf)?;
            let pcap = usize::try_from(pn.max(0)).expect("non-negative");
            let mut partitions = Vec::with_capacity(pcap);
            for _ in 0..pn.max(0) {
                partitions.push(get_i32(&mut buf)?);
            }
            initialized.push((topic_id, partitions));
        }
        let dn = get_i32(&mut buf)?;
        let dcap = usize::try_from(dn.max(0)).expect("non-negative");
        let mut deleting = Vec::with_capacity(dcap);
        for _ in 0..dn.max(0) {
            deleting.push(get_uuid(&mut buf)?);
        }
        Ok(Self {
            initialized,
            deleting,
        })
    }
}

fn get_uuid(buf: &mut &[u8]) -> Result<uuid::Uuid, BrokerError> {
    if buf.len() < 16 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "topic_id not 16 bytes",
        )));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&buf[..16]);
    bytes::Buf::advance(buf, 16);
    Ok(uuid::Uuid::from_bytes(arr))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::share::persistence::{
        KEY_SHARE_GROUP_STATE_PARTITION_METADATA, ShareGroupKey, encode_share_key, parse_share_key,
        test_support::peek_version,
    };

    #[test]
    fn state_partition_metadata_round_trip() {
        let key = ShareGroupKey::StatePartitionMetadata {
            group_id: "g1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert!(ver == KEY_SHARE_GROUP_STATE_PARTITION_METADATA);
        assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupStatePartitionMetadataValue {
            initialized: vec![
                (uuid::Uuid::from_bytes([1; 16]), vec![0, 1, 2]),
                (uuid::Uuid::from_bytes([2; 16]), vec![]),
            ],
            deleting: vec![uuid::Uuid::from_bytes([9; 16])],
        };
        assert!(ShareGroupStatePartitionMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn state_partition_metadata_empty_round_trip() {
        let v = ShareGroupStatePartitionMetadataValue::default();
        assert!(ShareGroupStatePartitionMetadataValue::decode(&v.encode()).unwrap() == v);
    }
}
