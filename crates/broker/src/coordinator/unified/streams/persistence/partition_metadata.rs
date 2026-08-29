//! The streams partition metadata record at key version 18.
//!
//! The value is the snapshot of every topic the group consumes or produces,
//! each with the uuid and the partition count that the metadata image held
//! when the broker computed the assignment. The uuid encodes as a
//! length-prefixed 16-byte value.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::ProtocolError;

use crate::{
    coordinator::unified::persistence::{
        get_bytes, get_i16, get_i32, get_string, put_bytes, put_string,
    },
    error::BrokerError,
};

/// A topic that the group consumes or produces, with its uuid and its
/// partition count as the metadata image held them when the broker computed
/// the assignment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsTopicMeta {
    pub topic_name: String,
    pub topic_id: uuid::Uuid,
    pub num_partitions: i32,
}

/// Key v18 value: per-topic partition metadata snapshot for the group.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamsGroupPartitionMetadataValue {
    pub topics: Vec<StreamsTopicMeta>,
}

impl StreamsGroupPartitionMetadataValue {
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        let n = i32::try_from(self.topics.len()).expect("fits");
        buf.put_i32(n);
        for t in &self.topics {
            put_string(&mut buf, &t.topic_name);
            put_bytes(&mut buf, &Bytes::copy_from_slice(t.topic_id.as_bytes()));
            buf.put_i32(t.num_partitions);
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
        let mut topics = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let topic_name = get_string(&mut buf)?;
            let id_bytes = get_bytes(&mut buf)?;
            if id_bytes.len() != 16 {
                return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                    "topic_id not 16 bytes",
                )));
            }
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&id_bytes);
            let topic_id = uuid::Uuid::from_bytes(arr);
            let num_partitions = get_i32(&mut buf)?;
            topics.push(StreamsTopicMeta {
                topic_name,
                topic_id,
                num_partitions,
            });
        }
        Ok(Self { topics })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::streams::persistence::{
        KEY_STREAMS_PARTITION_METADATA, StreamsGroupKey, encode_partition_metadata_key,
        parse_streams_key, test_support::peek_version,
    };

    #[test]
    fn partition_metadata_round_trip() {
        let kb = encode_partition_metadata_key("g1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_PARTITION_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::PartitionMetadata {
                    group_id: "g1".into()
                }
        );

        let v = StreamsGroupPartitionMetadataValue {
            topics: vec![
                StreamsTopicMeta {
                    topic_name: "in-a".into(),
                    topic_id: uuid::Uuid::from_bytes([1; 16]),
                    num_partitions: 6,
                },
                StreamsTopicMeta {
                    topic_name: "in-b".into(),
                    topic_id: uuid::Uuid::from_bytes([2; 16]),
                    num_partitions: 0,
                },
            ],
        };
        assert!(StreamsGroupPartitionMetadataValue::decode(&v.encode()).unwrap() == v);
    }
}
