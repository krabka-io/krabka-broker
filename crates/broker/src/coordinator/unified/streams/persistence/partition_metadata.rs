//! The streams partition metadata record at key version 18.
//!
//! The value is the snapshot of every topic the group consumes or produces,
//! each with the uuid and the partition count that the metadata image held
//! when the broker computed the assignment.
//!
//! # Layout
//!
//! This is the one record here with no Apache Kafka schema at tag `4.3.1`:
//! KIP-1101 replaced Kafka's streams partition-metadata record with a metadata
//! hash, and its `apiKey` 18 is now unassigned. Kafka's
//! `GroupCoordinatorRecordSerde` therefore does not know the type at all, and
//! its tools skip the record instead of mis-decoding it.
//!
//! The layout still follows the flexible conventions of the records around it,
//! so that one set of leaf helpers serves every value the broker writes: a
//! compact array of `{TopicName string, TopicId uuid, NumPartitions int32}`,
//! each entry and the message ending with a tagged-field count.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        flex::{
            get_compact_array_len, get_compact_string, get_uuid, put_compact_array_len,
            put_compact_string, put_empty_tagged_fields, put_uuid, skip_tagged_fields,
        },
        get_i16, get_i32,
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
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_compact_array_len(&mut buf, self.topics.len());
        for t in &self.topics {
            put_compact_string(&mut buf, &t.topic_name);
            put_uuid(&mut buf, *t.topic_id.as_bytes());
            buf.put_i32(t.num_partitions);
            put_empty_tagged_fields(&mut buf);
        }
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let n = get_compact_array_len(&mut buf)?;
        let mut topics = Vec::with_capacity(n);
        for _ in 0..n {
            let topic_name = get_compact_string(&mut buf)?;
            let topic_id = uuid::Uuid::from_bytes(get_uuid(&mut buf)?);
            let num_partitions = get_i32(&mut buf)?;
            skip_tagged_fields(&mut buf)?;
            topics.push(StreamsTopicMeta {
                topic_name,
                topic_id,
                num_partitions,
            });
        }
        skip_tagged_fields(&mut buf)?;
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

    #[test]
    fn partition_metadata_bytes_are_flexible() {
        let v = StreamsGroupPartitionMetadataValue {
            topics: vec![StreamsTopicMeta {
                topic_name: "t".into(),
                topic_id: uuid::Uuid::from_bytes([1; 16]),
                num_partitions: 6,
            }],
        };
        let mut want: Vec<u8> = vec![0x00, 0x00, 0x02];
        want.extend_from_slice(b"\x02t");
        want.extend_from_slice(&[1u8; 16]);
        want.extend_from_slice(&6i32.to_be_bytes());
        want.push(0x00); // entry tagged fields
        want.push(0x00); // message tagged fields
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn partition_metadata_rejects_a_missing_tagged_trailer() {
        let full = StreamsGroupPartitionMetadataValue::default().encode();
        assert!(StreamsGroupPartitionMetadataValue::decode(&full[..full.len() - 1]).is_err());
    }
}
