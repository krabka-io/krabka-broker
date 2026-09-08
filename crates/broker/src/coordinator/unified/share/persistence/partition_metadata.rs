//! The KIP-932 share-group state partition metadata record at key version 15.
//!
//! # Layout
//!
//! From `ShareGroupStatePartitionMetadataValue.json` at Apache Kafka tag
//! `4.3.1`, which declares `"flexibleVersions": "0+"`. Three plain arrays, in
//! this order:
//!
//! - `InitializingTopics` (`[]TopicPartitionsInfo{TopicId uuid, TopicName
//!   string, Partitions []int32}`),
//! - `InitializedTopics` (the same struct),
//! - `DeletingTopics` (`[]TopicInfo{TopicId uuid, TopicName string}`).
//!
//! # Topic names
//!
//! `__consumer_offsets` is read by tooling that has no access to the broker's
//! topic-id index, so every entry carries the topic's real name, exactly as
//! `GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord`
//! writes `InitMapValue.name()`. The name reaches this record from the
//! metadata image, and it survives a restart because the decoder keeps the
//! name each entry was written with.
//!
//! A name the broker cannot resolve is written as [`UNKNOWN_TOPIC_NAME`],
//! which is what `GroupMetadataManager.attachTopicName` and
//! `GroupMetadataManager.attachInitValue` write for a topic id the metadata
//! image no longer holds.
//!
//! The broker's share-state lifecycle has no separate "initializing" phase to
//! persist, so it writes an empty `InitializingTopics`, and it drives the
//! persister's delete to completion before it writes the record rather than
//! staging the topic in `DeletingTopics`, so it writes that array empty too.
//! Both are values Kafka's reader accepts, and a record from another writer
//! that carries either still decodes: `DeletingTopics` round-trips through
//! [`ShareGroupStatePartitionMetadataValue::deleting`], and an
//! `InitializingTopics` entry is dropped, because a partition whose state is
//! only being initialized is not yet initialized and so is nothing the broker
//! would act on.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        flex::{
            get_compact_array_len, get_compact_string, get_i32_array, get_uuid,
            put_compact_array_len, put_compact_string, put_empty_tagged_fields, put_i32_array,
            put_uuid, skip_tagged_fields,
        },
        get_i16,
    },
    error::BrokerError,
};

/// The `TopicName` written for a topic id the metadata image cannot resolve,
/// byte for byte what Kafka's `GroupMetadataManager` writes in the same spot.
pub const UNKNOWN_TOPIC_NAME: &str = "<UNKNOWN>";

/// One `TopicPartitionsInfo` of the `InitializedTopics` array.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InitializedTopic {
    pub topic_id: uuid::Uuid,
    pub topic_name: String,
    pub partitions: Vec<i32>,
}

/// One `TopicInfo` of the `DeletingTopics` array.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeletingTopic {
    pub topic_id: uuid::Uuid,
    pub topic_name: String,
}

/// KIP-932 `ShareGroupStatePartitionMetadata`, key v15.
///
/// The record tracks which `(topic_id, partition)` share-states a group has
/// initialized. It also holds the topics whose share-state the broker is
/// deleting. The record lets the group coordinator skip the re-initialization
/// of partitions across restarts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShareGroupStatePartitionMetadataValue {
    pub initialized: Vec<InitializedTopic>,
    pub deleting: Vec<DeletingTopic>,
}

impl ShareGroupStatePartitionMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_compact_array_len(&mut buf, 0); // InitializingTopics
        put_compact_array_len(&mut buf, self.initialized.len());
        for topic in &self.initialized {
            put_uuid(&mut buf, *topic.topic_id.as_bytes());
            put_compact_string(&mut buf, &topic.topic_name);
            put_i32_array(&mut buf, &topic.partitions);
            put_empty_tagged_fields(&mut buf);
        }
        put_compact_array_len(&mut buf, self.deleting.len());
        for topic in &self.deleting {
            put_uuid(&mut buf, *topic.topic_id.as_bytes());
            put_compact_string(&mut buf, &topic.topic_name);
            put_empty_tagged_fields(&mut buf);
        }
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }

    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        // InitializingTopics: the broker never writes one, and a partition
        // whose state is still being initialized is not yet initialized, so a
        // record from elsewhere loses nothing the broker would act on.
        let initializing = get_compact_array_len(&mut buf)?;
        for _ in 0..initializing {
            let _topic_id = get_uuid(&mut buf)?;
            let _topic_name = get_compact_string(&mut buf)?;
            let _partitions = get_i32_array(&mut buf)?;
            skip_tagged_fields(&mut buf)?;
        }
        let n = get_compact_array_len(&mut buf)?;
        let mut initialized = Vec::with_capacity(n);
        for _ in 0..n {
            let topic_id = uuid::Uuid::from_bytes(get_uuid(&mut buf)?);
            let topic_name = get_compact_string(&mut buf)?;
            let partitions = get_i32_array(&mut buf)?;
            skip_tagged_fields(&mut buf)?;
            initialized.push(InitializedTopic {
                topic_id,
                topic_name,
                partitions,
            });
        }
        let dn = get_compact_array_len(&mut buf)?;
        let mut deleting = Vec::with_capacity(dn);
        for _ in 0..dn {
            let topic_id = uuid::Uuid::from_bytes(get_uuid(&mut buf)?);
            let topic_name = get_compact_string(&mut buf)?;
            skip_tagged_fields(&mut buf)?;
            deleting.push(DeletingTopic {
                topic_id,
                topic_name,
            });
        }
        skip_tagged_fields(&mut buf)?;
        Ok(Self {
            initialized,
            deleting,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::share::persistence::{
        KEY_SHARE_GROUP_STATE_PARTITION_METADATA, ShareGroupKey, encode_share_key, parse_share_key,
        test_support::peek_version,
    };

    fn initialized(id: u8, name: &str, partitions: Vec<i32>) -> InitializedTopic {
        InitializedTopic {
            topic_id: uuid::Uuid::from_bytes([id; 16]),
            topic_name: name.to_owned(),
            partitions,
        }
    }

    fn deleting(id: u8, name: &str) -> DeletingTopic {
        DeletingTopic {
            topic_id: uuid::Uuid::from_bytes([id; 16]),
            topic_name: name.to_owned(),
        }
    }

    #[test]
    fn state_partition_metadata_bytes_match_kafka_schema() {
        let v = ShareGroupStatePartitionMetadataValue {
            initialized: vec![initialized(1, "orders", vec![0])],
            deleting: vec![deleting(9, "carts")],
        };
        let mut want: Vec<u8> = vec![0x00, 0x00];
        want.push(0x01); // empty InitializingTopics
        want.push(0x02); // one InitializedTopics entry
        want.extend_from_slice(&[1u8; 16]);
        want.push(0x07); // TopicName "orders"
        want.extend_from_slice(b"orders");
        want.push(0x02); // one partition
        want.extend_from_slice(&0i32.to_be_bytes());
        want.push(0x00); // TopicPartitionsInfo tagged fields
        want.push(0x02); // one DeletingTopics entry
        want.extend_from_slice(&[9u8; 16]);
        want.push(0x06); // TopicName "carts"
        want.extend_from_slice(b"carts");
        want.push(0x00); // TopicInfo tagged fields
        want.push(0x00); // message tagged fields
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn state_partition_metadata_round_trip() {
        let key = ShareGroupKey::StatePartitionMetadata {
            group_id: "g1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert!(ver == KEY_SHARE_GROUP_STATE_PARTITION_METADATA);
        assert!(parse_share_key(ver, body).unwrap() == key);

        let cases = [
            ShareGroupStatePartitionMetadataValue::default(),
            ShareGroupStatePartitionMetadataValue {
                initialized: vec![
                    initialized(1, "orders", vec![0, 1, 2]),
                    initialized(2, "shipments", vec![]),
                ],
                deleting: vec![deleting(9, "carts")],
            },
            // A name the writer could not resolve stays exactly what Kafka
            // would have written, and a name with multi-byte characters keeps
            // its byte length through the compact-string codec.
            ShareGroupStatePartitionMetadataValue {
                initialized: vec![initialized(3, UNKNOWN_TOPIC_NAME, vec![7])],
                deleting: vec![deleting(4, "temperaturmålinger")],
            },
        ];
        for v in cases {
            assert!(ShareGroupStatePartitionMetadataValue::decode(&v.encode()).unwrap() == v);
        }
    }

    #[test]
    fn state_partition_metadata_skips_a_kafka_initializing_list() {
        // A record another writer left with an InitializingTopics entry still
        // decodes, and the entry does not shift the fields after it.
        let mut bytes: Vec<u8> = vec![0x00, 0x00];
        bytes.push(0x02); // one InitializingTopics entry
        bytes.extend_from_slice(&[7u8; 16]);
        bytes.extend_from_slice(b"\x02t"); // TopicName "t"
        bytes.push(0x02);
        bytes.extend_from_slice(&4i32.to_be_bytes());
        bytes.push(0x00);
        bytes.push(0x01); // empty InitializedTopics
        bytes.push(0x01); // empty DeletingTopics
        bytes.push(0x00); // message tagged fields
        assert!(
            ShareGroupStatePartitionMetadataValue::decode(&bytes).unwrap()
                == ShareGroupStatePartitionMetadataValue::default()
        );
    }

    #[test]
    fn state_partition_metadata_rejects_a_missing_tagged_trailer() {
        let full = ShareGroupStatePartitionMetadataValue::default().encode();
        assert!(ShareGroupStatePartitionMetadataValue::decode(&full[..full.len() - 1]).is_err());
    }
}
