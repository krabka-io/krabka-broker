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
//! The broker tracks initialization by topic id, and its share-state lifecycle
//! has no separate "initializing" phase to persist, so it writes an empty
//! `InitializingTopics` and an empty `TopicName` on each entry it does write.
//! Both are values Kafka's reader accepts; the decoder drops them again.

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

/// The `TopicName` the broker writes. Its share-group state is keyed by topic
/// id, and the name is not part of that state.
const UNKNOWN_TOPIC_NAME: &str = "";

/// KIP-932 `ShareGroupStatePartitionMetadata`, key v15.
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
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_compact_array_len(&mut buf, 0); // InitializingTopics
        put_compact_array_len(&mut buf, self.initialized.len());
        for (topic_id, partitions) in &self.initialized {
            put_uuid(&mut buf, *topic_id.as_bytes());
            put_compact_string(&mut buf, UNKNOWN_TOPIC_NAME);
            put_i32_array(&mut buf, partitions);
            put_empty_tagged_fields(&mut buf);
        }
        put_compact_array_len(&mut buf, self.deleting.len());
        for topic_id in &self.deleting {
            put_uuid(&mut buf, *topic_id.as_bytes());
            put_compact_string(&mut buf, UNKNOWN_TOPIC_NAME);
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
            let _topic_name = get_compact_string(&mut buf)?;
            let partitions = get_i32_array(&mut buf)?;
            skip_tagged_fields(&mut buf)?;
            initialized.push((topic_id, partitions));
        }
        let dn = get_compact_array_len(&mut buf)?;
        let mut deleting = Vec::with_capacity(dn);
        for _ in 0..dn {
            deleting.push(uuid::Uuid::from_bytes(get_uuid(&mut buf)?));
            let _topic_name = get_compact_string(&mut buf)?;
            skip_tagged_fields(&mut buf)?;
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

    #[test]
    fn state_partition_metadata_bytes_match_kafka_schema() {
        let v = ShareGroupStatePartitionMetadataValue {
            initialized: vec![(uuid::Uuid::from_bytes([1; 16]), vec![0])],
            deleting: vec![uuid::Uuid::from_bytes([9; 16])],
        };
        let mut want: Vec<u8> = vec![0x00, 0x00];
        want.push(0x01); // empty InitializingTopics
        want.push(0x02); // one InitializedTopics entry
        want.extend_from_slice(&[1u8; 16]);
        want.push(0x01); // empty TopicName
        want.push(0x02); // one partition
        want.extend_from_slice(&0i32.to_be_bytes());
        want.push(0x00); // TopicPartitionsInfo tagged fields
        want.push(0x02); // one DeletingTopics entry
        want.extend_from_slice(&[9u8; 16]);
        want.push(0x01); // empty TopicName
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
