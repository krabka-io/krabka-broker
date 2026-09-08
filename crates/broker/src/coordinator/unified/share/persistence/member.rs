//! The KIP-932 share-group member metadata record at key version 10.
//!
//! [`ShareGroupMemberMetadataValue`] holds what a share consumer told the
//! coordinator about itself: its optional rack, its client identity, and the
//! topics it subscribed to by name. Share groups have no regex subscription, no
//! server assignor, and no rebalance timeout, so those consumer-only fields are
//! absent.
//!
//! # Layout
//!
//! From `ShareGroupMemberMetadataValue.json` at Apache Kafka tag `4.3.1`, which
//! declares `"flexibleVersions": "0+"`: `RackId` (nullable string), `ClientId`
//! (string), `ClientHost` (string) and `SubscribedTopicNames` (`[]string`).
//! Every string is compact, the array is compact, and the message ends with its
//! tagged-field count.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        flex::{
            get_compact_nullable_string, get_compact_string, get_string_array,
            put_compact_nullable_string, put_compact_string, put_empty_tagged_fields,
            put_string_array, skip_tagged_fields,
        },
        get_i16,
    },
    error::BrokerError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMemberMetadataValue {
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
}

impl ShareGroupMemberMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_compact_nullable_string(&mut buf, self.rack_id.as_deref());
        put_compact_string(&mut buf, &self.client_id);
        put_compact_string(&mut buf, &self.client_host);
        put_string_array(&mut buf, &self.subscribed_topic_names);
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let rack_id = get_compact_nullable_string(&mut buf)?;
        let client_id = get_compact_string(&mut buf)?;
        let client_host = get_compact_string(&mut buf)?;
        let subscribed_topic_names = get_string_array(&mut buf)?;
        skip_tagged_fields(&mut buf)?;
        Ok(Self {
            rack_id,
            client_id,
            client_host,
            subscribed_topic_names,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::share::persistence::{
        KEY_SHARE_MEMBER_METADATA, ShareGroupKey, encode_share_key, parse_share_key,
        test_support::peek_version,
    };

    #[test]
    fn member_metadata_round_trip() {
        let key = ShareGroupKey::MemberMetadata {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        let b = encode_share_key(&key);
        let (ver, body) = peek_version(&b);
        assert!(ver == KEY_SHARE_MEMBER_METADATA);
        assert!(parse_share_key(ver, body).unwrap() == key);

        let v = ShareGroupMemberMetadataValue {
            rack_id: Some("us-east-1a".into()),
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["a".into(), "b".into()],
        };
        assert!(ShareGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_bytes_match_kafka_schema() {
        let v = ShareGroupMemberMetadataValue {
            rack_id: Some("r".into()),
            client_id: "c".into(),
            client_host: "h".into(),
            subscribed_topic_names: vec!["a".into()],
        };
        assert!(&v.encode()[..] == b"\x00\x00\x02r\x02c\x02h\x02\x02a\x00");
    }

    #[test]
    fn member_metadata_rejects_a_missing_tagged_trailer() {
        let v = ShareGroupMemberMetadataValue {
            rack_id: None,
            client_id: "c".into(),
            client_host: "h".into(),
            subscribed_topic_names: vec![],
        };
        let full = v.encode();
        assert!(ShareGroupMemberMetadataValue::decode(&full[..full.len() - 1]).is_err());
    }

    #[test]
    fn member_metadata_null_rack_round_trip() {
        let v = ShareGroupMemberMetadataValue {
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec![],
        };
        assert!(ShareGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }
}
