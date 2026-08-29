//! The KIP-932 share-group member metadata record at key version 10.
//!
//! [`ShareGroupMemberMetadataValue`] holds what a share consumer told the
//! coordinator about itself: its optional rack, its client identity, and the
//! topics it subscribed to by name. Share groups have no regex subscription, no
//! server assignor, and no rebalance timeout, so those consumer-only fields are
//! absent.

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        get_i16, get_i32, get_nullable_string, get_string, put_nullable_string, put_string,
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
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_nullable_string(&mut buf, self.rack_id.as_deref());
        put_string(&mut buf, &self.client_id);
        put_string(&mut buf, &self.client_host);
        let n = i32::try_from(self.subscribed_topic_names.len()).expect("fits");
        buf.put_i32(n);
        for s in &self.subscribed_topic_names {
            put_string(&mut buf, s);
        }
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let rack_id = get_nullable_string(&mut buf)?;
        let client_id = get_string(&mut buf)?;
        let client_host = get_string(&mut buf)?;
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut subscribed_topic_names = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            subscribed_topic_names.push(get_string(&mut buf)?);
        }
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
