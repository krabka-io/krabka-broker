//! The streams member metadata record at key version 16.
//!
//! The value holds a member's static identity: its instance and rack ids, its
//! client id and host, its process id, its client tags, and the rebalance
//! timeout and topology epoch it joined with. [`StreamsEndpoint`] is the
//! optional host and port a member advertises for interactive queries.

use bytes::{BufMut, Bytes, BytesMut};

use super::codec::{get_i8, get_u32};
use crate::{
    coordinator::unified::persistence::{
        get_i16, get_i32, get_nullable_string, get_string, put_nullable_string, put_string,
    },
    error::BrokerError,
};

/// A member's advertised host and port endpoint, for interactive-query
/// routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsEndpoint {
    pub host: String,
    pub port: u32,
}

/// Key v16 value: a streams group member's static metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsGroupMemberMetadataValue {
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub process_id: String,
    pub user_endpoint: Option<StreamsEndpoint>,
    pub client_tags: Vec<(String, String)>,
    pub rebalance_timeout_ms: i32,
    pub topology_epoch: i32,
}

impl StreamsGroupMemberMetadataValue {
    #[must_use]
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_nullable_string(&mut buf, self.instance_id.as_deref());
        put_nullable_string(&mut buf, self.rack_id.as_deref());
        put_string(&mut buf, &self.client_id);
        put_string(&mut buf, &self.client_host);
        put_string(&mut buf, &self.process_id);
        // user_endpoint: a single i8 presence flag, then host + port if present.
        match &self.user_endpoint {
            Some(ep) => {
                buf.put_i8(1);
                put_string(&mut buf, &ep.host);
                buf.put_u32(ep.port);
            }
            None => buf.put_i8(0),
        }
        let n = i32::try_from(self.client_tags.len()).expect("fits");
        buf.put_i32(n);
        for (k, v) in &self.client_tags {
            put_string(&mut buf, k);
            put_string(&mut buf, v);
        }
        buf.put_i32(self.rebalance_timeout_ms);
        buf.put_i32(self.topology_epoch);
        buf.freeze()
    }
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let instance_id = get_nullable_string(&mut buf)?;
        let rack_id = get_nullable_string(&mut buf)?;
        let client_id = get_string(&mut buf)?;
        let client_host = get_string(&mut buf)?;
        let process_id = get_string(&mut buf)?;
        let user_endpoint = if get_i8(&mut buf)? == 0 {
            None
        } else {
            let host = get_string(&mut buf)?;
            let port = get_u32(&mut buf)?;
            Some(StreamsEndpoint { host, port })
        };
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut client_tags = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let k = get_string(&mut buf)?;
            let v = get_string(&mut buf)?;
            client_tags.push((k, v));
        }
        let rebalance_timeout_ms = get_i32(&mut buf)?;
        let topology_epoch = get_i32(&mut buf)?;
        Ok(Self {
            instance_id,
            rack_id,
            client_id,
            client_host,
            process_id,
            user_endpoint,
            client_tags,
            rebalance_timeout_ms,
            topology_epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::streams::persistence::{
        KEY_STREAMS_MEMBER_METADATA, StreamsGroupKey, encode_member_metadata_key,
        parse_streams_key, test_support::peek_version,
    };

    #[test]
    fn member_metadata_round_trip() {
        let kb = encode_member_metadata_key("g1", "m1");
        let (ver, body) = peek_version(&kb);
        assert!(ver == KEY_STREAMS_MEMBER_METADATA);
        assert!(
            parse_streams_key(ver, body).unwrap()
                == StreamsGroupKey::MemberMetadata {
                    group_id: "g1".into(),
                    member_id: "m1".into(),
                }
        );

        let v = StreamsGroupMemberMetadataValue {
            instance_id: Some("i1".into()),
            rack_id: Some("us-east-1a".into()),
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            process_id: "p-uuid".into(),
            user_endpoint: Some(StreamsEndpoint {
                host: "host-a".into(),
                port: 8080,
            }),
            client_tags: vec![("zone".into(), "a".into()), ("tier".into(), "hot".into())],
            rebalance_timeout_ms: 60_000,
            topology_epoch: 3,
        };
        assert!(StreamsGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_null_optionals_round_trip() {
        let v = StreamsGroupMemberMetadataValue {
            instance_id: None,
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            process_id: "p-uuid".into(),
            user_endpoint: None,
            client_tags: vec![],
            rebalance_timeout_ms: 45_000,
            topology_epoch: 0,
        };
        assert!(StreamsGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }
}
