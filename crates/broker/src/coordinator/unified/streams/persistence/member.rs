//! The streams member metadata record at key version 19.
//!
//! The value holds a member's static identity: its instance and rack ids, its
//! client id and host, its process id, its client tags, and the rebalance
//! timeout and topology epoch it joined with. [`StreamsEndpoint`] is the
//! optional host and port a member advertises for interactive queries.
//!
//! # Layout
//!
//! From `StreamsGroupMemberMetadataValue.json` at Apache Kafka tag `4.3.1`,
//! which declares `"flexibleVersions": "0+"`. In field order: `InstanceId`
//! (nullable string), `RackId` (nullable string), `ClientId` (string),
//! `ClientHost` (string), `RebalanceTimeoutMs` (int32), `TopologyEpoch`
//! (int32), `ProcessId` (string), `UserEndpoint` (nullable `Endpoint{Host
//! string, Port uint16}`) and `ClientTags` (`[]KeyValue{Key string, Value
//! string}`).
//!
//! `UserEndpoint` is a nullable struct that is not tagged, so it is on the wire
//! as one `int8`: -1 for null, or 1 followed by the struct and the struct's own
//! tagged-field count. The port is a `uint16`, two bytes, not four.

use bytes::{BufMut, Bytes, BytesMut};

use super::codec::{decode_key_value_list, encode_key_value_list};
use crate::{
    coordinator::unified::persistence::{
        flex::{
            get_compact_nullable_string, get_compact_string, get_i8, get_u16,
            put_compact_nullable_string, put_compact_string, put_empty_tagged_fields,
            skip_tagged_fields,
        },
        get_i16, get_i32,
    },
    error::BrokerError,
};

/// A member's advertised host and port endpoint, for interactive-query
/// routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamsEndpoint {
    pub host: String,
    /// Kafka's `uint16`, so the whole unsigned range fits and nothing wider
    /// does.
    pub port: u16,
}

/// Key v19 value: a streams group member's static metadata.
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
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_compact_nullable_string(&mut buf, self.instance_id.as_deref());
        put_compact_nullable_string(&mut buf, self.rack_id.as_deref());
        put_compact_string(&mut buf, &self.client_id);
        put_compact_string(&mut buf, &self.client_host);
        buf.put_i32(self.rebalance_timeout_ms);
        buf.put_i32(self.topology_epoch);
        put_compact_string(&mut buf, &self.process_id);
        match &self.user_endpoint {
            Some(ep) => {
                buf.put_i8(1);
                put_compact_string(&mut buf, &ep.host);
                buf.put_u16(ep.port);
                put_empty_tagged_fields(&mut buf);
            }
            None => buf.put_i8(-1),
        }
        encode_key_value_list(&mut buf, &self.client_tags);
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }

    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let instance_id = get_compact_nullable_string(&mut buf)?;
        let rack_id = get_compact_nullable_string(&mut buf)?;
        let client_id = get_compact_string(&mut buf)?;
        let client_host = get_compact_string(&mut buf)?;
        let rebalance_timeout_ms = get_i32(&mut buf)?;
        let topology_epoch = get_i32(&mut buf)?;
        let process_id = get_compact_string(&mut buf)?;
        let user_endpoint = if get_i8(&mut buf)? < 0 {
            None
        } else {
            let host = get_compact_string(&mut buf)?;
            let port = get_u16(&mut buf)?;
            skip_tagged_fields(&mut buf)?;
            Some(StreamsEndpoint { host, port })
        };
        let client_tags = decode_key_value_list(&mut buf)?;
        skip_tagged_fields(&mut buf)?;
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

    fn sample() -> StreamsGroupMemberMetadataValue {
        StreamsGroupMemberMetadataValue {
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
        }
    }

    #[test]
    fn member_metadata_bytes_match_kafka_schema() {
        let v = StreamsGroupMemberMetadataValue {
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "h".into(),
            process_id: "p".into(),
            user_endpoint: Some(StreamsEndpoint {
                host: "e".into(),
                port: 8080,
            }),
            client_tags: vec![("k".into(), "v".into())],
            rebalance_timeout_ms: 1,
            topology_epoch: 2,
        };
        let mut want: Vec<u8> = vec![0x00, 0x00];
        want.extend_from_slice(b"\x00\x00"); // InstanceId, RackId null
        want.extend_from_slice(b"\x02c\x02h"); // ClientId, ClientHost
        want.extend_from_slice(&1i32.to_be_bytes()); // RebalanceTimeoutMs
        want.extend_from_slice(&2i32.to_be_bytes()); // TopologyEpoch
        want.extend_from_slice(b"\x02p"); // ProcessId
        want.push(0x01); // UserEndpoint present
        want.extend_from_slice(b"\x02e"); // Host
        want.extend_from_slice(&8080u16.to_be_bytes()); // Port, uint16
        want.push(0x00); // Endpoint tagged fields
        want.push(0x02); // one ClientTags entry
        want.extend_from_slice(b"\x02k\x02v");
        want.push(0x00); // KeyValue tagged fields
        want.push(0x00); // message tagged fields
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn null_user_endpoint_is_one_negative_byte() {
        let v = StreamsGroupMemberMetadataValue {
            user_endpoint: None,
            client_tags: vec![],
            ..sample()
        };
        let bytes = v.encode();
        // ... ProcessId "p-uuid", then -1, then the empty ClientTags and the
        // message's tagged-field count.
        assert!(&bytes[bytes.len() - 3..] == b"\xff\x01\x00");
    }

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

        let v = sample();
        assert!(StreamsGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_null_optionals_round_trip() {
        let v = StreamsGroupMemberMetadataValue {
            instance_id: None,
            rack_id: None,
            user_endpoint: None,
            client_tags: vec![],
            rebalance_timeout_ms: 45_000,
            topology_epoch: 0,
            ..sample()
        };
        assert!(StreamsGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn a_port_above_the_i16_range_round_trips() {
        let v = StreamsGroupMemberMetadataValue {
            user_endpoint: Some(StreamsEndpoint {
                host: "h".into(),
                port: 60_000,
            }),
            ..sample()
        };
        assert!(StreamsGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_rejects_a_missing_tagged_trailer() {
        let full = sample().encode();
        assert!(StreamsGroupMemberMetadataValue::decode(&full[..full.len() - 1]).is_err());
    }
}
