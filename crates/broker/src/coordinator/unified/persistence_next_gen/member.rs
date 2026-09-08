//! The KIP-848 member metadata record at key version 5.
//!
//! [`MemberMetadataValue`] holds what a next-gen consumer told the coordinator
//! about itself: its client identity, its subscription by name and by regex,
//! and the server assignor it asked for. An upgraded group also stores the
//! member's classic sub-state in [`ClassicMemberMetadata`] so that a downgrade
//! can restore it.
//!
//! # Layout
//!
//! From `ConsumerGroupMemberMetadataValue.json` at Apache Kafka tag `4.3.1`,
//! which declares `"flexibleVersions": "0+"`. In field order: `InstanceId`
//! (nullable string), `RackId` (nullable string), `ClientId` (string),
//! `ClientHost` (string), `SubscribedTopicNames` (`[]string`),
//! `SubscribedTopicRegex` (nullable string), `RebalanceTimeoutMs` (int32),
//! `ServerAssignor` (nullable string), and then the tagged
//! `ClassicMemberMetadata` (tag 0, nullable, default null) whose own fields are
//! `SessionTimeoutMs` (int32) and `SupportedProtocols`
//! (`[]ClassicProtocol{Name string, Metadata bytes}`).
//!
//! Every string is compact, every array is compact, and the message and the
//! nested structs each end with a tagged-field count.
//!
//! ## The one krabka-private field
//!
//! Kafka's schema has no home for `last_synced_assignment`, the blob the
//! hosted-classic path compares a freshly translated assignment against. It
//! travels as a tagged field at [`TAG_LAST_SYNCED_ASSIGNMENT`], a tag number
//! far outside the range Kafka assigns. Kafka's generated reader collects a tag
//! it does not know into the message's unknown tagged fields and reads on, so
//! the record still decodes; a record for a member with no classic sub-state
//! carries no such field and is byte-identical to what Kafka writes.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::ProtocolError;

use crate::{
    coordinator::unified::persistence::{
        flex::{
            get_compact_array_len, get_compact_bytes, get_compact_nullable_string,
            get_compact_string, get_string_array, put_compact_array_len, put_compact_bytes,
            put_compact_nullable_string, put_compact_string, put_empty_tagged_fields,
            put_string_array, put_tagged_fields, read_tagged, skip_tagged_fields,
        },
        get_i16, get_i32,
    },
    error::BrokerError,
};

/// Kafka's tag for the `ClassicMemberMetadata` field of
/// `ConsumerGroupMemberMetadataValue`.
const TAG_CLASSIC_MEMBER_METADATA: u32 = 0;

/// krabka's own tag for the classic member's last synced assignment, which
/// Kafka's schema does not carry. It sits far above Kafka's assigned tags so
/// that a later Kafka field cannot collide with it.
const TAG_LAST_SYNCED_ASSIGNMENT: u32 = 1000;

/// Classic-protocol sub-state for a member hosted inside an upgraded consumer
/// group (KIP-848 migration). It mirrors Kafka's
/// `ConsumerGroupMemberMetadataValue.ClassicMemberMetadata`. It lets a
/// downgrade restore the classic member losslessly after a coordinator
/// failover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicMemberMetadata {
    pub session_timeout_ms: i32,
    pub supported_protocols: Vec<(String, Bytes)>,
    /// Not part of Kafka's schema. See the module docs: it rides in the
    /// krabka-private tagged field.
    pub last_synced_assignment: Bytes,
}

impl ClassicMemberMetadata {
    /// Encodes the payload of Kafka's `ClassicMemberMetadata` tagged field:
    /// the struct's own fields plus its own tagged-field trailer.
    fn encode_tag_payload(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i32(self.session_timeout_ms);
        put_compact_array_len(&mut buf, self.supported_protocols.len());
        for (name, meta) in &self.supported_protocols {
            put_compact_string(&mut buf, name);
            put_compact_bytes(&mut buf, meta);
            put_empty_tagged_fields(&mut buf);
        }
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }

    fn decode_tag_payload(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let session_timeout_ms = get_i32(buf)?;
        let n = get_compact_array_len(buf)?;
        let mut supported_protocols = Vec::with_capacity(n);
        for _ in 0..n {
            let name = get_compact_string(buf)?;
            let meta = get_compact_bytes(buf)?;
            skip_tagged_fields(buf)?;
            supported_protocols.push((name, meta));
        }
        skip_tagged_fields(buf)?;
        Ok(Self {
            session_timeout_ms,
            supported_protocols,
            last_synced_assignment: Bytes::new(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberMetadataValue {
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    /// KIP-848 v1+ `subscribed_topic_regex`. `None` = exact-name
    /// subscription only. The reconciler unions regex matches with
    /// `subscribed_topic_names` against the current metadata image.
    pub subscribed_topic_regex: Option<String>,
    pub server_assignor: Option<String>,
    pub rebalance_timeout_ms: i32,
    /// `Some` if and only if this is a hosted classic member. `None` for a
    /// native consumer-protocol member.
    pub classic: Option<ClassicMemberMetadata>,
}

impl MemberMetadataValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_compact_nullable_string(&mut buf, self.instance_id.as_deref());
        put_compact_nullable_string(&mut buf, self.rack_id.as_deref());
        put_compact_string(&mut buf, &self.client_id);
        put_compact_string(&mut buf, &self.client_host);
        put_string_array(&mut buf, &self.subscribed_topic_names);
        put_compact_nullable_string(&mut buf, self.subscribed_topic_regex.as_deref());
        buf.put_i32(self.rebalance_timeout_ms);
        put_compact_nullable_string(&mut buf, self.server_assignor.as_deref());
        let mut tags = Vec::new();
        if let Some(c) = &self.classic {
            tags.push((TAG_CLASSIC_MEMBER_METADATA, c.encode_tag_payload()));
            if !c.last_synced_assignment.is_empty() {
                tags.push((TAG_LAST_SYNCED_ASSIGNMENT, c.last_synced_assignment.clone()));
            }
        }
        put_tagged_fields(&mut buf, tags);
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
        let subscribed_topic_names = get_string_array(&mut buf)?;
        let subscribed_topic_regex = get_compact_nullable_string(&mut buf)?;
        let rebalance_timeout_ms = get_i32(&mut buf)?;
        let server_assignor = get_compact_nullable_string(&mut buf)?;

        let mut classic: Option<ClassicMemberMetadata> = None;
        let mut last_synced_assignment = Bytes::new();
        let mut tag_error: Option<BrokerError> = None;
        read_tagged(&mut buf, |tag, payload| match tag {
            TAG_CLASSIC_MEMBER_METADATA => match ClassicMemberMetadata::decode_tag_payload(payload)
            {
                Ok(c) => {
                    classic = Some(c);
                    Ok(true)
                }
                Err(e) => {
                    tag_error = Some(e);
                    Err(ProtocolError::InvalidValue("bad ClassicMemberMetadata"))
                }
            },
            TAG_LAST_SYNCED_ASSIGNMENT => {
                last_synced_assignment = Bytes::copy_from_slice(&payload[..]);
                *payload = &payload[payload.len()..];
                Ok(true)
            }
            _ => Ok(false),
        })?;
        if let Some(e) = tag_error {
            return Err(e);
        }
        if let Some(c) = classic.as_mut() {
            c.last_synced_assignment = last_synced_assignment;
        }
        Ok(Self {
            instance_id,
            rack_id,
            client_id,
            client_host,
            subscribed_topic_names,
            subscribed_topic_regex,
            server_assignor,
            rebalance_timeout_ms,
            classic,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn native() -> MemberMetadataValue {
        MemberMetadataValue {
            instance_id: Some("i1".into()),
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["a".into(), "b".into()],
            subscribed_topic_regex: None,
            server_assignor: Some("uniform".into()),
            rebalance_timeout_ms: 60_000,
            classic: None,
        }
    }

    #[test]
    fn member_metadata_bytes_match_kafka_schema() {
        let mut want: Vec<u8> = Vec::new();
        want.extend_from_slice(b"\x00\x00"); // value version 0
        want.extend_from_slice(b"\x03i1"); // InstanceId, compact string
        want.push(0x00); // RackId, compact null
        want.extend_from_slice(b"\x03c1"); // ClientId
        want.extend_from_slice(b"\x0b/127.0.0.1"); // ClientHost
        want.extend_from_slice(b"\x03\x02a\x02b"); // SubscribedTopicNames
        want.push(0x00); // SubscribedTopicRegex, null
        want.extend_from_slice(&60_000i32.to_be_bytes()); // RebalanceTimeoutMs
        want.extend_from_slice(b"\x08uniform"); // ServerAssignor
        want.push(0x00); // no tagged fields
        assert!(&native().encode()[..] == &want[..]);
    }

    #[test]
    fn member_metadata_roundtrip() {
        let v = native();
        assert!(MemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_with_regex_roundtrip() {
        // KIP-848 v1+: `subscribed_topic_regex` survives encode/decode
        // so bootstrap replay can hydrate the regex subscription
        // without waiting for a heartbeat.
        let v = MemberMetadataValue {
            rack_id: Some("us-east-1a".into()),
            subscribed_topic_names: vec!["audit".into()],
            subscribed_topic_regex: Some("^orders-.*".into()),
            ..native()
        };
        assert!(MemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn classic_sub_state_bytes_match_kafka_schema() {
        let v = MemberMetadataValue {
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "h".into(),
            subscribed_topic_names: vec![],
            subscribed_topic_regex: None,
            server_assignor: None,
            rebalance_timeout_ms: 1,
            classic: Some(ClassicMemberMetadata {
                session_timeout_ms: 2,
                supported_protocols: vec![("range".into(), Bytes::from_static(b"m"))],
                last_synced_assignment: Bytes::new(),
            }),
        };
        let mut want: Vec<u8> = Vec::new();
        want.extend_from_slice(b"\x00\x00"); // version
        want.extend_from_slice(b"\x00\x00"); // InstanceId, RackId null
        want.extend_from_slice(b"\x02c\x02h"); // ClientId, ClientHost
        want.push(0x01); // empty SubscribedTopicNames
        want.push(0x00); // SubscribedTopicRegex null
        want.extend_from_slice(&1i32.to_be_bytes()); // RebalanceTimeoutMs
        want.push(0x00); // ServerAssignor null
        // One tagged field: tag 0, then the payload length, then the struct.
        let mut payload: Vec<u8> = Vec::new();
        payload.extend_from_slice(&2i32.to_be_bytes()); // SessionTimeoutMs
        payload.push(0x02); // one supported protocol
        payload.extend_from_slice(b"\x06range"); // Name
        payload.extend_from_slice(b"\x02m"); // Metadata, compact bytes
        payload.push(0x00); // ClassicProtocol tagged fields
        payload.push(0x00); // ClassicMemberMetadata tagged fields
        want.push(0x01); // one tagged field
        want.push(0x00); // tag 0
        want.push(u8::try_from(payload.len()).unwrap());
        want.extend_from_slice(&payload);
        assert!(&v.encode()[..] == &want[..]);
    }

    #[test]
    fn member_metadata_round_trips_classic_block() {
        let v = MemberMetadataValue {
            instance_id: Some("inst-a".into()),
            subscribed_topic_names: vec!["t1".into(), "t2".into()],
            classic: Some(ClassicMemberMetadata {
                session_timeout_ms: 30_000,
                supported_protocols: vec![("range".into(), Bytes::from_static(b"meta"))],
                last_synced_assignment: Bytes::from_static(b"asn"),
            }),
            ..native()
        };
        let decoded = MemberMetadataValue::decode(&v.encode()).unwrap();
        assert!(decoded == v);

        let mut plain = v.clone();
        plain.classic = None;
        assert!(MemberMetadataValue::decode(&plain.encode()).unwrap() == plain);
    }

    #[test]
    fn member_metadata_rejects_a_missing_tagged_trailer() {
        let full = native().encode();
        assert!(MemberMetadataValue::decode(&full[..full.len() - 1]).is_err());
    }
}
