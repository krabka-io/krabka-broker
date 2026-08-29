//! The KIP-848 member metadata record at key version 5.
//!
//! [`MemberMetadataValue`] holds what a next-gen consumer told the coordinator
//! about itself: its client identity, its subscription by name and by regex,
//! and the server assignor it asked for. An upgraded group also stores the
//! member's classic sub-state in [`ClassicMemberMetadata`] so that a downgrade
//! can restore it.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_protocol::ProtocolError;

use crate::{
    coordinator::unified::persistence::{
        get_bytes, get_i16, get_i32, get_nullable_string, get_string, put_bytes,
        put_nullable_string, put_string,
    },
    error::BrokerError,
};

/// Classic-protocol sub-state for a member hosted inside an upgraded consumer
/// group (KIP-848 migration). It mirrors Kafka's
/// `ConsumerGroupMemberMetadataValue.ClassicMemberMetadata`. It lets a
/// downgrade restore the classic member losslessly after a coordinator
/// failover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassicMemberMetadata {
    pub session_timeout_ms: i32,
    pub supported_protocols: Vec<(String, Bytes)>,
    pub last_synced_assignment: Bytes,
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
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_nullable_string(&mut buf, self.instance_id.as_deref());
        put_nullable_string(&mut buf, self.rack_id.as_deref());
        put_string(&mut buf, &self.client_id);
        put_string(&mut buf, &self.client_host);
        let n = i32::try_from(self.subscribed_topic_names.len()).expect("fits");
        buf.put_i32(n);
        for s in &self.subscribed_topic_names {
            put_string(&mut buf, s);
        }
        put_nullable_string(&mut buf, self.subscribed_topic_regex.as_deref());
        put_nullable_string(&mut buf, self.server_assignor.as_deref());
        buf.put_i32(self.rebalance_timeout_ms);
        match &self.classic {
            None => buf.put_i8(0),
            Some(c) => {
                buf.put_i8(1);
                buf.put_i32(c.session_timeout_ms);
                let pn = i32::try_from(c.supported_protocols.len()).expect("fits");
                buf.put_i32(pn);
                for (name, meta) in &c.supported_protocols {
                    put_string(&mut buf, name);
                    put_bytes(&mut buf, meta);
                }
                put_bytes(&mut buf, &c.last_synced_assignment);
            }
        }
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
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative");
        let mut subscribed_topic_names = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            subscribed_topic_names.push(get_string(&mut buf)?);
        }
        let subscribed_topic_regex = get_nullable_string(&mut buf)?;
        let server_assignor = get_nullable_string(&mut buf)?;
        let rebalance_timeout_ms = get_i32(&mut buf)?;
        let classic = {
            if buf.remaining() < 1 {
                return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                    "missing classic-presence byte",
                )));
            }
            if buf.get_i8() == 0 {
                None
            } else {
                let session_timeout_ms = get_i32(&mut buf)?;
                let pn = get_i32(&mut buf)?;
                let pcap = usize::try_from(pn.max(0)).expect("non-negative");
                let mut supported_protocols = Vec::with_capacity(pcap);
                for _ in 0..pn.max(0) {
                    let name = get_string(&mut buf)?;
                    let meta = get_bytes(&mut buf)?;
                    supported_protocols.push((name, meta));
                }
                let last_synced_assignment = get_bytes(&mut buf)?;
                Some(ClassicMemberMetadata {
                    session_timeout_ms,
                    supported_protocols,
                    last_synced_assignment,
                })
            }
        };
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

    #[test]
    fn member_metadata_roundtrip() {
        let v = MemberMetadataValue {
            instance_id: Some("i1".into()),
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["a".into(), "b".into()],
            subscribed_topic_regex: None,
            server_assignor: Some("uniform".into()),
            rebalance_timeout_ms: 60_000,
            classic: None,
        };
        assert!(MemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_with_regex_roundtrip() {
        // KIP-848 v1+: `subscribed_topic_regex` survives encode/decode
        // so bootstrap replay can hydrate the regex subscription
        // without waiting for a heartbeat.
        let v = MemberMetadataValue {
            instance_id: Some("i1".into()),
            rack_id: Some("us-east-1a".into()),
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["audit".into()],
            subscribed_topic_regex: Some("^orders-.*".into()),
            server_assignor: Some("uniform".into()),
            rebalance_timeout_ms: 60_000,
            classic: None,
        };
        assert!(MemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_round_trips_classic_block() {
        use bytes::Bytes;
        let v = MemberMetadataValue {
            instance_id: Some("inst-a".into()),
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["t1".into(), "t2".into()],
            subscribed_topic_regex: None,
            server_assignor: Some("uniform".into()),
            rebalance_timeout_ms: 60_000,
            classic: Some(ClassicMemberMetadata {
                session_timeout_ms: 30_000,
                supported_protocols: vec![("range".into(), Bytes::from_static(b"meta"))],
                last_synced_assignment: Bytes::from_static(b"asn"),
            }),
        };
        let decoded = MemberMetadataValue::decode(&v.encode()).unwrap();
        assert!(decoded == v);

        let mut native = v.clone();
        native.classic = None;
        assert!(MemberMetadataValue::decode(&native.encode()).unwrap() == native);
    }
}
