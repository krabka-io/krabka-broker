//! The resolved-regular-expression record at key version 16.
//!
//! [`RegularExpressionValue`] is what a KIP-848 consumer group knows about one
//! `SubscribedTopicRegex` its members use: the topics the pattern resolved to,
//! the metadata version they were resolved at, and when. Kafka's
//! `GroupMetadataManager` writes it whenever it resolves a regex, and replays
//! it, so that a coordinator failover restores the topics of every regex
//! subscription without a heartbeat that carries the pattern.
//!
//! # Layout
//!
//! From `ConsumerGroupRegularExpressionValue.json` at Apache Kafka tag
//! `4.3.1`, which declares `"flexibleVersions": "0+"`. In field order:
//! `Topics` (`[]string`), `Version` (int64) and `Timestamp` (int64). The key,
//! `ConsumerGroupRegularExpressionKey`, is the group id and the regular
//! expression, both non-flexible strings; see
//! [`NextGenKey::RegularExpression`](super::NextGenKey::RegularExpression).

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    coordinator::unified::persistence::{
        flex::{get_string_array, put_empty_tagged_fields, put_string_array, skip_tagged_fields},
        get_i16, get_i64,
    },
    error::BrokerError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegularExpressionValue {
    /// The topics that the regular expression resolved to, which the resolving
    /// principal may `Describe`.
    pub topics: Vec<String>,
    /// The version of the metadata image the topics were resolved from.
    pub version: i64,
    /// The wall-clock time of the resolution, in milliseconds since the epoch.
    pub timestamp_ms: i64,
}

impl RegularExpressionValue {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_string_array(&mut buf, &self.topics);
        buf.put_i64(self.version);
        buf.put_i64(self.timestamp_ms);
        put_empty_tagged_fields(&mut buf);
        buf.freeze()
    }

    /// # Errors
    /// Returns an error when the record is truncated or corrupt.
    pub fn decode(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let _v = get_i16(&mut buf)?;
        let topics = get_string_array(&mut buf)?;
        let version = get_i64(&mut buf)?;
        let timestamp_ms = get_i64(&mut buf)?;
        skip_tagged_fields(&mut buf)?;
        Ok(Self {
            topics,
            version,
            timestamp_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn value() -> RegularExpressionValue {
        RegularExpressionValue {
            topics: vec!["a".into(), "bc".into()],
            version: 7,
            timestamp_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn bytes_match_kafka_schema() {
        let mut want: Vec<u8> = Vec::new();
        want.extend_from_slice(b"\x00\x00"); // value version 0
        want.extend_from_slice(b"\x03\x02a\x03bc"); // Topics, compact array of compact strings
        want.extend_from_slice(&7i64.to_be_bytes()); // Version
        want.extend_from_slice(&1_700_000_000_000i64.to_be_bytes()); // Timestamp
        want.push(0x00); // no tagged fields
        assert!(&value().encode()[..] == &want[..]);
    }

    #[test]
    fn roundtrip() {
        let v = value();
        assert!(RegularExpressionValue::decode(&v.encode()).unwrap() == v);
        let empty = RegularExpressionValue {
            topics: vec![],
            ..v
        };
        assert!(RegularExpressionValue::decode(&empty.encode()).unwrap() == empty);
    }

    #[test]
    fn a_truncated_record_is_refused() {
        let full = value().encode();
        assert!(RegularExpressionValue::decode(&full[..full.len() - 1]).is_err());
    }
}
