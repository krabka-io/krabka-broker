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

use {
    crate::{
        coordinator::unified::persistence::{
            flex::{get_string_array, put_string_array},
            get_i64,
        },
        error::BrokerError,
    },
    bytes::{BufMut, BytesMut},
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

#[krabka_macros::flexible_value(record = "ConsumerGroupRegularExpressionValue")]
impl RegularExpressionValue {
    fn encode_fields(&self, buf: &mut BytesMut) -> Result<(), BrokerError> {
        put_string_array(buf, &self.topics)?;
        buf.put_i64(self.version);
        buf.put_i64(self.timestamp_ms);
        Ok(())
    }

    fn decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let topics = get_string_array(buf)?;
        let version = get_i64(buf)?;
        let timestamp_ms = get_i64(buf)?;
        Ok(Self {
            topics,
            version,
            timestamp_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::coordinator::unified::test_support::wire_bytes, assert2::assert};

    fn value() -> RegularExpressionValue {
        RegularExpressionValue {
            topics: vec!["a".into(), "bc".into()],
            version: 7,
            timestamp_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn roundtrip() {
        let v = value();
        assert!(RegularExpressionValue::decode(&v.encode().unwrap()).unwrap() == v);
        let empty = RegularExpressionValue {
            topics: vec![],
            ..v
        };
        assert!(RegularExpressionValue::decode(&empty.encode().unwrap()).unwrap() == empty);
    }

    #[test]
    fn a_truncated_record_is_refused() {
        let full = value().encode().unwrap();
        assert!(RegularExpressionValue::decode(&full[..full.len() - 1]).is_err());
    }
    mod bdd_regex_value {
        use {
            super::*,
            crate::coordinator::unified::codec_observations::EncodedRecord,
            assert2::assert,
            rstest_bdd_macros::{given, scenarios, then, when},
        };
        type EncodedValue = EncodedRecord<RegularExpressionValue>;
        #[rstest::fixture]
        fn stored_value() -> EncodedValue {
            // Start this scenario with fresh inputs and observations.
            EncodedValue::default()
        }
        #[given(
            "a resolved expression contains topics a and bc at version 7 and timestamp 1700000000000"
        )]
        fn prepared_value(stored_value: &mut EncodedValue) {
            let v = value();

            stored_value.value = Some(v);
        }
        #[when(
            "the coordinator persists the resolved consumer subscription in the compacted group log"
        )]
        fn encode_group_record(stored_value: &mut EncodedValue) {
            stored_value.encoded = Some(
                stored_value
                    .value
                    .as_ref()
                    .expect("group record prepared")
                    .encode()
                    .unwrap(),
            );
        }
        #[then(
            "the persisted resolved consumer subscription matches Kafka schema bytes including compact lengths and tagged trailers"
        )]
        fn kafka_record_layout(stored_value: &EncodedValue) {
            let want = wire_bytes(&[
                "0000",             // value version 0
                "030261036263",     // Topics, compact array of compact strings
                "0000000000000007", // Version
                "0000018bcfe56800", // Timestamp
                "00",               // no tagged fields
            ]);

            assert!(&stored_value.encoded.as_ref().expect("record encoded")[..] == &want[..]);
        }
        scenarios!("tests/features/migrated/src/coordinator/unified/persistence_next_gen/regex.feature",tags="@regex_value",fixtures=[stored_value: EncodedValue]);
    }
}
