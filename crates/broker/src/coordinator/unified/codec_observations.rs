//! Typed observations shared by Kafka persisted-record compatibility scenarios.
//! Native record construction, codec calls and assertions stay in their domains.

use std::fmt::Debug;

/// A prepared persisted record and the bytes produced by its real encoder.
pub struct EncodedRecord<T> {
    pub value: Option<T>,
    pub encoded: Option<bytes::Bytes>,
}

impl<T> Default for EncodedRecord<T> {
    fn default() -> Self {
        Self {
            value: None,
            encoded: None,
        }
    }
}

/// Independent Kafka wire bytes, native input, and both codec observations.
pub struct KafkaRecordRoundTrip<T> {
    pub input: Option<T>,
    pub golden: Vec<u8>,
    pub encoded: Option<bytes::Bytes>,
    pub decoded: Option<T>,
}

impl<T> Default for KafkaRecordRoundTrip<T> {
    fn default() -> Self {
        Self {
            input: None,
            golden: Vec::new(),
            encoded: None,
            decoded: None,
        }
    }
}

/// A prepared native record, independent Kafka bytes and its encoder result.
pub struct KafkaEncodedRecord<T> {
    pub value: Option<T>,
    pub golden: Vec<u8>,
    pub encoded: Option<bytes::Bytes>,
}
impl<T> Default for KafkaEncodedRecord<T> {
    fn default() -> Self {
        Self {
            value: None,
            golden: Vec::new(),
            encoded: None,
        }
    }
}

/// Both native codec results against an independently specified Kafka record.
pub struct KafkaDecodedRecord<T, E> {
    pub value: Option<T>,
    pub golden: Vec<u8>,
    pub encoded: Option<bytes::Bytes>,
    pub decoded: Option<Result<T, E>>,
}
impl<T, E> Default for KafkaDecodedRecord<T, E> {
    fn default() -> Self {
        Self {
            value: None,
            golden: Vec::new(),
            encoded: None,
            decoded: None,
        }
    }
}
impl<T: PartialEq + Debug, E: Debug> KafkaDecodedRecord<T, E> {
    /// Compare the full wire layout and all recovered native fields.
    pub fn assert_compatible(&self) {
        assert_kafka_bytes(self.encoded.as_ref(), &self.golden);
        assert2::assert!(
            self.decoded
                .as_ref()
                .expect("decode performed")
                .as_ref()
                .expect("valid Kafka record")
                == self.value.as_ref().expect("original record")
        );
    }
}

/// Group and member identifiers of a prepared Kafka coordinator key, plus its native encoder observations.
#[derive(Default)]
pub struct KafkaMetadataKey {
    pub kind: String,
    pub group: String,
    pub member: Option<String>,
    pub encoded: Option<bytes::Bytes>,
    pub version: Option<i16>,
}
impl KafkaMetadataKey {
    /// Prepare the identifiers explicitly supplied by a group-key compatibility scenario.
    pub fn set_key_fields(&mut self, kind: String, group: String, member: String) {
        self.kind = kind;
        self.group = group;
        self.member = (member != "absent").then_some(member);
    }

    /// Compare every encoded byte against the independently supplied Kafka layout.
    pub fn assert_kafka_bytes(&self, expected: &[u8]) {
        assert2::assert!(
            self.encoded
                .as_ref()
                .expect("coordinator key encoded")
                .as_ref()
                == expected
        );
    }
}

impl<T: PartialEq + Debug> KafkaRecordRoundTrip<T> {
    /// Assert the complete Kafka byte layout and the independently decoded fields.
    pub fn assert_compatible(&self) {
        assert_kafka_bytes(self.encoded.as_ref(), &self.golden);
        assert2::assert!(self.decoded == self.input);
    }
}

/// Compare a completed native encoder result against independent Kafka bytes.
fn assert_kafka_bytes(actual: Option<&bytes::Bytes>, expected: &[u8]) {
    assert2::assert!(&actual.expect("record encoded")[..] == expected);
}
