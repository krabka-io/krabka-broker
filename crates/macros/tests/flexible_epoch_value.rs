//! The derive preserves epoch wire bytes and passes codec errors through.
#![cfg(test)]

use {assert2::assert, std::str};

#[path = "support/codec.rs"]
mod error;

mod coordinator {
    pub mod unified {
        pub mod persistence {
            use crate::error::{BrokerError, take};

            pub fn get_i32(buf: &mut &[u8]) -> Result<i32, BrokerError> {
                Ok(i32::from_be_bytes(take(buf)?))
            }

            pub mod flex {
                use {
                    crate::error::{BrokerError, take},
                    bytes::{BufMut, BytesMut},
                };

                pub fn get_value_version(
                    buf: &mut &[u8],
                    highest: i16,
                    record: &'static str,
                ) -> Result<i16, BrokerError> {
                    let version = i16::from_be_bytes(take(buf)?);
                    if (0..=highest).contains(&version) {
                        Ok(version)
                    } else {
                        Err(record)
                    }
                }

                pub fn put_tag0_i64(buf: &mut BytesMut, value: i64) {
                    if value == 0 {
                        buf.put_u8(0);
                    } else {
                        buf.extend_from_slice(&[1, 0, 8]);
                        buf.put_i64(value);
                    }
                }

                pub fn get_tag0_i64(buf: &mut &[u8]) -> Result<i64, BrokerError> {
                    // shortcut: fixture tags and sizes fit one byte; use protocol helpers for wider values.
                    let [count] = take(buf)?;
                    let mut value = 0;
                    for _ in 0..count {
                        let [tag, size] = take(buf)?;
                        let (payload, rest) = buf
                            .split_at_checked(usize::from(size))
                            .ok_or("truncated tag")?;
                        *buf = rest;
                        if tag == 0 {
                            value =
                                i64::from_be_bytes(payload.try_into().map_err(|_| "bad tag zero")?);
                        }
                    }
                    Ok(value)
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::FlexibleEpochValue)]
#[epoch_value(record = "ConsumerGroupTargetAssignmentMetadataValue")]
struct Value {
    assignment_epoch: i32,
    assignment_timestamp_ms: i64,
}

struct EpochCodecScenario {
    value: Value,
    golden: Vec<u8>,
    encoded: Vec<u8>,
    decoded: Option<Result<Value, error::BrokerError>>,
    prefixes: Vec<Result<Value, error::BrokerError>>,
}

#[rstest::fixture]
fn epoch_codec() -> EpochCodecScenario {
    // Start this scenario with fresh inputs and observations.

    EpochCodecScenario {
        value: Value {
            assignment_epoch: 0,
            assignment_timestamp_ms: 0,
        },
        golden: Vec::new(),
        encoded: Vec::new(),
        decoded: None,
        prefixes: Vec::new(),
    }
}

#[rstest_bdd_macros::given("assignment epoch {epoch} at timestamp {timestamp}")]
fn epoch_value(epoch_codec: &mut EpochCodecScenario, epoch: i32, timestamp: i64) {
    epoch_codec.value = Value {
        assignment_epoch: epoch,
        assignment_timestamp_ms: timestamp,
    };
}

#[rstest_bdd_macros::given("Kafka epoch value bytes {golden}")]
fn epoch_bytes(epoch_codec: &mut EpochCodecScenario, golden: String) {
    let digits: String = golden.split_whitespace().collect();
    let (pairs, remainder) = digits.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty(), "complete hexadecimal bytes");
    epoch_codec.golden = pairs
        .iter()
        .map(|pair| {
            u8::from_str_radix(str::from_utf8(pair).expect("ASCII hex"), 16)
                .expect("hexadecimal wire fixture")
        })
        .collect();
}

#[rstest_bdd_macros::when(
    "the epoch value and every shorter byte prefix are passed through the codec"
)]
fn code_epoch_value(epoch_codec: &mut EpochCodecScenario) {
    epoch_codec.encoded = epoch_codec.value.encode().to_vec();
    epoch_codec.decoded = Some(Value::decode(&epoch_codec.golden));
    epoch_codec.prefixes = (0..epoch_codec.golden.len())
        .map(|end| Value::decode(&epoch_codec.golden[..end]))
        .collect();
}

#[rstest_bdd_macros::then("the Kafka bytes round trip and every truncated prefix is rejected")]
fn epoch_codec_results(epoch_codec: &EpochCodecScenario) {
    assert!(epoch_codec.encoded == epoch_codec.golden);
    assert!(epoch_codec.decoded == Some(Ok(epoch_codec.value)));
    for prefix in &epoch_codec.prefixes {
        assert!(prefix.is_err());
    }
}

rstest_bdd_macros::scenarios!("tests/features/epoch_codec", fixtures = [epoch_codec: EpochCodecScenario]);

#[test]
fn retains_record_specific_version_errors_and_codec_errors() {
    assert!(
        Value::decode(b"\x00\x01")
            == Err("unknown ConsumerGroupTargetAssignmentMetadataValue version")
    );
    assert!(
        Value::decode(b"\xff\xff")
            == Err("unknown ConsumerGroupTargetAssignmentMetadataValue version")
    );
    assert!(Value::decode(b"\x00\x00\x00\x00\x00\x07\x01\x00\x01\x00") == Err("bad tag zero"));
    assert!(
        Value::decode(b"\x00\x00\x00\x00\x00\x07\x01\x01\x01\x7f")
            == Ok(Value {
                assignment_epoch: 7,
                assignment_timestamp_ms: 0,
            })
    );
}
