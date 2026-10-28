//! The derive preserves epoch wire bytes and passes codec errors through.
#![cfg(test)]

use assert2::assert;

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

#[cfg(test)]
#[rstest_bdd_macros::then(
    "flexible epoch value: keeps exact kafka bytes and every truncation error for {example}"
)]
fn bdd_verify_keeps_exact_kafka_bytes_and_every_truncation_error(example: String) {
    type Example = (i32, i64, &'static [u8]);
    let (epoch, timestamp, golden): Example = match example.as_str() {
        "zero timestamp" => (7, 0, &b"\x00\x00\x00\x00\x00\x07\x00"[..]),
        "tagged timestamp" => (
            7,
            42,
            &b"\x00\x00\x00\x00\x00\x07\x01\x00\x08\x00\x00\x00\x00\x00\x00\x00\x2a"[..],
        ),
        "negative values" => (
            -1,
            -1,
            &b"\x00\x00\xff\xff\xff\xff\x01\x00\x08\xff\xff\xff\xff\xff\xff\xff\xff"[..],
        ),
        _ => panic!("unknown BDD example: {example}"),
    };

    let value = Value {
        assignment_epoch: epoch,
        assignment_timestamp_ms: timestamp,
    };
    assert2::assert!(value.encode() == golden);
    assert2::assert!(Value::decode(golden) == Ok(value));
    for end in 0..golden.len() {
        assert2::assert!(Value::decode(&golden[..end]).is_err());
    }
}

#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/flexible_epoch_value.feature",
    name = "keeps exact kafka bytes and every truncation error"
)]
fn keeps_exact_kafka_bytes_and_every_truncation_error(example: String) { let _ = example; }

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
