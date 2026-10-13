//! The derive preserves epoch wire bytes and passes codec errors through.

use assert2::assert;

// The consumer fixture supplies the same codec interfaces without broker dependencies.
mod bytes {
    pub(crate) type Bytes = Vec<u8>;
    pub(crate) struct BytesMut(pub(crate) Vec<u8>);

    impl BytesMut {
        pub(crate) fn new() -> Self { Self(Vec::new()) }

        pub(crate) fn freeze(self) -> Bytes { self.0 }
    }

    pub(crate) trait BufMut {
        fn put_i16(&mut self, value: i16);
        fn put_i32(&mut self, value: i32);
    }

    impl BufMut for BytesMut {
        fn put_i16(&mut self, value: i16) { self.0.extend(value.to_be_bytes()); }

        fn put_i32(&mut self, value: i32) { self.0.extend(value.to_be_bytes()); }
    }
}

mod error {
    pub(crate) type BrokerError = &'static str;
}

mod coordinator {
    pub(crate) mod unified {
        pub(crate) mod persistence {
            use crate::error::BrokerError;

            fn take<const N: usize>(buf: &mut &[u8]) -> Result<[u8; N], BrokerError> {
                let (value, rest) = buf.split_first_chunk::<N>().ok_or("truncated field")?;
                *buf = rest;
                Ok(*value)
            }

            pub(crate) fn get_i32(buf: &mut &[u8]) -> Result<i32, BrokerError> {
                Ok(i32::from_be_bytes(take(buf)?))
            }

            pub(crate) mod flex {
                use {
                    super::take,
                    crate::{bytes::BytesMut, error::BrokerError},
                };

                pub(crate) fn get_value_version(
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

                pub(crate) fn put_tag0_i64(buf: &mut BytesMut, value: i64) {
                    if value == 0 {
                        buf.0.push(0);
                    } else {
                        buf.0.extend([1, 0, 8]);
                        buf.0.extend(value.to_be_bytes());
                    }
                }

                pub(crate) fn get_tag0_i64(buf: &mut &[u8]) -> Result<i64, BrokerError> {
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

#[test]
fn keeps_exact_kafka_bytes_and_every_truncation_error() {
    for (epoch, timestamp, golden) in [
        (7, 0, &b"\x00\x00\x00\x00\x00\x07\x00"[..]),
        (
            7,
            42,
            &b"\x00\x00\x00\x00\x00\x07\x01\x00\x08\x00\x00\x00\x00\x00\x00\x00\x2a"[..],
        ),
        (
            -1,
            -1,
            &b"\x00\x00\xff\xff\xff\xff\x01\x00\x08\xff\xff\xff\xff\xff\xff\xff\xff"[..],
        ),
    ] {
        let value = Value {
            assignment_epoch: epoch,
            assignment_timestamp_ms: timestamp,
        };
        assert!(value.encode() == golden);
        assert!(Value::decode(golden) == Ok(value));
        for end in 0..golden.len() {
            assert!(Value::decode(&golden[..end]).is_err());
        }
    }
}

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
