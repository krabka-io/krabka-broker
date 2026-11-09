//! Encoding one v0/v1 message: the byte count it occupies inside its frame,
//! and the write that prefixes the CRC-covered body with its CRC.
//!
//! The size calculation sits beside the write because both walk the same
//! optional fields, the v1-only timestamp and the two nullable byte strings,
//! and a frame whose `message_size` disagrees with the bytes written is
//! unparseable.

use {
    super::{Magic, Message},
    crate::error::LegacyRecordsError,
    bytes::{BufMut, Bytes},
};

/// `len` as the `int32` length field the wire gives it.
///
/// # Errors
/// Returns `Malformed` when `len` exceeds `i32::MAX`, since a saturated field
/// would disagree with the bytes after it.
pub fn wire_len(len: usize) -> Result<i32, LegacyRecordsError> {
    i32::try_from(len).map_err(|_| {
        LegacyRecordsError::Malformed(format!(
            "a length of {len} bytes does not fit the i32 length field"
        ))
    })
}

impl Message {
    /// Number of bytes that the message occupies inside the per-entry frame.
    ///
    /// The count starts at the CRC field. It goes up to and includes the
    /// value.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        // crc(4) + magic(1) + attrs(1) [+ ts(8) if v1] + key + value
        let mut n = 4 + 1 + 1;
        if matches!(self.magic, Magic::V1) {
            n += 8;
        }
        n += nullable_bytes_len(self.key.as_ref());
        n += nullable_bytes_len(self.value.as_ref());
        n
    }

    /// Encode the message into `buf`, including the CRC field.
    ///
    /// This method extends `buf`. It writes nothing before the CRC.
    ///
    /// # Errors
    /// Returns `Malformed`, and writes nothing, when the key or the value is
    /// longer than its `int32` length field can say.
    pub fn encode_into<B: BufMut>(&self, buf: &mut B) -> Result<(), LegacyRecordsError> {
        let key_len = nullable_len_field(self.key.as_ref())?;
        let value_len = nullable_len_field(self.value.as_ref())?;
        // Build the CRC-covered payload first, then prefix it with the CRC.
        // `encoded_len` counts the 4-byte CRC this body excludes; reserving it
        // anyway costs four bytes and keeps arithmetic out of a capacity hint,
        // where a wrong answer is invisible and so untestable.
        let mut body = Vec::with_capacity(self.encoded_len());
        body.push(self.magic.as_i8().cast_unsigned());
        body.push(self.attributes.cast_unsigned());
        if matches!(self.magic, Magic::V1) {
            let ts = self.timestamp.unwrap_or(-1);
            body.extend_from_slice(&ts.to_be_bytes());
        }
        put_nullable_bytes(&mut body, key_len, self.key.as_ref());
        put_nullable_bytes(&mut body, value_len, self.value.as_ref());

        let crc = crc32fast::hash(&body);
        buf.put_u32(crc);
        buf.put_slice(&body);
        Ok(())
    }
}

fn nullable_bytes_len(b: Option<&Bytes>) -> usize { 4 + b.map_or(0, Bytes::len) }

/// The length field of a nullable byte string: `-1` for null.
fn nullable_len_field(b: Option<&Bytes>) -> Result<i32, LegacyRecordsError> {
    b.map_or(Ok(-1), |data| wire_len(data.len()))
}

fn put_nullable_bytes(buf: &mut Vec<u8>, len: i32, b: Option<&Bytes>) {
    buf.extend_from_slice(&len.to_be_bytes());
    if let Some(data) = b {
        buf.extend_from_slice(data);
    }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::message::test_support::{
            RoundTrip, fixture_v0, fixture_v1, fixture_v1_null, round_trip,
        },
    };

    #[yare::parameterized(
        v0 = { fixture_v0() },
        v1 = { fixture_v1() },
    )]
    fn message_roundtrips(message: Message) {
        let RoundTrip { buffer, decoded } = round_trip(&message);
        assert2::assert!(buffer.len() == message.encoded_len());
        assert2::assert!(decoded == message);
    }

    #[yare::parameterized(
        zero = { 0, Some(0) },
        largest = { usize::try_from(i32::MAX).unwrap(), Some(i32::MAX) },
        one_past = { usize::try_from(i32::MAX).unwrap() + 1, None },
    )]
    fn wire_len_refuses_a_length_past_i32(len: usize, expected: Option<i32>) {
        assert2::assert!(wire_len(len).ok() == expected);
    }

    /// A key longer than its `int32` length field is refused, and nothing is
    /// written, rather than written with a saturated length.
    #[test]
    fn encode_into_refuses_a_key_its_length_field_cannot_say() {
        let message = Message {
            key: Some(Bytes::from(vec![
                0u8;
                usize::try_from(i32::MAX).unwrap() + 1
            ])),
            ..fixture_v0()
        };
        let mut buffer = bytes::BytesMut::new();
        assert2::assert!(matches!(
            message.encode_into(&mut buffer),
            Err(LegacyRecordsError::Malformed(_))
        ));
        assert2::assert!(buffer.is_empty());
    }

    #[test]
    fn v1_null_key_and_value() {
        let m = fixture_v1_null();
        let RoundTrip { decoded, .. } = round_trip(&m);
        assert2::assert!(decoded == m);
    }

    #[test]
    fn v1_missing_timestamp_encodes_minus_one() {
        let m = Message {
            magic: Magic::V1,
            attributes: 0,
            timestamp: None,
            key: None,
            value: None,
        };
        let RoundTrip { decoded, .. } = round_trip(&m);
        assert2::assert!(
            decoded
                == Message {
                    timestamp: Some(-1),
                    ..m
                }
        );
    }
}
