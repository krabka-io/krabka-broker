//! Codec for the Kafka `TransactionLogKey` record, version 0.
//!
//! The key carries the transactional id, which the companion value record does
//! not repeat. A `__transaction_state` record is therefore decoded key first,
//! and the id it yields is handed to the value decoder.

use bytes::BytesMut;
use krabka_protocol::{
    ProtocolError,
    primitives::{
        fixed::{get_i16, put_i16},
        string_bytes::{get_string_owned, put_string},
    },
};

use crate::error::BrokerError;

/// Encode the Kafka `TransactionLogKey`, version 0.
pub(crate) fn encode_key(transactional_id: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();
    put_i16(&mut buf, 0);
    put_string(&mut buf, transactional_id);
    buf.to_vec()
}

/// Decode a Kafka `TransactionLogKey` and return the transactional id.
pub(crate) fn decode_key(bytes: &[u8]) -> Result<String, BrokerError> {
    let mut buf = bytes;
    let version = get_i16(&mut buf)?;
    if version != 0 {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "unsupported TransactionLogKey version",
        )));
    }
    let transactional_id = get_string_owned(&mut buf)?;
    if !buf.is_empty() {
        return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "TransactionLogKey: trailing bytes after decode",
        )));
    }
    Ok(transactional_id)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn key_round_trip() {
        let encoded = encode_key("abc");
        assert!(decode_key(&encoded).unwrap() == "abc");
        // `00 00` version + int16 length (3) + bytes.
        assert!(encoded == &[0x00, 0x00, 0x00, 0x03, b'a', b'b', b'c']);
    }

    #[test]
    fn decode_key_rejects_unknown_version_and_truncation() {
        let key = encode_key("abc");
        // unknown version
        let mut bad = key.clone();
        bad[1] = 0x09;
        assert!(decode_key(&bad).is_err());
        // truncated
        assert!(decode_key(&key[..1]).is_err());
    }
}
