//! The three `Message` fixtures that the codec's round-trip tests build on.
//!
//! `round_trip` encodes a message and decodes it back, so the fixtures and the
//! helper sit beside the two codec modules rather than inside either one.

use {
    super::{Magic, Message},
    bytes::Bytes,
};

pub(super) fn fixture_v0() -> Message {
    Message {
        magic: Magic::V0,
        attributes: 0,
        timestamp: None,
        key: Some(Bytes::from_static(b"k")),
        value: Some(Bytes::from_static(b"v")),
    }
}

pub(super) fn fixture_v1() -> Message {
    Message {
        magic: Magic::V1,
        attributes: 0,
        timestamp: Some(1_700_000_000),
        key: Some(Bytes::from_static(b"key")),
        value: Some(Bytes::from_static(b"value")),
    }
}

pub(super) fn fixture_v1_null() -> Message {
    Message {
        magic: Magic::V1,
        attributes: 0,
        timestamp: Some(42),
        key: None,
        value: None,
    }
}

/// The wire bytes of an encoded message and the message decoded back from them.
pub(super) struct RoundTrip {
    pub(super) buffer: bytes::BytesMut,
    pub(super) decoded: Message,
}

pub(super) fn round_trip(message: &Message) -> RoundTrip {
    let mut buffer = bytes::BytesMut::new();
    message.encode_into(&mut buffer).unwrap();
    let decoded = Message::decode_from(&mut &buffer[..], message.encoded_len()).unwrap();
    RoundTrip { buffer, decoded }
}
