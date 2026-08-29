//! Decoding the topic list out of a group member's consumer-protocol
//! subscription blob.
//!
//! KIP-496 refuses to delete an offset while a live member of a classic
//! consumer-protocol group still subscribes to the topic. Answering that
//! question means parsing the opaque `protocol_metadata` the member sent with
//! its `JoinGroup`, which is pure wire decoding rather than response
//! construction, so it sits in its own module.

use bytes::Buf as _;
use krabka_protocol::{
    Decode, owned::consumer_protocol_subscription::ConsumerProtocolSubscription,
};

/// Decodes the `topics` list from a member's `protocol_metadata` blob.
///
/// The blob carries a leading `i16` version, then the schema body. That
/// version belongs to the "consumer" protocol's version negotiation, and is
/// separate from the per-field version gates of the
/// `ConsumerProtocolSubscription` schema.
///
/// The function returns an empty list on any decode error. This is a
/// best-effort decode, because a malformed subscription would otherwise let
/// the broker silently delete stale offsets.
pub(super) fn decode_subscribed_topics(metadata: &[u8]) -> Vec<String> {
    if metadata.len() < 2 {
        return Vec::new();
    }
    let mut cur = metadata;
    let version = cur.get_i16();
    if !(0..=3).contains(&version) {
        return Vec::new();
    }
    ConsumerProtocolSubscription::decode(&mut cur, version)
        .map(|s| s.topics)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::BufMut as _;
    use krabka_protocol::Encode as _;

    use super::*;

    fn encode_subscription(topics: &[&str]) -> Vec<u8> {
        let sub = ConsumerProtocolSubscription {
            topics: topics.iter().map(|s| (*s).to_string()).collect(),
            ..Default::default()
        };
        let mut out = bytes::BytesMut::new();
        out.put_i16(0); // protocol version negotiation prefix
        sub.encode(&mut out, 0).unwrap();
        out.to_vec()
    }

    #[test]
    fn decode_subscription_extracts_topic_names() {
        let bytes = encode_subscription(&["foo", "bar"]);
        let got = decode_subscribed_topics(&bytes);
        assert!(got == vec!["foo".to_string(), "bar".to_string()]);
    }

    #[test]
    fn decode_subscription_empty_input_is_empty() {
        assert!(decode_subscribed_topics(&[]).is_empty());
    }

    #[test]
    fn decode_subscription_short_input_is_empty() {
        assert!(decode_subscribed_topics(&[0u8]).is_empty());
    }

    #[test]
    fn decode_subscription_rejects_out_of_range_version() {
        // Version 99 is not a known ConsumerProtocolSubscription version.
        let bytes = vec![0u8, 99u8];
        assert!(decode_subscribed_topics(&bytes).is_empty());
    }

    #[test]
    fn decode_subscription_malformed_body_returns_empty() {
        // Valid version prefix, but truncated body → decode fails.
        let bytes = vec![0u8, 0u8];
        assert!(decode_subscribed_topics(&bytes).is_empty());
    }
}
