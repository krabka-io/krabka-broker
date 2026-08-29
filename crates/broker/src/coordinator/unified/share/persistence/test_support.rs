//! The one test helper the share-group record round-trip tests share.
//!
//! Each key test encodes a key and then checks both the leading version and the
//! parse of the body, so it needs to split the two apart the way the broker's
//! `__consumer_offsets` dispatch does.

/// Split a freshly encoded key into its leading version and its body.
///
/// This mirrors how the broker dispatches `__consumer_offsets` keys on the
/// leading `i16`.
pub(super) fn peek_version(buf: &[u8]) -> (i16, &[u8]) {
    let mut r = buf;
    let v = bytes::Buf::get_i16(&mut r);
    (v, r)
}
