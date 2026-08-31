//! Response framing and the KIP-219 throttle. It prepends the response header
//! to a handler's body, charges the request quota, and patches the leading
//! `ThrottleTimeMs` of the responses whose schema carries that field first.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::api_key::ApiKey;
use krabka_units::{Time, convert::TimeExt};

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId},
    network::codec,
};

pub(super) async fn maybe_apply_request_quota(
    broker: &Broker,
    mut response_bytes: Bytes,
    parsed: &crate::network::request::ParsedRequest<'_>,
    auth: &crate::network::auth::ConnectionAuth,
    started: std::time::Instant,
) -> Bytes {
    let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let self_accounts = matches!(
        ApiKey::from_i16(parsed.api_key),
        Some(ApiKey::Produce | ApiKey::Fetch)
    );
    if !self_accounts && let Some(principal) = auth.principal() {
        let image = broker.controller.current_image();
        let delay = crate::quota::consume_request_quota(
            &image,
            &broker.quota_buckets,
            &principal.name,
            parsed.client_id.unwrap_or(""),
            elapsed_micros,
            broker.config.quota_throttle_max,
        );
        if delay > <Time as TimeExt>::ZERO {
            if throttle_is_leading_field(parsed.api_key, parsed.api_version) {
                let delay_ms = crate::quota::throttle_time_ms(delay);
                response_bytes = patch_leading_throttle(
                    response_bytes,
                    parsed.api_key,
                    parsed.body_flexible,
                    delay_ms,
                );
            }
            tokio::time::sleep(delay.to_std()).await;
        }
    }
    response_bytes
}

/// Prepends the response header, the `corr_id` and an optional tagged-fields
/// byte, in front of the handler's body bytes.
// PERF — measured; decision: KEEP.
//
// This copies the whole body to prepend a 4-5 byte header, and the sink copies
// it a second time: the sink is a `Framed<S, LengthDelimitedCodec>`, and
// `LengthDelimitedCodec` only implements `Encoder<Bytes>` (a single concrete
// impl), so `framed.send` requires a contiguous `Bytes` and will not accept a
// `bytes::Buf::chain(header, body)`. Worse, that `Encoder::encode` itself does
// `dst.extend_from_slice(&data[..])` into the codec's write buffer. Removing
// both copies means swapping the codec for a custom `Encoder<impl Buf>` that
// vectored-writes header and body, which reaches `codec.rs`, the roundtrip
// test, and every signature that names `Framed<S, LengthDelimitedCodec>`.
//
// `benches/perf_deferrals.rs` prices that chained-`Buf` prototype against this
// path. Both sides write to a sink that keeps no bytes, so the socket write is
// out of the comparison on both and the reported saving is an upper bound:
//
//     body       copy    chained     saved
//     1 KiB     67 ns      32 ns     35 ns   (52%)
//     64 KiB  1557 ns      32 ns    1.5 us   (98%)
//     1 MiB     31 us      32 ns     31 us   (99.9%)
//
// Keep. The saving is proportional to the body, and the one API whose bodies
// are unbounded does not come through here: Fetch is written by
// `network::fetch_writer`, which already skips both copies (its module doc has
// the same motivation). What is left on this path is produce, metadata and
// admin responses on the order of a kilobyte, where the whole framing step
// costs under 100 ns against a request that takes tens of microseconds to
// serve. Note the old price estimate of "~50 `framed.send` call sites" is
// stale — the dispatch tree has since funnelled its responses through three —
// but a custom codec is still the wrong trade for ~35 ns a response. Revisit
// only if a non-Fetch response becomes both large and hot; a Metadata response
// for a very large cluster is the plausible candidate, and clients refresh it
// on the order of minutes.
pub(super) fn encode_response(
    api_key: ApiKeyCode,
    correlation_id: CorrelationId,
    body_flexible: bool,
    body: &[u8],
    max_frame_bytes: usize,
) -> Result<Bytes, BrokerError> {
    let header_len = crate::network::response_header_len(api_key, body_flexible);
    codec::validate_frame_length(header_len + body.len(), max_frame_bytes)?;
    let mut buf = BytesMut::with_capacity(header_len + body.len());
    buf.put_i32(correlation_id);
    if crate::network::response_header_v1(api_key, body_flexible) {
        buf.put_u8(0); // empty tagged fields
    }
    buf.put_slice(body);
    Ok(buf.freeze())
}

/// KIP-219 (throttle-then-respond): returns `true` when the response of
/// `api_key` carries `ThrottleTimeMs` as its FIRST body field at `version`.
/// The dispatch loop then reports the request-quota throttle by patching that
/// leading int32 in place.
///
/// The boundaries are verified against the 4.x response schemas. APIs that are
/// absent from this table keep the pre-KIP-219 behavior: the channel mute
/// still enforces the throttle, but the response does not echo it. Produce (0)
/// and Fetch (1) account for themselves and never reach this path.
/// `OffsetDelete` (47) is deliberately excluded, because its leading field is
/// `ErrorCode` and a patch would corrupt it.
fn throttle_is_leading_field(api_key: ApiKeyCode, version: ApiVersion) -> bool {
    // The version bounds are the schema versions at which each API moved
    // `ThrottleTimeMs` to the front of its response (verified against the
    // 4.x response schemas); they are deliberately kept as literals here
    // and pinned by the schema-boundary tests.
    match ApiKey::from_i16(api_key) {
        Some(ApiKey::ListOffsets | ApiKey::JoinGroup | ApiKey::OffsetForLeaderEpoch) => {
            version >= 2
        }
        Some(ApiKey::Metadata | ApiKey::OffsetCommit | ApiKey::OffsetFetch) => version >= 3,
        Some(
            ApiKey::FindCoordinator
            | ApiKey::Heartbeat
            | ApiKey::LeaveGroup
            | ApiKey::SyncGroup
            | ApiKey::DescribeGroups
            | ApiKey::ListGroups,
        ) => version >= 1,
        // InitProducerId / DescribeCluster / ConsumerGroupHeartbeat (all 0+)
        Some(ApiKey::InitProducerId | ApiKey::DescribeCluster | ApiKey::ConsumerGroupHeartbeat) => {
            true
        }
        _ => false,
    }
}

/// Patches the leading `ThrottleTimeMs` int32 of an already-encoded response
/// in place, and raises it to `max(existing, delay_ms)`.
///
/// The body starts right after the response header, whose length mirrors
/// `encode_response`: 5 bytes when the body is flexible and the api is not
/// `ApiVersions`, and 4 bytes otherwise. Callers must first confirm
/// `throttle_is_leading_field`.
fn patch_leading_throttle(
    resp: Bytes,
    api_key: ApiKeyCode,
    body_flexible: bool,
    delay_ms: i32,
) -> Bytes {
    let off = crate::network::response_header_len(api_key, body_flexible);
    if resp.len() < off + 4 {
        return resp;
    }
    let mut buf = BytesMut::from(resp.as_ref());
    let existing = i32::from_be_bytes([buf[off], buf[off + 1], buf[off + 2], buf[off + 3]]);
    let patched = existing.max(delay_ms);
    buf[off..off + 4].copy_from_slice(&patched.to_be_bytes());
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::network::dispatch::{API_VERSIONS_KEY, test_support::DEFAULT_MAX_FRAME_BYTES};

    #[test]
    fn encode_response_apiversions_uses_v0_header() {
        // ApiVersions response is always header v0 (no tagged byte) even
        // for flexible body versions.
        let body = [0u8, 0u8]; // error_code=0
        let out = encode_response(API_VERSIONS_KEY, 7, true, &body, DEFAULT_MAX_FRAME_BYTES)
            .expect("encode response");
        // 4 byte corr_id + body, no tagged byte.
        assert!(out.len() == 4 + body.len());
    }

    #[test]
    fn throttle_leading_field_table_matches_schemas() {
        // Present-and-leading version boundaries (verified vs 4.x schemas).
        // OffsetDelete (47) leads with ErrorCode — must never be patched.
        // Produce/Fetch self-account; ApiVersions is not in the table.
        let cases = [
            (11, 1, false), // JoinGroup v1: no throttle
            (11, 2, true),  // JoinGroup v2+: leading
            (3, 2, false),  // Metadata v2: no throttle
            (3, 3, true),   // Metadata v3+
            (12, 1, true),  // Heartbeat v1+
            (68, 0, true),  // ConsumerGroupHeartbeat v0+
            (47, 0, false), // OffsetDelete
            (0, 9, false),  // Produce
            (1, 13, false), // Fetch
            (18, 3, false), // ApiVersions
        ];
        for (api_key, version, want) in cases {
            assert!(
                throttle_is_leading_field(api_key, version) == want,
                "api_key {api_key} version {version}"
            );
        }
    }

    #[test]
    fn patch_leading_throttle_sets_field_flexible_and_nonflexible() {
        let read =
            |b: &[u8], off: usize| i32::from_be_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]]);

        // Flexible response header (ConsumerGroupHeartbeat, flexible v0+):
        // header = 5 bytes (corr_id + tagged byte); throttle int32 at offset 5.
        let mut body = BytesMut::new();
        body.put_i32(0); // ThrottleTimeMs = 0
        let resp =
            encode_response(68, 7, true, &body, DEFAULT_MAX_FRAME_BYTES).expect("encode response");
        let patched = patch_leading_throttle(resp, 68, true, 250);
        assert!(read(&patched, 5) == 250);
        assert!(read(&patched, 0) == 7); // corr_id preserved

        // Non-flexible response header (Metadata v3): header = 4 bytes.
        let mut body = BytesMut::new();
        body.put_i32(10); // existing throttle 10 < 250
        let resp =
            encode_response(3, 9, false, &body, DEFAULT_MAX_FRAME_BYTES).expect("encode response");
        let patched = patch_leading_throttle(resp, 3, false, 250);
        assert!(read(&patched, 4) == 250);
        assert!(read(&patched, 0) == 9);
    }

    #[test]
    fn patch_leading_throttle_keeps_existing_when_larger() {
        // max(existing, delay): an already-larger throttle is not lowered.
        let mut body = BytesMut::new();
        body.put_i32(500);
        let resp =
            encode_response(3, 1, false, &body, DEFAULT_MAX_FRAME_BYTES).expect("encode response");
        let patched = patch_leading_throttle(resp, 3, false, 100);
        let v = i32::from_be_bytes([patched[4], patched[5], patched[6], patched[7]]);
        assert!(v == 500);
    }

    #[test]
    fn encode_response_other_flexible_inserts_tagged_byte() {
        let body = [0u8, 0u8];
        let out =
            encode_response(3, 7, true, &body, DEFAULT_MAX_FRAME_BYTES).expect("encode response");
        assert!(out.len() == 5 + body.len());
        assert!(out[4] == 0); // tagged byte
    }

    #[test]
    fn encode_response_enforces_max_frame_at_runtime() {
        let body = [0u8; 4];
        assert!(encode_response(3, 7, false, &body, 8).is_ok());
        assert!(encode_response(3, 7, false, &body, 7).is_err());
    }
}
