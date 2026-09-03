//! Response framing and the KIP-219 throttle. It prepends the response header
//! to a handler's body, charges the request quota, and patches the leading
//! `ThrottleTimeMs` of the responses whose schema carries that field first.
//!
//! Charging a quota never sleeps here. It returns the throttle window
//! alongside the bytes, and the connection loop enforces it by muting the
//! connection *after* the response is written, which is what KIP-219 asks for.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::api_key::ApiKey;
use krabka_units::{Time, convert::TimeExt};

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId},
    network::codec,
};

/// A framed response together with the KIP-219 window the connection must
/// stay muted for once those bytes are on the wire.
#[derive(Debug)]
pub(super) struct ThrottledResponse {
    pub(super) bytes: Bytes,
    pub(super) throttle: Time,
}

impl ThrottledResponse {
    /// A response that trips no quota, so the connection is not muted.
    pub(super) fn unthrottled(bytes: Bytes) -> Self {
        Self {
            bytes,
            throttle: <Time as TimeExt>::ZERO,
        }
    }
}

/// The schema version and header flexibility of the response that was
/// actually encoded.
///
/// It is not always the request's. `send_unsupported_version` replies at the
/// nearest supported version, which for a request below an API's minimum is
/// higher than the one the client asked for and can be flexible where the
/// request header was not. `patch_leading_throttle` derives the body offset
/// from the flexibility, and `throttle_is_leading_field` from the version, so
/// both must read the response's values: patching a flexible v0 body from the
/// request's non-flexible header offset overwrites the tagged-fields byte and
/// three quarters of `ThrottleTimeMs`.
#[derive(Clone, Copy)]
pub(super) struct ResponseShape {
    pub(super) version: ApiVersion,
    pub(super) body_flexible: bool,
}

impl ResponseShape {
    /// The ordinary case: the handler answered at the version the client
    /// asked for, with the header flexibility the request header used.
    pub(super) fn mirroring_request(parsed: &crate::network::request::ParsedRequest<'_>) -> Self {
        Self {
            version: parsed.api_version,
            body_flexible: parsed.body_flexible,
        }
    }
}

/// Charges the KIP-124 request quota for a finished request and returns the
/// response with the throttle window it earned.
///
/// The function does not wait. It patches the response's leading
/// `ThrottleTimeMs` where the schema has one, so the client learns how long to
/// back off, and returns the window so the caller can mute the connection
/// after the write.
pub(super) fn apply_request_quota(
    broker: &Broker,
    mut response_bytes: Bytes,
    parsed: &crate::network::request::ParsedRequest<'_>,
    shape: ResponseShape,
    auth: &crate::network::auth::ConnectionAuth,
    started: std::time::Instant,
) -> ThrottledResponse {
    let elapsed_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let self_accounts = matches!(
        ApiKey::from_i16(parsed.api_key),
        Some(ApiKey::Produce | ApiKey::Fetch)
    );
    let mut throttle = <Time as TimeExt>::ZERO;
    if !self_accounts {
        // KIP-124 keys the request quota on the principal, so a connection
        // that never authenticated is charged nothing. The zero still reaches
        // the throttle histogram below, which is what keeps that family's
        // `_count` equal to the number of requests this path accounted for.
        let charged = match auth.principal() {
            None => crate::quota::QuotaDelay::zero(),
            Some(principal) => {
                let image = broker.controller.current_image();
                crate::quota::consume_request_quota(
                    &image,
                    &broker.quota_buckets,
                    &principal.name,
                    parsed.client_id.unwrap_or(""),
                    elapsed_micros,
                    broker.config.quota_throttle_max,
                )
            }
        };
        // The request quota is the only one an api that does not account for
        // itself is charged, so it is the only entry, and the delay it asks
        // for is the window this request is muted for.
        let delay = broker.metrics.record_applied_throttle(
            parsed.api_key,
            &[(crate::metrics::QuotaType::Request, charged).into()],
        );
        if delay > <Time as TimeExt>::ZERO {
            if throttle_is_leading_field(parsed.api_key, shape.version) {
                let delay_ms = crate::quota::throttle_time_ms(delay);
                response_bytes = patch_leading_throttle(
                    response_bytes,
                    parsed.api_key,
                    shape.body_flexible,
                    delay_ms,
                );
            }
            throttle = delay;
        }
    }
    ThrottledResponse {
        bytes: response_bytes,
        throttle,
    }
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
//     1 KiB     57 ns      32 ns     25 ns   (43%)
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
// but a custom codec is still the wrong trade for ~25 ns a response. Revisit
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
/// The table is an exhaustive audit of every response schema in the pinned
/// `krabka-protocol` checkout. The `throttle_audit` sibling module pins it: it
/// enumerates every advertised `(api_key, version)` pair and compares this
/// predicate against the byte layout the generated encoder actually produces.
/// Adding an API to [`crate::api_catalog`] without classifying it here fails
/// that test.
///
/// Classifying an API correctly is necessary but not sufficient for it to echo
/// a delay. This predicate is only consulted where `apply_request_quota`
/// runs: the dispatch entries whose policy is
/// `RequestQuotaPolicy::ApplyFallbackAccounting` (the `DispatchEntry::plain`
/// ones) and the unsupported-version reply path, which takes it for every
/// `api_key`. The `InlineExempt` entries -- every handler that takes a
/// `RequestContext`, which is most of the admin and ACL surface -- are exempt
/// from the request quota altogether and so are neither delayed nor throttle-
/// stamped. Narrowing that exemption is KIP-124 work rather than KIP-219 work;
/// this table is what a narrowing would land on.
///
/// `Produce` (0) and `Fetch` (1) never reach this predicate. Both their
/// bandwidth quota and their share of the request quota are charged by the
/// handler, which sets `ThrottleTimeMs` on the typed response before encoding,
/// so `apply_request_quota` returns for both before consulting the
/// table. They are still classified below, and the audit still probes them, so
/// a schema move cannot pass unnoticed.
///
/// Everything else that answers `false` falls into three groups.
///
/// * Versions below the one at which the API moved `ThrottleTimeMs` to the
///   front of its response body: `Metadata` v0-2, `JoinGroup` v0-1,
///   `ListOffsets` v0-1, `FindCoordinator` v0 and the rest. The version bounds
///   in the arms below are exactly those boundaries.
/// * No `ThrottleTimeMs` field at any version: `SaslHandshake` (17),
///   `WriteTxnMarkers` (27), `SaslAuthenticate` (36), `DescribeQuorum` (55),
///   the share-coordinator state RPCs (83-87) and `GetReplicaLogInfo` (93).
///   There is nothing to echo. `Vote` (52), `BeginQuorumEpoch` (53),
///   `EndQuorumEpoch` (54) and `Envelope` (58) are throttle-free as well, but
///   the broker does not advertise them, so the audit does not reach them.
/// * KIP-219 divergences -- the field is present but sits behind another
///   field, so patching a leading int32 would corrupt the response. These are
///   recorded as `THROTTLE_ECHO_DIVERGENCES` in the audit module and as rows
///   in the generated `docs/KIP_MATRIX.md`:
///   `Produce` (0) v1+ and `ApiVersions` (18) v1+ carry it after a
///   variable-length array, the four delegation-token APIs (38-41) carry it
///   last, and `OffsetDelete` (47) leads with `ErrorCode`.
///   What that costs on the wire differs per API, and the audit records it
///   as a `QuotaReach` pinned against the dispatch registry: `Produce` is
///   `SelfAccounted`, so it never reaches this predicate and its handler
///   fills the field in; `ApiVersions` is `ApplyFallbackAccounting`, so an
///   ordinary request can be held while the response reports zero; the other
///   five are `InlineExempt`, so only the unsupported-version reply path --
///   which charges every `api_key` -- can hold one.
///   Echoing the field on any of them needs it set on the typed response
///   before encoding, which is how the Produce and Fetch handlers already do
///   it.
pub(super) fn throttle_is_leading_field(api_key: ApiKeyCode, version: ApiVersion) -> bool {
    // The version bounds are the schema versions at which each API moved
    // `ThrottleTimeMs` to the front of its response. They are deliberately
    // kept as literals and pinned by `throttle_audit`.
    let Some(api) = ApiKey::from_i16(api_key) else {
        return false;
    };
    match api {
        // Leading at every version the pinned schema defines.
        ApiKey::DeleteRecords
        | ApiKey::InitProducerId
        | ApiKey::AddPartitionsToTxn
        | ApiKey::AddOffsetsToTxn
        | ApiKey::EndTxn
        | ApiKey::TxnOffsetCommit
        | ApiKey::AlterConfigs
        | ApiKey::CreatePartitions
        | ApiKey::DeleteGroups
        | ApiKey::ElectLeaders
        | ApiKey::IncrementalAlterConfigs
        | ApiKey::AlterPartitionReassignments
        | ApiKey::ListPartitionReassignments
        | ApiKey::DescribeClientQuotas
        | ApiKey::AlterClientQuotas
        | ApiKey::DescribeUserScramCredentials
        | ApiKey::AlterUserScramCredentials
        | ApiKey::UpdateFeatures
        | ApiKey::FetchSnapshot
        | ApiKey::DescribeCluster
        | ApiKey::DescribeProducers
        | ApiKey::BrokerRegistration
        | ApiKey::BrokerHeartbeat
        | ApiKey::UnregisterBroker
        | ApiKey::DescribeTransactions
        | ApiKey::ListTransactions
        | ApiKey::AllocateProducerIds
        | ApiKey::ConsumerGroupHeartbeat
        | ApiKey::ConsumerGroupDescribe
        | ApiKey::ControllerRegistration
        | ApiKey::GetTelemetrySubscriptions
        | ApiKey::PushTelemetry
        | ApiKey::AssignReplicasToDirs
        | ApiKey::ListConfigResources
        | ApiKey::DescribeTopicPartitions
        | ApiKey::AddRaftVoter
        | ApiKey::RemoveRaftVoter
        | ApiKey::UpdateRaftVoter
        | ApiKey::StreamsGroupHeartbeat
        | ApiKey::StreamsGroupDescribe
        | ApiKey::DescribeShareGroupOffsets
        | ApiKey::AlterShareGroupOffsets
        | ApiKey::DeleteShareGroupOffsets => true,
        // Moved to the front at v1.
        ApiKey::Fetch
        | ApiKey::FindCoordinator
        | ApiKey::Heartbeat
        | ApiKey::LeaveGroup
        | ApiKey::SyncGroup
        | ApiKey::DescribeGroups
        | ApiKey::ListGroups
        | ApiKey::DeleteTopics
        | ApiKey::DescribeAcls
        | ApiKey::CreateAcls
        | ApiKey::DeleteAcls
        | ApiKey::DescribeConfigs
        | ApiKey::AlterReplicaLogDirs
        | ApiKey::DescribeLogDirs
        | ApiKey::ShareGroupHeartbeat
        | ApiKey::ShareGroupDescribe
        | ApiKey::ShareFetch
        | ApiKey::ShareAcknowledge => version >= 1,
        // Moved to the front at v2.
        ApiKey::ListOffsets
        | ApiKey::JoinGroup
        | ApiKey::CreateTopics
        | ApiKey::OffsetForLeaderEpoch
        | ApiKey::AlterPartition => version >= 2,
        // Moved to the front at v3.
        ApiKey::Metadata | ApiKey::OffsetCommit | ApiKey::OffsetFetch => version >= 3,
        // Throttle-free, self-accounting, or a recorded divergence; see the
        // three groups in the doc comment. `ApiKey` is `#[non_exhaustive]`, so
        // a future variant lands here until the audit forces it to be
        // classified.
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
