// rustc 1.95 clippy ICEs on this file in the same places as throttle.rs /
// elect_leaders.rs:
//
// 1. `clippy::pedantic` lints — annotate-snippets upstream bug.
// 2. `clippy::unnecessary_unwrap` — UnwrappableVariablesVisitor ICE.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.

//! Broker-side integration tests for KIP-13/124/257 client quotas.
//!
//! Tests:
//! 1. `alter_then_describe_round_trip` — `AlterClientQuotas` sets
//!    `(user=alice) producer_byte_rate=1024`; `DescribeClientQuotas` returns it.
//! 2. `producer_byte_rate_throttles_produce` — Set low `producer_byte_rate` for
//!    alice; produce a large payload; assert `throttle_time_ms` > 0.
//! 3. `consumer_byte_rate_throttles_fetch` — Set low `consumer_byte_rate` for
//!    alice; produce then fetch a large payload; assert `throttle_time_ms` > 0.
//! 4. `user_client_tuple_overrides_user_specific` — Set
//!    (user=alice, client-id=krabka-quota-test) `producer_byte_rate=128` AND
//!    (user=alice) `producer_byte_rate=8192`; produce with that client id; the
//!    tight tuple limit fires, not the user-only limit.
//! 5. `non_super_user_denied` — alice (no ACLs) calls `AlterClientQuotas`;
//!    must receive `CLUSTER_AUTHORIZATION_FAILED` (31) on every entry.
//! 6. `request_percentage_throttles_produce` — Set a tiny `request_percentage`
//!    (KIP-124) for alice with NO byte-rate quota; produce a small payload;
//!    assert `throttle_time_ms` > 0. Proves the request-quota throttle is
//!    communicated in the response (KIP-219 throttle-then-respond) and not
//!    just silently muted.
//! 7. `request_percentage_throttle_is_echoed_on_a_patched_api` — the same tiny
//!    `request_percentage`, but on `AddOffsetsToTxn`, whose delay the dispatch
//!    loop reports by patching the leading `ThrottleTimeMs` of the encoded
//!    body rather than by having the handler fill it in. Asserts the throttled
//!    response differs from the unthrottled one in that field alone.
//! 8. `request_quota_patch_uses_the_reply_version_not_the_request_version` —
//!    the same tiny `request_percentage`, but on an `AllocateProducerIds`
//!    request at a version the broker does not support. The reply is encoded
//!    at the nearest supported version, whose header flexibility is not the
//!    request's, so a patch that takes its offset from the request writes one
//!    byte early. Asserts the reported back-off stays within
//!    `quota_throttle_max` and the rest of the reply is untouched.
//!
//! Test 4 exercises header `client_id` propagation through Produce and the
//! tuple-over-user precedence rule end to end. Fetch uses the same request
//! context field and has a focused handler test for tuple matching.
//!
//! These tests are gated to non-Windows to match the multi-broker test
//! convention from slices 10b/12b/14/15/15b.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `client_quotas/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "client_quotas/admin_api.rs"]
mod admin_api;
#[path = "client_quotas/cluster.rs"]
mod cluster;
#[path = "client_quotas/data_plane.rs"]
mod data_plane;
#[path = "client_quotas/quota_admin.rs"]
mod quota_admin;
#[path = "client_quotas/throttling.rs"]
mod throttling;
#[path = "client_quotas/wire.rs"]
mod wire;
