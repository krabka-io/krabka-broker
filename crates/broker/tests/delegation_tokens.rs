// rustc 1.95 clippy ICEs on this file family (same as throttle.rs /
// describe_user_scram_credentials.rs). Suppress locally; the workspace
// lint gate still applies elsewhere.

//! KIP-48 end-to-end integration: the full delegation-token lifecycle
//! against a single-broker test cluster. Spec §8.2.
//!
//! One long `#[tokio::test]` walks every wire step the spec covers.
//!
//!   (a) SASL/PLAIN authenticate as `alice`.
//!   (b) `CreateDelegationToken` over that connection, with owner=alice,
//!       renewers=[User:bob], and `max_lifetime_ms = -1`, which defers to
//!       the broker ceiling. Capture `token_id` and `hmac`.
//!   (c) Open a second TCP connection and drive SASL/SCRAM-SHA-256 with
//!       username=`token_id` and password=base64(hmac). The KIP-48
//!       token-fallback path in `network::auth::handle_authenticate_scram`
//!       synthesizes a SCRAM credential for the token and accepts it. The
//!       principal must appear as `User:alice`, the token owner, and NOT as
//!       the `token_id`. The test asserts this by running
//!       `CreateDelegationToken` again on the token-authed connection and
//!       expecting `DELEGATION_TOKEN_REQUEST_NOT_ALLOWED` (64). That error
//!       is reachable only when the broker sees this session as
//!       `authenticated_via_token = true`, which the broker sets together
//!       with the owner-principal override.
//!   (d) Same connection: run `CreateDelegationToken` again and expect 64.
//!   (e) Third TCP connection, SASL/PLAIN as `bob`, then
//!       `RenewDelegationToken` with the captured HMAC. Expect
//!       `error_code = 0`, because the renewer-authorization gate accepts
//!       the listed renewer. Per KIP-48, the create handler sets
//!       `expiry_timestamp_ms = now + min(default_renew_period,
//!       chosen_lifetime)` and `max_timestamp_ms = now + chosen_lifetime`
//!       as SEPARATE values. A Renew with a large `renew_period_ms`
//!       therefore extends the expiry strictly beyond its initial value, up
//!       to `max_timestamp_ms` but never past it.
//!   (f) `alice`'s connection: `DescribeDelegationToken` with
//!       `owners=[User:alice]`. Expect 1 token, matching `token_id`.
//!   (g) `alice`'s connection: `ExpireDelegationToken` with
//!       `expiry_time_period_ms = -1`, the immediate-delete sentinel.
//!       Expect `error_code = 0`.
//!   (h) Fourth TCP connection: try SASL/SCRAM-SHA-256 with the same token
//!       credentials. Expect a failure, because the token's tombstone is in
//!       the image and the SCRAM credential lookup misses.
//!
//! This suite deliberately reuses the wire-driver shape from
//! `auth_handlers/harness.rs`, that is PLAIN, SCRAM-SHA-256, and the
//! `round_trip` helper, and from `describe_user_scram_credentials.rs`, the
//! `(handle, dir, addr)` cluster tuple. It adds no public test-support
//! surface. The helpers live in private child modules of this test binary, so
//! they do not leak into other tests.
//!
//! The child modules split the suite by the token surface each one covers.
//! `wire` and `rpc` hold the framing, the SASL drivers, and one helper per
//! delegation-token RPC. `cluster` boots the fixtures and waits on the
//! metadata image. `lifecycle`, `act_as`, and `super_user_bypass` hold the
//! tests themselves.

/// Canonical Kafka error code that mirrors `krabka_broker::codes::
/// DELEGATION_TOKEN_REQUEST_NOT_ALLOWED`. The broker's `codes` module is
/// private to the crate, so this file keeps a local copy. Keep it in sync
/// with `crates/broker/src/codes.rs` and the Apache Kafka error table.
pub(crate) const DELEGATION_TOKEN_REQUEST_NOT_ALLOWED: i16 = 64;
/// Canonical Kafka error code that mirrors `krabka_broker::codes::
/// DELEGATION_TOKEN_AUTHORIZATION_FAILED`. The same sync rule applies.
pub(crate) const DELEGATION_TOKEN_AUTHORIZATION_FAILED: i16 = 65;

#[path = "delegation_tokens/wire.rs"]
mod wire;

#[path = "delegation_tokens/rpc.rs"]
mod rpc;

#[path = "delegation_tokens/cluster.rs"]
mod cluster;

#[path = "delegation_tokens/act_as.rs"]
mod act_as;

#[path = "delegation_tokens/lifecycle.rs"]
mod lifecycle;

#[path = "delegation_tokens/super_user_bypass.rs"]
mod super_user_bypass;

#[path = "delegation_tokens/unauthenticated.rs"]
mod unauthenticated;

mod support;

#[path = "delegation_tokens/audit.rs"]
mod audit;
