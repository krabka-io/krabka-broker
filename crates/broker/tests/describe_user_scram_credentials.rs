// rustc 1.95 clippy ICEs on this file (same as throttle.rs / elect_leaders.rs).
// Suppress locally; the workspace lint gate still applies elsewhere.

//! Broker-side integration tests for `DescribeUserScramCredentials`
//! (`api_key` 50, KIP-554 read half).
//!
//! Tests:
//! 1. `describe_all_users_round_trip`: seed alice's SCRAM credential with
//!    `submit_metadata_record_for_test`, describe with `users=None`, then
//!    assert mechanism=2 (SCRAM-SHA-512) in the response.
//! 2. `describe_unknown_user_returns_error`: describe `users=[ghost]`, then
//!    assert the per-user `error_code = 91` (`RESOURCE_NOT_FOUND`).
//!
//! These tests are gated to non-Windows to match the multi-broker test
//! convention from slices 10b/12b/14/15/15b/16.
//!
//! The binary root carries only the module tree. `scram_wire` holds the framing
//! and the SASL/PLAIN handshake, `scram_cluster` boots the broker and seeds the
//! metadata records, `scram_driver` drives `api_key` 50, and the two remaining
//! children hold the response-row tests and the authorization tests.

// Cargo compiles this file as its own test binary, so a plain `mod` here
// resolves against `tests/`. `#[path]` re-bases each declaration onto the
// sibling `describe_user_scram_credentials/` directory, which keeps the parts
// out of `tests/`, where every `.rs` file would become another test binary.
#[path = "describe_user_scram_credentials/scram_authorization.rs"]
mod scram_authorization;
#[path = "describe_user_scram_credentials/scram_cluster.rs"]
mod scram_cluster;
#[path = "describe_user_scram_credentials/scram_describe.rs"]
mod scram_describe;
#[path = "describe_user_scram_credentials/scram_driver.rs"]
mod scram_driver;
#[path = "describe_user_scram_credentials/scram_wire.rs"]
mod scram_wire;
