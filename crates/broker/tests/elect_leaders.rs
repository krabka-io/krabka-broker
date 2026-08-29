// rustc 1.95 clippy ICEs on this file in two places:
//
// 1. `clippy::pedantic` lints — same upstream annotate-snippets bug as
//    `tests/acl_handlers.rs` and `tests/admin_handlers.rs`.
// 2. `clippy::unwrap_in_result` — the `UnwrappableVariablesVisitor` in
//    `clippy_lints::unwrap` ICEs on the `.expect()` calls inside `round_trip`
//    (which returns `Result`) because the computed span has start > end.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.
// `clippy::unnecessary_unwrap` fires on the `l1.unwrap()` inside `if l1.is_some()`
// and its span computation ICEs in annotate-snippets on Rust 1.95.
// `clippy::too_many_lines` fires on the auto-rebalance integration test body.

//! Broker-side integration tests for the operator-triggered `ElectLeaders` RPC.
//!
//! The tests drive the wire path end-to-end with a Rust PLAINTEXT client. They
//! then read the resulting partition state through `BrokerHandle` test
//! accessors.
//!
//! Both tests use a **3-broker PLAINTEXT cluster** and not a 2-broker SASL
//! cluster, for two reasons:
//!
//! * A 2-broker raft cluster cannot form a quorum (2/2) when one broker is
//!   dead. The automatic partition-leader election and the metadata commits
//!   that these tests need thus never succeed. A 3-broker cluster keeps quorum
//!   (2/3) with one dead node, which is enough for both test scenarios.
//!
//! * The compatibility shim of the authorizer maps an empty `super_users` list
//!   and zero ACLs to Allow. The test can thus exercise the full `ElectLeaders`
//!   wire path without a SASL handshake, which keeps the test helpers simpler.
//!
//! These tests are gated to non-Windows to match the multi-broker test
//! convention from slices 10b/12b. The openraft `debug_assert!` races on the
//! hosted Windows task scheduler are unrelated to the protocol under test.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `elect_leaders/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "elect_leaders/authorization.rs"]
mod authorization;
#[path = "elect_leaders/auto_rebalance.rs"]
mod auto_rebalance;
#[path = "elect_leaders/preferred.rs"]
mod preferred;
#[path = "elect_leaders/sasl.rs"]
mod sasl;
#[path = "elect_leaders/unclean.rs"]
mod unclean;
#[path = "elect_leaders/wait.rs"]
mod wait;
#[path = "elect_leaders/wire.rs"]
mod wire;

/// Shared cluster lock for every test in this binary.
///
/// The lock serializes the tests onto one 3-broker cluster at a time. It
/// mirrors the locks in `quorum.rs` and `leader_election.rs`. Without it, the
/// static 3-voter clusters of the tests boot at the same time on the same
/// loopback with short raft timings. They then starve each other of elections
/// and of ISR re-admission, which shows as intermittent `FENCED_LEADER_EPOCH`
/// churn.
fn cluster_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
