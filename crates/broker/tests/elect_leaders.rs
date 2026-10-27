#![cfg(test)]

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
//! convention established by the existing integration suites. The openraft `debug_assert!` races on the
//! hosted Windows task scheduler are unrelated to the protocol under test.

mod support;

use support::cluster_lock;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `elect_leaders/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "elect_leaders/authorization.rs"]
mod authorization;
#[path = "elect_leaders/auto_rebalance.rs"]
mod auto_rebalance;
mod kafka_wire;
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
