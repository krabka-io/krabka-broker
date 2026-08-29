// rustc 1.95 clippy ICEs on this file in the same places as elect_leaders.rs:
//
// 1. `clippy::pedantic` lints — annotate-snippets upstream bug.
// 2. `clippy::unnecessary_unwrap` — UnwrappableVariablesVisitor ICE.
//
// Both are suppressed locally; the rest of the workspace still enforces the
// full lint gate.

//! Broker-side integration tests for `AlterPartitionReassignments`
//! (`api_key` 45) and `ListPartitionReassignments` (`api_key` 46).
//!
//! These tests use a 3-broker PLAINTEXT cluster. The authorizer's
//! compatibility shim allows every request when there are no `super_users` and
//! no ACLs, so the tests exercise the full wire path without a SASL handshake.
//!
//! They are gated to non-Windows, to match the multi-broker test convention
//! from slices 10b, 12b, and 14.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `partition_reassignment/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
#[path = "partition_reassignment/plaintext_cluster.rs"]
mod plaintext_cluster;
#[path = "partition_reassignment/plaintext_wire.rs"]
mod plaintext_wire;
#[path = "partition_reassignment/reassign_authorization.rs"]
mod reassign_authorization;
#[path = "partition_reassignment/reassign_lifecycle.rs"]
mod reassign_lifecycle;
#[path = "partition_reassignment/reassign_rpc.rs"]
mod reassign_rpc;
#[path = "partition_reassignment/sasl_wire.rs"]
mod sasl_wire;
