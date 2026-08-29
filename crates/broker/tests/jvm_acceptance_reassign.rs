//! Partition reassignment and preferred-leader election driven by the JVM admin
//! tools against a three-broker SASL cluster.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on. The binary root carries only the module
//! tree, and each child covers one admin-tool flow.

mod jvm_acceptance;
mod support;

// Cargo compiles this file as its own test binary, so a plain `mod` here
// resolves against `tests/`. `#[path]` re-bases each declaration onto the
// sibling `jvm_acceptance_reassign/` directory, which keeps the parts out of
// `tests/`, where every `.rs` file would become another test binary.
#[path = "jvm_acceptance_reassign/preferred_election.rs"]
mod preferred_election;
#[path = "jvm_acceptance_reassign/reassign_execute.rs"]
mod reassign_execute;
#[path = "jvm_acceptance_reassign/reassign_throttle.rs"]
mod reassign_throttle;
