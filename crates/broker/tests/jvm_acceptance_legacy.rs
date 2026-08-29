//! Kafka 0.10.1 clients (Confluent Platform 3.1.2) against a modern broker,
//! exercising v1 `MessageSet` records and the up/down-conversion paths.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on. The binary root carries only the module
//! tree, and each child covers one conversion path: the pure-legacy round
//! trip, the two mixed-vintage round trips, and the compressed batches.

mod jvm_acceptance;
mod support;

// Cargo compiles this file as its own test binary, so a plain `mod` here
// resolves against `tests/`. `#[path]` re-bases each declaration onto the
// sibling `jvm_acceptance_legacy/` directory, which keeps the parts out of
// `tests/`, where every `.rs` file would become another test binary.
#[path = "jvm_acceptance_legacy/compression.rs"]
mod compression;
#[path = "jvm_acceptance_legacy/cross_version.rs"]
mod cross_version;
#[path = "jvm_acceptance_legacy/legacy_round_trip.rs"]
mod legacy_round_trip;
