// rustc 1.95 clippy::pedantic ICEs on this file (an upstream bug in
// clippy's body-analysis pass). Disable pedantic locally; the rest of
// the workspace still enforces the full pedantic gate.

//! Broker-side integration tests for the admin handlers.
//!
//! Each test starts a 1-broker cluster with [`support::start_n_node`] and
//! dispatches the relevant request through `krabka-client-core`. The test
//! then asserts on the response, or on observable broker state that the
//! `BrokerHandle` test-helper methods expose.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `admin_handlers/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "admin_handlers/admin_alter_configs.rs"]
mod admin_alter_configs;
#[path = "admin_handlers/admin_create_partitions.rs"]
mod admin_create_partitions;
#[path = "admin_handlers/admin_create_topics.rs"]
mod admin_create_topics;
#[path = "admin_handlers/admin_delete_records.rs"]
mod admin_delete_records;
#[path = "admin_handlers/admin_describe_cluster.rs"]
mod admin_describe_cluster;
#[path = "admin_handlers/admin_describe_quorum.rs"]
mod admin_describe_quorum;
#[path = "admin_handlers/admin_harness.rs"]
mod admin_harness;
#[path = "admin_handlers/admin_listings.rs"]
mod admin_listings;

/// Kafka resource type id for a topic.
const RESOURCE_TYPE_TOPIC: i8 = 2;
