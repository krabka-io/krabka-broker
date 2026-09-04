//! Kafka CLI round-trips against a single host broker: console produce/consume,
//! topic and config administration, consumer-group listing and offset deletion.
//!
//! The shared harness lives in [`jvm_acceptance`]; see it for the container
//! networking these suites depend on. The binary root carries only the module
//! tree, and each child covers one command-line tool or one surface of it.

mod jvm_acceptance;
mod support;

// Cargo compiles this file as its own test binary, so a plain `mod` here
// resolves against `tests/`. `#[path]` re-bases each declaration onto the
// sibling `jvm_acceptance_cli/` directory, which keeps the parts out of
// `tests/`, where every `.rs` file would become another test binary.
#[path = "jvm_acceptance_cli/cluster.rs"]
mod cluster;
#[path = "jvm_acceptance_cli/compaction.rs"]
mod compaction;
#[path = "jvm_acceptance_cli/config_output.rs"]
mod config_output;
#[path = "jvm_acceptance_cli/configs.rs"]
mod configs;
#[path = "jvm_acceptance_cli/console.rs"]
mod console;
#[path = "jvm_acceptance_cli/console_groups.rs"]
mod console_groups;
#[path = "jvm_acceptance_cli/consumer_groups.rs"]
mod consumer_groups;
#[path = "jvm_acceptance_cli/delete_records.rs"]
mod delete_records;
#[path = "jvm_acceptance_cli/elr_columns.rs"]
mod elr_columns;
#[path = "jvm_acceptance_cli/group_output.rs"]
mod group_output;
// The oracle harness these suites compare against. It lives beside this
// binary's parts and is re-based into `jvm_acceptance_sasl` and
// `jvm_acceptance_reassign` with `#[path]`, so the three share one copy.
#[path = "jvm_acceptance_cli/oracle.rs"]
mod oracle;
#[path = "jvm_acceptance_cli/topics.rs"]
mod topics;
