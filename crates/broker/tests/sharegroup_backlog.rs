//! End-to-end coverage for the coordinator-emitted share-group backlog gauge.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `sharegroup_backlog/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "sharegroup_backlog/harness.rs"]
mod harness;
#[path = "sharegroup_backlog/rf_three.rs"]
mod rf_three;
#[path = "sharegroup_backlog/single_broker.rs"]
mod single_broker;
