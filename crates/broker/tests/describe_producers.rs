// Rust 1.95 annotate-snippets ICE on clippy::pedantic in test files.

//! KIP-664 `DescribeProducers` admin RPC (`api_key` 61). It reports the
//! broker's in-memory producer-state snapshot.
//!
//! Tests:
//!   * an empty partition returns an empty `active_producers` list
//!   * after an idempotent `Produce`, the response carries the producer's id,
//!     epoch, `last_sequence`, and `last_timestamp`
//!   * several producers on the same partition all appear
//!   * an unknown topic, or a partition out of range, gives
//!     `UNKNOWN_TOPIC_OR_PARTITION (3)` for that partition
//!
//! The binary root carries only the module tree. `producers_harness` creates
//! the topics, producer ids and record batches the tests share, and each
//! remaining child covers one behaviour: the idempotent-producer snapshot, the
//! transaction fields, and the per-partition error paths.

mod support;

// Cargo compiles this file as its own test binary, so a plain `mod` here
// resolves against `tests/`. `#[path]` re-bases each declaration onto the
// sibling `describe_producers/` directory, which keeps the parts out of
// `tests/`, where every `.rs` file would become another test binary.
#[path = "describe_producers/producers_errors.rs"]
mod producers_errors;
#[path = "describe_producers/producers_harness.rs"]
mod producers_harness;
#[path = "describe_producers/producers_idempotent.rs"]
mod producers_idempotent;
#[path = "describe_producers/producers_transactional.rs"]
mod producers_transactional;
