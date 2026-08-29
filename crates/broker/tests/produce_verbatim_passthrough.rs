//! End-to-end coverage for the verbatim produce passthrough, which is the
//! zero-copy append path. The broker structurally validates a
//! producer-LZ4-compressed v2 batch, stores the original bytes without
//! re-encoding, and round-trips them byte-identically on Fetch. A
//! recompression-forcing topic config, a control batch, and an idempotent
//! producer all behave correctly across the path.
//!
//! These tests complement the unit tests in
//! `handlers::produce::prepare::tests::verbatim`, which pin the dispatch
//! (`prepare_batch` and `build_produce_data`) at the function level. These
//! tests drive the whole broker over the wire. Produce and
//! Fetch auto-negotiate to v13 (KIP-516 topic-id), so every batch travels the
//! v≥3 native-v2 path that the verbatim dispatch covers.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `produce_verbatim_passthrough/` directory, which keeps the parts out of
// `tests/` where every `.rs` file would become another test binary.
#[path = "produce_verbatim_passthrough/harness.rs"]
mod harness;
#[path = "produce_verbatim_passthrough/idempotence.rs"]
mod idempotence;
#[path = "produce_verbatim_passthrough/passthrough.rs"]
mod passthrough;
#[path = "produce_verbatim_passthrough/rejection.rs"]
mod rejection;
