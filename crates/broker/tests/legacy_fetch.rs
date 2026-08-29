//! End-to-end tests: produce a v2 batch through the modern Produce path, then
//! Fetch on a legacy version. The wire must carry a v0 or v1 `MessageSet` that
//! decodes back to the same records. The tests cover:
//!   - Fetch v3, which gives `Magic::V1` and keeps the KIP-32 timestamps.
//!   - Fetch v0, which gives `Magic::V0` and strips the per-message
//!     timestamps.
//!   - zstd-compressed batches, which the broker re-compresses as snappy.
//!   - control batches, which the broker drops from the down-converted
//!     response.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `legacy_fetch/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "legacy_fetch/control_batch.rs"]
mod control_batch;
#[path = "legacy_fetch/downconversion.rs"]
mod downconversion;
#[path = "legacy_fetch/harness.rs"]
mod harness;
#[path = "legacy_fetch/recompression.rs"]
mod recompression;
