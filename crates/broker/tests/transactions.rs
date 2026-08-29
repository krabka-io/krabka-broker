//! In-process transactional integration tests.
//!
//! These tests exercise the full end-to-end transactional path: producer
//! init, begin, send, commit or abort, then consumer isolation.
//!
//! They are gated off Windows, like the other multi-node tests. openraft and
//! tokio scheduling on Windows runners cause intermittent
//! `INVALID_TXN_STATE` errors during `InitProducerId`. The transactional
//! control plane is correct on every platform. The gate avoids a flaky CI
//! signal until the Windows scheduling work is done.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `transactions/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "transactions/txn_consume_process_produce.rs"]
mod txn_consume_process_produce;
#[path = "transactions/txn_fencing.rs"]
mod txn_fencing;
#[path = "transactions/txn_harness.rs"]
mod txn_harness;
#[path = "transactions/txn_isolation.rs"]
mod txn_isolation;
#[path = "transactions/txn_sasl.rs"]
mod txn_sasl;
