//! KIP-890 per-level `transaction.version` integration tests.
//!
//! An in-process test broker self-bootstraps `transaction.version=2` (`TV_2`),
//! so the existing `transactions.rs` suite already covers the `TV_2` happy path.
//! These tests prove the *other* two levels end-to-end, the `TV_2`
//! verify-only `AddPartitionsToTxn` path, and that persisted txn state
//! survives a broker restart (the startup DECODE/recover-from-disk path):
//!
//! 1. **`TV_1`**: downgrade `transaction.version` to 1 (flexible, v1
//!    `TransactionLogValue` records, no epoch bump), then run a full
//!    transactional produce → commit → `read_committed` consume. Success
//!    proves that the coordinator persists `__transaction_state` through the
//!    v1 *encode* path at the resolved level, and that the transaction
//!    commits and reads end-to-end.
//! 2. **`TV_0`**: downgrade to 0 (tombstone → Classic, non-flexible v0
//!    records), then run the same full cycle. This proves the v0 encode path
//!    and the cycle.
//! 3. **verify-only `AddPartitionsToTxn`** at `TV_2`: confirm per-partition
//!    `NONE (0)` for an already-added partition and
//!    `TRANSACTION_ABORTABLE (120)` for one that was never added.
//! 4. **restart recovery** (v0 and v1): persist an `Ongoing` entry, restart the
//!    broker on the same data dir, and prove that `TxnCoordinator::recover`
//!    decodes the `__transaction_state` record from disk. The proof is a
//!    commit of the recovered txn through `EndTxn`. This is the only path that
//!    exercises the startup decode/recover code, which the live-broker tests
//!    above cannot reach.
//!
//! This suite is Windows-gated like `transactions.rs`. On the hosted Windows
//! runner, openraft and tokio scheduling cause intermittent
//! `INVALID_TXN_STATE` during `InitProducerId`.
//!
//! Cases 1 and 2 live in `txnver_full_cycle`, case 3 in `txnver_verify_only`,
//! and case 4 in `txnver_restart_recovery`; `txnver_harness` carries the broker
//! boot, topic creation, and feature-downgrade fixtures they share.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `transaction_version/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "transaction_version/txnver_full_cycle.rs"]
mod txnver_full_cycle;
#[path = "transaction_version/txnver_harness.rs"]
mod txnver_harness;
#[path = "transaction_version/txnver_restart_recovery.rs"]
mod txnver_restart_recovery;
#[path = "transaction_version/txnver_verify_only.rs"]
mod txnver_verify_only;
