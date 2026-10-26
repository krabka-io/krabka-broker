//! Wire handlers for the transaction protocol APIs.
//!
//! Each sub-module implements the `handle` function for one `api_key`.
//! `crate::handlers::build_table`, in the top-level handlers module,
//! registers them. This module only declares the sub-modules.

pub mod add_offset_commits_to_txn;
pub mod add_partitions_to_txn;
pub mod end_txn;
pub mod txn_offset_commit;
pub mod write_txn_markers;

#[cfg(test)]
mod authorization_tests;
#[cfg(test)]
mod client_version_tests;
