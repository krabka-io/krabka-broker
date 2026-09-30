//! Diskless WAL admission decisions.

#[cfg(creusot)]
use std::clone::Clone;

use creusot_std::prelude::*;

/// Result of authorizing and epoch-fencing one diskless WAL Fetch.
#[cfg_attr(creusot, derive(Clone, Copy, DeepModel))]
#[cfg_attr(not(creusot), derive(Clone, Copy, Debug, PartialEq, Eq))]
pub enum WalFetchAdmission {
    Denied,
    FencedLeaderEpoch,
    UnknownLeaderEpoch,
    Serve,
}

mod batch_equal;
#[cfg(creusot)]
pub use batch_equal::wal_batch_layout;
pub use batch_equal::{
    exact_wal_batch_range, wal_batch_equal, wal_checkpoint_range_valid, wal_covering_batch_range,
};

mod select_wal_voters;
#[cfg(creusot)]
#[cfg(creusot)]
use select_wal_voters::contains;
pub use select_wal_voters::{
    select_wal_voter_index, select_wal_voters, wal_fetch_admission, wal_voter_set_valid,
};

#[cfg(test)]
mod tests;
