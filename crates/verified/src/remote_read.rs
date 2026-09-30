//! Remote-segment admission and relative-offset arithmetic.

#[cfg(creusot)]
use creusot_std::prelude::*;

mod remote_time_index_candidate_count;
pub use remote_time_index_candidate_count::{
    remote_time_index_candidate_count, tiered_earliest_finished_index,
    tiered_latest_finished_index, tiered_owning_epoch_index,
};

mod relative_offset;
pub use relative_offset::{remote_fetch_end_position, remote_read_relative_offset};

#[cfg(test)]
mod tests;
