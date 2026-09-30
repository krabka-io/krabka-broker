//! Durability arithmetic for a stretch cluster.
//!
//! A stretch cluster holds one Kafka cluster in more than one site. The claim
//! that such a cluster keeps the data and stays available through the loss of
//! one whole site rests on three numbers: the replica count that survives a
//! site loss, the `min.insync.replicas` value that stays satisfiable after that
//! loss, and the split of the `KRaft` voters over the sites. This module states
//! those three numbers as kernels, and Creusot proves the contracts.
//!
//! The replica numbers are stated against a model of the placement,
//! `round_robin_load`: replica `r` of a partition lands on site `r % sites`.
//! The contracts say what each site then holds and what the loss of each site
//! leaves, and the proof derives the `ceil(rf / sites)` arithmetic from that
//! model. That the broker's placer actually places replicas this way is not
//! proved here; it is the host's responsibility.
//!
//! The preconditions bound the inputs at 1024. A replication factor, a site
//! count, and a voter count are all small in a real deployment. The bounds only
//! keep the arithmetic away from `i64` overflow, and a stated bound is honest
//! about what the proof covers.

#[cfg(creusot)]
use creusot_std::prelude::*;

mod quorum_survives_any_single_site_loss;
pub use quorum_survives_any_single_site_loss::quorum_survives_any_single_site_loss;
#[cfg(creusot)]
pub use quorum_survives_any_single_site_loss::{
    lemma_sum_prefix_step, lemma_two_sites_never_survive, sum_all, sum_prefix,
};

mod min_insync_is_site_loss_safe;
#[cfg(creusot)]
pub use min_insync_is_site_loss_safe::{
    lemma_div_exact, lemma_div_monotone, lemma_round_robin_load, lemma_round_robin_step,
    lemma_site_load_at_most_first, round_robin_load,
};
pub use min_insync_is_site_loss_safe::{min_insync_is_site_loss_safe, site_loss_survivors};

#[cfg(test)]
mod tests;
