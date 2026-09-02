//! COMPOSITIONAL end-to-end data-path model. It is the first model beyond the
//! per-slice ones. It composes the four real seam cores over a tiny cluster of
//! 3 brokers and 1 partition: HWM/ISR (`ReplicaState`), leader-epoch truncation
//! (`epoch_and_offset_for_entries`), failover selection (`failover_one` and
//! `select_leader`), KIP-966 ELR maintenance (`next_partition_elr`), and fetch
//! visibility (`compute_visibility_window`). It verifies the canonical broker
//! guarantee end-to-end. An `acks=all` record is never lost across clean leader
//! changes, every consumer read is consistent, and unclean-election loss is
//! exactly characterized.
//!
//! Each per-broker log is a `Vec<u8>` of leader epochs, where offset = index
//! and value ≡ offset. A ghost `committed` tracks durability, with one epoch
//! per offset that was ever ≤ HWM. The visibility seam checks read consistency.
//! The model was built incrementally: DPC-1 spine → DPC-2 clean failover →
//! DPC-3 unclean → DPC-4 the ELR rule. State explosion is the central risk, so
//! see the bounds and the host memory watchdog.
//!
//! # The rule that makes a claim
//!
//! The unclean-recovery election has two rules and they are not alike. The
//! most-complete-log fallback is a guess, and it is metered and audited as
//! one. The KIP-966 rule that runs ahead of it makes a claim: a surviving
//! member of the published eligible-leader set is elected over a strictly
//! longer log, and the election reports itself as losing nothing, on the
//! strength of how the controller maintained that set. `data_elr` is the
//! configuration that checks it — see [`elr`] for why the model has to run the
//! real maintenance rule rather than stipulate a set, and [`election`] for the
//! two durability obligations that the claim is and is not about.
//!
//! # Module layout
//!
//! This file is the module root. It holds the four checked configurations,
//! one per test. Each child holds one concern: `bounds` the cluster size and
//! the casts it makes total, `state` the search space, `hwm` and `truncation`
//! and `election` and `elr` the four seams onto the real broker cores, `model`
//! the stateright [`Model`](stateright::Model) implementation, and `runner`
//! the checker bounds.

use std::time::Instant;

mod bounds;
mod election;
mod elr;
mod hwm;
mod model;
mod runner;
mod state;
mod truncation;

use self::{model::DpModel, runner::run};

/// `min.insync.replicas` of 1, Kafka's default: the ELR rule clears the set
/// on every ISR change, so no configuration below drives it.
const NO_ELR: usize = 1;
/// The log length the configurations that carry no ELR state can afford.
const LONG_LOG: usize = 4;

#[test]
fn data_clean() {
    run(
        DpModel::config(Instant::now(), false, false, NO_ELR, LONG_LOG),
        "data_clean",
    );
}

#[test]
fn data_unclean() {
    run(
        DpModel::config(Instant::now(), true, false, NO_ELR, LONG_LOG),
        "data_unclean",
    );
}

/// DPC-4. `min.insync.replicas` of 2 on a 3-replica partition is the smallest
/// configuration in which a replica can leave an ISR that is about to fall
/// below min ISR, which is the only way KIP-966 puts one in the
/// eligible-leader set. The unclean strategy is what then reaches the election
/// that reads it.
///
/// The ELR bookkeeping makes each state larger and the search wider, so this
/// configuration runs at a log length of 3 rather than 4. Three records is one
/// more than the shortest path that reaches the property: a record replicated
/// to the whole ISR, an ISR that then falls below min ISR, and a second record
/// that only the leader and one non-eligible survivor hold.
#[test]
fn data_elr() {
    run(
        DpModel::config(Instant::now(), true, false, 2, 3),
        "data_elr",
    );
}

#[test]
fn data_diskless_wal_acked_never_lost() {
    run(
        DpModel::config(Instant::now(), false, true, NO_ELR, LONG_LOG),
        "data_diskless_wal_acked_never_lost",
    );
}

#[test]
fn data_diskless_offsets_gap_free_and_unique() {
    run(
        DpModel::config(Instant::now(), false, true, NO_ELR, LONG_LOG),
        "data_diskless_offsets_gap_free_and_unique",
    );
}
