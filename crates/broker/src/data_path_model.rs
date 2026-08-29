//! COMPOSITIONAL end-to-end data-path model. It is the first model beyond the
//! per-slice ones. It composes the four real seam cores over a tiny cluster of
//! 3 brokers and 1 partition: HWM/ISR (`ReplicaState`), leader-epoch truncation
//! (`epoch_and_offset_for_entries`), failover selection (`failover_one` and
//! `select_best_replica`), and fetch visibility
//! (`compute_visibility_window`). It verifies the canonical broker guarantee
//! end-to-end. An `acks=all` record is never lost across clean leader changes,
//! every consumer read is consistent, and unclean-election loss is exactly
//! characterized.
//!
//! Each per-broker log is a `Vec<u8>` of leader epochs, where offset = index
//! and value ≡ offset. A ghost `committed` tracks durability, with one epoch
//! per offset that was ever ≤ HWM. The visibility seam checks read consistency.
//! The model was built incrementally: DPC-1 spine → DPC-2 clean failover →
//! DPC-3 unclean. State explosion is the central risk, so see the bounds and
//! the host memory watchdog.
//!
//! # Module layout
//!
//! This file is the module root. It holds the four checked configurations,
//! one per test. Each child holds one concern: `bounds` the cluster size and
//! the casts it makes total, `state` the search space, `hwm` and `truncation`
//! and `election` the three seams onto the real broker cores, `model` the
//! stateright [`Model`](stateright::Model) implementation, and `runner` the
//! checker bounds.

use std::time::Instant;

mod bounds;
mod election;
mod hwm;
mod model;
mod runner;
mod state;
mod truncation;

use self::{model::DpModel, runner::run};

#[test]
fn data_clean() {
    run(
        DpModel {
            base: Instant::now(),
            unclean: false,
            diskless: false,
        },
        "data_clean",
    );
}

#[test]
fn data_unclean() {
    run(
        DpModel {
            base: Instant::now(),
            unclean: true,
            diskless: false,
        },
        "data_unclean",
    );
}

#[test]
fn data_diskless_wal_acked_never_lost() {
    run(
        DpModel {
            base: Instant::now(),
            unclean: false,
            diskless: true,
        },
        "data_diskless_wal_acked_never_lost",
    );
}

#[test]
fn data_diskless_offsets_gap_free_and_unique() {
    run(
        DpModel {
            base: Instant::now(),
            unclean: false,
            diskless: true,
        },
        "data_diskless_offsets_gap_free_and_unique",
    );
}
