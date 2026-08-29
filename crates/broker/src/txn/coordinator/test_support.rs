//! Shared fixtures for the transaction-coordinator unit tests.
//!
//! The builders make a `TxnCoordinator` with no live partitions and a plain
//! `TxnEntry`, so the tests in more than one submodule build the same fixture
//! without repeating it.

use std::sync::Arc;

use krabka_log::ProducerId;

use super::TxnCoordinator;
use crate::{partition_registry::PartitionRegistry, txn::state::TxnEntry};

pub(super) fn test_coordinator() -> TxnCoordinator {
    test_coordinator_with_partitions(50)
}

pub(super) fn test_coordinator_with_partitions(num_partitions: i32) -> TxnCoordinator {
    TxnCoordinator::new(
        krabka_metadata::NodeId(1),
        Arc::new(PartitionRegistry::new()),
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        num_partitions,
        krabka_units::mebibytes(1),
    )
}

pub(super) fn entry(pid: i64, prev: i64) -> TxnEntry {
    let mut e = TxnEntry::new_empty("tid-a".into(), ProducerId(pid), 0, 60_000, 0);
    e.prev_producer_id = ProducerId(prev);
    e
}
