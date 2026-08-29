//! The one local append that every barrier marker goes through.
//!
//! Both the coordinator's own fan-out and the `WriteBarrierMarkers` handler
//! land here, so a marker that a remote coordinator asks for takes exactly the
//! batch shape of one this broker placed itself.

use std::sync::atomic::Ordering;

use krabka_log::Offset;

use crate::{
    barrier::marker::{BarrierMarker, build_barrier_batch},
    error::BrokerError,
    partition::Partition,
};

/// Append one barrier marker to a local partition, and return its offset.
///
/// The batch carries the partition's current leader epoch, because the writer
/// does not stamp it and a default of zero is a false epoch in the header.
///
/// The `WriteBarrierMarkers` handler appends through this function too, so a
/// marker that a remote coordinator asks for takes the same batch shape as one
/// this broker's own coordinator places.
///
/// # Errors
/// Returns a [`BrokerError`] when the partition writer is gone, or when the
/// log rejects the batch.
pub(crate) async fn append_marker(
    partition: &Partition,
    marker: &BarrierMarker,
) -> Result<Offset, BrokerError> {
    let leader_epoch = partition.current_leader_epoch.load(Ordering::Acquire);
    let batch = build_barrier_batch(marker, partition.log_end_offset(), leader_epoch);
    partition.produce_control_batch(batch).await
}
