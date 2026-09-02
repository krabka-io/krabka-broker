//! The one local append that every barrier marker goes through.
//!
//! Both the coordinator's own fan-out and the `WriteBarrierMarkers` handler
//! land here, so a marker that a remote coordinator asks for takes exactly the
//! batch shape of one this broker placed itself.

use krabka_ids::NodeId;
use krabka_log::Offset;
use krabka_verified::{
    BarrierMarkerFenceDecision, BarrierMarkerFenceFacts, barrier_marker_fence_decision,
};

use crate::{
    barrier::marker::{BarrierMarker, build_barrier_batch},
    error::BrokerError,
    partition::Partition,
};

/// A marker append rejected by the leadership fence or by the partition
/// writer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MarkerAppendError {
    #[error("barrier marker append was fenced: {0:?}")]
    Fence(BarrierMarkerFenceDecision),
    #[error(transparent)]
    Broker(#[from] BrokerError),
}

/// Append one barrier marker to a local partition, and return its offset.
///
/// The batch carries the expected leader epoch after the installed partition
/// matches it. The writer does not stamp the epoch, and a default of zero can
/// be false in the header.
///
/// The `WriteBarrierMarkers` handler appends through this function too, so a
/// marker that a remote coordinator asks for takes the same batch shape as one
/// this broker's own coordinator places.
///
/// # Errors
/// Returns a [`MarkerAppendError`] when the installed leadership generation
/// differs from the expected one, when the partition writer is gone, or when
/// the log rejects the batch.
pub(crate) async fn append_marker(
    partition: &Partition,
    marker: &BarrierMarker,
    expected_leader: NodeId,
    expected_epoch: i32,
) -> Result<Offset, MarkerAppendError> {
    // The read guard linearizes this admission and append with metadata's
    // write-locked leader transition. A marker cannot pass the fence and then
    // enter the writer after another generation is installed.
    let installed = partition.lock_produce_transition().await;
    let decision = barrier_marker_fence_decision(BarrierMarkerFenceFacts {
        image_present: true,
        expected_leader: expected_leader.get(),
        expected_epoch,
        image_leader: expected_leader.get(),
        image_epoch: expected_epoch,
        current_leader: installed.leader_node_id.0,
        current_epoch: installed.leader_epoch.get(),
    });
    if decision != BarrierMarkerFenceDecision::Append {
        return Err(MarkerAppendError::Fence(decision));
    }
    let batch = build_barrier_batch(marker, partition.log_end_offset(), expected_epoch);
    Ok(partition.produce_control_batch(batch).await?)
}
