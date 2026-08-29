//! The partition-delete lifecycle.
//!
//! [`RemotePartitionDeleteMetadata`] records how far the deletion of one
//! partition's remote data has got, and [`RemotePartitionDeleteState`] holds
//! the three states plus the rule for moving between them. This lifecycle is
//! separate from the per-segment one in
//! [`RemoteLogSegmentState`](crate::metadata::RemoteLogSegmentState).

use crate::metadata::TopicIdPartition;

/// Lifecycle state of a remote *partition* deletion.
///
/// ```text
/// DeletePartitionMarked ──► DeletePartitionStarted ──► DeletePartitionFinished
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RemotePartitionDeleteState {
    /// The partition has been marked for deletion of all its remote
    /// segments.
    DeletePartitionMarked,
    /// Deletion of the partition's remote segments has begun.
    DeletePartitionStarted,
    /// All remote segments for the partition have been deleted.
    DeletePartitionFinished,
}

impl RemotePartitionDeleteState {
    /// `true` if a partition currently in `from` may transition to `target`.
    ///
    /// A `from` of `None` means the partition was never marked.
    #[must_use]
    pub fn is_valid_transition(from: Option<Self>, target: Self) -> bool {
        use RemotePartitionDeleteState::{
            DeletePartitionFinished, DeletePartitionMarked, DeletePartitionStarted,
        };
        matches!(
            (from, target),
            (None, DeletePartitionMarked)
                | (Some(DeletePartitionMarked), DeletePartitionStarted)
                | (Some(DeletePartitionStarted), DeletePartitionFinished)
        )
    }
}

/// Metadata describing the deletion lifecycle of a partition's remote data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePartitionDeleteMetadata {
    /// The partition being deleted from the remote tier.
    pub topic_id_partition: TopicIdPartition,
    /// Current deletion state.
    pub state: RemotePartitionDeleteState,
    /// Wall-clock time of this event.
    pub event_timestamp_ms: i64,
    /// Broker that produced this metadata.
    pub broker_id: i32,
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn partition_delete_transitions() {
        use RemotePartitionDeleteState::{
            DeletePartitionFinished, DeletePartitionMarked, DeletePartitionStarted,
        };
        for (from, to, want) in [
            (None, DeletePartitionMarked, true),
            (Some(DeletePartitionMarked), DeletePartitionStarted, true),
            (Some(DeletePartitionStarted), DeletePartitionFinished, true),
            // Invalid: skipping, restarting, or marking twice.
            (None, DeletePartitionStarted, false),
            (Some(DeletePartitionMarked), DeletePartitionMarked, false),
            (Some(DeletePartitionFinished), DeletePartitionStarted, false),
        ] {
            check!(
                RemotePartitionDeleteState::is_valid_transition(from, to) == want,
                "{from:?} -> {to:?}"
            );
        }
    }
}
