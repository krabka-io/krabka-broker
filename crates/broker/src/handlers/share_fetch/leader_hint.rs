//! The `CurrentLeader` hint and the KIP-951 `NodeEndpoints` that
//! `ShareFetch` and `ShareAcknowledge` send for a partition row that another
//! broker leads.
//!
//! Kafka's `KafkaApis.processShareFetchResponse` and
//! `processShareAcknowledgeResponse` look at every row whose error is
//! `NOT_LEADER_OR_FOLLOWER` or `FENCED_LEADER_EPOCH`. They set the row's
//! `CurrentLeader` from the metadata cache and add the leader's node, on the
//! listener of the request, to `NodeEndpoints` once per node. The share
//! consumer moves the partition to that node without a `Metadata` round trip.

use crate::{codes, share_partition::manager::SharePartitionLeaderManager};

/// Whether a row with this error names the partition's current leader.
#[must_use]
pub(crate) fn names_the_leader(error_code: i16) -> bool {
    matches!(
        error_code,
        codes::NOT_LEADER_OR_FOLLOWER | codes::FENCED_LEADER_EPOCH
    )
}

/// The `(leader_id, leader_epoch)` hint of `(topic_id, partition)`.
#[must_use]
pub(crate) fn current_leader(
    manager: &SharePartitionLeaderManager,
    topic_id: uuid::Uuid,
    partition: i32,
) -> (i32, i32) {
    manager.current_leader_of(topic_id, partition)
}
