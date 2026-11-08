//! The `CurrentLeader` hint and the KIP-951 `NodeEndpoints` that
//! `ShareFetch` and `ShareAcknowledge` send for a partition row that another
//! broker leads.
//!
//! Kafka's `KafkaApis.processShareFetchResponse` and
//! `processShareAcknowledgeResponse` look at every row whose error is
//! `NOT_LEADER_OR_FOLLOWER` or `FENCED_LEADER_EPOCH`. They set the row's
//! `CurrentLeader` from the metadata cache and add the leader's node, on the
//! listener of the request, to `NodeEndpoints` once per node. The hint itself
//! comes from `SharePartitionLeaderManager::current_leader_of`. The share
//! consumer moves the partition to that node without a `Metadata` round trip.

use crate::codes;

/// Whether a row with this error names the partition's current leader.
#[must_use]
pub fn names_the_leader(error_code: i16) -> bool {
    matches!(
        error_code,
        codes::NOT_LEADER_OR_FOLLOWER | codes::FENCED_LEADER_EPOCH
    )
}
