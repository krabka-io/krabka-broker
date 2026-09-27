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

use krabka_metadata::MetadataImage;

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

/// One `NodeEndpoints` entry, in the shape both responses share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeaderEndpoint {
    pub(crate) node_id: i32,
    pub(crate) host: String,
    pub(crate) port: i32,
    pub(crate) rack: Option<String>,
}

/// The endpoint of every node in `leader_ids`, once per node and ordered by
/// node id.
///
/// Each node resolves on `connection_listener_name`, the listener the request
/// arrived on, through the same selection that `Metadata` uses. A negative id
/// and a node that the image does not know contribute nothing, as Kafka's
/// `getAliveBrokerNode` finds no node for them.
#[must_use]
pub(crate) fn leader_endpoints(
    image: &MetadataImage,
    connection_listener_name: &str,
    inter_broker_listener_name: &str,
    leader_ids: impl IntoIterator<Item = i32>,
) -> Vec<LeaderEndpoint> {
    let mut node_ids: Vec<i32> = leader_ids.into_iter().filter(|id| *id >= 0).collect();
    node_ids.sort_unstable();
    node_ids.dedup();
    node_ids
        .into_iter()
        .filter_map(|node_id| {
            let record = image.broker(krabka_metadata::NodeId(u64::try_from(node_id).ok()?))?;
            let (host, port) = crate::handlers::metadata::pick_endpoint_host_port(
                record,
                connection_listener_name,
                inter_broker_listener_name,
            );
            Some(LeaderEndpoint {
                node_id,
                host,
                port,
                rack: record.rack.clone(),
            })
        })
        .collect()
}
