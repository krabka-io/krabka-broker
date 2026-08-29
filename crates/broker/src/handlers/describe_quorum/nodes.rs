//! Building the KIP-853 top-level `Nodes` block of a `DescribeQuorum`
//! response.
//!
//! The block names every voter and the listeners it advertises, and it exists
//! only from version 2 onward. It reads from a different part of
//! [`krabka_raft::QuorumState`] than the per-partition rows do, so it lives
//! apart from them.

use krabka_protocol::owned::describe_quorum_response::{Listener, Node};
use krabka_raft::QuorumState;

/// Builds the KIP-853 (v2+) `Nodes` block: one entry per voter, with the
/// listeners of that voter's directory id.
///
/// The data comes from `quorum.voter_nodes`, which the raft layer fills from
/// the replicated membership config. Only the leader knows that config in
/// full, and a follower can carry a partial map. The encoder drops this whole
/// field on v0 and v1, so building it every time is harmless.
pub(super) fn build_nodes(quorum: &QuorumState) -> Vec<Node> {
    quorum
        .voter_nodes
        .iter()
        .map(|(&id, node)| Node {
            node_id: i32::try_from(id.0).unwrap_or(-1),
            listeners: node
                .endpoints
                .iter()
                .map(|e| Listener {
                    name: e.name.clone(),
                    host: e.host.clone(),
                    port: e.port,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}
