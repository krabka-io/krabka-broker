//! Client-side drivers for the streams (`StreamsGroupHeartbeat`) side of the
//! upgrade scenarios.
//!
//! Both scenarios send a streams heartbeat at the group id a classic group
//! already owns, so the topology builder and the bounded convergence loop are
//! kept here apart from the classic-side `JoinGroup` drivers.

use krabka_client_core::Client;
use krabka_protocol::owned::{
    streams_group_heartbeat_request::Topology,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};

pub use crate::support::streams::first_join;

pub fn topology(source_topic: &str) -> Topology {
    crate::support::streams::topology(source_topic, vec![])
}

/// Drive a single streams member to convergence (at least `want_active`
/// active-task partitions). Returns `(member_id, last_response)`.
pub async fn streams_join_and_converge(
    client: &Client,
    group: &str,
    topo: Topology,
    want_active: usize,
    tries: usize,
) -> (String, StreamsGroupHeartbeatResponse) {
    crate::support::streams::streams_join_and_converge(
        client,
        group,
        topo,
        want_active,
        tries,
        false,
    )
    .await
}
