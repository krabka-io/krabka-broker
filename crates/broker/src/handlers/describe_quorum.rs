//! `DescribeQuorum` (`api_key=55`, KIP-595). It returns the raft-quorum state
//! for the cluster-metadata topic.
//!
//! Krabka's `KRaft` setup runs one raft log, the controller quorum that
//! `controller_quorum_voters` configures, and applies committed records to
//! `MetadataImage`. Clients, such as the JVM `kafka-metadata-quorum
//! --describe` admin tool, ask for `__cluster_metadata` partition 0. The
//! broker answers from [`krabka_raft::ControllerHandle::quorum_state`]:
//!
//! - `leader_id` is `current_leader`. It is `-1` when the leader is unknown,
//!   for example during an election.
//! - `leader_epoch` is `current_term`, capped at `i32::MAX`.
//! - `high_watermark` is `last_applied_index` on this node's state machine,
//!   capped at `i64::MAX`.
//! - `current_voters` is openraft's voter set. Each voter's `log_end_offset`
//!   is openraft's `replication.matched.index`. openraft fills the per-voter
//!   replication map only on the leader, so on a follower every voter falls
//!   back to the JVM `-1` "Unknown" sentinel. Callers are meant to route
//!   `kafka-metadata-quorum --describe` to the leader.
//! - `observers` is empty, because Krabka has no observer role yet.
//!
//! For any topic OTHER than `__cluster_metadata`, the per-partition row gets
//! `INVALID_TOPIC_EXCEPTION` (17). That matches the JVM behavior on a
//! non-metadata topic.
//!
//! The authorization gate lives in `authz`, the per-partition rows in
//! `topics`, and the KIP-853 `Nodes` block in `nodes`. This file holds only
//! the wire entry point that stitches them together.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        describe_quorum_request::DescribeQuorumRequest,
        describe_quorum_response::DescribeQuorumResponse,
    },
};

mod authz;
mod nodes;
mod topics;

use self::{authz::cluster_describe_denied, nodes::build_nodes, topics::build_topic_responses};
use crate::{broker::Broker, codes, error::BrokerError};

#[tracing::instrument(
    name = "handle_describe_quorum",
    level = "info",
    skip_all,
    fields(api = "DescribeQuorum", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let image = broker.controller.current_image();

    // Whole-request Cluster Describe gate. DescribeQuorum is
    // cluster-wide raft introspection — same gate as DescribeCluster.
    if cluster_describe_denied(broker, &image, ctx) {
        let resp = DescribeQuorumResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        };
        return crate::handlers::encode_response(&resp, version);
    }

    // Broker-only observer forward to the active controller quorum (#392)
    if let Some(forwarded) = broker
        .controller
        .forward_raw(55, version, Bytes::copy_from_slice(req_bytes))
        .await
    {
        return forwarded.map_err(BrokerError::from);
    }

    let mut cur: &[u8] = req_bytes;
    let req = DescribeQuorumRequest::decode(&mut cur, version)?;

    // Snapshot raft state once — cheap clone of openraft's metrics
    // watch value. Carries the live current_term, last_applied_index,
    // and per-voter matched-log indexes (the last one populated only
    // when this node is the leader).
    let quorum = broker.controller.quorum_state();

    let topics = build_topic_responses(&req.topics, &quorum);

    // KIP-853 (v2+) adds a top-level `Nodes` block carrying each voter's
    // directory id + listeners. Encoding skips it on v0/v1 (the fields are
    // gated `versions: "2+"`), so populating it unconditionally stays
    // byte-exact for older clients.
    let nodes = build_nodes(&quorum);

    let resp = DescribeQuorumResponse {
        error_code: codes::NONE,
        topics,
        nodes,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}
