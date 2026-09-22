//! `DescribeQuorum` (`api_key=55`, KIP-595). It returns the raft-quorum state
//! for the cluster-metadata topic.
//!
//! Krabka's `KRaft` setup runs one raft log, the controller quorum that
//! `controller_quorum_voters` configures, and applies committed records to
//! `MetadataImage`. Clients, such as the JVM `kafka-metadata-quorum
//! --describe` admin tool, ask for `__cluster_metadata` partition 0.
//!
//! Kafka's `KafkaApis` forwards `DescribeQuorum` from the broker listener to
//! the active controller unconditionally (`forwardToController`), so this
//! handler does the same: [`krabka_raft::ControllerHandle::forward_raw`]
//! (reached through [`crate::metadata_source::MetadataSource::forward_raw`])
//! sends the request on to the active controller whenever this node itself
//! is not the leader, whether it is a broker-only observer or a
//! combined/controller node. It goes wrapped in a KIP-590 `Envelope`
//! (`forward` submodule) carrying THIS caller's own principal and address,
//! not this node's inter-broker identity, so the leader authorizes and
//! audits the caller that actually asked (review of #1034). A node that IS
//! the active controller answers locally, from
//! [`krabka_raft::ControllerHandle::quorum_snapshot`], with the same
//! [`krabka_raft::describe_quorum`] builder the controller listener uses for
//! a request that arrives there directly (#814, #1034) -- one
//! implementation on both listeners.
//!
//! The authorization gate lives in `authz`, the envelope wrap/unwrap in
//! `forward`. This file holds the wire entry point: the gate, the forward,
//! and the local answer.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        describe_quorum_request::DescribeQuorumRequest,
        describe_quorum_response::DescribeQuorumResponse,
    },
};

mod authz;
mod forward;

use self::authz::cluster_describe_denied;
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

    // Forward to the active controller whenever this node is not it (#392,
    // #1034): a broker-only observer always forwards; a combined/controller
    // node forwards only while it is not the leader. Wrapped in a KIP-590
    // `Envelope` carrying this caller's own identity (`forward::build`), not
    // this node's inter-broker one -- see the module doc and `forward`'s.
    let envelope_body = forward::build(broker, req_bytes, version, ctx)?;
    if let Some(forwarded) = broker
        .controller
        .forward_raw(
            forward::ENVELOPE_API_KEY,
            forward::ENVELOPE_VERSION,
            envelope_body,
        )
        .await
    {
        return match forwarded {
            Ok(envelope_response) => forward::unwrap_response(broker, &envelope_response, version),
            // No leader known yet (startup or an election in progress), an
            // unresolvable voter address, or the dial/round-trip itself
            // failed: a Kafka client expects a typed
            // `NOT_LEADER_OR_FOLLOWER` for this transient case, not a
            // dropped connection (review of #1034) -- the generic registry
            // dispatch loop has no response shape to build for a bare
            // `Err(BrokerError)` here and just closes the connection.
            Err(_raft_error) => crate::handlers::encode_response(
                &DescribeQuorumResponse {
                    error_code: codes::NOT_LEADER_OR_FOLLOWER,
                    ..Default::default()
                },
                version,
            ),
        };
    }

    let mut cur: &[u8] = req_bytes;
    let req = DescribeQuorumRequest::decode(&mut cur, version)?;

    // Reaching here means `forward_raw` answered `None`: this node holds a
    // quorum snapshot and is the active controller, the only case a
    // `MetadataSource` implementer declines to forward on.
    let Some(quorum) = broker.controller.quorum_snapshot() else {
        let resp = DescribeQuorumResponse {
            error_code: codes::NOT_LEADER_OR_FOLLOWER,
            ..Default::default()
        };
        return crate::handlers::encode_response(&resp, version);
    };

    let resp = krabka_raft::describe_quorum(&req, &quorum);
    crate::handlers::encode_response(&resp, version)
}
