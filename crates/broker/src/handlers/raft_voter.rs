//! The prelude the three KIP-853 voter handlers share.
//!
//! `AddRaftVoter`, `RemoveRaftVoter` and `UpdateRaftVoter` each decode the
//! body, apply one cluster ACL gate, forward the untouched bytes to the active
//! controller from a broker-only observer, and answer
//! `NOT_LEADER_OR_FOLLOWER` on a node with no quorum state. Only the request
//! checks after that differ.

use std::{ops::ControlFlow, sync::Arc};

use bytes::Bytes;
use krabka_metadata::MetadataImage;
use krabka_protocol::{Decode, Encode};

use crate::{broker::Broker, error::BrokerError, handlers::RequestContext};

/// A cluster ACL gate: `true` when the authorizer denies `ctx`'s principal.
pub(super) type ClusterGate =
    fn(&dyn crate::authorizer::Authorizer, &MetadataImage, &RequestContext<'_>) -> bool;

/// The encoded, contextual entry point of each KIP-853 voter operation.
macro_rules! handler {
    ($broker:ident, $version:ident, $bytes:ident, $ctx:ident, $body:block) => {
        pub(crate) async fn handle(
            $broker: &crate::broker::Broker,
            $version: i16,
            $bytes: &[u8],
            $ctx: &crate::handlers::RequestContext<'_>,
        ) -> Result<bytes::Bytes, crate::error::BrokerError> $body
    };
}

pub(super) use handler;

#[cfg(test)]
macro_rules! test_dispatch {
    ($api:expr, $request:ty, $response:ty) => {
        crate::test_support::context_helper!(client_id = "admin-client");
        async fn answer(
            broker: &crate::broker::Broker,
            version: i16,
            request: &$request,
            ctx: &crate::handlers::RequestContext<'_>,
        ) -> $response {
            crate::test_support::dispatch_wire(broker, $api, version, request, ctx).await
        }
    };
}

#[cfg(test)]
pub(super) use test_dispatch;

/// What a voter handler runs its own checks on once the prelude lets the
/// request through.
pub(super) struct Admitted<R> {
    pub(super) req: R,
    pub(super) image: Arc<MetadataImage>,
    pub(super) quorum: krabka_raft::QuorumStateSnapshot,
}

/// The answers the prelude gives on its own: `denied` when `gate` refuses the
/// principal, and `not_leader` when this node holds no quorum state.
pub(super) struct Refusals<R> {
    pub(super) denied: R,
    pub(super) not_leader: R,
}

/// Decodes the request, applies `gate`, forwards the raw body as `api_key`
/// from a broker-only observer, and reads the quorum.
///
/// `Break` carries the encoded answer when the gate refuses, the controller
/// answered a forward, or this node is not the leader.
///
/// # Errors
///
/// Returns [`BrokerError`] when the body does not decode, the forward fails,
/// or a refusal does not encode.
pub(super) async fn prelude<Req: for<'a> Decode<'a>, Resp: Encode>(
    broker: &Broker,
    version: i16,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
    api_key: i16,
    gate: ClusterGate,
    refusals: Refusals<Resp>,
) -> Result<ControlFlow<Bytes, Admitted<Req>>, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = Req::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    if gate(broker.config.authorizer.as_ref(), &image, ctx) {
        return crate::handlers::encode_response(&refusals.denied, version).map(ControlFlow::Break);
    }

    // Broker-only observer forward to the active controller quorum (#392)
    if let Some(forwarded) = broker
        .controller
        .forward_raw(api_key, version, Bytes::copy_from_slice(req_bytes))
        .await
    {
        return forwarded.map(ControlFlow::Break).map_err(BrokerError::from);
    }

    // The request checks, their order and their codes are the controller
    // listener's own (`krabka_raft::voter_requests`).
    let Some(quorum) = broker.controller.quorum_snapshot() else {
        return crate::handlers::encode_response(&refusals.not_leader, version)
            .map(ControlFlow::Break);
    };
    Ok(ControlFlow::Continue(Admitted { req, image, quorum }))
}

/// Encodes a response that carries only `error_code` and `error_message`.
///
/// # Errors
///
/// Returns [`BrokerError`] when the response does not encode.
pub(super) fn respond<R: crate::handlers::ErrorResponse + Encode>(
    version: i16,
    error_code: i16,
    error_message: Option<String>,
) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(&R::error(error_code, error_message), version)
}
