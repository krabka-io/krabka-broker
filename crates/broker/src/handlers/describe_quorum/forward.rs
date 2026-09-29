//! Wraps a `DescribeQuorum` request in a KIP-590 `Envelope` before it is
//! forwarded to the active controller, and unwraps the leader's
//! `EnvelopeResponse` back into the plain `DescribeQuorumResponse` body a
//! caller expects.
//!
//! [`krabka_raft::ControllerHandle::forward_raw`] dials the leader's
//! controller listener as THIS node's own inter-broker service identity, not
//! the caller's. Forwarding the bare `DescribeQuorumRequest` bytes at
//! `api_key=55` would have the leader's controller listener re-authorize
//! `Describe` against that service identity instead of the caller who
//! already passed [`super::authz::cluster_describe_denied`] on this node --
//! fencing out a legitimately-authorized caller for the sole reason that it
//! happened to land on a follower (review of #1034). Sending an `Envelope`
//! (`api_key=58`, KIP-590) instead carries the caller's own principal and
//! address across the hop, so the leader's `controller_admin::serve_envelope`
//! reconstructs a `RequestContext` under the identity that actually asked,
//! and [`super::handle`] re-runs the same `Describe` gate correctly on the
//! leader.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        describe_quorum_request::API_KEY as DESCRIBE_QUORUM_API_KEY,
        envelope_request,
        envelope_response::{self, EnvelopeResponse},
    },
};

use crate::{
    broker::Broker,
    codes,
    envelope::{self, ForwardedPrincipal, ForwardedRequest},
    error::BrokerError,
    handlers::RequestContext,
};

/// `Envelope`'s api key (58, KIP-590): what this module actually sends on the
/// wire, in place of the bare `DescribeQuorum` api key (55).
pub(super) const ENVELOPE_API_KEY: i16 = envelope_request::API_KEY;

/// `Envelope`'s one wire version.
pub(super) const ENVELOPE_VERSION: i16 = envelope_request::MIN_VERSION;

/// Kafka's `EnvelopeUtils`'s `CLUSTER_AUTHORIZATION_FAILED`, which the
/// receiving side's `unwrap_envelope` answers with when the OUTER (forwarding
/// hop's own) principal lacks `ClusterAction`. Every other `EnvelopeResponse`
/// error code is a protocol-shape failure this module does not expect to
/// produce, so it is treated the same as a forwarding failure below.
const ENVELOPE_CLUSTER_AUTHORIZATION_FAILED: i16 = codes::CLUSTER_AUTHORIZATION_FAILED;

/// Build the `EnvelopeRequest` wire bytes for forwarding `req_bytes` (the
/// already-decoded-shape `DescribeQuorumRequest` body at `version`) to the
/// active controller, carrying `ctx`'s principal and peer address the way a
/// JVM broker's own `KafkaApis.forwardToController` would.
///
/// # Errors
/// Returns an error if the generated codec rejects the assembled envelope.
pub(super) fn build(
    broker: &Broker,
    req_bytes: &[u8],
    version: i16,
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    // The embedded `RequestHeader` carries a correlation id local to this one
    // hop. Only the envelope response echoes it back, and this module strips
    // that header before it returns.
    let request_data = envelope::wrap_request(&ForwardedRequest {
        api_key: DESCRIBE_QUORUM_API_KEY,
        api_version: version,
        correlation_id: 0,
        client_id: ctx.client_id.map(ToOwned::to_owned),
        body: Bytes::copy_from_slice(req_bytes),
        body_flexible: broker
            .handlers()
            .body_flexible(DESCRIBE_QUORUM_API_KEY, version),
    });
    // Krabka's own `envelope::deserialize_principal` reads the principal back
    // on the receiving side and authorizes on `name` alone.
    let principal = ForwardedPrincipal {
        name: ctx.principal.name.clone(),
        token_authenticated: false,
    };
    let request = envelope::envelope_request(request_data, &principal, ctx.peer.ip())?;
    let mut out = BytesMut::with_capacity(request.encoded_len(ENVELOPE_VERSION));
    request.encode(&mut out, ENVELOPE_VERSION)?;
    Ok(out.freeze())
}

/// Decode the leader's `EnvelopeResponse` and return the plain
/// `DescribeQuorumResponse` body it wrapped, exactly as if this node had
/// answered locally.
///
/// # Errors
/// Returns an error if the bytes do not decode as an `EnvelopeResponse`, or
/// as an encoded `DescribeQuorumResponse` error frame if the envelope itself
/// was refused or malformed.
pub(super) fn unwrap_response(
    broker: &Broker,
    envelope_bytes: &[u8],
    version: i16,
) -> Result<Bytes, BrokerError> {
    let mut cur = envelope_bytes;
    let resp = EnvelopeResponse::decode(&mut cur, envelope_response::MIN_VERSION)?;

    if resp.error_code != codes::NONE {
        // The leader refused to open the envelope, or something about its
        // shape did not parse there. `ClusterAuthorizationFailed` is a real,
        // reportable answer (the forwarding hop's own service identity lacks
        // `ClusterAction`, a cluster misconfiguration this caller cannot fix
        // by retrying); every other code here is a protocol-shape failure
        // this module does not expect to produce, so it is folded into the
        // same transient answer as an unresolved leader.
        let error_code = if resp.error_code == ENVELOPE_CLUSTER_AUTHORIZATION_FAILED {
            codes::CLUSTER_AUTHORIZATION_FAILED
        } else {
            codes::NOT_LEADER_OR_FOLLOWER
        };
        return crate::handlers::encode_response(
            &super::top_level_error_response(error_code),
            version,
        );
    }

    // A served envelope's `response_data` is the embedded `ResponseHeader`
    // in front of the handler's own body (`envelope::wrap_response`). Strip
    // that header off to get the same bytes a local answer would have
    // produced. The correlation id is local to this hop, so it is not checked.
    let flexible = broker
        .handlers()
        .body_flexible(DESCRIBE_QUORUM_API_KEY, version);
    let data = resp.response_data.unwrap_or_default();
    let (_, body) = envelope::unwrap_response(DESCRIBE_QUORUM_API_KEY, flexible, &data)?;
    Ok(body)
}
