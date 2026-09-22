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

use bytes::{Buf as _, BufMut as _, Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        default_principal_data::DefaultPrincipalData,
        describe_quorum_request::API_KEY as DESCRIBE_QUORUM_API_KEY,
        describe_quorum_response::DescribeQuorumResponse,
        envelope_request::{self, EnvelopeRequest},
        envelope_response::{self, EnvelopeResponse},
    },
    primitives::string_bytes::put_nullable_string,
};

use crate::{broker::Broker, codes, error::BrokerError, handlers::RequestContext};

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
    let flexible = broker
        .handlers()
        .body_flexible(DESCRIBE_QUORUM_API_KEY, version);

    // The embedded `RequestHeader`: `api_key`, `api_version`, a correlation
    // id local to this one hop (only ever echoed back inside the envelope
    // response, which this module strips before returning), `client_id`,
    // and -- exactly when the embedded body is flexible, which `DescribeQuorum`
    // always is -- a trailing empty tagged-fields byte.
    let mut header = BytesMut::new();
    header.put_i16(DESCRIBE_QUORUM_API_KEY);
    header.put_i16(version);
    header.put_i32(0);
    put_nullable_string(&mut header, Some(ctx.client_id));
    if flexible {
        header.put_u8(0);
    }
    header.extend_from_slice(req_bytes);

    // `DefaultKafkaPrincipalBuilder.serialize`: a big-endian i16 schema
    // version, then the flexible `DefaultPrincipalData` body. Krabka's own
    // `envelope::deserialize_principal` reads this back on the receiving
    // side and authorizes on `name` alone, so `type_` is set to Kafka's own
    // "User" constant without needing to match `ctx.principal.auth_method`.
    let mut principal_data = BytesMut::new();
    DefaultPrincipalData {
        type_: "User".to_string(),
        name: ctx.principal.name.clone(),
        token_authenticated: false,
        ..Default::default()
    }
    .encode(&mut principal_data, 0)?;
    let mut principal = BytesMut::with_capacity(2 + principal_data.len());
    principal.put_i16(0);
    principal.extend_from_slice(&principal_data);

    let mut out = BytesMut::new();
    EnvelopeRequest {
        request_data: header.freeze(),
        request_principal: Some(principal.freeze()),
        client_host_address: Bytes::copy_from_slice(&peer_address_octets(ctx.peer.ip())),
        ..Default::default()
    }
    .encode(&mut out, ENVELOPE_VERSION)?;
    Ok(out.freeze())
}

/// `InetAddress.getAddress()`: four octets for an IPv4 host, sixteen for an
/// IPv6 one. This is the SEND side of what
/// `envelope::deserialize_client_host_address` reads back.
fn peer_address_octets(ip: std::net::IpAddr) -> Vec<u8> {
    match ip {
        std::net::IpAddr::V4(v4) => v4.octets().to_vec(),
        std::net::IpAddr::V6(v6) => v6.octets().to_vec(),
    }
}

/// Decode the leader's `EnvelopeResponse` and return the plain
/// `DescribeQuorumResponse` body it wrapped, exactly as if this node had
/// answered locally.
///
/// # Errors
/// Returns an error if the bytes do not decode as an `EnvelopeResponse`, or
/// as an encoded [`DescribeQuorumResponse`] error frame if the envelope
/// itself was refused or malformed.
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
            &DescribeQuorumResponse {
                error_code,
                ..Default::default()
            },
            version,
        );
    }

    // A served envelope's `response_data` is the embedded `ResponseHeader`
    // (the correlation id this module put in the request, plus a trailing
    // tagged-fields byte exactly when the embedded body is flexible) in
    // front of the handler's own body (`envelope::wrap_response`, the
    // encode-side mirror of this). Strip that header off to get the same
    // bytes a local answer would have produced.
    let flexible = broker
        .handlers()
        .body_flexible(DESCRIBE_QUORUM_API_KEY, version);
    let header_len = crate::network::response_header_len(DESCRIBE_QUORUM_API_KEY, flexible);
    let mut data = resp.response_data.unwrap_or_default();
    if data.remaining() < header_len {
        return Err(BrokerError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue(
                "forwarded DescribeQuorum envelope response shorter than its embedded header",
            ),
        ));
    }
    data.advance(header_len);
    Ok(data)
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};

    use assert2::check;

    use super::*;

    #[test]
    fn peer_address_octets_matches_inet_address_get_address() {
        check!(
            peer_address_octets(std::net::IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)))
                == vec![10, 1, 2, 3]
        );
        check!(
            peer_address_octets(std::net::IpAddr::V6(Ipv6Addr::LOCALHOST))
                == vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
        );
    }
}
