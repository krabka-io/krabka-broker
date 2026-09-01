//! KIP-590 `Envelope` (`api_key` 58): the wire form a forwarding node uses to
//! carry a client's admin write to the active controller, and the pure
//! encode/decode halves of serving it.
//!
//! Everything here was settled against `mirror.gcr.io/apache/kafka:4.3.1`
//! rather than the wiki, by loading its own `kafka-clients-4.3.1.jar`:
//!
//! - `ApiKeys.ENVELOPE` is id 58, versions `0..=0`, flexible from 0, and
//!   `clusterAction = true`. Its `listeners` list is exactly `[CONTROLLER]`,
//!   so `Envelope` is advertised on the controller listener and on no other.
//!   `ApiKeys.clientApis()` and `ApiKeys.brokerApis()` both exclude it.
//! - `EnvelopeRequest` v0 is `{request_data: COMPACT_BYTES,
//!   request_principal: COMPACT_NULLABLE_BYTES, client_host_address:
//!   COMPACT_BYTES, _tagged_fields}`, and `EnvelopeResponse` v0 is
//!   `{response_data: COMPACT_NULLABLE_BYTES, error_code: INT16,
//!   _tagged_fields}`. Both `request_data` and `response_data` carry the
//!   embedded *header and* body, not a bare body.
//! - `DefaultKafkaPrincipalBuilder.serialize` is
//!   `MessageUtil.toVersionPrefixedBytes`: a big-endian `int16` schema
//!   version followed by the flexible `DefaultPrincipalData` body
//!   `{type: COMPACT_STRING, name: COMPACT_STRING, token_authenticated:
//!   BOOLEAN, _tagged_fields}`. `serialize(User:alice)` is exactly
//!   `00 00 05 55 73 65 72 06 61 6c 69 63 65 00 00`, and its `deserialize`
//!   refuses any version outside `0..=0`.
//! - `kafka.server.EnvelopeUtils` maps the failures: a request header it
//!   cannot parse is an `UnsupportedVersionException`, an inner api key whose
//!   `ApiKeys.forwardable` is false is an `InvalidRequestException`, and a
//!   principal it cannot deserialize is a `PrincipalDeserializationException`.
//!   It performs no principal-*type* check, so neither does this module.

use bytes::{BufMut as _, Bytes, BytesMut};
use krabka_protocol::{
    Decode as _, Encode as _, UnknownTaggedFields,
    owned::{
        default_principal_data::{self, DefaultPrincipalData},
        envelope_request::EnvelopeRequest,
        envelope_response::EnvelopeResponse,
    },
};

use crate::{
    codes,
    handlers::{ApiKeyCode, ApiVersion, CorrelationId},
};

/// A failure serving one `Envelope`, and the `EnvelopeResponse.error_code`
/// Kafka answers it with.
///
/// Every variant mirrors one throw site in `kafka.server.EnvelopeUtils`, so
/// the code a JVM forwarder sees is the code its own controller would send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnvelopeError {
    /// The outer principal lacks `ClusterAction` on the cluster resource.
    /// `ApiKeys.ENVELOPE.clusterAction` is true, so this is the gate Kafka
    /// applies before it looks at the payload at all.
    ClusterAuthorizationFailed,
    /// `request_principal` was absent, carried an unreadable schema version,
    /// or did not decode as a `DefaultPrincipalData` body.
    PrincipalDeserializationFailure,
    /// The embedded request header did not parse.
    UnsupportedVersion,
    /// The envelope itself was malformed, or the embedded request is not one
    /// Kafka forwards.
    InvalidRequest,
}

impl EnvelopeError {
    /// The `EnvelopeResponse.error_code` this failure is reported as.
    pub(crate) const fn code(self) -> i16 {
        match self {
            Self::ClusterAuthorizationFailed => codes::CLUSTER_AUTHORIZATION_FAILED,
            Self::PrincipalDeserializationFailure => codes::PRINCIPAL_DESERIALIZATION_FAILURE,
            Self::UnsupportedVersion => codes::UNSUPPORTED_VERSION,
            Self::InvalidRequest => codes::INVALID_REQUEST,
        }
    }
}

/// `ApiKeys.forwardable`, read out of `kafka-clients-4.3.1.jar`.
///
/// KIP-590 lets a broker wrap only these api keys, and
/// `EnvelopeUtils.handleEnvelopeRequest` refuses any other embedded key with
/// `InvalidRequestException`. Accepting a key outside the set would let a
/// forwarding hop launder a request the client could not have sent itself.
const FORWARDABLE_API_KEYS: &[ApiKeyCode] = &[
    19, // CreateTopics
    20, // DeleteTopics
    30, // CreateAcls
    31, // DeleteAcls
    33, // AlterConfigs
    37, // CreatePartitions
    38, // CreateDelegationToken
    39, // RenewDelegationToken
    40, // ExpireDelegationToken
    43, // ElectLeaders
    44, // IncrementalAlterConfigs
    45, // AlterPartitionReassignments
    46, // ListPartitionReassignments
    49, // AlterClientQuotas
    51, // AlterUserScramCredentials
    55, // DescribeQuorum
    57, // UpdateFeatures
    64, // UnregisterBroker
    67, // AllocateProducerIds
    80, // AddRaftVoter
    81, // RemoveRaftVoter
];

/// Whether Kafka wraps `api_key` in an `Envelope` when a broker forwards it.
pub(crate) fn is_forwardable(api_key: ApiKeyCode) -> bool {
    FORWARDABLE_API_KEYS.contains(&api_key)
}

/// The client identity a forwarder put in `request_principal`.
///
/// `KafkaPrincipal` carries a name and a token flag and nothing else — no
/// mechanism, no groups — so this is deliberately narrower than
/// [`krabka_security::Principal`]. The caller pairs it with the forwarding
/// hop's own authentication method to build the session identity the handler
/// runs under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForwardedPrincipal {
    pub(crate) name: String,
    pub(crate) token_authenticated: bool,
}

/// One embedded request, lifted out of an `EnvelopeRequest.request_data`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForwardedRequest {
    pub(crate) api_key: ApiKeyCode,
    pub(crate) api_version: ApiVersion,
    pub(crate) correlation_id: CorrelationId,
    pub(crate) client_id: Option<String>,
    pub(crate) body: Bytes,
    /// Whether the embedded *body* is flexible, which is also what decides
    /// the embedded response header version. See [`wrap_response`].
    pub(crate) body_flexible: bool,
}

/// Read a `request_principal` the way
/// `DefaultKafkaPrincipalBuilder.deserialize` does: a big-endian `int16`
/// schema version, then the flexible `DefaultPrincipalData` body at that
/// version.
///
/// The principal *type* is read but not enforced: Kafka's own `deserialize`
/// hands `DefaultPrincipalData.type` straight to the `KafkaPrincipal`
/// constructor without a check, and krabka authorizes on the name alone.
///
/// # Errors
/// [`EnvelopeError::PrincipalDeserializationFailure`] when the bytes are
/// absent, too short to hold the version prefix, carry a version outside
/// `0..=0`, or do not decode as a `DefaultPrincipalData` body. Kafka's
/// `deserialize` throws `SerializationException` in each of those cases and
/// `EnvelopeUtils` rewraps it as `PrincipalDeserializationException`.
pub(crate) fn deserialize_principal(
    bytes: Option<&[u8]>,
) -> Result<ForwardedPrincipal, EnvelopeError> {
    let mut cur = bytes.ok_or(EnvelopeError::PrincipalDeserializationFailure)?;
    let Some((prefix, rest)) = cur.split_at_checked(2) else {
        return Err(EnvelopeError::PrincipalDeserializationFailure);
    };
    let version = i16::from_be_bytes([prefix[0], prefix[1]]);
    cur = rest;
    if !(default_principal_data::MIN_VERSION..=default_principal_data::MAX_VERSION)
        .contains(&version)
    {
        return Err(EnvelopeError::PrincipalDeserializationFailure);
    }
    let data = DefaultPrincipalData::decode(&mut cur, version)
        .map_err(|_| EnvelopeError::PrincipalDeserializationFailure)?;
    Ok(ForwardedPrincipal {
        name: data.name,
        token_authenticated: data.token_authenticated,
    })
}

/// Read a `client_host_address` the way
/// `kafka.server.EnvelopeUtils.parseForwardedClientAddress` does:
/// `InetAddress.getByAddress(byte[])` over the raw address octets the
/// forwarding hop copied out of its own client's connection.
///
/// `getByAddress` accepts a 4-byte IPv4 address or a 16-byte IPv6 one and
/// nothing else, and it folds an IPv4-mapped `::ffff:a.b.c.d` back to the
/// `Inet4Address` it names — which is what [`std::net::IpAddr::to_canonical`]
/// does here, so a mapped address authorizes against the same host string
/// Kafka would use.
///
/// This address, not the forwarding broker's, is the host the embedded
/// request authorizes and audits against: `EnvelopeUtils` builds the inner
/// `RequestContext` with it, so a host ACL sees the client that actually
/// connected.
///
/// # Errors
/// [`EnvelopeError::InvalidRequest`] for any other length. Kafka's
/// `getByAddress` throws `UnknownHostException` there and `EnvelopeUtils`
/// rewraps it as `InvalidRequestException`.
pub(crate) fn deserialize_client_host_address(
    bytes: &[u8],
) -> Result<std::net::IpAddr, EnvelopeError> {
    if let Ok(octets) = <[u8; 4]>::try_from(bytes) {
        return Ok(std::net::IpAddr::from(octets));
    }
    if let Ok(octets) = <[u8; 16]>::try_from(bytes) {
        return Ok(std::net::IpAddr::from(octets).to_canonical());
    }
    Err(EnvelopeError::InvalidRequest)
}

/// Decode an `EnvelopeRequest` at `version`.
///
/// # Errors
/// [`EnvelopeError::InvalidRequest`] when the body is not a well-formed
/// `EnvelopeRequest`, which is how Kafka reports a malformed envelope.
pub(crate) fn decode_request(body: &[u8], version: i16) -> Result<EnvelopeRequest, EnvelopeError> {
    let mut cur = body;
    EnvelopeRequest::decode(&mut cur, version).map_err(|_| EnvelopeError::InvalidRequest)
}

/// Split an `EnvelopeRequest.request_data` into its embedded request header
/// and body.
///
/// `flexible_for` is the broker's own flexibility oracle for the embedded api
/// key and version, because the embedded header is v2 exactly when the
/// embedded body is flexible — the same rule the client-listener dispatch
/// loop applies.
///
/// # Errors
/// [`EnvelopeError::UnsupportedVersion`] when the header does not parse, and
/// [`EnvelopeError::InvalidRequest`] when the embedded api key is not one
/// Kafka forwards.
pub(crate) fn unwrap_request<F>(
    request_data: &Bytes,
    flexible_for: F,
) -> Result<ForwardedRequest, EnvelopeError>
where
    F: Fn(ApiKeyCode, ApiVersion) -> bool,
{
    let parsed = crate::network::request::parse_request(request_data, flexible_for)
        .map_err(|_| EnvelopeError::UnsupportedVersion)?;
    if !is_forwardable(parsed.api_key) {
        return Err(EnvelopeError::InvalidRequest);
    }
    Ok(ForwardedRequest {
        api_key: parsed.api_key,
        api_version: parsed.api_version,
        correlation_id: parsed.correlation_id,
        client_id: parsed.client_id.map(ToOwned::to_owned),
        body: request_data.slice_ref(parsed.body),
        body_flexible: parsed.body_flexible,
    })
}

/// Build the `EnvelopeResponse.response_data` for a served embedded request:
/// the embedded response header in front of the handler's body.
///
/// The embedded response header is v1 — that is, it carries a trailing
/// tagged-fields byte — exactly when the embedded *body* is flexible, which is
/// the same rule [`crate::network::response_header_v1`] applies on the client
/// listener. The correlation id is the client's own, because it rode inside
/// the forwarded header, so the bytes this returns are byte-identical to what
/// the client would have received had it reached the controller itself.
pub(crate) fn wrap_response(forwarded: &ForwardedRequest, body: &[u8]) -> Bytes {
    let header_v1 = crate::network::response_header_v1(forwarded.api_key, forwarded.body_flexible);
    let mut out = BytesMut::with_capacity(4 + usize::from(header_v1) + body.len());
    out.put_i32(forwarded.correlation_id);
    if header_v1 {
        out.put_u8(0);
    }
    out.put_slice(body);
    out.freeze()
}

/// Encode an `EnvelopeResponse` carrying a served `response_data`.
///
/// # Errors
/// Returns an error if the generated codec rejects the response.
pub(crate) fn encode_success(
    response_data: Bytes,
    version: i16,
) -> Result<Bytes, krabka_protocol::ProtocolError> {
    encode_response(
        &EnvelopeResponse {
            response_data: Some(response_data),
            error_code: codes::NONE,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
        version,
    )
}

/// Encode the `EnvelopeResponse` for a failure.
///
/// Kafka sends a null `response_data` alongside the error code, so a
/// forwarding broker has nothing to relay and surfaces the code itself.
///
/// # Errors
/// Returns an error if the generated codec rejects the response.
pub(crate) fn encode_failure(
    error: EnvelopeError,
    version: i16,
) -> Result<Bytes, krabka_protocol::ProtocolError> {
    encode_response(
        &EnvelopeResponse {
            response_data: None,
            error_code: error.code(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
        version,
    )
}

fn encode_response(
    response: &EnvelopeResponse,
    version: i16,
) -> Result<Bytes, krabka_protocol::ProtocolError> {
    let mut buf = BytesMut::with_capacity(response.encoded_len(version));
    response.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// `DefaultKafkaPrincipalBuilder.serialize(new KafkaPrincipal("User",
    /// "alice"))` in `mirror.gcr.io/apache/kafka:4.3.1`, captured verbatim:
    /// int16 version 0, compact string "User", compact string "alice",
    /// `token_authenticated = false`, empty tagged fields.
    const JVM_USER_ALICE: &[u8] = &[
        0x00, 0x00, 0x05, b'U', b's', b'e', b'r', 0x06, b'a', b'l', b'i', b'c', b'e', 0x00, 0x00,
    ];
    /// The same call for `new KafkaPrincipal("User", "bob", true)`, whose
    /// `token_authenticated` byte is `0x01`.
    const JVM_USER_BOB_TOKEN: &[u8] = &[
        0x00, 0x00, 0x05, b'U', b's', b'e', b'r', 0x04, b'b', b'o', b'b', 0x01, 0x00,
    ];

    fn forwarded_principal(name: &str, token_authenticated: bool) -> ForwardedPrincipal {
        ForwardedPrincipal {
            name: name.to_owned(),
            token_authenticated,
        }
    }

    /// The bytes a real JVM forwarder puts in `request_principal` decode into
    /// the identity that JVM meant. These buffers are the oracle: they were
    /// produced by `DefaultKafkaPrincipalBuilder` in the pinned image, not by
    /// this module, so a field-order or length-prefix slip fails here.
    #[test]
    fn a_jvm_serialized_principal_decodes_to_the_identity_it_names() {
        let cases = [
            (
                "User:alice, not token authenticated",
                JVM_USER_ALICE,
                forwarded_principal("alice", false),
            ),
            (
                "User:bob, token authenticated",
                JVM_USER_BOB_TOKEN,
                forwarded_principal("bob", true),
            ),
        ];

        for (case, bytes, want) in cases {
            check!(
                deserialize_principal(Some(bytes)) == Ok(want),
                "case: {case}"
            );
        }
    }

    /// Every shape Kafka's `deserialize` refuses. A missing principal is
    /// included because `request_principal` is nullable on the wire: KIP-590
    /// always populates it, and a null one leaves the controller with no
    /// identity to authorize the embedded request against.
    #[test]
    fn a_principal_kafka_would_refuse_is_a_deserialization_failure() {
        let valid = JVM_USER_ALICE;
        let mut wrong_version = valid.to_vec();
        wrong_version[1] = 1;
        let mut truncated_body = valid.to_vec();
        truncated_body.truncate(4);

        let cases: [(&str, Option<Vec<u8>>); 5] = [
            ("null principal", None),
            ("empty principal", Some(Vec::new())),
            ("version prefix only", Some(vec![0x00])),
            (
                "schema version above the supported range",
                Some(wrong_version),
            ),
            ("body truncated mid-string", Some(truncated_body)),
        ];

        for (case, bytes) in cases {
            check!(
                deserialize_principal(bytes.as_deref())
                    == Err(EnvelopeError::PrincipalDeserializationFailure),
                "case: {case}"
            );
        }
    }

    /// The `EnvelopeResponse.error_code` for each failure, as
    /// `kafka.server.EnvelopeUtils` assigns them. A substitution here is
    /// invisible to a Rust caller and changes what a JVM forwarder reports to
    /// its own client.
    #[test]
    fn each_failure_carries_the_error_code_kafka_assigns_it() {
        let cases = [
            (EnvelopeError::ClusterAuthorizationFailed, 31),
            (EnvelopeError::PrincipalDeserializationFailure, 97),
            (EnvelopeError::UnsupportedVersion, 35),
            (EnvelopeError::InvalidRequest, 42),
        ];

        for (error, want) in cases {
            check!(error.code() == want, "{error:?}");
        }
    }

    /// `client_host_address` is exactly what `InetAddress.getByAddress`
    /// accepts: four octets or sixteen, with an IPv4-mapped sixteen folded
    /// back to the IPv4 address it names. Every other length is the
    /// `UnknownHostException` `EnvelopeUtils` reports as an invalid request.
    #[test]
    fn a_client_host_address_decodes_the_way_inet_address_get_by_address_does() {
        let mapped_v4 = {
            let mut octets = [0u8; 16];
            octets[10] = 0xff;
            octets[11] = 0xff;
            octets[12..].copy_from_slice(&[10, 1, 2, 3]);
            octets
        };
        let cases: [(&str, &[u8], Result<std::net::IpAddr, EnvelopeError>); 6] = [
            ("four octets are an IPv4 address", &[10, 1, 2, 3], {
                Ok(std::net::IpAddr::from([10, 1, 2, 3]))
            }),
            (
                "sixteen octets are an IPv6 address",
                &[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                Ok("2001:db8::1".parse().expect("literal IPv6 address")),
            ),
            (
                "an IPv4-mapped IPv6 address folds to its IPv4 form",
                &mapped_v4,
                Ok(std::net::IpAddr::from([10, 1, 2, 3])),
            ),
            ("an empty address", &[], Err(EnvelopeError::InvalidRequest)),
            (
                "three octets",
                &[10, 1, 2],
                Err(EnvelopeError::InvalidRequest),
            ),
            (
                "five octets",
                &[10, 1, 2, 3, 4],
                Err(EnvelopeError::InvalidRequest),
            ),
        ];

        for (case, bytes, want) in cases {
            check!(
                deserialize_client_host_address(bytes) == want,
                "case: {case}"
            );
        }
    }

    /// The forwardable set is `ApiKeys.forwardable` from
    /// `kafka-clients-4.3.1.jar`. The five write APIs issue #82 names are in
    /// it, and so is `UpdateFeatures`, which `kafka-features.sh` drives.
    #[test]
    fn the_forwardable_set_is_the_one_kafka_publishes() {
        let want: std::collections::BTreeSet<i16> = maplit::btreeset! {
            19, 20, 30, 31, 33, 37, 38, 39, 40, 43, 44, 45, 46, 49, 51, 55, 57, 64, 67, 80, 81,
        };
        let declared: std::collections::BTreeSet<i16> =
            FORWARDABLE_API_KEYS.iter().copied().collect();

        check!(declared == want);
        // Envelope itself is not forwardable, so an envelope cannot nest.
        check!(!is_forwardable(58));
        // Produce is a data-plane api the controller never serves.
        check!(!is_forwardable(0));
    }

    fn request_frame(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<&str>,
        flexible: bool,
        body: &[u8],
    ) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_i16(api_key);
        buf.put_i16(api_version);
        buf.put_i32(correlation_id);
        match client_id {
            Some(id) => {
                buf.put_i16(i16::try_from(id.len()).expect("client id length"));
                buf.put_slice(id.as_bytes());
            }
            None => buf.put_i16(-1),
        }
        if flexible {
            buf.put_u8(0);
        }
        buf.put_slice(body);
        buf.freeze()
    }

    /// The embedded header is split off whole, and the body that remains is
    /// the handler's input. The flexible case also proves the tagged-fields
    /// byte is consumed rather than left at the head of the body.
    #[test]
    fn an_embedded_request_is_split_into_its_header_and_body() {
        let cases = [
            (
                "flexible IncrementalAlterConfigs",
                request_frame(44, 1, 77, Some("adminclient-1"), true, b"body"),
                true,
                ForwardedRequest {
                    api_key: 44,
                    api_version: 1,
                    correlation_id: 77,
                    client_id: Some("adminclient-1".to_owned()),
                    body: Bytes::from_static(b"body"),
                    body_flexible: true,
                },
            ),
            (
                "non-flexible AlterConfigs with a null client id",
                request_frame(33, 0, 3, None, false, b"body"),
                false,
                ForwardedRequest {
                    api_key: 33,
                    api_version: 0,
                    correlation_id: 3,
                    client_id: None,
                    body: Bytes::from_static(b"body"),
                    body_flexible: false,
                },
            ),
        ];

        for (case, frame, flexible, want) in cases {
            assert!(let Ok(forwarded) = unwrap_request(&frame, |_, _| flexible),
                "case: {case}"
            );

            check!(forwarded == want, "case: {case}");
        }
    }

    /// An embedded key outside `ApiKeys.forwardable` is refused before any
    /// handler runs, and a frame too short to hold a header is refused as an
    /// unparseable header.
    #[test]
    fn an_embedded_request_kafka_would_not_forward_is_refused() {
        let cases: [(&str, Bytes, EnvelopeError); 3] = [
            (
                "Produce is not forwardable",
                request_frame(0, 9, 1, None, true, b""),
                EnvelopeError::InvalidRequest,
            ),
            (
                "a nested Envelope is not forwardable",
                request_frame(58, 0, 1, None, true, b""),
                EnvelopeError::InvalidRequest,
            ),
            (
                "a frame too short to hold a header",
                Bytes::from_static(&[0, 19, 0]),
                EnvelopeError::UnsupportedVersion,
            ),
        ];

        for (case, frame, want) in cases {
            check!(
                unwrap_request(&frame, |_, _| true) == Err(want),
                "case: {case}"
            );
        }
    }

    /// `response_data` is the embedded response header followed by the
    /// handler's body, and the header is v1 exactly when the embedded body is
    /// flexible. The correlation id is the *client's*, carried in from the
    /// forwarded header, so the forwarder can relay these bytes untouched.
    #[test]
    fn a_served_response_is_wrapped_with_the_clients_own_response_header() {
        let forwarded = |api_key: i16, body_flexible: bool| ForwardedRequest {
            api_key,
            api_version: 0,
            correlation_id: 0x0102_0304,
            client_id: None,
            body: Bytes::new(),
            body_flexible,
        };
        let cases = [
            (
                "flexible body takes a v1 header",
                forwarded(44, true),
                vec![0x01, 0x02, 0x03, 0x04, 0x00, b'x'],
            ),
            (
                "non-flexible body takes a v0 header",
                forwarded(33, false),
                vec![0x01, 0x02, 0x03, 0x04, b'x'],
            ),
        ];

        for (case, forwarded, want) in cases {
            check!(
                wrap_response(&forwarded, b"x").as_ref() == want.as_slice(),
                "case: {case}"
            );
        }
    }

    /// A success and a failure round-trip through the generated v0 codec as
    /// whole structs, so a field-order or nullability slip shows up here
    /// rather than at a JVM forwarder.
    #[test]
    fn an_envelope_response_round_trips_through_the_v0_codec() {
        let cases = [
            (
                "served",
                encode_success(Bytes::from_static(b"payload"), 0).expect("encode"),
                EnvelopeResponse {
                    response_data: Some(Bytes::from_static(b"payload")),
                    error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ),
            (
                "refused",
                encode_failure(EnvelopeError::ClusterAuthorizationFailed, 0).expect("encode"),
                EnvelopeResponse {
                    response_data: None,
                    error_code: 31,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                },
            ),
        ];

        for (case, encoded, want) in cases {
            let mut cur = encoded.as_ref();
            assert!(let Ok(decoded) = EnvelopeResponse::decode(&mut cur, 0),
                "case: {case}"
            );

            check!(decoded == want, "case: {case}");
            check!(cur.is_empty(), "case: {case}");
        }
    }

    /// An `EnvelopeRequest` decodes back into the same struct, and a body that
    /// is not an envelope at all is refused rather than mistaken for one.
    #[test]
    fn an_envelope_request_round_trips_and_a_malformed_one_is_refused() {
        let request = EnvelopeRequest {
            request_data: request_frame(19, 7, 5, Some("c"), true, b"topics"),
            request_principal: Some(Bytes::from_static(JVM_USER_ALICE)),
            client_host_address: Bytes::from_static(&[127, 0, 0, 1]),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let mut encoded = BytesMut::new();
        request.encode(&mut encoded, 0).expect("encode");

        check!(decode_request(&encoded, 0) == Ok(request));
        // A single byte cannot hold three length-prefixed fields.
        check!(decode_request(&[0x00], 0) == Err(EnvelopeError::InvalidRequest));
    }
}
