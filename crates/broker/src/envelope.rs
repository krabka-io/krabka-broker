//! KIP-590 `Envelope` (`api_key` 58): the wire form a forwarding node uses to
//! carry a client's admin write to the active controller. This module holds
//! the pure encode/decode halves of sending one and of serving one.
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
//!   cannot parse, or an embedded body that does not decode, is an
//!   `UnsupportedVersionException`, an inner api key whose
//!   `ApiKeys.forwardable` is false is an `InvalidRequestException`, and a
//!   principal it cannot deserialize is a `PrincipalDeserializationException`.
//!   It performs no principal-*type* check, so neither does this module.

use {
    crate::{
        api_catalog::UnstableApiVersions,
        codes,
        handlers::{ApiKeyCode, ApiVersion, CorrelationId},
        network::{request::parse_request, response_header_v1},
    },
    bytes::{Buf as _, BufMut as _, Bytes, BytesMut},
    krabka_protocol::{
        Decode as _, Encode as _, ProtocolError, UnknownTaggedFields, api_key,
        owned::{
            add_raft_voter_request, allocate_producer_ids_request, alter_client_quotas_request,
            alter_configs_request, alter_partition_reassignments_request,
            alter_user_scram_credentials_request, create_acls_request,
            create_delegation_token_request, create_partitions_request, create_topics_request,
            default_principal_data::{self, DefaultPrincipalData},
            delete_acls_request, delete_topics_request, describe_quorum_request,
            elect_leaders_request,
            envelope_request::EnvelopeRequest,
            envelope_response::EnvelopeResponse,
            expire_delegation_token_request, incremental_alter_configs_request,
            list_partition_reassignments_request, remove_raft_voter_request,
            renew_delegation_token_request, unregister_broker_request,
            unregister_controller_request, update_features_request,
        },
        primitives::string_bytes::put_nullable_string,
        tagged_fields::read_tagged_fields,
    },
    std::net::IpAddr,
};

/// A failure serving one `Envelope`, and the `EnvelopeResponse.error_code`
/// Kafka answers it with.
///
/// Every variant mirrors one throw site in `kafka.server.EnvelopeUtils`, so
/// the code a JVM forwarder sees is the code its own controller would send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeError {
    /// The outer principal lacks `ClusterAction` on the cluster resource.
    /// `ApiKeys.ENVELOPE.clusterAction` is true, so this is the gate Kafka
    /// applies before it looks at the payload at all.
    ClusterAuthorizationFailed,
    /// `request_principal` was absent, carried an unreadable schema version,
    /// or did not decode as a `DefaultPrincipalData` body.
    PrincipalDeserializationFailure,
    /// The embedded request header did not parse, or its body did not decode.
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

krabka_macros::forwardable_requests! {
    19 => create_topics_request::CreateTopicsRequest, // CreateTopics
    20 => delete_topics_request::DeleteTopicsRequest, // DeleteTopics
    30 => create_acls_request::CreateAclsRequest, // CreateAcls
    31 => delete_acls_request::DeleteAclsRequest, // DeleteAcls
    33 => alter_configs_request::AlterConfigsRequest, // AlterConfigs
    37 => create_partitions_request::CreatePartitionsRequest, // CreatePartitions
    38 => create_delegation_token_request::CreateDelegationTokenRequest, // CreateDelegationToken
    39 => renew_delegation_token_request::RenewDelegationTokenRequest, // RenewDelegationToken
    40 => expire_delegation_token_request::ExpireDelegationTokenRequest, // ExpireDelegationToken
    43 => elect_leaders_request::ElectLeadersRequest, // ElectLeaders
    44 => incremental_alter_configs_request::IncrementalAlterConfigsRequest, // IncrementalAlterConfigs
    45 => alter_partition_reassignments_request::AlterPartitionReassignmentsRequest, // AlterPartitionReassignments
    46 => list_partition_reassignments_request::ListPartitionReassignmentsRequest, // ListPartitionReassignments
    49 => alter_client_quotas_request::AlterClientQuotasRequest, // AlterClientQuotas
    51 => alter_user_scram_credentials_request::AlterUserScramCredentialsRequest, // AlterUserScramCredentials
    55 => describe_quorum_request::DescribeQuorumRequest, // DescribeQuorum
    57 => update_features_request::UpdateFeaturesRequest, // UpdateFeatures
    64 => unregister_broker_request::UnregisterBrokerRequest, // UnregisterBroker
    67 => allocate_producer_ids_request::AllocateProducerIdsRequest, // AllocateProducerIds
    80 => add_raft_voter_request::AddRaftVoterRequest, // AddRaftVoter
    81 => remove_raft_voter_request::RemoveRaftVoterRequest, // RemoveRaftVoter
    94 => unregister_controller_request::UnregisterControllerRequest, // UnregisterController (Kafka trunk, KIP-1312)
}

/// Whether Kafka wraps `api_key` in an `Envelope` when a broker forwards it.
pub fn is_forwardable(api_key: ApiKeyCode) -> bool { FORWARDABLE_API_KEYS.contains(&api_key) }

/// Whether `api_key` is an api key `RequestHeader.parse` accepts on a listener
/// running with `unstable`: one the release has (`ApiKeys.forId`) with a valid
/// version (`hasValidVersion`).
///
/// Kafka 4.3.1 has no key from 93 up, and its removed `LeaderAndIsr`,
/// `StopReplica`, `UpdateMetadata` and `ControlledShutdown` (keys 4 to 7) have
/// no valid version. Trunk's own keys are known while
/// `unstable.api.versions.enable` is on.
fn is_known_api_key(api_key: ApiKeyCode, unstable: UnstableApiVersions) -> bool {
    match unstable {
        UnstableApiVersions::Disabled => krabka_raft::kafka_4_3_1_api(api_key).is_some(),
        UnstableApiVersions::Enabled => api_key::ApiKey::from_i16(api_key).is_some(),
    }
}

/// The client identity a forwarder put in `request_principal`.
///
/// `KafkaPrincipal` carries a name and a token flag and nothing else — no
/// mechanism, no groups — so this is deliberately narrower than
/// [`krabka_security::Principal`]. The caller pairs it with the forwarding
/// hop's own authentication method to build the session identity the handler
/// runs under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardedPrincipal {
    pub(crate) name: String,
    pub(crate) token_authenticated: bool,
}

/// One embedded request, lifted out of an `EnvelopeRequest.request_data`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardedRequest {
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
pub fn deserialize_principal(bytes: Option<&[u8]>) -> Result<ForwardedPrincipal, EnvelopeError> {
    let mut cur = bytes.ok_or(EnvelopeError::PrincipalDeserializationFailure)?;
    let Some((prefix, rest)) = cur.split_first_chunk::<2>() else {
        return Err(EnvelopeError::PrincipalDeserializationFailure);
    };
    let version = i16::from_be_bytes(*prefix);
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
pub fn deserialize_client_host_address(bytes: &[u8]) -> Result<IpAddr, EnvelopeError> {
    if let Ok(octets) = <[u8; 4]>::try_from(bytes) {
        return Ok(IpAddr::from(octets));
    }
    if let Ok(octets) = <[u8; 16]>::try_from(bytes) {
        return Ok(IpAddr::from(octets).to_canonical());
    }
    Err(EnvelopeError::InvalidRequest)
}

/// Write a `request_principal` the way `DefaultKafkaPrincipalBuilder.serialize`
/// does: a big-endian `int16` schema version 0, then the flexible
/// `DefaultPrincipalData` body at that version. [`deserialize_principal`]
/// reads it back.
///
/// The principal type is always Kafka's `"User"`, because
/// `DefaultKafkaPrincipalBuilder` builds every principal with that type and
/// krabka authorizes on the name alone.
///
/// # Errors
/// Returns an error if the generated codec rejects the principal body.
pub fn serialize_principal(principal: &ForwardedPrincipal) -> Result<Bytes, ProtocolError> {
    let data = DefaultPrincipalData {
        type_: "User".to_owned(),
        name: principal.name.clone(),
        token_authenticated: principal.token_authenticated,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let version = default_principal_data::MIN_VERSION;
    let mut out = BytesMut::with_capacity(2 + data.encoded_len(version));
    out.put_i16(version);
    data.encode(&mut out, version)?;
    Ok(out.freeze())
}

/// Write a `client_host_address` the way `InetAddress.getAddress()` does: four
/// octets for an IPv4 host, sixteen for an IPv6 one.
/// [`deserialize_client_host_address`] reads it back.
///
/// An IPv4-mapped `::ffff:a.b.c.d` goes out as its four IPv4 octets. A JVM
/// socket reports such a peer as the `Inet4Address` it names, so a JVM
/// forwarder sends the same four octets for the same client.
pub fn serialize_client_host_address(ip: IpAddr) -> Bytes {
    match ip.to_canonical() {
        IpAddr::V4(v4) => Bytes::copy_from_slice(&v4.octets()),
        IpAddr::V6(v6) => Bytes::copy_from_slice(&v6.octets()),
    }
}

/// Build an `EnvelopeRequest.request_data`: the embedded request header in
/// front of the embedded body. This is Kafka's
/// `AbstractRequest.serializeWithHeader`, and [`unwrap_request`] reads it
/// back.
///
/// The header is `api_key`, `api_version`, `correlation_id` and a nullable
/// `client_id`. It is v2, with a trailing empty tagged-fields byte, exactly
/// when the embedded body is flexible.
pub fn wrap_request(forwarded: &ForwardedRequest) -> Bytes {
    let client_id_len = forwarded.client_id.as_ref().map_or(0, String::len);
    let mut out = BytesMut::with_capacity(10 + client_id_len + 1 + forwarded.body.len());
    out.put_i16(forwarded.api_key);
    out.put_i16(forwarded.api_version);
    out.put_i32(forwarded.correlation_id);
    put_nullable_string(&mut out, forwarded.client_id.as_deref());
    if forwarded.body_flexible {
        out.put_u8(0);
    }
    out.put_slice(&forwarded.body);
    out.freeze()
}

/// Build the `EnvelopeRequest` that carries `request_data` to the controller
/// in the name of `principal`, who connected from `client`. This is Kafka's
/// `ForwardingManagerUtil.buildEnvelopeRequest`.
///
/// # Errors
/// Returns an error if the generated codec rejects the principal body.
pub fn envelope_request(
    request_data: Bytes,
    principal: &ForwardedPrincipal,
    client: IpAddr,
) -> Result<EnvelopeRequest, ProtocolError> {
    Ok(EnvelopeRequest {
        request_data,
        request_principal: Some(serialize_principal(principal)?),
        client_host_address: serialize_client_host_address(client),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    })
}

/// Split a served `EnvelopeResponse.response_data` into the correlation id of
/// its embedded response header and the embedded body. This is the reverse of
/// [`wrap_response`].
///
/// `body_flexible` says whether the embedded body is flexible at the version
/// the request was sent at. The embedded header then carries a tagged-fields
/// section, except for `ApiVersions`, as [`crate::network::response_header_v1`]
/// says.
///
/// # Errors
/// Returns an error if the data is too short to hold the header, or if its
/// tagged-fields section does not parse.
pub fn unwrap_response(
    api_key: ApiKeyCode,
    body_flexible: bool,
    response_data: &Bytes,
) -> Result<(CorrelationId, Bytes), ProtocolError> {
    let mut cur = response_data.as_ref();
    if cur.remaining() < 4 {
        return Err(ProtocolError::UnexpectedEof {
            needed: 4 - cur.remaining(),
        });
    }
    let correlation_id = cur.get_i32();
    if response_header_v1(api_key, body_flexible) {
        read_tagged_fields(&mut cur, |_, _| Ok(false))?;
    }
    Ok((correlation_id, response_data.slice_ref(cur)))
}

/// Decode an `EnvelopeRequest` at `version`.
///
/// # Errors
/// [`EnvelopeError::InvalidRequest`] when the body is not a well-formed
/// `EnvelopeRequest`, which is how Kafka reports a malformed envelope.
pub fn decode_request(body: &[u8], version: i16) -> Result<EnvelopeRequest, EnvelopeError> {
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
/// The checks run in the order of `EnvelopeUtils.handleEnvelopeRequest`:
/// the header, then the forwardable test, then the body.
///
/// # Errors
/// [`EnvelopeError::UnsupportedVersion`] when the header does not parse, which
/// covers an api key `unstable` leaves unknown, and when the body does not
/// decode: `EnvelopeUtils` turns the `InvalidRequestException` of either parse
/// into `UnsupportedVersionException`, so the forwarder can tell this from a
/// malformed envelope. [`EnvelopeError::InvalidRequest`] when the embedded api
/// key is a known one that Kafka does not forward.
pub fn unwrap_request<F>(
    request_data: &Bytes,
    flexible_for: F,
    unstable: UnstableApiVersions,
) -> Result<ForwardedRequest, EnvelopeError>
where
    F: Fn(ApiKeyCode, ApiVersion) -> bool,
{
    let parsed =
        parse_request(request_data, flexible_for).map_err(|_| EnvelopeError::UnsupportedVersion)?;
    if !is_known_api_key(parsed.api_key, unstable) {
        return Err(EnvelopeError::UnsupportedVersion);
    }
    if !is_forwardable(parsed.api_key) {
        return Err(EnvelopeError::InvalidRequest);
    }
    if !embedded_body_decodes(parsed.api_key, parsed.api_version, parsed.body) {
        return Err(EnvelopeError::UnsupportedVersion);
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
pub fn wrap_response(forwarded: &ForwardedRequest, body: &[u8]) -> Bytes {
    let header_v1 = response_header_v1(forwarded.api_key, forwarded.body_flexible);
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
pub fn encode_success(
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
pub fn encode_failure(
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
    use {
        super::*,
        crate::api_catalog::UnstableApiVersions,
        assert2::{assert, check},
        krabka_protocol::owned::{
            alter_configs_request::AlterConfigsRequest, create_topics_request::CreateTopicsRequest,
            incremental_alter_configs_request::IncrementalAlterConfigsRequest,
            unregister_controller_request::UnregisterControllerRequest,
        },
        rstest_bdd::datatable::Rows,
        std::{collections::BTreeSet, net::Ipv6Addr},
    };

    /// `DefaultKafkaPrincipalBuilder.serialize(new KafkaPrincipal("User",
    /// "alice"))` in `mirror.gcr.io/apache/kafka:4.3.1`, captured verbatim:
    /// int16 version 0, compact string "User", compact string "alice",
    /// `token_authenticated = false`, empty tagged fields.
    const JVM_USER_ALICE: &[u8] = krabka_macros::jvm_principal_golden!(alice, false);
    /// The same call for `new KafkaPrincipal("User", "bob", true)`, whose
    /// `token_authenticated` byte is `0x01`.
    const JVM_USER_BOB_TOKEN: &[u8] = krabka_macros::jvm_principal_golden!(bob, true);

    fn forwarded_principal(name: &str, token_authenticated: bool) -> ForwardedPrincipal {
        ForwardedPrincipal {
            name: name.to_owned(),
            token_authenticated,
        }
    }

    /// The bytes of a default request of type `R` at `version`, which the
    /// embedded-body check decodes.
    fn body_of<R: krabka_protocol::Encode + Default>(version: i16) -> Vec<u8> {
        let mut body = BytesMut::new();
        R::default()
            .encode(&mut body, version)
            .expect("encode a default request");
        body.to_vec()
    }

    const UNSTABLE_OFF: UnstableApiVersions = UnstableApiVersions::Disabled;

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

    #[derive(Default)]
    struct ForwardedEnvelopeScenario {
        principal_bytes: Option<Vec<u8>>,
        principal: Option<Result<ForwardedPrincipal, EnvelopeError>>,
        failure: Option<EnvelopeError>,
        code: Option<i16>,
        declared_keys: Vec<i16>,
        forwardable_keys: Option<BTreeSet<i16>>,
        nested_envelope: Option<bool>,
        produce: Option<bool>,
        frame: Option<Bytes>,
        unstable: Option<UnstableApiVersions>,
        embedded: Option<Result<(), EnvelopeError>>,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct ForwardableApiKey {
        key: i16,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct EmbeddedFrame {
        api: i16,
        version: i16,
        body: String,
        unstable: bool,
    }

    #[rstest::fixture]
    fn forwarded_envelope() -> ForwardedEnvelopeScenario {
        // Each forwarded request carries independent bytes, flags, and outcomes.
        ForwardedEnvelopeScenario::default()
    }

    fn envelope_failure(name: &str) -> EnvelopeError {
        match name {
            "ClusterAuthorizationFailed" => EnvelopeError::ClusterAuthorizationFailed,
            "PrincipalDeserializationFailure" => EnvelopeError::PrincipalDeserializationFailure,
            "UnsupportedVersion" => EnvelopeError::UnsupportedVersion,
            "InvalidRequest" => EnvelopeError::InvalidRequest,
            other => panic!("unknown Envelope failure {other}"),
        }
    }

    #[rstest_bdd_macros::given("the Envelope request principal is null")]
    fn absent_forwarded_principal(forwarded_envelope: &mut ForwardedEnvelopeScenario) {
        forwarded_envelope.principal_bytes = None;
    }

    #[rstest_bdd_macros::given("the serialized Envelope principal has these hexadecimal bytes:")]
    fn forwarded_principal_bytes(
        forwarded_envelope: &mut ForwardedEnvelopeScenario,
        docstring: String,
    ) {
        forwarded_envelope.principal_bytes = Some(
            docstring
                .split_whitespace()
                .map(|byte| u8::from_str_radix(byte, 16).expect("principal bytes are hexadecimal"))
                .collect(),
        );
    }

    #[rstest_bdd_macros::when("the controller deserializes the forwarded Kafka principal")]
    fn deserialize_forwarded_principal(forwarded_envelope: &mut ForwardedEnvelopeScenario) {
        forwarded_envelope.principal = Some(deserialize_principal(
            forwarded_envelope.principal_bytes.as_deref(),
        ));
    }

    #[rstest_bdd_macros::then(
        "the forwarded principal is rejected as PrincipalDeserializationFailure"
    )]
    fn refused_forwarded_principal(forwarded_envelope: &ForwardedEnvelopeScenario) {
        check!(
            forwarded_envelope.principal
                == Some(Err(EnvelopeError::PrincipalDeserializationFailure))
        );
    }

    #[rstest_bdd_macros::given("the Envelope failure is {failure}")]
    fn forwarded_failure(forwarded_envelope: &mut ForwardedEnvelopeScenario, failure: String) {
        forwarded_envelope.failure = Some(envelope_failure(&failure));
    }

    #[rstest_bdd_macros::when("the Envelope failure is translated to its Kafka wire code")]
    fn forwarded_failure_code(forwarded_envelope: &mut ForwardedEnvelopeScenario) {
        forwarded_envelope.code = Some(
            forwarded_envelope
                .failure
                .expect("an Envelope failure was given")
                .code(),
        );
    }

    #[rstest_bdd_macros::then("the Envelope wire error code is {code}")]
    fn kafka_forwarded_failure_code(forwarded_envelope: &ForwardedEnvelopeScenario, code: i16) {
        check!(forwarded_envelope.code == Some(code));
    }

    #[rstest_bdd_macros::given("the router uses its declared Envelope forwarding policy")]
    fn declared_forwarding_policy(forwarded_envelope: &mut ForwardedEnvelopeScenario) {
        forwarded_envelope.declared_keys = FORWARDABLE_API_KEYS.to_vec();
    }

    #[rstest_bdd_macros::when(
        "the forwardability predicate checks the declared requests, Envelope, and Produce"
    )]
    fn evaluate_forwarding_policy(forwarded_envelope: &mut ForwardedEnvelopeScenario) {
        forwarded_envelope.forwardable_keys = Some(
            forwarded_envelope
                .declared_keys
                .iter()
                .copied()
                .filter(|key| is_forwardable(*key))
                .collect(),
        );
        forwarded_envelope.nested_envelope = Some(is_forwardable(58));
        forwarded_envelope.produce = Some(is_forwardable(0));
    }

    #[rstest_bdd_macros::then("the declared and admitted forwardable API keys are exactly:")]
    fn kafka_forwarding_policy(
        forwarded_envelope: &ForwardedEnvelopeScenario,
        #[datatable] rows: Rows<ForwardableApiKey>,
    ) {
        let expected: BTreeSet<_> = rows.into_iter().map(|row| row.key).collect();
        let declared: BTreeSet<_> = forwarded_envelope.declared_keys.iter().copied().collect();
        check!(declared == expected);
        check!(forwarded_envelope.forwardable_keys.as_ref() == Some(&expected));
    }

    #[rstest_bdd_macros::then(
        "neither a nested Envelope nor a data-plane Produce request is forwardable"
    )]
    fn no_nested_or_data_plane_forwarding(forwarded_envelope: &ForwardedEnvelopeScenario) {
        check!(forwarded_envelope.nested_envelope == Some(false));
        check!(forwarded_envelope.produce == Some(false));
    }

    #[rstest_bdd_macros::given("the embedded frame has only three header bytes")]
    fn short_embedded_header(forwarded_envelope: &mut ForwardedEnvelopeScenario) {
        forwarded_envelope.frame = Some(Bytes::from_static(&[0, 19, 0]));
        forwarded_envelope.unstable = Some(UnstableApiVersions::Disabled);
    }

    #[rstest_bdd_macros::given("the embedded forwarded frame is:")]
    fn embedded_forwarded_frame(
        forwarded_envelope: &mut ForwardedEnvelopeScenario,
        #[datatable] rows: Rows<EmbeddedFrame>,
    ) {
        let mut rows = rows.into_iter();
        let row = rows.next().expect("one embedded frame was specified");
        assert!(rows.next().is_none());
        let body = match row.body.as_str() {
            "empty" => Vec::new(),
            "garbage" => b"garbage".to_vec(),
            "CreateTopics-v7" => body_of::<CreateTopicsRequest>(7),
            "truncated-CreateTopics-v7" => {
                let mut body = body_of::<CreateTopicsRequest>(7);
                body.truncate(body.len() - 1);
                body
            }
            "UnregisterController-v0" => body_of::<UnregisterControllerRequest>(0),
            body => panic!("unknown embedded body fixture {body}"),
        };
        forwarded_envelope.frame = Some(
            request_frame()
                .api_key(row.api)
                .api_version(row.version)
                .body(&body)
                .call(),
        );
        forwarded_envelope.unstable = Some(if row.unstable {
            UnstableApiVersions::Enabled
        } else {
            UnstableApiVersions::Disabled
        });
    }

    #[rstest_bdd_macros::when(
        "the controller unwraps the embedded request after cluster authorization succeeds"
    )]
    fn unwrap_forwarded_frame(forwarded_envelope: &mut ForwardedEnvelopeScenario) {
        let frame = forwarded_envelope
            .frame
            .as_ref()
            .expect("an embedded frame was given");
        let unstable = forwarded_envelope
            .unstable
            .expect("the API-version policy was given");
        forwarded_envelope.embedded =
            Some(unwrap_request(frame, |_, _| true, unstable).map(|_| ()));
    }

    #[rstest_bdd_macros::then("the embedded request is rejected as {failure}")]
    fn rejected_embedded_request(forwarded_envelope: &ForwardedEnvelopeScenario, failure: String) {
        check!(forwarded_envelope.embedded == Some(Err(envelope_failure(&failure))));
    }

    #[rstest_bdd_macros::then("the embedded request is admitted for controller dispatch")]
    fn admitted_embedded_request(forwarded_envelope: &ForwardedEnvelopeScenario) {
        check!(forwarded_envelope.embedded == Some(Ok(())));
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/envelope",
        fixtures = [forwarded_envelope: ForwardedEnvelopeScenario]
    );

    /// `client_host_address` is exactly what `InetAddress.getByAddress`
    /// accepts: four octets or sixteen, with an IPv4-mapped sixteen folded
    /// back to the IPv4 address it names. Every other length is the
    /// `UnknownHostException` `EnvelopeUtils` reports as an invalid request.
    #[test]
    fn a_client_host_address_decodes_the_way_inet_address_get_by_address_does() {
        struct Case<'a> {
            label: &'static str,
            bytes: &'a [u8],
            want: Result<IpAddr, EnvelopeError>,
        }

        let mapped_v4 = {
            let mut octets = [0u8; 16];
            octets[10] = 0xff;
            octets[11] = 0xff;
            octets[12..].copy_from_slice(&[10, 1, 2, 3]);
            octets
        };
        let cases: [Case<'_>; 6] = [
            Case {
                label: "four octets are an IPv4 address",
                bytes: &[10, 1, 2, 3],
                want: { Ok(IpAddr::from([10, 1, 2, 3])) },
            },
            Case {
                label: "sixteen octets are an IPv6 address",
                bytes: &[0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                want: Ok("2001:db8::1".parse().expect("literal IPv6 address")),
            },
            Case {
                label: "an IPv4-mapped IPv6 address folds to its IPv4 form",
                bytes: &mapped_v4,
                want: Ok(IpAddr::from([10, 1, 2, 3])),
            },
            Case {
                label: "an empty address",
                bytes: &[],
                want: Err(EnvelopeError::InvalidRequest),
            },
            Case {
                label: "three octets",
                bytes: &[10, 1, 2],
                want: Err(EnvelopeError::InvalidRequest),
            },
            Case {
                label: "five octets",
                bytes: &[10, 1, 2, 3, 4],
                want: Err(EnvelopeError::InvalidRequest),
            },
        ];

        for Case { label, bytes, want } in cases {
            check!(
                deserialize_client_host_address(bytes) == want,
                "label: {label}"
            );
        }
    }

    /// The send side writes the bytes a JVM forwarder writes, and the receive
    /// side reads them back into the identity that went in.
    #[test]
    fn a_serialized_principal_matches_the_jvm_bytes_and_decodes_back() {
        let cases = [
            (
                "User:alice, not token authenticated",
                forwarded_principal("alice", false),
                JVM_USER_ALICE,
            ),
            (
                "User:bob, token authenticated",
                forwarded_principal("bob", true),
                JVM_USER_BOB_TOKEN,
            ),
        ];

        for (case, principal, jvm) in cases {
            assert!(let Ok(bytes) = serialize_principal(&principal), "case: {case}");

            check!(bytes.as_ref() == jvm, "case: {case}");
            check!(
                deserialize_principal(Some(&bytes)) == Ok(principal),
                "case: {case}"
            );
        }
    }

    /// `client_host_address` goes out as `InetAddress.getAddress()` writes
    /// it, and the receive side reads the same address back. An IPv4-mapped
    /// peer goes out as the four octets of the IPv4 address it names, which
    /// is what a JVM socket reports for such a peer.
    #[test]
    fn a_client_host_address_round_trips_as_inet_address_get_address_writes_it() {
        struct Case {
            label: &'static str,
            ip: IpAddr,
            octets: &'static [u8],
            decoded: IpAddr,
        }
        let cases: [Case; 3] = [
            Case {
                label: "an IPv4 address is four octets",
                ip: IpAddr::from([10, 1, 2, 3]),
                octets: &[10, 1, 2, 3],
                decoded: IpAddr::from([10, 1, 2, 3]),
            },
            Case {
                label: "an IPv6 address is sixteen octets",
                ip: IpAddr::V6(Ipv6Addr::LOCALHOST),
                octets: &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                decoded: IpAddr::V6(Ipv6Addr::LOCALHOST),
            },
            Case {
                label: "an IPv4-mapped IPv6 address is the four IPv4 octets",
                ip: "::ffff:10.1.2.3".parse().expect("literal IPv6 address"),
                octets: &[10, 1, 2, 3],
                decoded: IpAddr::from([10, 1, 2, 3]),
            },
        ];

        for Case {
            label,
            ip,
            octets,
            decoded,
        } in cases
        {
            let bytes = serialize_client_host_address(ip);

            check!(bytes.as_ref() == octets, "label: {label}");
            check!(
                deserialize_client_host_address(&bytes) == Ok(decoded),
                "label: {label}"
            );
        }
    }

    /// `wrap_request` writes the header `unwrap_request` reads, so an embedded
    /// request round-trips whole, with and without a flexible body and a
    /// client id. The flexible case writes the same bytes as the hand-built
    /// frame the receive-side tests use.
    #[test]
    fn a_wrapped_request_unwraps_to_the_same_request() {
        let create_topics = body_of::<CreateTopicsRequest>(7);
        let alter_configs = body_of::<AlterConfigsRequest>(0);
        let cases = [
            (
                "flexible CreateTopics with a client id",
                ForwardedRequest {
                    api_key: 19,
                    api_version: 7,
                    correlation_id: 5,
                    client_id: Some("c".to_owned()),
                    body: Bytes::from(create_topics.clone()),
                    body_flexible: true,
                },
                request_frame()
                    .correlation_id(5)
                    .maybe_client_id(Some("c"))
                    .body(&create_topics)
                    .call(),
            ),
            (
                "non-flexible AlterConfigs with a null client id",
                ForwardedRequest {
                    api_key: 33,
                    api_version: 0,
                    correlation_id: -3,
                    client_id: None,
                    body: Bytes::from(alter_configs.clone()),
                    body_flexible: false,
                },
                request_frame()
                    .api_key(33)
                    .api_version(0)
                    .correlation_id(-3)
                    .header(EmbeddedHeader::Legacy)
                    .body(&alter_configs)
                    .call(),
            ),
        ];

        for (case, forwarded, frame) in cases {
            let wrapped = wrap_request(&forwarded);

            check!(wrapped == frame, "case: {case}");
            check!(
                unwrap_request(&wrapped, |_, _| forwarded.body_flexible, UNSTABLE_OFF)
                    == Ok(forwarded),
                "case: {case}"
            );
        }
    }

    #[derive(Clone, Copy, Default)]
    enum ForwardedBodyEncoding {
        Legacy,
        #[default]
        Flexible,
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ForwardedResponseSetup {
        #[default(krabka_ids::ApiKey(19))]
        api_key: krabka_ids::ApiKey,
        api_version: krabka_ids::ApiVersion,
        #[default(EmbeddedCorrelationId(0x0102_0304))]
        correlation_id: EmbeddedCorrelationId,
        encoding: ForwardedBodyEncoding,
    }

    #[bon::builder]
    fn forwarded(
        #[builder(into, default = krabka_ids :: ApiKey (19))] api_key: krabka_ids::ApiKey,
        #[builder(into, default)] api_version: krabka_ids::ApiVersion,
        #[builder(into, default = EmbeddedCorrelationId (0x0102_0304))]
        correlation_id: EmbeddedCorrelationId,
        #[builder(default)] encoding: ForwardedBodyEncoding,
    ) -> ForwardedRequest {
        let setup = ForwardedResponseSetup {
            api_key,
            api_version,
            correlation_id,
            encoding,
        };
        ForwardedRequest {
            api_key: setup.api_key.0,
            api_version: setup.api_version.0,
            correlation_id: setup.correlation_id.0,
            client_id: None,
            body: Bytes::new(),
            body_flexible: matches!(setup.encoding, ForwardedBodyEncoding::Flexible),
        }
    }

    /// `unwrap_response` strips the header `wrap_response` writes and hands
    /// back the correlation id and the body. `ApiVersions` keeps a v0 header
    /// even when its body is flexible.
    #[test]
    fn a_wrapped_response_unwraps_to_its_correlation_id_and_body() {
        let cases = [
            ("flexible body takes a v1 header", forwarded().call()),
            (
                "non-flexible body takes a v0 header",
                forwarded().encoding(ForwardedBodyEncoding::Legacy).call(),
            ),
            (
                "flexible ApiVersions takes a v0 header",
                forwarded().api_key(18).call(),
            ),
        ];

        for (case, forwarded) in cases {
            let wrapped = wrap_response(&forwarded, b"body");

            check!(
                unwrap_response(forwarded.api_key, forwarded.body_flexible, &wrapped)
                    .map_err(|error| error.to_string())
                    == Ok((0x0102_0304, Bytes::from_static(b"body"))),
                "case: {case}"
            );
        }
    }

    /// Data too short to hold the correlation id, or a flexible header whose
    /// tagged-fields section is cut off, is refused.
    #[test]
    fn a_response_too_short_for_its_header_is_refused() {
        struct Case {
            label: &'static str,
            data: &'static [u8],
            flexible: bool,
        }
        let cases: [Case; 2] = [
            Case {
                label: "three bytes",
                data: &[0, 0, 1],
                flexible: false,
            },
            Case {
                label: "a tagged-fields count with no fields after it",
                data: &[0, 0, 0, 1, 1],
                flexible: true,
            },
        ];

        for Case {
            label,
            data,
            flexible,
        } in cases
        {
            check!(
                unwrap_response(19, flexible, &Bytes::copy_from_slice(data)).is_err(),
                "label: {label}"
            );
        }
    }

    /// The Envelope that the send side builds carries the three pieces the
    /// receive side reads, and it survives the v0 codec.
    #[test]
    fn a_built_envelope_request_carries_what_the_receive_side_reads() {
        let request_data = request_frame()
            .correlation_id(5)
            .maybe_client_id(Some("c"))
            .body(b"topics")
            .call();
        let client = IpAddr::from([127, 0, 0, 1]);

        assert!(let Ok(request) = envelope_request(
            request_data.clone(),
            &forwarded_principal("alice", false),
            client,
        ));
        let mut encoded = BytesMut::new();
        request.encode(&mut encoded, 0).expect("encode");

        let want = EnvelopeRequest {
            request_data,
            request_principal: Some(Bytes::from_static(JVM_USER_ALICE)),
            client_host_address: Bytes::from_static(&[127, 0, 0, 1]),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        check!(decode_request(&encoded, 0) == Ok(want));
    }

    krabka_macros::request_frame_fixture!(wire_request_frame);

    #[derive(
        Debug,
        Clone,
        Copy,
        PartialEq,
        Eq,
        derive_more::Display,
        derive_more::From,
        derive_more::Into,
    )]
    struct EmbeddedCorrelationId(i32);

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum EmbeddedHeader {
        Legacy,
        Flexible,
    }

    #[derive(Clone, Copy)]
    struct EmbeddedFrameSetup<'a> {
        api_key: krabka_ids::ApiKey,
        api_version: krabka_ids::ApiVersion,
        correlation_id: EmbeddedCorrelationId,
        client_id: Option<&'a str>,
        header: EmbeddedHeader,
        body: &'a [u8],
    }

    impl Default for EmbeddedFrameSetup<'_> {
        fn default() -> Self {
            Self {
                api_key: krabka_ids::ApiKey(19),
                api_version: krabka_ids::ApiVersion(7),
                correlation_id: EmbeddedCorrelationId(1),
                client_id: None,
                header: EmbeddedHeader::Flexible,
                body: &[],
            }
        }
    }

    #[bon::builder]
    fn request_frame(
        #[builder(into, default = EmbeddedFrameSetup::default().api_key)]
        api_key: krabka_ids::ApiKey,
        #[builder(into, default = EmbeddedFrameSetup::default().api_version)]
        api_version: krabka_ids::ApiVersion,
        #[builder(into, default = EmbeddedFrameSetup::default().correlation_id)]
        correlation_id: EmbeddedCorrelationId,
        client_id: Option<&'_ str>,
        #[builder(default = EmbeddedFrameSetup::default().header)] header: EmbeddedHeader,
        #[builder(default = EmbeddedFrameSetup::default().body)] body: &'_ [u8],
    ) -> Bytes {
        let setup = EmbeddedFrameSetup {
            api_key,
            api_version,
            correlation_id,
            client_id,
            header,
            body,
        };
        let EmbeddedFrameSetup {
            api_key,
            api_version,
            correlation_id,
            client_id,
            header,
            body,
        } = setup;
        wire_request_frame()
            .api_key(api_key.0)
            .api_version(api_version.0)
            .correlation_id(correlation_id.0)
            .maybe_client_id(client_id.map(str::as_bytes))
            .maybe_tagged((header == EmbeddedHeader::Flexible).then_some(&[0][..]))
            .body(body)
            .call()
            .freeze()
    }

    /// The embedded header is split off whole, and the body that remains is
    /// the handler's input. The flexible case also proves the tagged-fields
    /// byte is consumed rather than left at the head of the body.
    #[test]
    fn an_embedded_request_is_split_into_its_header_and_body() {
        let incremental = body_of::<IncrementalAlterConfigsRequest>(1);
        let alter_configs = body_of::<AlterConfigsRequest>(0);
        let cases = [
            (
                "flexible IncrementalAlterConfigs",
                request_frame()
                    .api_key(44)
                    .api_version(1)
                    .correlation_id(77)
                    .maybe_client_id(Some("adminclient-1"))
                    .body(&incremental)
                    .call(),
                true,
                ForwardedRequest {
                    api_key: 44,
                    api_version: 1,
                    correlation_id: 77,
                    client_id: Some("adminclient-1".to_owned()),
                    body: Bytes::from(incremental),
                    body_flexible: true,
                },
            ),
            (
                "non-flexible AlterConfigs with a null client id",
                request_frame()
                    .api_key(33)
                    .api_version(0)
                    .correlation_id(3)
                    .header(EmbeddedHeader::Legacy)
                    .body(&alter_configs)
                    .call(),
                false,
                ForwardedRequest {
                    api_key: 33,
                    api_version: 0,
                    correlation_id: 3,
                    client_id: None,
                    body: Bytes::from(alter_configs),
                    body_flexible: false,
                },
            ),
        ];

        for (case, frame, flexible, want) in cases {
            assert!(let Ok(forwarded) = unwrap_request(&frame, |_, _| flexible, UNSTABLE_OFF),
                "case: {case}"
            );

            check!(forwarded == want, "case: {case}");
        }
    }

    /// `response_data` is the embedded response header followed by the
    /// handler's body, and the header is v1 exactly when the embedded body is
    /// flexible. The correlation id is the *client's*, carried in from the
    /// forwarded header, so the forwarder can relay these bytes untouched.
    #[test]
    fn a_served_response_is_wrapped_with_the_clients_own_response_header() {
        let cases = [
            (
                "flexible body takes a v1 header",
                forwarded().api_key(44).call(),
                vec![0x01, 0x02, 0x03, 0x04, 0x00, b'x'],
            ),
            (
                "non-flexible body takes a v0 header",
                forwarded()
                    .api_key(33)
                    .encoding(ForwardedBodyEncoding::Legacy)
                    .call(),
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
            request_data: request_frame()
                .correlation_id(5)
                .maybe_client_id(Some("c"))
                .body(b"topics")
                .call(),
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
