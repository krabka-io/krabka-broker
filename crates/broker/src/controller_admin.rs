//! KIP-919 Admin RPC routing for the controller listener.

use std::sync::{Arc, OnceLock, Weak};

use bytes::Bytes;
use krabka_raft::{
    ControllerAdminRequest, ControllerAdminResponse, ControllerAdminRouteFuture,
    ControllerAdminRouter, ControllerApiVersion, RaftError,
};

use crate::{
    broker::Broker,
    envelope::{self, EnvelopeError, ForwardedRequest},
    handlers::{ApiKeyCode, ApiVersion, CorrelationId, DispatchKind, RequestContext},
    network::auth::ConnectionAuth,
};

/// `Envelope`'s api key. KIP-590 forwarding is the one controller-listener API
/// whose payload is another API's request, so it does not go through the
/// registry lookup the rest of [`SUPPORTED_APIS`] shares.
const ENVELOPE_API_KEY: ApiKeyCode = krabka_protocol::owned::envelope_request::API_KEY;

macro_rules! api_version {
    ($request:ident) => {
        ControllerApiVersion {
            api_key: krabka_protocol::owned::$request::API_KEY,
            min_version: krabka_protocol::owned::$request::MIN_VERSION,
            max_version: krabka_protocol::owned::$request::MAX_VERSION,
            flexible_min: krabka_protocol::owned::$request::FLEXIBLE_MIN,
        }
    };
}

/// KIP-919's controller-listener Admin subset, plus the KIP-590 `Envelope`
/// forwarding RPC. `DescribeQuorum`, `DescribeCluster`, `ApiVersions`, and
/// controller registration are served directly by `krabka-raft`, so only the
/// shared broker-handler subset lives here.
const SUPPORTED_APIS: &[ControllerApiVersion] = &[
    // KIP-590 `Envelope`. `ApiKeys.ENVELOPE.messageType.listeners()` in
    // `kafka-clients-4.3.1.jar` is exactly `[CONTROLLER]`, so it is advertised
    // here and never on the client listener.
    api_version!(envelope_request),
    api_version!(alter_configs_request),
    api_version!(create_acls_request),
    api_version!(delete_acls_request),
    api_version!(describe_acls_request),
    api_version!(describe_configs_request),
    api_version!(describe_client_quotas_request),
    api_version!(alter_client_quotas_request),
    api_version!(incremental_alter_configs_request),
    api_version!(describe_delegation_token_request),
    api_version!(elect_leaders_request),
    api_version!(alter_partition_reassignments_request),
    api_version!(list_partition_reassignments_request),
    api_version!(describe_user_scram_credentials_request),
    api_version!(update_features_request),
    api_version!(unregister_broker_request),
];

/// Late-bound bridge from the controller, which starts before the broker, to
/// the broker's single Admin handler registry. The weak pointer avoids a
/// controller -> router -> broker -> controller ownership cycle.
pub(crate) struct BrokerControllerAdminRouter {
    broker: OnceLock<Weak<Broker>>,
}

impl BrokerControllerAdminRouter {
    pub(crate) const fn new() -> Self {
        Self {
            broker: OnceLock::new(),
        }
    }

    pub(crate) fn bind(&self, broker: &Arc<Broker>) -> Result<(), &'static str> {
        self.broker
            .set(Arc::downgrade(broker))
            .map_err(|_| "controller Admin router already bound")
    }
}

impl ControllerAdminRouter for BrokerControllerAdminRouter {
    fn api_versions(&self) -> &[ControllerApiVersion] {
        SUPPORTED_APIS
    }

    fn route(&self, request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_> {
        Box::pin(async move {
            let Some(version) = SUPPORTED_APIS
                .iter()
                .find(|version| version.api_key == request.api_key)
                .copied()
            else {
                // Kafka's AdminClient rejects APIs outside the KIP-919
                // controller surface locally. A raw disabled API is rejected
                // by the controller listener before dispatch, which closes the
                // connection rather than inventing a response body.
                return Ok(None);
            };
            if !(version.min_version..=version.max_version).contains(&request.api_version) {
                // The listener likewise closes raw requests whose version is
                // outside the advertised range. The client normally prevents
                // these through ApiVersions negotiation.
                return Ok(None);
            }
            let broker =
                self.broker.get().and_then(Weak::upgrade).ok_or_else(|| {
                    RaftError::ControllerAdmin("broker startup is incomplete".into())
                })?;

            let principal = outer_principal(&request);
            if request.api_key == ENVELOPE_API_KEY {
                return Ok(Some(ControllerAdminResponse {
                    body: serve_envelope(&broker, &request, &principal).await?,
                    // `EnvelopeResponse` is flexible at every valid version.
                    flexible: request.api_version >= version.flexible_min,
                }));
            }

            let body = invoke_registered_handler(
                &broker,
                Invocation {
                    api_key: request.api_key,
                    api_version: request.api_version,
                    correlation_id: request.correlation_id,
                    body: &request.body,
                    client_id: request.client_id.as_deref().unwrap_or(""),
                    peer: &request.peer,
                    principal: &principal,
                    authenticated_via_token: request.authenticated_via_token,
                },
            )
            .await
            .map_err(|error| RaftError::ControllerAdmin(error.to_string()))?;

            Ok(Some(ControllerAdminResponse {
                body,
                flexible: request.api_version >= version.flexible_min,
            }))
        })
    }
}

/// The identity of the peer that opened the controller-listener connection.
///
/// A Plaintext or TLS-only controller listener authenticates nobody, so there
/// is no principal to carry and the request runs as `ANONYMOUS` — the same
/// default the SASL-less listener applies everywhere else.
fn outer_principal(request: &ControllerAdminRequest) -> krabka_security::Principal {
    request
        .principal
        .clone()
        .unwrap_or_else(|| krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: Vec::new(),
        })
}

/// One request handed to a registered broker handler, whether it arrived on
/// the controller listener directly or inside a KIP-590 `Envelope`.
struct Invocation<'a> {
    api_key: ApiKeyCode,
    api_version: ApiVersion,
    correlation_id: CorrelationId,
    body: &'a [u8],
    client_id: &'a str,
    /// The address the handler authorizes and audits against. For a forwarded
    /// request this is the *client's* address, out of
    /// `EnvelopeRequest.client_host_address`, not the forwarding hop's.
    peer: &'a std::net::SocketAddr,
    /// The identity the handler authorizes against. For a forwarded request
    /// this is the *client's* principal, not the forwarding hop's.
    principal: &'a krabka_security::Principal,
    authenticated_via_token: bool,
}

/// Dispatch `invocation` through the broker's Admin handler registry.
///
/// Shared by the direct controller-listener path and the `Envelope` path so a
/// forwarded `IncrementalAlterConfigs` runs the same handler, under the same
/// `RequestContext`, as one that arrived unwrapped.
async fn invoke_registered_handler(
    broker: &std::sync::Arc<Broker>,
    invocation: Invocation<'_>,
) -> Result<Bytes, crate::error::BrokerError> {
    let Invocation {
        api_key,
        api_version,
        correlation_id,
        body,
        client_id,
        peer,
        principal,
        authenticated_via_token,
    } = invocation;
    let entry = broker.handlers().get(api_key).ok_or_else(|| {
        crate::error::BrokerError::UnsupportedApi {
            api_key,
            version: api_version,
        }
    })?;
    match entry.kind() {
        // A plain handler takes no session at all. `AllocateProducerIds` (67)
        // is the one api key that is both `ApiKeys.forwardable` and registered
        // this way, and a JVM broker forwards it to obtain a producer-id block
        // before any producer of its own can initialise, so the `Envelope`
        // path has to reach it. The envelope's `ClusterAction` gate has
        // already run by the time this is called.
        DispatchKind::Plain(handler) => handler(broker, api_version, correlation_id, body).await,
        DispatchKind::Context(handler) => {
            let context = RequestContext::new(
                principal,
                peer,
                client_id,
                "controller-admin",
                false,
                "CONTROLLER",
            );
            handler(broker, api_version, correlation_id, body, &context).await
        }
        DispatchKind::Auth(handler) => {
            let auth = ConnectionAuth::Authenticated {
                principal: principal.clone(),
                mechanism: krabka_security::SaslMechanism::Plain,
                expires_at_ms: None,
                authenticated_via_token,
            };
            handler(broker, api_version, correlation_id, body, &auth, peer).await
        }
        _ => Err(crate::error::BrokerError::UnsupportedApi {
            api_key,
            version: api_version,
        }),
    }
}

/// Serve one KIP-590 `Envelope` and return the encoded `EnvelopeResponse`
/// body.
///
/// A refusal is reported inside the envelope, as an `error_code` beside a null
/// `response_data`, which is how `kafka.server.EnvelopeUtils` answers: the
/// forwarding hop needs a well-formed envelope back so it can translate the
/// code for its own client. Only a failure of the embedded *handler* — which
/// is a broker fault, not a protocol one — escapes as a `RaftError`, and that
/// closes the connection, matching every other controller-admin API here.
async fn serve_envelope(
    broker: &std::sync::Arc<Broker>,
    request: &ControllerAdminRequest,
    outer: &krabka_security::Principal,
) -> Result<Bytes, RaftError> {
    let version = request.api_version;
    let encoded = match unwrap_envelope(broker, request, outer) {
        Ok(Unwrapped {
            forwarded,
            principal,
            client_host,
            token_authenticated,
        }) => {
            let body = invoke_registered_handler(
                broker,
                Invocation {
                    api_key: forwarded.api_key,
                    api_version: forwarded.api_version,
                    correlation_id: forwarded.correlation_id,
                    body: &forwarded.body,
                    client_id: forwarded.client_id.as_deref().unwrap_or(""),
                    // The *client's* address, not this connection's: the peer
                    // here is the forwarding broker, and authorizing or
                    // auditing the embedded request against that address both
                    // denies clients a host ACL allows and allows clients it
                    // denies. `EnvelopeUtils` builds its inner
                    // `RequestContext` from `client_host_address` for exactly
                    // this reason.
                    peer: &client_host,
                    principal: &principal,
                    // The *client's* flag, out of `request_principal`, not the
                    // forwarding hop's: KIP-48 forbids a token-authenticated
                    // caller from minting or renewing another token, and that
                    // rule has to follow the identity it belongs to.
                    authenticated_via_token: token_authenticated,
                },
            )
            .await
            .map_err(|error| RaftError::ControllerAdmin(error.to_string()))?;
            tracing::debug!(
                api_key = forwarded.api_key,
                api_version = forwarded.api_version,
                principal = %principal.name,
                "served a forwarded request from an Envelope"
            );
            envelope::encode_success(envelope::wrap_response(&forwarded, &body), version)
        }
        Err(error) => {
            tracing::warn!(?error, "refused an Envelope");
            envelope::encode_failure(error, version)
        }
    };
    encoded.map_err(|error| RaftError::ControllerAdmin(error.to_string()))
}

/// One `Envelope` that passed the `ClusterAction` gate and decoded cleanly:
/// the session the embedded request runs under, and the request itself.
struct Unwrapped {
    forwarded: ForwardedRequest,
    /// The client identity out of `request_principal`, not the forwarding
    /// hop's.
    principal: krabka_security::Principal,
    /// The client address out of `client_host_address`, not the forwarding
    /// hop's. KIP-590 carries the bare address octets and no port — Kafka's
    /// inner `RequestContext` holds an `InetAddress` — so the port is zero
    /// and only [`std::net::SocketAddr::ip`] is meaningful, which is all any
    /// host ACL or audit record reads.
    client_host: std::net::SocketAddr,
    /// Whether that client authenticated with a delegation token.
    token_authenticated: bool,
}

/// Authorize, decode and validate one `Envelope`.
///
/// The order of the checks is `EnvelopeUtils.handleEnvelopeRequest`'s own:
/// principal, then client address, then the embedded header, then the
/// forwardable test. A malformed envelope that fails two of them reports the
/// code Kafka's first failure would.
fn unwrap_envelope(
    broker: &Broker,
    request: &ControllerAdminRequest,
    outer: &krabka_security::Principal,
) -> Result<Unwrapped, EnvelopeError> {
    // `ApiKeys.ENVELOPE.clusterAction` is true: only a peer that holds
    // `ClusterAction` on the cluster resource may speak for another identity.
    let image = broker.controller.current_image();
    if crate::authorizer::AuthorizationResult::Deny
        == broker.config.authorizer.authorize(
            image.as_ref(),
            &crate::authorizer::AuthorizationRequest {
                principal: outer,
                host: &request.peer,
                resource_type: krabka_metadata::ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: krabka_metadata::AclOperation::ClusterAction,
            },
        )
    {
        return Err(EnvelopeError::ClusterAuthorizationFailed);
    }

    let envelope = envelope::decode_request(&request.body, request.api_version)?;
    let forwarded_principal =
        envelope::deserialize_principal(envelope.request_principal.as_deref())?;
    let client_host = envelope::deserialize_client_host_address(&envelope.client_host_address)?;
    let forwarded = envelope::unwrap_request(&envelope.request_data, |api_key, api_version| {
        broker.handlers().body_flexible(api_key, api_version)
    })?;
    // The embedded key is forwardable in Kafka's table; it still has to be one
    // this broker serves, at a version it serves, before a handler sees it.
    let entry = broker
        .handlers()
        .get(forwarded.api_key)
        .ok_or(EnvelopeError::InvalidRequest)?;
    if !entry.supports_version(forwarded.api_version) {
        return Err(EnvelopeError::UnsupportedVersion);
    }

    Ok(Unwrapped {
        forwarded,
        principal: krabka_security::Principal {
            name: forwarded_principal.name,
            // `KafkaPrincipal` carries no mechanism and no groups, so the
            // reconstructed session keeps the forwarding hop's own auth method
            // and an empty group list. Only the admin audit trail reads the
            // method, and no authorizer reads the groups.
            auth_method: outer.auth_method,
            groups: Vec::new(),
        },
        client_host: std::net::SocketAddr::new(client_host, 0),
        token_authenticated: forwarded_principal.token_authenticated,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::check;
    use bytes::Bytes;

    use super::*;

    #[test]
    fn supported_set_matches_kip_919_shared_handlers_plus_envelope() {
        let keys: BTreeSet<_> = SUPPORTED_APIS.iter().map(|api| api.api_key).collect();

        check!(keys.len() == SUPPORTED_APIS.len());
        check!(
            keys == maplit::btreeset! {
                29, 30, 31, 32, 33, 41, 43, 44, 45, 46, 48, 49, 50, 57, 58, 64,
            }
        );
    }

    /// `Envelope` is advertised at the versions `kafka-clients-4.3.1.jar`
    /// publishes: `ApiKeys.ENVELOPE` is `0..=0`, flexible from 0. The
    /// controller listener merges this row into its `ApiVersions` table and
    /// frames the request header from `flexible_min`, so a wrong minimum here
    /// eats the first byte of the envelope body.
    #[test]
    fn envelope_is_advertised_at_the_versions_kafka_publishes() {
        let envelope = SUPPORTED_APIS
            .iter()
            .find(|api| api.api_key == ENVELOPE_API_KEY)
            .copied();

        check!(
            envelope
                == Some(ControllerApiVersion {
                    api_key: 58,
                    min_version: 0,
                    max_version: 0,
                    flexible_min: 0,
                })
        );
    }

    fn request(api_key: i16, api_version: i16) -> ControllerAdminRequest {
        ControllerAdminRequest {
            api_key,
            api_version,
            correlation_id: 1,
            client_id: None,
            body: Bytes::new(),
            peer: "127.0.0.1:9093".parse().unwrap(),
            principal: None,
            authenticated_via_token: false,
        }
    }

    #[tokio::test]
    async fn unsupported_api_falls_through_before_broker_binding() {
        let router = BrokerControllerAdminRouter::new();

        check!(router.route(request(2, 0)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn unsupported_version_falls_through_before_broker_binding() {
        let router = BrokerControllerAdminRouter::new();
        let update_features = SUPPORTED_APIS.iter().find(|api| api.api_key == 57).unwrap();

        check!(
            router
                .route(request(57, update_features.max_version + 1))
                .await
                .unwrap()
                .is_none()
        );
    }
}
