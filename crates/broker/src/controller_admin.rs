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

/// The Kafka 4.x controller-listener surface, in `api_key` order.
///
/// Apache Kafka marks each request schema with the listeners that accept it,
/// and a controller's `ApiVersionManager` advertises exactly the ones tagged
/// `controller`. A live `mirror.gcr.io/apache/kafka:4.3.1` controller answers
/// `ApiVersions` with 1, 17-20, 29-33, 36-41, 43-46, 49-60, 62-64, 67, 70, 73
/// and 80-82; 4.0.0's schemas carry the same tags.
///
/// This table is that set minus the RPCs the controller listener already
/// answers without a broker handler: `Fetch`, `ApiVersions`, the KIP-595
/// quorum RPCs, `FetchSnapshot`, `DescribeCluster`, broker and controller
/// registration, and the KIP-853 voter RPCs. What remains is the subset that
/// reuses a broker handler, which is what this router bridges to.
///
/// `BrokerHeartbeat` is the one entry here that is not an Admin API, and it
/// belongs for the same reason the Admin subset does: KIP-919 puts it on the
/// controller listener, and only the broker crate holds what answering it
/// takes -- the heartbeat registry that decides fencing, the KIP-112
/// offline-dir failover, and the controlled-shutdown drain. Routing it here is
/// what keeps a heartbeat sent to a controller-only node from being answered
/// by a handler that does none of that.
///
/// `Envelope` is the other entry that is not an Admin API, and it is the one
/// whose payload is another API's request rather than a body a handler reads,
/// so [`serve_envelope`] answers it instead of the registry lookup the rest
/// share. `ApiKeys.ENVELOPE.messageType.listeners()` in
/// `kafka-clients-4.3.1.jar` is exactly `[CONTROLLER]`, so it is advertised
/// here and never on the client listener.
///
/// Four of Kafka's keys are in neither list, so krabka's controller listener
/// advertises 37 of the 41. `SaslHandshake` and `SaslAuthenticate` are
/// consumed by `BrokerRaftHandshake` before the controller server sees the
/// stream, so the listener speaks them without listing them. `AlterPartition`
/// and `AllocateProducerIds` do have broker handlers, but krabka's brokers
/// send both to a controller's *broker* endpoint rather than to its controller
/// listener, so routing them here would advertise a path nothing takes -- a
/// forwarded `AllocateProducerIds` still reaches its handler, through the
/// `Envelope` above rather than through a key of its own.
/// `controller_listener_advertises_no_key_kafka_does_not` pins that shortfall.
///
/// `DescribeClientQuotas` is deliberately absent for a different reason: its
/// schema is tagged `broker` only, so a Kafka controller neither advertises
/// nor answers it, and neither does this listener.
const SUPPORTED_APIS: &[ControllerApiVersion] = &[
    api_version!(create_topics_request),
    api_version!(delete_topics_request),
    api_version!(describe_acls_request),
    api_version!(create_acls_request),
    api_version!(delete_acls_request),
    api_version!(describe_configs_request),
    api_version!(alter_configs_request),
    api_version!(create_partitions_request),
    api_version!(create_delegation_token_request),
    api_version!(renew_delegation_token_request),
    api_version!(expire_delegation_token_request),
    api_version!(describe_delegation_token_request),
    api_version!(elect_leaders_request),
    api_version!(incremental_alter_configs_request),
    api_version!(alter_partition_reassignments_request),
    api_version!(list_partition_reassignments_request),
    api_version!(alter_client_quotas_request),
    api_version!(describe_user_scram_credentials_request),
    api_version!(alter_user_scram_credentials_request),
    api_version!(update_features_request),
    api_version!(envelope_request),
    api_version!(broker_heartbeat_request),
    api_version!(unregister_broker_request),
    api_version!(assign_replicas_to_dirs_request),
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
                // connection rather than inventing a response body. Send a
                // `Metadata` request to a live
                // `mirror.gcr.io/apache/kafka:4.3.1` controller listener and
                // it half-closes too: no error frame comes back.
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
                    // This request arrived on the controller listener itself,
                    // which already ran the `ClusterAction` gate for the whole
                    // connection.
                    listener_authorized_cluster_action: true,
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
    /// Whether the listener has already authorized this identity for
    /// `ClusterAction`, which is what lets `BrokerHeartbeat` skip its own ACL
    /// gate. See [`RequestContext::listener_authorized_cluster_action`].
    ///
    /// False on the `Envelope` path, and it has to be: the listener authorized
    /// the *forwarding broker*, while the embedded request runs as the client
    /// the envelope names. Carrying the flag across would hand every forwarded
    /// client the inter-broker control plane.
    listener_authorized_cluster_action: bool,
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
        listener_authorized_cluster_action,
    } = invocation;
    let entry = broker.handlers().get(api_key).ok_or_else(|| {
        crate::error::BrokerError::UnsupportedApi {
            api_key,
            version: api_version,
        }
    })?;
    match entry.kind() {
        // A plain handler takes no session at all. `AssignReplicasToDirs` (73)
        // reaches it straight off the listener, and `AllocateProducerIds` (67)
        // through the `Envelope` path -- 67 is the one api key that is both
        // `ApiKeys.forwardable` and registered this way, and a JVM broker
        // forwards it to obtain a producer-id block before any producer of its
        // own can initialise, so the `Envelope` path has to reach it. The
        // envelope's `ClusterAction` gate has already run by the time this is
        // called.
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
            let context = if listener_authorized_cluster_action {
                context.listener_authorized_for_cluster_action()
            } else {
                context
            };
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
                    // The listener authorized the forwarding broker for
                    // `ClusterAction`, not the client this request runs as, so
                    // the embedded handler faces its own ACL gate.
                    listener_authorized_cluster_action: false,
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

    /// The Kafka 4.x controller-listener set, as a live
    /// `mirror.gcr.io/apache/kafka:4.3.1` controller advertises it (the same
    /// set the 4.0.0 request schemas tag `controller`), minus the keys the
    /// controller listener answers without this router and the four it does
    /// not answer at all.
    #[test]
    fn supported_set_matches_the_kafka_controller_listener_surface() {
        let keys: BTreeSet<_> = SUPPORTED_APIS.iter().map(|api| api.api_key).collect();

        check!(keys.len() == SUPPORTED_APIS.len());
        check!(
            keys == maplit::btreeset! {
                19, 20, 29, 30, 31, 32, 33, 37, 38, 39, 40, 41, 43, 44, 45, 46, 49, 50, 51, 57, 58,
                63, 64, 73,
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

    /// A supported key that arrives before [`BrokerControllerAdminRouter::bind`]
    /// has run is an error, not a fall-through.
    ///
    /// The controller starts ahead of the broker -- that ordering is why the
    /// binding is a `OnceLock` at all -- so the listener can accept an Admin
    /// request with nothing behind the router yet. `Ok(None)` is the wrong
    /// answer for it: that is what an api this listener does not speak returns,
    /// and it half-closes the connection the way a Kafka controller does for a
    /// disabled api, which tells a client the cluster will never serve the key.
    /// A transient startup gap has to read as a failure of this request
    /// instead.
    #[tokio::test]
    async fn a_supported_key_before_the_broker_binds_is_an_error() {
        let router = BrokerControllerAdminRouter::new();

        let answer = router
            .route(request(
                krabka_protocol::owned::create_topics_request::API_KEY,
                krabka_protocol::owned::create_topics_request::MAX_VERSION,
            ))
            .await;

        check!(
            answer.err().map(|error| error.to_string())
                == Some("controller admin dispatch: broker startup is incomplete".to_owned())
        );
    }

    /// Every key that can reach [`invoke_registered_handler`] resolves to a
    /// registered handler of a kind it can call.
    ///
    /// Two of that function's arms answer `UnsupportedApi`: one for a key the
    /// registry does not hold, one for a key whose [`DispatchKind`] carries a
    /// session this path cannot build. Both are unreachable, and this is the
    /// invariant that makes them so -- which is worth pinning where the arms
    /// themselves are not, because breaking it is silent. A key dropped from
    /// the registry, or a handler re-registered as `Produce`, `Telemetry`,
    /// `Fetch` or `SaslMetadata`, turns a served api into an `UnsupportedApi`
    /// that fails the controller connection, and every other test in the tree
    /// would still pass.
    ///
    /// Two ways in, so two sets. Off the listener, the keys [`SUPPORTED_APIS`]
    /// advertises, `Envelope` aside -- [`serve_envelope`] answers that one
    /// rather than the registry. Through an `Envelope`, the forwardable keys
    /// this broker serves, since [`unwrap_envelope`] refuses every other
    /// embedded key before dispatch.
    #[test]
    fn every_key_that_reaches_a_handler_has_one_this_path_can_call() {
        let registry = crate::handlers::registry::build_registry();

        let reachable: BTreeSet<ApiKeyCode> = SUPPORTED_APIS
            .iter()
            .map(|api| api.api_key)
            .filter(|api_key| *api_key != ENVELOPE_API_KEY)
            .chain(
                registry
                    .registered_api_keys()
                    .filter(|api_key| envelope::is_forwardable(*api_key)),
            )
            .collect();

        let undispatchable: BTreeSet<ApiKeyCode> = reachable
            .iter()
            .copied()
            .filter(|api_key| {
                !registry.get(*api_key).is_some_and(|entry| {
                    matches!(
                        entry.kind(),
                        DispatchKind::Plain(_) | DispatchKind::Context(_) | DispatchKind::Auth(_)
                    )
                })
            })
            .collect();

        check!(undispatchable == BTreeSet::new());
        check!(
            reachable.len() == 27,
            "the 23 advertised keys, plus the four forwardable ones this \
             broker serves without advertising them here: DescribeQuorum, \
             AllocateProducerIds, AddRaftVoter and RemoveRaftVoter"
        );
    }

    /// `AssignReplicasToDirs` is the one advertised key whose broker handler
    /// takes no principal and no connection, so it is the only key that
    /// reaches the [`DispatchKind::Plain`] arm straight off the listener
    /// rather than through an `Envelope`. Route it against a bound broker and
    /// decode
    /// what comes back, which is what proves the arm hands the body to the
    /// registry rather than falling through to the incompatible-kind error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_plain_kind_api_key_routes_to_the_broker_registry() {
        use krabka_protocol::{Decode as _, Encode as _};

        let dir = tempfile::TempDir::new().expect("tempdir");
        let data_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind data listener");
        let controller_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind controller listener");
        let data_addr = data_listener.local_addr().expect("data addr");
        let controller_addr = controller_listener.local_addr().expect("controller addr");
        let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        config.listen_addr = data_addr;
        config.advertised_listener = data_addr.to_string();
        config.controller_listen_addr = controller_addr;
        config.controller_quorum_voters =
            vec![(krabka_raft::NodeId(1), controller_addr.to_string())];
        let handle = crate::Broker::start_with_listeners(
            config,
            Some(controller_listener),
            Some(data_listener),
        )
        .await
        .expect("broker start");
        handle.wait_until_controller_leader().await;

        let router = BrokerControllerAdminRouter::new();
        router.bind(handle.broker_for_test()).expect("bind once");

        let api_key = krabka_protocol::owned::assign_replicas_to_dirs_request::API_KEY;
        let api_version = krabka_protocol::owned::assign_replicas_to_dirs_request::MAX_VERSION;
        let mut body = bytes::BytesMut::new();
        krabka_protocol::owned::assign_replicas_to_dirs_request::AssignReplicasToDirsRequest {
            broker_id: 1,
            broker_epoch: -1,
            directories: Vec::new(),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields::default(),
        }
        .encode(&mut body, api_version)
        .expect("encode the request body");

        let answer = router
            .route(ControllerAdminRequest {
                body: body.freeze(),
                ..request(api_key, api_version)
            })
            .await
            .expect("the plain arm routes without error")
            .expect("a routed key answers with a body");

        let mut cursor = answer.body.clone();
        let decoded = krabka_protocol::owned::assign_replicas_to_dirs_response::AssignReplicasToDirsResponse::decode(
            &mut cursor,
            api_version,
        )
        .expect("decode the routed response");

        check!(answer.flexible);
        check!(
            decoded
                == krabka_protocol::owned::assign_replicas_to_dirs_response::AssignReplicasToDirsResponse::default()
        );
        handle.shutdown().await;
    }
}
