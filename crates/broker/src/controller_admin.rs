//! KIP-919 Admin RPC routing for the controller listener.

use std::sync::{Arc, OnceLock, Weak};

use krabka_raft::{
    ControllerAdminRequest, ControllerAdminResponse, ControllerAdminRouteFuture,
    ControllerAdminRouter, ControllerApiVersion, RaftError,
};

use crate::{
    broker::Broker,
    handlers::{DispatchKind, RequestContext},
    network::auth::ConnectionAuth,
};

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

/// The Kafka 4.x controller-listener Admin surface, in `api_key` order.
///
/// Apache Kafka marks each request schema with the listeners that accept it,
/// and a controller's `ApiVersionManager` advertises exactly the ones tagged
/// `controller`. A live `mirror.gcr.io/apache/kafka:4.3.1` controller answers
/// `ApiVersions` with 1, 17-20, 29-33, 36-41, 43-46, 49-60, 62-64, 67, 70, 73
/// and 80-82; 4.0.0's schemas carry the same tags.
///
/// This table is that set minus the RPCs `krabka-raft` serves itself: `Fetch`,
/// `ApiVersions`, the SASL handshake pair, the KIP-595 quorum RPCs,
/// `AlterPartition`, `Envelope`, `FetchSnapshot`, `DescribeCluster`,
/// `AllocateProducerIds`, broker and controller registration, and the KIP-853
/// voter RPCs. What remains is the subset that reuses a broker handler, which
/// is what this router bridges to.
///
/// `DescribeClientQuotas` is deliberately absent: its schema is tagged
/// `broker` only, so a Kafka controller neither advertises nor answers it.
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

            let entry = broker.handlers().get(request.api_key).ok_or_else(|| {
                RaftError::ControllerAdmin(format!(
                    "api_key {} is missing from the broker registry",
                    request.api_key
                ))
            })?;
            let principal = request
                .principal
                .unwrap_or_else(|| krabka_security::Principal {
                    name: "ANONYMOUS".into(),
                    auth_method: krabka_security::AuthMethod::Anonymous,
                    groups: Vec::new(),
                });
            let client_id = request.client_id.as_deref().unwrap_or("");

            let result = match entry.kind() {
                DispatchKind::Plain(handler) => {
                    handler(
                        &broker,
                        request.api_version,
                        request.correlation_id,
                        &request.body,
                    )
                    .await
                }
                DispatchKind::Context(handler) => {
                    let context = RequestContext::new(
                        &principal,
                        &request.peer,
                        client_id,
                        "controller-admin",
                        false,
                        "CONTROLLER",
                    );
                    handler(
                        &broker,
                        request.api_version,
                        request.correlation_id,
                        &request.body,
                        &context,
                    )
                    .await
                }
                DispatchKind::Auth(handler) => {
                    let auth = ConnectionAuth::Authenticated {
                        principal,
                        mechanism: krabka_security::SaslMechanism::Plain,
                        expires_at_ms: None,
                        authenticated_via_token: request.authenticated_via_token,
                    };
                    handler(
                        &broker,
                        request.api_version,
                        request.correlation_id,
                        &request.body,
                        &auth,
                        &request.peer,
                    )
                    .await
                }
                _ => {
                    return Err(RaftError::ControllerAdmin(format!(
                        "api_key {} has an incompatible handler kind",
                        request.api_key
                    )));
                }
            }
            .map_err(|error| RaftError::ControllerAdmin(error.to_string()))?;

            Ok(Some(ControllerAdminResponse {
                body: result,
                flexible: request.api_version >= version.flexible_min,
            }))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::check;
    use bytes::Bytes;

    use super::*;

    /// The Kafka 4.x controller-listener set, as a live
    /// `mirror.gcr.io/apache/kafka:4.3.1` controller advertises it (the same
    /// set the 4.0.0 request schemas tag `controller`), minus the keys
    /// `krabka-raft` answers without this router.
    #[test]
    fn supported_set_matches_the_kafka_controller_listener_surface() {
        let keys: BTreeSet<_> = SUPPORTED_APIS.iter().map(|api| api.api_key).collect();

        check!(keys.len() == SUPPORTED_APIS.len());
        check!(
            keys == maplit::btreeset! {
                19, 20, 29, 30, 31, 32, 33, 37, 38, 39, 40, 41, 43, 44, 45, 46, 49, 50, 51, 57, 64,
                73,
            }
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
