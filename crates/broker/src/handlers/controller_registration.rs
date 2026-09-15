//! `ControllerRegistration` (`api_key=70`). KIP-919 controller registration.
//!
//! As Kafka's `ClusterControlManager.registerController` does, this registers
//! any controller id the request names (a KIP-853 controller joins the voter
//! set after it registers), refuses a `metadata.version` below 3.7-IV0 (level
//! 15) with `UNSUPPORTED_VERSION`, and writes `zkMigrationReady` false.

use std::collections::{BTreeMap, HashSet};

use bytes::Bytes;
use krabka_metadata::{
    AclOperation, BrokerEndpoint, ControllerRegistrationRecord, MetadataRecord, NodeId,
    ResourceType,
};
use krabka_protocol::{
    Decode,
    owned::{
        controller_registration_request::ControllerRegistrationRequest,
        controller_registration_response::ControllerRegistrationResponse,
    },
};
use krabka_raft::RaftError;
use krabka_security::ListenerProtocol;

use crate::{broker::Broker, codes, error::BrokerError, handlers::RequestContext};

pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur = req_bytes;
    let req = ControllerRegistrationRequest::decode(&mut cur, version)?;
    let image = broker.controller.current_image();
    if crate::handlers::acl_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        ResourceType::Cluster,
        crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
        AclOperation::ClusterAction,
    ) {
        return response(
            version,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("cluster action denied".into()),
        );
    }
    if broker.controller.watch_leader().borrow().as_ref() != Some(&broker.config.node_id) {
        return response(version, codes::NOT_CONTROLLER, None);
    }
    // Kafka's `MetadataVersion.isControllerRegistrationSupported`. An image
    // with no finalized level runs at the latest level, as a bootstrap does.
    if image
        .finalized_metadata_version()
        .unwrap_or(krabka_metadata::metadata_version::METADATA_VERSION_MAX)
        < krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL
    {
        return response(
            version,
            codes::UNSUPPORTED_VERSION,
            Some(
                "The current MetadataVersion is too old to support controller registrations."
                    .into(),
            ),
        );
    }

    let node_id = match u64::try_from(req.controller_id) {
        Ok(id) => NodeId(id),
        Err(_) => {
            return response(
                version,
                codes::INVALID_REGISTRATION,
                Some("controller id must be non-negative".into()),
            );
        }
    };

    let endpoints = match decode_listeners(&req.listeners) {
        Ok(endpoints) => endpoints,
        Err(message) => return response(version, codes::INVALID_REGISTRATION, Some(message)),
    };
    let features = req
        .features
        .into_iter()
        .map(|feature| {
            (
                feature.name,
                (feature.min_supported_version, feature.max_supported_version),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if features
        .iter()
        .any(|(name, (min, max))| name.is_empty() || min > max)
    {
        return response(
            version,
            codes::INVALID_REGISTRATION,
            Some("invalid controller feature range".into()),
        );
    }

    let record = ControllerRegistrationRecord {
        node_id,
        incarnation_id: uuid::Uuid::from_bytes(req.incarnation_id.0),
        // ZooKeeper migration is gone. Kafka writes false whatever the request
        // says.
        zk_migration_ready: false,
        endpoints,
        features,
    };
    if image.controller(node_id) == Some(&record) {
        return response(version, 0, None);
    }
    match broker
        .controller
        .submit_change(vec![MetadataRecord::V1ControllerRegistration(record)])
        .await
    {
        Ok(_) => response(version, 0, None),
        Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
            response(version, codes::NOT_CONTROLLER, None)
        }
        Err(RaftError::Metadata(error)) => response(
            version,
            codes::INVALID_REGISTRATION,
            Some(error.to_string()),
        ),
        Err(error) => response(
            version,
            codes::UNKNOWN_SERVER_ERROR,
            Some(error.to_string()),
        ),
    }
}

fn decode_listeners(
    listeners: &[krabka_protocol::owned::controller_registration_request::Listener],
) -> Result<Vec<BrokerEndpoint>, String> {
    if listeners.is_empty() {
        return Err("controller registration has no listeners".into());
    }
    let mut names = HashSet::with_capacity(listeners.len());
    listeners
        .iter()
        .map(|listener| {
            if listener.name.is_empty()
                || listener.host.is_empty()
                || listener.port == 0
                || !names.insert(listener.name.clone())
            {
                return Err("invalid or duplicate controller listener".into());
            }
            let protocol = match listener.security_protocol {
                0 => ListenerProtocol::Plaintext,
                1 => ListenerProtocol::Ssl,
                2 => ListenerProtocol::SaslPlaintext,
                3 => ListenerProtocol::SaslSsl,
                _ => return Err("unknown controller listener security protocol".into()),
            };
            Ok(BrokerEndpoint {
                name: listener.name.clone(),
                host: listener.host.clone(),
                port: listener.port,
                protocol,
            })
        })
        .collect()
}

fn response(
    version: i16,
    error_code: i16,
    error_message: Option<String>,
) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(
        &ControllerRegistrationResponse {
            error_code,
            error_message,
            ..Default::default()
        },
        version,
    )
}

#[cfg(test)]
mod tests {
    use krabka_protocol::owned::controller_registration_request::Listener;

    use super::*;

    crate::test_support::wire_helpers!(
        ControllerRegistrationRequest,
        ControllerRegistrationResponse,
        client_id = "controller"
    );

    #[test]
    fn controller_listener_validation_is_strict() {
        let listener = Listener {
            name: "CONTROLLER".into(),
            host: "controller-1".into(),
            port: 9093,
            security_protocol: 0,
            ..Default::default()
        };
        assert2::assert!(decode_listeners(std::slice::from_ref(&listener)).is_ok());
        assert2::assert!(decode_listeners(&[listener.clone(), listener]).is_err());
    }

    /// A controller that is not a voter registers, with `zkMigrationReady`
    /// false whatever it sent.
    #[tokio::test]
    async fn a_controller_that_is_not_a_voter_registers() {
        use std::{net::SocketAddr, sync::Arc};

        use krabka_security::{AuthMethod, Principal};

        let (broker_handle, _dir) = crate::test_support::start_broker_with_authorizer(Arc::new(
            crate::authorizer::AllowAllAuthorizer,
        ))
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "controller".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9093".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let version = krabka_protocol::owned::controller_registration_request::MAX_VERSION;
        let listener = Listener {
            name: "CONTROLLER".into(),
            host: "controller-7".into(),
            port: 9093,
            security_protocol: 0,
            ..Default::default()
        };
        let body = encode_request(
            &ControllerRegistrationRequest {
                controller_id: 7,
                incarnation_id: krabka_protocol::primitives::uuid::Uuid([7; 16]),
                zk_migration_ready: true,
                listeners: vec![listener],
                ..Default::default()
            },
            version,
        );

        let answer = handle(&broker, version, 1, &body, &ctx)
            .await
            .expect("an answer");

        assert2::check!(
            decode_response(&answer, version) == ControllerRegistrationResponse::default()
        );
        assert2::check!(
            broker
                .controller
                .current_image()
                .controller(NodeId(7))
                .cloned()
                == Some(ControllerRegistrationRecord {
                    node_id: NodeId(7),
                    incarnation_id: uuid::Uuid::from_bytes([7; 16]),
                    zk_migration_ready: false,
                    endpoints: vec![BrokerEndpoint {
                        name: "CONTROLLER".into(),
                        host: "controller-7".into(),
                        port: 9093,
                        protocol: ListenerProtocol::Plaintext,
                    }],
                    features: BTreeMap::new(),
                })
        );
        broker_handle.shutdown().await;
    }
}
