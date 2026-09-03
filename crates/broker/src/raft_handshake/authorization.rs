//! Post-authentication authorization of a controller-listener peer (H-1).
//!
//! Authentication proves *who* the peer is; these methods decide what that
//! principal may drive. `authorize_cluster_action` is the gate the raft and
//! controller RPCs sit behind, and `authorize_cluster_alter` records whether
//! the peer may also drive the `Alter` operations that the connection carries
//! forward. Both evaluate against the controller's current metadata image, so
//! an ACL change takes effect for the next connection.

use krabka_metadata::{AclOperation, ResourceType};
use krabka_raft::RaftHandshakeError;

use super::BrokerRaftHandshake;
use crate::authorizer::{AuthorizationRequest, AuthorizationResult};

impl BrokerRaftHandshake {
    /// H-1: authorizes an authenticated controller-listener peer for
    /// controller and raft RPCs.
    ///
    /// Authentication established *who* the peer is. This method enforces that
    /// the principal holds `CLUSTER_ACTION` on `Cluster("kafka-cluster")`.
    /// That is the same gate the inter-broker control-plane RPCs use, such as
    /// `BrokerHeartbeat`. The method evaluates it against the controller's
    /// *current* metadata image, so ACL changes take effect for new
    /// connections. On Deny, the broker drops the connection.
    pub(super) fn authorize_cluster_action(
        &self,
        principal: &krabka_security::Principal,
        peer: &std::net::SocketAddr,
    ) -> Result<(), RaftHandshakeError> {
        // The image is reached through the late-bound controller handle
        // (the same cell used for SCRAM lookup). If it is not yet wired the
        // controller cannot be operating, so fail closed.
        let controller = self.controller.get().ok_or_else(|| {
            RaftHandshakeError::Sasl(
                "controller handle not initialised for CLUSTER_ACTION authorization".into(),
            )
        })?;
        let image = controller.current_image();
        let decision = self.authorizer.authorize(
            &*image,
            &AuthorizationRequest {
                principal,
                host: peer,
                resource_type: ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: AclOperation::ClusterAction,
            },
        );
        if decision == AuthorizationResult::Deny {
            tracing::warn!(
                principal = %principal.name,
                peer = %peer,
                "denying controller-listener peer: principal lacks CLUSTER_ACTION on kafka-cluster"
            );
            return Err(RaftHandshakeError::Sasl(
                "principal not authorized for CLUSTER_ACTION on the controller listener".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn authorize_cluster_alter(
        &self,
        principal: &krabka_security::Principal,
        peer: &std::net::SocketAddr,
    ) -> Result<bool, RaftHandshakeError> {
        let controller = self.controller.get().ok_or_else(|| {
            RaftHandshakeError::Sasl(
                "controller handle not initialised for Alter authorization".into(),
            )
        })?;
        let image = controller.current_image();
        Ok(self.authorizer.authorize(
            &*image,
            &AuthorizationRequest {
                principal,
                host: peer,
                resource_type: ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation: AclOperation::Alter,
            },
        ) == AuthorizationResult::Allow)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use assert2::assert;
    use krabka_security::{ListenerProtocol, SaslMechanism};
    use tokio::sync::OnceCell;

    use super::*;
    use crate::test_support::DenyAll;

    #[tokio::test]
    async fn authorize_cluster_action_denies_when_authorizer_denies() {
        let dir = tempfile::tempdir().expect("tempdir");
        let controller = Arc::new(
            krabka_raft::Controller::start(krabka_raft::ControllerConfig::for_tests(
                krabka_raft::NodeId(1),
                dir.path().to_path_buf(),
            ))
            .await
            .expect("controller"),
        );
        let controller_cell = Arc::new(OnceCell::new());
        assert!(controller_cell.set(controller.clone()).is_ok());

        let cfg = BrokerRaftHandshake {
            tls_acceptor: None,
            plain_credentials: HashMap::new(),
            enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
            gssapi: None,
            oauthbearer_validator: krabka_security::OAuthBearerValidator::default(),
            protocol: ListenerProtocol::SaslPlaintext,
            controller: controller_cell,
            audit_log: Arc::new(OnceCell::new()),
            max_frame_bytes: 4096,
            authorizer: Arc::new(DenyAll),
        };
        let principal = krabka_security::Principal {
            name: "broker".to_string(),
            auth_method: krabka_security::AuthMethod::SaslPlain,
            groups: Vec::new(),
        };
        let peer = "127.0.0.1:9092".parse().expect("peer");

        let err = cfg
            .authorize_cluster_action(&principal, &peer)
            .expect_err("deny must reject");
        assert!(matches!(err, RaftHandshakeError::Sasl(msg) if msg.contains("not authorized")));

        drop(cfg);
        let controller = Arc::try_unwrap(controller)
            .unwrap_or_else(|_| panic!("controller handle still shared after auth test"));
        controller.shutdown().await;
    }
}
