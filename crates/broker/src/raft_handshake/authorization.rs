//! The per-request cluster grants of a controller-listener connection.
//!
//! Kafka's `ControllerApis` authorizes every request against the connection
//! principal. [`ControllerPeerGrants`] holds that principal and asks the
//! broker authorizer for each request, against the controller's current
//! metadata image, so an ACL change applies to the next request of an open
//! connection.

use std::sync::Arc;

use krabka_metadata::{AclOperation, ResourceType};
use krabka_raft::{ClusterGrants, ClusterOperation};

use super::ControllerHandleArc;
use crate::authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer};

/// The cluster grants of one controller-listener connection.
pub(super) struct ControllerPeerGrants {
    pub(super) authorizer: Arc<dyn Authorizer>,
    pub(super) controller: ControllerHandleArc,
    /// The SASL principal, the mTLS principal, or `ANONYMOUS`.
    pub(super) principal: krabka_security::Principal,
    pub(super) peer: std::net::SocketAddr,
}

impl ClusterGrants for ControllerPeerGrants {
    fn allows(&self, operation: ClusterOperation) -> bool {
        // The controller handle is late-bound. Before it is set the
        // controller cannot serve, so deny.
        let Some(controller) = self.controller.get() else {
            return false;
        };
        let operation = match operation {
            ClusterOperation::ClusterAction => AclOperation::ClusterAction,
            ClusterOperation::Alter => AclOperation::Alter,
            ClusterOperation::Describe => AclOperation::Describe,
        };
        let image = controller.current_image();
        self.authorizer.authorize(
            &*image,
            &AuthorizationRequest {
                principal: &self.principal,
                host: &self.peer,
                resource_type: ResourceType::Cluster,
                resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
                operation,
            },
        ) == AuthorizationResult::Allow
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use tokio::sync::OnceCell;

    use super::*;
    use crate::test_support::GrantsInPrincipalName;

    /// Each cluster operation maps to the ACL operation of the same name, the
    /// principal and peer of the connection reach the authorizer, and a
    /// connection that arrives before the controller handle is set is denied.
    #[tokio::test]
    async fn grants_ask_the_authorizer_for_each_operation() {
        let dir = tempfile::tempdir().expect("tempdir");
        let controller = Arc::new(
            krabka_raft::Controller::start(krabka_raft::ControllerConfig::for_tests(
                krabka_raft::NodeId(1),
                dir.path().to_path_buf(),
            ))
            .await
            .expect("controller"),
        );
        let bound: ControllerHandleArc = Arc::new(OnceCell::new());
        check!(bound.set(Arc::clone(&controller)).is_ok());
        let grants = |controller: &ControllerHandleArc, name: &str| ControllerPeerGrants {
            authorizer: Arc::new(GrantsInPrincipalName),
            controller: Arc::clone(controller),
            principal: crate::test_support::principal(name),
            peer: crate::test_support::peer(),
        };

        let operations = [
            ClusterOperation::ClusterAction,
            ClusterOperation::Alter,
            ClusterOperation::Describe,
        ];
        let cases = [
            ("none", [false, false, false]),
            ("Cluster:ClusterAction", [true, false, false]),
            ("Cluster:Alter", [false, true, false]),
            ("Cluster:Describe", [false, false, true]),
            ("Topic:ClusterAction+Group:Alter", [false, false, false]),
        ];
        for (name, expected) in cases {
            let connection = grants(&bound, name);
            check!(
                operations.map(|operation| connection.allows(operation)) == expected,
                "{name}"
            );
        }

        let unbound: ControllerHandleArc = Arc::new(OnceCell::new());
        let early = grants(
            &unbound,
            "Cluster:ClusterAction+Cluster:Alter+Cluster:Describe",
        );
        check!(operations.map(|operation| early.allows(operation)) == [false, false, false]);

        drop(bound);
        let controller = Arc::try_unwrap(controller)
            .unwrap_or_else(|_| panic!("controller handle still shared after the test"));
        controller.shutdown().await;
    }
}
