//! The ACL preamble of `BrokerHeartbeat`: the `ClusterAction` gate on
//! `Cluster("kafka-cluster")` and the whole-response body a Deny produces.
//!
//! `BrokerHeartbeat` is an inter-broker control-plane RPC, so it is the
//! `ClusterAction` operation rather than one of the client operations.

use bytes::Bytes;
use krabka_metadata::{AclOperation, MetadataImage, ResourceType};

use super::response::{denied_response_body, encode_response};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    error::BrokerError,
};

/// `ClusterAction` on `Cluster("kafka-cluster")` gate. Returns `true`
/// when the authorizer denies the principal for this inter-broker
/// control-plane RPC.
pub(super) fn cluster_action_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &MetadataImage,
    principal: &krabka_security::Principal,
    host: &std::net::SocketAddr,
) -> bool {
    authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::ClusterAction,
        },
    ) == AuthorizationResult::Deny
}

/// Whole-response `CLUSTER_AUTHORIZATION_FAILED (31)` response, built on Deny.
pub(super) fn denied_response(version: i16) -> Result<Bytes, BrokerError> {
    encode_response(version, &denied_response_body())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::Decode;

    use super::*;
    use crate::codes;

    /// With empty ACLs and no super-users, the authorizer denies
    /// `ClusterAction` to every principal, so the denied response carries
    /// `CLUSTER_AUTHORIZATION_FAILED`.
    #[test]
    fn cluster_action_denied_yields_cluster_authorization_failed() {
        use krabka_protocol::owned::broker_heartbeat_response::{self, BrokerHeartbeatResponse};

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = MetadataImage::new(uuid::Uuid::nil());
        let principal = krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        assert!(cluster_action_denied(
            &authorizer,
            &image,
            &principal,
            &peer
        ));

        let bytes = denied_response(broker_heartbeat_response::MAX_VERSION).expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp =
            BrokerHeartbeatResponse::decode(&mut cur, broker_heartbeat_response::MAX_VERSION)
                .unwrap();
        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
        assert!(resp.is_fenced);
    }

    #[test]
    fn cluster_action_allowed_by_allow_all_authorizer() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        let principal = krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        assert!(!cluster_action_denied(
            &crate::authorizer::AllowAllAuthorizer,
            &image,
            &principal,
            &peer
        ));
    }
}
