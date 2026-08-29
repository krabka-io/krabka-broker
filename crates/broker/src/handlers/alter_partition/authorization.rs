//! The whole-request authorization gate of `AlterPartition` and the reply it
//! produces on a Deny.
//!
//! `AlterPartition` is an inter-broker control-plane RPC, so Kafka checks
//! `ClusterAction` on the `Cluster` resource once for the request. A denial
//! fails the whole response rather than any individual partition row, which
//! keeps the gate and its canned reply together in one module.

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    UnknownTaggedFields, owned::alter_partition_response::AlterPartitionResponse,
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    codes,
    error::BrokerError,
};

/// Gate for `ClusterAction` on `Cluster("kafka-cluster")`. It returns `true`
/// when the authorizer denies the principal. This is an inter-broker
/// control-plane RPC.
pub(super) fn cluster_action_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
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

/// Builds the whole-response `CLUSTER_AUTHORIZATION_FAILED (31)` reply for a
/// Deny decision.
pub(super) fn denied_response(version: i16) -> Result<Bytes, BrokerError> {
    super::encode_resp(
        version,
        &AlterPartitionResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            topics: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::MetadataImage;
    use krabka_protocol::{Decode, owned::alter_partition_response};

    use super::*;

    /// With empty ACLs and no super-users, the authorizer denies
    /// `ClusterAction` to every principal, so the denied response carries
    /// `CLUSTER_AUTHORIZATION_FAILED`.
    #[test]
    fn cluster_action_denied_yields_cluster_authorization_failed() {
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

        let bytes = denied_response(alter_partition_response::MAX_VERSION).expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = AlterPartitionResponse::decode(&mut cur, alter_partition_response::MAX_VERSION)
            .unwrap();
        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    }

    #[test]
    fn cluster_action_allowed_does_not_yield_denial() {
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
