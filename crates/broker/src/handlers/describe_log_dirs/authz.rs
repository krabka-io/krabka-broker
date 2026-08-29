//! The whole-request `DescribeLogDirs` ACL gate and the response a Deny gets.
//!
//! `DescribeLogDirs` is authorized once per request rather than per directory,
//! because the reply describes the broker rather than any one topic. That makes
//! the gate and its refusal shape a concern of their own, separate from the
//! directory scan.

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::describe_log_dirs_response::DescribeLogDirsResponse;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    codes,
    error::BrokerError,
};

/// Gate for `Describe` on `Cluster("kafka-cluster")`.
///
/// Returns `true` when the authorizer denies the operation.
pub(super) fn cluster_describe_denied(
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
            operation: AclOperation::Describe,
        },
    ) == AuthorizationResult::Deny
}

/// Whole-response `CLUSTER_AUTHORIZATION_FAILED (31)` response for a Deny.
pub(super) fn denied_response(version: i16) -> Result<Bytes, BrokerError> {
    let resp = DescribeLogDirsResponse {
        throttle_time_ms: 0,
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        results: Vec::new(),
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{
        Decode,
        owned::describe_log_dirs_response::{self, DescribeLogDirsResponse},
    };

    use super::*;

    #[test]
    fn cluster_describe_denied_yields_cluster_authorization_failed() {
        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        assert!(cluster_describe_denied(
            &authorizer,
            &image,
            &principal,
            &peer
        ));

        let bytes = denied_response(describe_log_dirs_response::MAX_VERSION).expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp =
            DescribeLogDirsResponse::decode(&mut cur, describe_log_dirs_response::MAX_VERSION)
                .unwrap();
        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    }
}
