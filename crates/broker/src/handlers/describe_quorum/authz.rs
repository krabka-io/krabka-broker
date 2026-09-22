//! The whole-request `Cluster` `Describe` gate for `DescribeQuorum`.
//!
//! `DescribeQuorum` is cluster-wide raft introspection rather than a per-topic
//! read, so it is authorized once for the request with the same gate
//! `DescribeCluster` uses. Keeping that decision here leaves the response
//! builders free of authorization concerns.

use krabka_metadata::{AclOperation, MetadataImage, ResourceType};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
};

/// Reports whether the principal is denied `Describe` on the cluster
/// resource. A denial makes the whole response carry
/// `CLUSTER_AUTHORIZATION_FAILED` with no topic rows.
pub(super) fn cluster_describe_denied(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
) -> bool {
    let allow = broker.config.authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Describe,
        },
    );
    allow == AuthorizationResult::Deny
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;

    use super::*;
    use crate::test_support::{
        DenyAll, peer, principal, request_context,
        start_broker_with_authorizer_no_audit as start_broker,
    };

    #[tokio::test]
    async fn cluster_describe_denied_reports_authorizer_outcome() {
        let (denied_handle, _dir1) = start_broker(Arc::new(DenyAll)).await;
        let denied_broker = denied_handle.broker_arc_for_test();
        let (allowed_handle, _dir2) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let allowed_broker = allowed_handle.broker_arc_for_test();

        let p = principal("alice");
        let peer_addr = peer();
        let ctx = request_context(&p, &peer_addr, "describe-quorum-authz-test");

        let image = denied_broker.controller.current_image();
        check!(cluster_describe_denied(&denied_broker, &image, &ctx));
        check!(!cluster_describe_denied(&allowed_broker, &image, &ctx));
    }
}
