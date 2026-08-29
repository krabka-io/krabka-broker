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
