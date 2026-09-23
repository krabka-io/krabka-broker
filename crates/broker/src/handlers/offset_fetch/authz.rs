//! The group-level `Describe` gate that every `OffsetFetch` request passes
//! through, on both the legacy and the KIP-516 request shapes.
//!
//! Committed offsets belong to a group, so the first authorization decision is
//! always `Describe` on `Group(group_id)`; the per-topic `Read` checks that
//! follow are made where the topic rows are built. Keeping the group decision
//! in one place is what lets the v0 to v7 and v8 and above paths apply it
//! identically.

use krabka_metadata::{AclOperation, ResourceType};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
};

/// Reports whether the principal may `Describe` the group, the gate that
/// precedes every committed-offset read. A denial makes the whole response
/// (legacy shape) or the group's entry (v8 and above) carry
/// `GROUP_AUTHORIZATION_FAILED`.
pub(super) fn group_authorized(
    broker: &Broker,
    context: &crate::handlers::RequestContext<'_>,
    group_id: &str,
) -> bool {
    broker.config.authorizer.authorize(
        &*broker.controller.current_image(),
        &AuthorizationRequest {
            principal: context.principal,
            host: context.peer,
            resource_type: ResourceType::Group,
            resource_name: group_id,
            operation: AclOperation::Describe,
        },
    ) == AuthorizationResult::Allow
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
    async fn group_authorized_reports_authorizer_outcome() {
        let (denied_handle, _dir1) = start_broker(Arc::new(DenyAll)).await;
        let denied_broker = denied_handle.broker_arc_for_test();
        let (allowed_handle, _dir2) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let allowed_broker = allowed_handle.broker_arc_for_test();

        let p = principal("alice");
        let peer_addr = peer();
        let ctx = request_context(&p, &peer_addr, "offset-fetch-authz-test");

        check!(!group_authorized(&denied_broker, &ctx, "my-group"));
        check!(group_authorized(&allowed_broker, &ctx, "my-group"));
    }
}
