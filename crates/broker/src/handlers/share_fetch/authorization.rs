//! The two gates a `ShareFetch` passes before it may acquire anything: the
//! group-membership check against the live share actor, and the per-topic
//! `Read` ACL.
//!
//! They are the only part of the handler that consults the authorizer and the
//! group coordinator, so they sit apart from the acquisition machinery.

use krabka_metadata::{AclOperation, MetadataImage, ResourceType};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    coordinator::unified::share::actor::ShareGroupActorMessage,
    handlers::RequestContext,
};

pub(super) async fn member_is_valid(broker: &Broker, group: &str, member: &str) -> bool {
    let Some(handle) = broker.group_coordinator.find_share(group) else {
        return true;
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    if handle
        .tx
        .send(ShareGroupActorMessage::Describe { reply: tx })
        .await
        .is_err()
    {
        return true;
    }
    match rx.await {
        Ok(view) => view
            .members
            .iter()
            .any(|candidate| candidate.member_id == member),
        Err(_) => true,
    }
}

/// Reports whether the per-topic `Read` ACL denies this row.
pub(super) fn topic_read_denied(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    topic_name: Option<&str>,
) -> bool {
    match topic_name {
        Some(name) => {
            broker.config.authorizer.authorize(
                image,
                &AuthorizationRequest {
                    principal: ctx.principal,
                    host: ctx.peer,
                    resource_type: ResourceType::Topic,
                    resource_name: name,
                    operation: AclOperation::Read,
                },
            ) == AuthorizationResult::Deny
        }
        // Unknown topic_id: no name to key the ACL by; treated as denied so
        // we never serve data for an unresolvable topic.
        None => true,
    }
}
