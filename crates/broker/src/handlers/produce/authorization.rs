//! The `Produce` ACL preamble, which resolves the transactional-id and the
//! per-topic `Write` authorization decisions before any partition is appended.

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;

use super::framing::ProduceFramed;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
};

pub(super) struct ProduceAuthorization {
    pub(super) transactional_id_denied: bool,
    pub(super) denied_topics: std::collections::HashSet<String>,
}

pub(super) fn authorize_produce(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    request: &ProduceFramed,
) -> ProduceAuthorization {
    let transactional_id_denied = request.transactional_id.as_deref().is_some_and(|id| {
        !id.is_empty()
            && broker.config.authorizer.authorize(
                image,
                &AuthorizationRequest {
                    principal: context.principal,
                    host: context.peer,
                    resource_type: ResourceType::TransactionalId,
                    resource_name: id,
                    operation: AclOperation::Write,
                },
            ) == AuthorizationResult::Deny
    });
    let topic_names: Vec<String> = request
        .topic_data
        .iter()
        .map(|topic| {
            if !topic.name.is_empty() {
                topic.name.clone()
            } else if topic.topic_id != WireUuid::ZERO {
                image
                    .topic_name_by_id(&uuid::Uuid::from_bytes(topic.topic_id.0))
                    .unwrap_or_default()
                    .to_string()
            } else {
                String::new()
            }
        })
        .collect();
    let denied_topics = authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        context.principal,
        context.peer,
        AclOperation::Write,
        topic_names.iter().map(String::as_str),
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Deny)
    .map(|(name, _)| name.to_string())
    .collect();
    ProduceAuthorization {
        transactional_id_denied,
        denied_topics,
    }
}
