//! The `Produce` ACL preamble, which resolves the transactional-id and the
//! per-topic `Write` authorization decisions before any partition is appended.

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;

use super::framing::ProduceFramed;
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
};

/// Kafka's `produceRequest.transactionalId != null && authorize(WRITE,
/// TRANSACTIONAL_ID, transactionalId)`, from `KafkaApis.handleProduceRequest`.
///
/// This only decides whether the request's `transactional_id` is authorized
/// for `Write`. The caller decides separately whether the check applies at
/// all -- Kafka only consults this when
/// `RequestUtils.hasTransactionalRecords` is true, that is, when some batch
/// in the request carries the transactional attribute
/// ([`ProduceFramed::has_transactional_batch`]).
///
/// Kafka's predicate is `transactionalId != null`: only a wire-null id is
/// rejected outright here. A non-null empty id is passed to the configured
/// authorizer like any other resource name, exactly as Kafka's own
/// `authHelper.authorize` call does -- an `AllowAllAuthorizer` allows it, and
/// a deny-capable one decides on the empty resource name itself.
pub(super) fn is_authorized_transactional(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    transactional_id: Option<&str>,
) -> bool {
    let Some(id) = transactional_id else {
        return false;
    };
    broker.config.authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal: context.principal,
            host: context.peer,
            resource_type: ResourceType::TransactionalId,
            resource_name: id,
            operation: AclOperation::Write,
        },
    ) == AuthorizationResult::Allow
}

/// The topic `Write` ACL preamble: every topic named in the request,
/// authorized once, ahead of the per-partition append loop.
///
/// Topic name resolution for v ≥ 13 (`topic_id` only on the wire) is re-done
/// here even though the handler resolves it again per topic below -- ACLs are
/// keyed by topic *name*, and this batch-authorizes every name in the request
/// in one call rather than one authorizer round trip per topic.
pub(super) fn authorize_produce_topics(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    request: &ProduceFramed,
) -> std::collections::HashSet<String> {
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
    authorize_topics(
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
    .collect()
}
