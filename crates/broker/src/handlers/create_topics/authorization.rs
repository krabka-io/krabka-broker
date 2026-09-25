//! The `Create` authorization gate of `CreateTopics`.
//!
//! Kafka checks `Create` on the `Cluster` resource once, as a shortcut: an
//! Allow there authorizes every surviving topic name without a further
//! lookup. A Deny is not a whole-request failure -- it falls back to
//! `Create` on each `Topic(name)` individually
//! (`ControllerApis.handleCreateTopics`/`createTopics`), so a principal
//! scoped to a literal or prefixed topic ACL (the standard Kafka
//! Streams/Connect setup) can still create the topics its ACL covers.

use std::collections::HashMap;

use krabka_metadata::AclOperation;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
};

pub(super) fn cluster_create_denied(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
) -> bool {
    broker.config.authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal: context.principal,
            host: context.peer,
            resource_type: krabka_metadata::ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: AclOperation::Create,
        },
    ) == AuthorizationResult::Deny
}

/// The `Create` decision for a set of candidate topic names: an Allow for
/// every name when cluster `Create` covers the request, else the per-name
/// `Create` decision on `Topic(name)`.
///
/// `names` is the request's topic names with duplicates and the protected
/// `__cluster_metadata` name already removed -- those never reach the
/// authorizer, exactly as Kafka's `getCreatableTopics.apply(allowedTopicNames)`
/// only ever sees `allowedTopicNames`.
pub(super) fn authorize_create_topics<'a>(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    names: impl IntoIterator<Item = &'a str>,
) -> HashMap<&'a str, AuthorizationResult> {
    let names: Vec<&str> = names.into_iter().collect();
    if !cluster_create_denied(broker, image, context) {
        return names
            .into_iter()
            .map(|name| (name, AuthorizationResult::Allow))
            .collect();
    }
    authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        context.principal,
        context.peer,
        AclOperation::Create,
        names,
    )
}

/// KIP-525's second, per-topic check: may this principal be told what the
/// topic it just created is configured with?
///
/// Kafka's `ControllerApis.handleCreateTopics` filters the requested names by
/// `DESCRIBE_CONFIGS` on `Topic(name)` and hands the surviving set to the
/// controller, which fills `configs` for those and stamps
/// `TOPIC_AUTHORIZATION_FAILED` on `topicConfigErrorCode` for the rest. A
/// denial never fails the create: the topic exists either way, and only the
/// disclosure is withheld.
pub(super) fn describe_configs_denied(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    topic: &str,
) -> bool {
    broker.config.authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal: context.principal,
            host: context.peer,
            resource_type: krabka_metadata::ResourceType::Topic,
            resource_name: topic,
            operation: AclOperation::DescribeConfigs,
        },
    ) == AuthorizationResult::Deny
}
