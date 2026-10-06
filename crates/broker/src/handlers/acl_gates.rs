//! The ACL gates that a handler applies before it does any work.
//!
//! Every gate returns `true` when the authorizer denies the principal, so a
//! handler reads as `if <gate>(..) { return <error>; }` and each caller stays
//! free to choose the error code that its RPC answers with.

use std::collections::HashSet;

use super::{RequestContext, acl_wire};
use crate::authorizer::{AuthorizationResult, authorize_topics};

pub(crate) fn acl_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    resource_type: krabka_metadata::ResourceType,
    resource_name: &str,
    operation: krabka_metadata::AclOperation,
) -> bool {
    authorizer.authorize(
        image,
        &crate::authorizer::AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type,
            resource_name,
            operation,
        },
    ) == crate::authorizer::AuthorizationResult::Deny
}

pub(crate) fn group_read_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    group_id: &str,
) -> bool {
    acl_denied(
        authorizer,
        image,
        ctx,
        krabka_metadata::ResourceType::Group,
        group_id,
        krabka_metadata::AclOperation::Read,
    )
}

/// The `Describe` gate on `Group(group_id)`, the twin of
/// [`group_read_denied`].
pub(crate) fn group_describe_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    group_id: &str,
) -> bool {
    acl_denied(
        authorizer,
        image,
        ctx,
        krabka_metadata::ResourceType::Group,
        group_id,
        krabka_metadata::AclOperation::Describe,
    )
}

pub(crate) fn cluster_alter_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
) -> bool {
    acl_denied(
        authorizer,
        image,
        ctx,
        krabka_metadata::ResourceType::Cluster,
        acl_wire::CLUSTER_RESOURCE_NAME,
        krabka_metadata::AclOperation::Alter,
    )
}

pub(crate) fn cluster_action_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
) -> bool {
    acl_denied(
        authorizer,
        image,
        ctx,
        krabka_metadata::ResourceType::Cluster,
        acl_wire::CLUSTER_RESOURCE_NAME,
        krabka_metadata::AclOperation::ClusterAction,
    )
}

/// The `Describe` gate on `Cluster("kafka-cluster")`, the twin of
/// [`cluster_alter_denied`].
///
/// It returns `true` when the authorizer denies the principal. The barrier,
/// write-freeze, and break-glass control planes each read the cluster through
/// this one gate, so a denial answers `CLUSTER_AUTHORIZATION_FAILED` (31)
/// whichever private api key the caller reached.
pub(crate) fn cluster_describe_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
) -> bool {
    acl_denied(
        authorizer,
        image,
        ctx,
        krabka_metadata::ResourceType::Cluster,
        acl_wire::CLUSTER_RESOURCE_NAME,
        krabka_metadata::AclOperation::Describe,
    )
}

/// The names among `names` whose `operation` on `Topic` the authorizer
/// denies to `ctx`'s principal.
///
/// Every name is authorized through [`authorize_topics`], so each denial is
/// audited the way a per-topic refusal is.
pub(crate) fn denied_topics<'a>(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    operation: krabka_metadata::AclOperation,
    names: impl IntoIterator<Item = &'a str>,
) -> HashSet<String> {
    authorize_topics(authorizer, image, ctx.principal, ctx.peer, operation, names)
        .into_iter()
        .filter(|(_, result)| *result == AuthorizationResult::Deny)
        .map(|(name, _)| name.to_owned())
        .collect()
}

/// `true` when `names` is non-empty and at least one of its distinct names is
/// `Describe`-denied for `ctx`'s principal.
///
/// It is the subscribed-topic-names gate of the `ConsumerGroupHeartbeat` and
/// `ShareGroupHeartbeat` handlers. A `None` or empty list means Kafka's
/// `subscribedTopicSet` is empty, which is vacuously fully authorized.
/// KIP-932's `ShareGroupHeartbeatRequest` carries no `SubscribedTopicRegex`,
/// so this names gate is the whole of its topic check.
pub(crate) fn subscribed_names_describe_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    names: Option<&[String]>,
) -> bool {
    let Some(names) = names.filter(|names| !names.is_empty()) else {
        return false;
    };
    let unique: HashSet<&str> = names.iter().map(String::as_str).collect();
    !denied_topics(
        authorizer,
        image,
        ctx,
        krabka_metadata::AclOperation::Describe,
        unique,
    )
    .is_empty()
}

/// Kafka's `filterByAuthorized(DESCRIBE, TOPIC, requiredTopics)` as the
/// streams-group handlers read it: `true` when any of `names` is
/// `Describe`-denied for `ctx`'s principal.
///
/// It stops at the first denial, so a name after it is never authorized and
/// never audited.
pub(crate) fn any_topic_describe_denied(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    ctx: &RequestContext<'_>,
    names: &[String],
) -> bool {
    names.iter().any(|topic| {
        acl_denied(
            authorizer,
            image,
            ctx,
            krabka_metadata::ResourceType::Topic,
            topic,
            krabka_metadata::AclOperation::Describe,
        )
    })
}

/// The name a `Produce` or `Fetch` topic row stands for: its `name` when the
/// wire carries one, else the name that `topic_id` resolves to in `image`.
///
/// A row with neither, or an id that does not resolve, gives the empty name.
pub(crate) fn requested_topic_name(
    image: &krabka_metadata::MetadataImage,
    name: &str,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
) -> String {
    if !name.is_empty() {
        name.to_owned()
    } else if topic_id != krabka_protocol::primitives::uuid::Uuid::ZERO {
        image
            .topic_name_by_id(&uuid::Uuid::from_bytes(topic_id.0))
            .unwrap_or_default()
            .to_owned()
    } else {
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, net::SocketAddr};

    use assert2::{assert, check};
    use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
    use krabka_security::{AuthMethod, Principal};

    use super::*;

    fn principal() -> Principal {
        Principal {
            name: "alice".to_string(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec!["operators".to_string()],
        }
    }

    #[test]
    fn acl_denied_reports_simple_acl_denial() {
        let authorizer = crate::authorizer::SimpleAclAuthorizer::new(HashSet::new());
        let image = MetadataImage::new(uuid::Uuid::nil());
        let principal = principal();
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = RequestContext::new(
            &principal,
            &peer,
            "client-a",
            "connection-a",
            false,
            "PLAINTEXT",
        );

        assert!(acl_denied(
            &authorizer,
            &image,
            &ctx,
            ResourceType::Topic,
            "orders",
            AclOperation::Describe,
        ));
    }

    /// The barrier, write-freeze, and break-glass read APIs all gate on
    /// [`cluster_describe_denied`], and every one of them answers
    /// `CLUSTER_AUTHORIZATION_FAILED` (31) when it returns `true`.
    #[test]
    fn cluster_describe_denied_refuses_a_principal_with_no_cluster_describe() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        let principal = principal();
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = RequestContext::new(
            &principal,
            &peer,
            "krabka-guard",
            "connection-a",
            false,
            "PLAINTEXT",
        );

        for (label, super_users, denied) in [
            ("an empty acl store denies by default", Vec::new(), true),
            (
                "a super user reads the cluster",
                vec![principal.name.clone()],
                false,
            ),
            (
                "another super user does not lend its grant",
                vec!["bob".to_string()],
                true,
            ),
        ] {
            let authorizer = crate::authorizer::SimpleAclAuthorizer::new(
                super_users.into_iter().collect::<HashSet<String>>(),
            );

            check!(
                cluster_describe_denied(&authorizer, &image, &ctx) == denied,
                "case {label}"
            );
        }

        // The code every caller answers on a denial, and the number Kafka
        // assigns it.
        check!(crate::codes::CLUSTER_AUTHORIZATION_FAILED == 31);
    }

    /// [`denied_topics`] keeps exactly the names the authorizer denies, with
    /// each one owned and every duplicate folded into one entry.
    #[test]
    fn denied_topics_keeps_only_the_denied_names() {
        let principal = principal();
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = RequestContext::new(
            &principal,
            &peer,
            "client-a",
            "connection-a",
            false,
            "PLAINTEXT",
        );
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&krabka_metadata::MetadataRecord::V1AccessControlEntry(
            crate::test_support::allow_acl(
                ResourceType::Topic,
                "orders",
                "User:alice",
                AclOperation::Write,
            ),
        ));
        let authorizer = crate::authorizer::SimpleAclAuthorizer::new(HashSet::new());

        for (label, operation, names, expected) in [
            ("no names", AclOperation::Write, vec![], vec![]),
            (
                "the granted name passes",
                AclOperation::Write,
                vec!["orders"],
                vec![],
            ),
            (
                "an ungranted name is denied once",
                AclOperation::Write,
                vec!["orders", "shipments", "shipments"],
                vec!["shipments"],
            ),
            (
                "a grant for another operation does not lend itself",
                AclOperation::Read,
                vec!["orders"],
                vec!["orders"],
            ),
        ] {
            let expected: HashSet<String> = expected.into_iter().map(String::from).collect();
            check!(
                denied_topics(&authorizer, &image, &ctx, operation, names) == expected,
                "case {label}"
            );
        }
    }

    /// [`requested_topic_name`] prefers the wire name and gives the empty
    /// name when neither the name nor the id names a topic.
    #[test]
    fn requested_topic_name_prefers_the_wire_name() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        let unknown = krabka_protocol::primitives::uuid::Uuid([7; 16]);
        let zero = krabka_protocol::primitives::uuid::Uuid::ZERO;

        for (label, name, id, expected) in [
            ("a wire name wins over an id", "orders", unknown, "orders"),
            ("no name and no id", "", zero, ""),
            ("an id that does not resolve", "", unknown, ""),
        ] {
            check!(
                requested_topic_name(&image, name, id) == expected,
                "case {label}"
            );
        }
    }
}
