//! The ACL gates that a handler applies before it does any work.
//!
//! Every gate returns `true` when the authorizer denies the principal, so a
//! handler reads as `if <gate>(..) { return <error>; }` and each caller stays
//! free to choose the error code that its RPC answers with.

use super::{RequestContext, acl_wire};

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
        let ctx = RequestContext {
            principal: &principal,
            peer: &peer,
            client_id: "client-a",
            connection_id: "connection-a",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        };

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
        let ctx = RequestContext {
            principal: &principal,
            peer: &peer,
            client_id: "krabka-guard",
            connection_id: "connection-a",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        };

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
}
