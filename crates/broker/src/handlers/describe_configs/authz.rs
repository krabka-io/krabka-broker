//! The per-resource `DescribeConfigs` ACL check, and the result entry a denied
//! resource gets.
//!
//! Authorization is decided per resource entry rather than per request, so one
//! denied topic in a multi-resource `DescribeConfigs` leaves the authorized
//! entries in the same response. This module holds that decision and nothing
//! about which configs a resource reports.

use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::owned::describe_configs_response::DescribeConfigsResult;

use super::wire::{RESOURCE_TYPE_BROKER, RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    codes,
};

/// Per-resource `DescribeConfigs` ACL check.
///
/// A Topic resource needs `DescribeConfigs` on `Topic(name)`. A Broker
/// resource needs `DescribeConfigs` on `Cluster("kafka-cluster")`.
///
/// This function returns the authorization-failed code to stamp on a Deny. It
/// returns `None` when the check allows the request, and for a resource type
/// that it does not gate. An ungated resource type still gets an empty configs
/// list with no error.
pub(super) fn resource_authz_failure(
    authorizer: &dyn crate::authorizer::Authorizer,
    image: &krabka_metadata::MetadataImage,
    principal: &krabka_security::Principal,
    host: &std::net::SocketAddr,
    resource_type: i8,
    resource_name: &str,
) -> Option<i16> {
    let (rt, name, failure_code): (ResourceType, &str, i16) = match resource_type {
        RESOURCE_TYPE_TOPIC => (
            ResourceType::Topic,
            resource_name,
            codes::TOPIC_AUTHORIZATION_FAILED,
        ),
        RESOURCE_TYPE_BROKER => (
            ResourceType::Cluster,
            crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            codes::CLUSTER_AUTHORIZATION_FAILED,
        ),
        RESOURCE_TYPE_GROUP => (
            ResourceType::Group,
            resource_name,
            codes::GROUP_AUTHORIZATION_FAILED,
        ),
        _ => return None,
    };
    let allow = authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal,
            host,
            resource_type: rt,
            resource_name: name,
            operation: AclOperation::DescribeConfigs,
        },
    );
    (allow == AuthorizationResult::Deny).then_some(failure_code)
}

/// Builds a `DescribeConfigsResult` that carries only the
/// authorization-failed error code, for a denied resource.
pub(super) fn denied_result(
    resource_type: i8,
    resource_name: String,
    error_code: i16,
) -> DescribeConfigsResult {
    DescribeConfigsResult {
        error_code,
        error_message: Some("authorization failed".into()),
        resource_type,
        resource_name,
        configs: Vec::new(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::MetadataImage;
    use krabka_protocol::UnknownTaggedFields;
    use uuid::Uuid;

    use super::*;

    fn anon() -> krabka_security::Principal {
        krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        }
    }

    #[test]
    fn topic_resource_denied_yields_topic_authorization_failed() {
        let authz = crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = MetadataImage::new(Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let code = super::resource_authz_failure(
            &authz,
            &image,
            &anon(),
            &peer,
            super::RESOURCE_TYPE_TOPIC,
            "t",
        );
        assert!(code == Some(crate::codes::TOPIC_AUTHORIZATION_FAILED));
        let res = super::denied_result(
            super::RESOURCE_TYPE_TOPIC,
            "t".into(),
            crate::codes::TOPIC_AUTHORIZATION_FAILED,
        );
        let expected = DescribeConfigsResult {
            error_code: crate::codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some("authorization failed".to_string()),
            resource_type: super::RESOURCE_TYPE_TOPIC,
            resource_name: "t".to_string(),
            configs: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(res == expected);
    }

    #[test]
    fn broker_resource_denied_yields_cluster_authorization_failed() {
        let authz = crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = MetadataImage::new(Uuid::nil());
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let code = super::resource_authz_failure(
            &authz,
            &image,
            &anon(),
            &peer,
            super::RESOURCE_TYPE_BROKER,
            "1",
        );
        assert!(code == Some(crate::codes::CLUSTER_AUTHORIZATION_FAILED));
    }
}
