//! The per-resource `DescribeConfigs` ACL check, and the result entry a denied
//! resource gets.
//!
//! Authorization is decided per resource entry rather than per request, so one
//! denied topic in a multi-resource `DescribeConfigs` leaves the authorized
//! entries in the same response. This module holds that decision and nothing
//! about which configs a resource reports.

use {
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        codes,
        handlers::config_alter::ConfigResourceType,
    },
    krabka_metadata::AclOperation,
    krabka_protocol::owned::{
        describe_configs_request::DescribeConfigsResource,
        describe_configs_response::DescribeConfigsResult,
    },
    std::net::SocketAddr,
};

/// Per-resource `DescribeConfigs` ACL check.
///
/// A Topic resource needs `DescribeConfigs` on `Topic(name)`, and a Group
/// resource on `Group(name)`. A Broker, `BROKER_LOGGER` or KIP-714
/// `CLIENT_METRICS` resource needs `DescribeConfigs` on
/// `Cluster("kafka-cluster")`, as Kafka's `ConfigHelper` requires: a
/// subscription's `match` names client ids and source addresses.
///
/// This function returns the authorization-failed code to stamp on a Deny. It
/// returns `None` when the check allows the request. A resource type Kafka
/// does not describe never reaches it: [`unexpected_resource_type_results`]
/// refuses the whole request first.
pub(super) fn resource_authz_failure(
    authorizer: &dyn Authorizer,
    image: &krabka_metadata::MetadataImage,
    principal: &krabka_security::Principal,
    host: &SocketAddr,
    resource_type: i8,
    resource_name: &str,
) -> Option<i16> {
    let acl = ConfigResourceType::from_wire(resource_type)?.acl();
    let allow = authorizer.authorize(image, &AuthorizationRequest {
        principal,
        host,
        resource_type: acl.resource_type,
        resource_name: acl.resource_name(resource_name),
        operation: AclOperation::DescribeConfigs,
    });
    (allow == AuthorizationResult::Deny).then_some(acl.failure_code)
}

/// Kafka's `Errors.INVALID_REQUEST.message()`.
const INVALID_REQUEST_MESSAGE: &str = codes::default_message(codes::INVALID_REQUEST);

/// Kafka's answer to a request that names a resource type it does not
/// describe, or `None` when every type is one it does.
///
/// `ConfigHelper.handleDescribeConfigsRequest` partitions the request by
/// authorization first and throws `InvalidRequestException("Unexpected
/// resource type ...")` for any type but `BROKER`, `BROKER_LOGGER`,
/// `CLIENT_METRICS`, `TOPIC` and `GROUP`. Nothing catches it inside, so
/// `DescribeConfigsRequest.getErrorResponse` stamps *every* resource of the
/// request with `INVALID_REQUEST`, the generic message and no configs.
pub(super) fn unexpected_resource_type_results(
    resources: &[DescribeConfigsResource],
) -> Option<Vec<DescribeConfigsResult>> {
    resources
        .iter()
        .any(|resource| ConfigResourceType::from_wire(resource.resource_type).is_none())
        .then(|| {
            resources
                .iter()
                .map(|resource| DescribeConfigsResult {
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some(INVALID_REQUEST_MESSAGE.to_owned()),
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    configs: Vec::new(),
                    ..Default::default()
                })
                .collect()
        })
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
        error_message: Some(authorization_failed_message(error_code).into()),
        resource_type,
        resource_name,
        configs: Vec::new(),
        ..Default::default()
    }
}

/// Kafka's `Errors.message()` for the three authorization failures.
const fn authorization_failed_message(error_code: i16) -> &'static str {
    codes::default_message(match error_code {
        codes::TOPIC_AUTHORIZATION_FAILED | codes::GROUP_AUTHORIZATION_FAILED => error_code,
        _ => codes::CLUSTER_AUTHORIZATION_FAILED,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::SimpleAclAuthorizer,
            codes::{CLUSTER_AUTHORIZATION_FAILED, INVALID_REQUEST, TOPIC_AUTHORIZATION_FAILED},
            handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            test_support::{allow_acl, peer, principal},
        },
        assert2::assert,
        krabka_metadata::{MetadataImage, ResourceType},
        std::collections::HashSet,
        uuid::Uuid,
    };

    fn anon() -> krabka_security::Principal { principal("ANONYMOUS") }

    /// Refuse an anonymous resource probe against the empty ACL image.
    fn empty_acl_resource_failure(kind: i8, name: &str) -> Option<i16> {
        let authorizer = SimpleAclAuthorizer::new(HashSet::new());
        let image = MetadataImage::new(Uuid::nil());
        let peer = peer();
        super::resource_authz_failure(&authorizer, &image, &anon(), &peer, kind, name)
    }

    #[test]
    fn topic_resource_denied_yields_topic_authorization_failed() {
        let code = empty_acl_resource_failure(ConfigResourceType::Topic.to_wire(), "t");
        assert!(code == Some(TOPIC_AUTHORIZATION_FAILED));
        let res = super::denied_result(
            ConfigResourceType::Topic.to_wire(),
            "t".into(),
            TOPIC_AUTHORIZATION_FAILED,
        );
        let expected = krabka_broker_macros::tagged_wire!(DescribeConfigsResult {
            error_code: TOPIC_AUTHORIZATION_FAILED,
            error_message: Some("Topic authorization failed.".to_owned()),
            resource_type: ConfigResourceType::Topic.to_wire(),
            resource_name: "t".to_owned(),
            configs: Vec::new(),
        });
        assert!(res == expected);
    }

    /// Kafka's `ConfigHelper` authorizes a KIP-714 subscription with
    /// `DescribeConfigs` on the cluster. `AlterConfigs` implies it, and
    /// `Describe` does not.
    #[test]
    fn a_client_metrics_resource_needs_describe_configs_on_the_cluster() {
        let cases = [
            (None, Some(CLUSTER_AUTHORIZATION_FAILED)),
            (Some(AclOperation::DescribeConfigs), None),
            (Some(AclOperation::AlterConfigs), None),
            (
                Some(AclOperation::Describe),
                Some(CLUSTER_AUTHORIZATION_FAILED),
            ),
        ];
        let authz = SimpleAclAuthorizer::new(HashSet::new());
        let peer = peer();
        for (grant, want) in cases {
            let mut image = MetadataImage::new(Uuid::nil());
            if let Some(operation) = grant {
                image.apply(&krabka_metadata::MetadataRecord::V1AccessControlEntry(
                    allow_acl()
                        .resource_type(ResourceType::Cluster)
                        .resource_name(CLUSTER_RESOURCE_NAME)
                        .principal("User:ANONYMOUS")
                        .operation(operation)
                        .call(),
                ));
            }
            let code = super::resource_authz_failure(
                &authz,
                &image,
                &anon(),
                &peer,
                ConfigResourceType::ClientMetrics.to_wire(),
                "sub-a",
            );
            assert!(code == want, "{grant:?}");
        }
        let denied = super::denied_result(
            ConfigResourceType::ClientMetrics.to_wire(),
            "sub-a".into(),
            CLUSTER_AUTHORIZATION_FAILED,
        );
        assert!(
            denied
                == krabka_broker_macros::tagged_wire!(DescribeConfigsResult {
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("Cluster authorization failed.".to_owned()),
                    resource_type: ConfigResourceType::ClientMetrics.to_wire(),
                    resource_name: "sub-a".to_owned(),
                    configs: Vec::new(),
                })
        );
    }

    /// One resource of a type Kafka does not describe fails the whole
    /// request: every resource, the known ones included, is `INVALID_REQUEST`
    /// with the generic message and no configs.
    #[test]
    fn an_unknown_resource_type_refuses_every_resource_of_the_request() {
        use krabka_protocol::owned::describe_configs_request::DescribeConfigsResource;

        let resource = |resource_type, name: &str| DescribeConfigsResource {
            resource_type,
            resource_name: name.to_owned(),
            ..Default::default()
        };
        let known = [
            resource(ConfigResourceType::Topic.to_wire(), "t"),
            resource(ConfigResourceType::Group.to_wire(), "g"),
        ];
        assert!(unexpected_resource_type_results(&known).is_none());

        let mixed = [
            resource(ConfigResourceType::Topic.to_wire(), "t"),
            resource(0, "x"),
        ];
        let refused = |resource_type, name: &str| {
            krabka_broker_macros::tagged_wire!(DescribeConfigsResult {
                error_code: INVALID_REQUEST,
                error_message: Some(
                    "This most likely occurs because of a request being malformed by the client \
                 library or the message was sent to an incompatible broker. See the broker logs \
                 for more details."
                        .to_owned(),
                ),
                resource_type,
                resource_name: name.to_owned(),
                configs: Vec::new(),
            })
        };
        assert!(
            unexpected_resource_type_results(&mixed)
                == Some(vec![
                    refused(ConfigResourceType::Topic.to_wire(), "t"),
                    refused(0, "x")
                ])
        );
    }

    #[test]
    fn broker_resource_denied_yields_cluster_authorization_failed() {
        let code = empty_acl_resource_failure(ConfigResourceType::Broker.to_wire(), "1");
        assert!(code == Some(CLUSTER_AUTHORIZATION_FAILED));
    }
}
