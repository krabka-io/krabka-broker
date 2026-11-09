//! Gherkin scenarios for the ACL decision of [`SimpleAclAuthorizer`]: default
//! deny, deny wins over allow, and `allow.everyone.if.no.acl.found`, bound to
//! the features under `tests/features/acl_authorization`.

use {
    krabka_authz::{AuthorizationRequest, AuthorizationResult, Authorizer, SimpleAclAuthorizer},
    krabka_metadata::{
        AclEntry, AclOperation, MetadataImage, MetadataRecord, PatternType, PermissionType,
        ResourceType,
    },
    krabka_security::{AuthMethod, Principal},
    std::{collections::HashSet, net::SocketAddr},
    uuid::Uuid,
};

/// What an ACL does for the requests it matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum Permission {
    Allows,
    Denies,
}

impl From<Permission> for PermissionType {
    fn from(permission: Permission) -> Self {
        match permission {
            Permission::Allows => Self::Allow,
            Permission::Denies => Self::Deny,
        }
    }
}

/// The authorizer's answer to a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum Decision {
    Allowed,
    Denied,
}

impl From<Decision> for AuthorizationResult {
    fn from(decision: Decision) -> Self {
        match decision {
            Decision::Allowed => Self::Allow,
            Decision::Denied => Self::Deny,
        }
    }
}

/// A cluster's ACLs and the broker's authorizer settings.
struct AclCluster {
    image: MetadataImage,
    allow_everyone_if_no_acl_found: bool,
}

impl AclCluster {
    fn new() -> Self {
        Self {
            image: MetadataImage::new(Uuid::nil()),
            allow_everyone_if_no_acl_found: false,
        }
    }

    /// Adds a literal topic ACL for `operation` from any host.
    fn add_topic_acl(
        &mut self,
        permission: Permission,
        principal: &str,
        operation: AclOperation,
        topic: &str,
    ) {
        self.image
            .apply(&MetadataRecord::V1AccessControlEntry(AclEntry {
                resource_type: ResourceType::Topic,
                resource_name: topic.into(),
                pattern_type: PatternType::Literal,
                principal: principal.into(),
                host: "*".into(),
                operation,
                permission_type: permission.into(),
            }));
    }

    /// Authorizes `principal`, written as Kafka writes it in an ACL, for
    /// `operation` on `topic`, with no super users configured.
    fn authorize(
        &self,
        principal: &str,
        operation: AclOperation,
        topic: &str,
    ) -> AuthorizationResult {
        let name = principal.strip_prefix("User:").unwrap_or(principal);
        let principal = Principal {
            name: name.into(),
            auth_method: AuthMethod::SaslPlain,
            groups: Vec::new(),
        };
        let host = SocketAddr::from(([127, 0, 0, 1], 9092));
        let authorizer = SimpleAclAuthorizer::new(HashSet::new())
            .with_allow_everyone_if_no_acl_found(self.allow_everyone_if_no_acl_found);
        authorizer.authorize(&self.image, &AuthorizationRequest {
            principal: &principal,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: topic,
            operation,
        })
    }
}

struct AclAuthorizationScenario {
    cluster: AclCluster,
    decision: krabka_bdd::Given<AuthorizationResult>,
}

#[rstest::fixture]
fn acl_authorization() -> AclAuthorizationScenario {
    AclAuthorizationScenario {
        cluster: AclCluster::new(),
        decision: krabka_bdd::Given::new(
            "decision",
            "When <principal> requests to Read topic <topic>",
        ),
    }
}

#[krabka_bdd::given("the broker sets allow.everyone.if.no.acl.found to {allow}")]
fn broker_sets_allow_everyone(acl_authorization: &mut AclAuthorizationScenario, allow: bool) {
    acl_authorization.cluster.allow_everyone_if_no_acl_found = allow;
}

#[krabka_bdd::given("an ACL {permission} {principal} to Read topic {topic}")]
fn an_acl(
    acl_authorization: &mut AclAuthorizationScenario,
    permission: Permission,
    principal: String,
    topic: String,
) {
    acl_authorization
        .cluster
        .add_topic_acl(permission, &principal, AclOperation::Read, &topic);
}

#[krabka_bdd::when("{principal} requests to Read topic {topic}")]
fn principal_requests(
    acl_authorization: &mut AclAuthorizationScenario,
    principal: String,
    topic: String,
) {
    let decision = acl_authorization
        .cluster
        .authorize(&principal, AclOperation::Read, &topic);
    acl_authorization.decision.set(decision);
}

#[krabka_bdd::then("the request is {decision}")]
fn the_request_is(acl_authorization: &AclAuthorizationScenario, decision: Decision) {
    assert2::assert!(*acl_authorization.decision.get() == AuthorizationResult::from(decision));
}

krabka_bdd::scenarios!(
    "tests/features/acl_authorization",
    fixtures = [acl_authorization: AclAuthorizationScenario],
);
