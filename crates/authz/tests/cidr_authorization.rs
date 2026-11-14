//! Host-range decisions through the broker image and gateway ACL snapshot.
#![cfg(test)]

use {
    assert2::assert,
    krabka_authz::{
        AclCache, AuthorizationRequest, AuthorizationResult, Authorizer, SimpleAclAuthorizer,
    },
    krabka_bdd::{Given, given, then, when},
    krabka_metadata::{
        AclEntry, AclOperation, FeatureLevelRecord, MetadataImage, MetadataRecord, PatternType,
        PermissionType, ResourceType,
        metadata_version::{CIDR_ACL_MIN_LEVEL, METADATA_VERSION_FEATURE, from_version_string},
    },
    krabka_security::{AuthMethod, Principal},
    rstest::fixture,
    std::{
        collections::HashSet,
        net::{IpAddr, SocketAddr},
    },
    uuid::Uuid,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum HostPermission {
    Allow,
    Deny,
}

impl From<HostPermission> for PermissionType {
    fn from(permission: HostPermission) -> Self {
        match permission {
            HostPermission::Allow => Self::Allow,
            HostPermission::Deny => Self::Deny,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum HostDecision {
    Allowed,
    Denied,
}

impl From<HostDecision> for AuthorizationResult {
    fn from(decision: HostDecision) -> Self {
        match decision {
            HostDecision::Allowed => Self::Allow,
            HostDecision::Denied => Self::Deny,
        }
    }
}

struct HostAuthorization {
    image: MetadataImage,
    entries: Vec<AclEntry>,
    cidr_hosts_supported: bool,
    decisions: Given<(AuthorizationResult, AuthorizationResult)>,
}

impl HostAuthorization {
    fn authorize(&self, peer: IpAddr) -> (AuthorizationResult, AuthorizationResult) {
        let principal = Principal {
            name: "alice".into(),
            auth_method: AuthMethod::SaslPlain,
            groups: Vec::new(),
        };
        let host = SocketAddr::new(peer, 9092);
        let request = AuthorizationRequest {
            principal: &principal,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "orders",
            operation: AclOperation::Read,
        };
        let authorizer = SimpleAclAuthorizer::new(HashSet::new());
        let cache = AclCache::new(self.entries.clone())
            .with_cidr_hosts_supported(self.cidr_hosts_supported);
        (
            authorizer.authorize(&self.image, &request),
            authorizer.authorize(&cache, &request),
        )
    }
}

#[fixture]
fn hosts() -> HostAuthorization {
    HostAuthorization {
        image: MetadataImage::new(Uuid::nil()),
        entries: Vec::new(),
        cidr_hosts_supported: false,
        decisions: Given::new(
            "decisions",
            "When Alice requests READ on orders from <peer>",
        ),
    }
}

#[given("the cluster metadata.version is {version}")]
fn cluster_version(hosts: &mut HostAuthorization, version: String) {
    if version != "unfinalized" {
        let level = from_version_string(&version)
            .expect("a supported metadata.version")
            .feature_level();
        hosts
            .image
            .apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: METADATA_VERSION_FEATURE.into(),
                level,
            }));
        hosts.cidr_hosts_supported = level >= CIDR_ACL_MIN_LEVEL;
    }
}

#[given("Alice has a READ {permission} ACL on orders from {host}")]
fn host_acl(hosts: &mut HostAuthorization, permission: HostPermission, host: String) {
    let entry = AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "orders".into(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".into(),
        host,
        operation: AclOperation::Read,
        permission_type: permission.into(),
    };
    hosts
        .image
        .apply(&MetadataRecord::V1AccessControlEntry(entry.clone()));
    hosts.entries.push(entry);
}

#[when("Alice requests READ on orders from {peer}")]
fn authorize_host(hosts: &mut HostAuthorization, peer: IpAddr) {
    let decisions = hosts.authorize(peer);
    hosts.decisions.set(decisions);
}

#[then("the broker and gateway decisions are {decision}")]
fn host_decisions(hosts: &HostAuthorization, decision: HostDecision) {
    let expected = AuthorizationResult::from(decision);
    assert!(*hosts.decisions.get() == (expected, expected));
}

krabka_bdd::scenarios!("tests/features/cidr_authorization", fixtures = [hosts: HostAuthorization]);
