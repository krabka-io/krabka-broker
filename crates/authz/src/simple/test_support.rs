//! Fixture builders shared by the `SimpleAclAuthorizer` unit tests.
//!
//! The principals, hosts, metadata images, ACL entries, and requests below are
//! needed both by the decision-order tests in [`super::tests`] and by the
//! entry-matching tests in [`super::matching`], so they live in one module
//! instead of being duplicated in each.

use std::{collections::HashSet, net::SocketAddr};

use krabka_metadata::{
    AclEntry, AclOperation, MetadataImage, PatternType, PermissionType, ResourceType,
};
use krabka_security::Principal;
use uuid::Uuid;

use crate::AuthorizationRequest;

pub(super) fn no_super() -> HashSet<String> {
    HashSet::new()
}
pub(super) fn one_super(name: &str) -> HashSet<String> {
    let mut s = HashSet::new();
    s.insert(name.to_string());
    s
}

pub(super) fn alice() -> Principal {
    Principal {
        name: "alice".into(),
        auth_method: krabka_security::AuthMethod::SaslPlain,
        groups: vec![],
    }
}

pub(super) fn addr() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

pub(super) fn img() -> MetadataImage {
    MetadataImage::new(Uuid::nil())
}

pub(super) fn topic_acl(
    permission: PermissionType,
    op: AclOperation,
    principal: &str,
    host: &str,
    pattern: PatternType,
    name: &str,
) -> AclEntry {
    AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: name.into(),
        pattern_type: pattern,
        principal: principal.into(),
        host: host.into(),
        operation: op,
        permission_type: permission,
    }
}

pub(super) fn req<'a>(
    p: &'a Principal,
    host: &'a SocketAddr,
    name: &'a str,
    op: AclOperation,
) -> AuthorizationRequest<'a> {
    AuthorizationRequest {
        principal: p,
        host,
        resource_type: ResourceType::Topic,
        resource_name: name,
        operation: op,
    }
}

pub(super) fn topic_acl_op(permission: PermissionType, op: AclOperation, name: &str) -> AclEntry {
    AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: name.into(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: op,
        permission_type: permission,
    }
}

pub(super) fn acl_op_on(
    rt: ResourceType,
    permission: PermissionType,
    op: AclOperation,
    name: &str,
) -> AclEntry {
    AclEntry {
        resource_type: rt,
        resource_name: name.into(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: op,
        permission_type: permission,
    }
}

pub(super) fn req_on<'a>(
    p: &'a Principal,
    host: &'a SocketAddr,
    rt: ResourceType,
    name: &'a str,
    op: AclOperation,
) -> AuthorizationRequest<'a> {
    AuthorizationRequest {
        principal: p,
        host,
        resource_type: rt,
        resource_name: name,
        operation: op,
    }
}
