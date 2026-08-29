//! Fixtures shared by the `delete_acls` test modules.
//!
//! The wire constants, the `AclEntry` and `DeleteAclsFilter` builders, the
//! request envelope, and the response and context helpers are used from more
//! than one of the sibling test modules, so they live here rather than being
//! repeated in each.

use bytes::Bytes;
use krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
use krabka_protocol::owned::{
    delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest},
    delete_acls_response::DeleteAclsResponse,
};

pub(super) const VERSION: i16 = 3;
pub(super) const RESOURCE_TYPE_TOPIC: i8 = 2;
pub(super) const PATTERN_TYPE_ANY: i8 = 1;
pub(super) const PATTERN_TYPE_LITERAL: i8 = 3;
pub(super) const PATTERN_TYPE_PREFIXED: i8 = 4;
pub(super) const OPERATION_ANY: i8 = 1;
pub(super) const OPERATION_READ: i8 = 3;
pub(super) const OPERATION_WRITE: i8 = 4;
pub(super) const PERMISSION_ANY: i8 = 1;
pub(super) const PERMISSION_ALLOW: i8 = 3;

pub(super) fn acl(resource_name: &str, principal: &str, operation: AclOperation) -> AclEntry {
    AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: resource_name.into(),
        pattern_type: PatternType::Literal,
        principal: principal.into(),
        host: "*".into(),
        operation,
        permission_type: PermissionType::Allow,
    }
}

pub(super) fn filter(resource_name: Option<&str>, principal: Option<&str>) -> DeleteAclsFilter {
    DeleteAclsFilter {
        resource_type_filter: RESOURCE_TYPE_TOPIC,
        resource_name_filter: resource_name.map(Into::into),
        pattern_type_filter: PATTERN_TYPE_LITERAL,
        principal_filter: principal.map(Into::into),
        host_filter: Some("*".into()),
        operation: OPERATION_READ,
        permission_type: PERMISSION_ALLOW,
        ..Default::default()
    }
}

pub(super) fn request(filters: Vec<DeleteAclsFilter>) -> DeleteAclsRequest {
    DeleteAclsRequest {
        filters,
        ..Default::default()
    }
}

/// The `decode_response` that `crate::test_support::response_helpers!` would
/// generate, written out because the sibling test modules reach it across
/// module boundaries and a macro-generated item cannot be re-exported.
pub(super) fn decode_response(bytes: &Bytes) -> DeleteAclsResponse {
    crate::test_support::decode_response(bytes, VERSION)
}

/// The `test_context` counterpart to [`decode_response`], with the
/// `admin-client` client id that the `DeleteAcls` tests use.
pub(super) fn test_context<'a>(
    principal: &'a krabka_security::Principal,
    peer: &'a std::net::SocketAddr,
) -> crate::handlers::RequestContext<'a> {
    crate::test_support::request_context(principal, peer, "admin-client")
}
