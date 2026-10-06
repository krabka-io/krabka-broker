//! Fixtures shared by the `delete_acls` test modules.
//!
//! The wire constants, the `AclEntry` and `DeleteAclsFilter` builders, the
//! request envelope, and the context helper are used from more
//! than one of the sibling test modules, so they live here rather than being
//! repeated in each.

use krabka_metadata::{AclEntry, AclOperation, ResourceType};
use krabka_protocol::owned::delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest};

pub(super) const VERSION: i16 = 3;
pub(super) const RESOURCE_TYPE_TOPIC: i8 = 2;
pub(super) const PATTERN_TYPE_ANY: i8 = 1;
pub(super) const PATTERN_TYPE_MATCH: i8 = 2;
pub(super) const PATTERN_TYPE_LITERAL: i8 = 3;
pub(super) const PATTERN_TYPE_PREFIXED: i8 = 4;
pub(super) const OPERATION_ANY: i8 = 1;
pub(super) const OPERATION_READ: i8 = 3;
pub(super) const OPERATION_WRITE: i8 = 4;
pub(super) const PERMISSION_ANY: i8 = 1;
pub(super) const PERMISSION_ALLOW: i8 = 3;

pub(super) fn acl(resource_name: &str, principal: &str, operation: AclOperation) -> AclEntry {
    crate::test_support::allow_acl(ResourceType::Topic, resource_name, principal, operation)
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

crate::test_support::context_helper!(pub(super) client_id = "admin-client");

/// An authorizer an operator actually configured, which lets the `admin` test
/// principal through as a super user.
///
/// The ACL RPCs answer `SECURITY_DISABLED` under the default
/// `AllowAllAuthorizer`, so every case about the deleting path needs a broker
/// that has an authorizer at all.
pub(super) fn configured_authorizer() -> std::sync::Arc<dyn crate::authorizer::Authorizer> {
    std::sync::Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
        std::iter::once("admin".to_owned()).collect(),
    ))
}
