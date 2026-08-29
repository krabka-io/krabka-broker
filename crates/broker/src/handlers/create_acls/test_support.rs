//! Fixtures shared by the `create_acls` test modules.
//!
//! The wire constants, the `AclCreation` and `CreateAclsRequest` builders, the
//! two-argument `validate` shim that drops the byte limits, and the response and
//! context helpers are used from more than one of the sibling test modules, so
//! they live here rather than being repeated in each.

use bytes::Bytes;
use krabka_metadata::AclEntry;
use krabka_protocol::owned::{
    create_acls_request::{AclCreation, CreateAclsRequest},
    create_acls_response::CreateAclsResponse,
};

use crate::broker::BrokerHandle;

pub(super) const VERSION: i16 = 3;
const RESOURCE_TYPE_TOPIC: i8 = 2;
const PATTERN_TYPE_LITERAL: i8 = 3;
pub(super) const OPERATION_READ: i8 = 3;
pub(super) const OPERATION_WRITE: i8 = 4;
const PERMISSION_ALLOW: i8 = 3;

pub(super) fn creation(resource_name: &str, principal: &str, operation: i8) -> AclCreation {
    AclCreation {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: resource_name.into(),
        resource_pattern_type: PATTERN_TYPE_LITERAL,
        principal: principal.into(),
        host: "*".into(),
        operation,
        permission_type: PERMISSION_ALLOW,
        ..Default::default()
    }
}

pub(super) fn request(creations: Vec<AclCreation>) -> CreateAclsRequest {
    CreateAclsRequest {
        creations,
        ..Default::default()
    }
}

/// The `decode_response` that `crate::test_support::response_helpers!` would
/// generate, written out because the sibling test modules reach it across
/// module boundaries and a macro-generated item cannot be re-exported.
pub(super) fn decode_response(bytes: &Bytes) -> CreateAclsResponse {
    crate::test_support::decode_response(bytes, VERSION)
}

/// The `test_context` counterpart to [`decode_response`], with the
/// `admin-client` client id that the `CreateAcls` tests use.
pub(super) fn test_context<'a>(
    principal: &'a krabka_security::Principal,
    peer: &'a std::net::SocketAddr,
) -> crate::handlers::RequestContext<'a> {
    crate::test_support::request_context(principal, peer, "admin-client")
}

pub(super) fn all_acls(handle: &BrokerHandle) -> Vec<krabka_metadata::AclEntry> {
    handle
        .controller_image_for_test()
        .all_acls()
        .cloned()
        .collect()
}

pub(super) fn validate(c: &AclCreation) -> Result<AclEntry, (i16, &'static str)> {
    super::validate::validate(c, usize::MAX, usize::MAX)
}
