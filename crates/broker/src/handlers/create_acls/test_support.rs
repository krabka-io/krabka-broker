//! Fixtures shared by the `create_acls` test modules.
//!
//! The wire constants, the `AclCreation` and `CreateAclsRequest` builders, the
//! one-argument `validate` shim that allows CIDR hosts, and the context helper
//! are used from more than one of the sibling test modules, so they live here
//! rather than being repeated in each.

use krabka_metadata::AclEntry;
use krabka_protocol::owned::create_acls_request::{AclCreation, CreateAclsRequest};

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

crate::test_support::context_helper!(pub(super) client_id = "admin-client");

pub(super) fn all_acls(handle: &BrokerHandle) -> Vec<krabka_metadata::AclEntry> {
    handle
        .controller_image_for_test()
        .all_acls()
        .cloned()
        .collect()
}

/// Validates `c` with CIDR ACL hosts supported, which is what every test not
/// about the CIDR gate wants.
pub(super) fn validate(c: &AclCreation) -> Result<AclEntry, (i16, String)> {
    super::validate::validate(
        c,
        super::validate::HostCheck::Trunk {
            cidr_hosts_supported: true,
        },
    )
}

/// An authorizer an operator actually configured, which lets the `admin` test
/// principal through as a super user.
///
/// The ACL RPCs answer `SECURITY_DISABLED` under the default
/// `AllowAllAuthorizer`, so every case about the creating path needs a broker
/// that has an authorizer at all.
pub(super) fn configured_authorizer() -> std::sync::Arc<dyn crate::authorizer::Authorizer> {
    std::sync::Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
        std::iter::once("admin".to_owned()).collect(),
    ))
}
