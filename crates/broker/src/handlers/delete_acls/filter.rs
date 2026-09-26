//! Translation of one wire `DeleteAclsFilter` into Kafka's `AclBindingFilter`,
//! and of one matched ACL into the deletion record that removes it.
//!
//! Kafka spells "no constraint" as `ANY` on the enum axes and as a null string
//! on the name, principal, and host axes. An empty string is a value, not a
//! wildcard, and `MATCH` selects every binding that applies to the named
//! resource. Getting that wrong would silently widen or narrow a delete, so
//! the decision sits in a file of its own.

use krabka_metadata::{AclEntry, AclEntryFilter};
use krabka_protocol::owned::delete_acls_request::DeleteAclsFilter;

use crate::handlers::acl_wire::binding_filter::{
    AclBindingFilter, UnknownElement, WireAclBindingFilter,
};

pub(super) fn build_filter(f: &DeleteAclsFilter) -> Result<AclBindingFilter, UnknownElement> {
    AclBindingFilter::from_wire(WireAclBindingFilter {
        resource_type: f.resource_type_filter,
        resource_name: f.resource_name_filter.as_deref(),
        pattern_type: f.pattern_type_filter,
        principal: f.principal_filter.as_deref(),
        host: f.host_filter.as_deref(),
        operation: f.operation,
        permission_type: f.permission_type,
    })
}

/// The deletion record filter that removes exactly `entry`.
///
/// Kafka's `AclControlManager.deleteAclsForFilter` writes one
/// `RemoveAccessControlEntryRecord` per matched ACL, so the log names each
/// removed binding rather than the request filter. Every axis is set, and the
/// metadata image holds each full tuple at most once, so this filter matches
/// that one entry.
pub(super) fn exact_filter(entry: &AclEntry) -> AclEntryFilter {
    AclEntryFilter {
        resource_type: Some(entry.resource_type),
        resource_name: Some(entry.resource_name.clone()),
        pattern_type: Some(entry.pattern_type),
        principal: Some(entry.principal.clone()),
        host: Some(entry.host.clone()),
        operation: Some(entry.operation),
        permission_type: Some(entry.permission_type),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType};

    use super::*;
    use crate::handlers::{
        acl_wire::binding_filter::{AxisFilter, PatternTypeFilter},
        delete_acls::test_support::{
            OPERATION_ANY, PATTERN_TYPE_MATCH, PERMISSION_ANY, RESOURCE_TYPE_TOPIC, acl, filter,
        },
    };

    #[test]
    fn build_filter_keeps_empty_strings_and_decodes_axes() {
        let f = DeleteAclsFilter {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some(String::new()),
            pattern_type_filter: PATTERN_TYPE_MATCH,
            principal_filter: Some(String::new()),
            host_filter: None,
            operation: OPERATION_ANY,
            permission_type: PERMISSION_ANY,
            ..Default::default()
        };

        let built = build_filter(&f).expect("filter");

        let expected = AclBindingFilter {
            resource_type: AxisFilter::Exact(ResourceType::Topic),
            resource_name: Some(String::new()),
            pattern_type: PatternTypeFilter::Match,
            principal: Some(String::new()),
            host: None,
            operation: AxisFilter::Any,
            permission_type: AxisFilter::Any,
        };
        assert!(built == expected);
    }

    /// Kafka's `DeleteAclsRequest.normalizeAndValidate` refuses only the
    /// `UNKNOWN` (0) byte. Any other byte it does not define parses, and
    /// `AclControlManager.validateFilter` refuses that one filter later.
    #[test]
    fn build_filter_refuses_only_the_unknown_byte() {
        type CorruptFilter = fn(&mut DeleteAclsFilter, i8);
        let cases: [(&str, CorruptFilter); 4] = [
            ("resource_type_filter", |f, b| f.resource_type_filter = b),
            ("pattern_type_filter", |f, b| f.pattern_type_filter = b),
            ("operation", |f, b| f.operation = b),
            ("permission_type", |f, b| f.permission_type = b),
        ];
        for (axis, corrupt) in cases {
            let mut unknown = filter(Some("orders"), Some("User:alice"));
            corrupt(&mut unknown, 0);
            check!(build_filter(&unknown) == Err(UnknownElement), "axis {axis}");

            let mut undefined = filter(Some("orders"), Some("User:alice"));
            corrupt(&mut undefined, 99);
            check!(build_filter(&undefined).is_ok(), "axis {axis}");
        }
    }

    #[test]
    fn exact_filter_matches_only_its_entry() {
        let entry = acl("orders", "User:alice", AclOperation::Read);
        let neighbours = [
            AclEntry {
                pattern_type: PatternType::Prefixed,
                ..entry.clone()
            },
            AclEntry {
                permission_type: PermissionType::Deny,
                ..entry.clone()
            },
            AclEntry {
                host: "10.0.0.1".into(),
                ..entry.clone()
            },
            acl("orders", "User:alice", AclOperation::Write),
            acl("orders", "User:bob", AclOperation::Read),
            acl("order", "User:alice", AclOperation::Read),
        ];

        let exact = exact_filter(&entry);

        check!(exact.matches(&entry));
        for other in &neighbours {
            check!(!exact.matches(other), "{other:?}");
        }
    }
}
