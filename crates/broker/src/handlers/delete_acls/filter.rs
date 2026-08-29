//! Translation of one wire `DeleteAclsFilter` into the `AclEntryFilter` that
//! the metadata image matches ACL entries against.
//!
//! Every axis is optional on the wire, and Kafka spells "no constraint" two
//! ways: an ANY code on the enum axes and an empty or absent string on the
//! name, principal, and host axes. Getting that collapse wrong would silently
//! widen or narrow a delete, so the decision sits in a file of its own.

use krabka_metadata::AclEntryFilter;
use krabka_protocol::owned::delete_acls_request::DeleteAclsFilter;

use crate::handlers::acl_wire::{
    WireAclError, operation_filter, pattern_type_filter, permission_filter, resource_type_filter,
};

pub(super) fn build_filter(f: &DeleteAclsFilter) -> Result<AclEntryFilter, WireAclError> {
    let resource_name = f.resource_name_filter.clone().filter(|s| !s.is_empty());
    let principal = f.principal_filter.clone().filter(|s| !s.is_empty());
    let host = f.host_filter.clone().filter(|s| !s.is_empty());
    Ok(AclEntryFilter {
        resource_type: resource_type_filter(f.resource_type_filter)?,
        resource_name,
        pattern_type: pattern_type_filter(f.pattern_type_filter)?,
        principal,
        host,
        operation: operation_filter(f.operation)?,
        permission_type: permission_filter(f.permission_type)?,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::ResourceType;

    use super::*;
    use crate::handlers::delete_acls::test_support::{
        OPERATION_ANY, PATTERN_TYPE_ANY, PERMISSION_ANY, RESOURCE_TYPE_TOPIC, filter,
    };

    #[test]
    fn build_filter_collapses_empty_strings_and_decodes_axes() {
        let f = DeleteAclsFilter {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some(String::new()),
            pattern_type_filter: PATTERN_TYPE_ANY,
            principal_filter: Some(String::new()),
            host_filter: Some(String::new()),
            operation: OPERATION_ANY,
            permission_type: PERMISSION_ANY,
            ..Default::default()
        };

        let built = build_filter(&f).expect("filter");

        let expected = AclEntryFilter {
            resource_type: Some(ResourceType::Topic),
            resource_name: None,
            pattern_type: None,
            principal: None,
            host: None,
            operation: None,
            permission_type: None,
        };
        assert!(built == expected);
    }

    #[test]
    fn build_filter_rejects_malformed_axes() {
        type CorruptFilter = fn(&mut DeleteAclsFilter);
        let cases: [(&str, CorruptFilter); 3] = [
            ("resource_type_filter", |f| f.resource_type_filter = 99),
            ("pattern_type_filter", |f| f.pattern_type_filter = 99),
            ("operation", |f| f.operation = 99),
        ];
        for (axis, corrupt) in cases {
            let mut f = filter(Some("orders"), Some("User:alice"));
            corrupt(&mut f);
            assert!(build_filter(&f).is_err(), "axis {axis}");
        }
    }
}
