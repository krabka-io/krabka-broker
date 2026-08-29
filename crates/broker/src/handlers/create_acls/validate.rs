//! Validation of a single `CreateAcls` binding, and the `AclEntry` a valid one
//! becomes.
//!
//! Every rejection here is a per-binding `INVALID_REQUEST` with a fixed message,
//! and the accepted shape is the rule the authorizer will later evaluate, so
//! this decision is the whole security-relevant core of the handler and sits in
//! a file of its own.

use krabka_metadata::AclEntry;

use crate::{
    codes,
    handlers::acl_wire::{
        operation_concrete, pattern_type_concrete, permission_concrete, resource_type_concrete,
    },
};

/// Kafka principal-type prefix. It is the only principal type that Krabka
/// accepts.
pub(super) const USER_PRINCIPAL_PREFIX: &str = "User:";

pub(super) fn validate(
    c: &krabka_protocol::owned::create_acls_request::AclCreation,
    max_principal_bytes: usize,
    max_resource_name_bytes: usize,
) -> Result<AclEntry, (i16, &'static str)> {
    let resource_type = resource_type_concrete(c.resource_type)
        .map_err(|_| (codes::INVALID_REQUEST, "bad resource_type"))?;
    let pattern_type = pattern_type_concrete(c.resource_pattern_type)
        .map_err(|_| (codes::INVALID_REQUEST, "bad pattern_type"))?;
    let operation =
        operation_concrete(c.operation).map_err(|_| (codes::INVALID_REQUEST, "bad operation"))?;
    let permission_type = permission_concrete(c.permission_type)
        .map_err(|_| (codes::INVALID_REQUEST, "bad permission_type"))?;

    if c.resource_name.is_empty() {
        return Err((codes::INVALID_REQUEST, "empty resource_name"));
    }
    if c.resource_name.len() > max_resource_name_bytes {
        return Err((codes::INVALID_REQUEST, "resource_name too long"));
    }
    if c.resource_name.contains('\0') {
        return Err((codes::INVALID_REQUEST, "resource_name contains NUL"));
    }
    if !c.principal.starts_with(USER_PRINCIPAL_PREFIX) {
        return Err((codes::INVALID_REQUEST, "principal must start with User:"));
    }
    if c.principal.len() > max_principal_bytes {
        return Err((codes::INVALID_REQUEST, "principal too long"));
    }
    if c.host.is_empty() {
        return Err((codes::INVALID_REQUEST, "empty host"));
    }
    Ok(AclEntry {
        resource_type,
        resource_name: c.resource_name.clone(),
        pattern_type,
        principal: c.principal.clone(),
        host: c.host.clone(),
        operation,
        permission_type,
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
    use krabka_protocol::owned::create_acls_request::AclCreation;

    use crate::handlers::create_acls::test_support::{OPERATION_READ, creation, validate};

    #[test]
    fn validate_rejects_malformed_resource_principal_and_host() {
        type CorruptCreation = fn(&mut AclCreation);

        let valid = creation("topic-a", "User:alice", OPERATION_READ);
        let entry = validate(&valid).expect("valid ACL creation");
        let expected = AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "topic-a".into(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        };
        assert!(entry == expected);

        let cases: [(CorruptCreation, &str); 4] = [
            (|c| c.resource_name.clear(), "empty resource_name"),
            (
                |c| c.resource_name = "bad\0name".into(),
                "resource_name contains NUL",
            ),
            (
                |c| c.principal = "alice".into(),
                "principal must start with User:",
            ),
            (|c| c.host.clear(), "empty host"),
        ];
        for (corrupt, want) in cases {
            let mut c = valid.clone();
            corrupt(&mut c);
            assert!(validate(&c).unwrap_err().1 == want, "expected {want:?}");
        }
    }
}
