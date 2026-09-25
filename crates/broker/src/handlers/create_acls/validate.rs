//! Validation of a single `CreateAcls` binding, and the `AclEntry` a valid one
//! becomes.
//!
//! Every rejection here is a per-binding `INVALID_REQUEST` with a fixed message,
//! and the accepted shape is the rule the authorizer will later evaluate, so
//! this decision is the whole security-relevant core of the handler and sits in
//! a file of its own.

use krabka_authz::cidr::Cidr;
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

/// The message `CreateAcls` answers with when a host containing `/` is
/// submitted below [`crate::features::CIDR_ACL_HOST_MIN_LEVEL`]. Verbatim
/// from Kafka's `AclControlManager.validateHostPattern`.
const CIDR_UNSUPPORTED_VERSION_MESSAGE: &str =
    "CIDR-based ACL host patterns require metadata version 4.4-IV1 or higher.";

pub(super) fn validate(
    c: &krabka_protocol::owned::create_acls_request::AclCreation,
    max_principal_bytes: usize,
    max_resource_name_bytes: usize,
    cidr_hosts_supported: bool,
) -> Result<AclEntry, (i16, String)> {
    let resource_type = resource_type_concrete(c.resource_type)
        .map_err(|_| (codes::INVALID_REQUEST, "bad resource_type".to_owned()))?;
    let pattern_type = pattern_type_concrete(c.resource_pattern_type)
        .map_err(|_| (codes::INVALID_REQUEST, "bad pattern_type".to_owned()))?;
    let operation = operation_concrete(c.operation)
        .map_err(|_| (codes::INVALID_REQUEST, "bad operation".to_owned()))?;
    let permission_type = permission_concrete(c.permission_type)
        .map_err(|_| (codes::INVALID_REQUEST, "bad permission_type".to_owned()))?;

    if c.resource_name.is_empty() {
        return Err((codes::INVALID_REQUEST, "empty resource_name".to_owned()));
    }
    if c.resource_name.len() > max_resource_name_bytes {
        return Err((codes::INVALID_REQUEST, "resource_name too long".to_owned()));
    }
    if c.resource_name.contains('\0') {
        return Err((
            codes::INVALID_REQUEST,
            "resource_name contains NUL".to_owned(),
        ));
    }
    if !c.principal.starts_with(USER_PRINCIPAL_PREFIX) {
        return Err((
            codes::INVALID_REQUEST,
            "principal must start with User:".to_owned(),
        ));
    }
    if c.principal.len() > max_principal_bytes {
        return Err((codes::INVALID_REQUEST, "principal too long".to_owned()));
    }
    validate_host(&c.host, cidr_hosts_supported)?;
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

/// Validates an ACL host the way Kafka's `AclControlManager.validateHostPattern`
/// does: the wildcard and a plain address are always accepted; a CIDR range
/// (anything containing `/`) needs `cidr_hosts_supported` (KIP-1276, gated on
/// `metadata.version` `4.4-IV1`) and must parse; an empty host is always
/// rejected. `CreateAcls` stores whatever host string is accepted as literal
/// text -- this only rejects what Kafka would also reject, it does not
/// normalize the text that gets stored.
fn validate_host(host: &str, cidr_hosts_supported: bool) -> Result<(), (i16, String)> {
    if host.is_empty() {
        return Err((
            codes::INVALID_REQUEST,
            "Host pattern cannot be null or empty".to_owned(),
        ));
    }
    if !host.contains('/') {
        return Ok(());
    }
    if !cidr_hosts_supported {
        return Err((
            codes::UNSUPPORTED_VERSION,
            CIDR_UNSUPPORTED_VERSION_MESSAGE.to_owned(),
        ));
    }
    Cidr::parse(host).map(|_| ()).map_err(|reason| {
        (
            codes::INVALID_REQUEST,
            format!("Invalid CIDR notation '{host}': {reason}"),
        )
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
            (|c| c.host.clear(), "Host pattern cannot be null or empty"),
        ];
        for (corrupt, want) in cases {
            let mut c = valid.clone();
            corrupt(&mut c);
            assert!(validate(&c).unwrap_err().1 == want, "expected {want:?}");
        }
    }

    /// KIP-1276 host validation, table-driven against
    /// `AclControlManager.validateHostPattern`'s four outcomes: the wildcard
    /// and a plain address always pass; a CIDR host needs
    /// `cidr_hosts_supported` and must parse; an empty host is always
    /// rejected regardless of the gate.
    #[test]
    fn validate_host_matches_kafka_cidr_gating() {
        type Case = (&'static str, bool, Option<(i16, &'static str)>);
        let cases: &[Case] = &[
            ("*", false, None),
            ("*", true, None),
            ("10.0.0.1", false, None),
            ("10.0.0.1", true, None),
            ("10.0.0.0/8", true, None),
            (
                "10.0.0.0/8",
                false,
                Some((
                    crate::codes::UNSUPPORTED_VERSION,
                    "CIDR-based ACL host patterns require metadata version 4.4-IV1 or higher.",
                )),
            ),
            (
                "10.0.0.0/33",
                true,
                Some((
                    crate::codes::INVALID_REQUEST,
                    "Invalid CIDR notation '10.0.0.0/33': prefix length 33 exceeds the 32-bit address",
                )),
            ),
            ("2001:db8::/32", true, None),
            (
                "",
                true,
                Some((
                    crate::codes::INVALID_REQUEST,
                    "Host pattern cannot be null or empty",
                )),
            ),
            (
                "",
                false,
                Some((
                    crate::codes::INVALID_REQUEST,
                    "Host pattern cannot be null or empty",
                )),
            ),
        ];
        for (host, cidr_hosts_supported, expected_err) in cases {
            let mut c = creation("topic-a", "User:alice", OPERATION_READ);
            c.host = (*host).into();
            let got = super::validate(&c, usize::MAX, usize::MAX, *cidr_hosts_supported).err();
            let expected = expected_err.map(|(code, msg)| (code, msg.to_owned()));
            assert!(
                got == expected,
                "host {host:?} cidr_hosts_supported {cidr_hosts_supported}"
            );
        }
    }
}
