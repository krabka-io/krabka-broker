//! Validation of a single `CreateAcls` binding, and the `AclEntry` a valid one
//! becomes.
//!
//! Kafka validates a binding in two places, and this module keeps its order:
//! `AclApis.handleCreateAcls` refuses a `CLUSTER` binding not named
//! `kafka-cluster` and an empty resource name, then the controller's
//! `AclControlManager.validateNewAcl` refuses a non-concrete resource type,
//! pattern type, operation or permission type, and a principal with no `:`.
//! Kafka trunk, and so `unstable.feature.versions.enable`, also refuses a
//! malformed host ([`HostCheck`]). Every refusal is a per-binding
//! `INVALID_REQUEST` (or `UNSUPPORTED_VERSION` for a CIDR host below its
//! metadata version) carrying Kafka's message text, which `kafka-acls` prints.
//!
//! Two whole-request refusals sit in front of this, in the handler. A wire
//! `UNKNOWN` (0) element fails Kafka's `CreateAclsRequest.validate` while the
//! request is parsed, which closes the connection ([`has_unknown_element`]).
//! An `ANY` or `MATCH` element makes the `ResourcePattern` or
//! `AccessControlEntry` constructor throw, which fails every creation
//! ([`has_filter_only_element`]).

use {
    crate::{
        codes,
        handlers::acl_wire::{
            CLUSTER_RESOURCE_NAME, operation_concrete, pattern_type_concrete, permission_concrete,
            resource_type_concrete,
        },
    },
    krabka_authz::cidr::Cidr,
    krabka_metadata::AclEntry,
    krabka_protocol::owned::create_acls_request::AclCreation,
};

/// The message `CreateAcls` answers with when a host containing `/` is
/// submitted below [`crate::features::CIDR_ACL_HOST_MIN_LEVEL`]. Verbatim
/// from Kafka's `AclControlManager.validateHostPattern`.
const CIDR_UNSUPPORTED_VERSION_MESSAGE: &str =
    "CIDR-based ACL host patterns require metadata version 4.4-IV1 or higher.";

/// Wire `resource_type` of Kafka's `ResourceType.CLUSTER`.
const RESOURCE_TYPE_CLUSTER: i8 = 4;
/// Wire `UNKNOWN` (0), shared by every ACL enum axis.
const WIRE_UNKNOWN: i8 = 0;
/// Wire `ANY` (1), shared by every ACL enum axis.
const WIRE_ANY: i8 = 1;
/// Wire `PatternType.MATCH` (2).
const WIRE_PATTERN_MATCH: i8 = 2;

/// True when any creation carries a wire `UNKNOWN` (0) resource type, pattern
/// type, operation or permission type.
///
/// Kafka's `CreateAclsRequest.validate` throws for such a request while it is
/// parsed, so the broker sends no response and closes the connection.
pub(super) fn has_unknown_element(creations: &[AclCreation]) -> bool {
    creations.iter().any(|c| {
        c.resource_type == WIRE_UNKNOWN
            || c.resource_pattern_type == WIRE_UNKNOWN
            || c.operation == WIRE_UNKNOWN
            || c.permission_type == WIRE_UNKNOWN
    })
}

/// True when any creation carries a filter-only value: `ANY` for the resource
/// type, operation or permission type, or `ANY` or `MATCH` for the pattern
/// type.
///
/// Kafka's `AclApis.handleCreateAcls` builds every binding before it
/// validates any of them, and the `ResourcePattern` and `AccessControlEntry`
/// constructors throw `IllegalArgumentException` for these values. The
/// request's error path then answers every creation with
/// `UNKNOWN_SERVER_ERROR` and no message, because `ApiError.fromThrowable`
/// drops the text of an unknown server error.
pub(super) fn has_filter_only_element(creations: &[AclCreation]) -> bool {
    creations.iter().any(|c| {
        c.resource_type == WIRE_ANY
            || matches!(c.resource_pattern_type, WIRE_ANY | WIRE_PATTERN_MATCH)
            || c.operation == WIRE_ANY
            || c.permission_type == WIRE_ANY
    })
}

/// Kafka's `ResourceType` constant name for a wire code, as
/// `ResourceType.fromCode` resolves it: an unrecognized code is `UNKNOWN`.
fn resource_type_name(code: i8) -> &'static str {
    match code {
        1 => "ANY",
        2 => "TOPIC",
        3 => "GROUP",
        4 => "CLUSTER",
        5 => "TRANSACTIONAL_ID",
        6 => "DELEGATION_TOKEN",
        7 => "USER",
        _ => "UNKNOWN",
    }
}

/// Kafka's `PatternType` constant name for a wire code.
fn pattern_type_name(code: i8) -> &'static str {
    match code {
        1 => "ANY",
        2 => "MATCH",
        3 => "LITERAL",
        4 => "PREFIXED",
        _ => "UNKNOWN",
    }
}

/// Kafka's `AclOperation` constant name for a wire code.
fn operation_name(code: i8) -> &'static str {
    match code {
        1 => "ANY",
        2 => "ALL",
        3 => "READ",
        4 => "WRITE",
        5 => "CREATE",
        6 => "DELETE",
        7 => "ALTER",
        8 => "DESCRIBE",
        9 => "CLUSTER_ACTION",
        10 => "DESCRIBE_CONFIGS",
        11 => "ALTER_CONFIGS",
        12 => "IDEMPOTENT_WRITE",
        13 => "CREATE_TOKENS",
        14 => "DESCRIBE_TOKENS",
        15 => "TWO_PHASE_COMMIT",
        _ => "UNKNOWN",
    }
}

/// Kafka's `AclPermissionType` constant name for a wire code.
fn permission_type_name(code: i8) -> &'static str {
    match code {
        1 => "ANY",
        2 => "DENY",
        3 => "ALLOW",
        _ => "UNKNOWN",
    }
}

/// A binding that `CreateAcls` refuses: Kafka's error code and message.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct AclRefusal {
    pub(super) code: i16,
    pub(super) message: String,
}

fn invalid(message: String) -> AclRefusal {
    AclRefusal {
        code: codes::INVALID_REQUEST,
        message,
    }
}

/// What `CreateAcls` checks about the host of a binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HostCheck {
    /// Kafka 4.3.1, whose `AclControlManager.validateNewAcl` has no host check
    /// and whose `AccessControlEntry` only refuses a null host. Any host,
    /// empty or containing `/`, is stored as text.
    Unchecked,
    /// Kafka trunk's `AclControlManager.validateHostPattern`, which
    /// `unstable.feature.versions.enable` turns on. A host with a `/` is a
    /// CIDR range when `cidr_hosts_supported`, and is refused otherwise.
    Trunk { cidr_hosts_supported: bool },
}

pub(super) fn validate(c: &AclCreation, host_check: HostCheck) -> Result<AclEntry, AclRefusal> {
    // `AclApis.handleCreateAcls`, before the binding reaches the controller.
    // The authorizer only ever asks about `kafka-cluster`, so a CLUSTER ACL
    // under any other name would be stored and never match.
    if c.resource_type == RESOURCE_TYPE_CLUSTER && c.resource_name != CLUSTER_RESOURCE_NAME {
        return Err(invalid(format!(
            "The only valid name for the CLUSTER resource is {CLUSTER_RESOURCE_NAME}"
        )));
    }
    if c.resource_name.is_empty() {
        return Err(invalid("Invalid empty resource name".to_owned()));
    }

    // `AclControlManager.validateNewAcl`: a code Kafka does not recognize is
    // refused under Kafka's name for it, `UNKNOWN`.
    let resource_type = resource_type_concrete(c.resource_type).map_err(|_| {
        invalid(format!(
            "Invalid resourceType {}",
            resource_type_name(c.resource_type)
        ))
    })?;
    let pattern_type = pattern_type_concrete(c.resource_pattern_type).map_err(|_| {
        invalid(format!(
            "Invalid patternType {}",
            pattern_type_name(c.resource_pattern_type)
        ))
    })?;
    let operation = operation_concrete(c.operation)
        .map_err(|_| invalid(format!("Invalid operation {}", operation_name(c.operation))))?;
    let permission_type = permission_concrete(c.permission_type).map_err(|_| {
        invalid(format!(
            "Invalid permissionType {}",
            permission_type_name(c.permission_type)
        ))
    })?;
    // Kafka accepts any `<type>:<name>` principal and refuses only one with
    // no colon at all.
    if !c.principal.contains(':') {
        return Err(invalid(format!(
            "Could not parse principal from `{}` (no colon is present separating the principal \
             type from the principal name)",
            c.principal
        )));
    }
    if let HostCheck::Trunk {
        cidr_hosts_supported,
    } = host_check
    {
        validate_host(&c.host, cidr_hosts_supported)?;
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

/// Validates an ACL host the way Kafka's `AclControlManager.validateHostPattern`
/// does: the wildcard and a plain address are always accepted; a CIDR range
/// (anything containing `/`) needs `cidr_hosts_supported` (KIP-1276, gated on
/// `metadata.version` `4.4-IV1`) and must parse; an empty host is always
/// rejected. `CreateAcls` stores whatever host string is accepted as literal
/// text -- this only rejects what Kafka would also reject, it does not
/// normalize the text that gets stored.
fn validate_host(host: &str, cidr_hosts_supported: bool) -> Result<(), AclRefusal> {
    if host.is_empty() {
        return Err(invalid("Host pattern cannot be null or empty".to_owned()));
    }
    if !host.contains('/') {
        return Ok(());
    }
    if !cidr_hosts_supported {
        return Err(AclRefusal {
            code: codes::UNSUPPORTED_VERSION,
            message: CIDR_UNSUPPORTED_VERSION_MESSAGE.to_owned(),
        });
    }
    Cidr::parse(host)
        .map(|_| ())
        .map_err(|reason| invalid(format!("Invalid CIDR notation '{host}': {reason}")))
}

#[cfg(test)]
mod tests {
    use {
        super::{AclRefusal, HostCheck, has_filter_only_element, has_unknown_element},
        crate::handlers::create_acls::test_support::{OPERATION_READ, creation, validate},
        assert2::assert,
        krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType},
        krabka_protocol::owned::create_acls_request::AclCreation,
    };

    struct AclValidationScenario {
        creation: AclCreation,
        result: Option<Result<AclEntry, AclRefusal>>,
        classification: Option<(bool, bool)>,
        cidr_supported: bool,
    }
    #[rstest::fixture]
    fn acl_validation() -> AclValidationScenario {
        let creation = creation("topic-a", "User:alice", OPERATION_READ);
        AclValidationScenario {
            creation,
            result: None,
            classification: None,
            cidr_supported: false,
        }
    }
    #[rstest_bdd_macros::given(
        "an ACL creation for resource type {resource} name {name} principal {principal} operation {operation} permission {permission}"
    )]
    fn acl_binding(
        acl_validation: &mut AclValidationScenario,
        resource: i8,
        name: String,
        principal: String,
        operation: i8,
        permission: i8,
    ) {
        let c = &mut acl_validation.creation;
        c.resource_type = resource;
        c.resource_name = name;
        c.principal = principal;
        c.operation = operation;
        c.permission_type = permission;
    }
    #[rstest_bdd_macros::given("the ACL creation field {field} is {value}")]
    fn acl_field(acl_validation: &mut AclValidationScenario, field: String, value: String) {
        let c = &mut acl_validation.creation;
        let value = if value == "empty" {
            String::new()
        } else {
            value
        };
        match field.as_str() {
            "resource type" => c.resource_type = value.parse().unwrap(),
            "pattern type" => c.resource_pattern_type = value.parse().unwrap(),
            "operation" => c.operation = value.parse().unwrap(),
            "permission" => c.permission_type = value.parse().unwrap(),
            "resource name" => c.resource_name = value,
            "principal" => c.principal = value,
            "host" => c.host = value,
            _ => panic!("unknown ACL field: {field}"),
        }
    }
    #[rstest_bdd_macros::given("the ACL host is {host} and CIDR host support is {supported}")]
    fn acl_host(acl_validation: &mut AclValidationScenario, host: String, supported: bool) {
        acl_validation.creation.host = if host == "empty" { String::new() } else { host };
        acl_validation.cidr_supported = supported;
    }
    #[rstest_bdd_macros::when("the ACL creation is validated using Kafka 4.3 rules")]
    fn validate_acl_creation(acl_validation: &mut AclValidationScenario) {
        acl_validation.result = Some(validate(&acl_validation.creation));
    }
    #[rstest_bdd_macros::when("the ACL creation is validated using trunk host rules")]
    fn validate_acl_host(acl_validation: &mut AclValidationScenario) {
        acl_validation.result = Some(super::validate(
            &acl_validation.creation,
            HostCheck::Trunk {
                cidr_hosts_supported: acl_validation.cidr_supported,
            },
        ));
    }
    #[rstest_bdd_macros::when(
        "the request checks the creation beside a valid baseline for unknown and filter-only elements"
    )]
    fn classify_acl_creation(acl_validation: &mut AclValidationScenario) {
        let creations = [
            creation("topic-a", "User:alice", OPERATION_READ),
            acl_validation.creation.clone(),
        ];
        acl_validation.classification = Some((
            has_unknown_element(&creations),
            has_filter_only_element(&creations),
        ));
    }
    #[rstest_bdd_macros::then(
        "the stored ACL has resource {resource} name {name} principal {principal} operation {operation} permission {permission}"
    )]
    fn check_acl_entry(
        acl_validation: &AclValidationScenario,
        resource: String,
        name: String,
        principal: String,
        operation: String,
        permission: String,
    ) {
        let resource_type = match resource.as_str() {
            "Topic" => ResourceType::Topic,
            "Cluster" => ResourceType::Cluster,
            "User" => ResourceType::User,
            _ => panic!("unknown resource: {resource}"),
        };
        let operation = match operation.as_str() {
            "Read" => AclOperation::Read,
            "Alter" => AclOperation::Alter,
            "CreateTokens" => AclOperation::CreateTokens,
            _ => panic!("unknown operation: {operation}"),
        };
        let permission_type = match permission.as_str() {
            "Allow" => PermissionType::Allow,
            "Deny" => PermissionType::Deny,
            _ => panic!("unknown permission: {permission}"),
        };
        assert!(
            acl_validation.result.as_ref().unwrap()
                == &Ok(AclEntry {
                    resource_type,
                    resource_name: name,
                    pattern_type: PatternType::Literal,
                    principal,
                    host: "*".into(),
                    operation,
                    permission_type
                })
        );
    }
    #[rstest_bdd_macros::then("ACL creation is refused with code {code} and message {message}")]
    fn check_acl_refusal(acl_validation: &AclValidationScenario, code: i16, message: String) {
        assert!(acl_validation.result.as_ref().unwrap() == &Err(AclRefusal { code, message }));
    }
    #[rstest_bdd_macros::then("the ACL host is accepted")]
    fn check_acl_host_accepted(acl_validation: &AclValidationScenario) {
        assert!(acl_validation.result.as_ref().unwrap().is_ok());
    }
    #[rstest_bdd_macros::then(
        "unknown elements are {unknown} and filter-only elements are {filter_only}"
    )]
    fn check_acl_classification(
        acl_validation: &AclValidationScenario,
        unknown: bool,
        filter_only: bool,
    ) {
        assert!(acl_validation.classification.unwrap() == (unknown, filter_only));
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_acl_validation",fixtures=[acl_validation:AclValidationScenario]);

    /// A resource name with a NUL byte is a valid Kafka resource name.
    #[test]
    fn validate_accepts_a_nul_in_the_resource_name() {
        let c = creation("bad\0name", "User:alice", OPERATION_READ);
        assert!(validate(&c).map(|entry| entry.resource_name) == Ok("bad\0name".to_owned()));
    }

    /// Kafka 4.3.1 has no host check, so every host is stored as the text it
    /// arrived as: the wildcard, an address, a range, a malformed range and
    /// the empty string.
    #[test]
    fn validate_accepts_any_host_without_the_trunk_check() {
        for host in ["*", "10.0.0.1", "10.0.0.0/8", "10.0.0.0/33", "a/b", ""] {
            let mut c = creation("topic-a", "User:alice", OPERATION_READ);
            c.host = host.into();
            assert!(
                super::validate(&c, HostCheck::Unchecked).map(|entry| entry.host)
                    == Ok(host.to_owned()),
                "host {host:?}"
            );
        }
    }
}
