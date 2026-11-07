//! Wire ↔ metadata enum mapping for ACL handlers.
//!
//! Kafka serializes ACL enums as `i8` discriminants. This module gives the
//! conversions and a small error type. The error type covers an unknown
//! discriminant and an ANY value where a concrete value is necessary.
//! [`binding_filter`] decodes and matches the filters of `DescribeAcls` and
//! `DeleteAcls`.

use krabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType};

pub mod binding_filter;

/// Kafka's singleton cluster resource name (`Resource.CLUSTER_NAME`).
/// Every cluster-scoped ACL and authorization check targets this name.
pub const CLUSTER_RESOURCE_NAME: &str = "kafka-cluster";

/// What Kafka's `AclApis.handleDescribeAcls` writes into the error message of
/// the `DescribeAcls` response when the cluster runs no authorizer.
pub const NO_AUTHORIZER_MESSAGE: &str = "No Authorizer is configured on the broker";

/// The message of the `SecurityDisabledException` that Kafka's
/// `AclApis.handleCreateAcls` and `handleDeleteAcls` build their responses
/// from when the cluster runs no authorizer. It is not the `DescribeAcls`
/// text: those two go through `getErrorResponse`, which keeps the exception
/// message.
pub const NO_AUTHORIZER_EXCEPTION_MESSAGE: &str = "No Authorizer is configured.";

/// Kafka's `QuorumController.MAX_RECORDS_PER_USER_OP`: the most ACL records a
/// single `CreateAcls` or `DeleteAcls` request may write. `AclControlManager`
/// bounds both requests by it.
pub const MAX_ACL_RECORDS_PER_REQUEST: usize = 10_000;

/// Wire `i8` discriminant of an ACL `resource_type` field.
pub type ResourceTypeCode = i8;
/// Wire `i8` discriminant of an ACL `pattern_type` field.
pub type PatternTypeCode = i8;
/// Wire `i8` discriminant of an ACL `operation` field.
pub type OperationCode = i8;
/// Wire `i8` discriminant of an ACL `permission_type` field.
pub type PermissionTypeCode = i8;

/// Wire byte for `UNKNOWN` (0). Every ACL enum axis shares it.
pub const WIRE_UNKNOWN: i8 = 0;
/// Wire byte for `ANY` (1), a filter wildcard and never a concrete value.
/// Every ACL enum axis shares it.
pub const WIRE_ANY: i8 = 1;
/// Wire byte for `PatternType::MATCH` (2, KIP-290), a filter-only
/// wildcard that matches both literal and prefixed patterns.
pub const WIRE_PATTERN_MATCH: i8 = 2;

/// Concrete `ResourceType` bytes, from Kafka's
/// `org.apache.kafka.common.resource.ResourceType`.
pub const WIRE_RESOURCE_TOPIC: i8 = 2;
pub const WIRE_RESOURCE_GROUP: i8 = 3;
pub const WIRE_RESOURCE_CLUSTER: i8 = 4;
pub const WIRE_RESOURCE_TRANSACTIONAL_ID: i8 = 5;
pub const WIRE_RESOURCE_DELEGATION_TOKEN: i8 = 6;
pub const WIRE_RESOURCE_USER: i8 = 7;

/// Concrete `PatternType` bytes, from Kafka's
/// `org.apache.kafka.common.resource.PatternType`.
pub const WIRE_PATTERN_LITERAL: i8 = 3;
pub const WIRE_PATTERN_PREFIXED: i8 = 4;

/// Concrete `AclOperation` bytes, from Kafka's
/// `org.apache.kafka.common.acl.AclOperation`.
pub const WIRE_OPERATION_ALL: i8 = 2;
pub const WIRE_OPERATION_READ: i8 = 3;
pub const WIRE_OPERATION_WRITE: i8 = 4;
pub const WIRE_OPERATION_CREATE: i8 = 5;
pub const WIRE_OPERATION_DELETE: i8 = 6;
pub const WIRE_OPERATION_ALTER: i8 = 7;
pub const WIRE_OPERATION_DESCRIBE: i8 = 8;
pub const WIRE_OPERATION_CLUSTER_ACTION: i8 = 9;
pub const WIRE_OPERATION_DESCRIBE_CONFIGS: i8 = 10;
pub const WIRE_OPERATION_ALTER_CONFIGS: i8 = 11;
pub const WIRE_OPERATION_IDEMPOTENT_WRITE: i8 = 12;
pub const WIRE_OPERATION_CREATE_TOKENS: i8 = 13;
pub const WIRE_OPERATION_DESCRIBE_TOKENS: i8 = 14;
pub const WIRE_OPERATION_TWO_PHASE_COMMIT: i8 = 15;

/// Concrete `AclPermissionType` bytes, from Kafka's
/// `org.apache.kafka.common.acl.AclPermissionType`.
pub const WIRE_PERMISSION_DENY: i8 = 2;
pub const WIRE_PERMISSION_ALLOW: i8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireAclError {
    UnknownDiscriminant,
    /// A caller passed ANY or MATCH where a concrete value is necessary.
    AnyRequiresFilter,
}

/// Parse a wire `resource_type` byte into a concrete `ResourceType`.
/// `CreateAcls` uses this function, because its resource must be concrete.
pub fn resource_type_concrete(b: ResourceTypeCode) -> Result<ResourceType, WireAclError> {
    match b {
        WIRE_RESOURCE_TOPIC => Ok(ResourceType::Topic),
        WIRE_RESOURCE_GROUP => Ok(ResourceType::Group),
        WIRE_RESOURCE_CLUSTER => Ok(ResourceType::Cluster),
        WIRE_RESOURCE_TRANSACTIONAL_ID => Ok(ResourceType::TransactionalId),
        WIRE_RESOURCE_DELEGATION_TOKEN => Ok(ResourceType::DelegationToken),
        WIRE_RESOURCE_USER => Ok(ResourceType::User),
        WIRE_UNKNOWN | WIRE_ANY => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

#[must_use]
pub fn resource_type_to_wire(rt: ResourceType) -> ResourceTypeCode {
    match rt {
        ResourceType::Topic => WIRE_RESOURCE_TOPIC,
        ResourceType::Group => WIRE_RESOURCE_GROUP,
        ResourceType::Cluster => WIRE_RESOURCE_CLUSTER,
        ResourceType::TransactionalId => WIRE_RESOURCE_TRANSACTIONAL_ID,
        ResourceType::DelegationToken => WIRE_RESOURCE_DELEGATION_TOKEN,
        ResourceType::User => WIRE_RESOURCE_USER,
    }
}

/// Parse a wire `pattern_type` byte into a concrete `PatternType`.
/// `CreateAcls` uses this function, because its pattern must be concrete.
pub fn pattern_type_concrete(b: PatternTypeCode) -> Result<PatternType, WireAclError> {
    match b {
        WIRE_PATTERN_LITERAL => Ok(PatternType::Literal),
        WIRE_PATTERN_PREFIXED => Ok(PatternType::Prefixed),
        WIRE_UNKNOWN..=WIRE_PATTERN_MATCH => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

#[must_use]
pub fn pattern_type_to_wire(pt: PatternType) -> PatternTypeCode {
    match pt {
        PatternType::Literal => WIRE_PATTERN_LITERAL,
        PatternType::Prefixed => WIRE_PATTERN_PREFIXED,
    }
}

/// Parse a wire `operation` byte into a concrete `AclOperation`.
/// `CreateAcls` uses this function, because its operation must be concrete.
pub fn operation_concrete(b: OperationCode) -> Result<AclOperation, WireAclError> {
    match b {
        WIRE_OPERATION_ALL => Ok(AclOperation::All),
        WIRE_OPERATION_READ => Ok(AclOperation::Read),
        WIRE_OPERATION_WRITE => Ok(AclOperation::Write),
        WIRE_OPERATION_CREATE => Ok(AclOperation::Create),
        WIRE_OPERATION_DELETE => Ok(AclOperation::Delete),
        WIRE_OPERATION_ALTER => Ok(AclOperation::Alter),
        WIRE_OPERATION_DESCRIBE => Ok(AclOperation::Describe),
        WIRE_OPERATION_CLUSTER_ACTION => Ok(AclOperation::ClusterAction),
        WIRE_OPERATION_DESCRIBE_CONFIGS => Ok(AclOperation::DescribeConfigs),
        WIRE_OPERATION_ALTER_CONFIGS => Ok(AclOperation::AlterConfigs),
        WIRE_OPERATION_IDEMPOTENT_WRITE => Ok(AclOperation::IdempotentWrite),
        WIRE_OPERATION_CREATE_TOKENS => Ok(AclOperation::CreateTokens),
        WIRE_OPERATION_DESCRIBE_TOKENS => Ok(AclOperation::DescribeTokens),
        WIRE_OPERATION_TWO_PHASE_COMMIT => Ok(AclOperation::TwoPhaseCommit),
        WIRE_UNKNOWN | WIRE_ANY => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

#[must_use]
pub fn operation_to_wire(op: AclOperation) -> OperationCode {
    match op {
        AclOperation::All => WIRE_OPERATION_ALL,
        AclOperation::Read => WIRE_OPERATION_READ,
        AclOperation::Write => WIRE_OPERATION_WRITE,
        AclOperation::Create => WIRE_OPERATION_CREATE,
        AclOperation::Delete => WIRE_OPERATION_DELETE,
        AclOperation::Alter => WIRE_OPERATION_ALTER,
        AclOperation::Describe => WIRE_OPERATION_DESCRIBE,
        AclOperation::ClusterAction => WIRE_OPERATION_CLUSTER_ACTION,
        AclOperation::DescribeConfigs => WIRE_OPERATION_DESCRIBE_CONFIGS,
        AclOperation::AlterConfigs => WIRE_OPERATION_ALTER_CONFIGS,
        AclOperation::IdempotentWrite => WIRE_OPERATION_IDEMPOTENT_WRITE,
        AclOperation::CreateTokens => WIRE_OPERATION_CREATE_TOKENS,
        AclOperation::DescribeTokens => WIRE_OPERATION_DESCRIBE_TOKENS,
        AclOperation::TwoPhaseCommit => WIRE_OPERATION_TWO_PHASE_COMMIT,
    }
}

/// Parse a wire `permission_type` byte into a concrete `PermissionType`.
/// `CreateAcls` uses this function, because its permission must be concrete.
pub fn permission_concrete(b: PermissionTypeCode) -> Result<PermissionType, WireAclError> {
    match b {
        WIRE_PERMISSION_DENY => Ok(PermissionType::Deny),
        WIRE_PERMISSION_ALLOW => Ok(PermissionType::Allow),
        WIRE_UNKNOWN | WIRE_ANY => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

#[must_use]
pub fn permission_to_wire(pt: PermissionType) -> PermissionTypeCode {
    match pt {
        PermissionType::Deny => WIRE_PERMISSION_DENY,
        PermissionType::Allow => WIRE_PERMISSION_ALLOW,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    #[test]
    fn resource_type_concrete_rejects_any() {
        let cases = [
            (1, Err(WireAclError::AnyRequiresFilter)),
            (2, Ok(ResourceType::Topic)),
            (3, Ok(ResourceType::Group)),
            (4, Ok(ResourceType::Cluster)),
        ];
        for (byte, want) in cases {
            assert!(resource_type_concrete(byte) == want, "byte {byte}");
        }
    }

    #[yare::parameterized(
        literal = { 3, Ok(PatternType::Literal) },
        prefixed = { 4, Ok(PatternType::Prefixed) },
        unknown = { WIRE_UNKNOWN, Err(WireAclError::AnyRequiresFilter) },
        any = { WIRE_ANY, Err(WireAclError::AnyRequiresFilter) },
        match_pattern = { WIRE_PATTERN_MATCH, Err(WireAclError::AnyRequiresFilter) },
        unknown_discriminant = { 5, Err(WireAclError::UnknownDiscriminant) },
    )]
    fn concrete_parsers_distinguish_wildcards_from_unknown_discriminants(
        byte: PatternTypeCode,
        want: Result<PatternType, WireAclError>,
    ) {
        // The wildcard bytes (UNKNOWN/ANY, and MATCH for pattern types) must
        // report AnyRequiresFilter — a different wire error than a junk
        // discriminant — and the concrete codes must map, not fall through.
        check!(pattern_type_concrete(byte) == want, "pattern byte {byte}");
    }

    #[yare::parameterized(
        unknown = { WIRE_UNKNOWN },
        any = { WIRE_ANY },
    )]
    fn operation_and_permission_wildcards_require_a_filter(byte: i8) {
        check!(
            operation_concrete(byte) == Err(WireAclError::AnyRequiresFilter),
            "operation byte {byte}"
        );
        check!(
            permission_concrete(byte) == Err(WireAclError::AnyRequiresFilter),
            "permission byte {byte}"
        );
    }

    #[yare::parameterized(
        all = { AclOperation::All },
        read = { AclOperation::Read },
        write = { AclOperation::Write },
        idempotent_write = { AclOperation::IdempotentWrite },
        two_phase_commit = { AclOperation::TwoPhaseCommit },
    )]
    fn operation_round_trip_through_wire(op: AclOperation) {
        let b = operation_to_wire(op);
        assert2::assert!(operation_concrete(b).unwrap() == op);
    }

    /// KIP-939: the `TWO_PHASE_COMMIT` operation is wire byte 15 and must
    /// round-trip through the concrete codec and the encoder, so that
    /// `CreateAcls` and `DescribeAcls` can carry the 2PC grant on a
    /// `TransactionalId`.
    #[test]
    fn two_phase_commit_operation_is_byte_15() {
        check!(operation_to_wire(AclOperation::TwoPhaseCommit) == 15);
        check!(operation_concrete(15) == Ok(AclOperation::TwoPhaseCommit));
    }

    /// KIP-373's `USER` resource type (7) and its `CREATE_TOKENS` (13) and
    /// `DESCRIBE_TOKENS` (14) operations round-trip through the wire codec,
    /// with Kafka's `ResourceType` and `AclOperation` codes.
    #[test]
    fn kip_373_user_resource_has_wire_code_seven() {
        check!(resource_type_concrete(7) == Ok(ResourceType::User));
        check!(resource_type_to_wire(ResourceType::User) == 7);
    }

    #[yare::parameterized(
        create_tokens = { 13, AclOperation::CreateTokens },
        describe_tokens = { 14, AclOperation::DescribeTokens },
    )]
    fn kip_373_user_resource_and_token_operations_round_trip(code: i8, operation: AclOperation) {
        check!(operation_concrete(code) == Ok(operation));
        check!(operation_to_wire(operation) == code);
    }

    /// The KIP-48 `TOKEN` resource type, also named `DELEGATION_TOKEN`, is
    /// wire byte 6. It must round-trip through the wire codec, so that
    /// `CreateAcls`, `DeleteAcls`, and `DescribeAcls` can carry ACLs that
    /// guard delegation tokens.
    #[test]
    fn delegation_token_resource_type_now_accepted() {
        use krabka_metadata::AclEntry;

        // Concrete (CreateAcls) codec.
        check!(resource_type_concrete(6) == Ok(ResourceType::DelegationToken));
        // The filter codec is `binding_filter`, tested beside it.
        // Encoder.
        check!(resource_type_to_wire(ResourceType::DelegationToken) == 6);

        // Build a concrete AclEntry at the canonical (Describe, Allow)
        // shape KIP-48 token ACLs use; verify the wire bytes line up.
        let entry = AclEntry {
            resource_type: resource_type_concrete(6).unwrap(),
            resource_name: "User:alice".into(),
            pattern_type: PatternType::Literal,
            principal: "User:bob".into(),
            host: "*".into(),
            operation: operation_concrete(8).unwrap(),
            permission_type: permission_concrete(3).unwrap(),
        };
        assert!(resource_type_to_wire(entry.resource_type) == 6);
        let expected = AclEntry {
            resource_type: ResourceType::DelegationToken,
            resource_name: "User:alice".into(),
            pattern_type: PatternType::Literal,
            principal: "User:bob".into(),
            host: "*".into(),
            operation: AclOperation::Describe,
            permission_type: PermissionType::Allow,
        };
        assert!(entry == expected);
    }
}
