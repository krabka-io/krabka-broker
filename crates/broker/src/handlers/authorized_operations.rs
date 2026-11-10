//! KIP-430: computes the `(cluster|topic|group)_authorized_operations`
//! bitfield.
//!
//! The `Metadata`, `DescribeCluster`, `DescribeGroups`, and
//! `ConsumerGroupDescribe` responses carry the bitfield when the matching
//! request flag is set.
//!
//! The encoding works like this. For each operation in the supported set of
//! the resource type, the module asks the authorizer. On Allow it ORs
//! `1 << op.code()` into the bitfield. `op.code()` is the same wire
//! discriminant the ACL handlers serialize. See
//! [`super::acl_wire::operation_to_wire`].
//!
//! Kafka's convention is that the field is `i32::MIN`, the "not present"
//! sentinel, when the include flag is *not* set. That is already the
//! schema-level default, so the handlers fill the field only when the request
//! opts in.
//!
//! The map from a resource to its supported-operation set follows
//! `org.apache.kafka.security.authorizer.AclEntry#supportedOperations`:
//!
//! | resource         | operations                                                        |
//! |------------------|-------------------------------------------------------------------|
//! | Topic            | Read, Write, Create, Delete, Alter, Describe, DescribeConfigs,    |
//! |                  | AlterConfigs                                                      |
//! | Group            | Read, Describe, Delete, DescribeConfigs, AlterConfigs             |
//! | Cluster          | Create, Alter, Describe, ClusterAction, AlterConfigs,             |
//! |                  | DescribeConfigs, IdempotentWrite                                  |
//! | TransactionalId  | Describe, Write, TwoPhaseCommit                                    |
//! | DelegationToken  | Describe                                                          |

use {
    super::{RequestContext, acl_wire::operation_to_wire},
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        codes,
    },
    krabka_metadata::{AclOperation, MetadataImage, ResourceType},
    krabka_protocol::owned::{
        consumer_group_describe_response, describe_groups_response, share_group_describe_response,
        streams_group_describe_response,
    },
};

/// Returns the operations whose Allow decision adds to the
/// authorized-operations bitfield for `resource_type`. It matches Kafka's
/// `AclEntry.supportedOperations(...)`.
#[must_use]
pub fn supported_operations(resource_type: ResourceType) -> &'static [AclOperation] {
    match resource_type {
        ResourceType::Topic => &[
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
        ],
        // KIP-848 group configs made DescribeConfigs and AlterConfigs
        // grantable on a group.
        ResourceType::Group => &[
            AclOperation::Read,
            AclOperation::Describe,
            AclOperation::Delete,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
        ],
        ResourceType::Cluster => &[
            AclOperation::Create,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::ClusterAction,
            AclOperation::AlterConfigs,
            AclOperation::DescribeConfigs,
            AclOperation::IdempotentWrite,
        ],
        ResourceType::TransactionalId => &[
            AclOperation::Describe,
            AclOperation::Write,
            // KIP-939: 2PC participation is a grantable TransactionalId
            // permission, so it surfaces in the authorized-operations bitfield.
            AclOperation::TwoPhaseCommit,
        ],
        ResourceType::DelegationToken => &[AclOperation::Describe],
        // KIP-373.
        ResourceType::User => &[AclOperation::CreateTokens, AclOperation::DescribeTokens],
    }
}

/// Computes the authorized-operations bitfield for
/// `(resource_type, resource_name)` from the point of view of the principal
/// and peer of `ctx`. The bit for an operation is
/// `1 << operation_to_wire(op)`, which matches Kafka's
/// `AuthorizationHelper.authorizedOperations(...)`.
///
/// Kafka builds each `Action` with `logIfAllowed` and `logIfDenied` off, since
/// a probe of every supported operation is no request to do any of them, so the
/// checks go through [`Authorizer::authorize_quiet`]: a Deny is not audited or
/// counted.
#[must_use]
pub fn authorized_operations_bits(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource_type: ResourceType,
    resource_name: &str,
) -> i32 {
    let mut bits: i32 = 0;
    for &op in supported_operations(resource_type) {
        let allow = authorizer.authorize_quiet(image, &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type,
            resource_name,
            operation: op,
        });
        if allow == AuthorizationResult::Allow {
            bits |= 1_i32 << operation_to_wire(op);
        }
    }
    bits
}

/// A row of a group describe response: `DescribeGroups`,
/// `ConsumerGroupDescribe`, `ShareGroupDescribe` or `StreamsGroupDescribe`.
pub trait DescribedGroupRow {
    /// The row that answers `group_id` with `error_code` and `error_message`
    /// alone, every other field at its generated default.
    fn error_row(group_id: &str, error_code: i16, error_message: Option<String>) -> Self;
    fn group_id(&self) -> &str;
    fn error_code(&self) -> i16;
    fn set_authorized_operations(&mut self, bits: i32);
}

krabka_broker_macros::described_group_rows! {
    consumer_group_describe_response::DescribedGroup,
    describe_groups_response::DescribedGroup,
    share_group_describe_response::DescribedGroup,
    streams_group_describe_response::DescribedGroup,
}

/// KIP-430: when `include` is set, gives every row whose error is `NONE` the
/// bitfield of the group operations the principal of `ctx` holds. Every other
/// row keeps the wire-default `i32::MIN` "not present" sentinel.
pub fn fill_group_authorized_operations<R: DescribedGroupRow>(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    include: bool,
    rows: &mut [R],
) {
    if !include {
        return;
    }
    for row in rows {
        if row.error_code() == codes::NONE {
            let bits = authorized_operations_bits(
                authorizer,
                image,
                ctx,
                ResourceType::Group,
                row.group_id(),
            );
            row.set_authorized_operations(bits);
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::authorizer::{AllowAllAuthorizer, SimpleAclAuthorizer},
        assert2::assert,
        krabka_bdd::Given,
        krabka_metadata::{AclEntry, MetadataRecord, PermissionType, ResourceType},
        krabka_security::Principal,
        serde_json::Value,
        std::{collections::HashSet, net::SocketAddr},
        uuid::Uuid,
    };

    fn principal(name: &str) -> Principal { sasl_principal(name) }

    fn ctx<'a>(principal: &'a Principal, peer: &'a SocketAddr) -> RequestContext<'a> {
        request_context(principal, peer, "authorized-operations-test")
    }

    fn addr() -> SocketAddr { "127.0.0.1:0".parse().unwrap() }

    use crate::test_support::{allow_acl, request_context, sasl_principal};

    fn bit(op: AclOperation) -> i32 { 1_i32 << operation_to_wire(op) }

    /// alice's bitfield on `name` under a `SimpleAclAuthorizer` with `super_users`.
    fn alice_acl_bits(
        super_users: &HashSet<String>,
        image: &MetadataImage,
        resource: ResourceType,
        name: &str,
    ) -> i32 {
        let auth = SimpleAclAuthorizer::new(super_users.clone());
        let principal = principal("alice");
        let host = addr();
        authorized_operations_bits(&auth, image, &ctx(&principal, &host), resource, name)
    }

    #[test]
    fn every_describe_schema_preserves_error_fields_and_default_operation_bits() {
        #[derive(Debug, PartialEq)]
        struct RowFields<'a> {
            error_message: Option<&'a str>,
            authorized_operations: i32,
        }
        fn check_row<Row: DescribedGroupRow>(fields: impl for<'a> Fn(&'a Row) -> RowFields<'a>) {
            let mut row = Row::error_row("group", -1, Some("refusal".into()));
            assert!(row.group_id() == "group");
            assert!(row.error_code() == -1);
            assert!(
                fields(&row)
                    == RowFields {
                        error_message: Some("refusal"),
                        authorized_operations: i32::MIN
                    }
            );
            row.set_authorized_operations(1 << 8);
            assert!(
                fields(&row)
                    == RowFields {
                        error_message: Some("refusal"),
                        authorized_operations: 1 << 8
                    }
            );
            assert!(row.group_id() == "group");
            assert!(row.error_code() == -1);
        }

        check_row::<consumer_group_describe_response::DescribedGroup>(|row| RowFields {
            error_message: row.error_message.as_deref(),
            authorized_operations: row.authorized_operations,
        });
        check_row::<describe_groups_response::DescribedGroup>(|row| RowFields {
            error_message: row.error_message.as_deref(),
            authorized_operations: row.authorized_operations,
        });
        check_row::<share_group_describe_response::DescribedGroup>(|row| RowFields {
            error_message: row.error_message.as_deref(),
            authorized_operations: row.authorized_operations,
        });
        check_row::<streams_group_describe_response::DescribedGroup>(|row| RowFields {
            error_message: row.error_message.as_deref(),
            authorized_operations: row.authorized_operations,
        });
    }

    /// A resource type as a scenario names it.
    #[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
    enum Resource {
        Topic,
        Group,
        Cluster,
    }

    impl From<Resource> for ResourceType {
        fn from(resource: Resource) -> Self {
            match resource {
                Resource::Topic => Self::Topic,
                Resource::Group => Self::Group,
                Resource::Cluster => Self::Cluster,
            }
        }
    }

    /// Whether an ACL a scenario gives alice allows or denies its operation.
    #[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
    enum Permission {
        Allowed,
        Denied,
    }

    impl From<Permission> for PermissionType {
        fn from(permission: Permission) -> Self {
            match permission {
                Permission::Allowed => Self::Allow,
                Permission::Denied => Self::Deny,
            }
        }
    }

    /// The name of the resource every ACL in the bitfield scenarios is on.
    const RESOURCE_NAME: &str = "orders";

    struct AuthorizationContract {
        resource: Given<ResourceType>,
        operation: Given<AclOperation>,
        supported: Given<HashSet<AclOperation>>,
        mask: Given<i32>,
        /// The ACLs alice holds. A scenario with no ACL step holds none.
        image: MetadataImage,
        /// The super users the authorizer knows. A scenario with no super-user
        /// step has none.
        super_users: HashSet<String>,
    }

    #[rstest::fixture]
    fn authorization_contract() -> AuthorizationContract {
        AuthorizationContract {
            resource: Given::new(
                "resource",
                "Given an authorization query concerns a <resource> resource",
            ),
            operation: Given::new("operation", "Given the authorized operation is <operation>"),
            supported: Given::new(
                "supported",
                "When the broker lists the resource's supported permissions",
            ),
            mask: Given::new(
                "mask",
                "When the broker reports alice's authorized operations on the <resource>",
            ),
            image: MetadataImage::new(Uuid::nil()),
            super_users: HashSet::new(),
        }
    }

    fn named_operation(name: &str) -> AclOperation {
        serde_json::from_value(Value::String(name.to_owned())).expect("a named ACL operation")
    }

    #[krabka_bdd::given("an authorization query concerns a {resource} resource")]
    fn authorization_resource(
        authorization_contract: &mut AuthorizationContract,
        resource: Resource,
    ) {
        authorization_contract.resource.set(resource.into());
    }

    #[krabka_bdd::given("the authorized operation is {operation}")]
    fn authorization_operation(
        authorization_contract: &mut AuthorizationContract,
        operation: String,
    ) {
        authorization_contract
            .operation
            .set(named_operation(&operation));
    }

    #[krabka_bdd::given("alice is {permission} {operation} on a/the {resource}")]
    fn alice_holds_acl(
        authorization_contract: &mut AuthorizationContract,
        permission: Permission,
        operation: String,
        resource: Resource,
    ) {
        authorization_contract
            .image
            .apply(&MetadataRecord::V1AccessControlEntry(AclEntry {
                permission_type: permission.into(),
                ..allow_acl()
                    .resource_type(resource.into())
                    .resource_name(RESOURCE_NAME)
                    .operation(named_operation(&operation))
                    .call()
            }));
    }

    #[krabka_bdd::given("{name} is a super user")]
    fn super_user(authorization_contract: &mut AuthorizationContract, name: String) {
        authorization_contract.super_users.insert(name);
    }

    #[krabka_bdd::when("the broker lists the resource's supported permissions")]
    fn query_supported_permissions(authorization_contract: &mut AuthorizationContract) {
        let resource = *authorization_contract.resource.get();
        authorization_contract
            .supported
            .set(supported_operations(resource).iter().copied().collect());
    }

    #[krabka_bdd::when("the operation is encoded in the authorized-operations bitfield")]
    fn encode_authorized_operation(authorization_contract: &mut AuthorizationContract) {
        let operation = *authorization_contract.operation.get();
        authorization_contract.mask.set(bit(operation));
    }

    #[krabka_bdd::when("the broker reports alice's authorized operations on a/the {resource}")]
    fn report_alice_operations(
        authorization_contract: &mut AuthorizationContract,
        resource: Resource,
    ) {
        let bits = alice_acl_bits(
            &authorization_contract.super_users,
            &authorization_contract.image,
            resource.into(),
            RESOURCE_NAME,
        );
        authorization_contract.mask.set(bits);
    }

    #[krabka_bdd::then("the supported permissions are exactly {permissions}")]
    fn supported_permissions(authorization_contract: &AuthorizationContract, permissions: String) {
        let expected: HashSet<_> = permissions.split(',').map(named_operation).collect();
        assert!(*authorization_contract.supported.get() == expected);
    }

    #[krabka_bdd::then("the operation contributes mask {mask}")]
    fn operation_mask(authorization_contract: &AuthorizationContract, mask: i32) {
        assert!(*authorization_contract.mask.get() == mask);
    }

    #[krabka_bdd::then("the authorized operations are exactly {operations}")]
    fn authorized_operations_are(
        authorization_contract: &AuthorizationContract,
        operations: String,
    ) {
        let expected = operations
            .split(',')
            .map(named_operation)
            .fold(0_i32, |acc, op| acc | bit(op));
        let actual = *authorization_contract.mask.get();
        assert!(actual == expected, "0b{actual:b} != 0b{expected:b}");
    }

    #[krabka_bdd::then("no operation is authorized")]
    fn no_operation_authorized(authorization_contract: &AuthorizationContract) {
        assert!(*authorization_contract.mask.get() == 0);
    }

    #[test]
    fn allow_all_authorizer_sets_every_supported_bit_for_each_resource() {
        let auth = AllowAllAuthorizer;
        let img = MetadataImage::new(Uuid::nil());
        let p = principal("anyone");
        let h = addr();

        for rt in [
            ResourceType::Topic,
            ResourceType::Group,
            ResourceType::Cluster,
            ResourceType::TransactionalId,
            ResourceType::DelegationToken,
        ] {
            let bits = authorized_operations_bits(&auth, &img, &ctx(&p, &h), rt, "name");
            let expected = supported_operations(rt)
                .iter()
                .copied()
                .fold(0_i32, |acc, op| acc | bit(op));
            assert!(bits == expected, "{rt:?}: full mask under AllowAll");
        }
    }

    /// The group bitfield goes only on an opted-in row whose error is `NONE`;
    /// every other row keeps the `i32::MIN` "not present" sentinel.
    #[test]
    fn fill_group_authorized_operations_fills_only_clean_opted_in_rows() {
        use krabka_protocol::owned::consumer_group_describe_response::DescribedGroup;

        let img = MetadataImage::new(Uuid::nil());
        let p = principal("anyone");
        let h = addr();
        let all_group_bits = supported_operations(ResourceType::Group)
            .iter()
            .fold(0_i32, |acc, &op| acc | bit(op));
        let rows = || {
            vec![
                DescribedGroup::error_row("clean", codes::NONE, None),
                DescribedGroup::error_row("missing", codes::GROUP_ID_NOT_FOUND, None),
            ]
        };
        for (include, clean_bits) in [(true, all_group_bits), (false, i32::MIN)] {
            let mut got = rows();
            fill_group_authorized_operations(
                &AllowAllAuthorizer,
                &img,
                &ctx(&p, &h),
                include,
                &mut got,
            );
            let want = vec![
                DescribedGroup {
                    group_id: "clean".into(),
                    authorized_operations: clean_bits,
                    ..DescribedGroup::default()
                },
                DescribedGroup {
                    group_id: "missing".into(),
                    error_code: codes::GROUP_ID_NOT_FOUND,
                    ..DescribedGroup::default()
                },
            ];
            assert!(got == want, "include = {include}");
        }
    }
    rstest_bdd_macros::scenarios!("tests/features/migrated/src/handlers/authorized_operations.feature", fixtures = [authorization_contract: AuthorizationContract]);
}
