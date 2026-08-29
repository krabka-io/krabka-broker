//! Predicates that decide whether one stored ACL entry applies to an
//! authorization request.
//!
//! An entry applies only when its principal, its host, and its operation all
//! match the request. The principal and host tests are equality-or-wildcard
//! checks. The operation test additionally carries Kafka's one-way
//! operation-implication table, which is why it lives beside them rather than
//! in the decision loop.

use krabka_metadata::{AclEntry, AclOperation};

pub(super) fn matches_principal(entry: &AclEntry, user_pattern: &str) -> bool {
    entry.principal == "User:*" || entry.principal == user_pattern
}

pub(super) fn matches_host(entry: &AclEntry, host: &str) -> bool {
    entry.host == "*" || entry.host == host
}

/// Returns true when an ACL with the `stored` operation grants access for an
/// authorization request with the `requested` operation.
///
/// Beyond an exact match and the `All` wildcard, this function applies Kafka's
/// operation-implication table:
///
/// | stored          | implies                |
/// |-----------------|------------------------|
/// | Read            | Describe               |
/// | Write           | Describe               |
/// | Delete          | Describe               |
/// | Alter           | Describe               |
/// | `AlterConfigs`  | `DescribeConfigs`      |
/// | All             | Everything             |
///
/// The table is one-way: Describe does NOT imply Read, and so on.
pub(super) fn matches_operation(stored: AclOperation, requested: AclOperation) -> bool {
    if stored == requested {
        return true;
    }
    if matches!(stored, AclOperation::All) {
        return true;
    }
    implies(stored, requested)
}

fn implies(stored: AclOperation, requested: AclOperation) -> bool {
    matches!(
        (stored, requested),
        (
            AclOperation::Read | AclOperation::Write | AclOperation::Delete | AclOperation::Alter,
            AclOperation::Describe,
        ) | (AclOperation::AlterConfigs, AclOperation::DescribeConfigs)
    )
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use krabka_metadata::{
        AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
    };

    use crate::{
        AuthorizationResult, Authorizer, SimpleAclAuthorizer,
        simple::test_support::{
            acl_op_on, addr, alice, img, no_super, req, req_on, topic_acl, topic_acl_op,
        },
    };

    #[test]
    fn principal_wildcard_matches_any_user() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:*",
            "*",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read))
                == AuthorizationResult::Allow
        );
    }

    #[test]
    fn host_filter_matches_specific_ip() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:alice",
            "127.0.0.1",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h_match: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let h_nomatch: SocketAddr = "127.0.0.2:5000".parse().unwrap();
        let auth = SimpleAclAuthorizer::new(no_super());
        for (_name, host, expected) in [
            ("host match", &h_match, AuthorizationResult::Allow),
            ("host mismatch", &h_nomatch, AuthorizationResult::Deny),
        ] {
            assert2::assert!(
                auth.authorize(&img, &req(&a, host, "foo", AclOperation::Read)) == expected
            );
        }
    }

    #[test]
    fn operation_all_matches_any_op() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::All,
            "User:alice",
            "*",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        for op in [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Describe,
            AclOperation::Delete,
        ] {
            assert2::assert!(
                auth.authorize(&img, &req(&a, &h, "foo", op)) == AuthorizationResult::Allow
            );
        }
    }

    #[test]
    fn operation_specific_does_not_match_others() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
            PermissionType::Allow,
            AclOperation::Read,
            "User:alice",
            "*",
            PatternType::Literal,
            "foo",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            [AclOperation::Read, AclOperation::Write]
                .map(|operation| { auth.authorize(&img, &req(&a, &h, "foo", operation)) })
                == [AuthorizationResult::Allow, AuthorizationResult::Deny]
        );
    }

    #[test]
    fn read_implies_describe_on_topic() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::Read,
            "foo",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Describe))
                == AuthorizationResult::Allow
        );
    }

    #[test]
    fn write_implies_describe_on_topic() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::Write,
            "foo",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Describe))
                == AuthorizationResult::Allow
        );
    }

    #[test]
    fn delete_implies_describe() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::Delete,
            "foo",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Describe))
                == AuthorizationResult::Allow
        );
    }

    #[test]
    fn alter_implies_describe() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::Alter,
            "foo",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Describe))
                == AuthorizationResult::Allow
        );
    }

    #[test]
    fn alter_configs_implies_describe_configs() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::AlterConfigs,
            "foo",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::DescribeConfigs))
                == AuthorizationResult::Allow
        );
    }

    #[test]
    fn describe_does_not_imply_read() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl_op(
            PermissionType::Allow,
            AclOperation::Describe,
            "foo",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read))
                == AuthorizationResult::Deny
        );
    }

    #[test]
    fn implication_works_on_group_resource() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_op_on(
            ResourceType::Group,
            PermissionType::Allow,
            AclOperation::Read,
            "cg-1",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(
                &img,
                &req_on(&a, &h, ResourceType::Group, "cg-1", AclOperation::Describe)
            ) == AuthorizationResult::Allow
        );
    }

    #[test]
    fn implication_works_on_cluster_resource() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_op_on(
            ResourceType::Cluster,
            PermissionType::Allow,
            AclOperation::Alter,
            "kafka-cluster",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(
                &img,
                &req_on(
                    &a,
                    &h,
                    ResourceType::Cluster,
                    "kafka-cluster",
                    AclOperation::Describe
                )
            ) == AuthorizationResult::Allow
        );
    }

    #[test]
    fn implication_works_on_transactional_id_resource() {
        let mut img = img();
        img.apply(&MetadataRecord::V1AccessControlEntry(acl_op_on(
            ResourceType::TransactionalId,
            PermissionType::Allow,
            AclOperation::Write,
            "tx-1",
        )));
        let a = alice();
        let h = addr();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(
                &img,
                &req_on(
                    &a,
                    &h,
                    ResourceType::TransactionalId,
                    "tx-1",
                    AclOperation::Describe
                )
            ) == AuthorizationResult::Allow
        );
    }
}
