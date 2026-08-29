//! The `--add-acl` spec parser.
//!
//! One flag value names a principal, a host, an operation, a permission, and a
//! resource pattern, and every one of those fields maps onto a `krabka_metadata`
//! enum. The name-to-variant tables that mapping needs are long enough to own a
//! file, and nothing outside the parse uses them.

use krabka_metadata::AclEntry;

pub(super) fn parse_acl_spec(spec: &str) -> Result<AclEntry, String> {
    use krabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType};

    let mut principal = None;
    let mut host = None;
    let mut operation = None;
    let mut permission = None;
    let mut resource_type = None;
    let mut resource_name = None;
    let mut pattern_type = PatternType::Literal;

    for kv in spec.split(',') {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| format!("malformed pair: {kv}"))?;
        match k {
            "principal" => principal = Some(v.to_string()),
            "host" => host = Some(v.to_string()),
            "operation" => {
                operation = Some(match v {
                    "All" => AclOperation::All,
                    "Read" => AclOperation::Read,
                    "Write" => AclOperation::Write,
                    "Create" => AclOperation::Create,
                    "Delete" => AclOperation::Delete,
                    "Alter" => AclOperation::Alter,
                    "Describe" => AclOperation::Describe,
                    "ClusterAction" => AclOperation::ClusterAction,
                    "DescribeConfigs" => AclOperation::DescribeConfigs,
                    "AlterConfigs" => AclOperation::AlterConfigs,
                    "IdempotentWrite" => AclOperation::IdempotentWrite,
                    "TwoPhaseCommit" => AclOperation::TwoPhaseCommit,
                    other => return Err(format!("unknown operation: {other}")),
                });
            }
            "permission" => {
                permission = Some(match v {
                    "Allow" => PermissionType::Allow,
                    "Deny" => PermissionType::Deny,
                    other => return Err(format!("unknown permission: {other}")),
                });
            }
            "resource" => {
                let mut parts = v.splitn(3, ':');
                let rt = parts.next().ok_or("missing resource type")?;
                let rn = parts.next().ok_or("missing resource name")?;
                if let Some(pt) = parts.next() {
                    pattern_type = match pt {
                        "Literal" => PatternType::Literal,
                        "Prefixed" => PatternType::Prefixed,
                        other => return Err(format!("unknown pattern: {other}")),
                    };
                }
                resource_type = Some(match rt {
                    "Topic" => ResourceType::Topic,
                    "Group" => ResourceType::Group,
                    "Cluster" => ResourceType::Cluster,
                    "TransactionalId" => ResourceType::TransactionalId,
                    other => return Err(format!("unknown resource type: {other}")),
                });
                resource_name = Some(rn.to_string());
            }
            other => return Err(format!("unknown key: {other}")),
        }
    }

    Ok(AclEntry {
        resource_type: resource_type.ok_or("resource required")?,
        resource_name: resource_name.ok_or("resource_name required")?,
        pattern_type,
        principal: principal.ok_or("principal required")?,
        host: host.ok_or("host required")?,
        operation: operation.ok_or("operation required")?,
        permission_type: permission.ok_or("permission required")?,
    })
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parse_acl_spec_minimal() {
        let s = "principal=User:admin,host=*,operation=All,permission=Allow,resource=Cluster:kafka-cluster";
        let entry = parse_acl_spec(s).unwrap();
        assert2::assert!(
            entry
                == AclEntry {
                    resource_type: krabka_metadata::ResourceType::Cluster,
                    resource_name: "kafka-cluster".to_string(),
                    pattern_type: krabka_metadata::PatternType::Literal,
                    principal: "User:admin".to_string(),
                    host: "*".to_string(),
                    operation: krabka_metadata::AclOperation::All,
                    permission_type: krabka_metadata::PermissionType::Allow,
                }
        );
    }

    #[test]
    fn parse_acl_spec_with_prefixed_pattern() {
        let s = "principal=User:alice,host=*,operation=Read,permission=Allow,resource=Topic:team-:Prefixed";
        let entry = parse_acl_spec(s).unwrap();
        assert2::assert!(entry.pattern_type == krabka_metadata::PatternType::Prefixed);
        assert2::assert!(entry.resource_name.as_str() == "team-");
    }

    #[test]
    fn parse_acl_spec_unknown_key_errors() {
        let s = "principal=User:admin,host=*,bogus=x";
        assert2::assert!(parse_acl_spec(s).is_err());
    }

    #[test]
    fn parse_acl_spec_all_operations() {
        use krabka_metadata::AclOperation;
        for (s, op) in [
            ("All", AclOperation::All),
            ("Read", AclOperation::Read),
            ("Write", AclOperation::Write),
            ("Create", AclOperation::Create),
            ("Delete", AclOperation::Delete),
            ("Alter", AclOperation::Alter),
            ("Describe", AclOperation::Describe),
            ("ClusterAction", AclOperation::ClusterAction),
            ("DescribeConfigs", AclOperation::DescribeConfigs),
            ("AlterConfigs", AclOperation::AlterConfigs),
            ("IdempotentWrite", AclOperation::IdempotentWrite),
            ("TwoPhaseCommit", AclOperation::TwoPhaseCommit),
        ] {
            let spec =
                format!("principal=User:u,host=*,operation={s},permission=Allow,resource=Topic:t");
            assert2::assert!(parse_acl_spec(&spec).unwrap().operation == op);
        }
    }

    #[test]
    fn parse_acl_spec_all_resource_types_and_deny() {
        use krabka_metadata::{PermissionType, ResourceType};
        for (s, rt) in [
            ("Topic", ResourceType::Topic),
            ("Group", ResourceType::Group),
            ("Cluster", ResourceType::Cluster),
            ("TransactionalId", ResourceType::TransactionalId),
        ] {
            let spec =
                format!("principal=User:u,host=*,operation=All,permission=Deny,resource={s}:n");
            let entry = parse_acl_spec(&spec).unwrap();
            assert2::assert!(entry.resource_type == rt);
            assert2::assert!(entry.permission_type == PermissionType::Deny);
        }
    }

    #[test]
    fn parse_acl_spec_error_branches() {
        for bad in [
            "principal=User:u,host=*,operation=Bogus,permission=Allow,resource=Topic:t",
            "principal=User:u,host=*,operation=All,permission=Maybe,resource=Topic:t",
            "principal=User:u,host=*,operation=All,permission=Allow,resource=Topic:t:Weird",
            "principal=User:u,host=*,operation=All,permission=Allow,resource=Bogus:t",
            "principal=User:u,host=*,operation=All,permission=Allow,resource=Topic",
            "malformedpair",
            "host=*,operation=All,permission=Allow,resource=Topic:t",
            "principal=User:u,operation=All,permission=Allow,resource=Topic:t",
            "principal=User:u,host=*,permission=Allow,resource=Topic:t",
            "principal=User:u,host=*,operation=All,resource=Topic:t",
            "principal=User:u,host=*,operation=All,permission=Allow",
        ] {
            assert2::assert!(parse_acl_spec(bad).is_err());
        }
    }
}
