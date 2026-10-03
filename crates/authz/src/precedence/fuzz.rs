use std::{collections::HashSet, net::SocketAddr};

use krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
use proptest::prelude::*;

use super::{check, principal};
use crate::AuthorizationRequest;

fn op_of(i: u8) -> AclOperation {
    use AclOperation::{
        All, Alter, AlterConfigs, ClusterAction, Create, Delete, Describe, DescribeConfigs,
        IdempotentWrite, Read, Write,
    };
    [
        All,
        Read,
        Write,
        Create,
        Delete,
        Alter,
        Describe,
        ClusterAction,
        DescribeConfigs,
        AlterConfigs,
        IdempotentWrite,
    ][i as usize % 11]
}
fn rt_of(i: u8) -> ResourceType {
    use ResourceType::{Cluster, DelegationToken, Group, Topic, TransactionalId};
    [Topic, Group, Cluster, TransactionalId, DelegationToken][i as usize % 5]
}
fn name_of(i: u8) -> &'static str {
    ["foo", "bar", "team-x", "te", "*", "other"][i as usize % 6]
}
fn princ_of(i: u8) -> &'static str {
    ["User:alice", "User:bob", "User:*"][i as usize % 3]
}
fn host_of(i: u8) -> &'static str {
    ["*", "10.0.0.1", "10.0.0.2"][i as usize % 3]
}

prop_compose! {
    fn arb_entry()(
        perm in any::<bool>(), op in 0u8..11, rt in 0u8..5, name in 0u8..6,
        princ in 0u8..3, host in 0u8..3, pat in any::<bool>(),
    ) -> AclEntry {
        AclEntry {
            resource_type: rt_of(rt),
            resource_name: name_of(name).into(),
            pattern_type: if pat { PatternType::Prefixed } else { PatternType::Literal },
            principal: princ_of(princ).into(),
            host: host_of(host).into(),
            operation: op_of(op),
            permission_type: if perm { PermissionType::Deny } else { PermissionType::Allow },
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]
    #[test]
    fn authorize_matches_oracle_and_sources_agree(
        entries in proptest::collection::vec(arb_entry(), 0..12),
        req_princ in 0u8..3, req_host in 0u8..3, req_rt in 0u8..5,
        req_name in 0u8..6, req_op in 0u8..11,
        super_alice in any::<bool>(), super_bob in any::<bool>(),
    ) {
        // Request principal name drawn from {alice, bob, carol}; carol is in
        // no ACL, exercising default-deny + a non-matching principal.
        let pname = ["alice", "bob", "carol"][req_princ as usize % 3];
        let p = principal(pname);
        let host: SocketAddr = format!(
            "{}:9092",
            ["10.0.0.1", "10.0.0.2", "10.0.0.9"][req_host as usize % 3]
        )
        .parse()
        .unwrap();
        let req = AuthorizationRequest {
            principal: &p,
            host: &host,
            resource_type: rt_of(req_rt),
            resource_name: name_of(req_name),
            operation: op_of(req_op),
        };
        let mut su: HashSet<String> = HashSet::new();
        if super_alice {
            su.insert("alice".into());
        }
        if super_bob {
            su.insert("bob".into());
        }
        // `check` asserts real(image) == oracle AND real(cache) == oracle (so
        // image == cache too), via assert2 panics that proptest shrinks on.
        check(&su, &entries, &req);
    }
}
