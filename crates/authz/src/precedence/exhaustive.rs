use super::*;

/// Fixed candidate pool that covers every decision dimension.
///
/// The pool holds Allow and Deny on the same (resource, op) for deny-wins. It
/// holds the ops that imply (Read, Write, and `AlterConfigs`), a leaf op
/// (Describe), and `All`. It holds Literal-exact, Literal-`*`, and Prefixed
/// patterns, the principal `User:alice` and the `User:*` wildcard, the `*` host
/// and a specific host, and one non-matching decoy.
fn candidate_pool() -> Vec<AclEntry> {
    use AclOperation::{All, AlterConfigs, Describe, Read, Write};
    use PatternType::{Literal, Prefixed};
    use PermissionType::{Allow, Deny};
    use ResourceType::Topic;
    vec![
        entry(Topic, Literal, "foo", "User:alice", "*", Read, Allow), // E0
        entry(Topic, Literal, "foo", "User:alice", "*", Read, Deny),  // E1 deny-wins vs E0
        entry(Topic, Literal, "*", "User:alice", "*", Write, Allow), // E2 literal-* wildcard, Write->Describe
        entry(Topic, Prefixed, "te", "User:alice", "*", Describe, Allow), // E3 prefix, leaf op
        entry(Topic, Literal, "foo", "User:*", "*", All, Allow),     // E4 principal wildcard, All
        entry(Topic, Literal, "foo", "User:*", "*", All, Deny),      // E5 broad deny-wins
        entry(Topic, Literal, "foo", "User:alice", "10.0.0.1", Read, Allow), // E6 host-specific
        entry(
            Topic,
            Literal,
            "foo",
            "User:alice",
            "*",
            AlterConfigs,
            Allow,
        ), // E7 ->DescribeConfigs
        entry(Topic, Literal, "bar", "User:bob", "*", Read, Allow), // E8 decoy (other principal/resource)
        entry(Topic, Prefixed, "te", "User:alice", "*", Describe, Deny), // E9 prefix deny on Describe
    ]
}

/// Representative requests that cover operation implication, both wildcards,
/// both patterns, principal and host filtering, and default-deny.
fn requests<'a>(
    alice: &'a Principal,
    bob: &'a Principal,
    h1: &'a SocketAddr,
    h2: &'a SocketAddr,
) -> Vec<AuthorizationRequest<'a>> {
    use AclOperation::{Create, Describe, DescribeConfigs, Read, Write};
    let r = |p: &'a Principal, h: &'a SocketAddr, name: &'a str, op| AuthorizationRequest {
        principal: p,
        host: h,
        resource_type: ResourceType::Topic,
        resource_name: name,
        operation: op,
    };
    vec![
        r(alice, h1, "foo", Read),
        r(alice, h1, "foo", Describe),
        r(alice, h1, "foo", Write),
        r(alice, h1, "team-x", Read),
        r(alice, h1, "tea", Describe),
        r(alice, h1, "foo", DescribeConfigs),
        r(alice, h1, "other", Read),
        r(alice, h2, "foo", Read),
        r(bob, h1, "foo", Read),
        r(bob, h1, "bar", Read),
        r(alice, h1, "foo", Create),
        r(alice, h1, "*", Read),
    ]
}

#[test]
fn acl_precedence_exhaustive() {
    let pool = candidate_pool();
    let k = pool.len();
    assert2::assert!(k == 10);

    let alice = principal(ALICE);
    let bob = principal("bob");
    let h1: SocketAddr = "10.0.0.1:9092".parse().unwrap();
    let h2: SocketAddr = "10.0.0.2:9092".parse().unwrap();
    let reqs = requests(&alice, &bob, &h1, &h2);

    let no_super: HashSet<String> = HashSet::new();
    let super_alice: HashSet<String> = std::iter::once(ALICE.to_string()).collect();

    for mask in 0u32..(1u32 << k) {
        let entries: Vec<AclEntry> = (0..k)
            .filter(|i| mask & (1 << i) != 0)
            .map(|i| pool[i].clone())
            .collect();
        for req in &reqs {
            check(&no_super, &entries, req);
            check(&super_alice, &entries, req);
        }
    }
}
