//! Decision-order tests for [`SimpleAclAuthorizer`]: super-user bypass,
//! deny-wins, resource-pattern matching, and the default-deny fallback.
//!
//! The tests that cover which stored entries apply at all live beside those
//! predicates in [`super::matching`].

use krabka_metadata::{AclOperation, MetadataRecord, PatternType};
use krabka_security::Principal;

use super::*;
use crate::{
    authorize_topics,
    simple::test_support::{addr, alice, img, no_super, one_super, req, topic_acl},
};

#[test]
fn empty_image_with_no_super_users_defaults_to_deny() {
    // There is no compat shim that returns Allow in this case —
    // `SimpleAclAuthorizer` is default-deny when nothing matches.
    // Operators who want "allow everything" should configure
    // `AllowAllAuthorizer` explicitly.
    let img = img();
    let a = alice();
    let h = addr();
    let auth = SimpleAclAuthorizer::new(no_super());
    assert2::assert!(
        auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read)) == AuthorizationResult::Deny
    );
}

#[test]
fn super_user_bypass_grants_everything_even_with_acls() {
    let mut img = img();
    // A DENY ACL that would otherwise reject.
    img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
        PermissionType::Deny,
        AclOperation::Read,
        "User:alice",
        "*",
        PatternType::Literal,
        "foo",
    )));
    let a = alice();
    let h = addr();
    let auth = SimpleAclAuthorizer::new(one_super("alice"));
    assert2::assert!(
        auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read)) == AuthorizationResult::Allow
    );
}

#[test]
fn deny_by_default_when_super_user_set_but_principal_mismatches() {
    let mut img = img();
    // An ACL exists but doesn't match alice.
    img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
        PermissionType::Allow,
        AclOperation::Read,
        "User:bob",
        "*",
        PatternType::Literal,
        "foo",
    )));
    let a = alice();
    let h = addr();
    let auth = SimpleAclAuthorizer::new(one_super("admin"));
    assert2::assert!(
        auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read)) == AuthorizationResult::Deny
    );
}

#[test]
fn literal_allow_matches_exact_name() {
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
    for (_name, resource, expected) in [
        ("exact match", "foo", AuthorizationResult::Allow),
        ("literal mismatch", "foobar", AuthorizationResult::Deny),
    ] {
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, resource, AclOperation::Read)) == expected
        );
    }
}

#[test]
fn prefixed_allow_matches_prefix() {
    let mut img = img();
    img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
        PermissionType::Allow,
        AclOperation::Read,
        "User:alice",
        "*",
        PatternType::Prefixed,
        "team-",
    )));
    let a = alice();
    let h = addr();
    let auth = SimpleAclAuthorizer::new(no_super());
    for (_name, resource, expected) in [
        ("prefix match", "team-foo", AuthorizationResult::Allow),
        ("prefix mismatch", "other", AuthorizationResult::Deny),
    ] {
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, resource, AclOperation::Read)) == expected
        );
    }
}

#[test]
fn deny_wins_over_allow() {
    let mut img = img();
    img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
        PermissionType::Allow,
        AclOperation::Read,
        "User:alice",
        "*",
        PatternType::Literal,
        "foo",
    )));
    img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
        PermissionType::Deny,
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
        auth.authorize(&img, &req(&a, &h, "foo", AclOperation::Read)) == AuthorizationResult::Deny
    );
}

#[test]
fn authorize_topics_batch_returns_per_topic_decisions() {
    let mut img = img();
    img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
        PermissionType::Allow,
        AclOperation::Read,
        "User:alice",
        "*",
        PatternType::Literal,
        "t1",
    )));
    img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(
        PermissionType::Deny,
        AclOperation::Read,
        "User:alice",
        "*",
        PatternType::Literal,
        "t2",
    )));
    let a = alice();
    let h = addr();
    let auth = SimpleAclAuthorizer::new(no_super());
    let map = authorize_topics(&auth, &img, &a, &h, AclOperation::Read, ["t1", "t2", "t3"]);
    let actual = ["t1", "t2", "t3"].map(|topic| map.get(topic).copied());
    // t3: no matching ACL → Deny by default (default-deny; there is
    // no shim that allows in this case).
    assert2::assert!(
        actual
            == [
                Some(AuthorizationResult::Allow),
                Some(AuthorizationResult::Deny),
                Some(AuthorizationResult::Deny),
            ]
    );
}

#[test]
fn multi_super_user_all_bypass() {
    let img = img();
    let h = addr();
    let supers = {
        let mut s = HashSet::new();
        s.insert("admin".to_string());
        s.insert("ops-bot".to_string());
        s
    };
    let admin = Principal {
        name: "admin".into(),
        auth_method: krabka_security::AuthMethod::SaslPlain,
        groups: vec![],
    };
    let ops = Principal {
        name: "ops-bot".into(),
        auth_method: krabka_security::AuthMethod::SaslPlain,
        groups: vec![],
    };
    let alice = alice();
    let auth = SimpleAclAuthorizer::new(supers);
    let actual = [&admin, &ops, &alice]
        .map(|principal| auth.authorize(&img, &req(principal, &h, "foo", AclOperation::Write)));
    // alice is not a super-user and the image has no matching ACL,
    // so default-deny applies (no compat shim).
    assert2::assert!(
        actual
            == [
                AuthorizationResult::Allow,
                AuthorizationResult::Allow,
                AuthorizationResult::Deny,
            ]
    );
}
