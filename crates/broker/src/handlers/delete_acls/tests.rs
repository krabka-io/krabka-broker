//! End-to-end `DeleteAcls` tests that drive [`super::handle`] against a live
//! in-process broker.
//!
//! These cover what only the whole handler shows: the cluster-alter denial
//! that stamps every filter and leaves the ACLs in place, and the matching-ACL
//! echo a successful filter returns next to the entries it actually removed,
//! Kafka's `AclBindingFilter` semantics over a mixed ACL set, and the two ways
//! Kafka refuses a filter with an `UNKNOWN` element.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_metadata::{AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType};
use krabka_protocol::{
    ProtocolError, UnknownTaggedFields,
    owned::{
        delete_acls_request::DeleteAclsFilter,
        delete_acls_response::{DeleteAclsFilterResult, DeleteAclsMatchingAcl, DeleteAclsResponse},
    },
};

use super::handle;
use crate::{
    broker::BrokerHandle,
    codes,
    error::BrokerError,
    handlers::delete_acls::test_support::{
        OPERATION_ANY, OPERATION_READ, PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL, PATTERN_TYPE_MATCH,
        PATTERN_TYPE_PREFIXED, PERMISSION_ALLOW, PERMISSION_ANY, RESOURCE_TYPE_TOPIC, VERSION, acl,
        configured_authorizer, decode_response, filter, request, test_context,
    },
    test_support::{
        DenyAll, peer, principal, start_broker_with_authorizer_no_audit as start_broker,
    },
};

async fn seed_acls(handle: &BrokerHandle, entries: Vec<AclEntry>) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(
            entries
                .into_iter()
                .map(MetadataRecord::V1AccessControlEntry)
                .collect(),
        )
        .await
        .expect("seed ACLs");
}

fn all_acls(handle: &BrokerHandle) -> Vec<AclEntry> {
    handle
        .controller_image_for_test()
        .all_acls()
        .cloned()
        .collect()
}

#[tokio::test]
async fn handle_denies_cluster_alter_for_each_filter() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    seed_acls(
        &broker_handle,
        vec![acl("orders", "User:alice", AclOperation::Read)],
    )
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("alice");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let req = request(vec![
        filter(Some("orders"), Some("User:alice")),
        filter(Some("payments"), Some("User:bob")),
    ]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let denied = DeleteAclsFilterResult {
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        error_message: Some("Request DeleteAcls needs ALTER permission.".into()),
        matching_acls: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let expected = DeleteAclsResponse {
        throttle_time_ms: 0,
        filter_results: vec![denied.clone(), denied],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).len() == 1);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_returns_matching_acl_fields_and_deletes_only_matches() {
    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    seed_acls(
        &broker_handle,
        vec![
            acl("orders", "User:alice", AclOperation::Read),
            acl("payments", "User:bob", AclOperation::Write),
        ],
    )
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let req = request(vec![filter(Some("orders"), Some("User:alice"))]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let expected = DeleteAclsResponse {
        throttle_time_ms: 0,
        filter_results: vec![DeleteAclsFilterResult {
            error_code: codes::NONE,
            error_message: None,
            matching_acls: vec![DeleteAclsMatchingAcl {
                error_code: codes::NONE,
                error_message: None,
                resource_type: RESOURCE_TYPE_TOPIC,
                resource_name: "orders".into(),
                pattern_type: PATTERN_TYPE_LITERAL,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: OPERATION_READ,
                permission_type: PERMISSION_ALLOW,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);

    let remaining = all_acls(&broker_handle);
    assert!(remaining == vec![acl("payments", "User:bob", AclOperation::Write)]);
    broker_handle.shutdown().await;
}

/// Kafka's `KafkaApis.handleDeleteAcls` refuses every filter with
/// `SECURITY_DISABLED` when no authorizer is configured, and reports no
/// matching ACLs. The default `AllowAllAuthorizer` is that state, and the
/// seeded binding must survive the refusal.
#[tokio::test]
async fn handle_answers_security_disabled_for_each_filter_when_no_authorizer_is_configured() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_acls(
        &broker_handle,
        vec![acl("orders", "User:alice", AclOperation::Read)],
    )
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let req = request(vec![
        filter(Some("orders"), Some("User:alice")),
        filter(Some("payments"), Some("User:bob")),
    ]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let disabled = DeleteAclsFilterResult {
        error_code: codes::SECURITY_DISABLED,
        error_message: Some("No Authorizer is configured on the broker".into()),
        matching_acls: Vec::new(),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let expected = DeleteAclsResponse {
        throttle_time_ms: 0,
        filter_results: vec![disabled.clone(), disabled],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(all_acls(&broker_handle) == vec![acl("orders", "User:alice", AclOperation::Read)]);
    broker_handle.shutdown().await;
}

/// An ACL on topic `name` with `pattern_type`, `principal` and
/// `permission_type`.
fn topic_acl(
    name: &str,
    pattern_type: PatternType,
    principal: &str,
    permission_type: PermissionType,
) -> AclEntry {
    AclEntry {
        pattern_type,
        permission_type,
        ..acl(name, principal, AclOperation::Read)
    }
}

/// A filter over topics with ANY operation and permission and a null host.
fn topic_filter(
    pattern_type: i8,
    resource_name: Option<&str>,
    principal: Option<&str>,
) -> DeleteAclsFilter {
    DeleteAclsFilter {
        resource_type_filter: RESOURCE_TYPE_TOPIC,
        resource_name_filter: resource_name.map(Into::into),
        pattern_type_filter: pattern_type,
        principal_filter: principal.map(Into::into),
        host_filter: None,
        operation: OPERATION_ANY,
        permission_type: PERMISSION_ANY,
        ..Default::default()
    }
}

/// Orders ACLs by resource name and principal, the two fields the table
/// below varies, so a set compares as a list.
fn sorted(mut acls: Vec<AclEntry>) -> Vec<AclEntry> {
    acls.sort_by(|a, b| (&a.resource_name, &a.principal).cmp(&(&b.resource_name, &b.principal)));
    acls
}

/// The table from issue #771: Kafka's `AclControlManager.deleteAcls` lists and
/// removes exactly the ACLs `AclBindingFilter.matches` selects. `MATCH` takes
/// every binding that applies to the named resource, and an empty string is a
/// value, not a wildcard.
#[tokio::test]
async fn handle_deletes_exactly_what_kafka_matches() {
    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);

    let literal_foo = topic_acl(
        "foo",
        PatternType::Literal,
        "User:alice",
        PermissionType::Allow,
    );
    let wildcard_deny = topic_acl("*", PatternType::Literal, "User:bob", PermissionType::Deny);
    let prefixed_fo = topic_acl(
        "fo",
        PatternType::Prefixed,
        "User:alice",
        PermissionType::Deny,
    );
    let prefixed_bar = topic_acl(
        "bar",
        PatternType::Prefixed,
        "User:bob",
        PermissionType::Allow,
    );
    let literal_food = topic_acl(
        "food",
        PatternType::Literal,
        "User:alice",
        PermissionType::Allow,
    );
    let seeded = vec![
        literal_foo.clone(),
        wildcard_deny.clone(),
        prefixed_fo.clone(),
        prefixed_bar.clone(),
        literal_food.clone(),
    ];

    let cases: Vec<(&str, DeleteAclsFilter, Vec<AclEntry>)> = vec![
        (
            "empty name and principal delete nothing",
            topic_filter(PATTERN_TYPE_ANY, Some(""), Some("")),
            Vec::new(),
        ),
        (
            "empty name deletes nothing",
            topic_filter(PATTERN_TYPE_ANY, Some(""), None),
            Vec::new(),
        ),
        (
            "empty principal deletes nothing",
            topic_filter(PATTERN_TYPE_ANY, None, Some("")),
            Vec::new(),
        ),
        (
            "null name and principal delete every topic ACL",
            topic_filter(PATTERN_TYPE_ANY, None, None),
            seeded.clone(),
        ),
        (
            "ANY with a name compares it exactly",
            topic_filter(PATTERN_TYPE_ANY, Some("foo"), None),
            vec![literal_foo.clone()],
        ),
        (
            "MATCH takes the literal, the wildcard and covering prefixes",
            topic_filter(PATTERN_TYPE_MATCH, Some("foo"), None),
            vec![
                literal_foo.clone(),
                wildcard_deny.clone(),
                prefixed_fo.clone(),
            ],
        ),
        (
            "MATCH narrows by principal",
            topic_filter(PATTERN_TYPE_MATCH, Some("foo"), Some("User:alice")),
            vec![literal_foo.clone(), prefixed_fo.clone()],
        ),
        (
            "LITERAL with a null name",
            topic_filter(PATTERN_TYPE_LITERAL, None, None),
            vec![
                literal_foo.clone(),
                wildcard_deny.clone(),
                literal_food.clone(),
            ],
        ),
        (
            "PREFIXED with a name compares it exactly",
            topic_filter(PATTERN_TYPE_PREFIXED, Some("foo"), None),
            Vec::new(),
        ),
    ];

    for (name, f, want_deleted) in cases {
        seed_acls(&broker_handle, seeded.clone()).await;

        let resp = handle(&broker, request(vec![f]), &ctx, VERSION)
            .await
            .expect("handle");
        let mut resp = decode_response(&resp);
        resp.filter_results[0].matching_acls.sort_by(|a, b| {
            (&a.resource_name, &a.principal).cmp(&(&b.resource_name, &b.principal))
        });

        let want_deleted = sorted(want_deleted);
        let expected = DeleteAclsResponse {
            throttle_time_ms: 0,
            filter_results: vec![DeleteAclsFilterResult {
                error_code: codes::NONE,
                error_message: None,
                matching_acls: want_deleted
                    .iter()
                    .map(super::response::matching_acl_result)
                    .collect(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        check!(resp == expected, "{name}");

        let want_left = sorted(
            seeded
                .iter()
                .filter(|e| !want_deleted.contains(e))
                .cloned()
                .collect(),
        );
        check!(sorted(all_acls(&broker_handle)) == want_left, "{name}");
    }
    broker_handle.shutdown().await;
}

/// Kafka's `AclControlManager.deleteAcls` matches every filter against the
/// same ACL set, so both filters list the shared ACL, and it is removed.
#[tokio::test]
async fn handle_lists_an_acl_under_every_filter_that_matches_it() {
    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    let shared = acl("orders", "User:alice", AclOperation::Read);
    let other = acl("payments", "User:bob", AclOperation::Read);
    seed_acls(&broker_handle, vec![shared.clone(), other.clone()]).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let req = request(vec![
        topic_filter(PATTERN_TYPE_ANY, Some("orders"), None),
        topic_filter(PATTERN_TYPE_MATCH, Some("orders"), Some("User:alice")),
    ]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let row = DeleteAclsFilterResult {
        error_code: codes::NONE,
        error_message: None,
        matching_acls: vec![super::response::matching_acl_result(&shared)],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    let expected = DeleteAclsResponse {
        throttle_time_ms: 0,
        filter_results: vec![row.clone(), row],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(all_acls(&broker_handle) == vec![other]);
    broker_handle.shutdown().await;
}

/// A byte Kafka does not define parses as `UNKNOWN`, and
/// `AclControlManager.validateFilter` refuses only that filter. The KIP-373
/// values are valid and match nothing krabka can store. The other filters
/// still run.
#[tokio::test]
async fn handle_refuses_a_filter_with_an_undefined_byte_and_runs_the_rest() {
    type Edit = fn(&mut DeleteAclsFilter);

    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    let doomed = acl("orders", "User:alice", AclOperation::Read);
    seed_acls(&broker_handle, vec![doomed.clone()]).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);

    let cases: [(Edit, Option<&str>); 7] = [
        (
            |f| f.resource_type_filter = 99,
            Some("Unknown patternFilter."),
        ),
        (
            |f| f.pattern_type_filter = 99,
            Some("Unknown patternFilter."),
        ),
        (|f| f.operation = 99, Some("Unknown entryFilter.")),
        (|f| f.permission_type = 99, Some("Unknown entryFilter.")),
        (|f| f.resource_type_filter = 7, None),
        (|f| f.operation = 13, None),
        (|f| f.operation = 14, None),
    ];
    let mut filters: Vec<DeleteAclsFilter> = cases
        .iter()
        .map(|(edit, _)| {
            let mut f = topic_filter(PATTERN_TYPE_ANY, None, None);
            edit(&mut f);
            f
        })
        .collect();
    filters.push(topic_filter(PATTERN_TYPE_ANY, Some("orders"), None));

    let resp = handle(&broker, request(filters), &ctx, VERSION)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    let mut filter_results: Vec<DeleteAclsFilterResult> = cases
        .iter()
        .map(|(_, message)| DeleteAclsFilterResult {
            error_code: if message.is_some() {
                codes::INVALID_REQUEST
            } else {
                codes::NONE
            },
            error_message: message.map(Into::into),
            matching_acls: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect();
    filter_results.push(DeleteAclsFilterResult {
        error_code: codes::NONE,
        error_message: None,
        matching_acls: vec![super::response::matching_acl_result(&doomed)],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    });
    let expected = DeleteAclsResponse {
        throttle_time_ms: 0,
        filter_results,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).is_empty());
    broker_handle.shutdown().await;
}

/// Kafka refuses an `UNKNOWN` (0) element in any filter while the request
/// parses, before the cluster-alter check, and closes the connection. No
/// filter runs.
#[tokio::test]
async fn handle_closes_the_connection_on_an_unknown_element() {
    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    seed_acls(
        &broker_handle,
        vec![acl("orders", "User:alice", AclOperation::Read)],
    )
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let mut unknown = filter(Some("payments"), None);
    unknown.permission_type = 0;
    let req = request(vec![filter(Some("orders"), Some("User:alice")), unknown]);

    let result = handle(&broker, req, &ctx, VERSION).await;

    assert!(
        let Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "Filters contain UNKNOWN elements"
        ))) = result
    );
    assert!(all_acls(&broker_handle) == vec![acl("orders", "User:alice", AclOperation::Read)]);
    broker_handle.shutdown().await;
}
