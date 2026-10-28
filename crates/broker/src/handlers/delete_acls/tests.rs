//! End-to-end `DeleteAcls` tests that drive [`super::handle`] against a live
//! in-process broker.
//!
//! These cover what only the whole handler shows: the cluster-alter denial
//! that stamps every filter and leaves the ACLs in place, and the matching-ACL
//! echo a successful filter returns next to the entries it actually removed,
//! Kafka's `AclBindingFilter` semantics over a mixed ACL set, and the two ways
//! Kafka refuses a filter with an `UNKNOWN` element.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::{Doomed, filter::build_filter, handle, match_filter},
    crate::{
        broker::BrokerHandle,
        codes,
        error::BrokerError,
        handlers::{
            acl_test_support as acl_fixtures,
            acl_test_support::{all_acls, seed_acls},
            delete_acls::test_support::{
                NamedFilter, OPERATION_ANY, PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL,
                PATTERN_TYPE_MATCH, PATTERN_TYPE_PREFIXED, PERMISSION_ANY, RESOURCE_TYPE_TOPIC,
                VERSION, acl, configured_authorizer, filter, named_filters, request, test_context,
            },
        },
        test_support::{DenyAll, start_broker_with_authorizer_no_audit as start_broker},
    },
    assert2::{assert, check},
    krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType},
    krabka_protocol::{
        ProtocolError,
        owned::{
            delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest},
            delete_acls_response::{DeleteAclsFilterResult, DeleteAclsResponse},
        },
    },
    std::sync::Arc,
};

fn orders_and_payments_request() -> DeleteAclsRequest {
    named_filters(&[
        NamedFilter {
            name: "orders",
            principal: "User:alice",
        },
        NamedFilter {
            name: "payments",
            principal: "User:bob",
        },
    ])
}

#[tokio::test]
async fn handle_denies_cluster_alter_for_each_filter() {
    krabka_macros::broker_test_setup! {
    bind(broker_handle, _dir, broker, ctx);
    boot [acl_fixtures::start_seeded_acls().start(start_broker(Arc::new(DenyAll))).entries(|| vec![acl_fixtures::alice_orders_acl()]).call()];
    user ["alice"];
    };
    let req = orders_and_payments_request();

    let resp = handle(&broker, req, VERSION, &ctx).await.expect("handle");

    let denied = krabka_macros::tagged_wire!(DeleteAclsFilterResult {
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        error_message: Some("Request DeleteAcls needs ALTER permission.".into()),
        matching_acls: Vec::new(),
    });
    let expected = krabka_macros::unthrottled_wire!(DeleteAclsResponse {
        filter_results: vec![denied.clone(), denied],
    });
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).len() == 1);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_returns_matching_acl_fields_and_deletes_only_matches() {
    krabka_macros::broker_test_setup! {
    bind(broker_handle, _dir, broker, ctx);
    boot [acl_fixtures::start_orders_payments_acls()];
    user ["admin"];
    };
    let req = request(vec![filter(Some("orders"), Some("User:alice"))]);

    let resp = handle(&broker, req, VERSION, &ctx).await.expect("handle");

    let expected = krabka_macros::unthrottled_wire!(DeleteAclsResponse {
        filter_results: vec![krabka_macros::tagged_wire!(DeleteAclsFilterResult {
            error_code: codes::NONE,
            error_message: None,
            matching_acls: vec![acl_fixtures::expected_deleted_orders_acl()],
        })],
    });
    assert!(resp == expected);

    let remaining = all_acls(&broker_handle);
    assert!(
        remaining
            == vec![
                acl()
                    .resource_name("payments")
                    .principal("User:bob")
                    .operation(AclOperation::Write)
                    .call()
            ]
    );
    broker_handle.shutdown().await;
}

/// Kafka's `KafkaApis.handleDeleteAcls` refuses every filter with
/// `SECURITY_DISABLED` when no authorizer is configured, and reports no
/// matching ACLs. The default `AllowAllAuthorizer` is that state, and the
/// seeded binding must survive the refusal.
#[tokio::test]
async fn handle_answers_security_disabled_for_each_filter_when_no_authorizer_is_configured() {
    krabka_macros::broker_test_setup! {
    bind(broker_handle, _dir, broker, ctx);
    boot [acl_fixtures::start_security_disabled_acls()];
    user ["admin"];
    };
    let req = orders_and_payments_request();

    let resp = handle(&broker, req, VERSION, &ctx).await.expect("handle");

    let disabled = krabka_macros::tagged_wire!(DeleteAclsFilterResult {
        error_code: codes::SECURITY_DISABLED,
        error_message: Some("No Authorizer is configured.".into()),
        matching_acls: Vec::new(),
    });
    let expected = krabka_macros::unthrottled_wire!(DeleteAclsResponse {
        filter_results: vec![disabled.clone(), disabled],
    });
    assert!(resp == expected);
    assert!(all_acls(&broker_handle) == vec![acl_fixtures::alice_orders_acl()]);
    broker_handle.shutdown().await;
}

/// An ACL on topic `name` with `pattern_type`, `principal` and
/// `permission_type`.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct TopicAclSetup<'a> {
    #[default("orders")]
    name: &'a str,
    #[default(PatternType::Literal)]
    pattern_type: PatternType,
    #[default("User:alice")]
    principal: &'a str,
    #[default(PermissionType::Allow)]
    permission_type: PermissionType,
}

#[bon::builder]
fn topic_acl(
    #[builder(default = "orders")] name: &'_ str,
    #[builder(default = PatternType :: Literal)] pattern_type: PatternType,
    #[builder(default = "User:alice")] principal: &'_ str,
    #[builder(default = PermissionType :: Allow)] permission_type: PermissionType,
) -> AclEntry {
    let setup = TopicAclSetup {
        name,
        pattern_type,
        principal,
        permission_type,
    };
    let TopicAclSetup {
        name,
        pattern_type,
        principal,
        permission_type,
    } = setup;
    AclEntry {
        pattern_type,
        permission_type,
        ..acl().resource_name(name).principal(principal).call()
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

#[derive(Default)]
struct AclDeletionScenario {
    started: Option<HandlerStartedBroker>,
    seeded: Vec<AclEntry>,
    response: Option<DeleteAclsResponse>,
}
#[rstest::fixture]
fn acl_deletion() -> AclDeletionScenario {
    let started = None;
    AclDeletionScenario {
        started,
        ..Default::default()
    }
}
fn topic_read_acl_rows(datatable: Vec<Vec<String>>) -> Vec<AclEntry> {
    datatable
        .into_iter()
        .skip(1)
        .map(|cells| {
            topic_acl()
                .name(&cells[0])
                .pattern_type(match cells[1].as_str() {
                    "literal" => PatternType::Literal,
                    "prefixed" => PatternType::Prefixed,
                    other => panic!("unknown stored pattern {other}"),
                })
                .principal(&cells[2])
                .permission_type(match cells[3].as_str() {
                    "allow" => PermissionType::Allow,
                    "deny" => PermissionType::Deny,
                    other => panic!("unknown permission {other}"),
                })
                .call()
        })
        .collect()
}
#[rstest_bdd_macros::given("the controller stores these Topic Read ACLs for host wildcard")]
async fn stored_deletion_acls(acl_deletion: &mut AclDeletionScenario, datatable: Vec<Vec<String>>) {
    let started = start_broker(configured_authorizer()).await;
    acl_deletion.seeded = topic_read_acl_rows(datatable);
    seed_acls(&started.broker, acl_deletion.seeded.clone()).await;
    acl_deletion.started = Some(started);
}
#[rstest_bdd_macros::when(
    "admin deletes Topic ACLs with pattern {pattern}, name \"{name}\" and principal \"{principal}\""
)]
async fn delete_filtered_acls(
    acl_deletion: &mut AclDeletionScenario,
    pattern: String,
    name: String,
    principal: String,
) {
    let pattern = match pattern.as_str() {
        "any" => PATTERN_TYPE_ANY,
        "match" => PATTERN_TYPE_MATCH,
        "literal" => PATTERN_TYPE_LITERAL,
        "prefixed" => PATTERN_TYPE_PREFIXED,
        other => panic!("unknown filter pattern {other}"),
    };
    let filter = topic_filter(
        pattern,
        match name.as_str() {
            "null" => None,
            "<empty>" => Some(""),
            value => Some(value),
        },
        match principal.as_str() {
            "null" => None,
            "<empty>" => Some(""),
            value => Some(value),
        },
    );
    let broker = acl_deletion
        .started
        .as_ref()
        .expect("stored ACL broker")
        .broker
        .broker_arc_for_test();
    krabka_macros::request_test_identity! { bind(principal, peer, ctx); user ["admin"]; };
    acl_deletion.response = Some(
        handle(&broker, request(vec![filter]), VERSION, &ctx)
            .await
            .expect("handle deletion"),
    );
}
#[rstest_bdd_macros::then(
    "the complete DeleteAcls response lists exactly these ACLs and removes only these entries"
)]
async fn deleted_acl_observations(
    acl_deletion: &mut AclDeletionScenario,
    datatable: Vec<Vec<String>>,
) {
    let deleted = sorted(topic_read_acl_rows(datatable));
    let expected = DeleteAclsResponse {
        filter_results: vec![DeleteAclsFilterResult {
            error_code: codes::NONE,
            error_message: None,
            matching_acls: deleted
                .iter()
                .map(super::response::matching_acl_result)
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let response = acl_deletion.response.as_mut().expect("deletion response");
    response.filter_results[0]
        .matching_acls
        .sort_by(|a, b| (&a.resource_name, &a.principal).cmp(&(&b.resource_name, &b.principal)));
    check!(*response == expected);
    let remaining = sorted(
        acl_deletion
            .seeded
            .iter()
            .filter(|entry| !deleted.contains(entry))
            .cloned()
            .collect(),
    );
    let started = acl_deletion.started.take().expect("stored ACL broker");
    check!(sorted(all_acls(&started.broker)) == remaining);
    started.broker.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delete_acl_matching.feature",
    name = "Empty name and principal match nothing"
)]
#[tokio::test]
async fn delete_acl_matching_case_0(acl_deletion: AclDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delete_acl_matching.feature",
    name = "Empty name matches nothing"
)]
#[tokio::test]
async fn delete_acl_matching_case_1(acl_deletion: AclDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delete_acl_matching.feature",
    name = "Empty principal matches nothing"
)]
#[tokio::test]
async fn delete_acl_matching_case_2(acl_deletion: AclDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delete_acl_matching.feature",
    name = "Null name and principal match every Topic ACL"
)]
#[tokio::test]
async fn delete_acl_matching_case_3(acl_deletion: AclDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delete_acl_matching.feature",
    name = "ANY compares a supplied name exactly"
)]
#[tokio::test]
async fn delete_acl_matching_case_4(acl_deletion: AclDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delete_acl_matching.feature",
    name = "MATCH includes literal wildcard and covering prefixes"
)]
#[tokio::test]
async fn delete_acl_matching_case_5(acl_deletion: AclDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delete_acl_matching.feature",
    name = "MATCH still filters by principal"
)]
#[tokio::test]
async fn delete_acl_matching_case_6(acl_deletion: AclDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delete_acl_matching.feature",
    name = "LITERAL with null name selects all literal ACLs"
)]
#[tokio::test]
async fn delete_acl_matching_case_7(acl_deletion: AclDeletionScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delete_acl_matching.feature",
    name = "PREFIXED compares the stored prefix exactly"
)]
#[tokio::test]
async fn delete_acl_matching_case_8(acl_deletion: AclDeletionScenario) {}

/// Kafka's `AclControlManager.deleteAcls` matches every filter against the
/// same ACL set, so both filters list the shared ACL, and it is removed.
#[tokio::test]
async fn handle_lists_an_acl_under_every_filter_that_matches_it() {
    let HandlerStartedBroker {
        broker: broker_handle,
        dir: _dir,
    } = start_broker(configured_authorizer()).await;
    let shared = acl().call();
    let other = acl().resource_name("payments").principal("User:bob").call();
    seed_acls(&broker_handle, vec![shared.clone(), other.clone()]).await;
    let broker = broker_handle.broker_arc_for_test();
    krabka_macros::request_test_identity! {
        bind(ctx);
        user ["admin"];
    };
    let req = request(vec![
        topic_filter(PATTERN_TYPE_ANY, Some("orders"), None),
        topic_filter(PATTERN_TYPE_MATCH, Some("orders"), Some("User:alice")),
    ]);

    let resp = handle(&broker, req, VERSION, &ctx).await.expect("handle");

    let row = krabka_macros::tagged_wire!(DeleteAclsFilterResult {
        error_code: codes::NONE,
        error_message: None,
        matching_acls: vec![super::response::matching_acl_result(&shared)],
    });
    let expected = krabka_macros::unthrottled_wire!(DeleteAclsResponse {
        filter_results: vec![row.clone(), row],
    });
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
    /// An edit that gives a filter an undefined byte, and the message Kafka
    /// answers with, if the byte is refused.
    struct UndefinedByteCase {
        edit: Edit,
        message: Option<&'static str>,
    }

    type Edit = fn(&mut DeleteAclsFilter);

    let HandlerStartedBroker {
        broker: broker_handle,
        dir: _dir,
    } = start_broker(configured_authorizer()).await;
    let doomed = acl().call();
    seed_acls(&broker_handle, vec![doomed.clone()]).await;
    let broker = broker_handle.broker_arc_for_test();
    krabka_macros::request_test_identity! {
        bind(ctx);
        user ["admin"];
    };

    let cases: [UndefinedByteCase; 7] = [
        UndefinedByteCase {
            edit: |f| f.resource_type_filter = 99,
            message: Some("Unknown patternFilter."),
        },
        UndefinedByteCase {
            edit: |f| f.pattern_type_filter = 99,
            message: Some("Unknown patternFilter."),
        },
        UndefinedByteCase {
            edit: |f| f.operation = 99,
            message: Some("Unknown entryFilter."),
        },
        UndefinedByteCase {
            edit: |f| f.permission_type = 99,
            message: Some("Unknown entryFilter."),
        },
        UndefinedByteCase {
            edit: |f| f.resource_type_filter = 7,
            message: None,
        },
        UndefinedByteCase {
            edit: |f| f.operation = 13,
            message: None,
        },
        UndefinedByteCase {
            edit: |f| f.operation = 14,
            message: None,
        },
    ];
    let mut filters: Vec<DeleteAclsFilter> = cases
        .iter()
        .map(|UndefinedByteCase { edit, .. }| {
            let mut f = topic_filter(PATTERN_TYPE_ANY, None, None);
            edit(&mut f);
            f
        })
        .collect();
    filters.push(topic_filter(PATTERN_TYPE_ANY, Some("orders"), None));

    let resp = handle(&broker, request(filters), VERSION, &ctx)
        .await
        .expect("handle");

    let mut filter_results: Vec<DeleteAclsFilterResult> = cases
        .iter()
        .map(|UndefinedByteCase { message, .. }| {
            krabka_macros::tagged_wire!(DeleteAclsFilterResult {
                error_code: if message.is_some() {
                    codes::INVALID_REQUEST
                } else {
                    codes::NONE
                },
                error_message: message.map(Into::into),
                matching_acls: Vec::new(),
            })
        })
        .collect();
    filter_results.push(krabka_macros::tagged_wire!(DeleteAclsFilterResult {
        error_code: codes::NONE,
        error_message: None,
        matching_acls: vec![super::response::matching_acl_result(&doomed)],
    }));
    let expected = krabka_macros::unthrottled_wire!(DeleteAclsResponse { filter_results });
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).is_empty());
    broker_handle.shutdown().await;
}

/// Kafka refuses an `UNKNOWN` (0) element in any filter while the request
/// parses, before the cluster-alter check, and closes the connection. No
/// filter runs.
#[tokio::test]
async fn handle_closes_the_connection_on_an_unknown_element() {
    krabka_macros::broker_test_setup! {
    bind(broker_handle, _dir, broker, ctx);
    boot [acl_fixtures::start_orders_acl()];
    user ["admin"];
    };
    let mut unknown = filter(Some("payments"), None);
    unknown.permission_type = 0;
    let req = request(vec![filter(Some("orders"), Some("User:alice")), unknown]);

    let result = handle(&broker, req, VERSION, &ctx).await;

    assert!(
        let Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "Filters contain UNKNOWN elements"
        ))) = result
    );
    assert!(all_acls(&broker_handle) == vec![acl_fixtures::alice_orders_acl()]);
    broker_handle.shutdown().await;
}

/// Seeds `count` distinct topic ACLs in one submit, as `CreateAcls` writes
/// them. Several submits against a growing ACL set are far slower.
async fn seed_many_acls(handle: &BrokerHandle, count: usize) {
    seed_acls(
        handle,
        (0..count)
            .map(|n| acl().resource_name(&format!("topic-{n}")).call())
            .collect(),
    )
    .await;
}

/// Kafka's `AclControlManager.deleteAcls` caps a request at 10,000 removals.
/// The 10,001st match fails the whole request with `INVALID_REQUEST` on every
/// filter and deletes nothing. The check runs before a match is added, so a
/// second filter that matches the 10,000 ACLs already collected fails too.
///
/// The boundary itself is `match_filter_bounds_the_removals_of_a_request`: a
/// request that removes exactly 10,000 ACLs is not run here, because the image
/// removes an ACL by scanning the whole set and 10,000 of them take minutes in
/// a debug build.
#[tokio::test]
async fn handle_bounds_a_request_to_ten_thousand_removals() {
    let match_all = || filter(None, None);
    // (acls seeded, request filters)
    let cases = [
        (10_001, vec![match_all()]),
        (10_000, vec![match_all(), match_all()]),
    ];
    for (seeded, filters) in cases {
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker(configured_authorizer()).await;
        seed_many_acls(&broker_handle, seeded).await;
        let broker = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["admin"];
        };
        let filter_count = filters.len();

        let resp = handle(&broker, request(filters), VERSION, &ctx)
            .await
            .expect("handle");

        let bound = krabka_macros::tagged_wire!(DeleteAclsFilterResult {
            error_code: codes::INVALID_REQUEST,
            error_message: Some(
                "Cannot remove more than 10000 acls in a single delete operation.".into(),
            ),
            matching_acls: Vec::new(),
        });
        let expected = krabka_macros::unthrottled_wire!(DeleteAclsResponse {
            filter_results: vec![bound; filter_count],
        });
        check!(resp == expected, "{seeded} acls, {filter_count} filters");
        check!(
            all_acls(&broker_handle).len() == seeded,
            "{seeded} acls, {filter_count} filters"
        );
        broker_handle.shutdown().await;
    }
}

/// The bound of [`super::match_filter`], at a size a test can afford: a
/// request may remove exactly `bound` ACLs, and the next match fails it.
#[test]
fn match_filter_bounds_the_removals_of_a_request() {
    let image_acls: Vec<AclEntry> = (0..5)
        .map(|n| acl().resource_name(&format!("topic-{n}")).call())
        .collect();
    let build = |name: Option<&str>| build_filter(&filter(name, None)).expect("filter");
    let every_acl = build(None);
    let [topic_0, topic_1, topic_2] =
        ["topic-0", "topic-1", "topic-2"].map(|name| build(Some(name)));
    let bound = 3;
    // (label, filters in order, how many ACLs each matched; `None` is refused)
    let cases = [
        ("under the bound", vec![&topic_0, &topic_1], vec![
            Some(1),
            Some(1),
        ]),
        // A repeat adds no distinct removal, so it is never past the bound.
        (
            "repeats",
            vec![&topic_0, &topic_0, &topic_1, &topic_1],
            vec![Some(1); 4],
        ),
        (
            "exactly the bound",
            vec![&topic_0, &topic_1, &topic_2],
            vec![Some(1); 3],
        ),
        // The fourth ACL is the first one past the bound.
        ("one past the bound", vec![&every_acl], vec![None]),
        (
            "a later filter past the bound",
            vec![&topic_0, &topic_1, &every_acl],
            vec![Some(1), Some(1), None],
        ),
    ];
    for (label, filters, expected) in cases {
        let mut doomed = Doomed::default();
        let mut got = Vec::new();
        for filter in filters {
            let matched = match_filter(image_acls.iter(), filter, &mut doomed, bound);
            got.push(matched.as_ref().map(Vec::len));
            if matched.is_none() {
                break;
            }
        }
        check!(got == expected, "{label}");
    }
}
