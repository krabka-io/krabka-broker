//! End-to-end `CreateAcls` tests that drive [`super::handle`] against a live
//! in-process broker.
//!
//! These cover what only the whole handler shows: the cluster-alter denial that
//! stamps every creation, the positional interleaving of accepted and rejected
//! creations, and the absence of a principal or resource-name length limit.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_metadata::{
    AclEntry, AclOperation, FeatureLevelRecord, MetadataRecord, PatternType, PermissionType,
    ResourceType,
};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::create_acls_response::{AclCreationResult, CreateAclsResponse},
};

use super::handle;
use crate::{
    codes,
    handlers::create_acls::test_support::{
        OPERATION_READ, OPERATION_WRITE, VERSION, all_acls, configured_authorizer, creation,
        decode_response, request, test_context,
    },
    test_support::{
        DenyAll, peer, principal, start_broker_with_authorizer_no_audit as start_broker,
    },
};

/// The row Kafka's `AclApis.handleCreateAcls` sends for a committed
/// creation: a bare `AclCreationResult`, whose generated `ErrorMessage`
/// default is the empty string.
fn committed() -> AclCreationResult {
    AclCreationResult {
        error_code: codes::NONE,
        error_message: Some(String::new()),
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    }
}

/// Kafka puts no length limit on an ACL's resource name or principal, so a
/// binding far past any fixed ceiling is accepted and stored as sent.
#[tokio::test]
async fn handle_stores_long_resource_names_and_principals() {
    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let long_name = "r".repeat(4096);
    let long_principal = format!("User:{}", "a".repeat(4096));
    let req = request(vec![
        creation(&long_name, "User:a", OPERATION_READ),
        creation("r", &long_principal, OPERATION_READ),
    ]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let expected = CreateAclsResponse {
        throttle_time_ms: 0,
        results: vec![committed(), committed()],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    let stored = |resource_name: &str, principal: &str| AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: resource_name.into(),
        pattern_type: PatternType::Literal,
        principal: principal.into(),
        host: "*".into(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    };
    let mut acls = all_acls(&broker_handle);
    acls.sort_by_key(|acl| std::cmp::Reverse(acl.resource_name.len()));
    assert!(acls == vec![stored(&long_name, "User:a"), stored("r", &long_principal)]);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_denies_cluster_alter_for_each_creation() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("alice");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let req = request(vec![
        creation("topic-a", "User:bob", OPERATION_READ),
        creation("topic-b", "User:carol", OPERATION_WRITE),
    ]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let denied = AclCreationResult {
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        error_message: Some("create-acls denied".into()),
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    let expected = CreateAclsResponse {
        throttle_time_ms: 0,
        results: vec![denied.clone(), denied],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).is_empty());
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_submits_valid_creations_and_reports_invalid_creations_in_order() {
    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let mut invalid = creation("", "User:bob", OPERATION_WRITE);
    invalid.resource_name.clear();
    let req = request(vec![
        creation("topic-a", "User:alice", OPERATION_READ),
        invalid,
    ]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let expected = CreateAclsResponse {
        throttle_time_ms: 0,
        results: vec![
            committed(),
            AclCreationResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("Invalid empty resource name".into()),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);

    let acls = all_acls(&broker_handle);
    let expected_acls = vec![AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "topic-a".into(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".into(),
        host: "*".into(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    }];
    assert!(acls == expected_acls);
    broker_handle.shutdown().await;
}

/// Kafka's `KafkaApis.handleCreateAcls` refuses every creation with
/// `SECURITY_DISABLED` when no authorizer is configured, rather than storing
/// bindings that nothing will consult. The default `AllowAllAuthorizer` is
/// that state.
#[tokio::test]
async fn handle_answers_security_disabled_for_each_creation_when_no_authorizer_is_configured() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let req = request(vec![
        creation("topic-a", "User:alice", OPERATION_READ),
        creation("topic-b", "User:bob", OPERATION_WRITE),
    ]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let disabled = AclCreationResult {
        error_code: codes::SECURITY_DISABLED,
        error_message: Some("No Authorizer is configured.".into()),
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    let expected = CreateAclsResponse {
        throttle_time_ms: 0,
        results: vec![disabled.clone(), disabled],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).is_empty());
    broker_handle.shutdown().await;
}

/// A broker with `unstable.feature.versions.enable`, the mode in which
/// `CreateAcls` applies Kafka trunk's host validation.
async fn start_trunk_broker() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = configured_authorizer();
        cfg.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
    })
    .await
}

/// Kafka 4.3.1 has no host check, so by default `CreateAcls` stores a host
/// containing `/` and an empty host as the text they arrive as, where trunk's
/// `validateHostPattern` (KIP-1276) would refuse both.
#[tokio::test]
async fn handle_stores_any_host_by_default() {
    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let hosts = ["10.0.0.0/8", "not/a/cidr", ""];
    let creations = hosts
        .iter()
        .map(|host| {
            let mut c = creation("topic-a", "User:alice", OPERATION_READ);
            c.host = (*host).into();
            c
        })
        .collect();

    let resp = handle(&broker, request(creations), &ctx, VERSION)
        .await
        .expect("handle");
    let resp = decode_response(&resp);

    let expected = CreateAclsResponse {
        throttle_time_ms: 0,
        results: vec![committed(), committed(), committed()],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    let mut stored: Vec<String> = all_acls(&broker_handle)
        .into_iter()
        .map(|acl| acl.host)
        .collect();
    stored.sort();
    assert!(stored == vec!["", "10.0.0.0/8", "not/a/cidr"]);
    broker_handle.shutdown().await;
}

/// #652 / KIP-1276: a CIDR host is accepted, and stored as the literal text
/// the operator typed, once `metadata.version` reaches
/// [`crate::features::CIDR_ACL_HOST_MIN_LEVEL`]. The test seeds that level
/// with a raw controller submit, the same way the
/// `alter_user_scram_credentials` and `create_delegation_token` gate tests
/// seed their own metadata-version floors.
#[tokio::test]
async fn handle_accepts_cidr_host_at_the_cidr_metadata_version() {
    let (broker_handle, _dir) = start_trunk_broker().await;
    let broker = broker_handle.broker_arc_for_test();
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crate::features::METADATA_VERSION.to_string(),
            level: crate::features::CIDR_ACL_HOST_MIN_LEVEL,
        })])
        .await
        .expect("seed cidr-supporting metadata.version");
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let mut cidr_creation = creation("topic-a", "User:alice", OPERATION_READ);
    cidr_creation.host = "10.0.0.0/8".into();
    let req = request(vec![cidr_creation]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let expected = CreateAclsResponse {
        throttle_time_ms: 0,
        results: vec![committed()],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    let acls = all_acls(&broker_handle);
    let expected_acls = vec![AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "topic-a".into(),
        pattern_type: PatternType::Literal,
        principal: "User:alice".into(),
        host: "10.0.0.0/8".into(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    }];
    assert!(acls == expected_acls);
    broker_handle.shutdown().await;
}

/// The same CIDR host is refused below `CIDR_ACL_HOST_MIN_LEVEL`, with
/// Kafka's exact `UNSUPPORTED_VERSION` message, and nothing is stored.
#[tokio::test]
async fn handle_rejects_cidr_host_below_the_cidr_metadata_version() {
    let (broker_handle, _dir) = start_trunk_broker().await;
    let broker = broker_handle.broker_arc_for_test();
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crate::features::METADATA_VERSION.to_string(),
            level: crate::features::CIDR_ACL_HOST_MIN_LEVEL - 1,
        })])
        .await
        .expect("seed pre-cidr metadata.version");
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let mut cidr_creation = creation("topic-a", "User:alice", OPERATION_READ);
    cidr_creation.host = "10.0.0.0/8".into();
    let req = request(vec![cidr_creation]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let expected = CreateAclsResponse {
        throttle_time_ms: 0,
        results: vec![AclCreationResult {
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: Some(
                "CIDR-based ACL host patterns require metadata version 4.4-IV1 or higher.".into(),
            ),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).is_empty());
    broker_handle.shutdown().await;
}

/// #772: an `ANY` or `MATCH` element makes Kafka's binding construction throw
/// for the whole request, so every creation -- the valid one too -- answers
/// `UNKNOWN_SERVER_ERROR` with no message, and nothing is stored.
#[tokio::test]
async fn handle_fails_every_creation_when_one_carries_a_filter_only_value() {
    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let mut any_operation = creation("topic-b", "User:bob", OPERATION_READ);
    any_operation.operation = 1;
    let req = request(vec![
        creation("topic-a", "User:alice", OPERATION_READ),
        any_operation,
    ]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let failed = AclCreationResult {
        error_code: codes::UNKNOWN_SERVER_ERROR,
        error_message: None,
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    let expected = CreateAclsResponse {
        throttle_time_ms: 0,
        results: vec![failed.clone(), failed],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).is_empty());
    broker_handle.shutdown().await;
}

/// #772: a wire `UNKNOWN` element fails Kafka's request parse, which closes
/// the connection with no response. The handler's error return is what
/// closes it here, and it comes before authorization, as a parse does.
#[tokio::test]
async fn handle_errors_so_the_connection_closes_on_an_unknown_element() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("alice");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let mut unknown_permission = creation("topic-a", "User:bob", OPERATION_READ);
    unknown_permission.permission_type = 0;
    let req = request(vec![unknown_permission]);

    let result = handle(&broker, req, &ctx, VERSION).await;

    assert!(let Err(crate::error::BrokerError::Protocol(_)) = result);
    assert!(all_acls(&broker_handle).is_empty());
    broker_handle.shutdown().await;
}

/// #772: a CLUSTER binding under any name but `kafka-cluster` is refused
/// with Kafka's message, and a non-`User` principal type is stored.
#[tokio::test]
async fn handle_pins_the_cluster_name_and_accepts_other_principal_types() {
    let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let mut wrong_cluster = creation("my-cluster", "User:alice", OPERATION_READ);
    wrong_cluster.resource_type = 4;
    let req = request(vec![
        wrong_cluster,
        creation("topic-a", "Group:ops", OPERATION_READ),
    ]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let expected = CreateAclsResponse {
        throttle_time_ms: 0,
        results: vec![
            AclCreationResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some(
                    "The only valid name for the CLUSTER resource is kafka-cluster".into(),
                ),
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
            committed(),
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    let expected_acls = vec![AclEntry {
        resource_type: ResourceType::Topic,
        resource_name: "topic-a".into(),
        pattern_type: PatternType::Literal,
        principal: "Group:ops".into(),
        host: "*".into(),
        operation: AclOperation::Read,
        permission_type: PermissionType::Allow,
    }];
    assert!(all_acls(&broker_handle) == expected_acls);
    broker_handle.shutdown().await;
}

/// Kafka's `AclControlManager.createAcls` collects the records of the new
/// ACLs into a list bounded at 10,000, and does not catch the overflow: the
/// controller answers every binding with `POLICY_VIOLATION`, the invalid ones
/// included, and stores nothing. Only valid, new, distinct ACLs count.
#[tokio::test]
async fn handle_bounds_a_request_to_ten_thousand_new_acls() {
    let distinct = |count: usize| {
        (0..count)
            .map(|n| creation(&format!("topic-{n}"), "User:alice", OPERATION_READ))
            .collect::<Vec<_>>()
    };
    let violation = AclCreationResult {
        error_code: codes::POLICY_VIOLATION,
        error_message: Some("Unable to perform excessively large batch operation.".into()),
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    let empty_name = AclCreationResult {
        error_code: codes::INVALID_REQUEST,
        error_message: Some("Invalid empty resource name".into()),
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    // 10,001 identical bindings are one new ACL.
    let repeated = vec![creation("topic-a", "User:alice", OPERATION_READ); 10_001];
    let mut with_invalid = distinct(10_000);
    with_invalid.push(creation("", "User:alice", OPERATION_READ));
    let mut over_with_invalid = distinct(10_001);
    over_with_invalid.push(creation("", "User:alice", OPERATION_READ));
    // (label, creations, results, ACLs stored)
    let cases = [
        (
            "10,000 distinct",
            distinct(10_000),
            vec![committed(); 10_000],
            10_000,
        ),
        (
            "10,001 distinct",
            distinct(10_001),
            vec![violation.clone(); 10_001],
            0,
        ),
        ("10,001 identical", repeated, vec![committed(); 10_001], 1),
        (
            "10,000 distinct and an invalid binding",
            with_invalid,
            [vec![committed(); 10_000], vec![empty_name]].concat(),
            10_000,
        ),
        (
            "10,001 distinct and an invalid binding",
            over_with_invalid,
            vec![violation; 10_002],
            0,
        ),
    ];
    for (label, creations, results, stored) in cases {
        let (broker_handle, _dir) = start_broker(configured_authorizer()).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let resp = handle(&broker, request(creations), &ctx, VERSION)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = CreateAclsResponse {
            throttle_time_ms: 0,
            results,
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        check!(resp == expected, "{label}");
        check!(all_acls(&broker_handle).len() == stored, "{label}");
        broker_handle.shutdown().await;
    }
}

/// A freshly bootstrapped cluster finalizes Kafka 4.3's `4.3-IV0`, below the
/// `4.4-IV1` CIDR host patterns need, so it refuses a CIDR host until an
/// operator opts into 4.4-IV1. (An image with no `metadata.version` at all is
/// judged against 4.3-IV0 too; `features::tests` pins that.)
#[tokio::test]
async fn handle_rejects_cidr_host_on_a_freshly_bootstrapped_cluster() {
    let (broker_handle, _dir) = start_trunk_broker().await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let mut cidr_creation = creation("topic-a", "User:alice", OPERATION_READ);
    cidr_creation.host = "10.0.0.0/8".into();
    let req = request(vec![cidr_creation]);

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    let expected = CreateAclsResponse {
        throttle_time_ms: 0,
        results: vec![AclCreationResult {
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: Some(
                "CIDR-based ACL host patterns require metadata version 4.4-IV1 or higher.".into(),
            ),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        }],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    assert!(all_acls(&broker_handle).is_empty());
    broker_handle.shutdown().await;
}
