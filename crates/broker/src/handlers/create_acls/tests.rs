//! End-to-end `CreateAcls` tests that drive [`super::handle`] against a live
//! in-process broker.
//!
//! These cover what only the whole handler shows: the cluster-alter denial that
//! stamps every creation, the positional interleaving of accepted and rejected
//! creations, and the configured principal and resource-name byte limits.

use std::sync::Arc;

use assert2::assert;
use krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::create_acls_response::{AclCreationResult, CreateAclsResponse},
};
use krabka_units::convert::ByteSizeExt as _;

use super::{handle, validate::USER_PRINCIPAL_PREFIX};
use crate::{
    codes,
    handlers::create_acls::test_support::{
        OPERATION_READ, OPERATION_WRITE, VERSION, all_acls, creation, decode_response, request,
        test_context,
    },
    test_support::{
        DenyAll, peer, principal, start_broker_with,
        start_broker_with_authorizer_no_audit as start_broker,
    },
};

#[tokio::test]
async fn handle_honors_configured_acl_input_limits() {
    const PRINCIPAL_LIMIT: usize = 10;
    const RESOURCE_NAME_LIMIT: usize = 8;

    fn limit(characters: usize) -> krabka_units::ByteSize {
        krabka_units::ByteSize::from_bytes(u64::try_from(characters).expect("test limit fits u64"))
    }

    let (broker_handle, _dir) = start_broker_with(|config| {
        config.acl_max_principal = limit(PRINCIPAL_LIMIT);
        config.acl_max_resource_name = limit(RESOURCE_NAME_LIMIT);
        config.audit_enabled = false;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let ctx = test_context(&p, &peer);
    let cases = [
        (
            "r".repeat(RESOURCE_NAME_LIMIT),
            "User:a".to_string(),
            codes::NONE,
            None,
        ),
        (
            "r".repeat(RESOURCE_NAME_LIMIT + 1),
            "User:a".to_string(),
            codes::INVALID_REQUEST,
            Some("resource_name too long"),
        ),
        (
            "r".to_string(),
            format!(
                "User:{}",
                "a".repeat(PRINCIPAL_LIMIT - USER_PRINCIPAL_PREFIX.len())
            ),
            codes::NONE,
            None,
        ),
        (
            "r".to_string(),
            format!(
                "User:{}",
                "a".repeat(PRINCIPAL_LIMIT + 1 - USER_PRINCIPAL_PREFIX.len())
            ),
            codes::INVALID_REQUEST,
            Some("principal too long"),
        ),
    ];
    let req = request(
        cases
            .iter()
            .map(|(resource_name, principal, _, _)| {
                creation(resource_name, principal, OPERATION_READ)
            })
            .collect(),
    );

    let resp = handle(&broker, req, &ctx, VERSION).await.expect("handle");
    let resp = decode_response(&resp);

    for (result, (_, _, expected_code, expected_message)) in resp.results.iter().zip(&cases) {
        assert!(
            (result.error_code, result.error_message.as_deref())
                == (*expected_code, *expected_message)
        );
    }
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
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
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
            AclCreationResult {
                error_code: 0,
                error_message: None,
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
            AclCreationResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("empty resource_name".into()),
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
