//! End-to-end `DeleteAcls` tests that drive [`super::handle`] against a live
//! in-process broker.
//!
//! These cover what only the whole handler shows: the cluster-alter denial
//! that stamps every filter and leaves the ACLs in place, and the matching-ACL
//! echo a successful filter returns next to the entries it actually removed.

use std::sync::Arc;

use assert2::assert;
use krabka_metadata::{AclEntry, AclOperation, MetadataRecord};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::delete_acls_response::{
        DeleteAclsFilterResult, DeleteAclsMatchingAcl, DeleteAclsResponse,
    },
};

use super::handle;
use crate::{
    broker::BrokerHandle,
    codes,
    handlers::delete_acls::test_support::{
        OPERATION_READ, PATTERN_TYPE_LITERAL, PERMISSION_ALLOW, RESOURCE_TYPE_TOPIC, VERSION, acl,
        decode_response, filter, request, test_context,
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
        error_message: Some("delete-acls denied".into()),
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
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
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
