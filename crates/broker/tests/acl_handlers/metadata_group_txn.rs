//! `Metadata`, `JoinGroup`, and `InitProducerId` enforcement (T24): the Topic,
//! Group, and `TransactionalId` resource types each gate a different request,
//! and `Metadata` gates one of two ways depending on how the client asks.
//!
//! Each test boots a fresh single-broker `SASL_PLAINTEXT` cluster with admin
//! as the super-user. The test seeds whatever ACL records the scenario
//! requires via the controller-direct test helper (which keeps the compat
//! shim off because at least one ACL exists), then alice authenticates
//! separately and drives the typed request.

use assert2::assert;
use krabka_broker::Broker;
use krabka_protocol::owned::init_producer_id_request::InitProducerIdRequest;

use crate::{
    ERR_GROUP_AUTHORIZATION_FAILED, ERR_MEMBER_ID_REQUIRED, ERR_TOPIC_AUTHORIZATION_FAILED,
    ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
    acl_admin::create_topic_as_admin,
    client_api::{drive_init_producer_id_as_plain, drive_join_group_as_plain, join_group_request},
    polling::{retry_join_group_until_allowed, retry_metadata_until_topic_visible},
    sasl_cluster::sasl_plain_broker_config,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_silent_filter_on_fetch_all() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "t1", 1).await;
    create_topic_as_admin(addr, "t2", 1).await;

    // Seed Allow Describe Topic LITERAL "t1" User:alice. The presence of
    // any ACL in the image also disables the compat shim, so the
    // authorizer evaluates every request rather than short-circuiting to
    // Allow.
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1AccessControlEntry(
            krabka_metadata::AclEntry {
                resource_type: krabka_metadata::ResourceType::Topic,
                resource_name: "t1".into(),
                pattern_type: krabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: krabka_metadata::AclOperation::Describe,
                permission_type: krabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Describe-on-t1 ACL for alice");

    // Wait for the ACL to propagate before issuing Metadata as alice —
    // until then alice sees no topics at all. Once t1 appears, the image
    // has applied the binding.
    let resp = retry_metadata_until_topic_visible(addr, "alice", b"wonderland", "t1", None)
        .await
        .expect("Metadata must round-trip");
    handle.shutdown().await;

    let names: Vec<&str> = resp
        .topics
        .iter()
        .filter_map(|t| t.name.as_deref())
        .collect();
    assert!(
        names.contains(&"t1"),
        "t1 must be visible to alice, got {names:?}"
    );
    assert!(
        !names.contains(&"t2"),
        "t2 must be silently filtered out of fetch-all, got {names:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_explicit_deny_on_named_topic() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "t1", 1).await;
    create_topic_as_admin(addr, "t2", 1).await;

    // Seed Allow Describe on t1 for alice. This both turns the compat
    // shim off and gives alice *something* she's authorized to see, so
    // the Deny on t2 isn't merely "no ACLs anywhere".
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1AccessControlEntry(
            krabka_metadata::AclEntry {
                resource_type: krabka_metadata::ResourceType::Topic,
                resource_name: "t1".into(),
                pattern_type: krabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: krabka_metadata::AclOperation::Describe,
                permission_type: krabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Describe-on-t1 ACL for alice");

    // Ask Metadata for t2 *by name*. The named-topic path returns an
    // error row instead of silently filtering. Use the retry helper so
    // we don't race the raft commit-then-apply gap on the seeded ACL.
    let resp = retry_metadata_until_topic_visible(
        addr,
        "alice",
        b"wonderland",
        "t2",
        Some(vec!["t2".to_string()]),
    )
    .await
    .expect("Metadata must round-trip");
    handle.shutdown().await;

    assert!(resp.topics.len() == 1, "one topic row in response");
    let row = &resp.topics[0];
    assert!(row.name.as_deref() == Some("t2"));
    assert!(
        row.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "alice has no ACL on t2, expected TOPIC_AUTHORIZATION_FAILED (29), got {row:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_group_denied_without_group_read_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Seed a meaningless ACL so the compat shim is off. Without this the
    // authorizer would short-circuit to Allow on every check and the
    // Deny assertion below would never fire.
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1AccessControlEntry(
            krabka_metadata::AclEntry {
                resource_type: krabka_metadata::ResourceType::Topic,
                resource_name: "_nothing".into(),
                pattern_type: krabka_metadata::PatternType::Literal,
                principal: "User:admin".into(),
                host: "*".into(),
                operation: krabka_metadata::AclOperation::Read,
                permission_type: krabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed dummy ACL");

    // alice has NO Group Read ACL on "cg-1" → JoinGroup must return 30.
    let denied =
        drive_join_group_as_plain(addr, "alice", b"wonderland", join_group_request("cg-1"))
            .await
            .expect("JoinGroup must round-trip");
    assert!(
        denied.error_code == ERR_GROUP_AUTHORIZATION_FAILED,
        "alice has no Group Read on cg-1, expected GROUP_AUTHORIZATION_FAILED (30), got {denied:?}"
    );

    // Provision Allow Read Group LITERAL "cg-1" User:alice.
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1AccessControlEntry(
            krabka_metadata::AclEntry {
                resource_type: krabka_metadata::ResourceType::Group,
                resource_name: "cg-1".into(),
                pattern_type: krabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: krabka_metadata::AclOperation::Read,
                permission_type: krabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Read-on-cg-1 ACL for alice");

    // Retry until the ACL is applied (not 30 any more). The first
    // non-denied response is MEMBER_ID_REQUIRED (79) — JoinGroup with
    // empty member_id gets a broker-generated id and tells the client to
    // retry with it. Capture that id and call again to complete the
    // join.
    let bootstrap = retry_join_group_until_allowed(addr, "alice", b"wonderland", "cg-1")
        .await
        .expect("JoinGroup retry must round-trip");
    assert!(
        bootstrap.error_code == ERR_MEMBER_ID_REQUIRED,
        "first authorized JoinGroup must return MEMBER_ID_REQUIRED (79) with a generated member_id, got {bootstrap:?}"
    );
    assert!(
        !bootstrap.member_id.is_empty(),
        "broker must return a non-empty generated member_id on MEMBER_ID_REQUIRED"
    );

    // Second call with the generated member_id should complete the
    // rebalance (single-member group) and return error_code=0.
    let mut req2 = join_group_request("cg-1");
    req2.member_id = bootstrap.member_id;
    let joined = drive_join_group_as_plain(addr, "alice", b"wonderland", req2)
        .await
        .expect("second JoinGroup must round-trip");
    handle.shutdown().await;

    assert!(
        joined.error_code == 0,
        "JoinGroup must succeed with alice's Group Read ACL on cg-1, got {joined:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_denied_without_txn_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Seed a dummy ACL to disable the compat shim.
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1AccessControlEntry(
            krabka_metadata::AclEntry {
                resource_type: krabka_metadata::ResourceType::Topic,
                resource_name: "_nothing".into(),
                pattern_type: krabka_metadata::PatternType::Literal,
                principal: "User:admin".into(),
                host: "*".into(),
                operation: krabka_metadata::AclOperation::Read,
                permission_type: krabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed dummy ACL");

    let req = InitProducerIdRequest {
        transactional_id: Some("tx-1".to_string()),
        transaction_timeout_ms: 60_000,
        producer_id: -1,
        producer_epoch: -1,
        ..Default::default()
    };
    let resp = drive_init_producer_id_as_plain(addr, "alice", b"wonderland", req)
        .await
        .expect("InitProducerId must round-trip");
    handle.shutdown().await;

    assert!(
        resp.error_code == ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        "alice has no TransactionalId Write ACL on tx-1, expected TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53), got {resp:?}"
    );
}
