//! End-to-end operation implications: a Read or a Write ACL on a topic also
//! grants Describe, so Metadata-by-name resolves the topic without a
//! separate Describe seed.

use assert2::assert;
use krabka_broker::Broker;

use crate::{
    acl_admin::create_topic_as_admin, polling::retry_metadata_until_topic_visible,
    sasl_cluster::sasl_plain_broker_config,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implication_metadata_describes_after_read_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "foo", 1).await;

    // Seed Allow READ Topic LITERAL "foo" User:alice host=*. No explicit
    // Describe ACL — relies on the Read→Describe implication for
    // the Metadata-by-name visibility check.
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1AccessControlEntry(
            krabka_metadata::AclEntry {
                resource_type: krabka_metadata::ResourceType::Topic,
                resource_name: "foo".into(),
                pattern_type: krabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: krabka_metadata::AclOperation::Read,
                permission_type: krabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Read-on-foo ACL for alice");

    // Wait for raft commit-then-apply, then ask Metadata for foo by name.
    // Pre-13b would have returned TOPIC_AUTHORIZATION_FAILED (29).
    let resp = retry_metadata_until_topic_visible(
        addr,
        "alice",
        b"wonderland",
        "foo",
        Some(vec!["foo".to_string()]),
    )
    .await
    .expect("Metadata must round-trip");
    handle.shutdown().await;

    assert!(resp.topics.len() == 1, "one topic row in response");
    let row = &resp.topics[0];
    assert!(row.name.as_deref() == Some("foo"));
    assert!(
        row.error_code == 0,
        "Read implies Describe, foo must be visible to alice with error_code=0, got {row:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implication_metadata_describes_after_write_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "foo", 1).await;

    // Seed Allow WRITE Topic LITERAL "foo" User:alice host=*. No explicit
    // Describe ACL — relies on the Write→Describe implication.
    handle
        .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1AccessControlEntry(
            krabka_metadata::AclEntry {
                resource_type: krabka_metadata::ResourceType::Topic,
                resource_name: "foo".into(),
                pattern_type: krabka_metadata::PatternType::Literal,
                principal: "User:alice".into(),
                host: "*".into(),
                operation: krabka_metadata::AclOperation::Write,
                permission_type: krabka_metadata::PermissionType::Allow,
            },
        ))
        .await
        .expect("seed Write-on-foo ACL for alice");

    let resp = retry_metadata_until_topic_visible(
        addr,
        "alice",
        b"wonderland",
        "foo",
        Some(vec!["foo".to_string()]),
    )
    .await
    .expect("Metadata must round-trip");
    handle.shutdown().await;

    assert!(resp.topics.len() == 1, "one topic row in response");
    let row = &resp.topics[0];
    assert!(row.name.as_deref() == Some("foo"));
    assert!(
        row.error_code == 0,
        "Write implies Describe, foo must be visible to alice with error_code=0, got {row:?}"
    );
}
