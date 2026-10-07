//! End-to-end operation implications: a Read or a Write ACL on a topic also
//! grants Describe, so Metadata-by-name resolves the topic without a
//! separate Describe seed.

use assert2::assert;

use crate::{acl_admin::create_topic_as_admin, polling::retry_metadata_until_topic_visible};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implication_metadata_describes_after_read_acl() {
    metadata_implication(krabka_metadata::AclOperation::Read).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implication_metadata_describes_after_write_acl() {
    metadata_implication(krabka_metadata::AclOperation::Write).await;
}

async fn metadata_implication(operation: krabka_metadata::AclOperation) {
    let (handle, _dir, _) = crate::sasl_cluster::start_admin_alice().await;
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "foo", 1).await;

    // Read and Write both imply Describe without a separate Describe ACL.
    handle
        .submit_metadata_record_for_test(crate::support::acl::topic_acl_record(
            "foo",
            "User:alice",
            operation,
        ))
        .await
        .expect("seed implication ACL for alice");

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
        "{operation:?} implies Describe, foo must be visible to alice with error_code=0, got {row:?}"
    );
}
