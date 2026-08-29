//! Produce and Fetch enforcement (T23): a Write ACL on the topic gates a
//! Produce, a Read ACL gates a Fetch, and the decision lands on the
//! per-partition row of the response.
//!
//! Each test boots a fresh single-broker `SASL_PLAINTEXT` cluster with admin
//! as the super-user. Admin (over SASL) drives a `CreateTopics` request to
//! materialise the partition, the test seeds whatever ACL records are needed
//! via the controller-direct test helper, then alice authenticates (a
//! separate connection) and drives a Produce / Fetch. The assertions look at
//! the per-partition `error_code` row of the response and, on the happy
//! path, also check the broker's local log end offset via the existing
//! `BrokerHandle::local_log_end_offset` helper.

use assert2::assert;
use krabka_broker::Broker;
use krabka_protocol::owned::fetch_request::{FetchPartition, FetchRequest, FetchTopic};

use crate::{
    ERR_TOPIC_AUTHORIZATION_FAILED,
    acl_admin::create_topic_as_admin,
    client_api::{drive_fetch_as_plain, drive_produce_as_plain, single_record_produce_request},
    polling::retry_produce_until_allowed,
    sasl_cluster::sasl_plain_broker_config,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_denied_without_topic_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Admin creates topic "foo" with one partition (rf=1, single-node).
    create_topic_as_admin(addr, "foo", 1).await;

    // Seed a meaningless ACL via direct controller write. The super-user
    // is already set so `authorize()`'s compat shim is off, but populating
    // at least one ACL makes the test read closer to a "real" cluster
    // post-bootstrap.
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

    // alice has NO Write-on-foo binding → Produce must return 29.
    let resp = drive_produce_as_plain(
        addr,
        "alice",
        b"wonderland",
        single_record_produce_request("foo", 0, b"hello"),
    )
    .await
    .expect("Produce must round-trip");
    handle.shutdown().await;

    assert!(resp.responses.len() == 1, "one topic in response");
    assert!(
        resp.responses[0].partition_responses.len() == 1,
        "one partition row in response"
    );
    let p = &resp.responses[0].partition_responses[0];
    assert!(
        p.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "alice has no Write ACL on foo, expected TOPIC_AUTHORIZATION_FAILED (29), got {p:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_allowed_with_topic_write_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "foo", 1).await;

    // Provision Allow Write Topic LITERAL "foo" User:alice host=* via a
    // direct controller write. (CreateAcls as admin would also work,
    // but `submit_metadata_record_for_test` is one fewer round-trip and
    // exercises the same authorizer state.)
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

    // The ACL submit above is committed via the controller's raft path
    // then applied into the in-memory `MetadataImage` asynchronously, so
    // Produce reads from that image. Retry on Deny for up to 10 s — on
    // CI the commit-then-apply gap is usually a few ms but can spike.
    let resp = retry_produce_until_allowed(addr, "alice", b"wonderland", "foo")
        .await
        .expect("Produce must round-trip");

    assert!(resp.responses.len() == 1);
    assert!(resp.responses[0].partition_responses.len() == 1);
    let p = &resp.responses[0].partition_responses[0];
    assert!(
        p.error_code == 0,
        "alice has Write ACL on foo, expected error_code=0, got {p:?}"
    );

    // Verify the record actually landed in the local log.
    let leo = handle
        .local_log_end_offset("foo", 0)
        .expect("foo-0 must be hosted on this broker");
    handle.shutdown().await;
    assert!(
        leo >= 1,
        "log_end_offset must advance after a successful Produce, got {leo}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_denied_without_topic_read_acl() {
    let log_dir = tempfile::tempdir().unwrap();
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    create_topic_as_admin(addr, "foo", 1).await;

    // Seed a dummy ACL via direct controller write. Same rationale as in
    // produce_denied_without_topic_acl.
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

    // alice has NO Read-on-foo binding → Fetch must return 29 on the
    // partition row.
    let req = FetchRequest {
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1_048_576,
        topics: vec![FetchTopic {
            topic: "foo".to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1_048_576,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = drive_fetch_as_plain(addr, "alice", b"wonderland", req)
        .await
        .expect("Fetch must round-trip");
    handle.shutdown().await;

    assert!(resp.responses.len() == 1, "one topic in response");
    assert!(
        resp.responses[0].partitions.len() == 1,
        "one partition row in response"
    );
    let p = &resp.responses[0].partitions[0];
    assert!(
        p.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "alice has no Read ACL on foo, expected TOPIC_AUTHORIZATION_FAILED (29), got {p:?}"
    );
}
