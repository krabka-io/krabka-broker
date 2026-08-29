//! The authorizer decorator's deny path, seen from the client side.
//!
//! The decorator sits in front of every admin operation, so a broker that
//! panics or wedges on a denial takes the audit sink down with it. The case
//! here has a deny-all authorizer refuse a `CreateTopics`, and then reads the
//! audit topic back to show that the broker still serves. Its own doc comment
//! records why it stops short of asserting the `AuthorizationDenied` record.

use krabka_broker::coordinator::AUDIT_TOPIC;
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    fetch_request::{FetchPartition, FetchRequest, FetchTopic},
};

use crate::support;

/// Verifies that the authorizer-decorator path denies an unauthorized
/// operation.
///
/// This test asserts that:
///   1. The broker denies a `CreateTopics` request with
///      `CLUSTER_AUTHORIZATION_FAILED`.
///   2. The broker stays healthy and does not crash.
///
/// This test does NOT assert that the broker emitted an `AuthorizationDenied`
/// audit record to the audit topic.
///
/// The full end-to-end path, which sends a denied request and then observes
/// the `AuthorizationDenied` record in the audit topic through the same
/// client, is impractical for these reasons:
///   - The test client connects anonymously, with the principal
///     `"ANONYMOUS"`.
///   - `SimpleAclAuthorizer` with no ACLs and no super-users denies every
///     request, including the `Fetch` that reads the audit topic back.
///   - There is no plaintext SASL path that would give the anonymous reader a
///     higher principal without SCRAM credentials.
///
/// The unit test `deny_decision_emits_audit_record` in
/// `crates/broker/src/audit_authorizer.rs` already proves the audit emit on a
/// deny.
#[tokio::test]
async fn denied_operation_returns_cluster_authorization_failed() {
    // Start a broker with a deny-all authorizer.
    let p = support::start_with_deny_all_authz().await;

    // Attempt a create that will be denied.
    let resp = p
        .client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "denied-topic".into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .unwrap();

    // Verify the broker actually denied the request (error_code
    // CLUSTER_AUTHORIZATION_FAILED = 31).
    let denied = resp
        .topics
        .iter()
        .any(|t| t.error_code == krabka_broker::codes::CLUSTER_AUTHORIZATION_FAILED);
    assert2::check!(denied, "expected CreateTopics to be denied; resp: {resp:?}");

    // Verify the broker is still alive by checking the audit topic is reachable.
    let topic_id = support::topic_id_for(&p.client, AUDIT_TOPIC).await;
    let fr = p
        .client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 0,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    // The broker responded to the Fetch request without crashing.
    let _ = fr;

    p.broker.shutdown().await;
}
