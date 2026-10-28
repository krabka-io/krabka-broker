//! Broker-side integration tests for KIP-599 `controller_mutation_rate`.
//!
//! Tests:
//! 1. `controller_mutation_rate_throttles_create_topics`. Set rate=2.0 for
//!    alice. Let one strict request cross the limit, then assert the next is
//!    rejected with `THROTTLING_QUOTA_EXCEEDED`.
//! 2. `unthrottled_create_topics_unaffected`. No quota. Create a topic.
//!    Assert `throttle_time_ms` == 0.
//! 3. `controller_mutation_rate_throttles_delete_topics`. Pre-create a topic
//!    with 10 partitions. Set rate=2.0 for alice. Alice deletes. Assert
//!    `throttle_time_ms` > 0.
#![cfg(test)]

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `controller_mutation_quota/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
#[path = "controller_mutation_quota/cluster.rs"]
mod cluster;
mod kafka_wire;
mod support;
#[path = "controller_mutation_quota/topic_admin.rs"]
mod topic_admin;

/// The client id every request header in this suite carries, which is what the
/// broker reads when it looks the mutation quota up.
const CLIENT_ID: &str = "krabka-mutation-quota-test";

// ─────────────────────────────────────────────────────────────────────────────
// Integration tests
// ─────────────────────────────────────────────────────────────────────────────
use {
    crate::{
        cluster::start_single_broker_sasl_plaintext_with_users,
        kafka_wire::{
            PlainAuthenticationSetup,
            quotas::{QuotaEntity, alter_user_quota},
        },
        support::{
            acl::{cluster_acl_record, topic_acl_record},
            client::metric_value,
        },
        topic_admin::{TopicMutationResult, drive_create_topics_sasl, drive_delete_topics_sasl},
    },
    assert2::{assert, check},
    krabka_metadata::AclOperation,
    std::net::SocketAddr,
};

/// Wait until the `controller_mutation_rate` quota is visible in the broker's
/// metadata image.
async fn wait_for_mutation_quota_in_image(handle: &krabka_broker::BrokerHandle) {
    handle
        .wait_for_image(|img| {
            img.client_quotas()
                .values()
                .any(|configs| configs.contains_key("controller_mutation_rate"))
        })
        .await;
}

/// Install Alice's shared mutation quota and return its complete wire response.
async fn set_controller_mutation_quota(addr: SocketAddr) -> Vec<(QuotaEntity, i16)> {
    alter_user_quota()
        .addr(addr)
        .authentication(PlainAuthenticationSetup::admin(CLIENT_ID))
        .key("controller_mutation_rate")
        .value(2.0)
        .call()
        .await
}

struct MutationQuota {
    broker: Option<krabka_broker::BrokerHandle>,
    directory: Option<tempfile::TempDir>,
    result: Option<TopicMutationResult>,
}
#[rstest::fixture]
fn mutation() -> MutationQuota {
    // Start this scenario with fresh inputs and observations.

    MutationQuota {
        broker: None,
        directory: None,
        result: None,
    }
}
#[rstest_bdd_macros::given("alice has Cluster Create permission and controller mutation rate 2")]
async fn limited_topic_creator(mutation: &mut MutationQuota) {
    let (handle, directory, addr) = start_single_broker_sasl_plaintext_with_users("admin", &[
        ("admin", "admin-secret"),
        ("alice", "alice-secret"),
    ])
    .await;

    // Seed an ACL granting alice Cluster Create — this also disables the
    // compat shim (allow-all when no ACLs present in image).
    let admin_acl = cluster_acl_record().operation(AclOperation::Create).call();
    handle
        .submit_metadata_record_for_test(admin_acl)
        .await
        .expect("seed ACL");

    // Set controller_mutation_rate=2.0 for (user=alice).
    let alter = set_controller_mutation_quota(addr).await;
    assert!(alter[0].1 == 0, "alter should succeed");

    // Wait until the controller_mutation_rate quota is committed to this
    // broker's metadata image. The CreateTopics handler reads the rate
    // straight from the image on the first consume (the bucket is created
    // lazily with that rate; the refresh task only re-rates existing buckets),
    // so image visibility — not the refresh task — is the real precondition.
    wait_for_mutation_quota_in_image(&handle).await;

    mutation.broker = Some(handle);
    mutation.directory = Some(directory);
}
#[rstest_bdd_macros::given("admin is a super user without a controller mutation quota")]
async fn unlimited_topic_creator(mutation: &mut MutationQuota) {
    let (handle, directory, _) =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", "admin-secret")]).await;
    // No controller_mutation_rate quota configured.
    // admin is super_user, no ACL seeding needed.

    mutation.broker = Some(handle);
    mutation.directory = Some(directory);
}
#[rstest_bdd_macros::when("{user} creates topic {topic} with {partitions} partitions")]
async fn quota_create_topic(
    mutation: &mut MutationQuota,
    user: String,
    topic: String,
    partitions: i32,
) {
    let password = match user.as_str() {
        "alice" => "alice-secret",
        "admin" => "admin-secret",
        other => panic!("unknown principal {other}"),
    };
    let addr = mutation
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    mutation.result =
        Some(drive_create_topics_sasl(addr, &user, password, &topic, partitions).await);
}
#[rstest_bdd_macros::then("the topic mutation has error code {error}")]
fn quota_mutation_result(mutation: &MutationQuota, error: i16) {
    let result = mutation.result.as_ref().expect("topic mutation attempted");
    check!(
        result.error_code == error,
        "expected error {error}, got {}",
        result.error_code
    );
}
#[rstest_bdd_macros::then("the topic mutation reports no throttle delay")]
fn quota_mutation_unthrottled(mutation: &MutationQuota) {
    check!(
        mutation
            .result
            .as_ref()
            .expect("topic mutation attempted")
            .throttle_time_ms
            == 0
    );
}
#[rstest_bdd_macros::then("the topic mutation reports a positive throttle delay")]
fn quota_mutation_throttled(mutation: &MutationQuota) {
    check!(
        mutation
            .result
            .as_ref()
            .expect("topic mutation attempted")
            .throttle_time_ms
            > 0
    );
}
#[rstest_bdd_macros::given(
    "alice may Delete a ten partition topic and has controller mutation rate 2"
)]
async fn limited_topic_deleter(mutation: &mut MutationQuota) {
    let (handle, directory, addr) = start_single_broker_sasl_plaintext_with_users("admin", &[
        ("admin", "admin-secret"),
        ("alice", "alice-secret"),
    ])
    .await;

    // Seed a dummy ACL to disable the compat shim (allow-all when no ACLs present).
    // Use an unrelated ACL; the real alice ACLs come below.
    let shim_disable = topic_acl_record()
        .topic("__compat_shim_disable__")
        .principal("User:admin")
        .call();
    handle
        .submit_metadata_record_for_test(shim_disable)
        .await
        .expect("seed compat shim disable ACL");
    // Wait until the shim-disable ACL is visible in the metadata image so the
    // allow-all compat shim is actually off before the scenario proceeds.
    handle
        .wait_for_image(|img| {
            img.all_acls()
                .any(|a| a.resource_name == "__compat_shim_disable__")
        })
        .await;

    // Pre-create topic as admin (no quota for admin) with 10 partitions.
    let TopicMutationResult { error_code: ec, .. } =
        drive_create_topics_sasl(addr, "admin", "admin-secret", "to-delete", 10).await;
    assert!(ec == 0);

    // Grant alice Topic Delete on "to-delete".
    let alice_delete_acl = topic_acl_record()
        .topic("to-delete")
        .operation(AclOperation::Delete)
        .call();
    handle
        .submit_metadata_record_for_test(alice_delete_acl)
        .await
        .expect("seed alice Delete ACL");
    // Wait until alice's Delete ACL on "to-delete" is visible in the image so
    // the later delete is authorized.
    handle
        .wait_for_image(|img| {
            img.all_acls()
                .any(|a| a.resource_name == "to-delete" && a.principal == "User:alice")
        })
        .await;

    // Now set the quota for alice and delete.
    let alter = set_controller_mutation_quota(addr).await;
    assert!(alter[0].1 == 0);
    // Wait for alice's controller_mutation_rate quota to land in the image
    // before deleting; DeleteTopics reads the rate from the image on consume.
    wait_for_mutation_quota_in_image(&handle).await;

    mutation.broker = Some(handle);
    mutation.directory = Some(directory);
}
#[rstest_bdd_macros::when("alice deletes to-delete")]
async fn quota_delete_topic(mutation: &mut MutationQuota) {
    let addr = mutation
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    mutation.result =
        Some(drive_delete_topics_sasl(addr, "alice", "alice-secret", "to-delete").await);
}
#[rstest_bdd_macros::then(
    "quota and DeleteTopics latency metrics both cover the reported applied throttle"
)]
async fn quota_applied_metrics(mutation: &MutationQuota) {
    let handle = mutation.broker.as_ref().expect("broker started");
    let throttle_ms = mutation
        .result
        .as_ref()
        .expect("topic mutation attempted")
        .throttle_time_ms;
    // The broker sleeps on the KIP-599 delay, so it is an applied throttle and
    // has to be visible as one: an operator watching DeleteTopics latency rise
    // must be able to tell "alice is over her mutation quota" from "the
    // controller is wedged".
    let applied_seconds = f64::from(throttle_ms) / 1000.0;
    let by_quota = metric_value(
        handle,
        "krabka_broker_quota_throttle_duration_seconds_sum{quota_type=\"ControllerMutation\"}",
    )
    .await;
    check!(
        by_quota >= applied_seconds,
        "controller_mutation_rate must be credited with the applied throttle \
         ({applied_seconds}s); got {by_quota}"
    );
    let phase = metric_value(
        handle,
        "krabka_broker_request_throttle_duration_seconds_sum{api_key=\"DeleteTopics\"}",
    )
    .await;
    check!(
        phase >= applied_seconds,
        "the throttle phase must cover the delay the response reported \
         ({applied_seconds}s); got {phase}"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/controller_mutation_quota.feature",
    name = "controller mutation rate throttles create topics"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_mutation_rate_throttles_create_topics(mutation: MutationQuota) {
    mutation.broker.expect("broker started").shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/controller_mutation_quota.feature",
    name = "unthrottled create topics unaffected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unthrottled_create_topics_unaffected(mutation: MutationQuota) {
    mutation.broker.expect("broker started").shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/controller_mutation_quota.feature",
    name = "controller mutation rate throttles delete topics"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_mutation_rate_throttles_delete_topics(mutation: MutationQuota) {
    mutation.broker.expect("broker started").shutdown().await;
}
