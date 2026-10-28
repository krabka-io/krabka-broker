#![cfg(test)]

//! Broker-level integration test for (user, client-id) tuple quota
//! end-to-end enforcement.
//!
//! Tests:
//! 1. `tuple_quota_throttles_only_matching_client_id`. It sets
//!    `(user=alice, client-id=app-x) producer_byte_rate=1024`. A produce of
//!    about 4 KB as (alice, app-x) must give `throttle_time_ms > 0`. A produce
//!    of about 4 KB as (alice, other) must give `throttle_time_ms == 0`,
//!    because no quota matches.
//!
//! This test covers the end-to-end behaviour: the Produce handler must forward
//! `ctx.client_id` to the quota lookup instead of "". If it forwards "", the
//! tuple lookup never matches and `throttle_time_ms` is 0 in both cases.
//!
//! The test is gated to non-Windows, to match the multi-broker test convention
//! established by the existing integration suites.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `tuple_quota_enforcement/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
mod kafka_wire;
mod support;
#[path = "tuple_quota_enforcement/tuple_quota_cluster.rs"]
mod tuple_quota_cluster;
#[path = "tuple_quota_enforcement/tuple_quota_drivers.rs"]
mod tuple_quota_drivers;

/// The default client id request headers in this suite carry. The tuple-quota
/// test sends a second produce under another id, which the broker reads for the
/// quota lookup.
const CLIENT_ID: &str = "krabka-tuple-quota-test";

use {
    crate::{
        kafka_wire::{PlainAuthenticationSetup, quotas::QuotaAlterSetup},
        tuple_quota_cluster::{
            create_topic_as_admin, seed_alice_write_acl, seed_compat_shim_disable_acl,
            start_single_broker_sasl_plaintext_with_users,
        },
        tuple_quota_drivers::{await_authorized_produce, drive_alter_client_quotas_sasl},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_metadata::EntityKey,
    krabka_protocol::owned::produce_response::ProduceResponse,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::net::SocketAddr,
    tempfile::TempDir,
};

#[derive(Default)]
struct TupleQuota {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    address: Option<SocketAddr>,
    admin_password: String,
    alice_password: String,
    altered: Option<Vec<(EntityKey, i16)>>,
    produced: Option<ProduceResponse>,
}

#[fixture]
fn tuple_quota() -> TupleQuota {
    // Each authenticated client request retains its real broker throttle observation.
    TupleQuota::default()
}

#[given("Alice may write tuple-quota-topic using authenticated client IDs")]
async fn authorized_tuple_writer(tuple_quota: &mut TupleQuota) {
    tuple_quota.admin_password = uuid::Uuid::new_v4().to_string();
    tuple_quota.alice_password = uuid::Uuid::new_v4().to_string();
    let (broker, directory, address) = start_single_broker_sasl_plaintext_with_users("admin", &[
        ("admin", tuple_quota.admin_password.as_str()),
        ("alice", tuple_quota.alice_password.as_str()),
    ])
    .await;
    seed_compat_shim_disable_acl(&broker).await;
    create_topic_as_admin()
        .addr(address)
        .client_id(CLIENT_ID)
        .password(tuple_quota.admin_password.as_bytes())
        .topic(support::topics::ConfiguredTopicSetup {
            name: "tuple-quota-topic".into(),
            ..Default::default()
        })
        .call()
        .await;
    broker
        .wait_until_partition_present("tuple-quota-topic", 0)
        .await;
    seed_alice_write_acl(&broker, "tuple-quota-topic").await;
    tuple_quota.broker = Some(broker);
    tuple_quota.directory = Some(directory);
    tuple_quota.address = Some(address);
}

#[when("the administrator assigns only Alice's app-x tuple a 1024-byte-per-second producer quota")]
async fn assign_tuple_quota(tuple_quota: &mut TupleQuota) {
    tuple_quota.altered = Some(
        drive_alter_client_quotas_sasl(
            tuple_quota.address.expect("listener"),
            vec![(
                vec![
                    ("user".into(), Some("alice".into())),
                    ("client-id".into(), Some("app-x".into())),
                ],
                vec![("producer_byte_rate".into(), 1024.0, false)],
            )],
            &QuotaAlterSetup {
                authentication: PlainAuthenticationSetup {
                    client_id: CLIENT_ID,
                    user: "admin",
                    password: tuple_quota.admin_password.as_bytes().to_vec(),
                },
                ..Default::default()
            },
        )
        .await,
    );
}

#[then("the tuple quota alteration succeeds and appears in committed metadata")]
async fn tuple_quota_committed(tuple_quota: &TupleQuota) {
    let response = tuple_quota.altered.as_ref().expect("quota altered");
    assert!(response.len() == 1);
    assert!(response[0].1 == 0, "quota alter failed: {response:?}");
    tuple_quota
        .broker
        .as_ref()
        .expect("broker")
        .wait_for_image(|image| {
            let key: EntityKey = vec![
                ("client-id".into(), Some("app-x".into())),
                ("user".into(), Some("alice".into())),
            ];
            image
                .client_quotas()
                .get(&key)
                .and_then(|configs| configs.get("producer_byte_rate"))
                == Some(&1024.0)
        })
        .await;
}

#[when("Alice produces four kilobytes through a fresh connection with client ID {client_id}")]
async fn produce_under_tuple_quota(tuple_quota: &mut TupleQuota, client_id: String) {
    tuple_quota.produced = Some(
        await_authorized_produce(
            tuple_quota.address.expect("listener"),
            tuple_quota.alice_password.as_bytes(),
            &client_id,
        )
        .await,
    );
}

#[then("the tuple-quota produce succeeds and is throttled: {throttled}")]
fn tuple_quota_applied(tuple_quota: &TupleQuota, throttled: bool) {
    let response = tuple_quota.produced.as_ref().expect("Produce answered");
    assert!(response.responses[0].partition_responses[0].error_code == 0);
    if throttled {
        assert!(
            response.throttle_time_ms > 0,
            "matching tuple must throttle: {response:?}"
        );
    } else {
        assert!(
            response.throttle_time_ms == 0,
            "unmatched client has no quota: {response:?}"
        );
    }
}

#[scenario(
    path = "tests/features/migrated/tests/tuple_quota_enforcement.feature",
    name = "tuple quota throttles only matching client id"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tuple_quota_throttles_only_matching_client_id(tuple_quota: TupleQuota) {
    tuple_quota.broker.take().expect("broker").shutdown().await;
    drop(tuple_quota);
}
