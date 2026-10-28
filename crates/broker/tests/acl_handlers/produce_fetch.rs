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

use {
    crate::{
        ERR_TOPIC_AUTHORIZATION_FAILED,
        client_api::{drive_fetch_as_plain, drive_produce_as_plain, single_record_produce_request},
        framing::CLIENT_ID,
        kafka_wire::{PlainAuthenticationSetup, plain_authentication_setup},
        polling::retry_produce_until_allowed,
        sasl_cluster::{start_alice_topic_grant, start_denied_foo},
        support::{
            fetch::{FetchLimits, RequestWaitMillis, named_topic_fetch},
            produce::single_partition_response,
        },
    },
    assert2::assert,
    krabka_protocol::owned::{fetch_response::FetchResponse, produce_response::ProduceResponse},
};

struct TopicDataAccess {
    broker: Option<krabka_broker::BrokerHandle>,
    directory: Option<tempfile::TempDir>,
    produced: Option<ProduceResponse>,
    fetched: Option<FetchResponse>,
}
#[rstest::fixture]
fn data_access() -> TopicDataAccess {
    // Start this scenario with fresh inputs and observations.

    TopicDataAccess {
        broker: None,
        directory: None,
        produced: None,
        fetched: None,
    }
}
#[rstest_bdd_macros::given("foo exists but alice has no topic grants")]
async fn no_topic_grant(data_access: &mut TopicDataAccess) {
    let (broker, directory, _) = start_denied_foo().await;
    data_access.broker = Some(broker);
    data_access.directory = Some(directory);
}
#[rstest_bdd_macros::given("alice has an Allow Write grant on literal topic foo")]
async fn topic_write_grant(data_access: &mut TopicDataAccess) {
    let (broker, directory, _) =
        start_alice_topic_grant(krabka_metadata::AclOperation::Write).await;
    data_access.broker = Some(broker);
    data_access.directory = Some(directory);
}
#[rstest_bdd_macros::when("alice produces hello to foo partition 0 without a grant")]
async fn unauthorized_topic_produce(data_access: &mut TopicDataAccess) {
    let addr = data_access
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    data_access.produced = Some(
        drive_produce_as_plain(
            addr,
            single_record_produce_request("foo", 0, b"hello"),
            &plain_authentication_setup().client_id(CLIENT_ID).call(),
        )
        .await
        .expect("Produce must round-trip"),
    );
}
#[rstest_bdd_macros::when("alice produces to foo after the Write grant applies")]
async fn authorized_topic_produce(data_access: &mut TopicDataAccess) {
    let addr = data_access
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    data_access.produced = Some(
        retry_produce_until_allowed(addr, "alice", b"wonderland", "foo")
            .await
            .expect("Produce must round-trip"),
    );
}
#[rstest_bdd_macros::then("the produced partition reports error code {error}")]
fn topic_produce_result(data_access: &TopicDataAccess, error: i16) {
    let response = data_access.produced.as_ref().expect("Produce attempted");
    let partition = single_partition_response(response);
    assert!(partition.error_code == error, "{partition:?}");
}
#[rstest_bdd_macros::then("foo partition 0 has at least one locally stored record")]
fn produced_record_stored(data_access: &TopicDataAccess) {
    let leo = data_access
        .broker
        .as_ref()
        .expect("broker started")
        .local_log_end_offset("foo", 0)
        .expect("foo-0 must be hosted on this broker");
    assert!(
        leo >= 1,
        "log_end_offset must advance after a successful Produce, got {leo}"
    );
}
#[rstest_bdd_macros::when("alice fetches foo without a Read grant")]
async fn unauthorized_topic_fetch(data_access: &mut TopicDataAccess) {
    let addr = data_access
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let req = named_topic_fetch("foo", FetchLimits::one_mebibyte(RequestWaitMillis(0)));
    data_access.fetched = Some(
        drive_fetch_as_plain(addr, req, &PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            ..Default::default()
        })
        .await
        .expect("Fetch must round-trip"),
    );
}
#[rstest_bdd_macros::then(
    "Fetch returns one topic and partition with topic authorization error 29"
)]
fn topic_fetch_denied(data_access: &TopicDataAccess) {
    let resp = data_access.fetched.as_ref().expect("Fetch attempted");
    assert!(resp.responses.len() == 1, "one topic in response");
    assert!(
        resp.responses[0].partitions.len() == 1,
        "one partition row in response"
    );
    let partition = &resp.responses[0].partitions[0];
    assert!(
        partition.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "{partition:?}"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/produce_fetch.feature",
    name = "produce denied without topic acl"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_denied_without_topic_acl(data_access: TopicDataAccess) {
    data_access.broker.expect("broker started").shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/produce_fetch.feature",
    name = "produce allowed with topic write acl"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_allowed_with_topic_write_acl(data_access: TopicDataAccess) {
    data_access.broker.expect("broker started").shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/produce_fetch.feature",
    name = "fetch denied without topic read acl"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_denied_without_topic_read_acl(data_access: TopicDataAccess) {
    data_access.broker.expect("broker started").shutdown().await;
}
