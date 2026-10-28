//! `Metadata`, `JoinGroup`, and `InitProducerId` enforcement (T24): the Topic,
//! Group, and `TransactionalId` resource types each gate a different request,
//! and `Metadata` gates one of two ways depending on how the client asks.
//!
//! Each test boots a fresh single-broker `SASL_PLAINTEXT` cluster with admin
//! as the super-user. The test seeds whatever ACL records the scenario
//! requires via the controller-direct test helper (which keeps the compat
//! shim off because at least one ACL exists), then alice authenticates
//! separately and drives the typed request.

use {
    crate::{
        ERR_GROUP_AUTHORIZATION_FAILED, ERR_MEMBER_ID_REQUIRED, ERR_TOPIC_AUTHORIZATION_FAILED,
        ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        acl_admin::create_topic_as_admin,
        client_api::{
            drive_init_producer_id_as_plain, drive_join_group_as_plain, join_group_request,
        },
        framing::CLIENT_ID,
        kafka_wire::PlainAuthenticationSetup,
        polling::{
            named_metadata_as_alice, retry_join_group_until_allowed,
            retry_metadata_until_topic_visible,
        },
        sasl_cluster::start_admin_alice,
        support::{
            acl::{seed_dummy_acl, topic_acl_record},
            transactions::init_producer_request,
        },
    },
    assert2::assert,
    krabka_protocol::owned::{
        init_producer_id_response::InitProducerIdResponse, join_group_response::JoinGroupResponse,
        metadata_response::MetadataResponse,
    },
};

/// A running broker with its data directory and listen address.
struct AliceMetadataFixture {
    handle: krabka_broker::BrokerHandle,
    dir: tempfile::TempDir,
}

/// Only t1 is visible to Alice, with a real ACL image disabling the compat shim.
async fn alice_metadata_fixture() -> AliceMetadataFixture {
    let (handle, dir, _) = start_admin_alice().await;
    let addr = handle.listen_addr();
    create_topic_as_admin(addr, "t1", 1).await;
    create_topic_as_admin(addr, "t2", 1).await;
    handle
        .submit_metadata_record_for_test(
            topic_acl_record()
                .topic("t1")
                .operation(krabka_metadata::AclOperation::Describe)
                .call(),
        )
        .await
        .expect("seed Describe-on-t1 ACL for alice");
    AliceMetadataFixture { handle, dir }
}

struct ResourceAuthorization {
    broker: Option<krabka_broker::BrokerHandle>,
    directory: Option<tempfile::TempDir>,
    metadata: Option<MetadataResponse>,
    joined: Option<JoinGroupResponse>,
    initialized: Option<InitProducerIdResponse>,
}
#[rstest::fixture]
fn authorization() -> ResourceAuthorization {
    // Start this scenario with fresh inputs and observations.

    ResourceAuthorization {
        broker: None,
        directory: None,
        metadata: None,
        joined: None,
        initialized: None,
    }
}
#[rstest_bdd_macros::given("topics t1 and t2 exist but alice may Describe only t1")]
async fn alice_partial_metadata(authorization: &mut ResourceAuthorization) {
    let AliceMetadataFixture { handle, dir } = alice_metadata_fixture().await;
    authorization.broker = Some(handle);
    authorization.directory = Some(dir);
}
#[rstest_bdd_macros::when("alice requests Metadata for all topics")]
async fn alice_metadata_0(authorization: &mut ResourceAuthorization) {
    let addr = authorization
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let resp = retry_metadata_until_topic_visible(addr, "alice", b"wonderland", "t1", None)
        .await
        .expect("Metadata must round-trip");
    authorization.metadata = Some(resp);
}
#[rstest_bdd_macros::then("all-topic Metadata silently includes t1 and excludes t2")]
fn alice_metadata_result_0(authorization: &ResourceAuthorization) {
    let resp = authorization.metadata.as_ref().expect("Metadata requested");
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
#[rstest_bdd_macros::when("alice requests Metadata explicitly for t2")]
async fn alice_metadata_1(authorization: &mut ResourceAuthorization) {
    let addr = authorization
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let resp = named_metadata_as_alice(addr, "t2").await;
    authorization.metadata = Some(resp);
}
#[rstest_bdd_macros::then("named Metadata includes only t2 with topic authorization error 29")]
fn alice_metadata_result_1(authorization: &ResourceAuthorization) {
    let resp = authorization.metadata.as_ref().expect("Metadata requested");
    assert!(resp.topics.len() == 1, "one topic row in response");
    let row = &resp.topics[0];
    assert!(row.name.as_deref() == Some("t2"));
    assert!(
        row.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "alice has no ACL on t2, expected TOPIC_AUTHORIZATION_FAILED (29), got {row:?}"
    );
}
#[rstest_bdd_macros::given(
    "alice has no group or transactional grants and ACL authorization is active"
)]
async fn alice_resource_denied(authorization: &mut ResourceAuthorization) {
    let (handle, directory, _) = start_admin_alice().await;
    handle.wait_until_group_coordinator_ready().await;
    seed_dummy_acl(&handle).await;
    authorization.broker = Some(handle);
    authorization.directory = Some(directory);
}
#[rstest_bdd_macros::when("alice joins cg-1 without a member id")]
async fn alice_group_join_denied(authorization: &mut ResourceAuthorization) {
    let addr = authorization
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let denied = drive_join_group_as_plain(
        addr,
        join_group_request("cg-1"),
        &PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            ..Default::default()
        },
    )
    .await
    .expect("JoinGroup must round-trip");
    authorization.joined = Some(denied);
}
#[rstest_bdd_macros::then("JoinGroup is denied with group authorization error 30")]
fn alice_group_denial(authorization: &ResourceAuthorization) {
    let denied = authorization.joined.as_ref().expect("JoinGroup attempted");
    assert!(
        denied.error_code == ERR_GROUP_AUTHORIZATION_FAILED,
        "alice has no Group Read on cg-1, expected GROUP_AUTHORIZATION_FAILED (30), got {denied:?}"
    );
}
#[rstest_bdd_macros::when("alice is granted Read on literal group cg-1")]
async fn grant_alice_group_read(authorization: &mut ResourceAuthorization) {
    authorization
        .broker
        .as_ref()
        .expect("broker started")
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
}
#[rstest_bdd_macros::when("alice retries joining cg-1 after the group grant applies")]
async fn alice_group_bootstrap(authorization: &mut ResourceAuthorization) {
    let addr = authorization
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let bootstrap = retry_join_group_until_allowed(addr, "alice", b"wonderland", "cg-1")
        .await
        .expect("JoinGroup retry must round-trip");
    authorization.joined = Some(bootstrap);
}
#[rstest_bdd_macros::then("JoinGroup requires a nonempty broker generated member id with error 79")]
fn alice_group_member_required(authorization: &ResourceAuthorization) {
    let bootstrap = authorization.joined.as_ref().expect("JoinGroup retried");
    assert!(
        bootstrap.error_code == ERR_MEMBER_ID_REQUIRED,
        "first authorized JoinGroup must return MEMBER_ID_REQUIRED (79) with a generated member_id, got {bootstrap:?}"
    );
    assert!(
        !bootstrap.member_id.is_empty(),
        "broker must return a non-empty generated member_id on MEMBER_ID_REQUIRED"
    );
}
#[rstest_bdd_macros::when("alice joins cg-1 with the assigned member id")]
async fn alice_group_join_authorized(authorization: &mut ResourceAuthorization) {
    let addr = authorization
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let mut req2 = join_group_request("cg-1");
    req2.member_id = authorization
        .joined
        .as_ref()
        .expect("member id assigned")
        .member_id
        .clone();
    let joined = drive_join_group_as_plain(addr, req2, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        ..Default::default()
    })
    .await
    .expect("second JoinGroup must round-trip");
    authorization.joined = Some(joined);
}
#[rstest_bdd_macros::then("alice completes the group rebalance without error")]
fn alice_group_join_succeeded(authorization: &ResourceAuthorization) {
    let joined = authorization.joined.as_ref().expect("member joined");
    assert!(
        joined.error_code == 0,
        "JoinGroup must succeed with alice's Group Read ACL on cg-1, got {joined:?}"
    );
}
#[rstest_bdd_macros::when("alice initializes transactional producer tx-1")]
async fn alice_transactional_producer(authorization: &mut ResourceAuthorization) {
    let addr = authorization
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let req = init_producer_request()
        .maybe_transactional_id(Some("tx-1".to_owned()))
        .call();
    let resp = drive_init_producer_id_as_plain(addr, req, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        ..Default::default()
    })
    .await
    .expect("InitProducerId must round-trip");
    authorization.initialized = Some(resp);
}
#[rstest_bdd_macros::then("InitProducerId is denied with transactional id authorization error 53")]
fn alice_transactional_denial(authorization: &ResourceAuthorization) {
    let resp = authorization
        .initialized
        .as_ref()
        .expect("producer initialization attempted");
    assert!(
        resp.error_code == ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        "alice has no TransactionalId Write ACL on tx-1, expected TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53), got {resp:?}"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/metadata_group_txn.feature",
    name = "metadata silent filter on fetch all"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_silent_filter_on_fetch_all(authorization: ResourceAuthorization) {
    authorization
        .broker
        .expect("broker started")
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/metadata_group_txn.feature",
    name = "metadata explicit deny on named topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_explicit_deny_on_named_topic(authorization: ResourceAuthorization) {
    authorization
        .broker
        .expect("broker started")
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/metadata_group_txn.feature",
    name = "join group denied without group read acl"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_group_denied_without_group_read_acl(authorization: ResourceAuthorization) {
    authorization
        .broker
        .expect("broker started")
        .shutdown()
        .await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/metadata_group_txn.feature",
    name = "init producer id denied without txn acl"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_producer_id_denied_without_txn_acl(authorization: ResourceAuthorization) {
    authorization
        .broker
        .expect("broker started")
        .shutdown()
        .await;
}
