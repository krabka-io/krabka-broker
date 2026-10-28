//! Principal handling for the `super_users` set: a `CreateAcls` request from
//! any principal in the set succeeds, while a principal outside it still
//! gets `CLUSTER_AUTHORIZATION_FAILED` on every binding.

use {
    crate::{
        OPERATION_READ, OPERATION_WRITE,
        acl_admin::{drive_create_acls_as_plain, topic_allow_creation},
        framing::CLIENT_ID,
        kafka_wire::PlainAuthenticationSetup,
        sasl_cluster::sasl_plain_broker_config_multi_super,
    },
    assert2::assert,
    krabka_broker::Broker,
    krabka_protocol::owned::{
        create_acls_request::CreateAclsRequest, create_acls_response::CreateAclsResponse,
    },
};

struct SuperUserProvisioning {
    directory: Option<tempfile::TempDir>,
    broker: Option<krabka_broker::BrokerHandle>,
    response: Option<CreateAclsResponse>,
}
#[rstest::fixture]
fn provisioning() -> SuperUserProvisioning {
    // Start this scenario with fresh inputs and observations.

    SuperUserProvisioning {
        directory: None,
        broker: None,
        response: None,
    }
}
#[rstest_bdd_macros::given(
    "admin and ops-bot are super users while credentialed alice has no cluster grants"
)]
async fn multiple_super_users(provisioning: &mut SuperUserProvisioning) {
    let log_dir = tempfile::tempdir().unwrap();
    // Two super-users: admin + ops-bot. alice has PLAIN creds but is NOT
    // in the super-users set, so her CreateAcls must hit the cluster gate.
    let cfg = sasl_plain_broker_config_multi_super(
        log_dir.path(),
        &[
            ("admin", "admin-secret"),
            ("ops-bot", "ops-secret"),
            ("alice", "wonderland"),
        ],
        &["admin", "ops-bot"],
    );

    let handle = Broker::start(cfg).await.expect("broker must start");

    provisioning.directory = Some(log_dir);
    provisioning.broker = Some(handle);
}
#[rstest_bdd_macros::when("admin provisions a topic binding")]
async fn admin_provisions(provisioning: &mut SuperUserProvisioning) {
    let addr = provisioning
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let admin_req = CreateAclsRequest {
        creations: vec![topic_allow_creation("t-admin", "User:bob", OPERATION_READ)],
        ..Default::default()
    };
    let admin_resp =
        drive_create_acls_as_plain(addr, admin_req, &PlainAuthenticationSetup::admin(CLIENT_ID))
            .await
            .expect("CreateAcls as admin must round-trip");

    provisioning.response = Some(admin_resp);
}
#[rstest_bdd_macros::when("ops-bot provisions a topic binding")]
async fn ops_bot_provisions(provisioning: &mut SuperUserProvisioning) {
    let addr = provisioning
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let ops_req = CreateAclsRequest {
        creations: vec![topic_allow_creation("t-ops", "User:carol", OPERATION_WRITE)],
        ..Default::default()
    };
    let ops_resp = drive_create_acls_as_plain(addr, ops_req, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        user: "ops-bot",
        password: (b"ops-secret").to_vec(),
    })
    .await
    .expect("CreateAcls as ops-bot must round-trip");

    provisioning.response = Some(ops_resp);
}
#[rstest_bdd_macros::when("alice provisions two topic bindings")]
async fn alice_provisions(provisioning: &mut SuperUserProvisioning) {
    let addr = provisioning
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let alice_req = CreateAclsRequest {
        creations: vec![
            topic_allow_creation("t-x", "User:dave", OPERATION_READ),
            topic_allow_creation("t-y", "User:eve", OPERATION_WRITE),
        ],
        ..Default::default()
    };
    let alice_resp = drive_create_acls_as_plain(addr, alice_req, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        ..Default::default()
    })
    .await
    .expect("CreateAcls request must round-trip even when denied");

    provisioning.response = Some(alice_resp);
}
#[rstest_bdd_macros::then("all {count} ACL binding results have error code {error}")]
fn provisioning_results(provisioning: &SuperUserProvisioning, count: usize, error: i16) {
    let response = provisioning
        .response
        .as_ref()
        .expect("CreateAcls requested");
    assert!(response.results.len() == count);
    for (index, result) in response.results.iter().enumerate() {
        assert!(result.error_code == error, "binding {index}: {result:?}");
    }
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/super_users.feature",
    name = "multi super user both can provision"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_super_user_both_can_provision(provisioning: SuperUserProvisioning) {
    provisioning
        .broker
        .expect("broker started")
        .shutdown()
        .await;
}
