//! The `CreateAcls` / `DescribeAcls` / `DeleteAcls` admin flow over a real
//! `SASL_PLAINTEXT` listener: a super-user provisions bindings and reads
//! them back, a targeted filter deletes exactly one of them, and a
//! non-super principal is refused once per binding.

use {
    crate::{
        OPERATION_READ, OPERATION_WRITE, PATTERN_TYPE_LITERAL, PERMISSION_ALLOW,
        RESOURCE_TYPE_TOPIC,
        acl_admin::{
            describe_all_topic_acls, drive_create_acls_as_plain, drive_delete_acls_as_plain,
            drive_describe_acls_as_plain, topic_allow_creation,
        },
        framing::CLIENT_ID,
        kafka_wire::PlainAuthenticationSetup,
        sasl_cluster::{sasl_plain_broker_config, start_admin_with},
    },
    assert2::{assert, check},
    krabka_broker::Broker,
    krabka_protocol::owned::{
        create_acls_request::CreateAclsRequest,
        create_acls_response::CreateAclsResponse,
        delete_acls_request::{DeleteAclsFilter, DeleteAclsRequest},
        delete_acls_response::DeleteAclsResponse,
        describe_acls_response::DescribeAclsResponse,
    },
};

struct AclProvisioning {
    directory: Option<tempfile::TempDir>,
    broker: Option<krabka_broker::BrokerHandle>,
    created: Option<CreateAclsResponse>,
    described: Option<DescribeAclsResponse>,
    deleted: Option<DeleteAclsResponse>,
}
#[rstest::fixture]
fn acls() -> AclProvisioning {
    // Start this scenario with fresh inputs and observations.

    AclProvisioning {
        directory: None,
        broker: None,
        created: None,
        described: None,
        deleted: None,
    }
}
#[rstest_bdd_macros::given("a super user admin on the SASL listener")]
async fn acl_admin_listener(acls: &mut AclProvisioning) {
    let (directory, handle, _) = start_admin_with(&[("admin", "admin-secret")]).await;
    acls.directory = Some(directory);
    acls.broker = Some(handle);
}
#[rstest_bdd_macros::when("admin creates a Read grant on foo for alice")]
async fn acl_create_read(acls: &mut AclProvisioning) {
    let addr = acls.broker.as_ref().expect("broker started").listen_addr();
    // Provision: Allow Read on Topic LITERAL "foo" for User:alice from *.
    let create_req = CreateAclsRequest {
        creations: vec![topic_allow_creation("foo", "User:alice", OPERATION_READ)],
        ..Default::default()
    };
    let create_resp = drive_create_acls_as_plain(
        addr,
        create_req,
        &PlainAuthenticationSetup::admin(CLIENT_ID),
    )
    .await
    .expect("CreateAcls as super-user must succeed");
    acls.created = Some(create_resp);
}
#[rstest_bdd_macros::then("the Read grant creation returns one successful binding")]
fn acl_read_created(acls: &AclProvisioning) {
    let create_resp = acls.created.as_ref().expect("creation attempted");
    assert!(
        create_resp.results.len() == 1,
        "one result per creation: {create_resp:?}"
    );
    assert!(
        create_resp.results[0].error_code == 0,
        "super-user creation must return error_code=0, got {:?}",
        create_resp.results[0]
    );
}
#[rstest_bdd_macros::when("admin describes all topic ACLs")]
async fn acl_describe_topics(acls: &mut AclProvisioning) {
    let addr = acls.broker.as_ref().expect("broker started").listen_addr();
    let describe_resp = drive_describe_acls_as_plain(
        addr,
        describe_all_topic_acls(),
        &PlainAuthenticationSetup::admin(CLIENT_ID),
    )
    .await
    .expect("DescribeAcls as super-user must succeed");
    acls.described = Some(describe_resp);
}
#[rstest_bdd_macros::then("the only ACL is alice Read Allow on literal foo from any host")]
fn acl_only_alice_read(acls: &AclProvisioning) {
    let describe_resp = acls.described.as_ref().expect("ACLs described");
    assert!(
        describe_resp.error_code == 0,
        "DescribeAcls must succeed, got {describe_resp:?}"
    );
    assert!(
        describe_resp.resources.len() == 1,
        "expected exactly one matching resource, got {:?}",
        describe_resp.resources
    );
    let resource = &describe_resp.resources[0];
    check!(resource.resource_type == RESOURCE_TYPE_TOPIC);
    check!(resource.resource_name == "foo");
    check!(resource.pattern_type == PATTERN_TYPE_LITERAL);
    assert!(
        resource.acls.len() == 1,
        "expected exactly one ACL description, got {:?}",
        resource.acls
    );
    let acl = &resource.acls[0];
    check!(acl.principal == "User:alice");
    check!(acl.host == "*");
    check!(acl.operation == OPERATION_READ);
    check!(acl.permission_type == PERMISSION_ALLOW);
}
#[rstest_bdd_macros::given("alice is credentialed but only admin is a super user")]
async fn acl_unprivileged_listener(acls: &mut AclProvisioning) {
    let log_dir = tempfile::tempdir().unwrap();
    // alice is NOT the super-user. admin is configured as super-user so
    // the compat shim stays off and the cluster-Alter gate applies.
    let cfg = sasl_plain_broker_config(
        log_dir.path(),
        &[("admin", "admin-secret"), ("alice", "wonderland")],
        Some("admin"),
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    acls.directory = Some(log_dir);
    acls.broker = Some(handle);
}
#[rstest_bdd_macros::when("alice creates Read on foo and Write on bar for other principals")]
async fn acl_unprivileged_create(acls: &mut AclProvisioning) {
    let addr = acls.broker.as_ref().expect("broker started").listen_addr();
    let req = CreateAclsRequest {
        creations: vec![
            topic_allow_creation("foo", "User:bob", OPERATION_READ),
            topic_allow_creation("bar", "User:carol", OPERATION_WRITE),
        ],
        ..Default::default()
    };
    let resp = drive_create_acls_as_plain(addr, req, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        ..Default::default()
    })
    .await
    .expect("CreateAcls request must round-trip even when denied");
    acls.created = Some(resp);
}
#[rstest_bdd_macros::then("both requested bindings are denied with cluster authorization error 31")]
fn acl_unprivileged_denied(acls: &AclProvisioning) {
    let resp = acls.created.as_ref().expect("creation attempted");
    assert!(resp.results.len() == 2, "one result row per creation");
    for (i, r) in resp.results.iter().enumerate() {
        assert!(
            r.error_code == 31, /* CLUSTER_AUTHORIZATION_FAILED */
            "binding {i} must be denied with CLUSTER_AUTHORIZATION_FAILED, got {r:?}"
        );
    }
}
#[rstest_bdd_macros::given("admin has provisioned alice Read on foo and Write on bar")]
async fn acl_two_existing(acls: &mut AclProvisioning) {
    let addr = acls.broker.as_ref().expect("broker started").listen_addr();
    // Provision two ACLs (Read on "foo", Write on "bar").
    let create_req = CreateAclsRequest {
        creations: vec![
            topic_allow_creation("foo", "User:alice", OPERATION_READ),
            topic_allow_creation("bar", "User:alice", OPERATION_WRITE),
        ],
        ..Default::default()
    };
    let create_resp = drive_create_acls_as_plain(
        addr,
        create_req,
        &PlainAuthenticationSetup::admin(CLIENT_ID),
    )
    .await
    .expect("provisioning CreateAcls must succeed");
    assert!(create_resp.results.len() == 2);
    for r in &create_resp.results {
        assert!(r.error_code == 0, "provisioning must succeed, got {r:?}");
    }
}
#[rstest_bdd_macros::when("admin deletes only alice Read Allow on literal foo from any host")]
async fn acl_targeted_delete(acls: &mut AclProvisioning) {
    let addr = acls.broker.as_ref().expect("broker started").listen_addr();
    let delete_req = DeleteAclsRequest {
        filters: vec![DeleteAclsFilter {
            resource_type_filter: RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some("foo".to_owned()),
            pattern_type_filter: PATTERN_TYPE_LITERAL,
            principal_filter: Some("User:alice".to_owned()),
            host_filter: Some("*".to_owned()),
            operation: OPERATION_READ,
            permission_type: PERMISSION_ALLOW,
            ..Default::default()
        }],
        ..Default::default()
    };
    let delete_resp = drive_delete_acls_as_plain(
        addr,
        delete_req,
        &PlainAuthenticationSetup::admin(CLIENT_ID),
    )
    .await
    .expect("DeleteAcls must succeed");
    acls.deleted = Some(delete_resp);
}
#[rstest_bdd_macros::then("the precise delete returns exactly the successful Read binding on foo")]
fn acl_targeted_deleted(acls: &AclProvisioning) {
    let delete_resp = acls.deleted.as_ref().expect("deletion attempted");
    assert!(
        delete_resp.filter_results.len() == 1,
        "one filter result row per filter"
    );
    assert!(
        delete_resp.filter_results[0].error_code == 0,
        "filter must succeed, got {:?}",
        delete_resp.filter_results[0]
    );
    let matching = &delete_resp.filter_results[0].matching_acls;
    assert!(
        matching.len() == 1,
        "exactly one ACL must match the precise filter, got {matching:?}"
    );
    check!(matching[0].resource_name == "foo");
    check!(matching[0].operation == OPERATION_READ);
    check!(matching[0].error_code == 0);
}
#[derive(Debug, PartialEq)]
struct SurvivingBinding {
    resource_name: String,
    operation: i8,
    permission_type: i8,
}
#[rstest_bdd_macros::then("only alice Write Allow on bar survives the targeted deletion")]
fn acl_write_survives(acls: &AclProvisioning) {
    let describe_resp = acls.described.as_ref().expect("ACLs described");
    assert!(describe_resp.error_code == 0);
    // Flatten all (resource, acl) pairs so the assertion doesn't depend
    // on whether the broker groups by resource or emits one resource per
    // ACL — the contract is "the deleted binding is gone, the other one
    // is still there".
    let mut surviving: Vec<SurvivingBinding> = Vec::new();
    for r in &describe_resp.resources {
        for a in &r.acls {
            surviving.push(SurvivingBinding {
                resource_name: r.resource_name.clone(),
                operation: a.operation,
                permission_type: a.permission_type,
            });
        }
    }
    assert!(
        surviving.len() == 1,
        "exactly one binding must remain, got {surviving:?}"
    );
    assert!(
        surviving[0]
            == SurvivingBinding {
                resource_name: "bar".to_owned(),
                operation: OPERATION_WRITE,
                permission_type: PERMISSION_ALLOW,
            },
        "the surviving binding must be Write-on-bar, got {:?}",
        surviving[0]
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/provisioning.feature",
    name = "create acls super user can provision and describe"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_acls_super_user_can_provision_and_describe(acls: AclProvisioning) {
    acls.broker.expect("broker started").shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/provisioning.feature",
    name = "create acls non super user rejected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn create_acls_non_super_user_rejected(acls: AclProvisioning) {
    acls.broker.expect("broker started").shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/acl_handlers/provisioning.feature",
    name = "delete acls removes matching"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_acls_removes_matching(acls: AclProvisioning) {
    acls.broker.expect("broker started").shutdown().await;
}
