//! The authorization gate on `ElectLeaders`: a principal with credentials but
//! no Cluster Alter ACL is denied per requested partition.
//!
//! This is the only scenario in the suite that runs on a `SASL_PLAINTEXT`
//! listener. The authorizer needs a named principal to deny, and the test also
//! seeds one throw-away ACL so the compatibility shim, which allows everything
//! while `image.acls` is empty, is out of the way.

use {
    crate::{
        kafka_wire::PlainAuthenticationSetup,
        sasl::{create_topic_sasl_plain, drive_elect_leaders_sasl_plain},
        support::{
            acl::{topic_acl_record, use_simple_acl_authorizer},
            admin::ElectionSetup,
            retry::{all_cluster_authorization_failed, retry_until},
            sasl::sasl_temp_config,
            topics::ConfiguredTopicSetup,
        },
        wait::wait_partition_exists,
        wire::CLIENT_ID,
    },
    assert2::assert,
    krabka_broker::Broker,
    krabka_security::SaslMechanism,
    std::time::Duration,
    tokio::time,
};

#[derive(Default)]
struct ElectionAuthorizationScenario {
    directory: Option<tempfile::TempDir>,
    handle: Option<krabka_broker::BrokerHandle>,
    response: Option<Vec<(i32, i16)>>,
}
#[rstest::fixture]
fn election_authorization() -> ElectionAuthorizationScenario {
    let directory = None;
    ElectionAuthorizationScenario {
        directory,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "a SASL PLAIN broker grants admin and ANONYMOUS super-user access, authenticates alice without ACLs and holds foo-auth-test partition 0"
)]
async fn denied_election_principal(election_authorization: &mut ElectionAuthorizationScenario) {
    // Build a single-broker SASL_PLAINTEXT config.
    // admin is the super-user so the compat shim stays off once an ACL
    // exists; alice has credentials but no ACLs.
    let (log_dir, mut cfg) = sasl_temp_config(vec![SaslMechanism::Plain]);
    cfg.plain_credentials
        .insert("admin".to_owned(), "admin-secret".to_owned());
    cfg.plain_credentials
        .insert("alice".to_owned(), "alice-secret".to_owned());
    // The controller listener is PLAINTEXT, so this broker's own heartbeat
    // arrives as ANONYMOUS; like a Kafka inter-broker principal it needs
    // `ClusterAction`, or the broker never unfences and places no replica.
    cfg.super_users = ["admin".to_owned(), "ANONYMOUS".to_owned()]
        .into_iter()
        .collect();
    // Install `SimpleAclAuthorizer` so the cluster-Alter gate
    // fires for non-super principals; default is `AllowAllAuthorizer`.
    use_simple_acl_authorizer(&mut cfg);

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // Create the topic as admin (rf=1 fine for a single-broker cluster).
    create_topic_sasl_plain()
        .addr(addr)
        .client_id(CLIENT_ID)
        .topic(ConfiguredTopicSetup {
            name: ("foo-auth-test").into(),
            ..Default::default()
        })
        .call()
        .await;
    wait_partition_exists(&handle, "foo-auth-test", 0).await;

    // Seed a dummy ACL so the compat shim is disabled. The ACL itself
    // is irrelevant — any non-empty `image.acls` flips the shim off and
    // forces the authorizer to evaluate every request.
    handle
        .submit_metadata_record_for_test(
            topic_acl_record()
                .topic("__compat_shim_disable__")
                .principal("User:admin")
                .call(),
        )
        .await
        .expect("seed dummy ACL");

    // `submit_metadata_record_for_test` blocks until the raft entry is
    // committed and the state machine applies it to the image, so the ACL
    // is guaranteed to be in the image before we proceed. A small extra
    // wait absorbs any race on very slow CI runners.
    // intentional: defensive barrier for ACL visibility to the authorizer;
    // no ACL-image awaiter/metric exists, and the retry loop below is the
    // real convergence gate.
    time::sleep(Duration::from_millis(50)).await;

    election_authorization.directory = Some(log_dir);
    election_authorization.handle = Some(handle);
}
#[rstest_bdd_macros::when(
    "alice requests a preferred election of foo-auth-test partition 0 through SASL PLAIN"
)]
async fn request_election_without_cluster_alter(
    election_authorization: &mut ElectionAuthorizationScenario,
) {
    let addr = election_authorization
        .handle
        .as_ref()
        .expect("SASL broker")
        .listen_addr();
    let resp = retry_until()
        .attempt(|| async {
            drive_elect_leaders_sasl_plain(
                addr,
                ElectionSetup {
                    topic: ("foo-auth-test").to_owned(),
                    ..Default::default()
                },
                &PlainAuthenticationSetup::with_password(CLIENT_ID, b"alice-secret"),
            )
            .await
        })
        .is_complete(all_cluster_authorization_failed)
        .call()
        .await;

    election_authorization.response = Some(resp);
}
#[rstest_bdd_macros::then(
    "every requested partition is refused with CLUSTER_AUTHORIZATION_FAILED code 31"
)]
async fn denied_preferred_election(election_authorization: &mut ElectionAuthorizationScenario) {
    assert!(
        election_authorization
            .response
            .as_ref()
            .expect("election response")
            == &[(0, 31)]
    );
    election_authorization
        .handle
        .take()
        .expect("SASL broker")
        .shutdown()
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/controller/election_authorization.feature",
    name = "Authenticated users need Cluster Alter for preferred elections"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_without_acl_denied(election_authorization: ElectionAuthorizationScenario) {}
