//! Tests for the client-quota admin surface: an `AlterClientQuotas` value
//! becomes visible in the metadata image and in `DescribeClientQuotas`, and a
//! principal that is neither a super-user nor ACL-authorized is refused.

use {
    super::{
        cluster::{seed_compat_shim_disable_acl, start_single_broker_sasl_plaintext_with_users},
        quota_admin::{drive_alter_client_quotas_sasl, drive_describe_client_quotas_sasl},
    },
    crate::{
        CLIENT_ID,
        cluster::wait_alice_quota,
        kafka_wire::{
            PlainAuthenticationSetup,
            quotas::{QuotaAlterSetup, QuotaDescribeSetup, QuotaEntity, alter_user_quota},
        },
        support::retry::{all_cluster_authorization_failed, retry_until},
    },
    assert2::assert,
};
type QuotaValues = Vec<(String, f64)>;
struct ClientQuotaAdmin {
    broker: Option<krabka_broker::BrokerHandle>,
    directory: Option<tempfile::TempDir>,
    altered: Option<Vec<(QuotaEntity, i16)>>,
    described: Option<Vec<(QuotaEntity, QuotaValues)>>,
}
#[rstest::fixture]
fn quota_admin() -> ClientQuotaAdmin {
    // Start this scenario with fresh inputs and observations.

    ClientQuotaAdmin {
        broker: None,
        directory: None,
        altered: None,
        described: None,
    }
}
#[rstest_bdd_macros::given(
    "admin is a super user and alice is an ordinary authenticated principal"
)]
async fn quota_admin_principals(quota_admin: &mut ClientQuotaAdmin) {
    let (handle, directory, _) = start_single_broker_sasl_plaintext_with_users("admin", &[
        ("admin", "admin-secret"),
        ("alice", "alice-secret"),
    ])
    .await;

    quota_admin.broker = Some(handle);
    quota_admin.directory = Some(directory);
}
#[rstest_bdd_macros::given("the empty ACL compatibility shortcut is disabled")]
async fn quota_admin_acl_enforced(quota_admin: &mut ClientQuotaAdmin) {
    seed_compat_shim_disable_acl(quota_admin.broker.as_ref().expect("broker started")).await;
}
#[rstest_bdd_macros::when("admin sets alice producer byte rate to 1024")]
async fn quota_admin_set(quota_admin: &mut ClientQuotaAdmin) {
    let addr = quota_admin
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let alter_resp = alter_user_quota()
        .addr(addr)
        .authentication(PlainAuthenticationSetup::admin(CLIENT_ID))
        .call()
        .await;
    quota_admin.altered = Some(alter_resp);
}
#[rstest_bdd_macros::then("the admin quota alteration returns one successful entry")]
fn quota_admin_set_accepted(quota_admin: &ClientQuotaAdmin) {
    let alter_resp = quota_admin
        .altered
        .as_ref()
        .expect("quota alteration attempted");
    assert!(alter_resp.len() == 1, "one entry in response");
    assert!(
        alter_resp[0].1 == 0,
        "alter should succeed; error_code={}",
        alter_resp[0].1
    );
}
#[rstest_bdd_macros::then("alice committed producer byte rate is 1024")]
async fn quota_admin_image(quota_admin: &ClientQuotaAdmin) {
    wait_alice_quota(
        quota_admin.broker.as_ref().expect("broker started"),
        "producer_byte_rate",
        1024.0,
    )
    .await;
}
#[rstest_bdd_macros::when("admin describes quotas for any named user")]
async fn quota_admin_describe(quota_admin: &mut ClientQuotaAdmin) {
    let addr = quota_admin
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let desc = drive_describe_client_quotas_sasl(
        addr,
        vec![("user".into(), 2 /* ANY */, None)],
        &QuotaDescribeSetup {
            authentication: PlainAuthenticationSetup::admin(CLIENT_ID),
            ..Default::default()
        },
    )
    .await;

    quota_admin.described = Some(desc);
}
#[rstest_bdd_macros::then("DescribeClientQuotas includes alice producer byte rate 1024")]
fn quota_admin_described(quota_admin: &ClientQuotaAdmin) {
    let desc = quota_admin.described.as_ref().expect("quotas described");
    let pbr = desc
        .iter()
        .find(|(entity, _)| {
            entity
                .iter()
                .any(|(t, n)| t == "user" && n.as_deref() == Some("alice"))
        })
        .and_then(|(_, values)| {
            values
                .iter()
                .find(|(k, _)| k == "producer_byte_rate")
                .map(|(_, v)| *v)
        });
    assert!(
        pbr == Some(1024.0),
        "expected producer_byte_rate=1024 from describe; got {desc:?}"
    );
}
#[rstest_bdd_macros::when("alice attempts to set her producer byte rate to 999")]
async fn quota_alice_unauthorized_alter(quota_admin: &mut ClientQuotaAdmin) {
    let addr = quota_admin
        .broker
        .as_ref()
        .expect("broker started")
        .listen_addr();
    let resp = retry_until()
        .attempt(|| async {
            drive_alter_client_quotas_sasl(
                addr,
                vec![(vec![("user".into(), Some("alice".into()))], vec![(
                    "producer_byte_rate".into(),
                    999.0,
                    false,
                )])],
                &QuotaAlterSetup {
                    authentication: PlainAuthenticationSetup::with_password(
                        CLIENT_ID,
                        b"alice-secret",
                    ),
                    ..Default::default()
                },
            )
            .await
        })
        .is_complete(all_cluster_authorization_failed)
        .call()
        .await;

    quota_admin.altered = Some(resp);
}
#[rstest_bdd_macros::then(
    "the requested quota entry is refused with cluster authorization error 31"
)]
fn quota_alice_denied(quota_admin: &ClientQuotaAdmin) {
    let resp = quota_admin
        .altered
        .as_ref()
        .expect("quota alteration attempted");
    assert!(resp.len() == 1, "one result for the requested quota entry");
    assert!(
        resp.iter().all(|(_, ec)| *ec == 31),
        "all entries must carry CLUSTER_AUTHORIZATION_FAILED (31); got {resp:?}"
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_quotas/admin_api.feature",
    name = "alter then describe round trip"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_then_describe_round_trip(quota_admin: ClientQuotaAdmin) {
    quota_admin.broker.expect("broker started").shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/client_quotas/admin_api.feature",
    name = "non super user denied"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_denied(quota_admin: ClientQuotaAdmin) {
    quota_admin.broker.expect("broker started").shutdown().await;
}
