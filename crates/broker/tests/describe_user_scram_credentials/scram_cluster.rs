//! Boot of the single-broker `SASL_PLAINTEXT` cluster the tests describe
//! credentials against, together with the metadata records they seed into it.
//!
//! The seeding helpers go through `submit_metadata_record_for_test` rather than
//! the `AlterUserScramCredentials` wire path, which keeps every test in this
//! suite focused on the read half of KIP-554.

use {
    crate::support::{
        acl::{cluster_acl_record, use_simple_acl_authorizer},
        sasl::{sasl_temp_config, start_broker},
    },
    krabka_broker::BrokerHandle,
    krabka_metadata::{AclOperation, MetadataRecord, ResourceType},
    krabka_security::SaslMechanism,
    std::{net::SocketAddr, pin::Pin},
    tempfile::TempDir,
};

pub fn admin_test_password() -> String {
    ['a', 'd', 'm', 'i', 'n', '-', 's', 'e', 'c', 'r', 'e', 't']
        .iter()
        .collect()
}

pub fn alice_test_password() -> String {
    ['a', 'l', 'i', 'c', 'e', '-', 's', 'e', 'c', 'r', 'e', 't']
        .iter()
        .collect()
}

/// Start a single-broker SASL/PLAINTEXT cluster.
/// Returns `(handle, _dir, addr)`.
pub type BrokerStartup = Pin<Box<dyn Future<Output = (BrokerHandle, TempDir, SocketAddr)>>>;

pub fn start_single_broker_sasl_plaintext_with_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> BrokerStartup {
    let super_users = [super_user];
    start_single_broker_sasl_plaintext_with_acl_authorizer(&super_users, users)
}

pub fn start_single_broker_sasl_plaintext_with_acl_authorizer(
    super_users: &[&str],
    users: &[(&str, &str)],
) -> BrokerStartup {
    let (log_dir, mut cfg) = sasl_temp_config(vec![SaslMechanism::Plain]);
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_owned(), (*pass).to_owned());
    }
    cfg.super_users = super_users.iter().map(|user| (*user).to_owned()).collect();
    use_simple_acl_authorizer(&mut cfg);

    Box::pin(start_broker(cfg, log_dir))
}

pub async fn seed_cluster_acl(handle: &BrokerHandle, principal: &str, operation: AclOperation) {
    handle
        .submit_metadata_record_for_test(
            cluster_acl_record()
                .principal(&format!("User:{principal}"))
                .operation(operation)
                .call(),
        )
        .await
        .expect("seed cluster ACL");
    handle
        .wait_for_image(|img| {
            img.matching_acls(ResourceType::Cluster, "kafka-cluster")
                .any(|entry| entry.principal == format!("User:{principal}"))
        })
        .await;
}

pub async fn seed_scram_credential(
    handle: &BrokerHandle,
    user: &str,
    mechanism: SaslMechanism,
    iterations: u32,
) {
    handle
        .submit_metadata_record_for_test(MetadataRecord::V1ScramCredential(
            krabka_metadata::ScramCredentialRecord {
                user: user.into(),
                mechanism,
                iterations,
                salt: vec![1, 2, 3, 4],
                server_key: vec![5; 64],
                stored_key: vec![6; 64],
            },
        ))
        .await
        .expect("seed SCRAM credential");
    handle
        .wait_for_image(|img| !img.scram_credentials_for_user(user).is_empty())
        .await;
}

use {
    crate::scram_driver::drive_describe_user_scram_credentials_sasl,
    std::time::{SystemTime, UNIX_EPOCH},
};
pub type ScramUserRows = Vec<(String, i16, Vec<(i8, i32)>)>;
pub struct ScramDescriptions {
    pub broker: Option<BrokerHandle>,
    pub directory: Option<TempDir>,
    pub principal: String,
    pub password: String,
    pub response: Option<(i16, ScramUserRows)>,
}
impl ScramDescriptions {
    fn retain_principal(
        &mut self,
        principal: &str,
        password: String,
        (broker, directory, _): (BrokerHandle, TempDir, SocketAddr),
    ) {
        self.broker = Some(broker);
        self.directory = Some(directory);
        self.principal = principal.to_owned();
        self.password = password;
    }

    pub async fn shutdown(self) {
        self.broker
            .expect("SCRAM description broker started")
            .shutdown()
            .await;
    }
}
#[rstest::fixture]
pub fn scram() -> ScramDescriptions {
    // Start this scenario with fresh inputs and observations.

    ScramDescriptions {
        broker: None,
        directory: None,
        principal: String::new(),
        password: String::new(),
        response: None,
    }
}
#[rstest_bdd_macros::given("admin is a super user authenticated with its test password")]
async fn scram_description_admin(scram: &mut ScramDescriptions) {
    let password = admin_test_password();
    let started =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", &password)]).await;
    scram.retain_principal("admin", password, started);
}
#[rstest_bdd_macros::given("admin is a super user authenticated with a timestamped password")]
async fn scram_description_unique_admin(scram: &mut ScramDescriptions) {
    let password = format!(
        "admin-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before UNIX_EPOCH")
            .as_nanos()
    );
    let started =
        start_single_broker_sasl_plaintext_with_users("admin", &[("admin", &password)]).await;
    scram.retain_principal("admin", password, started);
}
#[rstest_bdd_macros::given(
    "alice authenticates with her test password and no principal is a super user"
)]
async fn scram_description_alice(scram: &mut ScramDescriptions) {
    let password = alice_test_password();
    let started =
        start_single_broker_sasl_plaintext_with_acl_authorizer(&[], &[("alice", &password)]).await;
    scram.retain_principal("alice", password, started);
}
#[rstest_bdd_macros::given(
    "alice authenticates with a fresh UUID password and no principal is a super user"
)]
async fn scram_description_unique_alice(scram: &mut ScramDescriptions) {
    let password = uuid::Uuid::new_v4().to_string();
    let started =
        start_single_broker_sasl_plaintext_with_acl_authorizer(&[], &[("alice", &password)]).await;
    scram.retain_principal("alice", password, started);
}
#[rstest_bdd_macros::given("alice has cluster Describe permission")]
async fn scram_description_cluster_acl(scram: &mut ScramDescriptions) {
    seed_cluster_acl(
        scram
            .broker
            .as_ref()
            .expect("SCRAM description broker started"),
        "alice",
        AclOperation::Describe,
    )
    .await;
}
#[rstest_bdd_macros::given("user {user} has a SHA 512 SCRAM credential at {iterations} iterations")]
async fn scram_description_seed(scram: &mut ScramDescriptions, user: String, iterations: u32) {
    seed_scram_credential(
        scram
            .broker
            .as_ref()
            .expect("SCRAM description broker started"),
        &user,
        SaslMechanism::ScramSha512,
        iterations,
    )
    .await;
}
#[rstest_bdd_macros::when("the authenticated principal describes SCRAM users {users}")]
async fn describe_scram_users(scram: &mut ScramDescriptions, users: String) {
    let filter = if users == "all" {
        None
    } else {
        Some(users.split(',').map(str::to_owned).collect())
    };
    scram.response = Some(
        drive_describe_user_scram_credentials_sasl(
            scram
                .broker
                .as_ref()
                .expect("SCRAM description broker started")
                .listen_addr(),
            &scram.principal,
            &scram.password,
            filter,
        )
        .await,
    );
}
#[rstest_bdd_macros::then("the SCRAM description has top level error {error}")]
fn scram_description_error(scram: &ScramDescriptions, error: i16) {
    assert2::assert!(scram.response.as_ref().expect("SCRAM users described").0 == error);
}
#[rstest_bdd_macros::then("the SCRAM description has no per-user rows")]
fn no_scram_description_users(scram: &ScramDescriptions) {
    assert2::assert!(
        scram
            .response
            .as_ref()
            .expect("SCRAM users described")
            .1
            .is_empty()
    );
}
