//! `AlterUserScramCredentials` (KIP-554) request validation.
//!
//! The cases here drive the per-row error codes: iteration counts below and
//! above the accepted range, an unknown mechanism byte, two rows for one
//! username, a deletion and an upsertion for the same username, and a
//! deletion whose target credential does not exist.

use {
    crate::{
        alter_scram::{
            KAFKA_MAX_SCRAM_ITERATIONS, SaltedPassword, WIRE_MECH_SCRAM_SHA_256,
            WIRE_MECH_SCRAM_SHA_512, drive_alter_user_scram_credentials_as_plain,
            pbkdf2_salt_and_salted, provision_as_admin,
        },
        harness::{
            ListeningBroker, RetainedBroker, alice_password, scram_admin_fixture, start_scram_admin,
        },
        support::{
            sasl::{ScramAlgorithm, ScramMechanismCode, scram_upsertion},
            sasl_plaintext_config,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::{Broker, BrokerHandle},
    krabka_metadata::{MetadataRecord, ScramCredentialRecord},
    krabka_protocol::owned::{
        alter_user_scram_credentials_request::{
            AlterUserScramCredentialsRequest, ScramCredentialDeletion,
        },
        alter_user_scram_credentials_response::AlterUserScramCredentialsResponse,
    },
    krabka_security::SaslMechanism,
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenario, then, when},
    std::time::{SystemTime, UNIX_EPOCH},
    tempfile::TempDir,
};

struct ScramChange {
    directory: Option<TempDir>,
    broker: Option<BrokerHandle>,
    password: Option<String>,
    request: Option<AlterUserScramCredentialsRequest>,
    response: Option<AlterUserScramCredentialsResponse>,
}

#[fixture]
fn credentials() -> ScramChange {
    // Start this scenario with fresh inputs and observations.

    ScramChange {
        directory: None,
        broker: None,
        password: None,
        request: None,
        response: None,
    }
}

#[given("a PLAIN-authenticated SCRAM superuser")]
async fn scram_superuser(credentials: &mut ScramChange) {
    let RetainedBroker { log_dir, handle } = scram_admin_fixture(vec![SaslMechanism::Plain]).await;
    credentials.directory = Some(log_dir);
    credentials.broker = Some(handle);
}

#[given("a PLAIN-authenticated SCRAM superuser with a unique password")]
async fn unique_scram_superuser(credentials: &mut ScramChange) {
    let RandomScramAdmin {
        log_dir,
        admin_password,
        handle,
    } = random_scram_admin_fixture().await;
    credentials.directory = Some(log_dir);
    credentials.password = Some(admin_password);
    credentials.broker = Some(handle);
}

#[given("a PLAIN-authenticated SCRAM superuser with a timestamp password")]
async fn timestamp_scram_superuser(credentials: &mut ScramChange) {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = sasl_plaintext_config(log_dir.path().to_path_buf());
    let admin_password = format!(
        "test-pass-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("admin".to_owned(), admin_password.clone());
    cfg.super_users = maplit::hashset! {"admin".to_owned()};
    credentials.broker = Some(Broker::start(cfg).await.expect("broker must start"));
    credentials.directory = Some(log_dir);
    credentials.password = Some(admin_password);
}

#[given("alice requests a valid SHA-512 salted password with only 1 iteration")]
fn low_iterations(credentials: &mut ScramChange) {
    credentials.request = Some(AlterUserScramCredentialsRequest {
        upsertions: vec![scram_upsertion().iterations(1).call()],
        ..Default::default()
    });
}

#[given("users too-high and max request 16385 and 16384 SHA-512 iterations")]
fn iteration_boundary(credentials: &mut ScramChange) {
    credentials.request = Some(AlterUserScramCredentialsRequest {
        upsertions: vec![
            scram_upsertion()
                .name("too-high".into())
                .iterations(KAFKA_MAX_SCRAM_ITERATIONS + 1)
                .call(),
            scram_upsertion()
                .name("max".into())
                .iterations(KAFKA_MAX_SCRAM_ITERATIONS)
                .salt(Bytes::from(vec![1u8; 16]))
                .salted_password(Bytes::from(vec![1u8; 64]))
                .call(),
        ],
        ..Default::default()
    });
}

#[given("alice requests the unrecognized SCRAM mechanism byte 99")]
fn unknown_mechanism(credentials: &mut ScramChange) {
    credentials.request = Some(AlterUserScramCredentialsRequest {
        upsertions: vec![
            scram_upsertion()
                .mechanism(ScramAlgorithm::Unrecognized(ScramMechanismCode(99)))
                .call(),
        ],
        ..Default::default()
    });
}

#[given("alice has SHA-512 and SHA-256 upsertions in the same request")]
fn duplicate_upsertions(credentials: &mut ScramChange) {
    let SaltedPassword { salt, salted } = pbkdf2_salt_and_salted(alice_password().as_bytes(), 4096);
    let upsert = scram_upsertion()
        .salt(Bytes::from(salt))
        .salted_password(Bytes::from(salted.to_vec()))
        .call();
    let mut upsert_sha256 = upsert.clone();
    upsert_sha256.mechanism = WIRE_MECH_SCRAM_SHA_256;
    upsert_sha256.salted_password = Bytes::from(vec![7; 32]);
    credentials.request = Some(AlterUserScramCredentialsRequest {
        upsertions: vec![upsert, upsert_sha256],
        ..Default::default()
    });
}

#[given("alice already has a SHA-512 credential")]
async fn existing_credential(credentials: &ScramChange) {
    let broker = credentials
        .broker
        .as_ref()
        .expect("a SCRAM superuser broker");
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            iterations: 4096,
            salt: vec![1; 16],
            server_key: vec![2; 64],
            stored_key: vec![3; 64],
        }))
        .await
        .expect("seed alice SCRAM credential");
    broker
        .wait_for_image(|image| {
            image
                .scram_credential("alice", SaslMechanism::ScramSha512)
                .is_some()
        })
        .await;
}

#[given("alice has a SHA-512 deletion and a SHA-256 upsertion in the same request")]
fn mixed_mutations(credentials: &mut ScramChange) {
    credentials.request = Some(AlterUserScramCredentialsRequest {
        deletions: vec![ScramCredentialDeletion {
            name: "alice".to_owned(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            ..Default::default()
        }],
        upsertions: vec![
            scram_upsertion()
                .mechanism(ScramAlgorithm::Sha256)
                .salt(Bytes::from(vec![4u8; 16]))
                .salted_password(Bytes::from(vec![5u8; 32]))
                .call(),
        ],
        ..Default::default()
    });
}

#[given("the request deletes the nonexistent SHA-512 credential for ghost")]
fn missing_deletion(credentials: &mut ScramChange) {
    credentials.request = Some(AlterUserScramCredentialsRequest {
        deletions: vec![ScramCredentialDeletion {
            name: "ghost".to_owned(),
            mechanism: WIRE_MECH_SCRAM_SHA_512,
            ..Default::default()
        }],
        ..Default::default()
    });
}

#[when("the superuser submits AlterUserScramCredentials")]
async fn alter_credentials(credentials: &mut ScramChange) {
    let addr = credentials
        .broker
        .as_ref()
        .expect("a SCRAM superuser broker")
        .listen_addr();
    let request = credentials
        .request
        .take()
        .expect("a credential mutation request");
    credentials.response = Some(if let Some(password) = &credentials.password {
        drive_alter_user_scram_credentials_as_plain(addr, "admin", password.as_bytes(), request)
            .await
            .expect("PLAIN auth + AlterUserScramCredentials")
    } else {
        provision_as_admin(addr, request, "PLAIN auth + AlterUserScramCredentials").await
    });
}

#[derive(DataTableRow)]
struct ExpectedCredential {
    user: String,
    error_code: i16,
}

#[then("the response has exactly these per-user results:")]
fn credential_results(credentials: &ScramChange, #[datatable] rows: Rows<ExpectedCredential>) {
    let response = credentials
        .response
        .as_ref()
        .expect("credentials were altered");
    let expected: Vec<_> = rows.into_iter().collect();
    assert!(
        response.results.len() == expected.len(),
        "one result row per distinct username: {response:?}"
    );
    for row in expected {
        let actual = response
            .results
            .iter()
            .find(|result| result.user == row.user)
            .unwrap_or_else(|| panic!("missing {} row in {response:?}", row.user));
        assert!(
            actual.error_code == row.error_code,
            "{} expected error {}: {actual:?}",
            row.user,
            row.error_code
        );
    }
}

#[scenario(
    path = "tests/features/migrated/tests/auth_handlers/alter_scram_validation.feature",
    name = "alter scram creds low iterations rejected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_low_iterations_rejected(credentials: ScramChange) {
    credentials
        .broker
        .expect("a SCRAM superuser broker")
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/auth_handlers/alter_scram_validation.feature",
    name = "alter scram creds high iterations rejected but max allowed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_high_iterations_rejected_but_max_allowed(credentials: ScramChange) {
    credentials
        .broker
        .expect("a SCRAM superuser broker")
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/auth_handlers/alter_scram_validation.feature",
    name = "alter scram creds unknown mechanism returns unsupported sasl mechanism"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_unknown_mechanism_returns_unsupported_sasl_mechanism(
    credentials: ScramChange,
) {
    credentials
        .broker
        .expect("a SCRAM superuser broker")
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/auth_handlers/alter_scram_validation.feature",
    name = "alter scram creds duplicate resource rejected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_duplicate_resource_rejected(credentials: ScramChange) {
    credentials
        .broker
        .expect("a SCRAM superuser broker")
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/auth_handlers/alter_scram_validation.feature",
    name = "alter scram creds duplicate deletion and upsertion rejected per user"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_duplicate_deletion_and_upsertion_rejected_per_user(
    credentials: ScramChange,
) {
    credentials
        .broker
        .expect("a SCRAM superuser broker")
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/auth_handlers/alter_scram_validation.feature",
    name = "alter scram creds missing deletion returns resource not found 91"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_missing_deletion_returns_resource_not_found_91(
    credentials: ScramChange,
) {
    credentials
        .broker
        .expect("a SCRAM superuser broker")
        .shutdown()
        .await;
}

/// A SCRAM-admin broker with a random admin password, and the log directory
/// that backs it.
struct RandomScramAdmin {
    log_dir: tempfile::TempDir,
    admin_password: String,
    handle: krabka_broker::BrokerHandle,
}

async fn random_scram_admin_fixture() -> RandomScramAdmin {
    let log_dir = tempfile::tempdir().unwrap();
    let admin_password = uuid::Uuid::new_v4().to_string();
    let ListeningBroker { handle } =
        start_scram_admin(log_dir.path(), &admin_password, vec![SaslMechanism::Plain]).await;
    RandomScramAdmin {
        log_dir,
        admin_password,
        handle,
    }
}
