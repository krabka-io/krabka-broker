//! `AlterUserScramCredentials` (`api_key` 51, KIP-554) provisioning.
//!
//! A super-user upserts a SCRAM-SHA-256 or SCRAM-SHA-512 credential and the
//! named user then authenticates with it, while a non-super-user is refused
//! per row. The module also owns the KIP-554 wire constants, the
//! PLAIN-authenticated request drive, and the PBKDF2 salting that the
//! validation cases in `alter_scram_validation` reuse.

use {
    crate::{
        harness::{
            RetainedBroker, admin_plain_password, alice_password, round_trip,
            sasl_authenticate_round_trip, scram_admin_fixture, wrong_scram_password,
        },
        kafka_wire::{decode_named, encode_named, plain_payload, sasl_handshake_on},
        scram::drive_sasl_scram_session,
        support::{
            acl::use_simple_acl_authorizer,
            sasl::{ScramAlgorithm, sasl_temp_config, scram_upsertion},
            wire::{
                CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
                SocketExchangeSetup,
            },
        },
    },
    assert2::{assert, check},
    krabka_broker::{Broker, BrokerHandle},
    krabka_ids::ApiKey as WireApiKey,
    krabka_protocol::owned::{
        alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
        alter_user_scram_credentials_response::AlterUserScramCredentialsResponse,
        api_versions_request::ApiVersionsRequest,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
    },
    krabka_security::SaslMechanism,
    rstest::fixture,
    rstest_bdd_macros::{given, then, when},
    std::{io, net::SocketAddr},
    tempfile::TempDir,
    tokio::net::TcpStream,
};

/// SCRAM mechanism byte on the `AlterUserScramCredentials` wire, from
/// KIP-554. `1` is `SCRAM-SHA-256` and `2` is `SCRAM-SHA-512`.
pub const WIRE_MECH_SCRAM_SHA_256: i8 = 1;
pub const WIRE_MECH_SCRAM_SHA_512: i8 = 2;
pub const KAFKA_MAX_SCRAM_ITERATIONS: i32 = 16_384;

/// Happy path: a super-user authenticates over PLAIN, sends an
/// `AlterUserScramCredentials` upsertion for `alice`, and the broker stores
/// the credential.
///
/// The test then authenticates as `alice` over SCRAM-SHA-512. This proves
/// that the upsertion wrote a valid credential to the metadata image.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/alter_scram.feature",
    name = "alter scram creds super user can provision"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_super_user_can_provision(provisioning: ScramProvisioning) {
    provisioning
        .broker
        .expect("a SCRAM provisioning broker")
        .shutdown()
        .await;
}

fn check_single_successful_user(resp: &AlterUserScramCredentialsResponse, expected_user: &str) {
    assert!(resp.results.len() == 1);
    check!(
        resp.results[0].error_code == 0,
        "expected error_code=0, got {:?}",
        resp.results[0]
    );
    check!(resp.results[0].user == expected_user);
}

/// Wire-mapping proof: `AlterUserScramCredentials` accepts `mechanism=1`,
/// which is SCRAM-SHA-256, and stores a credential.
///
/// The broker can later authenticate against that credential over SHA-256.
/// The test is a copy of `alter_scram_creds_super_user_can_provision`, but it
/// uses the SHA-256 wire byte and a 32-byte `salted_password` payload.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/alter_scram.feature",
    name = "alter scram creds super user can provision sha256"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_super_user_can_provision_sha256(provisioning: ScramProvisioning) {
    provisioning
        .broker
        .expect("a SCRAM provisioning broker")
        .shutdown()
        .await;
}

/// A non-super-user authenticates and tries to upsert.
///
/// The broker accepts the request, because it is a valid SASL listener API.
/// But every per-user row reports `CLUSTER_AUTHORIZATION_FAILED` (31). The
/// broker makes no metadata change.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/auth_handlers/alter_scram.feature",
    name = "alter scram creds non super user rejected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_scram_creds_non_super_user_rejected(provisioning: ScramProvisioning) {
    provisioning
        .broker
        .expect("a SCRAM provisioning broker")
        .shutdown()
        .await;
}

/// Authenticate over SASL/PLAIN against `addr` as `user`/`password`, send one
/// `AlterUserScramCredentials v0` request, and decode the response.
///
/// The request uses `api_key` 51 and is flexible. Every T15 test case calls
/// this helper, so the SASL boilerplate stays in one place.
pub async fn drive_alter_user_scram_credentials_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: AlterUserScramCredentialsRequest,
) -> Result<AlterUserScramCredentialsResponse, io::Error> {
    let mut stream = TcpStream::connect(addr).await?;

    // ── 1. ApiVersions (v0, non-flexible).
    let av_req = ApiVersionsRequest::default();
    let av_body = encode_named(&av_req, 0, "ApiVersions")?;
    let _ = round_trip(&mut stream, SocketExchangeSetup::default(), &av_body).await?;

    // ── 2. SaslHandshake v1.
    sasl_handshake_on()
        .stream(&mut stream)
        .client_id("krabka-sasl-test")
        .call()
        .await?;

    // ── 3. SaslAuthenticate v2 (flexible). auth_bytes = \0user\0password.
    let auth_body = encode_named(
        &SaslAuthenticateRequest {
            auth_bytes: plain_payload(user, password),
            ..Default::default()
        },
        2,
        "SaslAuthenticate",
    )?;
    let auth_resp_bytes = sasl_authenticate_round_trip(&mut stream, &auth_body).await?;
    let auth_resp =
        decode_named::<SaslAuthenticateResponse>(&auth_resp_bytes, 2, "SaslAuthenticate")?;
    if auth_resp.error_code != 0 {
        return Err(io::Error::other(format!(
            "SaslAuthenticate failed: error_code={}",
            auth_resp.error_code
        )));
    }

    // ── 4. AlterUserScramCredentials v0 (api_key 51, flexible from v0).
    let auscr_body = encode_named(&req, 0, "AUSCR")?;
    let auscr_resp_bytes = round_trip(
        &mut stream,
        SocketExchangeSetup {
            api_key: WireApiKey(51),
            correlation: WireCorrelationId(4),
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        },
        &auscr_body,
    )
    .await?;
    decode_named::<AlterUserScramCredentialsResponse>(&auscr_resp_bytes, 0, "AUSCR")
}

/// A SCRAM salt with the `N`-byte salted password PBKDF2 derives from it.
pub struct SaltedPassword<const N: usize> {
    pub salt: Vec<u8>,
    pub salted: [u8; N],
}

/// Compute the salt and salted password for a SCRAM-SHA-512 wire upsertion.
///
/// The salt is a fixed 16-byte vector, which keeps the test deterministic.
/// The salted password is the 64-byte PBKDF2-HMAC-SHA-512 output that the
/// KIP-554 wire request carries.
pub fn pbkdf2_salt_and_salted(password: &[u8], iterations: u32) -> SaltedPassword<64> {
    let salt: Vec<u8> = (0..16).collect();
    let salted: [u8; 64] =
        pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(password, &salt, iterations);
    SaltedPassword { salt, salted }
}

/// SHA-256 analog of [`pbkdf2_salt_and_salted`].
///
/// It produces the 32-byte PBKDF2-HMAC-SHA-256 output for the wire tests.
fn pbkdf2_salt_and_salted_sha256(password: &[u8], iterations: u32) -> SaltedPassword<32> {
    let salt: Vec<u8> = (0..16).collect();
    let salted: [u8; 32] =
        pbkdf2::pbkdf2_hmac_array::<sha2::Sha256, 32>(password, &salt, iterations);
    SaltedPassword { salt, salted }
}

/// The same deterministic Alice credential for the allow and deny provisioning paths.
fn alice_sha512_request() -> AlterUserScramCredentialsRequest {
    let SaltedPassword { salt, salted } = pbkdf2_salt_and_salted(alice_password().as_bytes(), 4096);
    scram_upsertion_request(ScramAlgorithm::Sha512, salt, salted.to_vec())
}

fn scram_upsertion_request(
    mechanism: ScramAlgorithm,
    salt: Vec<u8>,
    salted_password: Vec<u8>,
) -> AlterUserScramCredentialsRequest {
    AlterUserScramCredentialsRequest {
        upsertions: vec![
            scram_upsertion()
                .mechanism(mechanism)
                .salt(bytes::Bytes::from(salt))
                .salted_password(bytes::Bytes::from(salted_password))
                .call(),
        ],
        ..Default::default()
    }
}

/// Send an upsertion as the fixture's PLAIN super-user.
pub async fn provision_as_admin(
    addr: SocketAddr,
    request: AlterUserScramCredentialsRequest,
    context: &str,
) -> AlterUserScramCredentialsResponse {
    drive_alter_user_scram_credentials_as_plain(
        addr,
        "admin",
        admin_plain_password().as_bytes(),
        request,
    )
    .await
    .expect(context)
}

async fn post_upsertion_auth(
    broker: &krabka_broker::BrokerHandle,
    addr: SocketAddr,
    mechanism: SaslMechanism,
) -> io::Result<()> {
    broker
        .wait_for_image(|image| image.scram_credential("alice", mechanism).is_some())
        .await;
    drive_sasl_scram_session(addr, "alice", &alice_password(), mechanism).await
}

struct ScramProvisioning {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    mechanism: Option<SaslMechanism>,
    user: String,
    password: Vec<u8>,
    request: Option<AlterUserScramCredentialsRequest>,
    response: Option<AlterUserScramCredentialsResponse>,
    login: Option<io::Result<()>>,
}

#[fixture]
fn provisioning() -> ScramProvisioning {
    // Start this scenario with fresh inputs and observations.

    ScramProvisioning {
        broker: None,
        directory: None,
        mechanism: None,
        user: String::new(),
        password: Vec::new(),
        request: None,
        response: None,
        login: None,
    }
}

fn provisioned_mechanism(name: &str) -> SaslMechanism {
    match name {
        "SCRAM-SHA-256" => SaslMechanism::ScramSha256,
        "SCRAM-SHA-512" => SaslMechanism::ScramSha512,
        other => panic!("unknown provisioning mechanism {other}"),
    }
}

#[given("a PLAIN admin superuser on a broker supporting {mechanism}")]
async fn provisioning_superuser(provisioning: &mut ScramProvisioning, mechanism: String) {
    let mechanism = provisioned_mechanism(&mechanism);
    let RetainedBroker { log_dir, handle } =
        scram_admin_fixture(vec![SaslMechanism::Plain, mechanism]).await;
    provisioning.broker = Some(handle);
    provisioning.directory = Some(log_dir);
    provisioning.user = "admin".into();
    provisioning.password = admin_plain_password().into_bytes();
}

#[given("PLAIN user bob has no cluster-Alter permission")]
async fn unprivileged_provisioner(provisioning: &mut ScramProvisioning) {
    let (directory, mut config) = sasl_temp_config(vec![SaslMechanism::Plain]);
    config
        .plain_credentials
        .insert("bob".to_owned(), wrong_scram_password());
    config.super_users = maplit::hashset! {"admin".to_owned()};
    use_simple_acl_authorizer(&mut config);
    provisioning.broker = Some(Broker::start(config).await.expect("broker must start"));
    provisioning.directory = Some(directory);
    provisioning.user = "bob".into();
    provisioning.password = wrong_scram_password().into_bytes();
}

#[given("alice requests a {mechanism} credential derived from her password")]
fn requested_scram_credential(provisioning: &mut ScramProvisioning, mechanism: String) {
    let mechanism = provisioned_mechanism(&mechanism);
    provisioning.request = Some(match mechanism {
        SaslMechanism::ScramSha512 => alice_sha512_request(),
        SaslMechanism::ScramSha256 => {
            let SaltedPassword { salt, salted } =
                pbkdf2_salt_and_salted_sha256(alice_password().as_bytes(), 4096);
            scram_upsertion_request(ScramAlgorithm::Sha256, salt, salted.to_vec())
        }
        _ => unreachable!("the fixture parser only permits SCRAM mechanisms"),
    });
    provisioning.mechanism = Some(mechanism);
}

#[when("the PLAIN principal submits alice's AlterUserScramCredentials upsertion")]
async fn submit_scram_upsertion(provisioning: &mut ScramProvisioning) {
    let addr = provisioning
        .broker
        .as_ref()
        .expect("a provisioning broker")
        .listen_addr();
    let request = provisioning
        .request
        .take()
        .expect("a salted SCRAM credential request");
    provisioning.response = Some(
        drive_alter_user_scram_credentials_as_plain(
            addr,
            &provisioning.user,
            &provisioning.password,
            request,
        )
        .await
        .expect("PLAIN auth + AUSCR upsertion"),
    );
}

#[then("alice is the single successful credential result")]
fn provisioned_user(provisioning: &ScramProvisioning) {
    check_single_successful_user(
        provisioning
            .response
            .as_ref()
            .expect("a credential upsertion response"),
        "alice",
    );
}

#[when("alice authenticates against the committed credential and requests Metadata")]
async fn login_with_upserted_credential(provisioning: &mut ScramProvisioning) {
    let broker = provisioning.broker.as_ref().expect("a provisioning broker");
    provisioning.login = Some(
        post_upsertion_auth(
            broker,
            broker.listen_addr(),
            provisioning
                .mechanism
                .expect("the upserted SCRAM mechanism"),
        )
        .await,
    );
}

#[then("the upserted SCRAM credential and its server signature authenticate successfully")]
fn usable_scram_upsertion(provisioning: &ScramProvisioning) {
    provisioning
        .login
        .as_ref()
        .expect("the new credential was used")
        .as_ref()
        .expect("post-upsertion SCRAM auth must succeed");
}

#[then("the single upsertion result is CLUSTER_AUTHORIZATION_FAILED")]
fn unauthorized_upsertion(provisioning: &ScramProvisioning) {
    let response = provisioning
        .response
        .as_ref()
        .expect("a credential upsertion response");
    assert!(response.results.len() == 1);
    assert!(
        response.results[0].error_code == 31,
        "non-super-user must get CLUSTER_AUTHORIZATION_FAILED, got {:?}",
        response.results[0]
    );
}
