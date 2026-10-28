//! Single-broker `SASL_PLAINTEXT` fixtures for the KIP-48 suite, and the
//! metadata-image waiters the tests use to observe a committed token record.
//!
//! Both fixtures enable PLAIN and SCRAM-SHA-256 on one listener and set the
//! master delegation-token key, because the token-fallback authentication
//! path needs SCRAM even though the human users authenticate with PLAIN.

use {
    crate::support::sasl::{
        delegation_token_defaults, sasl_temp_config, start_broker as start_sasl_broker,
    },
    krabka_broker::{
        Broker, BrokerHandle, authorizer::SimpleAclAuthorizer, config::InterBrokerCredentials,
    },
    krabka_security::SaslMechanism,
    std::{net::SocketAddr, sync::Arc},
    tempfile::TempDir,
};

// ─────────────────────────────────────────────────────────────────────────────
// Cluster bring-up.
// ─────────────────────────────────────────────────────────────────────────────

/// Boots a single-broker `SASL_PLAINTEXT` cluster set up for the full KIP-48
/// lifecycle:
///   - PLAIN credentials for `alice` and `bob`
///   - both PLAIN and SCRAM-SHA-256 enabled on the listener. PLAIN serves the
///     human-user handshakes, and SCRAM-SHA-256 serves the token-fallback
///     path.
///   - `delegation_token_secret_key = Some("e2e-master-key")`, which gates
///     the four delegation-token RPCs and the SCRAM token-fallback lookup
pub async fn start_broker() -> (BrokerHandle, TempDir, SocketAddr) {
    let (log_dir, mut cfg) =
        sasl_temp_config(vec![SaslMechanism::Plain, SaslMechanism::ScramSha256]);
    cfg.plain_credentials
        .insert("alice".to_owned(), "wonderland".to_owned());
    cfg.plain_credentials
        .insert("bob".to_owned(), "builder".to_owned());
    // Inter-broker auth uses PLAIN as alice (the cluster only has one broker
    // so this is not exercised, but `BrokerConfig::validate` requires it
    // when the inter-broker listener is SASL).
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: "alice".to_owned(),
        password: "wonderland".to_owned(),
    });
    // KIP-48 distinguishes the absolute ceiling (`max_lifetime_ms` →
    // `max_timestamp_ms`) from the initial renew window (`default_renew_period`
    // → `expiry_timestamp_ms`). With 7d ceiling + 24h renew period (both
    // the Kafka defaults), the create handler emits expiry = issue + 24h
    // and max = issue + 7d as separate values, so Renew can extend the
    // expiry well past its initial value (and the lifecycle test asserts
    // strict-monotonic extension below).
    delegation_token_defaults(&mut cfg, b"e2e-master-key");

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();
    (handle, log_dir, addr)
}

/// Act-as variant. It boots a single-broker `SASL_PLAINTEXT` cluster with
/// caller-specified PLAIN credentials and a caller-specified set of
/// super-users.
///
/// `plain_creds` is `&[(username, password)]`. `super_users` is
/// `&[username]`. The names listed there go into `BrokerConfig.super_users`
/// and bypass ACL checks. In particular, per spec §1 they are the only
/// callers allowed to set `owner_principal_*` on `CreateDelegationToken`.
///
/// The protocol surface matches `start_broker`: PLAIN and SCRAM-SHA-256
/// enabled, the master delegation-token key set, a 7d ceiling, and a 24h
/// default renew period.
pub fn start_broker_with_super_users(
    plain_creds: &[(&str, &str)],
    super_users: &[&str],
) -> impl Future<Output = (BrokerHandle, TempDir, SocketAddr)> {
    let (log_dir, mut cfg) =
        sasl_temp_config(vec![SaslMechanism::Plain, SaslMechanism::ScramSha256]);
    for (user, password) in plain_creds {
        cfg.plain_credentials
            .insert((*user).to_owned(), (*password).to_owned());
    }
    for user in super_users {
        cfg.super_users.insert((*user).to_owned());
    }
    // Act-as is gated by KIP-373's `CreateTokens` ACL. With no ACL authorizer
    // Kafka allows every operation, so the gate is exercised against the ACL
    // authorizer, which grants the super users everything.
    cfg.authorizer = Arc::new(SimpleAclAuthorizer::new(
        cfg.super_users.iter().cloned().collect(),
    ));
    // Inter-broker auth uses PLAIN as the first listed user. `BrokerConfig::
    // validate` requires inter-broker credentials when the inter-broker
    // listener is SASL, even though a single-broker cluster never opens an
    // inter-broker connection.
    let (ib_user, ib_pw) = plain_creds
        .first()
        .expect("must supply at least one PLAIN credential for inter-broker auth");
    cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
        username: (*ib_user).to_owned(),
        password: (*ib_pw).to_owned(),
    });
    delegation_token_defaults(&mut cfg, b"act-as-master-key");

    Box::pin(start_sasl_broker(cfg, log_dir))
}

// ─────────────────────────────────────────────────────────────────────────────
// Image-watch helpers. `submit_change` returns once the record is replicated
// to the controller's state machine, but the listener's `MetadataImage` is
// served through the same controller handle, so a tight poll converges
// within a few ms.
// ─────────────────────────────────────────────────────────────────────────────

pub async fn wait_for_token(
    handle: &BrokerHandle,
    token_id: &str,
) -> krabka_metadata::DelegationToken {
    // Watch the committed metadata image (same controller handle
    // `controller_image_for_test` reads) until the V1DelegationToken record
    // materializes, then re-read it to return the applied token.
    handle
        .wait_for_image(|img| img.delegation_token_by_id(token_id).is_some())
        .await;
    handle
        .controller_image_for_test()
        .delegation_token_by_id(token_id)
        .expect("token present in image after wait_for_image")
        .clone()
}

pub async fn wait_for_token_gone(handle: &BrokerHandle, token_id: &str) {
    // Watch the committed metadata image until the token's tombstone applies.
    handle
        .wait_for_image(|img| img.delegation_token_by_id(token_id).is_none())
        .await;
}

/// The act-as cluster: an admin super user and an ordinary token owner.
pub async fn start_admin_alice() -> (BrokerHandle, TempDir, SocketAddr) {
    start_broker_with_super_users(&[("admin", "admin-pw"), ("alice", "alice-pw")], &["admin"]).await
}

use {
    crate::{
        rpc::{
            check_created_identity, send_create_delegation_token, send_describe_delegation_token,
            send_expire_delegation_token, send_renew_delegation_token,
        },
        wire::{sasl_plain_authenticate, sasl_scram_sha256_authenticate},
    },
    assert2::{assert, check},
    base64::{Engine as _, engine::general_purpose},
    krabka_protocol::owned::{
        create_delegation_token_request::{CreatableRenewers, CreateDelegationTokenRequest},
        create_delegation_token_response::CreateDelegationTokenResponse,
        describe_delegation_token_request::{
            DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
        },
        describe_delegation_token_response::DescribeDelegationTokenResponse,
        expire_delegation_token_request::ExpireDelegationTokenRequest,
        expire_delegation_token_response::ExpireDelegationTokenResponse,
        renew_delegation_token_request::RenewDelegationTokenRequest,
        renew_delegation_token_response::RenewDelegationTokenResponse,
    },
    std::{collections::BTreeMap, io},
    tokio::net::TcpStream,
};
pub struct TokenWorkflow {
    pub broker: Option<BrokerHandle>,
    pub directory: Option<TempDir>,
    pub address: Option<SocketAddr>,
    pub sessions: BTreeMap<String, TcpStream>,
    pub tokens: BTreeMap<String, CreateDelegationTokenResponse>,
    pub created: Option<CreateDelegationTokenResponse>,
    pub renewed: Option<RenewDelegationTokenResponse>,
    pub expired: Option<ExpireDelegationTokenResponse>,
    pub described: Option<DescribeDelegationTokenResponse>,
    pub authenticated: Option<io::Result<TcpStream>>,
}
impl TokenWorkflow {
    pub fn address(&self) -> SocketAddr { self.address.expect("delegation broker started") }

    pub async fn shutdown(self) {
        drop(self.sessions);
        drop(self.authenticated);
        self.broker
            .expect("delegation broker started")
            .shutdown()
            .await;
    }
}
#[rstest::fixture]
pub fn tokens() -> TokenWorkflow {
    // Start this scenario with fresh inputs and observations.

    TokenWorkflow {
        broker: None,
        directory: None,
        address: None,
        sessions: BTreeMap::new(),
        tokens: BTreeMap::new(),
        created: None,
        renewed: None,
        expired: None,
        described: None,
        authenticated: None,
    }
}
#[rstest_bdd_macros::given(
    "a token broker has alice wonderland and bob builder with a 7 day ceiling and 24 hour renew period"
)]
async fn delegation_lifecycle_broker(tokens: &mut TokenWorkflow) {
    let (broker, directory, address) = start_broker().await;
    tokens.broker = Some(broker);
    tokens.directory = Some(directory);
    tokens.address = Some(address);
}
#[rstest_bdd_macros::given(
    "a token broker has super user admin admin-pw and ordinary alice alice-pw"
)]
async fn delegation_act_as_broker(tokens: &mut TokenWorkflow) {
    let (broker, directory, address) = start_admin_alice().await;
    tokens.broker = Some(broker);
    tokens.directory = Some(directory);
    tokens.address = Some(address);
}
#[rstest_bdd_macros::given("a token broker has alice alice-pw and no super users")]
async fn delegation_unprivileged_broker(tokens: &mut TokenWorkflow) {
    let (broker, directory, address) =
        start_broker_with_super_users(&[("alice", "alice-pw")], &[]).await;
    tokens.broker = Some(broker);
    tokens.directory = Some(directory);
    tokens.address = Some(address);
}
#[rstest_bdd_macros::given("{principal} authenticates with PLAIN password {password}")]
async fn delegation_plain_actor(tokens: &mut TokenWorkflow, principal: String, password: String) {
    let session = sasl_plain_authenticate(tokens.address(), &principal, password.as_bytes())
        .await
        .expect("PLAIN authentication");
    tokens.sessions.insert(principal, session);
}
#[rstest_bdd_macros::when(
    "{principal} mints token {alias} for owner {owner} with renewers {renewers} and lifetime {lifetime}"
)]
async fn mint_named_token(
    tokens: &mut TokenWorkflow,
    principal: String,
    alias: String,
    owner: String,
    renewers: String,
    lifetime: i64,
) {
    let renewers = if renewers == "none" {
        Vec::new()
    } else {
        renewers
            .split(',')
            .map(|renewer| CreatableRenewers {
                principal_type: "User".into(),
                principal_name: renewer.into(),
                ..Default::default()
            })
            .collect()
    };
    let mut request = CreateDelegationTokenRequest {
        renewers,
        max_lifetime_ms: lifetime,
        ..Default::default()
    };
    if owner != "caller" {
        request.owner_principal_type = Some("User".into());
        request.owner_principal_name = Some(owner);
    }
    let response = send_create_delegation_token(
        tokens
            .sessions
            .get_mut(&principal)
            .expect("principal authenticated"),
        100,
        &request,
    )
    .await
    .expect("CreateDelegationToken");
    tokens.tokens.insert(alias, response.clone());
    tokens.created = Some(response);
}
#[rstest_bdd_macros::then("token creation reports error {error}")]
fn token_created_error(tokens: &TokenWorkflow, error: i16) {
    check!(
        tokens
            .created
            .as_ref()
            .expect("token creation attempted")
            .error_code
            == error
    );
}
#[rstest_bdd_macros::then(
    "token {alias} response names owner User {owner} requester User {requester} a token id and 64 byte HMAC"
)]
fn named_token_identity(tokens: &TokenWorkflow, alias: String, owner: String, requester: String) {
    check_created_identity(
        tokens.tokens.get(&alias).expect("named token minted"),
        &owner,
        &requester,
    );
}
#[rstest_bdd_macros::then(
    "token {alias} initial expiry follows issue time and is strictly before its maximum timestamp"
)]
fn named_token_lifetime(tokens: &TokenWorkflow, alias: String) {
    let response = tokens.tokens.get(&alias).expect("named token minted");
    check!(response.expiry_timestamp_ms > response.issue_timestamp_ms);
    assert!(
        response.expiry_timestamp_ms < response.max_timestamp_ms,
        "initial expiry must remain below max when renew period is below lifetime"
    );
}
#[rstest_bdd_macros::then(
    "token {alias} is committed with owner User {owner} requester User {requester} and renewers {renewers}"
)]
async fn named_token_committed(
    tokens: &TokenWorkflow,
    alias: String,
    owner: String,
    requester: String,
    renewers: String,
) {
    let created = tokens.tokens.get(&alias).expect("named token minted");
    let token = wait_for_token(
        tokens.broker.as_ref().expect("delegation broker started"),
        &created.token_id,
    )
    .await;
    check!(token.owner.principal_type == "User");
    check!(token.owner.name == owner);
    check!(token.requester.principal_type == "User");
    check!(token.requester.name == requester);
    let expected: Vec<_> = if renewers == "none" {
        Vec::new()
    } else {
        renewers.split(',').collect()
    };
    assert!(token.renewers.len() == expected.len());
    for (renewer, expected) in token.renewers.iter().zip(expected) {
        check!(renewer.principal_type == "User");
        check!(renewer.name == expected);
    }
}
#[rstest_bdd_macros::when(
    "a new connection authenticates with token {alias} using SCRAM SHA 256 and base64 HMAC"
)]
async fn authenticate_named_token(tokens: &mut TokenWorkflow, alias: String) {
    let token = tokens.tokens.get(&alias).expect("named token minted");
    let password = general_purpose::STANDARD.encode(&token.hmac);
    tokens.authenticated =
        Some(sasl_scram_sha256_authenticate(tokens.address(), &token.token_id, &password).await);
}
#[rstest_bdd_macros::then("the token SCRAM connection authenticates successfully")]
fn named_token_authenticated(tokens: &TokenWorkflow) {
    assert!(
        tokens
            .authenticated
            .as_ref()
            .expect("token login attempted")
            .is_ok()
    );
}
#[rstest_bdd_macros::when("the token authenticated connection tries to mint another token")]
async fn mint_from_token_session(tokens: &mut TokenWorkflow) {
    let stream = tokens
        .authenticated
        .as_mut()
        .expect("token login attempted")
        .as_mut()
        .expect("token authenticated");
    tokens.created = Some(
        send_create_delegation_token(stream, 200, &CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        })
        .await
        .expect("CreateDelegationToken on token session"),
    );
}
#[rstest_bdd_macros::when("{principal} renews token {alias} for {period} milliseconds")]
async fn renew_named_token(
    tokens: &mut TokenWorkflow,
    principal: String,
    alias: String,
    period: i64,
) {
    let hmac = tokens
        .tokens
        .get(&alias)
        .expect("named token minted")
        .hmac
        .clone();
    tokens.renewed = Some(
        send_renew_delegation_token(
            tokens
                .sessions
                .get_mut(&principal)
                .expect("principal authenticated"),
            300,
            &RenewDelegationTokenRequest {
                hmac,
                renew_period_ms: period,
                ..Default::default()
            },
        )
        .await
        .expect("RenewDelegationToken"),
    );
}
#[rstest_bdd_macros::then("token renewal reports error {error}")]
fn named_token_renewal_error(tokens: &TokenWorkflow, error: i16) {
    check!(
        tokens
            .renewed
            .as_ref()
            .expect("renewal attempted")
            .error_code
            == error
    );
}
#[rstest_bdd_macros::then(
    "token {alias} renewed expiry never moves backward or beyond the maximum timestamp"
)]
fn named_token_renewed_window(tokens: &TokenWorkflow, alias: String) {
    let created = tokens.tokens.get(&alias).expect("named token minted");
    let renewed = tokens.renewed.as_ref().expect("renewal attempted");
    check!(renewed.expiry_timestamp_ms >= created.expiry_timestamp_ms);
    check!(renewed.expiry_timestamp_ms <= created.max_timestamp_ms);
}
#[rstest_bdd_macros::when("{principal} describes tokens using owner filter User {owner}")]
async fn describe_named_tokens(tokens: &mut TokenWorkflow, principal: String, owner: String) {
    tokens.described = Some(
        send_describe_delegation_token(
            tokens
                .sessions
                .get_mut(&principal)
                .expect("principal authenticated"),
            400,
            &DescribeDelegationTokenRequest {
                owners: Some(vec![DescribeDelegationTokenOwner {
                    principal_type: "User".into(),
                    principal_name: owner,
                    ..Default::default()
                }]),
                ..Default::default()
            },
        )
        .await
        .expect("DescribeDelegationToken"),
    );
}
#[rstest_bdd_macros::then(
    "the successful description contains exactly token {alias} owned by User {owner}"
)]
fn named_token_described(tokens: &TokenWorkflow, alias: String, owner: String) {
    let described = tokens.described.as_ref().expect("tokens described");
    check!(described.error_code == 0);
    assert!(described.tokens.len() == 1);
    check!(
        described.tokens[0].token_id
            == tokens
                .tokens
                .get(&alias)
                .expect("named token minted")
                .token_id
    );
    check!(described.tokens[0].principal_type == "User");
    check!(described.tokens[0].principal_name == owner);
}
#[rstest_bdd_macros::then("the described token requester is User {requester}")]
fn named_token_described_requester(tokens: &TokenWorkflow, requester: String) {
    let described = tokens.described.as_ref().expect("tokens described");
    check!(described.tokens[0].token_requester_principal_type == "User");
    check!(described.tokens[0].token_requester_principal_name == requester);
}
#[rstest_bdd_macros::when("{principal} expires token {alias} with period {period}")]
async fn expire_named_token(
    tokens: &mut TokenWorkflow,
    principal: String,
    alias: String,
    period: i64,
) {
    let hmac = tokens
        .tokens
        .get(&alias)
        .expect("named token minted")
        .hmac
        .clone();
    tokens.expired = Some(
        send_expire_delegation_token(
            tokens
                .sessions
                .get_mut(&principal)
                .expect("principal authenticated"),
            500,
            &ExpireDelegationTokenRequest {
                hmac,
                expiry_time_period_ms: period,
                ..Default::default()
            },
        )
        .await
        .expect("ExpireDelegationToken"),
    );
}
#[rstest_bdd_macros::then("token expiry reports error {error}")]
fn named_token_expiry_error(tokens: &TokenWorkflow, error: i16) {
    check!(
        tokens
            .expired
            .as_ref()
            .expect("expiry attempted")
            .error_code
            == error
    );
}
#[rstest_bdd_macros::then("token {alias} has disappeared from the committed image")]
async fn named_token_removed(tokens: &TokenWorkflow, alias: String) {
    wait_for_token_gone(
        tokens.broker.as_ref().expect("delegation broker started"),
        &tokens
            .tokens
            .get(&alias)
            .expect("named token minted")
            .token_id,
    )
    .await;
}
#[rstest_bdd_macros::when(
    "all existing sessions disconnect and fresh SCRAM authentication uses expired token {alias}"
)]
async fn authenticate_expired_named_token(tokens: &mut TokenWorkflow, alias: String) {
    tokens.sessions.clear();
    drop(tokens.authenticated.take());
    let token = tokens.tokens.get(&alias).expect("named token minted");
    wait_for_token_gone(
        tokens.broker.as_ref().expect("delegation broker started"),
        &token.token_id,
    )
    .await;
    let password = general_purpose::STANDARD.encode(&token.hmac);
    tokens.authenticated =
        Some(sasl_scram_sha256_authenticate(tokens.address(), &token.token_id, &password).await);
}
#[rstest_bdd_macros::then("the expired token cannot authenticate on the fresh connection")]
fn expired_named_token_rejected(tokens: &TokenWorkflow) {
    assert!(
        tokens
            .authenticated
            .as_ref()
            .expect("fresh expired-token login attempted")
            .is_err()
    );
}
