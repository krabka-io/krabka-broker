//! Tests for the `CreateDelegationToken` handler, driven against a live
//! single-voter controller so that every case also pins what the quorum
//! persists.
//!
//! The cases cover Kafka's refusal order and shapes, owner
//! resolution, and the lifetime rules that separate `expiry_timestamp_ms`
//! from `max_timestamp_ms`.

use {
    super::{
        test_support::{empty_super_users, super_users_with, token_acl},
        *,
    },
    crate::{
        codes::{DELEGATION_TOKEN_AUTHORIZATION_FAILED, INVALID_REQUEST, NONE},
        handlers::{
            delegation_token_test_support::{
                DAY_MS as RENEW_24H_MS, SecretProvision, TokenFixture, anonymous, authed,
                test_controller, token_authed,
            },
            test_support::metadata_version_gated,
        },
        test_support::{KafkaErrorCode, UnixMillis, allow_acl},
    },
    assert2::assert,
    krabka_protocol::owned::{
        create_delegation_token_request::{CreatableRenewers, CreateDelegationTokenRequest},
        create_delegation_token_response::CreateDelegationTokenResponse,
    },
    krabka_security::{KafkaPrincipal, SecretBytes},
    tempfile::TempDir,
};

fn principal(principal_type: &str, name: &str) -> KafkaPrincipal {
    KafkaPrincipal {
        principal_type: principal_type.into(),
        name: name.into(),
    }
}

fn user(name: &str) -> KafkaPrincipal { principal("User", name) }

fn renewer(principal_type: &str, name: &str) -> CreatableRenewers {
    CreatableRenewers {
        principal_type: principal_type.into(),
        principal_name: name.into(),
        ..Default::default()
    }
}

fn act_as(principal_type: &str, name: &str) -> CreateDelegationTokenRequest {
    CreateDelegationTokenRequest {
        owner_principal_type: Some(principal_type.into()),
        owner_principal_name: Some(name.into()),
        max_lifetime_ms: -1,
        ..Default::default()
    }
}

fn self_mint_request() -> CreateDelegationTokenRequest {
    CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        ..Default::default()
    }
}

/// The response a refusal carries: `code`, both principals, and `timestamp`
/// in all three timestamp fields (`-1` on the broker, `0` from the
/// controller).
#[derive(krabka_macros::FieldDefaults)]
struct TokenRefusalSetup {
    code: KafkaErrorCode,
    #[default(user("alice"))]
    owner: KafkaPrincipal,
    #[default(user("alice"))]
    requester: KafkaPrincipal,
    #[default(UnixMillis(-1))]
    timestamp: UnixMillis,
}

#[bon::builder]
fn refusal(
    #[builder(into, default)] code: KafkaErrorCode,
    #[builder(default = user ("alice"))] owner: KafkaPrincipal,
    #[builder(default = user ("alice"))] requester: KafkaPrincipal,
    #[builder(into, default = UnixMillis (- 1))] timestamp: UnixMillis,
) -> CreateDelegationTokenResponse {
    let setup = TokenRefusalSetup {
        code,
        owner,
        requester,
        timestamp,
    };
    CreateDelegationTokenResponse {
        error_code: setup.code.0,
        principal_type: setup.owner.principal_type,
        principal_name: setup.owner.name,
        token_requester_principal_type: setup.requester.principal_type,
        token_requester_principal_name: setup.requester.name,
        issue_timestamp_ms: setup.timestamp.0,
        expiry_timestamp_ms: setup.timestamp.0,
        max_timestamp_ms: setup.timestamp.0,
        ..Default::default()
    }
}

#[derive(Default)]
struct TokenCreationScenario {
    resources: Option<TokenFixture>,
    auth: Option<crate::network::auth::ConnectionAuth>,
    provision: SecretProvision,
    super_users: Vec<String>,
    ceiling: i64,
    request: CreateDelegationTokenRequest,
    response: Option<CreateDelegationTokenResponse>,
}
#[rstest::fixture]
fn token_creation() -> TokenCreationScenario {
    let resources = None;
    TokenCreationScenario {
        resources,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given(
    "a token controller with requester {requester} authenticated by {method}, tokens enabled {enabled}, super users {supers} and lifetime ceiling {ceiling}"
)]
async fn token_creation_context(
    token_creation: &mut TokenCreationScenario,
    requester: String,
    method: String,
    enabled: bool,
    supers: String,
    ceiling: i64,
) {
    let mut resources = TokenFixture::new().await;
    resources.secret = SecretBytes::new(b"master-key".to_vec());
    token_creation.resources = Some(resources);
    token_creation.auth = Some(match method.as_str() {
        "token" => token_authed(&requester),
        "plain" => authed(&requester),
        "anonymous" => anonymous(),
        other => panic!("unknown authentication method {other}"),
    });
    token_creation.provision = if enabled {
        SecretProvision::Configured
    } else {
        SecretProvision::Absent
    };
    token_creation.super_users = supers
        .split(',')
        .filter(|name| *name != "none")
        .map(str::to_owned)
        .collect();
    token_creation.ceiling = ceiling;
}
#[rstest_bdd_macros::given(
    "the token request has owner type \"{kind}\", owner name \"{name}\", lifetime {lifetime} and renewers {renewers}"
)]
fn token_creation_request(
    token_creation: &mut TokenCreationScenario,
    kind: String,
    name: String,
    lifetime: i64,
    renewers: String,
) {
    token_creation.request = CreateDelegationTokenRequest {
        owner_principal_type: (kind != "null").then_some(kind),
        owner_principal_name: (name != "null").then(|| {
            if name == "<empty>" {
                String::new()
            } else {
                name
            }
        }),
        max_lifetime_ms: lifetime,
        renewers: renewers
            .split(',')
            .filter(|value| *value != "none")
            .map(|value| {
                let (kind, name) = value.split_once(':').expect("renewer principal");
                renewer(kind, name)
            })
            .collect(),
        ..Default::default()
    };
}
#[rstest_bdd_macros::when(
    "the controller handles CreateDelegationToken with a 24-hour renew period"
)]
async fn mint_delegation_token(token_creation: &mut TokenCreationScenario) {
    let resources = token_creation.resources.as_ref().expect("controller");
    let super_names = token_creation
        .super_users
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let authorizer = super_users_with(&super_names);
    token_creation.response = Some(
        handle(
            &token_creation.request,
            token_creation.auth.as_ref().expect("connection identity"),
            token_creation.provision.get(&resources.secret),
            token_creation.ceiling,
            RENEW_24H_MS,
            &*resources.controller,
            token_acl(&authorizer),
        )
        .await,
    );
}
fn token_principal(value: &str) -> KafkaPrincipal {
    let (kind, name) = value.split_once(':').expect("principal type:name");
    principal(kind, name)
}
#[rstest_bdd_macros::then(
    "token creation refuses with code {code}, owner {owner}, requester {requester} and timestamps {timestamp} without persisting tokens"
)]
async fn refused_token_creation(
    token_creation: &mut TokenCreationScenario,
    code: i16,
    owner: String,
    requester: String,
    timestamp: i64,
) {
    let expected = refusal()
        .code(code)
        .owner(token_principal(&owner))
        .requester(token_principal(&requester))
        .timestamp(timestamp)
        .call();
    assert!(token_creation.response.as_ref().expect("creation response") == &expected);
    let resources = token_creation.resources.take().expect("controller");
    assert!(
        resources
            .controller
            .current_image()
            .all_delegation_tokens()
            .count()
            == 0
    );
    resources.controller.cancel().await;
}
#[rstest_bdd_macros::then(
    "the minted token belongs to {owner}, names requester {requester}, renewers {renewers}, expiry delta {expiry} and maximum delta {maximum}"
)]
async fn persisted_token_creation(
    token_creation: &mut TokenCreationScenario,
    owner: String,
    requester: String,
    renewers: String,
    expiry: i64,
    maximum: i64,
) {
    let response = token_creation.response.as_ref().expect("creation response");
    assert!((response.token_id.is_empty(), response.hmac.len()) == (false, 64));
    assert!(response.hmac == compute_token_hmac(b"master-key", &response.token_id));
    let owner = token_principal(&owner);
    let requester = token_principal(&requester);
    let expected = CreateDelegationTokenResponse {
        issue_timestamp_ms: response.issue_timestamp_ms,
        expiry_timestamp_ms: response.issue_timestamp_ms + expiry,
        max_timestamp_ms: response.issue_timestamp_ms + maximum,
        token_id: response.token_id.clone(),
        hmac: response.hmac.clone(),
        ..refusal()
            .code(NONE)
            .owner(owner.clone())
            .requester(requester.clone())
            .timestamp(0)
            .call()
    };
    assert!(response == &expected);
    let expected_stored = krabka_metadata::DelegationToken {
        token_id: response.token_id.clone(),
        owner,
        requester,
        issue_timestamp_ms: response.issue_timestamp_ms,
        expiry_timestamp_ms: response.expiry_timestamp_ms,
        max_timestamp_ms: response.max_timestamp_ms,
        renewers: renewers
            .split(',')
            .filter(|value| *value != "none")
            .map(token_principal)
            .collect(),
    };
    let resources = token_creation.resources.take().expect("controller");
    {
        let image = resources.controller.current_image();
        let stored = image
            .delegation_token_by_id(&response.token_id)
            .expect("persisted token");
        assert!(*stored == expected_stored);
    }
    resources.controller.cancel().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Owner construction precedes token credential refusal"
)]
#[tokio::test]
async fn token_creation_case_0(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Owner construction also precedes super-user authorization"
)]
#[tokio::test]
async fn token_creation_case_1(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Token credentials cannot mint tokens"
)]
#[tokio::test]
async fn token_creation_case_2(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Token credentials cannot mint for another owner even as super user"
)]
#[tokio::test]
async fn token_creation_case_3(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Anonymous principals cannot mint tokens"
)]
#[tokio::test]
async fn token_creation_case_4(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Token credential refusal precedes disabled token support"
)]
#[tokio::test]
async fn token_creation_case_5(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Acting as another owner requires authorization"
)]
#[tokio::test]
async fn token_creation_case_6(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Act-as authorization precedes disabled token support"
)]
#[tokio::test]
async fn token_creation_case_7(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Renewers must be User principals"
)]
#[tokio::test]
async fn token_creation_case_8(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Renewer validation precedes disabled token support"
)]
#[tokio::test]
async fn token_creation_case_9(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Disabled token support returns controller timestamps"
)]
#[tokio::test]
async fn token_creation_case_10(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Default lifetime below renew period"
)]
#[tokio::test]
async fn token_creation_case_11(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Zero lifetime takes the ceiling"
)]
#[tokio::test]
async fn token_creation_case_12(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Negative lifetime takes the ceiling"
)]
#[tokio::test]
async fn token_creation_case_13(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Requested lifetime is capped by broker ceiling"
)]
#[tokio::test]
async fn token_creation_case_14(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Requested lifetime below ceiling is retained"
)]
#[tokio::test]
async fn token_creation_case_15(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Naming self needs no act-as permission"
)]
#[tokio::test]
async fn token_creation_case_16(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Empty owner name resolves to requester"
)]
#[tokio::test]
async fn token_creation_case_17(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Super user mints for another user"
)]
#[tokio::test]
async fn token_creation_case_18(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "Super user retains another principal type"
)]
#[tokio::test]
async fn token_creation_case_19(token_creation: TokenCreationScenario) {}

#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_delegation_token_creation.feature",
    name = "User renewer is persisted"
)]
#[tokio::test]
async fn token_creation_case_20(token_creation: TokenCreationScenario) {}

/// KIP-373: `CreateTokens` on `User:<owner>` lets a requester mint a token for
/// that owner, and for no other (Kafka's `handleCreateTokenRequest`).
#[tokio::test]
async fn create_tokens_acl_admits_minting_for_that_owner_only() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    controller
        .submit_change(vec![krabka_metadata::MetadataRecord::V1AccessControlEntry(
            allow_acl()
                .resource_type(krabka_metadata::ResourceType::User)
                .resource_name("User:alice")
                .principal("User:bob")
                .operation(krabka_metadata::AclOperation::CreateTokens)
                .call(),
        )])
        .await
        .expect("seed acl");
    let secret = SecretBytes::new(b"master-key".to_vec());
    let authorizer = empty_super_users();

    let mint = |owner: &'static str| {
        let req = act_as("User", owner);
        let controller = &controller;
        let secret = &secret;
        let authorizer = &authorizer;
        async move {
            handle(
                &req,
                &authed("bob"),
                Some(secret),
                60_000,
                RENEW_24H_MS,
                &**controller,
                token_acl(authorizer),
            )
            .await
        }
    };
    let for_alice = mint("alice").await;
    let for_carol = mint("carol").await;

    assert!((for_alice.error_code, for_alice.principal_name.as_str()) == (NONE, "alice"));
    assert!(
        for_carol
            == refusal()
                .code(DELEGATION_TOKEN_AUTHORIZATION_FAILED)
                .owner(user("carol"))
                .requester(user("bob"))
                .call()
    );
    controller.cancel().await;
}

/// Kafka's `DelegationTokenControlManager.sum` saturates at `Long.MAX_VALUE`
/// instead of refusing a lifetime that runs past it.
#[tokio::test]
async fn lifetime_past_i64_max_saturates() {
    let TokenFixture {
        directory: _dir,
        controller,
        secret,
    } = TokenFixture::new().await;
    let req = self_mint_request();

    let resp = handle(
        &req,
        &authed("alice"),
        Some(&secret),
        i64::MAX,
        1,
        &*controller,
        token_acl(&empty_super_users()),
    )
    .await;

    assert!(
        (
            resp.error_code,
            resp.expiry_timestamp_ms,
            resp.max_timestamp_ms
        ) == (NONE, resp.issue_timestamp_ms + 1, i64::MAX)
    );
    controller.cancel().await;
}

/// Kafka's configuration validation rejects a lifetime or renew period below
/// 1; a host that still passes one mints nothing.
#[tokio::test]
async fn non_positive_configured_periods_mint_nothing() {
    let TokenFixture {
        directory: _dir,
        controller,
        secret,
    } = TokenFixture::new().await;
    let req = self_mint_request();

    for (ceiling_ms, renew_ms) in [(0, RENEW_24H_MS), (60_000, 0)] {
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            ceiling_ms,
            renew_ms,
            &*controller,
            token_acl(&empty_super_users()),
        )
        .await;
        let expected = refusal().code(INVALID_REQUEST).timestamp(0).call();
        assert!(resp == expected, "ceiling {ceiling_ms} renew {renew_ms}");
    }
    assert!(controller.current_image().all_delegation_tokens().count() == 0);
    controller.cancel().await;
}

#[test]
fn token_gate_uses_delegation_token_level() {
    use krabka_metadata::metadata_version::DELEGATION_TOKEN_MIN_LEVEL;

    // (finalized metadata.version level; None = fresh image) → gated?
    let cases = [(None, false), (Some(13), true), (Some(14), false)];
    for (level, want_gated) in cases {
        assert!(
            metadata_version_gated(level, DELEGATION_TOKEN_MIN_LEVEL) == want_gated,
            "level {level:?}"
        );
    }
}
