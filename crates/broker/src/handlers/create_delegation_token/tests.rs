//! Tests for the `CreateDelegationToken` handler, driven against a live
//! single-voter controller so that every case also pins what the quorum
//! persists.
//!
//! The cases cover Kafka's refusal order and shapes, owner
//! resolution, and the lifetime rules that separate `expiry_timestamp_ms`
//! from `max_timestamp_ms`.

use assert2::assert;
use krabka_protocol::owned::{
    create_delegation_token_request::{CreatableRenewers, CreateDelegationTokenRequest},
    create_delegation_token_response::CreateDelegationTokenResponse,
};
use krabka_security::{KafkaPrincipal, SecretBytes};
use tempfile::TempDir;

use super::{
    test_support::{
        RENEW_24H_MS, anonymous, authed, authed_with_token, empty_super_users, super_users_with,
        test_controller,
    },
    *,
};

fn principal(principal_type: &str, name: &str) -> KafkaPrincipal {
    KafkaPrincipal {
        principal_type: principal_type.into(),
        name: name.into(),
    }
}

fn user(name: &str) -> KafkaPrincipal {
    principal("User", name)
}

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

/// The response a refusal carries: `code`, both principals, and `timestamp`
/// in all three timestamp fields (`-1` on the broker, `0` from the
/// controller).
fn refusal(
    code: i16,
    owner: &KafkaPrincipal,
    requester: &KafkaPrincipal,
    timestamp: i64,
) -> CreateDelegationTokenResponse {
    CreateDelegationTokenResponse {
        error_code: code,
        principal_type: owner.principal_type.clone(),
        principal_name: owner.name.clone(),
        token_requester_principal_type: requester.principal_type.clone(),
        token_requester_principal_name: requester.name.clone(),
        issue_timestamp_ms: timestamp,
        expiry_timestamp_ms: timestamp,
        max_timestamp_ms: timestamp,
        ..Default::default()
    }
}

/// `KafkaApis.handleCreateTokenRequest` checks `allowTokenRequests` (64),
/// then the `CreateTokens` authorization (65), then the renewer types (67),
/// all before the controller's `DELEGATION_TOKEN_AUTH_DISABLED` (61). No
/// refusal mints a token, and each names the owner and the requester.
#[tokio::test]
async fn refusals_follow_kafka_order_and_name_both_principals() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    let self_mint = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        ..Default::default()
    };
    let group_renewer = CreateDelegationTokenRequest {
        renewers: vec![renewer("User", "bob"), renewer("Group", "eng")],
        ..self_mint.clone()
    };
    let not_allowed = crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED;
    let unauthorized = crate::codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED;
    let bad_renewer = crate::codes::INVALID_PRINCIPAL_TYPE;
    let disabled = crate::codes::DELEGATION_TOKEN_AUTH_DISABLED;

    // (case, requester, tokens enabled, request, super users, response)
    let cases = [
        (
            "token-authenticated requester",
            authed_with_token("alice", true),
            true,
            self_mint.clone(),
            empty_super_users(),
            refusal(not_allowed, &user("alice"), &user("alice"), -1),
        ),
        (
            "token-authenticated super user acting as another owner",
            authed_with_token("admin", true),
            true,
            act_as("User", "alice"),
            super_users_with(&["admin"]),
            refusal(not_allowed, &user("alice"), &user("admin"), -1),
        ),
        (
            "anonymous requester",
            anonymous(),
            true,
            self_mint.clone(),
            empty_super_users(),
            refusal(not_allowed, &user("ANONYMOUS"), &user("ANONYMOUS"), -1),
        ),
        (
            "token-authenticated requester with tokens disabled",
            authed_with_token("alice", true),
            false,
            self_mint.clone(),
            empty_super_users(),
            refusal(not_allowed, &user("alice"), &user("alice"), -1),
        ),
        (
            "another owner without authorization",
            authed("bob"),
            true,
            act_as("User", "alice"),
            super_users_with(&["admin"]),
            refusal(unauthorized, &user("alice"), &user("bob"), -1),
        ),
        (
            "another owner of another type without authorization, tokens disabled",
            authed("bob"),
            false,
            act_as("Group", "eng"),
            empty_super_users(),
            refusal(unauthorized, &principal("Group", "eng"), &user("bob"), -1),
        ),
        (
            "renewer that is not a User",
            authed("alice"),
            true,
            group_renewer.clone(),
            empty_super_users(),
            refusal(bad_renewer, &user("alice"), &user("alice"), -1),
        ),
        (
            "renewer that is not a User, tokens disabled",
            authed("alice"),
            false,
            group_renewer,
            empty_super_users(),
            refusal(bad_renewer, &user("alice"), &user("alice"), -1),
        ),
        (
            "tokens disabled",
            authed("alice"),
            false,
            act_as("User", "alice"),
            empty_super_users(),
            refusal(disabled, &user("alice"), &user("alice"), 0),
        ),
    ];
    for (case, auth, enabled, req, super_users, expected) in cases {
        let resp = handle(
            &req,
            &auth,
            enabled.then_some(&secret),
            60_000,
            RENEW_24H_MS,
            &*controller,
            &super_users,
        )
        .await;
        assert!(resp == expected, "{case}");
    }
    assert!(controller.current_image().all_delegation_tokens().count() == 0);
    controller.cancel().await;
}

/// Owner resolution and Kafka's lifetime rules: a null or empty owner
/// name means the requester, naming the requester needs no authorization, any
/// owner type is kept, a `max_lifetime_ms` of 0 or less takes the configured
/// ceiling, and the first expiry is `issue + min(renew period, lifetime)`.
#[tokio::test]
async fn mints_for_the_resolved_owner_with_kafka_deadlines() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"master-key".to_vec());
    let one_hour: i64 = 60 * 60 * 1_000;
    let seven_days: i64 = 7 * 24 * one_hour;
    let with_lifetime = |max_lifetime_ms| CreateDelegationTokenRequest {
        max_lifetime_ms,
        ..Default::default()
    };
    let name_only = CreateDelegationTokenRequest {
        owner_principal_name: Some("alice".into()),
        owner_principal_type: None,
        max_lifetime_ms: -1,
        ..Default::default()
    };
    let type_only = CreateDelegationTokenRequest {
        owner_principal_type: Some("User".into()),
        owner_principal_name: Some(String::new()),
        max_lifetime_ms: -1,
        ..Default::default()
    };
    let with_renewer = CreateDelegationTokenRequest {
        renewers: vec![renewer("User", "bob")],
        ..with_lifetime(-1)
    };

    // (case, requester, super users, request, broker ceiling, owner,
    //  renewers, expiry delta, max delta)
    let cases = [
        (
            "default lifetime under the renew period",
            "alice",
            empty_super_users(),
            with_lifetime(-1),
            60_000,
            user("alice"),
            vec![],
            60_000,
            60_000,
        ),
        (
            "zero lifetime takes the ceiling",
            "alice",
            empty_super_users(),
            with_lifetime(0),
            seven_days,
            user("alice"),
            vec![],
            RENEW_24H_MS,
            seven_days,
        ),
        (
            "negative lifetime takes the ceiling",
            "alice",
            empty_super_users(),
            with_lifetime(-5),
            seven_days,
            user("alice"),
            vec![],
            RENEW_24H_MS,
            seven_days,
        ),
        (
            "request above the ceiling",
            "alice",
            empty_super_users(),
            with_lifetime(seven_days),
            one_hour,
            user("alice"),
            vec![],
            one_hour,
            one_hour,
        ),
        (
            "request below the ceiling",
            "alice",
            empty_super_users(),
            with_lifetime(one_hour),
            seven_days,
            user("alice"),
            vec![],
            one_hour,
            one_hour,
        ),
        (
            "owner named as the requester",
            "alice",
            empty_super_users(),
            act_as("User", "alice"),
            seven_days,
            user("alice"),
            vec![],
            RENEW_24H_MS,
            seven_days,
        ),
        (
            "empty owner name means the requester",
            "alice",
            empty_super_users(),
            type_only,
            seven_days,
            user("alice"),
            vec![],
            RENEW_24H_MS,
            seven_days,
        ),
        (
            "super user for another owner",
            "admin",
            super_users_with(&["admin"]),
            act_as("User", "alice"),
            60_000,
            user("alice"),
            vec![],
            60_000,
            60_000,
        ),
        (
            "super user for another owner type",
            "admin",
            super_users_with(&["admin"]),
            act_as("Group", "eng"),
            60_000,
            principal("Group", "eng"),
            vec![],
            60_000,
            60_000,
        ),
        (
            "owner name without a type",
            "admin",
            super_users_with(&["admin"]),
            name_only,
            60_000,
            principal("", "alice"),
            vec![],
            60_000,
            60_000,
        ),
        (
            "User renewer",
            "alice",
            empty_super_users(),
            with_renewer,
            60_000,
            user("alice"),
            vec![user("bob")],
            60_000,
            60_000,
        ),
    ];
    for (case, requester, super_users, req, ceiling_ms, owner, renewers, expiry_delta, max_delta) in
        cases
    {
        let resp = handle(
            &req,
            &authed(requester),
            Some(&secret),
            ceiling_ms,
            RENEW_24H_MS,
            &*controller,
            &super_users,
        )
        .await;
        // The token id is a random UUID and the HMAC-SHA-256 output is 32
        // bytes; the response carries both raw.
        assert!(
            (resp.token_id.is_empty(), resp.hmac.len()) == (false, 32),
            "{case}"
        );
        let expected = CreateDelegationTokenResponse {
            issue_timestamp_ms: resp.issue_timestamp_ms,
            expiry_timestamp_ms: resp.issue_timestamp_ms + expiry_delta,
            max_timestamp_ms: resp.issue_timestamp_ms + max_delta,
            token_id: resp.token_id.clone(),
            hmac: resp.hmac.clone(),
            ..refusal(crate::codes::NONE, &owner, &user(requester), 0)
        };
        assert!(resp == expected, "{case}");

        let image = controller.current_image();
        let stored = image
            .delegation_token_by_id(&resp.token_id)
            .expect("token in image");
        let expected_stored = krabka_metadata::DelegationToken {
            token_id: resp.token_id.clone(),
            owner,
            hmac: resp.hmac.to_vec(),
            issue_timestamp_ms: resp.issue_timestamp_ms,
            expiry_timestamp_ms: resp.expiry_timestamp_ms,
            max_timestamp_ms: resp.max_timestamp_ms,
            renewers,
        };
        assert!(*stored == expected_stored, "{case}");
    }
    controller.cancel().await;
}

/// Kafka's `DelegationTokenControlManager.sum` saturates at `Long.MAX_VALUE`
/// instead of refusing a lifetime that runs past it.
#[tokio::test]
async fn lifetime_past_i64_max_saturates() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        ..Default::default()
    };

    let resp = handle(
        &req,
        &authed("alice"),
        Some(&secret),
        i64::MAX,
        1,
        &*controller,
        &empty_super_users(),
    )
    .await;

    assert!(
        (
            resp.error_code,
            resp.expiry_timestamp_ms,
            resp.max_timestamp_ms
        ) == (crate::codes::NONE, resp.issue_timestamp_ms + 1, i64::MAX)
    );
    controller.cancel().await;
}

/// Kafka's configuration validation rejects a lifetime or renew period below
/// 1; a host that still passes one mints nothing.
#[tokio::test]
async fn non_positive_configured_periods_mint_nothing() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        ..Default::default()
    };

    for (ceiling_ms, renew_ms) in [(0, RENEW_24H_MS), (60_000, 0)] {
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            ceiling_ms,
            renew_ms,
            &*controller,
            &empty_super_users(),
        )
        .await;
        let expected = refusal(
            crate::codes::INVALID_REQUEST,
            &user("alice"),
            &user("alice"),
            0,
        );
        assert!(resp == expected, "ceiling {ceiling_ms} renew {renew_ms}");
    }
    assert!(controller.current_image().all_delegation_tokens().count() == 0);
    controller.cancel().await;
}

#[test]
fn token_gate_uses_delegation_token_level() {
    use krabka_metadata::{
        FeatureLevelRecord, MetadataImage, MetadataRecord,
        metadata_version::DELEGATION_TOKEN_MIN_LEVEL,
    };

    let gate = |level: Option<i16>| {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        if let Some(level) = level {
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crate::features::METADATA_VERSION.to_string(),
                level,
            }));
        }
        crate::features::require_feature(
            &image,
            crate::features::METADATA_VERSION,
            DELEGATION_TOKEN_MIN_LEVEL,
        )
        .is_err()
    };

    // (finalized metadata.version level; None = fresh image) → gated?
    let cases = [(None, false), (Some(13), true), (Some(14), false)];
    for (level, want_gated) in cases {
        assert!(gate(level) == want_gated, "level {level:?}");
    }
}
