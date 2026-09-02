//! Tests for the `CreateDelegationToken` handler, driven against a live
//! single-voter controller so that every case also pins what the quorum
//! persists.
//!
//! The cases cover the KIP-48 preconditions, the owner-resolution matrix
//! including act-as, and the lifetime clamp that separates
//! `expiry_timestamp_ms` from `max_timestamp_ms`.

use assert2::assert;
use krabka_protocol::owned::{
    create_delegation_token_request::CreateDelegationTokenRequest,
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

#[tokio::test]
async fn returns_auth_disabled_when_no_secret_key() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let req = CreateDelegationTokenRequest::default();
    let auth = authed("alice");
    let resp = handle(
        &req,
        &auth,
        None,
        1_000,
        RENEW_24H_MS,
        &*controller,
        &empty_super_users(),
    )
    .await;
    assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    controller.cancel().await;
}

#[tokio::test]
async fn success_returns_token_id_and_hmac() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"master-key".to_vec());
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        ..Default::default()
    };
    // Broker ceiling 60s; default renew period 24h. KIP-48: the renew
    // period is clamped down to chosen_lifetime when smaller, so for
    // this 60s-ceiling case expiry == max == issue + 60s.
    let resp = handle(
        &req,
        &authed("alice"),
        Some(&secret),
        60_000,
        RENEW_24H_MS,
        &*controller,
        &empty_super_users(),
    )
    .await;
    // token_id is a random UUID; the HMAC-SHA-256 output is 32 bytes and
    // the response carries them raw.
    assert!((resp.token_id.is_empty(), resp.hmac.len()) == (false, 32));
    // 60s ceiling < 24h default renew period → both timestamps collapse
    // to issue + 60s (the chosen_lifetime ceiling).
    let expected = CreateDelegationTokenResponse {
        error_code: 0,
        principal_type: "User".into(),
        principal_name: "alice".into(),
        token_requester_principal_type: "User".into(),
        token_requester_principal_name: "alice".into(),
        issue_timestamp_ms: resp.issue_timestamp_ms,
        expiry_timestamp_ms: resp.issue_timestamp_ms + 60_000,
        max_timestamp_ms: resp.issue_timestamp_ms + 60_000,
        token_id: resp.token_id.clone(),
        hmac: resp.hmac.clone(),
        throttle_time_ms: 0,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    // Persisted in image with the same hmac + owner + timestamps.
    let img = controller.current_image();
    let stored = img
        .delegation_token_by_id(&resp.token_id)
        .expect("token in image");
    let expected_stored = krabka_metadata::DelegationToken {
        token_id: resp.token_id.clone(),
        owner: KafkaPrincipal {
            principal_type: "User".into(),
            name: "alice".into(),
        },
        hmac: resp.hmac.to_vec(),
        issue_timestamp_ms: resp.issue_timestamp_ms,
        expiry_timestamp_ms: resp.expiry_timestamp_ms,
        max_timestamp_ms: resp.max_timestamp_ms,
        renewers: vec![],
    };
    assert!(*stored == expected_stored);
    controller.cancel().await;
}

#[tokio::test]
async fn token_authenticated_caller_is_rejected_with_request_not_allowed() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        ..Default::default()
    };
    let resp = handle(
        &req,
        &authed_with_token("alice", true),
        Some(&secret),
        60_000,
        RENEW_24H_MS,
        &*controller,
        &empty_super_users(),
    )
    .await;
    assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    controller.cancel().await;
}

#[tokio::test]
async fn anonymous_caller_is_rejected_without_minting_or_returning_hmac() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        ..Default::default()
    };

    let resp = handle(
        &req,
        &anonymous(),
        Some(&secret),
        60_000,
        RENEW_24H_MS,
        &*controller,
        &empty_super_users(),
    )
    .await;

    assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    assert!(resp.hmac.is_empty());
    assert!(controller.current_image().all_delegation_tokens().count() == 0);
    controller.cancel().await;
}

#[tokio::test]
async fn max_lifetime_is_clamped_to_config_ceiling() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    // Caller requests 1 hour; broker ceiling is 5 minutes.
    let ceiling_ms = 5 * 60 * 1_000;
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: 60 * 60 * 1_000,
        ..Default::default()
    };
    let resp = handle(
        &req,
        &authed("alice"),
        Some(&secret),
        ceiling_ms,
        RENEW_24H_MS,
        &*controller,
        &empty_super_users(),
    )
    .await;
    // 5-minute ceiling < 24h default renew period → both timestamps
    // collapse to issue + ceiling.
    let expected = CreateDelegationTokenResponse {
        error_code: 0,
        principal_type: "User".into(),
        principal_name: "alice".into(),
        token_requester_principal_type: "User".into(),
        token_requester_principal_name: "alice".into(),
        issue_timestamp_ms: resp.issue_timestamp_ms,
        expiry_timestamp_ms: resp.issue_timestamp_ms + ceiling_ms,
        max_timestamp_ms: resp.issue_timestamp_ms + ceiling_ms,
        token_id: resp.token_id.clone(),
        hmac: resp.hmac.clone(),
        throttle_time_ms: 0,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    controller.cancel().await;
}

/// KIP-48 separates `expiry_timestamp_ms`, which starts at
/// `issue + min(default_renew, chosen_lifetime)`, from `max_timestamp_ms`,
/// which is `issue + chosen_lifetime`. `Renew` can therefore extend the
/// first value up to the second, instead of an exact round trip. This test
/// pins both branches of the `min`.
#[tokio::test]
async fn initial_expiry_is_default_renew_period_clamped_by_max_lifetime() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());

    let one_hour: i64 = 60 * 60 * 1_000;
    let seven_days: i64 = 7 * 24 * 60 * 60 * 1_000;
    // (broker ceiling, expected expiry delta, expected max delta), with
    // default_renew_period_ms = 24h throughout.
    let cases = [
        // Branch 1: ceiling = 1h < 24h renew period. The renew period is
        // clamped *down* to chosen_lifetime, so expiry must collapse to
        // max, and both must equal issue + 1h (the chosen_lifetime).
        (one_hour, one_hour, one_hour),
        // Branch 2: ceiling = 7d > 24h renew period. Now the renew period
        // is the smaller of the two, so expiry (issue + 24h) and max
        // (issue + 7d, the ceiling untouched) must be SEPARATE, leaving
        // room for Renew to extend expiry up to max.
        (seven_days, RENEW_24H_MS, seven_days),
    ];
    for (ceiling_ms, expiry_delta, max_delta) in cases {
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("alice"),
            Some(&secret),
            ceiling_ms,
            RENEW_24H_MS,
            &*controller,
            &empty_super_users(),
        )
        .await;
        let expected = CreateDelegationTokenResponse {
            error_code: 0,
            principal_type: "User".into(),
            principal_name: "alice".into(),
            token_requester_principal_type: "User".into(),
            token_requester_principal_name: "alice".into(),
            issue_timestamp_ms: resp.issue_timestamp_ms,
            expiry_timestamp_ms: resp.issue_timestamp_ms + expiry_delta,
            max_timestamp_ms: resp.issue_timestamp_ms + max_delta,
            token_id: resp.token_id.clone(),
            hmac: resp.hmac.clone(),
            throttle_time_ms: 0,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected, "ceiling {ceiling_ms}: {resp:?}");
    }

    controller.cancel().await;
}

#[tokio::test]
async fn invalid_lifetime_returns_invalid_request() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    // Zero is invalid (only `-1` selects the default).
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: 0,
        ..Default::default()
    };
    let resp = handle(
        &req,
        &authed("alice"),
        Some(&secret),
        60_000,
        RENEW_24H_MS,
        &*controller,
        &empty_super_users(),
    )
    .await;
    assert!(resp.error_code == crate::codes::INVALID_REQUEST);
    controller.cancel().await;
}

#[tokio::test]
async fn overflowing_lifetime_returns_invalid_request_without_minting() {
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

    assert!(resp.error_code == crate::codes::INVALID_REQUEST);
    assert!(controller.current_image().all_delegation_tokens().count() == 0);
    controller.cancel().await;
}

/// Spec §1.2 and §1.4: a super-user caller can create a token owned by a
/// different principal, by setting `owner_principal_type` and
/// `owner_principal_name`. The response names the owner *and* records the
/// original caller in the `token_requester_*` fields, so that the JVM
/// admin CLI can show "minted by X on behalf of Y".
#[tokio::test]
async fn act_as_super_user_sets_specified_owner() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        owner_principal_type: Some("User".to_string()),
        owner_principal_name: Some("alice".to_string()),
        ..Default::default()
    };
    let resp = handle(
        &req,
        &authed("admin"),
        Some(&secret),
        60_000,
        RENEW_24H_MS,
        &*controller,
        &super_users_with(&["admin"]),
    )
    .await;
    // Owner = the act-as target; requester = the caller (admin), set for
    // act-as mints. 60s ceiling < 24h renew period → expiry == max ==
    // issue + 60s.
    let expected = CreateDelegationTokenResponse {
        error_code: 0,
        principal_type: "User".into(),
        principal_name: "alice".into(),
        token_requester_principal_type: "User".into(),
        token_requester_principal_name: "admin".into(),
        issue_timestamp_ms: resp.issue_timestamp_ms,
        expiry_timestamp_ms: resp.issue_timestamp_ms + 60_000,
        max_timestamp_ms: resp.issue_timestamp_ms + 60_000,
        token_id: resp.token_id.clone(),
        hmac: resp.hmac.clone(),
        throttle_time_ms: 0,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected, "{resp:?}");
    // Persisted owner matches the response owner.
    let img = controller.current_image();
    let stored = img
        .delegation_token_by_id(&resp.token_id)
        .expect("token in image");
    let expected_stored = krabka_metadata::DelegationToken {
        token_id: resp.token_id.clone(),
        owner: KafkaPrincipal {
            principal_type: "User".into(),
            name: "alice".into(),
        },
        hmac: resp.hmac.to_vec(),
        issue_timestamp_ms: resp.issue_timestamp_ms,
        expiry_timestamp_ms: resp.expiry_timestamp_ms,
        max_timestamp_ms: resp.max_timestamp_ms,
        renewers: vec![],
    };
    assert!(*stored == expected_stored);
    controller.cancel().await;
}

/// Spec §1.2: act-as is privileged. A caller that is NOT in `super_users`
/// and that tries act-as gets `DELEGATION_TOKEN_AUTHORIZATION_FAILED`
/// (65). The broker separates "you are not allowed to do this" (65) from
/// "your request is malformed" (42).
#[tokio::test]
async fn act_as_non_super_user_rejected_with_authorization_failed() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        owner_principal_type: Some("User".to_string()),
        owner_principal_name: Some("alice".to_string()),
        ..Default::default()
    };
    let resp = handle(
        &req,
        // `bob` is not in the super-users set.
        &authed("bob"),
        Some(&secret),
        60_000,
        RENEW_24H_MS,
        &*controller,
        &super_users_with(&["admin"]),
    )
    .await;
    assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTHORIZATION_FAILED);
    controller.cancel().await;
}

/// Spec §1.2: act-as needs BOTH `owner_principal_type` and
/// `owner_principal_name`. A partial state is never valid, even for a
/// super-user. The broker returns `INVALID_REQUEST` (42), because the
/// request is malformed and not unauthorized.
#[tokio::test]
async fn act_as_with_only_one_field_set_returns_invalid_request() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());

    let cases = [
        // Type set but name empty.
        ("name missing", Some("User".to_string()), None),
        // Name set but type empty.
        ("type missing", None, Some("alice".to_string())),
    ];
    for (case, owner_principal_type, owner_principal_name) in cases {
        let req = CreateDelegationTokenRequest {
            max_lifetime_ms: -1,
            owner_principal_type,
            owner_principal_name,
            ..Default::default()
        };
        let resp = handle(
            &req,
            &authed("admin"),
            Some(&secret),
            60_000,
            RENEW_24H_MS,
            &*controller,
            &super_users_with(&["admin"]),
        )
        .await;
        assert!(resp.error_code == crate::codes::INVALID_REQUEST, "{case}");
    }

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

/// Spec §1.2: only `User` is valid as the act-as owner type, because
/// mTLS-DN owners are not supported. Any other type from a super-user
/// gives `INVALID_REQUEST` (42), because the request is syntactically
/// wrong and not unauthorized.
#[tokio::test]
async fn act_as_with_non_user_principal_type_returns_invalid_request() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let secret = SecretBytes::new(b"k".to_vec());
    let req = CreateDelegationTokenRequest {
        max_lifetime_ms: -1,
        owner_principal_type: Some("Group".to_string()),
        owner_principal_name: Some("eng".to_string()),
        ..Default::default()
    };
    let resp = handle(
        &req,
        &authed("admin"),
        Some(&secret),
        60_000,
        RENEW_24H_MS,
        &*controller,
        &super_users_with(&["admin"]),
    )
    .await;
    assert!(resp.error_code == crate::codes::INVALID_REQUEST);
    controller.cancel().await;
}
