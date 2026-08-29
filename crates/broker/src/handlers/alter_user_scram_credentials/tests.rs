//! Tests for the whole-request gates of the `AlterUserScramCredentials`
//! handler and for the submit of an accepted request.
//!
//! They cover the cluster authorization preamble, the `metadata.version`
//! feature gate, and the metadata image a successful request leaves behind.
//! Most of them drive a live broker, so they are kept out of the module root.

use std::{net::SocketAddr, sync::Arc};

use assert2::assert;
use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
use krabka_protocol::{
    UnknownTaggedFields, owned::alter_user_scram_credentials_request::ScramCredentialDeletion,
};
use krabka_security::{AuthMethod, Principal, SaslMechanism, scram::MIN_SCRAM_ITERATIONS};

use super::*;
use crate::{
    handlers::alter_user_scram_credentials::test_support::{
        deletion, expected_result, start_broker, test_context, valid_upsertion, wait_for_leader,
    },
    test_support::DenyAll,
};

#[test]
fn scram_gate_permits_unknown_and_at_or_above_level() {
    use krabka_metadata::{
        FeatureLevelRecord, MetadataImage, MetadataRecord, metadata_version::SCRAM_MIN_LEVEL,
    };

    let gate = |level: Option<i16>| {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        if let Some(level) = level {
            image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crate::features::METADATA_VERSION.to_string(),
                level,
            }));
        }
        crate::features::require_feature(&image, crate::features::METADATA_VERSION, SCRAM_MIN_LEVEL)
            .is_err()
    };

    let cases = [
        // No finalized metadata.version — gate permits.
        (None, false),
        // Below SCRAM_MIN_LEVEL — gate rejects.
        (Some(10), true),
        // At SCRAM_MIN_LEVEL — gate permits.
        (Some(11), false),
    ];
    for (level, want_err) in cases {
        assert!(gate(level) == want_err, "level {level:?}");
    }
}

#[tokio::test]
async fn handle_denies_invalid_rows_before_scram_validation() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let mut invalid_upsertion = valid_upsertion("bob");
    invalid_upsertion.iterations = MIN_SCRAM_ITERATIONS - 1;
    let req = AlterUserScramCredentialsRequest {
        deletions: vec![ScramCredentialDeletion {
            name: "alice".into(),
            mechanism: 99,
            ..Default::default()
        }],
        upsertions: vec![invalid_upsertion, valid_upsertion("bob")],
        ..Default::default()
    };

    let resp = handle(&broker, req, &ctx).await;

    let expected = AlterUserScramCredentialsResponse {
        throttle_time_ms: 0,
        results: vec![
            expected_result(
                "alice",
                codes::CLUSTER_AUTHORIZATION_FAILED,
                Some("not super-user"),
            ),
            expected_result(
                "bob",
                codes::CLUSTER_AUTHORIZATION_FAILED,
                Some("not super-user"),
            ),
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_authorizes_and_persists_valid_upsertion() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![valid_upsertion("alice")],
        ..Default::default()
    };

    let resp = handle(&broker, req, &ctx).await;

    let expected = AlterUserScramCredentialsResponse {
        throttle_time_ms: 0,
        results: vec![expected_result("alice", 0, None)],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    let image = broker.controller.current_image();
    assert!(
        image
            .scram_credential("alice", SaslMechanism::ScramSha256)
            .is_some()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_denies_valid_upsertion_without_cluster_alter() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let req = AlterUserScramCredentialsRequest {
        upsertions: vec![valid_upsertion("alice")],
        ..Default::default()
    };

    let resp = handle(&broker, req, &ctx).await;

    let expected = AlterUserScramCredentialsResponse {
        throttle_time_ms: 0,
        results: vec![expected_result(
            "alice",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("not super-user"),
        )],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    let image = broker.controller.current_image();
    assert!(
        image
            .scram_credential("alice", SaslMechanism::ScramSha256)
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_unsupported_metadata_version_reports_every_requested_user() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crate::features::METADATA_VERSION.to_string(),
            level: krabka_metadata::metadata_version::SCRAM_MIN_LEVEL - 1,
        })])
        .await
        .expect("seed low metadata.version");
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let req = AlterUserScramCredentialsRequest {
        deletions: vec![deletion("alice")],
        upsertions: vec![valid_upsertion("bob")],
        ..Default::default()
    };

    let resp = handle(&broker, req, &ctx).await;

    let msg = "SCRAM is not enabled at the cluster's metadata.version.";
    let expected = AlterUserScramCredentialsResponse {
        throttle_time_ms: 0,
        results: vec![
            expected_result("alice", codes::UNSUPPORTED_VERSION, Some(msg)),
            expected_result("bob", codes::UNSUPPORTED_VERSION, Some(msg)),
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_low_metadata_version_denied_request_reports_authorization_per_distinct_user() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crate::features::METADATA_VERSION.to_string(),
            level: krabka_metadata::metadata_version::SCRAM_MIN_LEVEL - 1,
        })])
        .await
        .expect("seed low metadata.version");
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let mut invalid_upsertion = valid_upsertion("bob");
    invalid_upsertion.iterations = MIN_SCRAM_ITERATIONS - 1;
    let req = AlterUserScramCredentialsRequest {
        deletions: vec![ScramCredentialDeletion {
            name: "alice".into(),
            mechanism: 99,
            ..Default::default()
        }],
        upsertions: vec![invalid_upsertion, valid_upsertion("bob")],
        ..Default::default()
    };

    let resp = handle(&broker, req, &ctx).await;

    let expected = AlterUserScramCredentialsResponse {
        throttle_time_ms: 0,
        results: vec![
            expected_result(
                "alice",
                codes::CLUSTER_AUTHORIZATION_FAILED,
                Some("not super-user"),
            ),
            expected_result(
                "bob",
                codes::CLUSTER_AUTHORIZATION_FAILED,
                Some("not super-user"),
            ),
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_low_metadata_version_authorized_request_deduplicates_unsupported_users() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crate::features::METADATA_VERSION.to_string(),
            level: krabka_metadata::metadata_version::SCRAM_MIN_LEVEL - 1,
        })])
        .await
        .expect("seed low metadata.version");
    let principal = Principal {
        name: "admin".into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    };
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    let ctx = test_context(&principal, &peer);
    let req = AlterUserScramCredentialsRequest {
        deletions: vec![deletion("alice")],
        upsertions: vec![
            valid_upsertion("bob"),
            valid_upsertion("bob"),
            valid_upsertion("alice"),
        ],
        ..Default::default()
    };

    let resp = handle(&broker, req, &ctx).await;

    let msg = "SCRAM is not enabled at the cluster's metadata.version.";
    let expected = AlterUserScramCredentialsResponse {
        throttle_time_ms: 0,
        results: vec![
            expected_result("alice", codes::UNSUPPORTED_VERSION, Some(msg)),
            expected_result("bob", codes::UNSUPPORTED_VERSION, Some(msg)),
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}
