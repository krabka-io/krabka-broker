//! End-to-end tests for the `AlterConfigs` handler: the per-resource
//! authorization preamble, the resource identity an unsupported type keeps,
//! and the response an accepted broker resource produces.
//!
//! Each of them drives a live broker, so they are kept out of the module root.

use std::sync::Arc;

use assert2::{assert, check};
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        alter_configs_request::AlterableConfig,
        alter_configs_response::{AlterConfigsResourceResponse, AlterConfigsResponse},
    },
};

use super::{
    RESOURCE_TYPE_BROKER, RESOURCE_TYPE_CLIENT_METRICS, RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
    test_support::{
        broker_resource, client_metrics_resource, drive_many, drive_one, group_resource, resource,
    },
};
use crate::{codes, test_support::DenyAll};

#[tokio::test]
async fn handle_preserves_resource_identity_for_unsupported_type() {
    let resp = Box::pin(drive_one(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        resource(77, "mystery"),
    ))
    .await;

    let expected = AlterConfigsResponse {
        throttle_time_ms: 0,
        responses: vec![AlterConfigsResourceResponse {
            error_code: codes::INVALID_REQUEST,
            error_message: Some("resource_type=77 not supported".to_string()),
            resource_type: 77,
            resource_name: "mystery".to_string(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
}

#[tokio::test]
async fn topic_resource_denial_uses_topic_authorization_error() {
    let resp = Box::pin(drive_one(
        Arc::new(DenyAll),
        resource(RESOURCE_TYPE_TOPIC, "orders"),
    ))
    .await;

    let expected = AlterConfigsResponse {
        throttle_time_ms: 0,
        responses: vec![AlterConfigsResourceResponse {
            error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            error_message: Some("Topic authorization failed.".to_string()),
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "orders".to_string(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
}

/// KIP-1017 / dynamic broker config extensions: legacy `AlterConfigs`
/// authorizes GROUP resources against `AlterConfigs` on `Group(name)`, the
/// same target `IncrementalAlterConfigs` uses.
#[tokio::test]
async fn group_resource_denial_uses_group_authorization_error() {
    let resp = Box::pin(drive_one(
        Arc::new(DenyAll),
        group_resource("streams-app", &[]),
    ))
    .await;

    let expected = AlterConfigsResponse {
        throttle_time_ms: 0,
        responses: vec![AlterConfigsResourceResponse {
            error_code: codes::GROUP_AUTHORIZATION_FAILED,
            error_message: Some("Group authorization failed.".to_string()),
            resource_type: RESOURCE_TYPE_GROUP,
            resource_name: "streams-app".to_string(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
}

/// `CLIENT_METRICS` is authorized against the cluster, like `BROKER`, but goes
/// through the controller path that carries `Errors.message()`.
#[tokio::test]
async fn client_metrics_resource_denial_uses_cluster_authorization_error_with_message() {
    let resp = Box::pin(drive_one(
        Arc::new(DenyAll),
        client_metrics_resource("sub-a", &[]),
    ))
    .await;

    let expected = AlterConfigsResponse {
        throttle_time_ms: 0,
        responses: vec![AlterConfigsResourceResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: Some("Cluster authorization failed.".to_string()),
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".to_string(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
}

#[tokio::test]
async fn authorized_group_resource_is_applied() {
    let resp = Box::pin(drive_one(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        group_resource(
            "streams-app",
            &[(
                crate::coordinator::unified::streams::config::KEY_NUM_STANDBY_REPLICAS,
                "1",
            )],
        ),
    ))
    .await;

    let expected = AlterConfigsResponse {
        throttle_time_ms: 0,
        responses: vec![AlterConfigsResourceResponse {
            error_code: codes::NONE,
            error_message: None,
            resource_type: RESOURCE_TYPE_GROUP,
            resource_name: "streams-app".to_string(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
}

#[tokio::test]
async fn authorized_client_metrics_resource_is_applied() {
    let resp = Box::pin(drive_one(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        client_metrics_resource("sub-a", &[("interval.ms", "60000")]),
    ))
    .await;

    let expected = AlterConfigsResponse {
        throttle_time_ms: 0,
        responses: vec![AlterConfigsResourceResponse {
            error_code: codes::NONE,
            error_message: None,
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".to_string(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
}

/// A resource that appears twice in the request is a Kafka `preprocess`
/// shape error, so it earns `INVALID_REQUEST` on both rows -- even for a
/// principal `DenyAll` would otherwise refuse for a different reason. This is
/// the validate-before-authorize ordering the legacy handler now matches.
#[tokio::test]
async fn duplicate_resource_reference_is_a_validation_error_even_when_unauthorized() {
    let resp = Box::pin(drive_many(
        Arc::new(DenyAll),
        vec![
            resource(RESOURCE_TYPE_TOPIC, "orders"),
            resource(RESOURCE_TYPE_TOPIC, "orders"),
        ],
    ))
    .await;

    assert!(resp.responses.len() == 2);
    for row in &resp.responses {
        check!(row.error_code == codes::INVALID_REQUEST);
        check!(row.error_message.as_deref() == Some("Each resource must appear at most once."));
        check!(row.resource_type == RESOURCE_TYPE_TOPIC);
        check!(row.resource_name == "orders");
    }
}

/// Duplicate config keys within one resource are a shape error, checked
/// before authorization runs.
#[tokio::test]
async fn duplicate_config_key_is_a_validation_error_even_when_unauthorized() {
    let mut duplicated = resource(RESOURCE_TYPE_TOPIC, "orders");
    duplicated.configs.push(duplicated.configs[0].clone());

    let resp = Box::pin(drive_one(Arc::new(DenyAll), duplicated)).await;

    assert!(resp.responses.len() == 1);
    let row = &resp.responses[0];
    assert!(row.error_code == codes::INVALID_REQUEST);
    assert!(row.error_message.as_deref() == Some("Error due to duplicate config keys"));
}

/// A null config value is a shape error for legacy `AlterConfigs`: unlike
/// `IncrementalAlterConfigs`, there is no DELETE operation that a null value
/// could mean, so it is simply malformed. Checked before authorization.
#[tokio::test]
async fn null_config_value_is_a_validation_error_even_when_unauthorized() {
    let mut resource = resource(RESOURCE_TYPE_TOPIC, "orders");
    resource.configs = vec![AlterableConfig {
        name: "retention.ms".into(),
        value: None,
        ..Default::default()
    }];

    let resp = Box::pin(drive_one(Arc::new(DenyAll), resource)).await;

    assert!(resp.responses.len() == 1);
    let row = &resp.responses[0];
    assert!(row.error_code == codes::INVALID_REQUEST);
    assert!(row.error_message.as_deref() == Some("Null value not supported for : retention.ms"));
}

/// A valid, uniquely-named resource with well-formed configs still gets an
/// authorization check: the validate-first ordering does not skip
/// authorization for a resource that passes validation.
#[tokio::test]
async fn valid_resource_still_gets_an_authorization_check() {
    for (label, req_resource, want_code) in [
        (
            "topic",
            resource(RESOURCE_TYPE_TOPIC, "orders"),
            codes::TOPIC_AUTHORIZATION_FAILED,
        ),
        (
            "group",
            group_resource("streams-app", &[]),
            codes::GROUP_AUTHORIZATION_FAILED,
        ),
        (
            "client-metrics",
            client_metrics_resource("sub-a", &[]),
            codes::CLUSTER_AUTHORIZATION_FAILED,
        ),
        (
            "broker",
            resource(RESOURCE_TYPE_BROKER, "1"),
            codes::CLUSTER_AUTHORIZATION_FAILED,
        ),
    ] {
        let resp = Box::pin(drive_one(Arc::new(DenyAll), req_resource)).await;
        assert!(resp.responses.len() == 1);
        check!(resp.responses[0].error_code == want_code, "{label}");
    }
}

#[tokio::test]
async fn broker_resource_denial_uses_cluster_authorization_error() {
    let resp = Box::pin(drive_one(
        Arc::new(DenyAll),
        resource(RESOURCE_TYPE_BROKER, "1"),
    ))
    .await;

    let expected = AlterConfigsResponse {
        throttle_time_ms: 0,
        responses: vec![AlterConfigsResourceResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            error_message: None,
            resource_type: RESOURCE_TYPE_BROKER,
            resource_name: "1".to_string(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
}

#[tokio::test]
async fn authorized_broker_resource_is_applied() {
    let resp = Box::pin(drive_one(
        Arc::new(crate::authorizer::AllowAllAuthorizer),
        broker_resource("1", &[(crate::throttle::LEADER_THROTTLED_RATE_KEY, "2048")]),
    ))
    .await;

    let expected = AlterConfigsResponse {
        throttle_time_ms: 0,
        responses: vec![AlterConfigsResourceResponse {
            error_code: codes::NONE,
            error_message: None,
            resource_type: RESOURCE_TYPE_BROKER,
            resource_name: "1".to_string(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
}

/// A topic `AlterConfigs` replaces the whole override map, so the audit record
/// has to name the keys the request deletes by omitting them as well as the
/// ones it writes. A key restated at its stored value changed nothing, and a
/// controller-managed key the record builder carries forward was never the
/// client's to delete.
///
/// Keys only: a config value can be a password or a key store path, and none
/// of them reach the record.
#[test]
fn a_topic_replacement_audits_every_key_whose_value_moves() {
    use crate::{config_keys, handlers::audit_resource};

    /// The topic's stored overrides, the complete replacement the request
    /// carries, and the audit resources it earns.
    type Audited<'a> = (
        &'a str,
        &'a [(&'a str, &'a str)],
        &'a [(&'a str, &'a str)],
        Vec<krabka_audit::AuditResource>,
    );

    let topic = |keys: &[&str]| {
        let mut expected = vec![audit_resource("Topic", "orders")];
        expected.extend(keys.iter().map(|key| audit_resource("ConfigKey", *key)));
        expected
    };
    let cases: [Audited<'_>; 4] = [
        (
            "a replacement that drops a stored key",
            &[
                (config_keys::RETENTION_MS, "60000"),
                (config_keys::CLEANUP_POLICY, "compact"),
            ],
            &[(config_keys::RETENTION_MS, "60000")],
            topic(&[config_keys::CLEANUP_POLICY]),
        ),
        (
            "a replacement that changes a stored value",
            &[(config_keys::RETENTION_MS, "60000")],
            &[(config_keys::RETENTION_MS, "120000")],
            topic(&[config_keys::RETENTION_MS]),
        ),
        (
            "a replacement that adds a key",
            &[],
            &[(config_keys::RETENTION_MS, "60000")],
            topic(&[config_keys::RETENTION_MS]),
        ),
        (
            "a replacement that restates every stored value",
            &[(config_keys::RETENTION_MS, "60000")],
            &[(config_keys::RETENTION_MS, "60000")],
            topic(&[]),
        ),
    ];

    for (label, stored, replacement, expected) in cases {
        let image =
            crate::handlers::alter_configs::test_support::image_with_topic_config("orders", stored);

        let audited = super::audit_resources_for(
            &crate::handlers::alter_configs::test_support::topic_resource("orders", replacement),
            &image,
        );

        assert!(audited == expected, "{label}");
    }
}
