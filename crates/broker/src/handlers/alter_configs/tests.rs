//! End-to-end tests for the `AlterConfigs` handler: the per-resource
//! authorization preamble, the resource identity an unsupported type keeps,
//! and the response an accepted broker resource produces.
//!
//! Each of them drives a live broker, so they are kept out of the module root.

use std::sync::Arc;

use assert2::assert;
use krabka_protocol::{
    UnknownTaggedFields,
    owned::alter_configs_response::{AlterConfigsResourceResponse, AlterConfigsResponse},
};

use super::{
    RESOURCE_TYPE_BROKER, RESOURCE_TYPE_TOPIC,
    test_support::{broker_resource, drive_one, resource},
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
            error_message: None,
            resource_type: RESOURCE_TYPE_TOPIC,
            resource_name: "orders".to_string(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
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
    let cases: [Audited<'_>; 5] = [
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
        (
            "a replacement that omits the controller-managed state",
            &[
                (config_keys::ELIGIBLE_LEADER_REPLICAS, "0:2,3:"),
                (config_keys::RETENTION_MS, "60000"),
            ],
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
