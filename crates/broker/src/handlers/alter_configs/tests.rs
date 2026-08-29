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
            error_code: codes::INVALID_RESOURCE_TYPE,
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
