//! End-to-end tests of the `CreateTopics` handler, driven over the wire
//! encoding against a running broker: the authorization gate, the per-topic
//! error rows, a successful create, and the KIP-599 mutation quota.

use std::{net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use krabka_metadata::MetadataRecord;
use krabka_protocol::{
    UnknownTaggedFields,
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
    },
};
use krabka_security::Principal;

use super::*;
use crate::{
    broker::BrokerHandle,
    test_support::{DenyAll, peer, principal},
};

const VERSION: i16 = 7;

fn topic(name: &str, partitions: i32, rf: i16) -> CreatableTopic {
    CreatableTopic {
        name: name.into(),
        num_partitions: partitions,
        replication_factor: rf,
        ..Default::default()
    }
}

fn topic_with_config(name: &str) -> CreatableTopic {
    CreatableTopic {
        configs: vec![CreatableTopicConfig {
            name: "retention.ms".into(),
            value: Some("60000".into()),
            ..Default::default()
        }],
        ..topic(name, 2, 1)
    }
}

fn topic_with_configs(name: &str, configs: &[(&str, &str)]) -> CreatableTopic {
    CreatableTopic {
        configs: configs
            .iter()
            .map(|(key, value)| CreatableTopicConfig {
                name: (*key).into(),
                value: Some((*value).into()),
                ..Default::default()
            })
            .collect(),
        ..topic(name, 1, 1)
    }
}

fn request(topics: Vec<CreatableTopic>) -> CreateTopicsRequest {
    CreateTopicsRequest {
        topics,
        timeout_ms: 5_000,
        ..Default::default()
    }
}

crate::test_support::wire_helpers!(
    CreateTopicsRequest,
    CreateTopicsResponse,
    version = VERSION,
    client_id = "admin-client"
);

use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

async fn drive(
    broker: &Broker,
    req: &CreateTopicsRequest,
    principal: &Principal,
    peer: &SocketAddr,
) -> CreateTopicsResponse {
    let ctx = test_context(principal, peer);
    let req_bytes = encode_request(req);
    let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
        .await
        .expect("handle");
    decode_response(&bytes)
}

async fn seed_controller_quota(handle: &BrokerHandle, rate: f64) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1ClientQuota(
            krabka_metadata::ClientQuotaRecord {
                entity: vec![
                    krabka_metadata::QuotaEntity {
                        entity_type: "user".into(),
                        entity_name: Some("admin".into()),
                    },
                    krabka_metadata::QuotaEntity {
                        entity_type: "client-id".into(),
                        entity_name: Some("admin-client".into()),
                    },
                ],
                config_key: "controller_mutation_rate".into(),
                config_value: Some(rate),
            },
        )])
        .await
        .expect("seed quota");
}

#[tokio::test]
async fn handle_denies_cluster_create_for_each_topic() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("alice");
    let peer = peer();
    let req = request(vec![topic("orders", 1, 1), topic("payments", 1, 1)]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![
            CreatableTopicResult {
                name: "orders".into(),
                topic_id: ProtoUuid([0; 16]),
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("create-topics denied".into()),
                num_partitions: -1,
                replication_factor: -1,
                configs: None,
                topic_config_error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            CreatableTopicResult {
                name: "payments".into(),
                topic_id: ProtoUuid([0; 16]),
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("create-topics denied".into()),
                num_partitions: -1,
                replication_factor: -1,
                configs: None,
                topic_config_error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic("orders")
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_reports_invalid_partition_count_and_replication_factor() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic("bad-count", 0, 1), topic("bad-rf", 1, 2)]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![
            CreatableTopicResult {
                name: "bad-count".into(),
                topic_id: ProtoUuid([0; 16]),
                error_code: codes::INVALID_PARTITIONS,
                error_message: None,
                num_partitions: -1,
                replication_factor: -1,
                configs: None,
                topic_config_error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            CreatableTopicResult {
                name: "bad-rf".into(),
                topic_id: ProtoUuid([0; 16]),
                error_code: codes::INVALID_REPLICATION_FACTOR,
                error_message: None,
                num_partitions: -1,
                replication_factor: -1,
                configs: None,
                topic_config_error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    for name in ["bad-count", "bad-rf"] {
        let image = broker_handle.controller_image_for_test();
        assert!(image.topic(name).is_none(), "topic {name} not committed");
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_success_persists_topic_config_and_success_fields() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_with_config("configured")]);

    let resp = drive(&broker, &req, &p, &peer).await;

    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].topic_id != ProtoUuid([0; 16]));
    let expected = CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![CreatableTopicResult {
            name: "configured".into(),
            // Randomly generated per create; copied from the actual
            // response (the != nil assert above pins non-default).
            topic_id: resp.topics[0].topic_id,
            error_code: codes::NONE,
            error_message: None,
            num_partitions: 2,
            replication_factor: 1,
            configs: Some(Vec::new()),
            topic_config_error_code: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);

    let image = broker_handle.controller_image_for_test();
    let topic = image.topic("configured").expect("topic in image");
    assert!(topic.partitions == 2);
    let configs = image.topic_config("configured").expect("topic configs");
    assert!(configs.get("retention.ms").map(String::as_str) == Some("60000"));
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_invalid_topic_configs_before_creating_the_topic() {
    /// One rejection case: the row's label, a config map that must never
    /// reach the metadata quorum, and the substrings the operator needs to
    /// see in the rejection.
    type RejectedConfig<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a [&'a str]);

    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();

    let cases: [RejectedConfig<'_>; 4] = [
        ("unknown-key", &[("flush.ms", "1000")], &["flush.ms"]),
        (
            "bad-delivery-mode",
            &[("delivery.mode", "later")],
            &["delivery.mode"],
        ),
        (
            "bad-delivery-delay",
            &[("delivery.max.delay.ms", "-2")],
            &["-2"],
        ),
        (
            "compacted-schedule",
            &[
                ("cleanup.policy", "compact"),
                ("delivery.mode", "scheduled"),
            ],
            &["cleanup.policy", "delivery.mode"],
        ),
    ];

    for (name, configs, needles) in cases {
        let request = request(vec![topic_with_configs(name, configs)]);

        let resp = drive(&broker, &request, &p, &peer).await;

        assert!(resp.topics.len() == 1, "topic {name}");
        check!(
            resp.topics[0].error_code == codes::INVALID_CONFIG,
            "topic {name}"
        );
        let message = resp.topics[0].error_message.clone().unwrap_or_default();
        for needle in needles {
            check!(message.contains(needle), "topic {name}: {message}");
        }
        check!(
            broker_handle
                .controller_image_for_test()
                .topic(name)
                .is_none(),
            "topic {name} must not be created"
        );
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_creates_a_scheduled_topic_and_persists_its_delivery_configs() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_with_configs(
        "retries",
        &[
            ("delivery.mode", "scheduled"),
            ("delivery.max.delay.ms", "-1"),
            ("delivery.schedule.monotonic", "true"),
        ],
    )]);

    let resp = drive(&broker, &req, &p, &peer).await;

    assert!(resp.topics.len() == 1);
    let expected = CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![CreatableTopicResult {
            name: "retries".into(),
            topic_id: resp.topics[0].topic_id,
            error_code: codes::NONE,
            error_message: None,
            num_partitions: 1,
            replication_factor: 1,
            configs: Some(Vec::new()),
            topic_config_error_code: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);

    let image = broker_handle.controller_image_for_test();
    let configs = image.topic_config("retries").expect("topic configs");
    let expected_configs = std::collections::BTreeMap::from([
        ("delivery.mode".to_string(), "scheduled".to_string()),
        ("delivery.max.delay.ms".to_string(), "-1".to_string()),
        (
            "delivery.schedule.monotonic".to_string(),
            "true".to_string(),
        ),
    ]);
    assert!(*configs == expected_configs);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn duplicate_topic_reports_error_without_success_fields() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic("dupe", 1, 1)]);
    let first = drive(&broker, &req, &p, &peer).await;
    assert!(first.topics[0].error_code == codes::NONE);

    let second = drive(&broker, &req, &p, &peer).await;

    assert!(second.topics.len() == 1);
    let expected = CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![CreatableTopicResult {
            name: "dupe".into(),
            // A fresh topic_id is generated before submit_change even on
            // the error path; copied from the actual response.
            topic_id: second.topics[0].topic_id,
            error_code: codes::TOPIC_ALREADY_EXISTS,
            error_message: None,
            num_partitions: -1,
            replication_factor: -1,
            configs: None,
            topic_config_error_code: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(second == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn strict_create_topics_rejects_after_quota_exhaustion() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    seed_controller_quota(&broker_handle, 2.0).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic("throttled", 5, 1)]);

    let resp = drive(&broker, &req, &p, &peer).await;

    assert!(resp.topics.len() == 1);
    let expected = CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![CreatableTopicResult {
            name: "throttled".into(),
            // Randomly generated per create; copied from the actual response.
            topic_id: resp.topics[0].topic_id,
            error_code: codes::NONE,
            error_message: None,
            num_partitions: 5,
            replication_factor: 1,
            configs: Some(Vec::new()),
            topic_config_error_code: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);

    let rejected = drive(&broker, &request(vec![topic("rejected", 1, 1)]), &p, &peer).await;
    let expected = CreateTopicsResponse {
        throttle_time_ms: 1_000,
        topics: vec![CreatableTopicResult {
            name: "rejected".into(),
            error_code: codes::THROTTLING_QUOTA_EXCEEDED,
            ..Default::default()
        }],
        ..Default::default()
    };
    assert!(rejected == expected);
    broker_handle.shutdown().await;
}
