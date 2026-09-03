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
        create_topics_response::{CreatableTopicConfigs, CreateTopicsResponse},
    },
};
use krabka_security::Principal;

use super::*;
use crate::{
    broker::BrokerHandle,
    test_support::{DenyAll, peer, principal},
};

const VERSION: i16 = 7;

/// `ConfigSource.DYNAMIC_TOPIC_CONFIG`, the source a value the create request
/// carried reports.
const DYNAMIC_TOPIC_CONFIG: i8 = 1;

/// `ConfigSource.DEFAULT_CONFIG`, the source an untouched key reports.
const DEFAULT_CONFIG: i8 = 5;

/// The KIP-525 configs list a v5+ row carries for a topic created with
/// `overrides` on a cluster that holds no dynamic defaults.
///
/// Every topic-scope key is in it, so spelling the list out row by row would
/// transcribe the registry rather than say anything about the handler. What
/// this states instead is the layering the response must show: a key the
/// request set reads its value at `DYNAMIC_TOPIC_CONFIG`, every other key
/// reads the built-in default at `DEFAULT_CONFIG`, a sensitive key's value is
/// withheld, and the list is sorted by name.
fn expected_configs(overrides: &[(&str, &str)]) -> Vec<CreatableTopicConfigs> {
    use crate::config_keys::registry::{self, ConfigScope};

    let mut configs: Vec<CreatableTopicConfigs> = registry::keys_in(ConfigScope::Topic)
        .map(|row| {
            let stored = overrides
                .iter()
                .find(|(key, _)| *key == row.name)
                .map(|(_, value)| *value);
            let (value, config_source) = stored.map_or((row.default, DEFAULT_CONFIG), |value| {
                (Some(value), DYNAMIC_TOPIC_CONFIG)
            });
            CreatableTopicConfigs {
                name: row.name.to_owned(),
                value: (!row.is_sensitive())
                    .then_some(value)
                    .flatten()
                    .map(str::to_owned),
                read_only: row.read_only,
                config_source,
                is_sensitive: row.is_sensitive(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }
        })
        .collect();
    configs.sort_by(|left, right| left.name.cmp(&right.name));
    configs
}

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
            configs: Some(expected_configs(&[("retention.ms", "60000")])),
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

/// Kafka Streams creates every windowed-store changelog topic with
/// `cleanup.policy=compact,delete`, so a broker that refuses the list value
/// cannot host a Streams application with a windowed store. The value is
/// stored as the client sent it, which is what `DescribeConfigs` echoes back.
#[tokio::test]
async fn handle_creates_a_topic_whose_cleanup_policy_names_both_halves() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_with_configs(
        "windowed-changelog",
        &[
            ("cleanup.policy", "compact,delete"),
            ("min.compaction.lag.ms", "0"),
            ("message.timestamp.type", "CreateTime"),
        ],
    )]);

    let resp = drive(&broker, &req, &p, &peer).await;

    assert!(resp.topics.len() == 1);
    assert!(resp.topics[0].error_code == codes::NONE);
    let image = broker_handle.controller_image_for_test();
    let configs = image
        .topic_config("windowed-changelog")
        .expect("topic configs");
    assert!(configs.get("cleanup.policy").map(String::as_str) == Some("compact,delete"));
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

    let cases: [RejectedConfig<'_>; 10] = [
        (
            "unknown-key",
            &[("not.a.topic.config", "1000")],
            &["not.a.topic.config"],
        ),
        (
            "compacted-and-tiered",
            &[
                ("cleanup.policy", "compact"),
                ("remote.storage.enable", "true"),
            ],
            &["Tiered storage is not supported for compacted topics"],
        ),
        (
            "compact-and-delete-and-tiered",
            &[
                ("cleanup.policy", "compact,delete"),
                ("remote.storage.enable", "true"),
            ],
            &["Tiered storage is not supported for compacted topics"],
        ),
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
        (
            "bad-diskless",
            &[("krabka.diskless", "yes")],
            &["krabka.diskless"],
        ),
        (
            "diskless-and-tiered",
            &[
                ("krabka.diskless", "true"),
                ("remote.storage.enable", "true"),
            ],
            &["krabka.diskless", "remote.storage.enable"],
        ),
        (
            "diskless-and-scheduled",
            &[("krabka.diskless", "true"), ("delivery.mode", "scheduled")],
            &["krabka.diskless", "delivery.mode"],
        ),
        // `BrokerConfig::for_tests` configures no object store, and a diskless
        // topic without one could never flush or trim: the broker starts no
        // WAL index projection and no object flusher, so the local logs would
        // grow without bound behind a flag that advertises the opposite. This
        // is the one case here that the pure key/value validator cannot catch,
        // because it depends on the broker's own configuration.
        (
            "diskless-without-an-object-tier",
            &[(crate::config_keys::DISKLESS, "true")],
            &[crate::config_keys::DISKLESS, "remote_storage_backend"],
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
            configs: Some(expected_configs(&[
                ("delivery.mode", "scheduled"),
                ("delivery.max.delay.ms", "-1"),
                ("delivery.schedule.monotonic", "true"),
            ])),
            topic_config_error_code: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);

    let image = broker_handle.controller_image_for_test();
    let configs = image.topic_config("retries").expect("topic configs");
    let expected_configs = maplit::btreemap! {
    "delivery.mode".to_string() => "scheduled".to_string(),
    "delivery.max.delay.ms".to_string() => "-1".to_string(),
    "delivery.schedule.monotonic".to_string() => "true".to_string()};
    assert!(*configs == expected_configs);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_creates_a_diskless_topic_and_opens_its_partitions_on_the_wal_path() {
    // A diskless topic needs an object-store tier: without one the broker
    // starts no WAL index projection and no object flusher, and the handler
    // refuses the opt-in rather than create a topic that could never flush or
    // trim. Configure the tier this test's topic depends on.
    let object_store = tempfile::TempDir::new().expect("object store dir");
    let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        cfg.rack = Some("rack-a".into());
        cfg.diskless_wal_local_replica_count = 1;
        cfg.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
            dir: object_store.path().to_path_buf(),
        });
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_with_configs(
        "events",
        &[("krabka.diskless", "true")],
    )]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![CreatableTopicResult {
            name: "events".into(),
            topic_id: resp.topics[0].topic_id,
            error_code: codes::NONE,
            error_message: None,
            num_partitions: 1,
            replication_factor: 1,
            configs: Some(expected_configs(&[("krabka.diskless", "true")])),
            topic_config_error_code: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);

    // The override reaches the metadata log unchanged, ...
    let image = broker_handle.controller_image_for_test();
    let configs = image.topic_config("events").expect("topic configs");
    let expected_configs = maplit::btreemap! {"krabka.diskless".to_string() => "true".to_string()};
    assert!(*configs == expected_configs);
    // ... and the partition the handler materialized is on the diskless
    // runtime, which is the whole point of the key.
    let partition = broker
        .partitions
        .get("events", krabka_ids::PartitionIndex(0))
        .expect("partition materialized locally");
    assert!(partition.diskless);

    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_rejects_diskless_topic_without_a_rack_safe_wal_quorum() {
    let object_store = tempfile::TempDir::new().expect("object store dir");
    let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
        cfg.remote_storage_backend = Some(crate::config::RemoteStorageBackend::Local {
            dir: object_store.path().to_path_buf(),
        });
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let req = request(vec![topic_with_configs(
        "unplaceable-diskless",
        &[("krabka.diskless", "true")],
    )]);

    let resp = drive(&broker, &req, &principal("admin"), &peer()).await;

    assert!(resp.topics[0].error_code == codes::INVALID_CONFIG);
    let message = resp.topics[0].error_message.as_deref().unwrap_or_default();
    for needle in [
        "partition 0",
        "leader 1",
        "0 eligible",
        "3 are required",
        "broker.rack",
    ] {
        check!(message.contains(needle), "{message}");
    }
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic("unplaceable-diskless")
            .is_none()
    );
    broker_handle.shutdown().await;
}

#[test]
fn diskless_wal_validation_uses_the_local_registration_fallback() {
    let dir = tempfile::TempDir::new().expect("log dir");
    let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
    config.rack = Some("rack-a".into());
    config.diskless_wal_local_replica_count = 1;

    assert!(
        diskless_wal_placement_error(
            &krabka_metadata::MetadataImage::default(),
            &config,
            0,
            &[vec![config.node_id]],
        )
        .is_none()
    );
}

#[tokio::test]
async fn a_created_topic_with_the_key_off_stays_on_the_local_log_path() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_with_configs(
        "plain",
        &[("krabka.diskless", "false")],
    )]);

    let resp = drive(&broker, &req, &p, &peer).await;

    assert!(resp.topics[0].error_code == codes::NONE);
    let partition = broker
        .partitions
        .get("plain", krabka_ids::PartitionIndex(0))
        .expect("partition materialized locally");
    assert!(!partition.diskless);
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
            configs: Some(expected_configs(&[])),
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

/// An authorizer that allows everything but `DescribeConfigs`, the operation
/// KIP-525 hangs the configs disclosure on.
#[derive(Debug)]
struct DenyDescribeConfigs;

impl crate::authorizer::Authorizer for DenyDescribeConfigs {
    fn authorize(
        &self,
        _source: &dyn krabka_authz::AclSource,
        req: &crate::authorizer::AuthorizationRequest<'_>,
    ) -> crate::authorizer::AuthorizationResult {
        if req.operation == krabka_metadata::AclOperation::DescribeConfigs {
            crate::authorizer::AuthorizationResult::Deny
        } else {
            crate::authorizer::AuthorizationResult::Allow
        }
    }
}

/// KIP-525: the two layers a created topic's configs list distinguishes. The
/// whole-list expectations elsewhere in this file are built from the registry,
/// so this case names the two rows the layering turns on and their values.
#[tokio::test]
async fn created_topic_configs_separate_a_request_value_from_an_inherited_default() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_with_config("effective")]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let configs = resp.topics[0].configs.clone().expect("v5+ configs list");
    let entry = |name: &str| {
        configs
            .iter()
            .find(|entry| entry.name == name)
            .cloned()
            .unwrap_or_else(|| panic!("{name} in the configs list"))
    };
    // The value this very request carried, at DYNAMIC_TOPIC_CONFIG (1).
    let expected_retention = CreatableTopicConfigs {
        name: "retention.ms".into(),
        value: Some("60000".into()),
        read_only: false,
        config_source: DYNAMIC_TOPIC_CONFIG,
        is_sensitive: false,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    check!(entry("retention.ms") == expected_retention);
    // A key the request never mentioned, at DEFAULT_CONFIG (5).
    let expected_cleanup = CreatableTopicConfigs {
        name: "cleanup.policy".into(),
        value: Some("delete".into()),
        read_only: false,
        config_source: DEFAULT_CONFIG,
        is_sensitive: false,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    check!(entry("cleanup.policy") == expected_cleanup);
    check!(resp.topics[0].topic_config_error_code == 0);
    broker_handle.shutdown().await;
}

/// The list a `CreateTopics` row carries is the list `DescribeConfigs`
/// answers for the same topic. A client that reads
/// `createTopics(...).config(topic)` instead of issuing a follow-up
/// `DescribeConfigs` -- Terraform's `kafka_topic`, Connect's `TopicAdmin`,
/// Streams' `InternalTopicManager` -- must see no difference.
#[tokio::test]
async fn created_topic_configs_match_describe_configs_for_the_same_topic() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_with_config("mirrored")]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let image = broker_handle.controller_image_for_test();
    let described: Vec<CreatableTopicConfigs> =
        crate::handlers::describe_configs::effective_topic_configs(&image, "mirrored")
            .into_iter()
            .map(|entry| CreatableTopicConfigs {
                name: entry.name,
                value: entry.value,
                read_only: entry.read_only,
                config_source: entry.config_source,
                is_sensitive: entry.is_sensitive,
                ..Default::default()
            })
            .collect();
    assert!(resp.topics[0].configs.clone().expect("configs") == described);
    broker_handle.shutdown().await;
}

/// KIP-525: a principal that may create a topic but may not describe its
/// configs still gets the topic. Kafka withholds only the disclosure -- an
/// empty list, `TOPIC_AUTHORIZATION_FAILED` on `topicConfigErrorCode`, and
/// neither the partition count nor the replication factor, because
/// `AdminClient` fails every accessor on the row once the code is set.
#[tokio::test]
async fn create_without_describe_configs_withholds_the_configs_but_creates_the_topic() {
    let (broker_handle, _dir) = start_broker(Arc::new(DenyDescribeConfigs)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_with_config("undescribable")]);

    let resp = drive(&broker, &req, &p, &peer).await;

    let expected = CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![CreatableTopicResult {
            name: "undescribable".into(),
            topic_id: resp.topics[0].topic_id,
            error_code: codes::NONE,
            error_message: None,
            num_partitions: -1,
            replication_factor: -1,
            configs: Some(Vec::new()),
            topic_config_error_code: codes::TOPIC_AUTHORIZATION_FAILED,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    // The create itself went through: only the disclosure was withheld.
    let image = broker_handle.controller_image_for_test();
    assert!(image.topic("undescribable").is_some());
    broker_handle.shutdown().await;
}

/// `configs` and `topicConfigErrorCode` arrived in v5, so a v4 response
/// carries neither and encodes as it always did. The handler skips the
/// `DescribeConfigs` check there too: nothing it decides can reach the wire.
#[tokio::test]
async fn v4_response_encodes_without_the_kip_525_fields() {
    const V4: i16 = 4;

    let (broker_handle, _dir) = start_broker(Arc::new(DenyDescribeConfigs)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let req = request(vec![topic_with_config("legacy")]);
    let ctx = test_context(&p, &peer);

    let bytes = handle(
        &broker,
        V4,
        123,
        &crate::test_support::encode_request(&req, V4),
        &ctx,
    )
    .await
    .expect("handle");

    let resp: CreateTopicsResponse = crate::test_support::decode_response(&bytes, V4);
    let expected = CreateTopicsResponse {
        throttle_time_ms: 0,
        topics: vec![CreatableTopicResult {
            name: "legacy".into(),
            // v4 carries no topic id, no configs and no config error code.
            topic_id: ProtoUuid([0; 16]),
            error_code: codes::NONE,
            error_message: None,
            num_partitions: -1,
            replication_factor: -1,
            configs: None,
            topic_config_error_code: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        }],
        unknown_tagged_fields: UnknownTaggedFields::default(),
    };
    assert!(resp == expected);
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic("legacy")
            .is_some()
    );
    broker_handle.shutdown().await;
}
