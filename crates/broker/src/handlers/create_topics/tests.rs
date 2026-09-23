//! End-to-end tests of the `CreateTopics` handler, driven over the wire
//! encoding against a running broker: the authorization gate, the per-topic
//! error rows, a successful create, and the KIP-599 mutation quota.

use std::{net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use krabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};
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

/// A denial on cluster `Create` is a shortcut only. `DenyAll` also denies the
/// per-topic `Create` fallback, so the request still ends every row in
/// `TOPIC_AUTHORIZATION_FAILED` -- but never `CLUSTER_AUTHORIZATION_FAILED`,
/// which Kafka's `ControllerApis.createTopics` never answers for a topic row.
#[tokio::test]
async fn handle_falls_back_to_topic_authorization_failed_for_each_topic() {
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
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: Some("Authorization failed.".into()),
                num_partitions: -1,
                replication_factor: -1,
                configs: None,
                topic_config_error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            CreatableTopicResult {
                name: "payments".into(),
                topic_id: ProtoUuid([0; 16]),
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                error_message: Some("Authorization failed.".into()),
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
                error_message: Some(
                    "Number of partitions was set to an invalid non-positive value.".into(),
                ),
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

/// KIP-464: `num_partitions = -1` and `replication_factor = -1` take the
/// broker's `num.partitions` and `default.replication.factor` (#728). That is
/// what `kafka-topics --create` sends without `--partitions` and
/// `--replication-factor`. Kafka's `ReplicationControlManager.createTopic`
/// refuses a replication factor of 0 or below -1 first, then a partition count
/// of 0 or below -1.
#[tokio::test]
async fn minus_one_takes_the_broker_topic_creation_defaults() {
    const BAD_RF: &str =
        "Replication factor must be larger than 0, or -1 to use the default value.";
    const BAD_COUNT: &str = "Number of partitions was set to an invalid non-positive value.";
    /// (requested partitions, requested replication factor, error code,
    /// error message, created partitions, created replication factor)
    type Row = (i32, i16, i16, Option<&'static str>, i32, i16);

    let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.num_partitions = 4;
        cfg.default_replication_factor = 2;
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    for node_id in [2, 3, 4] {
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1BrokerRegistration(
                krabka_metadata::BrokerRegistrationRecord {
                    node_id: krabka_raft::NodeId(node_id),
                    broker_epoch: 0,
                    incarnation_id: Uuid::nil(),
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: std::collections::BTreeMap::new(),
                },
            )])
            .await
            .expect("seed broker registration");
    }

    let rows: [Row; 8] = [
        (-1, -1, codes::NONE, None, 4, 2),
        (-1, 3, codes::NONE, None, 4, 3),
        (6, -1, codes::NONE, None, 6, 2),
        (0, 1, codes::INVALID_PARTITIONS, Some(BAD_COUNT), -1, -1),
        (-2, 1, codes::INVALID_PARTITIONS, Some(BAD_COUNT), -1, -1),
        (
            1,
            0,
            codes::INVALID_REPLICATION_FACTOR,
            Some(BAD_RF),
            -1,
            -1,
        ),
        (
            0,
            0,
            codes::INVALID_REPLICATION_FACTOR,
            Some(BAD_RF),
            -1,
            -1,
        ),
        (
            1,
            -2,
            codes::INVALID_REPLICATION_FACTOR,
            Some(BAD_RF),
            -1,
            -1,
        ),
    ];
    for (row, (partitions, rf, error_code, error_message, created, created_rf)) in
        rows.into_iter().enumerate()
    {
        let name = format!("defaults-{row}");
        let resp = drive(
            &broker,
            &request(vec![topic(&name, partitions, rf)]),
            &principal("admin"),
            &peer(),
        )
        .await;

        let image = broker_handle.controller_image_for_test();
        let created_ok = error_code == codes::NONE;
        let expected = CreateTopicsResponse {
            throttle_time_ms: 0,
            topics: vec![CreatableTopicResult {
                name: name.clone(),
                topic_id: image.topic(&name).map_or(ProtoUuid([0; 16]), |topic| {
                    ProtoUuid(topic.topic_id.into_bytes())
                }),
                error_code,
                error_message: error_message.map(str::to_owned),
                num_partitions: created,
                replication_factor: created_rf,
                configs: created_ok.then(|| expected_configs(&[])),
                topic_config_error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        check!(resp == expected, "requested ({partitions}, {rf})");

        let committed = image
            .partitions_of(&name)
            .map(|partition| i16::try_from(partition.replicas.len()).expect("replication factor"))
            .collect::<Vec<_>>();
        let expected_committed =
            vec![created_rf; usize::try_from(created.max(0)).expect("partition count")];
        check!(
            committed == expected_committed,
            "requested ({partitions}, {rf})"
        );
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

/// The diskless WAL placement check names the leader the partition will
/// have, which is the first active replica of a manual assignment (#741), not
/// the first listed one.
#[tokio::test]
async fn diskless_wal_validation_names_the_active_leader_of_a_manual_assignment() {
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
    for node_id in [2, 3] {
        crate::test_support::seed_remote_broker(&broker_handle, node_id).await;
    }
    crate::test_support::fence_remote_broker(&broker_handle, 2).await;
    let req = request(vec![CreatableTopic {
        name: "manual-diskless".into(),
        num_partitions: -1,
        replication_factor: -1,
        assignments: vec![
            krabka_protocol::owned::create_topics_request::CreatableReplicaAssignment {
                partition_index: 0,
                broker_ids: vec![2, 3],
                ..Default::default()
            },
        ],
        configs: vec![CreatableTopicConfig {
            name: "krabka.diskless".into(),
            value: Some("true".into()),
            ..Default::default()
        }],
        ..Default::default()
    }]);

    let resp = drive(&broker, &req, &principal("admin"), &peer()).await;

    assert!(resp.topics[0].error_code == codes::INVALID_CONFIG);
    let message = resp.topics[0].error_message.as_deref().unwrap_or_default();
    check!(message.contains("partition 0 leader 3 "), "{message}");
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
            &[InitialLeadership {
                leader: config.node_id,
                isr: vec![config.node_id],
            }],
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
async fn validate_only_answers_the_verdict_and_commits_nothing() {
    /// One dry run: the topic name, and the row it has to answer with.
    type DryRun<'a> = (&'a str, CreatableTopicResult);

    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let committed = drive(&broker, &request(vec![topic("existing", 1, 1)]), &p, &peer).await;
    assert!(committed.topics[0].error_code == codes::NONE);

    let cases: [DryRun<'_>; 2] = [
        (
            // Kafka's `validateOnly` reports `TopicExistsException` for a name
            // that is already taken, and the committing path never runs here
            // to find that out.
            "existing",
            CreatableTopicResult {
                name: "existing".into(),
                error_code: codes::TOPIC_ALREADY_EXISTS,
                num_partitions: -1,
                replication_factor: -1,
                ..Default::default()
            },
        ),
        (
            // KIP-525 answers a dry run with the configuration the topic
            // *would* be created with: Kafka builds the row in `createTopic`
            // from `computeEffectiveTopicConfigs(creationConfigs)`, and
            // `validateOnly` discards the records alone. The list is filled in
            // below, against the same image the handler resolved it from.
            "fresh",
            CreatableTopicResult {
                name: "fresh".into(),
                error_code: codes::NONE,
                num_partitions: 1,
                replication_factor: 1,
                configs: None,
                ..Default::default()
            },
        ),
    ];

    for (name, expected_row) in cases {
        let req = CreateTopicsRequest {
            validate_only: true,
            ..request(vec![topic(name, 1, 1)])
        };

        let resp = drive(&broker, &req, &p, &peer).await;

        assert!(resp.topics.len() == 1, "topic {name}");
        // A refused row carries no configuration at all; the one that passed
        // carries what the topic would have been created with. The dry run
        // committed nothing, so that is the empty override map resolved
        // against the image.
        let configs = if expected_row.error_code == codes::NONE {
            Some(effective_topic_configs(
                &broker_handle.controller_image_for_test(),
                name,
                &std::collections::BTreeMap::new(),
            ))
        } else {
            expected_row.configs.clone()
        };
        let expected = CreateTopicsResponse {
            throttle_time_ms: 0,
            // A fresh topic_id is generated before the verdict on every row.
            topics: vec![CreatableTopicResult {
                topic_id: resp.topics[0].topic_id,
                configs,
                ..expected_row
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        check!(resp == expected, "topic {name}");
    }

    // The dry run for "fresh" passed every check and still committed nothing.
    assert!(
        broker_handle
            .controller_image_for_test()
            .topic("fresh")
            .is_none()
    );
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
        crate::handlers::describe_configs::effective_topic_configs(
            &image,
            "mirrored",
            image.topic_config("mirrored").expect("stored overrides"),
        )
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

/// Every file and directory under `root`, as paths relative to it, sorted.
fn tree(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read dir") {
            let path = entry.expect("dir entry").path();
            out.push(path.strip_prefix(root).expect("under root").to_path_buf());
            if path.is_dir() {
                walk(root, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, root, &mut out);
    out.sort();
    out
}

/// Kafka's topic-name rules: `ControllerApis` refuses `__cluster_metadata`,
/// and `ReplicationControlManager.validateNewTopicNames` refuses a name that
/// `Topic.validate` refuses or that collides with an existing topic. A refused
/// name commits nothing and creates no directory, inside the log directory or
/// outside it.
#[tokio::test]
async fn handle_refuses_invalid_and_colliding_topic_names() {
    /// One row: the requested name, and the error code and message it gets.
    type NameCase = (String, i16, Option<String>);

    let (broker_handle, dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.log_dir = cfg.log_dir.join("logs");
    })
    .await;
    let broker = broker_handle.broker_arc_for_test();
    let p = principal("admin");
    let peer = peer();
    let existing = drive(&broker, &request(vec![topic("a_b", 1, 1)]), &p, &peer).await;
    assert!(existing.topics[0].error_code == codes::NONE);

    let absolute = format!("{}/escape", dir.path().display());
    let too_long = "a".repeat(250);
    let illegal = |name: &str| {
        Some(format!(
            "Topic name is invalid: '{name}' contains one or more characters other than ASCII \
             alphanumerics, '.', '_' and '-'"
        ))
    };
    let cases: Vec<NameCase> = vec![
        (
            String::new(),
            codes::INVALID_TOPIC_EXCEPTION,
            Some("Topic name is invalid: the empty string is not allowed".into()),
        ),
        (
            ".".into(),
            codes::INVALID_TOPIC_EXCEPTION,
            Some("Topic name is invalid: '.' is not allowed".into()),
        ),
        (
            "..".into(),
            codes::INVALID_TOPIC_EXCEPTION,
            Some("Topic name is invalid: '..' is not allowed".into()),
        ),
        (
            too_long.clone(),
            codes::INVALID_TOPIC_EXCEPTION,
            Some(format!(
                "Topic name is invalid: the length of '{too_long}' is longer than the max \
                 allowed length 249"
            )),
        ),
        ("a/b".into(), codes::INVALID_TOPIC_EXCEPTION, illegal("a/b")),
        (
            "../x".into(),
            codes::INVALID_TOPIC_EXCEPTION,
            illegal("../x"),
        ),
        (
            absolute.clone(),
            codes::INVALID_TOPIC_EXCEPTION,
            illegal(&absolute),
        ),
        ("a b".into(), codes::INVALID_TOPIC_EXCEPTION, illegal("a b")),
        (
            "a.b".into(),
            codes::INVALID_TOPIC_EXCEPTION,
            Some("Topic 'a.b' collides with existing topic: a_b".into()),
        ),
        (
            "__cluster_metadata".into(),
            codes::INVALID_REQUEST,
            Some("Creation of internal topic __cluster_metadata is prohibited.".into()),
        ),
    ];

    let before = tree(dir.path());
    for validate_only in [true, false] {
        for (name, error_code, error_message) in &cases {
            let req = CreateTopicsRequest {
                validate_only,
                ..request(vec![topic(name, 1, 1)])
            };

            let resp = drive(&broker, &req, &p, &peer).await;

            let expected = CreateTopicsResponse {
                throttle_time_ms: 0,
                topics: vec![CreatableTopicResult {
                    name: name.clone(),
                    topic_id: ProtoUuid([0; 16]),
                    error_code: *error_code,
                    error_message: error_message.clone(),
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: None,
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            };
            check!(resp == expected, "{name:?} validate_only={validate_only}");
            check!(
                broker_handle
                    .controller_image_for_test()
                    .topic(name)
                    .is_none(),
                "{name:?}"
            );
        }
    }
    check!(tree(dir.path()) == before);

    // The longest legal name is accepted.
    let longest = "a".repeat(249);
    let created = drive(&broker, &request(vec![topic(&longest, 1, 1)]), &p, &peer).await;
    check!(created.topics[0].error_code == codes::NONE);
    broker_handle.shutdown().await;
}

/// A manual assignment keeps its replica list, but its ISR holds only the
/// listed brokers that are active, and its leader is the first of them
/// (#741). Kafka's `ReplicationControlManager.createTopic` filters the ISR
/// with `ClusterControlManager.isActive`, and answers
/// `INVALID_REPLICA_ASSIGNMENT` when no listed broker is active.
///
/// Brokers 2, 3 and 4 are remote registrations. A fenced heartbeat on the
/// controller makes a broker unavailable, as a real fenced broker is.
#[tokio::test]
async fn manual_assignment_leaves_unavailable_brokers_out_of_the_isr() {
    const V4: i16 = 4;

    /// One row: the fenced brokers, the witness brokers, the replica list of
    /// each partition, and the expected error code, error message and
    /// `(leader, isr)` per partition.
    type Row = (
        &'static [u64],
        &'static [u64],
        &'static [&'static [i32]],
        i16,
        Option<&'static str>,
        Vec<(krabka_raft::NodeId, Vec<krabka_raft::NodeId>)>,
    );
    let n = krabka_raft::NodeId;
    let rows: [Row; 7] = [
        (
            &[],
            &[],
            &[&[2, 3, 4]],
            codes::NONE,
            None,
            vec![(n(2), vec![n(2), n(3), n(4)])],
        ),
        (
            &[2],
            &[],
            &[&[2, 3, 4]],
            codes::NONE,
            None,
            vec![(n(3), vec![n(3), n(4)])],
        ),
        (
            &[2, 3],
            &[],
            &[&[3, 2, 4], &[4, 3, 2]],
            codes::NONE,
            None,
            vec![(n(4), vec![n(4)]), (n(4), vec![n(4)])],
        ),
        (
            &[2],
            &[],
            &[&[2]],
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(
                "All brokers specified in the manual partition assignment for partition 0 are \
                 fenced or in controlled shutdown.",
            ),
            vec![],
        ),
        (
            &[3],
            &[],
            &[&[2], &[3]],
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(
                "All brokers specified in the manual partition assignment for partition 1 are \
                 fenced or in controlled shutdown.",
            ),
            vec![],
        ),
        (
            &[2],
            &[3],
            &[&[2, 3, 4]],
            codes::NONE,
            None,
            vec![(n(4), vec![n(3), n(4)])],
        ),
        (
            &[2],
            &[3],
            &[&[2, 3]],
            codes::INVALID_REPLICA_ASSIGNMENT,
            Some(
                "All active brokers specified in the manual partition assignment for partition \
                 0 are witnesses, and a witness cannot lead.",
            ),
            vec![],
        ),
    ];

    for (fenced, witnesses, lists, error_code, error_message, partitions) in rows {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        for node_id in [2, 3, 4] {
            crate::test_support::seed_remote_broker(&broker_handle, node_id).await;
        }
        for &node_id in witnesses {
            crate::test_support::make_witness(&broker_handle, node_id).await;
        }
        for &node_id in fenced {
            crate::test_support::fence_remote_broker(&broker_handle, node_id).await;
        }
        let topic = CreatableTopic {
            name: "manual".into(),
            num_partitions: -1,
            replication_factor: -1,
            assignments: lists
                .iter()
                .enumerate()
                .map(|(index, broker_ids)| {
                    krabka_protocol::owned::create_topics_request::CreatableReplicaAssignment {
                        partition_index: i32::try_from(index).expect("index"),
                        broker_ids: broker_ids.to_vec(),
                        ..Default::default()
                    }
                })
                .collect(),
            ..Default::default()
        };
        let p = principal("admin");
        let peer = peer();
        let ctx = test_context(&p, &peer);

        let bytes = handle(
            &broker,
            V4,
            123,
            &crate::test_support::encode_request(&request(vec![topic]), V4),
            &ctx,
        )
        .await
        .expect("handle");
        let resp: CreateTopicsResponse = crate::test_support::decode_response(&bytes, V4);

        let expected = CreateTopicsResponse {
            throttle_time_ms: 0,
            topics: vec![CreatableTopicResult {
                name: "manual".into(),
                topic_id: ProtoUuid([0; 16]),
                error_code,
                error_message: error_message.map(str::to_owned),
                num_partitions: -1,
                replication_factor: -1,
                configs: None,
                topic_config_error_code: 0,
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        check!(resp == expected, "fenced {fenced:?}, assignment {lists:?}");

        let image = broker_handle.controller_image_for_test();
        let committed = (0..)
            .map_while(|index| image.partition("manual", index).cloned())
            .collect::<Vec<_>>();
        let expected_records = partitions
            .into_iter()
            .zip(lists)
            .enumerate()
            .map(
                |(index, ((leader, isr), replicas))| krabka_metadata::PartitionRecord {
                    topic: "manual".into(),
                    partition: i32::try_from(index).expect("index"),
                    leader,
                    replicas: replicas
                        .iter()
                        .map(|&id| n(u64::try_from(id).expect("broker id")))
                        .collect(),
                    isr,
                    leader_epoch: krabka_metadata::LeaderEpoch(INITIAL_LEADER_EPOCH),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                },
            )
            .collect::<Vec<_>>();
        check!(
            committed == expected_records,
            "fenced {fenced:?}, assignment {lists:?}"
        );
        broker_handle.shutdown().await;
    }
}

/// #698 / #1058: Kafka's `Create` decision for a `CreateTopics` request,
/// table-driven over which ACL `alice` holds.
///
/// `ControllerApis.handleCreateTopics`/`createTopics` filters duplicate names
/// and the protected `__cluster_metadata` name out before authorizing either
/// way, then checks cluster `Create` once as a shortcut, and falls back to
/// `Create` on each surviving `Topic(name)` when that shortcut is denied. A
/// literal ACL on `a` never covers `app-x`, and a prefixed ACL on `app-`
/// never covers `a` -- exactly the Kafka Streams/Connect application-id ACL
/// shape #698 reported as unusable. The request repeats `b` and asks for
/// `__cluster_metadata`; both are refused under every ACL shape.
#[tokio::test]
async fn handle_authorizes_create_per_topic_when_cluster_create_is_denied() {
    fn acl(
        resource_type: ResourceType,
        resource_name: &str,
        pattern_type: PatternType,
        operation: AclOperation,
    ) -> AclEntry {
        AclEntry {
            resource_type,
            resource_name: resource_name.into(),
            pattern_type,
            principal: "User:alice".into(),
            host: "*".into(),
            operation,
            permission_type: PermissionType::Allow,
        }
    }

    let cluster_create = acl(
        ResourceType::Cluster,
        crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
        PatternType::Literal,
        AclOperation::Create,
    );
    let literal_a = acl(ResourceType::Topic, "a", PatternType::Literal, AclOperation::Create);
    let prefixed_app = acl(
        ResourceType::Topic,
        "app-",
        PatternType::Prefixed,
        AclOperation::Create,
    );

    struct Case {
        acls: Vec<AclEntry>,
        // Which of "a" and "app-x" this ACL shape lets `alice` create.
        // "b" (duplicate) and "__cluster_metadata" (protected) are refused
        // under every shape and are not repeated here.
        created: &'static [&'static str],
    }

    let cases = [
        ("cluster Create authorizes every survivor", Case {
            acls: vec![cluster_create.clone()],
            created: &["a", "app-x"],
        }),
        ("a literal ACL authorizes only its exact name", Case {
            acls: vec![literal_a.clone()],
            created: &["a"],
        }),
        ("an app- prefixed ACL authorizes only its prefix", Case {
            acls: vec![prefixed_app.clone()],
            created: &["app-x"],
        }),
        ("no ACL authorizes nothing", Case {
            acls: vec![],
            created: &[],
        }),
    ];

    for (label, case) in cases {
        let (broker_handle, _dir) = start_broker(Arc::new(
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
        ))
        .await;
        let broker = broker_handle.broker_arc_for_test();
        if !case.acls.is_empty() {
            broker
                .controller
                .submit_change(
                    case.acls
                        .into_iter()
                        .map(MetadataRecord::V1AccessControlEntry)
                        .collect(),
                )
                .await
                .expect("seed acls");
        }

        let p = principal("alice");
        let peer = peer();
        let req = request(vec![
            topic("a", 1, 1),
            topic("app-x", 1, 1),
            topic("b", 1, 1),
            topic("b", 1, 1),
            topic("__cluster_metadata", 1, 1),
        ]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let row = |index: usize, name: &str| -> CreatableTopicResult {
            if case.created.contains(&name) {
                CreatableTopicResult {
                    name: name.into(),
                    topic_id: resp.topics[index].topic_id,
                    error_code: codes::NONE,
                    error_message: None,
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: Some(Vec::new()),
                    topic_config_error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
            } else {
                CreatableTopicResult {
                    name: name.into(),
                    topic_id: ProtoUuid([0; 16]),
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    error_message: Some("Authorization failed.".into()),
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: None,
                    topic_config_error_code: 0,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }
            }
        };
        let duplicate_row = |name: &str| CreatableTopicResult {
            name: name.into(),
            topic_id: ProtoUuid([0; 16]),
            error_code: codes::INVALID_REQUEST,
            error_message: Some("Duplicate topic name.".into()),
            num_partitions: -1,
            replication_factor: -1,
            configs: None,
            topic_config_error_code: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        let protected_row = CreatableTopicResult {
            name: "__cluster_metadata".into(),
            topic_id: ProtoUuid([0; 16]),
            error_code: codes::INVALID_REQUEST,
            error_message: Some(
                "Creation of internal topic __cluster_metadata is prohibited.".into(),
            ),
            num_partitions: -1,
            replication_factor: -1,
            configs: None,
            topic_config_error_code: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };

        let expected = CreateTopicsResponse {
            throttle_time_ms: 0,
            topics: vec![
                row(0, "a"),
                row(1, "app-x"),
                duplicate_row("b"),
                duplicate_row("b"),
                protected_row,
            ],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        check!(resp == expected, "case: {label}");

        let image = broker_handle.controller_image_for_test();
        check!(
            image.topic("a").is_some() == case.created.contains(&"a"),
            "case: {label}, topic a"
        );
        check!(
            image.topic("app-x").is_some() == case.created.contains(&"app-x"),
            "case: {label}, topic app-x"
        );
        check!(image.topic("b").is_none(), "case: {label}, topic b");
        check!(
            image.topic("__cluster_metadata").is_none(),
            "case: {label}, topic __cluster_metadata"
        );

        broker_handle.shutdown().await;
    }
}
