//! Formatting the target log directory and seeding it with the topics the
//! archive scan recovered.
//!
//! A restore drives `krabka_format::run_from_args_with_records` in process,
//! forwarding the target-side flags and handing it one `TopicRecord` per topic
//! and one `PartitionRecord` per partition, so the restored cluster boots with
//! its topics already present. This runs once, before any segment is written.

use std::{
    collections::{HashMap, HashSet},
    path::Path,
};

use krabka_ids::LeaderEpoch;
use krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicRecord};
use uuid::Uuid;

use crate::{
    args::RestoreArgs, discover::ArchiveInventory, error::RestoreError,
    report::MetadataRestoreReport,
};

/// The formatter outcome needed by the final restore report.
pub struct FormatTargetOutcome {
    /// Cluster id written to the target.
    pub cluster_id: Uuid,
    /// Metadata recovered from the optional controller snapshot.
    pub metadata: MetadataRestoreReport,
}

/// Format the target log directory, seed it with the recovered topics, and
/// return the cluster id it was formatted with.
///
/// The formatter generates a cluster id when none is given and does not report
/// it back, so this passes an explicit `--cluster-id` and keeps it for the
/// report. An operator who restores a cluster has to know its identity.
///
/// # Errors
///
/// Returns [`RestoreError::InvalidArgument`] when `--node-id` is absent: every restored partition names the target node as leader and sole replica, and defaulting that identity to node 0 could silently name a node the operator never said exists. Returns [`RestoreError::Format`] when the formatter rejects the target-side flags, and [`RestoreError::Io`] when the target cannot be written.
pub async fn format_target(
    args: &RestoreArgs,
    inventory: &ArchiveInventory,
) -> Result<FormatTargetOutcome, RestoreError> {
    let node_id = args.target.node_id.ok_or_else(|| {
        RestoreError::InvalidArgument(
            "--node-id is required: every restored partition names the target node as leader \
             and sole replica, so a restore cannot default that identity to node 0"
                .to_owned(),
        )
    })?;
    let cluster_id = args.target.cluster_id.unwrap_or_else(Uuid::new_v4);

    let mut format_argv = vec![
        "krabka-format".to_owned(),
        "--log-dir".to_owned(),
        args.target.log_dir.to_string_lossy().into_owned(),
        "--cluster-id".to_owned(),
        cluster_id.to_string(),
        "--node-id".to_owned(),
        node_id.to_string(),
    ];
    if args.target.standalone {
        format_argv.push("--standalone".to_owned());
    }
    if !args.target.initial_controllers.is_empty() {
        format_argv.push("--initial-controllers".to_owned());
        format_argv.push(args.target.initial_controllers.join(","));
    }
    if args.target.no_initial_controllers {
        format_argv.push("--no-initial-controllers".to_owned());
    }
    if let Some(listener) = &args.target.controller_listener {
        format_argv.push("--controller-listener".to_owned());
        format_argv.push(listener.clone());
    }

    let snapshot_records =
        read_metadata_snapshot(args.archive.metadata_snapshot.as_deref()).await?;
    let (extra, metadata) = seed_metadata_records(
        inventory,
        node_id,
        args.archive.metadata_snapshot.clone(),
        snapshot_records.as_deref(),
    )?;
    let code = krabka_format::run_from_args_with_records(format_argv, extra).await;
    if code == 0 {
        Ok(FormatTargetOutcome {
            cluster_id,
            metadata,
        })
    } else {
        Err(RestoreError::Format { code })
    }
}

async fn read_metadata_snapshot(
    path: Option<&Path>,
) -> Result<Option<Vec<MetadataRecord>>, RestoreError> {
    let Some(path) = path else {
        return Ok(None);
    };
    let bytes = tokio::fs::read(path).await.map_err(|error| {
        RestoreError::Io(std::io::Error::new(
            error.kind(),
            format!("cannot read metadata snapshot {}: {error}", path.display()),
        ))
    })?;
    krabka_raft::deserialize_metadata_snapshot(&bytes)
        .map(Some)
        .map_err(|error| RestoreError::MetadataSnapshot {
            path: path.to_path_buf(),
            reason: error.to_string(),
        })
}

/// Build the topic and partition records a restore seeds into the target formatter, from what the archive scan recovered.
///
/// Every topic's [`MetadataRecord::V1Topic`] precedes every [`MetadataRecord::V1Partition`], which is the ordering `krabka_format::run_with_records`'s own doc requires: a `MetadataImage` derives a topic's partition count from the partition records that apply after it, so a partition can only follow its own topic.
///
/// Pulled out as a pure function, separate from [`format_target`]'s formatter call, so a test can check exactly what gets seeded without running the formatter at all.
fn seed_metadata_records(
    inventory: &ArchiveInventory,
    node_id: NodeId,
    snapshot: Option<std::path::PathBuf>,
    snapshot_records: Option<&[MetadataRecord]>,
) -> Result<(Vec<MetadataRecord>, MetadataRestoreReport), RestoreError> {
    let mut topic_order: Vec<&str> = Vec::new();
    let mut topics: HashMap<&str, (Uuid, i32)> = HashMap::new();
    for entry in &inventory.partitions {
        let topic = entry.partition.topic.as_str();
        let counted = topics.entry(topic).or_insert_with(|| {
            topic_order.push(topic);
            (entry.partition.topic_id, 0)
        });
        counted.1 += 1;
    }

    let mut metadata = MetadataRestoreReport {
        snapshot,
        topic_configs: 0,
        access_control_entries: 0,
        client_quotas: 0,
        scram_credentials: 0,
        feature_levels: 0,
        topics_without_configuration: Vec::new(),
    };
    if snapshot_records.is_none() {
        metadata.topics_without_configuration = topic_order
            .iter()
            .map(|topic| (*topic).to_owned())
            .collect();
    }

    let mut features = Vec::new();
    let mut feature_epoch = Vec::new();
    let mut topic_configs = Vec::new();
    let mut scram = Vec::new();
    let mut acls = Vec::new();
    let mut quotas = Vec::new();
    let snapshot_topics: HashMap<&str, Uuid> = snapshot_records
        .into_iter()
        .flatten()
        .filter_map(|record| match record {
            MetadataRecord::V1Topic(topic) => Some((topic.name.as_str(), topic.topic_id)),
            _ => None,
        })
        .collect();
    let mut snapshot_features = HashSet::new();
    for record in snapshot_records.into_iter().flatten() {
        match record {
            MetadataRecord::V1FeatureLevel(feature) => {
                metadata.feature_levels += 1;
                snapshot_features.insert(feature.name.as_str());
                features.push(record.clone());
            }
            MetadataRecord::V1FeaturesEpoch(_) => feature_epoch.push(record.clone()),
            MetadataRecord::V1TopicConfig(config) => {
                if let Some((archive_topic_id, _)) = topics.get(config.topic.as_str()) {
                    match snapshot_topics.get(config.topic.as_str()) {
                        Some(snapshot_topic_id) if snapshot_topic_id == archive_topic_id => {
                            metadata.topic_configs += 1;
                            topic_configs.push(record.clone());
                        }
                        snapshot_topic_id => {
                            return Err(RestoreError::MetadataSnapshotTopicMismatch {
                                topic: config.topic.clone(),
                                archive_topic_id: *archive_topic_id,
                                snapshot_topic_id: snapshot_topic_id
                                    .map_or_else(|| "absent".to_owned(), ToString::to_string),
                            });
                        }
                    }
                }
            }
            MetadataRecord::V1ScramCredential(_) => {
                metadata.scram_credentials += 1;
                scram.push(record.clone());
            }
            MetadataRecord::V1AccessControlEntry(_) => {
                metadata.access_control_entries += 1;
                acls.push(record.clone());
            }
            MetadataRecord::V1ClientQuota(_) => {
                metadata.client_quotas += 1;
                quotas.push(record.clone());
            }
            _ => {}
        }
    }
    if snapshot_records.is_some() {
        features.extend(
            krabka_metadata::feature_registry()
                .iter()
                .filter(|feature| {
                    feature.name() != krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE
                        && !snapshot_features.contains(feature.name())
                })
                .map(|feature| {
                    MetadataRecord::V1FeatureLevel(krabka_metadata::FeatureLevelRecord {
                        name: feature.name().to_owned(),
                        level: 0,
                    })
                }),
        );
    }

    let mut records = Vec::with_capacity(
        features.len()
            + feature_epoch.len()
            + topic_order.len()
            + inventory.partitions.len()
            + topic_configs.len()
            + scram.len()
            + acls.len()
            + quotas.len(),
    );
    records.append(&mut features);
    records.append(&mut feature_epoch);
    for topic in &topic_order {
        let (topic_id, partitions) = topics
            .get(topic)
            .copied()
            .expect("every topic in topic_order was inserted into topics above");
        records.push(MetadataRecord::V1Topic(TopicRecord {
            name: (*topic).to_owned(),
            topic_id,
            partitions,
            replication_factor: 1,
        }));
    }
    for entry in &inventory.partitions {
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: entry.partition.topic.clone(),
            partition: entry.partition.partition,
            leader: node_id,
            replicas: vec![node_id],
            isr: vec![node_id],
            leader_epoch: LeaderEpoch(0),
            adding_replicas: Vec::new(),
            removing_replicas: Vec::new(),
            directories: Vec::new(),
            // KIP-631: 0 on creation. `PartitionRecord::default()`'s
            // `partition_epoch` is -1, the on-disk deserialization default for
            // a record written before this field existed; a freshly restored
            // partition is neither, so this is set explicitly rather than
            // pulled from `..Default::default()`.
            partition_epoch: 0,
        }));
    }
    records.append(&mut topic_configs);
    records.append(&mut scram);
    records.append(&mut acls);
    records.append(&mut quotas);
    Ok((records, metadata))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_metadata::{
        AclEntry, AclOperation, ClientQuotaRecord, FeatureLevelRecord, FeaturesEpochRecord,
        PatternType, PermissionType, QuotaEntity, ResourceType, ScramCredentialRecord,
        TopicConfigRecord,
    };
    use krabka_security::SaslMechanism;

    use super::*;
    use crate::materialize::test_support::{args_from, partition_inventory};

    #[test]
    fn seed_metadata_records_emits_every_topic_before_any_partition() {
        let orders_id = Uuid::new_v4();
        let payments_id = Uuid::new_v4();
        let inventory = ArchiveInventory {
            partitions: vec![
                partition_inventory("orders", orders_id, 0),
                partition_inventory("orders", orders_id, 1),
                partition_inventory("payments", payments_id, 0),
            ],
            unrecognized: Vec::new(),
        };

        let (records, metadata) =
            seed_metadata_records(&inventory, NodeId(7), None, None).expect("seed metadata");

        let partition_record = |topic: &str, partition: i32| {
            MetadataRecord::V1Partition(PartitionRecord {
                topic: topic.to_owned(),
                partition,
                leader: NodeId(7),
                replicas: vec![NodeId(7)],
                isr: vec![NodeId(7)],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: Vec::new(),
                removing_replicas: Vec::new(),
                directories: Vec::new(),
                partition_epoch: 0,
            })
        };
        let expected = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".to_owned(),
                topic_id: orders_id,
                partitions: 2,
                replication_factor: 1,
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: "payments".to_owned(),
                topic_id: payments_id,
                partitions: 1,
                replication_factor: 1,
            }),
            partition_record("orders", 0),
            partition_record("orders", 1),
            partition_record("payments", 0),
        ];
        check!(records == expected);
        check!(
            metadata.topics_without_configuration
                == vec!["orders".to_owned(), "payments".to_owned()]
        );
    }

    #[test]
    fn seed_metadata_records_restores_supported_snapshot_families_in_dependency_order() {
        let topic_id = Uuid::new_v4();
        let inventory = ArchiveInventory {
            partitions: vec![partition_inventory("orders", topic_id, 0)],
            unrecognized: Vec::new(),
        };
        let feature = MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: "metadata.version".to_owned(),
            level: 25,
        });
        let epoch = MetadataRecord::V1FeaturesEpoch(FeaturesEpochRecord { epoch: 7 });
        let config = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "orders".to_owned(),
            overrides: maplit::btreemap! {"cleanup.policy".to_owned() => "compact".to_owned()},
        });
        let ignored_config = MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "not-restored".to_owned(),
            overrides: maplit::btreemap! {"retention.ms".to_owned() => "1".to_owned()},
        });
        let scram = MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".to_owned(),
            mechanism: SaslMechanism::ScramSha256,
            salt: vec![1; 16],
            stored_key: vec![2; 32],
            server_key: vec![3; 32],
            iterations: 4096,
        });
        let acl = MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::Topic,
            resource_name: "orders".to_owned(),
            pattern_type: PatternType::Literal,
            principal: "User:alice".to_owned(),
            host: "*".to_owned(),
            operation: AclOperation::Read,
            permission_type: PermissionType::Allow,
        });
        let quota = MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "user".to_owned(),
                entity_name: Some("alice".to_owned()),
            }],
            config_key: "producer_byte_rate".to_owned(),
            config_value: Some(1024.0),
        });
        let snapshot_records = vec![
            quota.clone(),
            ignored_config,
            acl.clone(),
            scram.clone(),
            config.clone(),
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".to_owned(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            epoch.clone(),
            feature.clone(),
        ];
        let snapshot = std::path::PathBuf::from("/backup/metadata.checkpoint");

        let (records, report) = seed_metadata_records(
            &inventory,
            NodeId(7),
            Some(snapshot.clone()),
            Some(&snapshot_records),
        )
        .expect("seed metadata");

        let mut expected = vec![feature];
        expected.extend(
            krabka_metadata::feature_registry()
                .iter()
                .filter(|registered| {
                    !matches!(
                        registered.name(),
                        "metadata.version"
                            | krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE
                    )
                })
                .map(|registered| {
                    MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                        name: registered.name().to_owned(),
                        level: 0,
                    })
                }),
        );
        expected.extend([
            epoch,
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".to_owned(),
                topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(PartitionRecord {
                topic: "orders".to_owned(),
                partition: 0,
                leader: NodeId(7),
                replicas: vec![NodeId(7)],
                isr: vec![NodeId(7)],
                leader_epoch: LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            }),
            config,
            scram,
            acl,
            quota,
        ]);
        check!(records == expected);
        check!(
            report
                == MetadataRestoreReport {
                    snapshot: Some(snapshot),
                    topic_configs: 1,
                    access_control_entries: 1,
                    client_quotas: 1,
                    scram_credentials: 1,
                    feature_levels: 1,
                    topics_without_configuration: vec![],
                }
        );
    }

    #[test]
    fn snapshot_topic_config_must_match_the_archived_topic_id() {
        let archive_topic_id = Uuid::new_v4();
        let snapshot_topic_id = Uuid::new_v4();
        let inventory = ArchiveInventory {
            partitions: vec![partition_inventory("orders", archive_topic_id, 0)],
            unrecognized: Vec::new(),
        };
        let snapshot_records = vec![
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".to_owned(),
                topic_id: snapshot_topic_id,
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "orders".to_owned(),
                overrides: maplit::btreemap! {
                    "cleanup.policy".to_owned() => "delete".to_owned()
                },
            }),
        ];

        let result = seed_metadata_records(
            &inventory,
            NodeId(7),
            Some("metadata.checkpoint".into()),
            Some(&snapshot_records),
        );

        check!(matches!(
            result,
            Err(RestoreError::MetadataSnapshotTopicMismatch {
                topic,
                archive_topic_id: archive,
                snapshot_topic_id: snapshot,
            }) if topic == "orders"
                && archive == archive_topic_id
                && snapshot == snapshot_topic_id.to_string()
        ));

        let missing_topic = seed_metadata_records(
            &inventory,
            NodeId(7),
            Some("metadata.checkpoint".into()),
            Some(&snapshot_records[1..]),
        );
        check!(matches!(
            missing_topic,
            Err(RestoreError::MetadataSnapshotTopicMismatch {
                snapshot_topic_id,
                ..
            }) if snapshot_topic_id == "absent"
        ));
    }

    #[tokio::test]
    async fn unreadable_metadata_snapshot_is_an_io_failure() {
        let missing = tempfile::tempdir()
            .expect("temp dir")
            .path()
            .join("missing.checkpoint");

        let result = read_metadata_snapshot(Some(&missing)).await;

        let error = result.unwrap_err();
        check!(error.exit_code() == crate::error::EXIT_MATERIALIZE);
        check!(matches!(error, RestoreError::Io(_)));
    }

    #[tokio::test]
    async fn corrupt_metadata_snapshot_is_an_integrity_failure() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("metadata.checkpoint");
        std::fs::write(&path, b"not a metadata snapshot").expect("write corrupt snapshot");

        let error = read_metadata_snapshot(Some(&path)).await.unwrap_err();

        check!(error.exit_code() == crate::error::EXIT_INTEGRITY);
        check!(matches!(
            error,
            RestoreError::MetadataSnapshot {
                path: error_path,
                ..
            } if error_path == path
        ));
    }

    #[tokio::test]
    async fn format_target_requires_node_id() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(&[], target.path());
        let inventory = ArchiveInventory {
            partitions: vec![partition_inventory("orders", Uuid::new_v4(), 0)],
            unrecognized: Vec::new(),
        };

        let result = format_target(&args, &inventory).await;
        check!(matches!(result, Err(RestoreError::InvalidArgument(_))));
    }

    #[tokio::test]
    async fn format_target_formats_the_target_and_returns_the_cluster_id() {
        let target = tempfile::tempdir().expect("tempdir");
        let args = args_from(
            &[
                "--node-id",
                "1",
                "--standalone",
                "--controller-listener",
                "127.0.0.1:9093",
            ],
            target.path(),
        );
        let topic_id = Uuid::new_v4();
        let inventory = ArchiveInventory {
            partitions: vec![
                partition_inventory("orders", topic_id, 0),
                partition_inventory("orders", topic_id, 1),
            ],
            unrecognized: Vec::new(),
        };

        let outcome = format_target(&args, &inventory)
            .await
            .expect("format_target");
        check!(
            args.target.cluster_id.is_none() || Some(outcome.cluster_id) == args.target.cluster_id
        );
        check!(target.path().join("bootstrap.json").exists());
        check!(target.path().join("bootstrap.records.bin").exists());
    }

    #[tokio::test]
    async fn format_target_honors_an_explicit_cluster_id() {
        let target = tempfile::tempdir().expect("tempdir");
        let fixed = Uuid::new_v4();
        let args = args_from(
            &[
                "--node-id",
                "1",
                "--standalone",
                "--controller-listener",
                "127.0.0.1:9093",
                "--cluster-id",
                &fixed.to_string(),
            ],
            target.path(),
        );
        let inventory = ArchiveInventory {
            partitions: vec![partition_inventory("orders", Uuid::new_v4(), 0)],
            unrecognized: Vec::new(),
        };

        let outcome = format_target(&args, &inventory)
            .await
            .expect("format_target");
        check!(outcome.cluster_id == fixed);
    }
}
