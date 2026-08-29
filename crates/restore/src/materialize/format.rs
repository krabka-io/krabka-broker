//! Formatting the target log directory and seeding it with the topics the
//! archive scan recovered.
//!
//! A restore drives `krabka_format::run_from_args_with_records` in process,
//! forwarding the target-side flags and handing it one `TopicRecord` per topic
//! and one `PartitionRecord` per partition, so the restored cluster boots with
//! its topics already present. This runs once, before any segment is written.

use std::collections::HashMap;

use krabka_ids::LeaderEpoch;
use krabka_metadata::{MetadataRecord, NodeId, PartitionRecord, TopicRecord};
use uuid::Uuid;

use crate::{args::RestoreArgs, discover::ArchiveInventory, error::RestoreError};

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
) -> Result<Uuid, RestoreError> {
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

    let extra = seed_metadata_records(inventory, node_id);
    let code = krabka_format::run_from_args_with_records(format_argv, extra).await;
    if code == 0 {
        Ok(cluster_id)
    } else {
        Err(RestoreError::Format { code })
    }
}

/// Build the topic and partition records a restore seeds into the target formatter, from what the archive scan recovered.
///
/// Every topic's [`MetadataRecord::V1Topic`] precedes every [`MetadataRecord::V1Partition`], which is the ordering `krabka_format::run_with_records`'s own doc requires: a `MetadataImage` derives a topic's partition count from the partition records that apply after it, so a partition can only follow its own topic.
///
/// Pulled out as a pure function, separate from [`format_target`]'s formatter call, so a test can check exactly what gets seeded without running the formatter at all.
fn seed_metadata_records(inventory: &ArchiveInventory, node_id: NodeId) -> Vec<MetadataRecord> {
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

    let mut records = Vec::with_capacity(topic_order.len() + inventory.partitions.len());
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
    records
}

#[cfg(test)]
mod tests {
    use assert2::check;

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

        let records = seed_metadata_records(&inventory, NodeId(7));

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

        let cluster_id = format_target(&args, &inventory)
            .await
            .expect("format_target");
        check!(args.target.cluster_id.is_none() || Some(cluster_id) == args.target.cluster_id);
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

        let cluster_id = format_target(&args, &inventory)
            .await
            .expect("format_target");
        check!(cluster_id == fixed);
    }
}
