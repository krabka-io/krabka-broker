//! The [`MetadataProvider`] the running broker uses: a projection of the
//! controller's current `MetadataImage` into the topic ids, partition counts,
//! and partition racks that the assignors need.
//!
//! It is the one place where cluster metadata crosses into the coordinator, so
//! it sits apart from the coordinator state it feeds.

use std::sync::Arc;

use super::{actor::MetadataProvider, reconciler};

/// `MetadataProvider` backed by `krabka_raft::ControllerHandle::current_image()`.
pub struct ImageMetadataProvider {
    pub controller: Arc<dyn crate::metadata_source::MetadataSource>,
}

impl std::fmt::Debug for ImageMetadataProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageMetadataProvider")
            .finish_non_exhaustive()
    }
}

impl MetadataProvider for ImageMetadataProvider {
    fn snapshot(&self) -> reconciler::ReconcileInput {
        use krabka_protocol::primitives::uuid::Uuid as ProtoUuid;
        let image = self.controller.current_image();
        let mut topic_id_by_name = std::collections::HashMap::new();
        let mut partitions_per_topic = std::collections::HashMap::new();
        let mut partition_racks: std::collections::HashMap<(ProtoUuid, i32), Vec<String>> =
            std::collections::HashMap::new();
        for topic in image.topics() {
            let proto_id = ProtoUuid(*topic.topic_id.as_bytes());
            topic_id_by_name.insert(topic.name.clone(), proto_id);
            partitions_per_topic.insert(proto_id, topic.partitions);
            // Collect the set of racks the partition's
            // replicas are on, so the rack-aware UniformAssignor can
            // prefer rack-collocated subscribers. Partitions whose
            // replicas have no rack info don't get an entry — the
            // assignor then falls back to its non-rack-aware path.
            for pr in image.partitions_of(&topic.name) {
                let mut racks: Vec<String> = pr
                    .replicas
                    .iter()
                    .filter_map(|&node_id| image.broker(node_id).and_then(|b| b.rack.clone()))
                    .collect();
                racks.sort();
                racks.dedup();
                if !racks.is_empty() {
                    partition_racks.insert((proto_id, pr.partition), racks);
                }
            }
        }
        reconciler::ReconcileInput {
            topic_id_by_name,
            partitions_per_topic,
            partition_racks,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::coordinator::unified::test_support::{fixed_source, real_uuid};

    #[test]
    fn image_metadata_provider_snapshot_projects_topics_partitions_and_racks() {
        let mut image = krabka_metadata::MetadataImage::new(real_uuid(9));
        let topic_id = real_uuid(8);
        image.apply(&krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: "input".into(),
                topic_id,
                partitions: 3,
                replication_factor: 2,
            },
        ));
        for (node_id, rack) in [
            (1, Some("rack-a".to_string())),
            (2, Some("rack-b".to_string())),
            (3, None),
        ] {
            image.apply(&krabka_metadata::MetadataRecord::V1BrokerRegistration(
                krabka_metadata::BrokerRegistrationRecord {
                    node_id: krabka_metadata::NodeId(node_id),
                    broker_epoch: i64::try_from(node_id).unwrap(),
                    incarnation_id: real_uuid(u8::try_from(node_id).unwrap()),
                    host: format!("broker-{node_id}"),
                    port: 9092,
                    rack,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: std::collections::BTreeMap::new(),
                },
            ));
        }
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "input".into(),
                partition: 0,
                leader: krabka_metadata::NodeId(1),
                replicas: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
                isr: vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)],
                directories: vec![real_uuid(1), real_uuid(2)],
                ..Default::default()
            },
        ));
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                topic: "input".into(),
                partition: 1,
                leader: krabka_metadata::NodeId(3),
                replicas: vec![krabka_metadata::NodeId(3)],
                isr: vec![krabka_metadata::NodeId(3)],
                directories: vec![real_uuid(3)],
                ..Default::default()
            },
        ));

        let provider = ImageMetadataProvider {
            controller: fixed_source(image),
        };
        let snapshot = provider.snapshot();
        let proto_topic_id = krabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes());

        check!(snapshot.topic_id_by_name.get("input") == Some(&proto_topic_id));
        check!(snapshot.partitions_per_topic.get(&proto_topic_id) == Some(&2));
        check!(
            snapshot.partition_racks.get(&(proto_topic_id, 0))
                == Some(&vec!["rack-a".to_string(), "rack-b".to_string()])
        );
        check!(snapshot.partition_racks.get(&(proto_topic_id, 1)) == None);
    }
}
