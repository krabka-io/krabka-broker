//! The [`MetadataProvider`] the running broker uses: a projection of the
//! controller's current `MetadataImage` into the topic ids, partition counts,
//! and partition racks that the assignors need.
//!
//! It is the one place where cluster metadata crosses into the coordinator, so
//! it sits apart from the coordinator state it feeds.

use {
    super::{actor::MetadataProvider, assignor::TopicIdPartition, reconciler},
    crate::metadata_source::MetadataSource,
    krabka_metadata::MetadataImage,
    std::sync::{Arc, Mutex},
};

/// `MetadataProvider` backed by `krabka_raft::ControllerHandle::current_image()`.
#[derive(derive_more::Debug)]
pub struct ImageMetadataProvider {
    #[debug(skip)]
    controller: Arc<dyn MetadataSource>,
    #[debug(skip)]
    cached: Mutex<Option<CachedSnapshot>>,
}

struct CachedSnapshot {
    // Retain the source allocation, so pointer identity cannot suffer ABA.
    image: Arc<MetadataImage>,
    input: Arc<reconciler::ReconcileInput>,
}

impl ImageMetadataProvider {
    #[must_use]
    pub fn new(controller: Arc<dyn MetadataSource>) -> Self {
        Self {
            controller,
            cached: Mutex::new(None),
        }
    }
}

impl MetadataProvider for ImageMetadataProvider {
    fn snapshot(&self) -> Arc<reconciler::ReconcileInput> {
        let image = self.controller.current_image();
        let mut cached = self
            .cached
            .lock()
            .expect("coordinator metadata cache poisoned");
        if let Some(previous) = cached.as_ref()
            && Arc::ptr_eq(&previous.image, &image)
        {
            return Arc::clone(&previous.input);
        }
        let input = Arc::new(project_image(&image));
        *cached = Some(CachedSnapshot {
            image,
            input: Arc::clone(&input),
        });
        input
    }

    fn topic_name(&self, topic_id: &krabka_protocol::primitives::uuid::Uuid) -> Option<String> {
        self.controller
            .current_image()
            .topic_name_by_id(&uuid::Uuid::from_bytes(topic_id.0))
            .map(str::to_owned)
    }
}

/// Project exactly one immutable controller image, preserving replica rack multiplicity.
fn project_image(image: &MetadataImage) -> reconciler::ReconcileInput {
    use krabka_protocol::primitives::uuid::Uuid as ProtoUuid;
    let mut topic_id_by_name = std::collections::HashMap::new();
    let mut partitions_per_topic = std::collections::HashMap::new();
    let mut partition_racks: std::collections::HashMap<TopicIdPartition, Vec<String>> =
        std::collections::HashMap::new();
    for topic in image.topics() {
        let proto_id = ProtoUuid(*topic.topic_id.as_bytes());
        topic_id_by_name.insert(topic.name.clone(), proto_id);
        // Kafka's `KRaftCoordinatorMetadataImage.TopicMetadata` counts the
        // partitions that the image holds.
        partitions_per_topic.insert(proto_id, image.topic_partition_count(&topic.name));
        // The rack of each replica whose broker has one, as Kafka's
        // `partitionRacks` lists them: the metadata hash reads every
        // entry, and the assignors read the set. Partitions whose
        // replicas have no rack info don't get an entry.
        for pr in image.partitions_of(&topic.name) {
            let racks: Vec<String> = pr
                .replicas
                .iter()
                .filter_map(|&node_id| image.broker(node_id).and_then(|b| b.rack.clone()))
                .collect();
            if !racks.is_empty() {
                partition_racks.insert(
                    TopicIdPartition {
                        topic_id: proto_id,
                        partition: pr.partition,
                    },
                    racks,
                );
            }
        }
    }
    reconciler::ReconcileInput {
        topic_id_by_name,
        partitions_per_topic,
        partition_racks,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            coordinator::unified::test_support::{fixed_source, real_uuid},
            test_support::{FakeMetadataSource, broker_registration, in_sync_partition},
        },
        assert2::{assert, check},
    };

    fn projected_image(topic_id: u8, partitions: i32, racks: &[&str]) -> MetadataImage {
        use krabka_metadata::{MetadataRecord, NodeId, TopicRecord};

        let mut image = MetadataImage::new(real_uuid(9));
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "input".into(),
            topic_id: real_uuid(topic_id),
            partitions,
            replication_factor: i16::try_from(racks.len()).unwrap(),
        }));
        let replicas: Vec<_> = racks
            .iter()
            .enumerate()
            .map(|(index, rack)| {
                let node = NodeId(u64::try_from(index + 1).unwrap());
                image.apply(&MetadataRecord::V1BrokerRegistration(
                    krabka_metadata::BrokerRegistrationRecord {
                        rack: Some((*rack).into()),
                        ..broker_registration(krabka_raft::NodeId(node.0))
                    },
                ));
                node
            })
            .collect();
        for partition in 0..partitions {
            image.apply(&MetadataRecord::V1Partition(
                in_sync_partition()
                    .topic("input")
                    .partition(partition)
                    .replicas(replicas.clone())
                    .directories(replicas.iter().map(|_| real_uuid(1)).collect())
                    .call(),
            ));
        }
        image
    }

    #[test]
    fn unchanged_image_reuses_the_projection_and_new_images_preserve_old_snapshots() {
        let image = projected_image(1, 2, &["rack-a", "rack-a", "rack-b"]);
        let source = Arc::new(FakeMetadataSource::static_image(image));
        let provider = ImageMetadataProvider::new(source.clone());
        let original = provider.snapshot();
        for _ in 0..100 {
            assert!(Arc::ptr_eq(&original, &provider.snapshot()));
        }
        let original_hash = original.metadata_hash(["input"]);
        let topic = krabka_protocol::primitives::uuid::Uuid(real_uuid(1).into_bytes());
        assert!(
            original.partition_racks[&TopicIdPartition {
                topic_id: topic,
                partition: 0
            }] == vec!["rack-a", "rack-a", "rack-b"]
        );

        for replacement in [
            projected_image(2, 2, &["rack-a", "rack-a", "rack-b"]),
            projected_image(1, 3, &["rack-a", "rack-a", "rack-b"]),
            projected_image(1, 2, &["rack-a", "rack-a", "rack-c"]),
            projected_image(1, 2, &["rack-a", "rack-b"]),
            MetadataImage::new(real_uuid(9)),
        ] {
            source.set_image(replacement);
            let current = provider.snapshot();
            assert!(!Arc::ptr_eq(&original, &current));
            assert!(Arc::ptr_eq(&current, &provider.snapshot()));
            assert!(current.metadata_hash(["input"]) != original_hash);
            assert!(original.metadata_hash(["input"]) == original_hash);
            assert!(original.partitions_per_topic.get(&topic) == Some(&2));
        }
    }

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
                    broker_epoch: i64::try_from(node_id).unwrap(),
                    incarnation_id: real_uuid(u8::try_from(node_id).unwrap()),
                    host: format!("broker-{node_id}"),
                    rack,
                    ..broker_registration(krabka_raft::NodeId(node_id))
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

        let provider = ImageMetadataProvider::new(fixed_source(image));
        let snapshot = provider.snapshot();
        let proto_topic_id = krabka_protocol::primitives::uuid::Uuid(*topic_id.as_bytes());

        check!(snapshot.topic_id_by_name.get("input") == Some(&proto_topic_id));
        check!(snapshot.partitions_per_topic.get(&proto_topic_id) == Some(&2));
        check!(
            snapshot.partition_racks.get(&TopicIdPartition {
                topic_id: proto_topic_id,
                partition: 0
            }) == Some(&vec!["rack-a".to_string(), "rack-b".to_string()])
        );
        check!(
            snapshot.partition_racks.get(&TopicIdPartition {
                topic_id: proto_topic_id,
                partition: 1
            }) == None
        );
    }

    /// The consumer and share groups hash the provider's snapshot, and the
    /// streams groups hash the image itself: both must give Kafka's
    /// `Utils.computeTopicHash`, which lists the rack of every replica, two
    /// replicas on one rack included.
    #[test]
    fn the_snapshot_hashes_a_topic_as_the_image_does() {
        use {
            crate::coordinator::unified::topic_hash::image_topic_hash,
            krabka_metadata::{BrokerRegistrationRecord, MetadataRecord, NodeId},
        };

        let mut image = krabka_metadata::MetadataImage::new(real_uuid(9));
        image.apply(&MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: "input".into(),
            topic_id: real_uuid(8),
            partitions: 2,
            replication_factor: 3,
        }));
        for (node_id, rack) in [
            (1, Some("rack-b")),
            (2, Some("rack-a")),
            (3, Some("rack-b")),
        ] {
            image.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    rack: rack.map(str::to_owned),
                    ..broker_registration(krabka_raft::NodeId(node_id))
                },
            ));
        }
        for (partition, replicas) in [(0, vec![1, 2, 3]), (1, vec![3])] {
            image.apply(&MetadataRecord::V1Partition(
                in_sync_partition()
                    .topic("input")
                    .partition(partition)
                    .replicas(replicas.iter().copied().map(NodeId).collect())
                    .directories(replicas.iter().map(|_| real_uuid(1)).collect())
                    .call(),
            ));
        }
        let snapshot = ImageMetadataProvider::new(fixed_source(image.clone())).snapshot();

        let from_image = image_topic_hash("input", &image);
        check!(from_image.is_some());
        check!(snapshot.topic_hash("input") == from_image);
        check!(
            snapshot.metadata_hash(["input", "absent"])
                == crate::coordinator::unified::topic_hash::image_metadata_hash(["input"], &image)
        );
    }
}
