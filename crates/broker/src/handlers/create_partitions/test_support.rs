//! Fixture builders shared by the tests of the `CreatePartitions` handler and
//! of its submodules: the request shapes that the tests send, and the
//! metadata records that seed a topic and a controller-mutation quota into a
//! running broker.

use krabka_metadata::{MetadataRecord, PartitionRecord, TopicRecord};
use krabka_protocol::owned::create_partitions_request::{
    CreatePartitionsAssignment, CreatePartitionsRequest, CreatePartitionsTopic,
};
use krabka_raft::NodeId;

use crate::broker::BrokerHandle;

pub const VERSION: i16 = 3;

pub fn assn(broker_ids: &[i32]) -> CreatePartitionsAssignment {
    CreatePartitionsAssignment {
        broker_ids: broker_ids.to_vec(),
        ..Default::default()
    }
}

pub fn topic_req(
    name: &str,
    count: i32,
    assignments: Option<Vec<CreatePartitionsAssignment>>,
) -> CreatePartitionsTopic {
    CreatePartitionsTopic {
        name: name.into(),
        count,
        assignments,
        ..Default::default()
    }
}

pub fn request(topics: Vec<CreatePartitionsTopic>, validate_only: bool) -> CreatePartitionsRequest {
    CreatePartitionsRequest {
        topics,
        timeout_ms: 5_000,
        validate_only,
        ..Default::default()
    }
}

pub async fn seed_topic(handle: &BrokerHandle, name: &str, partitions: i32, rf: i16) {
    let replicas = vec![NodeId(handle.node_id())];
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: uuid::Uuid::new_v4(),
        partitions,
        replication_factor: rf,
    })];
    for partition in 0..partitions {
        records.push(MetadataRecord::V1Partition(PartitionRecord {
            topic: name.into(),
            partition,
            leader: NodeId(handle.node_id()),
            replicas: replicas.clone(),
            isr: replicas.clone(),
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
    }
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(records)
        .await
        .expect("seed topic");
}

pub async fn seed_controller_quota(handle: &BrokerHandle, rate: f64) {
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
