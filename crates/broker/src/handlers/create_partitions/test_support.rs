//! Fixture builders shared by the tests of the `CreatePartitions` handler and
//! of its submodules: the request shapes that the tests send, and the
//! metadata records that seed a topic and a controller-mutation quota into a
//! running broker.

use {
    crate::{
        broker::BrokerHandle,
        handlers::test_support::{
            TopicPartitionCount, TopicReplicationFactor,
            seed_controller_quota as seed_controller_quota_fixture, single_replica_partition,
        },
    },
    krabka_metadata::{MetadataRecord, TopicRecord},
    krabka_protocol::owned::{
        create_partitions_request::{
            CreatePartitionsAssignment, CreatePartitionsRequest, CreatePartitionsTopic,
        },
        create_partitions_response::{CreatePartitionsResponse, CreatePartitionsTopicResult},
    },
    krabka_raft::NodeId,
};

pub const VERSION: i16 = 3;

pub fn assn(broker_ids: &[i32]) -> CreatePartitionsAssignment {
    CreatePartitionsAssignment {
        broker_ids: broker_ids.to_vec(),
        ..Default::default()
    }
}

#[bon::builder]
pub fn topic_req(
    #[builder(default = "t")] name: &'_ str,
    #[builder(into, default = TopicPartitionCount (3))] count: TopicPartitionCount,
    assignments: Option<Vec<CreatePartitionsAssignment>>,
) -> CreatePartitionsTopic {
    CreatePartitionsTopic {
        name: name.into(),
        count: count.0,
        assignments,
        ..Default::default()
    }
}

pub fn assigned_topic(
    name: &str,
    count: i32,
    assignments: Vec<CreatePartitionsAssignment>,
) -> CreatePartitionsTopic {
    topic_req()
        .name(name)
        .count(TopicPartitionCount(count))
        .maybe_assignments(Some(assignments))
        .call()
}

#[derive(Clone, Copy, Default)]
pub enum PartitionMutation {
    #[default]
    Apply,
    Validate,
}

pub fn request(
    topics: Vec<CreatePartitionsTopic>,
    mutation: PartitionMutation,
) -> CreatePartitionsRequest {
    CreatePartitionsRequest {
        topics,
        timeout_ms: 5_000,
        validate_only: matches!(mutation, PartitionMutation::Validate),
        ..Default::default()
    }
}

#[bon::builder]
pub async fn seed_topic(
    handle: &BrokerHandle,
    #[builder(default = "t")] name: &'_ str,
    #[builder(into, default = TopicPartitionCount (2))] partitions: TopicPartitionCount,
    #[builder(into, default = TopicReplicationFactor (1))] rf: TopicReplicationFactor,
) {
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: uuid::Uuid::new_v4(),
        partitions: partitions.0,
        replication_factor: rf.0,
    })];
    for partition in 0..partitions.0 {
        records.push(MetadataRecord::V1Partition(single_replica_partition(
            name,
            krabka_ids::PartitionIndex(partition),
            NodeId(handle.node_id()),
        )));
    }
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(records)
        .await
        .expect("seed topic");
}

pub async fn seed_controller_quota(handle: &BrokerHandle, rate: f64) {
    seed_controller_quota_fixture(handle, rate).await;
}

/// Independent complete wire rows expected by the handler tests.
pub fn expected_result(
    name: &str,
    error_code: i16,
    error_message: Option<String>,
) -> CreatePartitionsTopicResult {
    krabka_broker_macros::tagged_wire!(CreatePartitionsTopicResult {
        name: name.into(),
        error_code,
        error_message,
    })
}

pub fn expected_response(results: Vec<CreatePartitionsTopicResult>) -> CreatePartitionsResponse {
    krabka_broker_macros::unthrottled_wire!(CreatePartitionsResponse { results })
}
