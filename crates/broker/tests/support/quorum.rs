//! Controller quorum request and response fixtures.

use krabka_protocol::owned::describe_quorum_request::{
    DescribeQuorumRequest, PartitionData, TopicData,
};

pub fn describe_quorum_request(
    topic: impl Into<String>,
    partitions: Vec<i32>,
) -> DescribeQuorumRequest {
    DescribeQuorumRequest {
        topics: vec![TopicData {
            topic_name: topic.into(),
            partitions: partitions
                .into_iter()
                .map(|partition_index| PartitionData {
                    partition_index,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub fn metadata_quorum_request() -> DescribeQuorumRequest {
    describe_quorum_request("__cluster_metadata", vec![0])
}

/// Read the elected quorum leader and retain the two-node fixture's explicit oracle.
///
/// # Panics
/// Panics if the response is missing its metadata partition or has an unexpected leader.
pub fn two_node_leader(
    response: &krabka_protocol::owned::describe_quorum_response::DescribeQuorumResponse,
    expected: (i32, i32),
) -> i32 {
    let leader_id = response.topics[0].partitions[0].leader_id;
    assert2::assert!(
        leader_id == expected.0 || leader_id == expected.1,
        "a 2-node cluster has an elected leader; got {leader_id}"
    );
    leader_id
}

/// The first cluster configuration whose broker is not the elected controller.
///
/// # Panics
/// Panics if no non-leader broker remains in the fixture.
pub fn follower_config(
    cluster: &[(
        krabka_broker::BrokerHandle,
        krabka_broker::BrokerConfig,
        tempfile::TempDir,
    )],
    leader_id: i32,
) -> &krabka_broker::BrokerConfig {
    &cluster
        .iter()
        .find(|(_, config, _)| config.broker_id != leader_id)
        .expect("the non-leader broker")
        .1
}
