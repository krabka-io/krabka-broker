//! Native admin request and cluster fixtures.

use krabka_client_admin::CreateTopicSpec;

pub fn topic_spec(name: impl Into<String>, partitions: i32, replicas: i32) -> CreateTopicSpec {
    CreateTopicSpec {
        name: name.into(),
        partitions,
        replicas,
        configs: std::collections::BTreeMap::default(),
        replica_assignments: std::collections::BTreeMap::default(),
    }
}

/// Partition error rows for the first election result naming the requested topic.
pub fn election_partition_errors(
    response: krabka_protocol::owned::elect_leaders_response::ElectLeadersResponse,
    topic: &str,
) -> Vec<(i32, i16)> {
    response
        .replica_election_results
        .into_iter()
        .find(|row| row.topic == topic)
        .map(|row| {
            row.partition_result
                .into_iter()
                .map(|partition| (partition.partition_id, partition.error_code))
                .collect()
        })
        .unwrap_or_default()
}

/// Start the native admin fixtures' standalone broker and default admin client.
/// The directory precedes its broker and client in the return tuple.
///
/// # Panics
/// Panics if broker startup or the admin connection fails.
pub async fn standalone_admin() -> (
    tempfile::TempDir,
    krabka_broker::BrokerHandle,
    String,
    krabka_client_admin::AdminClient,
) {
    let (dir, broker) = crate::support::standalone_broker().await;
    let bootstrap = broker.listen_addr().to_string();
    let admin = krabka_client_admin::AdminClient::connect(std::slice::from_ref(&bootstrap))
        .await
        .unwrap();
    (dir, broker, bootstrap, admin)
}

/// A one-topic producer-state request, retaining the complete protocol defaults.
pub fn describe_producers_request(
    name: String,
    partition_indexes: Vec<i32>,
) -> krabka_protocol::owned::describe_producers_request::DescribeProducersRequest {
    krabka_protocol::owned::describe_producers_request::DescribeProducersRequest {
        topics: vec![
            krabka_protocol::owned::describe_producers_request::TopicRequest {
                name,
                partition_indexes,
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}
