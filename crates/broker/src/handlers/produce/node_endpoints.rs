//! KIP-951 `NodeEndpoints` for the `Produce` response: the address of every
//! node a partition row names in its `CurrentLeader` hint.
//!
//! The hint alone is a node id and an epoch. A Java producer's `Sender` hands
//! the response's `nodeEndpoints()` to `Metadata.updatePartitionLeadership`,
//! and with an empty array it cannot resolve the new leader's address. It logs
//! that the leader node is not known and falls back to a full `Metadata`
//! round-trip, which is exactly the round-trip KIP-951 removes.

use krabka_protocol::owned::produce_response::{NodeEndpoint, TopicProduceResponse};

/// The endpoint of every node named by a `CurrentLeader` hint in `topics`, one
/// entry per node id and ordered by it.
///
/// `connection_listener_name` is the listener the request arrived on. Kafka
/// answers with the advertised address of that listener, so a client on the
/// external listener gets the external `host:port`, and this resolves each
/// node through the same [`crate::handlers::metadata::pick_endpoint_host_port`]
/// selection that `Metadata` and `DescribeCluster` project a broker with.
///
/// A node the image does not know contributes no entry: there is no address to
/// advertise for it, and Kafka likewise skips a leader that its metadata cache
/// cannot resolve to a live node.
pub(super) fn produce_node_endpoints(
    image: &krabka_metadata::MetadataImage,
    connection_listener_name: &str,
    inter_broker_listener_name: &str,
    topics: &[TopicProduceResponse],
) -> Vec<NodeEndpoint> {
    let mut node_ids: Vec<i32> = topics
        .iter()
        .flat_map(|topic| topic.partition_responses.iter())
        .map(|partition| partition.current_leader.leader_id)
        .filter(|leader_id| *leader_id >= 0)
        .collect();
    node_ids.sort_unstable();
    node_ids.dedup();
    node_ids
        .into_iter()
        .filter_map(|node_id| {
            let record = image.broker(krabka_metadata::NodeId(u64::try_from(node_id).ok()?))?;
            let (host, port) = crate::handlers::metadata::pick_endpoint_host_port(
                record,
                connection_listener_name,
                inter_broker_listener_name,
            );
            Some(NodeEndpoint {
                node_id,
                host,
                port,
                rack: record.rack.clone(),
                ..Default::default()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{
        BrokerEndpoint, BrokerRegistrationRecord, MetadataImage, MetadataRecord, NodeId,
    };
    use krabka_protocol::owned::produce_response::{
        LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse,
    };

    use super::*;

    fn endpoint(name: &str, host: &str, port: u16) -> BrokerEndpoint {
        BrokerEndpoint {
            name: name.to_string(),
            host: host.to_string(),
            port,
            protocol: krabka_security::ListenerProtocol::Plaintext,
        }
    }

    /// Two registered brokers, each advertising an internal and an external
    /// address, and only node 2 carrying a rack.
    fn image() -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (node_id, rack) in [(1_u64, None), (2, Some("rack-b".to_string()))] {
            image.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: NodeId(node_id),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::nil(),
                    host: format!("legacy-{node_id}"),
                    port: 1000,
                    rack,
                    endpoints: vec![
                        endpoint("INTERNAL", &format!("internal-{node_id}"), 9092),
                        endpoint("EXTERNAL", &format!("external-{node_id}"), 9093),
                    ],
                    log_dirs: vec![],
                    features: std::collections::BTreeMap::new(),
                },
            ));
        }
        image
    }

    /// One topic whose partition rows hint at `leader_ids`, one row each.
    fn topics(leader_ids: &[i32]) -> Vec<TopicProduceResponse> {
        vec![TopicProduceResponse {
            name: "orders".to_string(),
            partition_responses: leader_ids
                .iter()
                .enumerate()
                .map(|(index, leader_id)| PartitionProduceResponse {
                    index: i32::try_from(index).expect("test row count fits an i32"),
                    error_code: crate::codes::NOT_LEADER_OR_FOLLOWER,
                    current_leader: LeaderIdAndEpoch {
                        leader_id: *leader_id,
                        leader_epoch: 7,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }]
    }

    fn expected(node_id: i32, host: &str, port: i32, rack: Option<&str>) -> NodeEndpoint {
        NodeEndpoint {
            node_id,
            host: host.to_string(),
            port,
            rack: rack.map(ToString::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn one_entry_per_hinted_node_on_the_connection_listener() {
        let image = image();
        for (name, listener, leader_ids, want) in [
            (
                "no hint at all",
                "EXTERNAL",
                vec![-1, -1],
                Vec::<NodeEndpoint>::new(),
            ),
            (
                "the external client gets the external addresses",
                "EXTERNAL",
                vec![1, 2],
                vec![
                    expected(1, "external-1", 9093, None),
                    expected(2, "external-2", 9093, Some("rack-b")),
                ],
            ),
            (
                "the internal client gets the internal addresses",
                "INTERNAL",
                vec![2, 1],
                vec![
                    expected(1, "internal-1", 9092, None),
                    expected(2, "internal-2", 9092, Some("rack-b")),
                ],
            ),
            (
                "many rows naming one node collapse to one entry",
                "EXTERNAL",
                vec![2, 2, 2, -1],
                vec![expected(2, "external-2", 9093, Some("rack-b"))],
            ),
            (
                "a node the image does not know contributes nothing",
                "EXTERNAL",
                vec![9],
                Vec::new(),
            ),
            (
                "an unknown listener falls back to the inter-broker one",
                "NONESUCH",
                vec![1],
                vec![expected(1, "internal-1", 9092, None)],
            ),
        ] {
            let got = produce_node_endpoints(&image, listener, "INTERNAL", &topics(&leader_ids));
            assert!(got == want, "{name}: got {got:?}, want {want:?}");
        }
    }
}
