//! Unauthenticated wire drivers for `AlterPartitionReassignments` (`api_key`
//! 45) and `ListPartitionReassignments` (`api_key` 46).
//!
//! Each driver opens a fresh PLAINTEXT connection, encodes the request, and
//! flattens the decoded response into plain tuples that a test can compare
//! directly.

use std::net::SocketAddr;

use bytes::BytesMut;
use krabka_protocol::{Decode, Encode};
use tokio::net::TcpStream;

use crate::plaintext_wire::round_trip;

/// Drives `AlterPartitionReassignments` over a fresh PLAINTEXT connection. It
/// returns `(topic_name, [(partition_index, error_code)])` rows.
pub async fn drive_alter_reassignments(
    addr: SocketAddr,
    rows: Vec<(&str, i32, Option<Vec<i32>>)>,
) -> Vec<(String, Vec<(i32, i16)>)> {
    use krabka_protocol::owned::{
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        alter_partition_reassignments_response::AlterPartitionReassignmentsResponse,
    };

    // Group by topic.
    let mut by_topic: std::collections::BTreeMap<String, Vec<ReassignablePartition>> =
        std::collections::BTreeMap::new();
    for (topic, partition, target_opt) in rows {
        by_topic
            .entry(topic.to_string())
            .or_default()
            .push(ReassignablePartition {
                partition_index: partition,
                replicas: target_opt,
                ..Default::default()
            });
    }
    let topics: Vec<ReassignableTopic> = by_topic
        .into_iter()
        .map(|(name, partitions)| ReassignableTopic {
            name,
            partitions,
            ..Default::default()
        })
        .collect();
    let req = AlterPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        allow_replication_factor_change: true,
        topics,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, 1)
        .expect("encode AlterPartitionReassignments");
    let resp_bytes = round_trip(&mut stream, 45, 1, 1, true, &body)
        .await
        .expect("AlterPartitionReassignments round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = AlterPartitionReassignmentsResponse::decode(&mut cur, 1)
        .expect("decode AlterPartitionReassignmentsResponse");

    resp.responses
        .into_iter()
        .map(|r| {
            (
                r.name,
                r.partitions
                    .into_iter()
                    .map(|p| (p.partition_index, p.error_code))
                    .collect(),
            )
        })
        .collect()
}

/// Drives `ListPartitionReassignments` over a fresh PLAINTEXT connection. It
/// returns
/// `(topic_name, [(partition_index, replicas, adding_replicas, removing_replicas)])`
/// rows.
pub async fn drive_list_reassignments(
    addr: SocketAddr,
    filter: Option<Vec<(&str, Vec<i32>)>>,
) -> Vec<(String, Vec<(i32, Vec<i32>, Vec<i32>, Vec<i32>)>)> {
    use krabka_protocol::owned::{
        list_partition_reassignments_request::{
            ListPartitionReassignmentsRequest, ListPartitionReassignmentsTopics,
        },
        list_partition_reassignments_response::ListPartitionReassignmentsResponse,
    };

    let topics_arg = filter.map(|list| {
        list.into_iter()
            .map(
                |(name, partition_indexes)| ListPartitionReassignmentsTopics {
                    name: name.to_string(),
                    partition_indexes,
                    ..Default::default()
                },
            )
            .collect()
    });
    let req = ListPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        topics: topics_arg,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    let mut body = BytesMut::new();
    req.encode(&mut body, 0)
        .expect("encode ListPartitionReassignments");
    let resp_bytes = round_trip(&mut stream, 46, 0, 1, true, &body)
        .await
        .expect("ListPartitionReassignments round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp = ListPartitionReassignmentsResponse::decode(&mut cur, 0)
        .expect("decode ListPartitionReassignmentsResponse");

    resp.topics
        .into_iter()
        .map(|t| {
            (
                t.name,
                t.partitions
                    .into_iter()
                    .map(|p| {
                        (
                            p.partition_index,
                            p.replicas,
                            p.adding_replicas,
                            p.removing_replicas,
                        )
                    })
                    .collect(),
            )
        })
        .collect()
}
