//! The request builder and the wire helpers that the
//! `AlterPartitionReassignments` tests share.
//!
//! The response tests and the end-to-end handler tests build the same
//! single-partition request and decode the same response type, so the
//! fixtures live in one module rather than once per test file.

use std::net::SocketAddr;

use bytes::Bytes;
use krabka_protocol::owned::{
    alter_partition_reassignments_request::{
        AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
    },
    alter_partition_reassignments_response::AlterPartitionReassignmentsResponse,
};
use krabka_security::Principal;

pub(super) fn request(
    allow_replication_factor_change: bool,
    topic: &str,
    partition_index: i32,
    replicas: Option<Vec<i32>>,
) -> AlterPartitionReassignmentsRequest {
    AlterPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        allow_replication_factor_change,
        topics: vec![ReassignableTopic {
            name: topic.into(),
            partitions: vec![ReassignablePartition {
                partition_index,
                replicas,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub(super) fn decode_response(bytes: &Bytes, version: i16) -> AlterPartitionReassignmentsResponse {
    crate::test_support::decode_response(bytes, version)
}

pub(super) fn test_context<'a>(
    principal: &'a Principal,
    peer: &'a SocketAddr,
) -> crate::handlers::RequestContext<'a> {
    crate::test_support::request_context(principal, peer, "admin-client")
}
