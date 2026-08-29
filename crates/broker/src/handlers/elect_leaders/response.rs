//! The wire shape of an `ElectLeaders` answer: the error code and text each
//! election failure carries, and the encoder both the whole-request refusals
//! and the successful path share.
//!
//! Kafka reports an `ElectLeaders` failure on the partition row rather than at
//! the top level, so even a request the broker refuses outright answers with
//! `error_code` 0 and stamps the real code on every row the client asked for.

use bytes::Bytes;
use krabka_protocol::{
    Encode,
    owned::{
        elect_leaders_request::ElectLeadersRequest,
        elect_leaders_response::{ElectLeadersResponse, PartitionResult, ReplicaElectionResult},
    },
};

use crate::{codes, leader_election::ElectError};

pub(super) fn elect_error_to_wire(err: ElectError) -> (i16, &'static str) {
    match err {
        ElectError::UnknownTopicOrPartition => (
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            "unknown topic or partition",
        ),
        ElectError::PreferredAlreadyLeader => (
            codes::ELECTION_NOT_NEEDED,
            "preferred replica is already leader",
        ),
        ElectError::ElectionNotNeeded => (
            codes::ELECTION_NOT_NEEDED,
            "isr still has a live member; unclean election not needed",
        ),
        ElectError::PreferredNotInIsr => (
            codes::PREFERRED_LEADER_NOT_AVAILABLE,
            "preferred replica not in ISR",
        ),
        ElectError::PreferredNotAlive => (
            codes::PREFERRED_LEADER_NOT_AVAILABLE,
            "preferred replica not alive",
        ),
        ElectError::PreferredIsWitness => (
            codes::PREFERRED_LEADER_NOT_AVAILABLE,
            "preferred replica is a witness and cannot lead",
        ),
        ElectError::NoEligibleReplica => {
            (codes::ELIGIBLE_LEADERS_NOT_AVAILABLE, "no alive replica")
        }
    }
}

pub(super) fn encode_whole_request_error(
    req: &ElectLeadersRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    // Build a response where every requested (topic, partition) row
    // carries the whole-request error code. Top-level error_code = 0
    // since the per-row codes carry the failure (matches Kafka).
    let results: Vec<ReplicaElectionResult> = match &req.topic_partitions {
        None => vec![],
        Some(list) => list
            .iter()
            .map(|tp| ReplicaElectionResult {
                topic: tp.topic.clone(),
                partition_result: tp
                    .partitions
                    .iter()
                    .map(|&p| PartitionResult {
                        partition_id: p,
                        error_code: code,
                        error_message: Some(msg.into()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
    };
    let resp = ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: 0,
        replica_election_results: results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

pub(super) fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(resp, api_version, "encode ElectLeaders")
}
