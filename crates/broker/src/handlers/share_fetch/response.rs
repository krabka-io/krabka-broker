//! Construction and encoding of the `ShareFetchResponse`, both the per-partition
//! rows and the top-level error shape.
//!
//! Kafka answers a `ShareFetch` with one row per requested partition, grouped
//! by topic in the order the topics first appeared, so the handler collects
//! rows as it resolves them and this module regroups them at the end. A
//! feature-gate, session, or membership failure instead answers with a
//! top-level error code and no rows at all.

use bytes::Bytes;
use krabka_protocol::owned::share_fetch_response::{
    LeaderIdAndEpoch, PartitionData, ShareFetchResponse, ShareFetchableTopicResponse,
};

use super::pending::PendingPartition;
use crate::{codes, error::BrokerError};

pub(super) fn acquisition_timeout_ms(
    config: &crate::coordinator::unified::share::config::ShareGroupConfig,
) -> i32 {
    i32::try_from(config.record_lock_duration.as_millis()).unwrap_or(i32::MAX)
}

pub(super) fn partition_response(partition_index: i32) -> PartitionData {
    PartitionData {
        partition_index,
        ..Default::default()
    }
}

pub(super) fn not_leader_response(
    partition_index: i32,
    leader_id: i32,
    leader_epoch: i32,
) -> PartitionData {
    PartitionData {
        partition_index,
        error_code: codes::NOT_LEADER_OR_FOLLOWER,
        current_leader: LeaderIdAndEpoch {
            leader_id,
            leader_epoch,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Groups the resolved pending partitions back into per-topic response
/// entries. It keeps the order in which the topics first appeared in the
/// request.
pub(super) fn group_responses(pending: Vec<PendingPartition>) -> Vec<ShareFetchableTopicResponse> {
    let mut order: Vec<uuid::Uuid> = Vec::new();
    let mut by_topic: std::collections::HashMap<uuid::Uuid, Vec<PartitionData>> =
        std::collections::HashMap::new();
    for p in pending {
        if !by_topic.contains_key(&p.topic_id) {
            order.push(p.topic_id);
        }
        by_topic.entry(p.topic_id).or_default().push(p.out);
    }
    order
        .into_iter()
        .map(|tid| ShareFetchableTopicResponse {
            topic_id: krabka_protocol::primitives::uuid::Uuid(*tid.as_bytes()),
            partitions: by_topic.remove(&tid).unwrap_or_default(),
            ..Default::default()
        })
        .collect()
}

pub(super) fn encode_success_response(
    version: i16,
    lock_timeout_ms: i32,
    responses: Vec<ShareFetchableTopicResponse>,
) -> Result<Bytes, BrokerError> {
    let response = ShareFetchResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        acquisition_lock_timeout_ms: lock_timeout_ms,
        responses,
        ..Default::default()
    };
    crate::handlers::encode_response(&response, version)
}

/// Encodes a `ShareFetchResponse` that carries a top-level error and no
/// per-partition row. The error is a feature-gate, session, or membership
/// failure.
pub(super) fn encode_error_response(
    version: i16,
    error_code: i16,
    lock_timeout_ms: i32,
) -> Result<Bytes, BrokerError> {
    let resp = ShareFetchResponse {
        throttle_time_ms: 0,
        error_code,
        error_message: None,
        acquisition_lock_timeout_ms: lock_timeout_ms,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::share_fetch_response::{self, AcquiredRecords},
        primitives::uuid::Uuid as ProtoUuid,
    };

    use super::*;

    fn decode_response(bytes: &Bytes) -> ShareFetchResponse {
        crate::test_support::decode_response(bytes, share_fetch_response::MAX_VERSION)
    }

    #[test]
    fn encode_error_response_preserves_top_level_fields() {
        let resp = encode_error_response(
            share_fetch_response::MAX_VERSION,
            codes::UNSUPPORTED_VERSION,
            12_345,
        )
        .expect("encode");
        let resp = decode_response(&resp);

        let expected = ShareFetchResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            acquisition_lock_timeout_ms: 12_345,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn partition_response_helpers_preserve_routing_fields() {
        let ordinary = partition_response(7);
        assert!(ordinary.partition_index == 7);
        assert!(ordinary.error_code == codes::NONE);

        let redirected = not_leader_response(7, 2, 9);
        assert!(redirected.partition_index == 7);
        assert!(redirected.error_code == codes::NOT_LEADER_OR_FOLLOWER);
        assert!(redirected.current_leader.leader_id == 2);
        assert!(redirected.current_leader.leader_epoch == 9);
    }

    #[test]
    fn group_responses_preserves_topic_order_and_partition_fields() {
        let first_topic = uuid::Uuid::from_u128(0xA1);
        let second_topic = uuid::Uuid::from_u128(0xB2);
        let pending = vec![
            PendingPartition {
                topic_id: first_topic,
                topic_name: Some("first".into()),
                partition_index: 0,
                partition_max_bytes: 0,
                leadable: false,
                fetchable: false,
                ack_batches: Vec::new(),
                out: PartitionData {
                    partition_index: 0,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    acknowledge_error_code: codes::NONE,
                    current_leader: LeaderIdAndEpoch {
                        leader_id: -1,
                        leader_epoch: -1,
                        ..Default::default()
                    },
                    acquired_records: vec![AcquiredRecords {
                        first_offset: 4,
                        last_offset: 7,
                        delivery_count: 2,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            },
            PendingPartition {
                topic_id: second_topic,
                topic_name: Some("second".into()),
                partition_index: 3,
                partition_max_bytes: 0,
                leadable: false,
                fetchable: false,
                ack_batches: Vec::new(),
                out: PartitionData {
                    partition_index: 3,
                    error_code: codes::NOT_LEADER_OR_FOLLOWER,
                    current_leader: LeaderIdAndEpoch {
                        leader_id: 2,
                        leader_epoch: 9,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            },
            PendingPartition {
                topic_id: first_topic,
                topic_name: Some("first".into()),
                partition_index: 1,
                partition_max_bytes: 0,
                leadable: false,
                fetchable: false,
                ack_batches: Vec::new(),
                out: PartitionData {
                    partition_index: 1,
                    error_code: codes::NONE,
                    ..Default::default()
                },
            },
        ];

        let responses = group_responses(pending);

        let expected = vec![
            ShareFetchableTopicResponse {
                topic_id: ProtoUuid(*first_topic.as_bytes()),
                partitions: vec![
                    PartitionData {
                        partition_index: 0,
                        error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                        error_message: None,
                        acknowledge_error_code: codes::NONE,
                        acknowledge_error_message: None,
                        current_leader: LeaderIdAndEpoch {
                            leader_id: -1,
                            leader_epoch: -1,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        },
                        records: None,
                        acquired_records: vec![AcquiredRecords {
                            first_offset: 4,
                            last_offset: 7,
                            delivery_count: 2,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        }],
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    },
                    PartitionData {
                        partition_index: 1,
                        error_code: codes::NONE,
                        error_message: None,
                        acknowledge_error_code: codes::NONE,
                        acknowledge_error_message: None,
                        current_leader: LeaderIdAndEpoch {
                            leader_id: 0,
                            leader_epoch: 0,
                            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                        },
                        records: None,
                        acquired_records: Vec::new(),
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    },
                ],
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
            ShareFetchableTopicResponse {
                topic_id: ProtoUuid(*second_topic.as_bytes()),
                partitions: vec![PartitionData {
                    partition_index: 3,
                    error_code: codes::NOT_LEADER_OR_FOLLOWER,
                    error_message: None,
                    acknowledge_error_code: codes::NONE,
                    acknowledge_error_message: None,
                    current_leader: LeaderIdAndEpoch {
                        leader_id: 2,
                        leader_epoch: 9,
                        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                    },
                    records: None,
                    acquired_records: Vec::new(),
                    unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
                }],
                unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
            },
        ];
        assert!(responses == expected);
    }
}
