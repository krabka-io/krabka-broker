//! The three responses this handler can send, and the encoder they all pass
//! through.
//!
//! The success path echoes the request's directory, topic, and partition
//! structure back, so building the response is a pure mapping that is worth
//! testing without a broker.

use bytes::Bytes;
use krabka_protocol::owned::{
    assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
    assign_replicas_to_dirs_response::{
        AssignReplicasToDirsResponse, DirectoryData as RespDirData, PartitionData as RespPartData,
        TopicData as RespTopicData,
    },
};

use crate::{codes, error::BrokerError};

pub(super) fn not_controller_response() -> AssignReplicasToDirsResponse {
    AssignReplicasToDirsResponse {
        error_code: codes::NOT_CONTROLLER,
        ..Default::default()
    }
}

/// Builds the success-path echo response from `req`. It mirrors the request's
/// directory, topic, and partition structure, and fills every partition's
/// `error_code` with `NONE`. The function is pure and does no I/O.
pub(crate) fn build_echo_response(
    req: &AssignReplicasToDirsRequest,
) -> AssignReplicasToDirsResponse {
    let directories = req
        .directories
        .iter()
        .map(|dir| RespDirData {
            id: dir.id,
            topics: dir
                .topics
                .iter()
                .map(|t| RespTopicData {
                    topic_id: t.topic_id,
                    partitions: t
                        .partitions
                        .iter()
                        .map(|p| RespPartData {
                            partition_index: p.partition_index,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    AssignReplicasToDirsResponse {
        directories,
        ..Default::default()
    }
}

pub(super) fn encode_resp(
    version: crate::handlers::ApiVersion,
    resp: &AssignReplicasToDirsResponse,
) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{
        owned::assign_replicas_to_dirs_request::{
            DirectoryData as ReqDirData, PartitionData as ReqPartData, TopicData as ReqTopicData,
        },
        primitives::uuid::Uuid as ProtocolUuid,
    };

    use super::*;
    use crate::handlers::assign_replicas_to_dirs::test_support::{VERSION, decode_response};

    #[test]
    fn not_controller_response_preserves_error_code() {
        let resp = not_controller_response();
        assert!(resp.error_code == codes::NOT_CONTROLLER, "{resp:?}");
        assert!(resp.directories.is_empty(), "{resp:?}");
    }

    #[test]
    fn encode_resp_preserves_encoded_body() {
        let req = AssignReplicasToDirsRequest {
            broker_id: 1,
            broker_epoch: -1,
            directories: vec![ReqDirData {
                id: ProtocolUuid(uuid::Uuid::from_u128(0xAA).into_bytes()),
                topics: vec![ReqTopicData {
                    topic_id: ProtocolUuid(uuid::Uuid::from_u128(0xBB).into_bytes()),
                    partitions: vec![ReqPartData {
                        partition_index: 3,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let resp = build_echo_response(&req);

        let bytes = encode_resp(VERSION, &resp).expect("encode response");
        let decoded = decode_response(&bytes);

        let expected = AssignReplicasToDirsResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            directories: vec![RespDirData {
                id: ProtocolUuid(uuid::Uuid::from_u128(0xAA).into_bytes()),
                topics: vec![RespTopicData {
                    topic_id: ProtocolUuid(uuid::Uuid::from_u128(0xBB).into_bytes()),
                    partitions: vec![RespPartData {
                        partition_index: 3,
                        error_code: codes::NONE,
                        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                    }],
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(decoded == expected, "{decoded:?}");
    }

    // ── build_echo_response ───────────────────────────────────────────────────

    #[test]
    fn build_echo_response_mirrors_request_structure_with_none_error_codes() {
        let dir_id_bytes = uuid::Uuid::from_u128(0xBB).into_bytes();
        let topic_id_bytes = uuid::Uuid::from_u128(0x5).into_bytes();

        let req = AssignReplicasToDirsRequest {
            broker_id: 1,
            broker_epoch: -1,
            directories: vec![ReqDirData {
                id: ProtocolUuid(dir_id_bytes),
                topics: vec![ReqTopicData {
                    topic_id: ProtocolUuid(topic_id_bytes),
                    partitions: vec![
                        ReqPartData {
                            partition_index: 0,
                            ..Default::default()
                        },
                        ReqPartData {
                            partition_index: 1,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let resp = build_echo_response(&req);

        // Mirrors the request directory/topic/partition structure with every
        // error_code filled with NONE (0).
        let expected = AssignReplicasToDirsResponse {
            throttle_time_ms: 0,
            error_code: 0,
            directories: vec![RespDirData {
                id: ProtocolUuid(dir_id_bytes),
                topics: vec![RespTopicData {
                    topic_id: ProtocolUuid(topic_id_bytes),
                    partitions: vec![
                        RespPartData {
                            partition_index: 0,
                            error_code: 0,
                            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                        },
                        RespPartData {
                            partition_index: 1,
                            error_code: 0,
                            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                        },
                    ],
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
                }],
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(vec![]),
        };
        assert!(resp == expected, "{resp:?}");
    }
}
