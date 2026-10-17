//! The classic, consumer, share and streams group apis.

use {
    super::{ErrorAnswer, Request, RequestFailure},
    crate::codes::UNKNOWN_SERVER_ERROR,
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            alter_share_group_offsets_request::AlterShareGroupOffsetsRequest,
            alter_share_group_offsets_response::AlterShareGroupOffsetsResponse,
            consumer_group_describe_request::ConsumerGroupDescribeRequest,
            consumer_group_describe_response::{
                ConsumerGroupDescribeResponse, DescribedGroup as ConsumerDescribedGroup,
            },
            consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
            consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
            delete_groups_request::DeleteGroupsRequest,
            delete_groups_response::{DeletableGroupResult, DeleteGroupsResponse},
            delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest,
            delete_share_group_offsets_response::DeleteShareGroupOffsetsResponse,
            describe_groups_request::DescribeGroupsRequest,
            describe_groups_response::{DescribeGroupsResponse, DescribedGroup},
            describe_share_group_offsets_request::DescribeShareGroupOffsetsRequest,
            describe_share_group_offsets_response::{
                DescribeShareGroupOffsetsResponse, DescribeShareGroupOffsetsResponseGroup,
            },
            find_coordinator_request::FindCoordinatorRequest,
            find_coordinator_response::{Coordinator, FindCoordinatorResponse},
            heartbeat_request::HeartbeatRequest,
            heartbeat_response::HeartbeatResponse,
            join_group_request::JoinGroupRequest,
            join_group_response::JoinGroupResponse,
            leave_group_request::LeaveGroupRequest,
            leave_group_response::LeaveGroupResponse,
            list_groups_request::ListGroupsRequest,
            list_groups_response::ListGroupsResponse,
            offset_commit_request::OffsetCommitRequest,
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
            offset_delete_request::OffsetDeleteRequest,
            offset_delete_response::OffsetDeleteResponse,
            offset_fetch_request::OffsetFetchRequest,
            offset_fetch_response::{
                OffsetFetchResponse, OffsetFetchResponseGroup, OffsetFetchResponsePartition,
                OffsetFetchResponseTopic,
            },
            share_acknowledge_request::ShareAcknowledgeRequest,
            share_acknowledge_response::ShareAcknowledgeResponse,
            share_fetch_request::ShareFetchRequest,
            share_fetch_response::ShareFetchResponse,
            share_group_describe_request::ShareGroupDescribeRequest,
            share_group_describe_response::{
                DescribedGroup as ShareDescribedGroup, ShareGroupDescribeResponse,
            },
            share_group_heartbeat_request::ShareGroupHeartbeatRequest,
            share_group_heartbeat_response::ShareGroupHeartbeatResponse,
            streams_group_describe_request::StreamsGroupDescribeRequest,
            streams_group_describe_response::{
                DescribedGroup as StreamsDescribedGroup, StreamsGroupDescribeResponse,
            },
            streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
            streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
            sync_group_request::SyncGroupRequest,
            sync_group_response::SyncGroupResponse,
        },
    },
};

/// `OffsetFetchRequest.TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION`.
const OFFSET_FETCH_TOP_LEVEL_ERROR_MIN_VERSION: i16 = 2;

/// `OffsetFetchRequest.BATCH_MIN_VERSION`.
const OFFSET_FETCH_BATCH_MIN_VERSION: i16 = 8;

/// `FindCoordinatorRequest.MIN_BATCHED_VERSION`.
const FIND_COORDINATOR_BATCHED_MIN_VERSION: i16 = 4;

/// The first `JoinGroup` version whose protocol name is nullable.
const JOIN_GROUP_NULLABLE_PROTOCOL_NAME_MIN_VERSION: i16 = 7;

/// `Node.noNode()`: id and port -1, empty host.
const NO_NODE_ID: i32 = -1;
const NO_NODE_PORT: i32 = -1;

/// `JoinGroupRequest.UNKNOWN_GENERATION_ID`.
const UNKNOWN_GENERATION_ID: i32 = -1;

/// `OffsetFetchResponse.INVALID_OFFSET`.
const INVALID_OFFSET: i64 = -1;

/// `RecordBatch.NO_PARTITION_LEADER_EPOCH`.
const NO_PARTITION_LEADER_EPOCH: i32 = -1;

/// `DescribeGroupsResponse.AUTHORIZED_OPERATIONS_OMITTED`.
const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

pub(super) fn answer(api: ApiKey, request: Request<'_>) -> Option<ErrorAnswer> {
    let code = request.code();
    let failure = request.failure;
    let version = request.version;
    Some(match api {
        ApiKey::OffsetCommit => request.respond(|req| offset_commit(&req, code)),
        ApiKey::OffsetFetch => request.respond(|req| offset_fetch(&req, version, code)),
        ApiKey::FindCoordinator => request.respond(|req| find_coordinator(&req, version, failure)),
        ApiKey::JoinGroup => request.respond(|_: JoinGroupRequest| join_group(version, code)),
        ApiKey::Heartbeat => request.respond(|_: HeartbeatRequest| HeartbeatResponse {
            error_code: code,
            ..Default::default()
        }),
        ApiKey::LeaveGroup => request.respond(|_: LeaveGroupRequest| LeaveGroupResponse {
            error_code: code,
            ..Default::default()
        }),
        ApiKey::SyncGroup => request.respond(|_: SyncGroupRequest| SyncGroupResponse {
            error_code: code,
            ..Default::default()
        }),
        ApiKey::DescribeGroups => request.respond(|req| describe_groups(&req, code)),
        ApiKey::ListGroups => request.respond(|_: ListGroupsRequest| ListGroupsResponse {
            error_code: code,
            ..Default::default()
        }),
        ApiKey::DeleteGroups => request.respond(|req: DeleteGroupsRequest| DeleteGroupsResponse {
            results: req
                .groups_names
                .into_iter()
                .map(|group_id| DeletableGroupResult {
                    group_id,
                    error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }),
        ApiKey::OffsetDelete => request.respond(|_: OffsetDeleteRequest| OffsetDeleteResponse {
            error_code: code,
            ..Default::default()
        }),
        _ => return heartbeat_or_describe(api, request),
    })
}

/// The KIP-848, KIP-932 and KIP-1071 group apis.
fn heartbeat_or_describe(api: ApiKey, request: Request<'_>) -> Option<ErrorAnswer> {
    let code = request.code();
    let failure = request.failure;
    Some(match api {
        ApiKey::ConsumerGroupHeartbeat => {
            request.respond(
                |_: ConsumerGroupHeartbeatRequest| ConsumerGroupHeartbeatResponse {
                    error_code: code,
                    ..Default::default()
                },
            )
        }
        ApiKey::ConsumerGroupDescribe => {
            request.respond(
                |req: ConsumerGroupDescribeRequest| ConsumerGroupDescribeResponse {
                    groups: req
                        .group_ids
                        .into_iter()
                        .map(|group_id| ConsumerDescribedGroup {
                            group_id,
                            error_code: code,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
            )
        }
        ApiKey::ShareGroupHeartbeat => {
            request.respond(
                |_: ShareGroupHeartbeatRequest| ShareGroupHeartbeatResponse {
                    error_code: code,
                    ..Default::default()
                },
            )
        }
        ApiKey::ShareGroupDescribe => {
            request.respond(
                |req: ShareGroupDescribeRequest| ShareGroupDescribeResponse {
                    groups: req
                        .group_ids
                        .into_iter()
                        .map(|group_id| ShareDescribedGroup {
                            group_id,
                            error_code: code,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
            )
        }
        ApiKey::StreamsGroupHeartbeat => {
            request.respond(
                |_: StreamsGroupHeartbeatRequest| StreamsGroupHeartbeatResponse {
                    error_code: code,
                    ..Default::default()
                },
            )
        }
        ApiKey::StreamsGroupDescribe => {
            request.respond(
                |req: StreamsGroupDescribeRequest| StreamsGroupDescribeResponse {
                    groups: req
                        .group_ids
                        .into_iter()
                        .map(|group_id| StreamsDescribedGroup {
                            group_id,
                            error_code: code,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                },
            )
        }
        _ => return share_offsets_or_fetch(api, request, failure),
    })
}

/// The KIP-932 share-group offset admin apis and the share fetch apis.
fn share_offsets_or_fetch(
    api: ApiKey,
    request: Request<'_>,
    failure: &RequestFailure<'_>,
) -> Option<ErrorAnswer> {
    let code = request.code();
    Some(match api {
        ApiKey::DescribeShareGroupOffsets => {
            request.respond(|req| describe_share_group_offsets(&req, failure))
        }
        ApiKey::AlterShareGroupOffsets => {
            request.respond(
                |_: AlterShareGroupOffsetsRequest| AlterShareGroupOffsetsResponse {
                    error_code: code,
                    error_message: Some(failure.error_message()),
                    ..Default::default()
                },
            )
        }
        ApiKey::DeleteShareGroupOffsets => {
            request.respond(
                |_: DeleteShareGroupOffsetsRequest| DeleteShareGroupOffsetsResponse {
                    error_code: code,
                    error_message: Some(failure.error_message()),
                    ..Default::default()
                },
            )
        }
        // `ShareFetchResponse.of` with no partitions and no node endpoints,
        // and an acquisition lock timeout of 0.
        ApiKey::ShareFetch => request.respond(|_: ShareFetchRequest| ShareFetchResponse {
            error_code: code,
            acquisition_lock_timeout_ms: 0,
            ..Default::default()
        }),
        ApiKey::ShareAcknowledge => {
            request.respond(|_: ShareAcknowledgeRequest| ShareAcknowledgeResponse {
                error_code: code,
                ..Default::default()
            })
        }
        _ => return None,
    })
}

/// `OffsetCommitRequest.getErrorResponse`: every requested partition with the
/// code, under its topic's name and id.
fn offset_commit(req: &OffsetCommitRequest, code: i16) -> OffsetCommitResponse {
    OffsetCommitResponse {
        topics: req
            .topics
            .iter()
            .map(|topic| OffsetCommitResponseTopic {
                name: topic.name.clone(),
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|partition| OffsetCommitResponsePartition {
                        partition_index: partition.partition_index,
                        error_code: code,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `OffsetFetchRequest.getErrorResponse`: before v2 every requested partition
/// carries the code, from v2 the top level does, and from v8 every requested
/// group does.
fn offset_fetch(req: &OffsetFetchRequest, version: i16, code: i16) -> OffsetFetchResponse {
    if version < OFFSET_FETCH_TOP_LEVEL_ERROR_MIN_VERSION {
        OffsetFetchResponse {
            topics: req
                .topics
                .iter()
                .flatten()
                .map(|topic| OffsetFetchResponseTopic {
                    name: topic.name.clone(),
                    partitions: topic
                        .partition_indexes
                        .iter()
                        .map(|&partition_index| OffsetFetchResponsePartition {
                            partition_index,
                            error_code: code,
                            committed_offset: INVALID_OFFSET,
                            metadata: Some(String::new()),
                            committed_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    } else if version < OFFSET_FETCH_BATCH_MIN_VERSION {
        OffsetFetchResponse {
            error_code: code,
            ..Default::default()
        }
    } else {
        OffsetFetchResponse {
            groups: req
                .groups
                .iter()
                .map(|group| OffsetFetchResponseGroup {
                    group_id: group.group_id.clone(),
                    error_code: code,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}

/// `FindCoordinatorRequest.getErrorResponse`: before v4 one coordinator-less
/// answer, from v4 one per requested key. Both carry the code's default
/// message.
fn find_coordinator(
    req: &FindCoordinatorRequest,
    version: i16,
    failure: &RequestFailure<'_>,
) -> FindCoordinatorResponse {
    if version < FIND_COORDINATOR_BATCHED_MIN_VERSION {
        FindCoordinatorResponse {
            error_code: failure.error_code,
            error_message: Some(failure.error_message()),
            node_id: NO_NODE_ID,
            host: String::new(),
            port: NO_NODE_PORT,
            ..Default::default()
        }
    } else {
        FindCoordinatorResponse {
            coordinators: req
                .coordinator_keys
                .iter()
                .map(|key| Coordinator {
                    key: key.clone(),
                    node_id: NO_NODE_ID,
                    host: String::new(),
                    port: NO_NODE_PORT,
                    error_code: failure.error_code,
                    error_message: Some(failure.error_message()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }
}

/// `JoinGroupRequest.getErrorResponse`: the code, an unknown generation,
/// leader and member, and a protocol name that is null from v7 and empty
/// before.
fn join_group(version: i16, code: i16) -> JoinGroupResponse {
    JoinGroupResponse {
        error_code: code,
        generation_id: UNKNOWN_GENERATION_ID,
        protocol_name: (version < JOIN_GROUP_NULLABLE_PROTOCOL_NAME_MIN_VERSION).then(String::new),
        leader: String::new(),
        member_id: String::new(),
        members: Vec::new(),
        ..Default::default()
    }
}

/// `DescribeGroupsRequest.getErrorResponse`: every requested group as
/// `DescribeGroupsResponse.groupError` builds it.
fn describe_groups(req: &DescribeGroupsRequest, code: i16) -> DescribeGroupsResponse {
    DescribeGroupsResponse {
        groups: req
            .groups
            .iter()
            .map(|group_id| DescribedGroup {
                error_code: code,
                group_id: group_id.clone(),
                group_state: String::new(),
                protocol_type: String::new(),
                protocol_data: String::new(),
                members: Vec::new(),
                authorized_operations: AUTHORIZED_OPERATIONS_OMITTED,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// `DescribeShareGroupOffsetsResponse(throttleTimeMs, groupIds, e)`: every
/// requested group with the code and the exception's message, or the code's
/// default message for `UNKNOWN_SERVER_ERROR`.
fn describe_share_group_offsets(
    req: &DescribeShareGroupOffsetsRequest,
    failure: &RequestFailure<'_>,
) -> DescribeShareGroupOffsetsResponse {
    let error_message = Some(if failure.error_code == UNKNOWN_SERVER_ERROR {
        failure.error_message()
    } else {
        failure.exception_message()
    });
    DescribeShareGroupOffsetsResponse {
        groups: req
            .groups
            .iter()
            .map(|group| DescribeShareGroupOffsetsResponseGroup {
                group_id: group.group_id.clone(),
                error_code: failure.error_code,
                error_message: error_message.clone(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}
