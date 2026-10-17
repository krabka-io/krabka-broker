use {
    super::{TOPIC_ID, case, check, default_message},
    crate::codes::ILLEGAL_SASL_STATE,
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
            offset_commit_request::{
                OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
            },
            offset_commit_response::{
                OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
            },
            offset_delete_request::OffsetDeleteRequest,
            offset_delete_response::OffsetDeleteResponse,
            offset_fetch_request::{
                OffsetFetchRequest, OffsetFetchRequestGroup, OffsetFetchRequestTopic,
            },
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

/// An `OffsetFetch` request that names `orders` partition 1 at the top level
/// and groups `g1` and `g2` in the batch.
fn offset_fetch_request() -> OffsetFetchRequest {
    OffsetFetchRequest {
        group_id: "g1".to_owned(),
        topics: Some(vec![OffsetFetchRequestTopic {
            name: "orders".to_owned(),
            partition_indexes: vec![1],
            ..Default::default()
        }]),
        groups: ["g1", "g2"]
            .map(|group_id| OffsetFetchRequestGroup {
                group_id: group_id.to_owned(),
                ..Default::default()
            })
            .into(),
        ..Default::default()
    }
}

/// `OffsetFetch` puts the code on each partition before v2, at the top level
/// from v2, and on each group from v8.
#[test]
fn offset_fetch_answers_by_version() {
    let group = |group_id: &str| OffsetFetchResponseGroup {
        group_id: group_id.to_owned(),
        error_code: ILLEGAL_SASL_STATE,
        ..Default::default()
    };
    check(vec![
        case(
            ApiKey::OffsetFetch,
            1,
            offset_fetch_request(),
            Some(OffsetFetchResponse {
                topics: vec![OffsetFetchResponseTopic {
                    name: "orders".to_owned(),
                    partitions: vec![OffsetFetchResponsePartition {
                        partition_index: 1,
                        committed_offset: -1,
                        committed_leader_epoch: -1,
                        metadata: Some(String::new()),
                        error_code: ILLEGAL_SASL_STATE,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::OffsetFetch,
            2,
            offset_fetch_request(),
            Some(OffsetFetchResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::OffsetFetch,
            7,
            offset_fetch_request(),
            Some(OffsetFetchResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::OffsetFetch,
            8,
            offset_fetch_request(),
            Some(OffsetFetchResponse {
                groups: vec![group("g1"), group("g2")],
                ..Default::default()
            }),
        ),
    ]);
}

/// `FindCoordinator` answers one coordinator-less node before v4, and one row
/// per key from v4, each with the code's default message.
#[test]
fn find_coordinator_answers_by_version() {
    let request = FindCoordinatorRequest {
        key: "g1".to_owned(),
        coordinator_keys: vec!["g1".to_owned(), "g2".to_owned()],
        ..Default::default()
    };
    let coordinator = |key: &str| Coordinator {
        key: key.to_owned(),
        node_id: -1,
        host: String::new(),
        port: -1,
        error_code: ILLEGAL_SASL_STATE,
        error_message: Some(default_message()),
        ..Default::default()
    };
    check(vec![
        case(
            ApiKey::FindCoordinator,
            3,
            request.clone(),
            Some(FindCoordinatorResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(default_message()),
                node_id: -1,
                host: String::new(),
                port: -1,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::FindCoordinator,
            4,
            request,
            Some(FindCoordinatorResponse {
                coordinators: vec![coordinator("g1"), coordinator("g2")],
                ..Default::default()
            }),
        ),
    ]);
}

/// `JoinGroup`'s protocol name is empty before v7 and null from v7.
#[test]
fn join_group_protocol_name_is_null_from_v7() {
    let response = |protocol_name| JoinGroupResponse {
        error_code: ILLEGAL_SASL_STATE,
        generation_id: -1,
        protocol_name,
        leader: String::new(),
        member_id: String::new(),
        ..Default::default()
    };
    let request = JoinGroupRequest {
        group_id: "g1".to_owned(),
        member_id: "m1".to_owned(),
        ..Default::default()
    };
    check(vec![
        case(
            ApiKey::JoinGroup,
            6,
            request.clone(),
            Some(response(Some(String::new()))),
        ),
        case(ApiKey::JoinGroup, 7, request, Some(response(None))),
    ]);
}

/// The classic group apis.
#[test]
fn classic_group_apis() {
    check(vec![
        case(
            ApiKey::OffsetCommit,
            10,
            OffsetCommitRequest {
                group_id: "g1".to_owned(),
                topics: vec![OffsetCommitRequestTopic {
                    topic_id: TOPIC_ID,
                    partitions: vec![OffsetCommitRequestPartition {
                        partition_index: 2,
                        committed_offset: 10,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(OffsetCommitResponse {
                topics: vec![OffsetCommitResponseTopic {
                    topic_id: TOPIC_ID,
                    partitions: vec![OffsetCommitResponsePartition {
                        partition_index: 2,
                        error_code: ILLEGAL_SASL_STATE,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::Heartbeat,
            4,
            HeartbeatRequest::default(),
            Some(HeartbeatResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::LeaveGroup,
            5,
            LeaveGroupRequest::default(),
            Some(LeaveGroupResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::SyncGroup,
            5,
            SyncGroupRequest::default(),
            Some(SyncGroupResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DescribeGroups,
            5,
            DescribeGroupsRequest {
                groups: vec!["g1".to_owned()],
                ..Default::default()
            },
            Some(DescribeGroupsResponse {
                groups: vec![DescribedGroup {
                    error_code: ILLEGAL_SASL_STATE,
                    group_id: "g1".to_owned(),
                    group_state: String::new(),
                    protocol_type: String::new(),
                    protocol_data: String::new(),
                    members: Vec::new(),
                    authorized_operations: i32::MIN,
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ListGroups,
            5,
            ListGroupsRequest::default(),
            Some(ListGroupsResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DeleteGroups,
            2,
            DeleteGroupsRequest {
                groups_names: vec!["g1".to_owned(), "g2".to_owned()],
                ..Default::default()
            },
            Some(DeleteGroupsResponse {
                results: ["g1", "g2"]
                    .map(|group_id| DeletableGroupResult {
                        group_id: group_id.to_owned(),
                        error_code: ILLEGAL_SASL_STATE,
                        ..Default::default()
                    })
                    .into(),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::OffsetDelete,
            0,
            OffsetDeleteRequest::default(),
            Some(OffsetDeleteResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
    ]);
}

/// The KIP-848, KIP-932 and KIP-1071 heartbeat and describe apis.
#[test]
fn next_generation_group_apis() {
    let groups = || vec!["g1".to_owned()];
    check(vec![
        case(
            ApiKey::ConsumerGroupHeartbeat,
            1,
            ConsumerGroupHeartbeatRequest::default(),
            Some(ConsumerGroupHeartbeatResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ConsumerGroupDescribe,
            1,
            ConsumerGroupDescribeRequest {
                group_ids: groups(),
                ..Default::default()
            },
            Some(ConsumerGroupDescribeResponse {
                groups: vec![ConsumerDescribedGroup {
                    group_id: "g1".to_owned(),
                    error_code: ILLEGAL_SASL_STATE,
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ShareGroupHeartbeat,
            1,
            ShareGroupHeartbeatRequest::default(),
            Some(ShareGroupHeartbeatResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ShareGroupDescribe,
            1,
            ShareGroupDescribeRequest {
                group_ids: groups(),
                ..Default::default()
            },
            Some(ShareGroupDescribeResponse {
                groups: vec![ShareDescribedGroup {
                    group_id: "g1".to_owned(),
                    error_code: ILLEGAL_SASL_STATE,
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::StreamsGroupHeartbeat,
            0,
            StreamsGroupHeartbeatRequest::default(),
            Some(StreamsGroupHeartbeatResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::StreamsGroupDescribe,
            0,
            StreamsGroupDescribeRequest {
                group_ids: groups(),
                ..Default::default()
            },
            Some(StreamsGroupDescribeResponse {
                groups: vec![StreamsDescribedGroup {
                    group_id: "g1".to_owned(),
                    error_code: ILLEGAL_SASL_STATE,
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
    ]);
}

/// The KIP-932 offset admin and share fetch apis.
#[test]
fn share_apis() {
    check(vec![
        case(
            ApiKey::AlterShareGroupOffsets,
            0,
            AlterShareGroupOffsetsRequest::default(),
            Some(AlterShareGroupOffsetsResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(default_message()),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DeleteShareGroupOffsets,
            0,
            DeleteShareGroupOffsetsRequest::default(),
            Some(DeleteShareGroupOffsetsResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(default_message()),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ShareFetch,
            1,
            ShareFetchRequest::default(),
            Some(ShareFetchResponse {
                error_code: ILLEGAL_SASL_STATE,
                acquisition_lock_timeout_ms: 0,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ShareAcknowledge,
            1,
            ShareAcknowledgeRequest::default(),
            Some(ShareAcknowledgeResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
    ]);
}
