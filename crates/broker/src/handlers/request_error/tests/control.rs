use {
    super::{TOPIC_ID, case, check, default_message},
    crate::codes::ILLEGAL_SASL_STATE,
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            add_raft_voter_request::AddRaftVoterRequest,
            add_raft_voter_response::AddRaftVoterResponse,
            allocate_producer_ids_request::AllocateProducerIdsRequest,
            allocate_producer_ids_response::AllocateProducerIdsResponse,
            alter_partition_request::AlterPartitionRequest,
            alter_partition_response::AlterPartitionResponse,
            assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
            assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
            begin_quorum_epoch_request::BeginQuorumEpochRequest,
            begin_quorum_epoch_response::BeginQuorumEpochResponse,
            broker_heartbeat_request::BrokerHeartbeatRequest,
            broker_heartbeat_response::BrokerHeartbeatResponse,
            broker_registration_request::BrokerRegistrationRequest,
            broker_registration_response::BrokerRegistrationResponse,
            controller_registration_request::ControllerRegistrationRequest,
            controller_registration_response::ControllerRegistrationResponse,
            delete_share_group_state_request::{
                DeleteShareGroupStateRequest, DeleteStateData, PartitionData as DeletePartition,
            },
            delete_share_group_state_response::{
                DeleteShareGroupStateResponse, DeleteStateResult,
                PartitionResult as DeletePartitionResult,
            },
            describe_quorum_request::DescribeQuorumRequest,
            describe_quorum_response::DescribeQuorumResponse,
            end_quorum_epoch_request::EndQuorumEpochRequest,
            end_quorum_epoch_response::EndQuorumEpochResponse,
            envelope_request::EnvelopeRequest,
            envelope_response::EnvelopeResponse,
            fetch_snapshot_request::FetchSnapshotRequest,
            fetch_snapshot_response::FetchSnapshotResponse,
            initialize_share_group_state_request::{
                InitializeShareGroupStateRequest, InitializeStateData,
                PartitionData as InitializePartition,
            },
            initialize_share_group_state_response::{
                InitializeShareGroupStateResponse, InitializeStateResult,
                PartitionResult as InitializePartitionResult,
            },
            read_share_group_state_request::{
                PartitionData as ReadPartition, ReadShareGroupStateRequest, ReadStateData,
            },
            read_share_group_state_response::{
                PartitionResult as ReadPartitionResult, ReadShareGroupStateResponse,
                ReadStateResult,
            },
            read_share_group_state_summary_request::{
                PartitionData as ReadSummaryPartition, ReadShareGroupStateSummaryRequest,
                ReadStateSummaryData,
            },
            read_share_group_state_summary_response::{
                PartitionResult as ReadSummaryPartitionResult, ReadShareGroupStateSummaryResponse,
                ReadStateSummaryResult,
            },
            remove_raft_voter_request::RemoveRaftVoterRequest,
            remove_raft_voter_response::RemoveRaftVoterResponse,
            update_raft_voter_request::UpdateRaftVoterRequest,
            update_raft_voter_response::UpdateRaftVoterResponse,
            vote_request::VoteRequest,
            vote_response::VoteResponse,
            write_share_group_state_request::{
                PartitionData as WritePartition, WriteShareGroupStateRequest, WriteStateData,
            },
            write_share_group_state_response::{
                PartitionResult as WritePartitionResult, WriteShareGroupStateResponse,
                WriteStateResult,
            },
        },
    },
};

/// The raft apis.
#[test]
fn raft_apis() {
    check(vec![
        case(
            ApiKey::Vote,
            2,
            VoteRequest::default(),
            Some(VoteResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::BeginQuorumEpoch,
            1,
            BeginQuorumEpochRequest::default(),
            Some(BeginQuorumEpochResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::EndQuorumEpoch,
            1,
            EndQuorumEpochRequest::default(),
            Some(EndQuorumEpochResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DescribeQuorum,
            2,
            DescribeQuorumRequest::default(),
            Some(DescribeQuorumResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(default_message()),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::AddRaftVoter,
            1,
            AddRaftVoterRequest::default(),
            Some(AddRaftVoterResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(default_message()),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::RemoveRaftVoter,
            0,
            RemoveRaftVoterRequest::default(),
            Some(RemoveRaftVoterResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(default_message()),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::UpdateRaftVoter,
            0,
            UpdateRaftVoterRequest::default(),
            Some(UpdateRaftVoterResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::FetchSnapshot,
            1,
            FetchSnapshotRequest::default(),
            Some(FetchSnapshotResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::Envelope,
            0,
            EnvelopeRequest::default(),
            Some(EnvelopeResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
    ]);
}

/// The apis a broker sends its controller.
#[test]
fn controller_apis() {
    check(vec![
        case(
            ApiKey::AlterPartition,
            3,
            AlterPartitionRequest::default(),
            Some(AlterPartitionResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::BrokerRegistration,
            4,
            BrokerRegistrationRequest::default(),
            Some(BrokerRegistrationResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::BrokerHeartbeat,
            2,
            BrokerHeartbeatRequest::default(),
            Some(BrokerHeartbeatResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::AllocateProducerIds,
            0,
            AllocateProducerIdsRequest::default(),
            Some(AllocateProducerIdsResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ControllerRegistration,
            0,
            ControllerRegistrationRequest::default(),
            Some(ControllerRegistrationResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(default_message()),
                ..Default::default()
            }),
        ),
        case(
            ApiKey::AssignReplicasToDirs,
            0,
            AssignReplicasToDirsRequest::default(),
            Some(AssignReplicasToDirsResponse {
                error_code: ILLEGAL_SASL_STATE,
                ..Default::default()
            }),
        ),
    ]);
}

/// The share-state apis answer each requested partition under its topic id,
/// the read and write apis with the code's default message.
#[test]
fn share_state_apis_repeat_every_requested_partition() {
    check(vec![
        case(
            ApiKey::InitializeShareGroupState,
            0,
            InitializeShareGroupStateRequest {
                topics: vec![InitializeStateData {
                    topic_id: TOPIC_ID,
                    partitions: vec![InitializePartition {
                        partition: 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(InitializeShareGroupStateResponse {
                results: vec![InitializeStateResult {
                    topic_id: TOPIC_ID,
                    partitions: vec![InitializePartitionResult {
                        partition: 1,
                        error_code: ILLEGAL_SASL_STATE,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::DeleteShareGroupState,
            0,
            DeleteShareGroupStateRequest {
                topics: vec![DeleteStateData {
                    topic_id: TOPIC_ID,
                    partitions: vec![DeletePartition {
                        partition: 2,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(DeleteShareGroupStateResponse {
                results: vec![DeleteStateResult {
                    topic_id: TOPIC_ID,
                    partitions: vec![DeletePartitionResult {
                        partition: 2,
                        error_code: ILLEGAL_SASL_STATE,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ReadShareGroupState,
            0,
            ReadShareGroupStateRequest {
                topics: vec![ReadStateData {
                    topic_id: TOPIC_ID,
                    partitions: vec![ReadPartition {
                        partition: 3,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(ReadShareGroupStateResponse {
                results: vec![ReadStateResult {
                    topic_id: TOPIC_ID,
                    partitions: vec![ReadPartitionResult {
                        partition: 3,
                        error_code: ILLEGAL_SASL_STATE,
                        error_message: Some(default_message()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::WriteShareGroupState,
            1,
            WriteShareGroupStateRequest {
                topics: vec![WriteStateData {
                    topic_id: TOPIC_ID,
                    partitions: vec![WritePartition {
                        partition: 4,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(WriteShareGroupStateResponse {
                results: vec![WriteStateResult {
                    topic_id: TOPIC_ID,
                    partitions: vec![WritePartitionResult {
                        partition: 4,
                        error_code: ILLEGAL_SASL_STATE,
                        error_message: Some(default_message()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
        case(
            ApiKey::ReadShareGroupStateSummary,
            1,
            ReadShareGroupStateSummaryRequest {
                topics: vec![ReadStateSummaryData {
                    topic_id: TOPIC_ID,
                    partitions: vec![ReadSummaryPartition {
                        partition: 5,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            Some(ReadShareGroupStateSummaryResponse {
                results: vec![ReadStateSummaryResult {
                    topic_id: TOPIC_ID,
                    partitions: vec![ReadSummaryPartitionResult {
                        partition: 5,
                        error_code: ILLEGAL_SASL_STATE,
                        error_message: Some(default_message()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ),
    ]);
}
