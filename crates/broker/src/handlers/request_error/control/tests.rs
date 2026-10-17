use {
    super::{
        super::tests::{TOPIC_ID, case, check, default_message},
        *,
    },
    crate::codes::ILLEGAL_SASL_STATE,
    krabka_protocol::owned::{
        delete_share_group_state_request::{DeleteStateData, PartitionData as DeletePartition},
        initialize_share_group_state_request::{
            InitializeStateData, PartitionData as InitializePartition,
        },
        read_share_group_state_request::{PartitionData as ReadPartition, ReadStateData},
        read_share_group_state_summary_request::{
            PartitionData as ReadSummaryPartition, ReadStateSummaryData,
        },
        write_share_group_state_request::{PartitionData as WritePartition, WriteStateData},
    },
};

/// `$outer` with one `$topic` row under [`TOPIC_ID`] in its `$list`, and in
/// that row one `$partition` row with the fields given. Every other field is
/// defaulted.
macro_rules! one_partition {
    ($outer:ident . $list:ident, $topic:ident, $partition:ident { $($field:ident: $value:expr),* $(,)? }) => {
        $outer {
            $list: vec![$topic {
                topic_id: TOPIC_ID,
                partitions: vec![$partition {
                    $($field: $value,)*
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    };
}

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
            one_partition!(
                InitializeShareGroupStateRequest.topics,
                InitializeStateData,
                InitializePartition { partition: 1 }
            ),
            Some(one_partition!(
                InitializeShareGroupStateResponse.results,
                InitializeStateResult,
                InitializePartitionResult {
                    partition: 1,
                    error_code: ILLEGAL_SASL_STATE,
                }
            )),
        ),
        case(
            ApiKey::DeleteShareGroupState,
            0,
            one_partition!(
                DeleteShareGroupStateRequest.topics,
                DeleteStateData,
                DeletePartition { partition: 2 }
            ),
            Some(one_partition!(
                DeleteShareGroupStateResponse.results,
                DeleteStateResult,
                DeletePartitionResult {
                    partition: 2,
                    error_code: ILLEGAL_SASL_STATE,
                }
            )),
        ),
        case(
            ApiKey::ReadShareGroupState,
            0,
            one_partition!(
                ReadShareGroupStateRequest.topics,
                ReadStateData,
                ReadPartition { partition: 3 }
            ),
            Some(one_partition!(
                ReadShareGroupStateResponse.results,
                ReadStateResult,
                ReadPartitionResult {
                    partition: 3,
                    error_code: ILLEGAL_SASL_STATE,
                    error_message: Some(default_message()),
                }
            )),
        ),
        case(
            ApiKey::WriteShareGroupState,
            1,
            one_partition!(
                WriteShareGroupStateRequest.topics,
                WriteStateData,
                WritePartition { partition: 4 }
            ),
            Some(one_partition!(
                WriteShareGroupStateResponse.results,
                WriteStateResult,
                WritePartitionResult {
                    partition: 4,
                    error_code: ILLEGAL_SASL_STATE,
                    error_message: Some(default_message()),
                }
            )),
        ),
        case(
            ApiKey::ReadShareGroupStateSummary,
            1,
            one_partition!(
                ReadShareGroupStateSummaryRequest.topics,
                ReadStateSummaryData,
                ReadSummaryPartition { partition: 5 }
            ),
            Some(one_partition!(
                ReadShareGroupStateSummaryResponse.results,
                ReadStateSummaryResult,
                ReadSummaryPartitionResult {
                    partition: 5,
                    error_code: ILLEGAL_SASL_STATE,
                    error_message: Some(default_message()),
                }
            )),
        ),
    ]);
}
