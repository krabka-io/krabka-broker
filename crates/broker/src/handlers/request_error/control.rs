//! The raft, controller, broker-lifecycle and share-state apis.

use {
    super::{ErrorAnswer, Request, RequestFailure},
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
            delete_share_group_state_request::DeleteShareGroupStateRequest,
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
            initialize_share_group_state_request::InitializeShareGroupStateRequest,
            initialize_share_group_state_response::{
                InitializeShareGroupStateResponse, InitializeStateResult,
                PartitionResult as InitializePartitionResult,
            },
            read_share_group_state_request::ReadShareGroupStateRequest,
            read_share_group_state_response::{
                PartitionResult as ReadPartitionResult, ReadShareGroupStateResponse,
                ReadStateResult,
            },
            read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest,
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
            write_share_group_state_request::WriteShareGroupStateRequest,
            write_share_group_state_response::{
                PartitionResult as WritePartitionResult, WriteShareGroupStateResponse,
                WriteStateResult,
            },
        },
    },
};

pub(super) fn answer(api: ApiKey, request: Request<'_>) -> Option<ErrorAnswer> {
    let code = request.code();
    let failure = request.failure;
    Some(match api {
        ApiKey::Vote => request.respond(|_: VoteRequest| VoteResponse {
            error_code: code,
            ..Default::default()
        }),
        ApiKey::BeginQuorumEpoch => {
            request.respond(|_: BeginQuorumEpochRequest| BeginQuorumEpochResponse {
                error_code: code,
                ..Default::default()
            })
        }
        ApiKey::EndQuorumEpoch => {
            request.respond(|_: EndQuorumEpochRequest| EndQuorumEpochResponse {
                error_code: code,
                ..Default::default()
            })
        }
        // `DescribeQuorumRequest.getTopLevelErrorResponse`.
        ApiKey::DescribeQuorum => {
            request.respond(|_: DescribeQuorumRequest| DescribeQuorumResponse {
                error_code: code,
                error_message: Some(failure.error_message()),
                ..Default::default()
            })
        }
        ApiKey::AddRaftVoter => request.respond(|_: AddRaftVoterRequest| AddRaftVoterResponse {
            error_code: code,
            error_message: Some(failure.error_message()),
            ..Default::default()
        }),
        ApiKey::RemoveRaftVoter => {
            request.respond(|_: RemoveRaftVoterRequest| RemoveRaftVoterResponse {
                error_code: code,
                error_message: Some(failure.error_message()),
                ..Default::default()
            })
        }
        ApiKey::UpdateRaftVoter => {
            request.respond(|_: UpdateRaftVoterRequest| UpdateRaftVoterResponse {
                error_code: code,
                ..Default::default()
            })
        }
        ApiKey::FetchSnapshot => request.respond(|_: FetchSnapshotRequest| FetchSnapshotResponse {
            error_code: code,
            ..Default::default()
        }),
        ApiKey::Envelope => request.respond(|_: EnvelopeRequest| EnvelopeResponse {
            error_code: code,
            ..Default::default()
        }),
        _ => return controller(api, request),
    })
}

/// The apis that a broker sends its controller.
fn controller(api: ApiKey, request: Request<'_>) -> Option<ErrorAnswer> {
    let code = request.code();
    let failure = request.failure;
    Some(match api {
        ApiKey::AlterPartition => {
            request.respond(|_: AlterPartitionRequest| AlterPartitionResponse {
                error_code: code,
                ..Default::default()
            })
        }
        ApiKey::BrokerRegistration => {
            request.respond(|_: BrokerRegistrationRequest| BrokerRegistrationResponse {
                error_code: code,
                ..Default::default()
            })
        }
        ApiKey::BrokerHeartbeat => {
            request.respond(|_: BrokerHeartbeatRequest| BrokerHeartbeatResponse {
                error_code: code,
                ..Default::default()
            })
        }
        ApiKey::AllocateProducerIds => {
            request.respond(
                |_: AllocateProducerIdsRequest| AllocateProducerIdsResponse {
                    error_code: code,
                    ..Default::default()
                },
            )
        }
        ApiKey::ControllerRegistration => {
            request.respond(
                |_: ControllerRegistrationRequest| ControllerRegistrationResponse {
                    error_code: code,
                    error_message: Some(failure.error_message()),
                    ..Default::default()
                },
            )
        }
        ApiKey::AssignReplicasToDirs => {
            request.respond(
                |_: AssignReplicasToDirsRequest| AssignReplicasToDirsResponse {
                    error_code: code,
                    ..Default::default()
                },
            )
        }
        _ => return share_state(api, request, failure),
    })
}

/// The KIP-932 share-state apis: every requested partition with the code,
/// under its topic id, and for the read and write apis the code's default
/// message.
fn share_state(
    api: ApiKey,
    request: Request<'_>,
    failure: &RequestFailure<'_>,
) -> Option<ErrorAnswer> {
    let code = failure.error_code;
    Some(match api {
        ApiKey::InitializeShareGroupState => {
            request.respond(|req: InitializeShareGroupStateRequest| {
                InitializeShareGroupStateResponse {
                    results: rows!(&req.topics, |topic| InitializeStateResult {
                        topic_id: topic.topic_id,
                        partitions: rows!(&topic.partitions, |partition| {
                            InitializePartitionResult {
                                partition: partition.partition,
                                error_code: code,
                            }
                        }),
                    }),
                    ..Default::default()
                }
            })
        }
        ApiKey::DeleteShareGroupState => {
            request.respond(
                |req: DeleteShareGroupStateRequest| DeleteShareGroupStateResponse {
                    results: rows!(&req.topics, |topic| DeleteStateResult {
                        topic_id: topic.topic_id,
                        partitions: rows!(&topic.partitions, |partition| DeletePartitionResult {
                            partition: partition.partition,
                            error_code: code,
                        }),
                    }),
                    ..Default::default()
                },
            )
        }
        _ => return share_state_with_message(api, request, failure),
    })
}

/// The share-state apis whose rows also carry the code's default message.
fn share_state_with_message(
    api: ApiKey,
    request: Request<'_>,
    failure: &RequestFailure<'_>,
) -> Option<ErrorAnswer> {
    let code = failure.error_code;
    Some(match api {
        ApiKey::ReadShareGroupState => {
            request.respond(
                |req: ReadShareGroupStateRequest| ReadShareGroupStateResponse {
                    results: rows!(&req.topics, |topic| ReadStateResult {
                        topic_id: topic.topic_id,
                        partitions: rows!(&topic.partitions, |partition| ReadPartitionResult {
                            partition: partition.partition,
                            error_code: code,
                            error_message: Some(failure.error_message()),
                        }),
                    }),
                    ..Default::default()
                },
            )
        }
        ApiKey::WriteShareGroupState => {
            request.respond(
                |req: WriteShareGroupStateRequest| WriteShareGroupStateResponse {
                    results: rows!(&req.topics, |topic| WriteStateResult {
                        topic_id: topic.topic_id,
                        partitions: rows!(&topic.partitions, |partition| WritePartitionResult {
                            partition: partition.partition,
                            error_code: code,
                            error_message: Some(failure.error_message()),
                        }),
                    }),
                    ..Default::default()
                },
            )
        }
        ApiKey::ReadShareGroupStateSummary => {
            request.respond(|req: ReadShareGroupStateSummaryRequest| {
                ReadShareGroupStateSummaryResponse {
                    results: rows!(&req.topics, |topic| ReadStateSummaryResult {
                        topic_id: topic.topic_id,
                        partitions: rows!(&topic.partitions, |partition| {
                            ReadSummaryPartitionResult {
                                partition: partition.partition,
                                error_code: code,
                                error_message: Some(failure.error_message()),
                            }
                        }),
                    }),
                    ..Default::default()
                }
            })
        }
        _ => return None,
    })
}

#[cfg(test)]
mod tests;
