//! The `NOT_CONTROLLER` answers of an election, each with the message of
//! Kafka's `ControllerExceptions.newWrongControllerException`.
//!
//! Kafka's `ControllerApis.handleElectLeaders` answers a failed election with
//! `ElectLeadersRequest.getErrorResponse(throttle, e)`, which puts the code of
//! `ApiError.fromThrowable(e)` at the top level and its code and message on
//! every named partition row. `ApiError.fromThrowable` keeps the message of
//! the controller's exception, which names `QuorumController.latestController()`,
//! the raft leader the refusing node knows of.

use {
    super::{admission_failure, election_outcome, request_refusal},
    crate::{
        codes,
        handlers::{
            forward_to_controller::not_active_controller_refusal,
            submit_failure::test_support::guarded_failure,
        },
    },
    krabka_protocol::owned::{
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        elect_leaders_response::{ElectLeadersResponse, PartitionResult, ReplicaElectionResult},
    },
    krabka_raft::{NodeId, RaftError},
};

/// An election of `orders` partitions 0 and 1.
fn request() -> ElectLeadersRequest {
    ElectLeadersRequest {
        topic_partitions: Some(vec![TopicPartitions {
            topic: "orders".to_owned(),
            partitions: vec![0, 1],
            ..Default::default()
        }]),
        timeout_ms: 5_000,
        ..Default::default()
    }
}

/// The response `ElectLeadersRequest.getErrorResponse` builds for [`request`]
/// refused with `error_code` and `message`.
fn refused(error_code: i16, message: &str) -> ElectLeadersResponse {
    let row = |partition_id| PartitionResult {
        partition_id,
        error_code,
        error_message: Some(message.to_owned()),
        ..Default::default()
    };
    ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code,
        replica_election_results: vec![ReplicaElectionResult {
            topic: "orders".to_owned(),
            partition_result: vec![row(0), row(1)],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// An election that reaches a node that is not the active controller, on
/// the controller listener or after forwarding, answers as
/// `QuorumController.ControllerWriteEvent.run` fails it: with
/// `newWrongControllerException(latestController())`, which names the leader
/// this node knows of.
#[yare::parameterized(
    known_leader = {
        Some(NodeId(3)),
        refused(codes::NOT_CONTROLLER, "The active controller appears to be node 3."),
    },
    no_known_leader = {
        None,
        refused(codes::NOT_CONTROLLER, "No controller appears to be active."),
    },
)]
fn a_node_that_is_not_the_active_controller_names_the_leader_it_knows(
    known_leader: Option<NodeId>,
    expected: ElectLeadersResponse,
) {
    let response = not_active_controller_refusal(&request(), known_leader, request_refusal);
    assert2::assert!(response == expected);
}

/// A guarded election that lost its leadership answers as Kafka's
/// `EventHandlerExceptionInfo.fromInternal` turns a raft `NotLeaderException`
/// into `newWrongControllerException`: it names the leader the refusal
/// redirected to. The guarded operation retries an uncommitted tail until the
/// deadline, so that row answers `REQUEST_TIMED_OUT` and never names a leader.
#[yare::parameterized(
    not_leader = {
        || RaftError::NotLeader { current_leader: Some(NodeId(2)) },
        refused(codes::NOT_CONTROLLER, "The active controller appears to be node 2."),
    },
    not_leader_without_hint = {
        || RaftError::NotLeader { current_leader: None },
        refused(codes::NOT_CONTROLLER, "No controller appears to be active."),
    },
    leader_unknown = {
        || RaftError::LeaderUnknown,
        refused(codes::NOT_CONTROLLER, "No controller appears to be active."),
    },
    uncommitted_tail = {
        || RaftError::UncommittedTail,
        refused(codes::REQUEST_TIMED_OUT, ""),
    },
)]
#[test_macro(tokio::test(start_paused = true))]
async fn a_guarded_election_that_lost_its_leadership_names_the_new_leader(
    error: fn() -> RaftError,
    expected: ElectLeadersResponse,
) {
    let response = election_outcome(&request(), guarded_failure(error).await);
    assert2::assert!(response == expected);
}

/// An election batch whose append lost its leadership stamps each row it
/// would have changed with the code and message Kafka's
/// `newWrongControllerException` carries. An uncommitted tail is returned to
/// the guarded operation for a retry, so it never reaches this mapping and
/// has no row. Any other failure keeps its `submit failed` text.
#[yare::parameterized(
    not_leader = {
        RaftError::NotLeader { current_leader: Some(NodeId(2)) },
        (codes::NOT_CONTROLLER, "The active controller appears to be node 2.".to_owned()),
    },
    not_leader_without_hint = {
        RaftError::NotLeader { current_leader: None },
        (codes::NOT_CONTROLLER, "No controller appears to be active.".to_owned()),
    },
    leader_unknown = {
        RaftError::LeaderUnknown,
        (codes::NOT_CONTROLLER, "No controller appears to be active.".to_owned()),
    },
    shutdown = {
        RaftError::Shutdown,
        (codes::COORDINATOR_NOT_AVAILABLE, format!("submit failed: {}", RaftError::Shutdown)),
    },
    change_rejected = {
        RaftError::ChangeRejected("stale".to_owned()),
        (
            codes::COORDINATOR_NOT_AVAILABLE,
            format!("submit failed: {}", RaftError::ChangeRejected("stale".to_owned())),
        ),
    },
)]
fn a_failed_election_append_answers_kafkas_row(error: RaftError, expected: (i16, String)) {
    assert2::assert!(admission_failure(&error) == expected);
}
