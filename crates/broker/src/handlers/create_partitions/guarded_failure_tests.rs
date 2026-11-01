//! The row of a topic whose guarded growth failed, for every error that the
//! growth can fail with, and the answer of a node that is not the active
//! controller.

use {
    super::{growth_outcome, inactive_controller_refusal},
    crate::{
        codes,
        handlers::{
            create_partitions::test_support::{PartitionMutation, request, topic_req},
            forward_to_controller::wrong_controller_message,
            submit_failure::test_support::{every_raft_error, guarded_failure},
        },
    },
    assert2::check,
    krabka_protocol::owned::create_partitions_response::{
        CreatePartitionsResponse, CreatePartitionsTopicResult,
    },
    krabka_raft::{NodeId, RaftError},
    tokio::time::error::Elapsed,
};

/// The row Kafka answers for a guarded growth result: the grown row, or the
/// error code of the failure, with the message of Kafka's
/// `newWrongControllerException` on a `NOT_CONTROLLER` row.
fn kafka_row(
    name: String,
    attempt: Result<Result<CreatePartitionsTopicResult, RaftError>, Elapsed>,
) -> CreatePartitionsTopicResult {
    let (error_code, error_message) = match attempt {
        Ok(Ok(row)) => return row,
        Err(_) => (codes::REQUEST_TIMED_OUT, None),
        Ok(Err(RaftError::NotLeader { current_leader })) => (
            codes::NOT_CONTROLLER,
            Some(wrong_controller_message(current_leader)),
        ),
        Ok(Err(RaftError::LeaderUnknown)) => {
            (codes::NOT_CONTROLLER, Some(wrong_controller_message(None)))
        }
        Ok(Err(_)) => (codes::UNKNOWN_SERVER_ERROR, None),
    };
    CreatePartitionsTopicResult {
        name,
        error_code,
        error_message,
        ..Default::default()
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_growth_answers_kafkas_row() {
    for (name, error) in every_raft_error() {
        let answered = growth_outcome("orders".to_owned(), guarded_failure(error).await);
        let expected = kafka_row("orders".to_owned(), guarded_failure(error).await);
        check!(answered == expected, "{name}");
    }
}

/// The row of a topic whose guarded growth failed with a controller error.
///
/// Kafka's `ControllerApis.handleCreatePartitions` answers a failed
/// `createPartitions` through `CreatePartitionsRequest.getErrorResponse`,
/// which sets `ApiError.message()` on the row: for a controller that is not
/// active, the message of `ControllerExceptions.newWrongControllerException`.
/// A guarded growth retries `UncommittedTail` until the deadline, so that row
/// answers `REQUEST_TIMED_OUT` whatever leader this node knows; the case
/// without a known leader is the same row and is left out.
#[yare::parameterized(
    not_leader = {
        || RaftError::NotLeader { current_leader: Some(NodeId(2)) },
        row(codes::NOT_CONTROLLER, Some("The active controller appears to be node 2.")),
    },
    not_leader_without_hint = {
        || RaftError::NotLeader { current_leader: None },
        row(codes::NOT_CONTROLLER, Some("No controller appears to be active.")),
    },
    leader_unknown = {
        || RaftError::LeaderUnknown,
        row(codes::NOT_CONTROLLER, Some("No controller appears to be active.")),
    },
    uncommitted_tail = { || RaftError::UncommittedTail, row(codes::REQUEST_TIMED_OUT, None) },
    shutdown = { || RaftError::Shutdown, row(codes::UNKNOWN_SERVER_ERROR, None) },
)]
#[test_macro(tokio::test(start_paused = true))]
async fn a_failed_growth_carries_kafkas_not_controller_message(
    error: fn() -> RaftError,
    expected: CreatePartitionsTopicResult,
) {
    let answered = growth_outcome("orders".to_owned(), guarded_failure(error).await);
    check!(answered == expected);
}

/// The row of topic `orders` with `error_code` and `error_message`.
fn row(error_code: i16, error_message: Option<&str>) -> CreatePartitionsTopicResult {
    CreatePartitionsTopicResult {
        name: "orders".to_owned(),
        error_code,
        error_message: error_message.map(str::to_owned),
        ..Default::default()
    }
}

/// The answer of a node that is not the active controller to a request that
/// reached it without forwarding.
///
/// Kafka's `ControllerApis.handleCreatePartitions` answers it through
/// `CreatePartitionsRequest.getErrorResponse`, which sets `ApiError.message()`
/// on every topic: the message of
/// `ControllerExceptions.newWrongControllerException`, which names
/// `QuorumController.latestController()`, the raft leader this node knows.
#[yare::parameterized(
    known_leader = {
        Some(NodeId(3)),
        Some("The active controller appears to be node 3."),
    },
    no_known_leader = { None, Some("No controller appears to be active.") },
)]
fn an_inactive_controller_answers_kafkas_refusal(
    known_leader: Option<NodeId>,
    message: Option<&str>,
) {
    let req = request(
        vec![
            topic_req().name("orders").call(),
            topic_req().name("payments").call(),
        ],
        PartitionMutation::Apply,
    );
    let refused = |name: &str| CreatePartitionsTopicResult {
        name: name.to_owned(),
        error_code: codes::NOT_CONTROLLER,
        error_message: message.map(str::to_owned),
        ..Default::default()
    };
    let expected = CreatePartitionsResponse {
        results: vec![refused("orders"), refused("payments")],
        ..Default::default()
    };
    check!(inactive_controller_refusal(&req, known_leader) == expected);
}
