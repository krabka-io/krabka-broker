//! The response to an election whose guarded submission failed, for every
//! error that the election can fail with, against the response that `main`
//! built before `classify_guarded`.
//!
//! A lost or unknown leader is left out: it answers Kafka's
//! `newWrongControllerException` text, which `main` did not, and
//! `not_controller_tests` covers it.

use {
    super::{election_outcome, request_refusal},
    crate::{
        codes,
        handlers::{
            submit_failure::test_support::{every_raft_error, guarded_failure},
            submit_failure_code,
        },
    },
    assert2::check,
    krabka_protocol::owned::{
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        elect_leaders_response::ElectLeadersResponse,
    },
    krabka_raft::RaftError,
    tokio::time::error::Elapsed,
};

/// The response that `main` built from a guarded election result.
fn response_on_main(
    req: &ElectLeadersRequest,
    result: Result<Result<ElectLeadersResponse, RaftError>, Elapsed>,
) -> ElectLeadersResponse {
    match result {
        Ok(Ok(response)) => response,
        Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => {
            request_refusal(req, codes::NOT_CONTROLLER, None)
        }
        Ok(Err(error)) => request_refusal(
            req,
            submit_failure_code(&error, codes::COORDINATOR_NOT_AVAILABLE),
            Some(&error.to_string()),
        ),
        Err(_) => request_refusal(req, codes::REQUEST_TIMED_OUT, None),
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_election_answers_the_response_main_answered() {
    let req = ElectLeadersRequest {
        topic_partitions: Some(vec![TopicPartitions {
            topic: "orders".to_owned(),
            partitions: vec![0, 1],
            ..Default::default()
        }]),
        timeout_ms: 5_000,
        ..Default::default()
    };
    let not_controller = ["not_leader", "leader_unknown"];
    for (name, error) in every_raft_error()
        .into_iter()
        .filter(|(name, _)| !not_controller.contains(name))
    {
        let refactored = election_outcome(&req, guarded_failure(error).await);
        let original = response_on_main(&req, guarded_failure(error).await);
        check!(refactored == original, "{name}");
    }
}
