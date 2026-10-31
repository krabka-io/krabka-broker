//! The row of a topic whose guarded growth failed, for every error that the
//! growth can fail with, against the row that `main` built before
//! `classify_guarded`.

use {
    super::growth_outcome,
    crate::{
        codes,
        handlers::submit_failure::test_support::{every_raft_error, guarded_failure},
    },
    assert2::check,
    krabka_protocol::owned::create_partitions_response::CreatePartitionsTopicResult,
    krabka_raft::RaftError,
    tokio::time::error::Elapsed,
};

/// The row that `main` built from a guarded growth result, without its log
/// line.
fn row_on_main(
    name: String,
    attempt: Result<Result<CreatePartitionsTopicResult, RaftError>, Elapsed>,
) -> CreatePartitionsTopicResult {
    let error_code = match attempt {
        Ok(Ok(row)) => return row,
        Err(_) => codes::REQUEST_TIMED_OUT,
        Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => codes::NOT_CONTROLLER,
        Ok(Err(_)) => codes::UNKNOWN_SERVER_ERROR,
    };
    CreatePartitionsTopicResult {
        name,
        error_code,
        ..Default::default()
    }
}

#[tokio::test(start_paused = true)]
async fn a_failed_growth_answers_the_row_main_answered() {
    for (name, error) in every_raft_error() {
        let refactored = growth_outcome("orders".to_owned(), guarded_failure(error).await);
        let original = row_on_main("orders".to_owned(), guarded_failure(error).await);
        check!(refactored == original, "{name}");
    }
}
