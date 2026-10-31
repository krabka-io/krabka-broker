//! The row of a topic whose guarded deletion failed, for every error that the
//! deletion can fail with, against the row that `main` built before
//! `classify_guarded`.

use {
    super::{delete_topic_result, deletion_outcome, vanished_code},
    crate::{
        codes,
        handlers::{
            submit_failure::test_support::{every_raft_error, guarded_failure},
            submit_failure_code,
        },
    },
    assert2::check,
    krabka_metadata::MetadataError,
    krabka_protocol::{
        owned::delete_topics_response::DeletableTopicResult, primitives::uuid::Uuid as WireUuid,
    },
    krabka_raft::RaftError,
    tokio::time::error::Elapsed,
    uuid::Uuid,
};

/// The row that `main` built from a guarded deletion result, without its log
/// line.
fn row_on_main(
    name: String,
    wire_topic_id: WireUuid,
    requested_by_id: bool,
    attempt: Result<Result<DeletableTopicResult, RaftError>, Elapsed>,
) -> DeletableTopicResult {
    let code = match attempt {
        Ok(Ok(row)) => return row,
        Err(_) => codes::REQUEST_TIMED_OUT,
        Ok(Err(RaftError::Metadata(MetadataError::UnknownTopic(_)))) => {
            vanished_code(requested_by_id)
        }
        Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => codes::NOT_CONTROLLER,
        Ok(Err(error)) => submit_failure_code(&error, codes::UNKNOWN_SERVER_ERROR),
    };
    delete_topic_result(Some(name), wire_topic_id, code)
}

#[tokio::test(start_paused = true)]
async fn a_failed_deletion_answers_the_row_main_answered() {
    let wire_topic_id = WireUuid(Uuid::from_u128(7).into_bytes());
    for requested_by_id in [false, true] {
        for (name, error) in every_raft_error() {
            let refactored = deletion_outcome(
                "orders".to_owned(),
                wire_topic_id,
                requested_by_id,
                guarded_failure(error).await,
            );
            let original = row_on_main(
                "orders".to_owned(),
                wire_topic_id,
                requested_by_id,
                guarded_failure(error).await,
            );
            check!(refactored == original, "{name}, by id: {requested_by_id}");
        }
    }
}
