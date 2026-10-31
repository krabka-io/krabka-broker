//! The error code that a handler answers when a controller write fails.

use {crate::codes, krabka_raft::RaftError, tokio::time::error::Elapsed};

/// The error code for a `submit_change` that failed with `error`.
///
/// The controller leader refuses a break-glass consume or a topic-freeze
/// replacement with [`RaftError::UncommittedTail`] while its log holds records
/// that are not committed. A new leader is in that state until it commits a
/// record from its own epoch. Kafka's controller is not active in the same
/// window, and it answers `NOT_CONTROLLER`. A Kafka admin client treats that
/// code as retriable: it finds the active controller again and retries the
/// call. So this function answers `NOT_CONTROLLER` for that refusal.
///
/// Every other failure answers `otherwise`, the code that the handler uses for
/// a failed submit.
pub fn submit_failure_code(error: &RaftError, otherwise: i16) -> i16 {
    match error {
        RaftError::UncommittedTail => codes::NOT_CONTROLLER,
        _ => otherwise,
    }
}

/// How a guarded controller operation failed.
///
/// [`classify_guarded`] builds it from the result of
/// [`guarded_operation_on_source`](super::forward_to_controller::guarded_operation_on_source).
#[derive(Debug)]
pub enum GuardedFailure {
    /// The request deadline passed before the operation finished.
    TimedOut,
    /// This node is not the controller leader, or knows no leader.
    NotController,
    /// The operation failed with any other error.
    Failed(RaftError),
}

impl GuardedFailure {
    /// The error code a handler answers for this failure: `REQUEST_TIMED_OUT`
    /// for a deadline, `NOT_CONTROLLER` for a lost or unknown leader, and
    /// [`submit_failure_code`] with `otherwise` for anything else.
    pub fn code(&self, otherwise: i16) -> i16 {
        match self {
            Self::TimedOut => codes::REQUEST_TIMED_OUT,
            Self::NotController => codes::NOT_CONTROLLER,
            Self::Failed(error) => submit_failure_code(error, otherwise),
        }
    }
}

/// The value of a guarded controller operation, or how it failed.
///
/// # Errors
/// Returns [`GuardedFailure::TimedOut`] when the deadline passed,
/// [`GuardedFailure::NotController`] for [`RaftError::NotLeader`] and
/// [`RaftError::LeaderUnknown`], and [`GuardedFailure::Failed`] for every
/// other error.
pub fn classify_guarded<T>(
    result: Result<Result<T, RaftError>, Elapsed>,
) -> Result<T, GuardedFailure> {
    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => {
            Err(GuardedFailure::NotController)
        }
        Ok(Err(error)) => Err(GuardedFailure::Failed(error)),
        Err(_) => Err(GuardedFailure::TimedOut),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{classify_guarded, submit_failure_code},
        crate::codes,
        assert2::check,
        krabka_raft::RaftError,
        std::{future::pending, time::Duration},
        tokio::time::{error::Elapsed, timeout},
    };

    #[yare::parameterized(
        uncommitted_unknown_server_default = {
            RaftError::UncommittedTail,
            codes::UNKNOWN_SERVER_ERROR,
            codes::NOT_CONTROLLER,
        },
        uncommitted_coordinator_default = {
            RaftError::UncommittedTail,
            codes::COORDINATOR_NOT_AVAILABLE,
            codes::NOT_CONTROLLER,
        },
        refusal_keeps_default = {
            RaftError::ChangeRejected("stale".to_owned()),
            codes::UNKNOWN_SERVER_ERROR,
            codes::UNKNOWN_SERVER_ERROR,
        },
        shutdown_keeps_default = {
            RaftError::Shutdown,
            codes::COORDINATOR_NOT_AVAILABLE,
            codes::COORDINATOR_NOT_AVAILABLE,
        },
    )]
    fn an_uncommitted_controller_tail_answers_not_controller(
        error: RaftError,
        otherwise: i16,
        expected: i16,
    ) {
        check!(submit_failure_code(&error, otherwise) == expected);
    }

    /// A guarded result: `None` stands for a passed deadline.
    async fn guarded(outcome: Option<Result<u8, RaftError>>) -> Result<Result<u8, RaftError>, Elapsed> {
        match outcome {
            Some(result) => Ok(result),
            None => Err(timeout(Duration::ZERO, pending::<()>()).await.unwrap_err()),
        }
    }

    #[yare::parameterized(
        success_keeps_value = { Some(Ok(7)), codes::UNKNOWN_SERVER_ERROR, Ok(7) },
        deadline_times_out = { None, codes::UNKNOWN_SERVER_ERROR, Err(codes::REQUEST_TIMED_OUT) },
        not_leader_is_not_controller = {
            Some(Err(RaftError::NotLeader { current_leader: None })),
            codes::UNKNOWN_SERVER_ERROR,
            Err(codes::NOT_CONTROLLER),
        },
        unknown_leader_is_not_controller = {
            Some(Err(RaftError::LeaderUnknown)),
            codes::COORDINATOR_NOT_AVAILABLE,
            Err(codes::NOT_CONTROLLER),
        },
        uncommitted_tail_is_not_controller = {
            Some(Err(RaftError::UncommittedTail)),
            codes::UNKNOWN_SERVER_ERROR,
            Err(codes::NOT_CONTROLLER),
        },
        other_failure_keeps_default = {
            Some(Err(RaftError::Shutdown)),
            codes::COORDINATOR_NOT_AVAILABLE,
            Err(codes::COORDINATOR_NOT_AVAILABLE),
        },
    )]
    #[test_macro(tokio::test)]
    async fn a_guarded_result_classifies_to_its_error_code(
        outcome: Option<Result<u8, RaftError>>,
        otherwise: i16,
        expected: Result<u8, i16>,
    ) {
        let classified = classify_guarded(guarded(outcome).await);
        check!(classified.map_err(|failure| failure.code(otherwise)) == expected);
    }
}
