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

/// The error code for a controller write that failed with `error`.
///
/// Kafka's controller fails a write with `NotControllerException` when it is
/// not, or is no longer, the active controller. A lost leader
/// ([`RaftError::NotLeader`]), an unknown leader
/// ([`RaftError::LeaderUnknown`]) and an uncommitted tail of a new leader
/// ([`RaftError::UncommittedTail`], see [`submit_failure_code`]) are all that
/// state, so they answer `NOT_CONTROLLER`. Every other failure answers
/// `otherwise`.
///
/// Use this function for a write that the handler submits once, where any of
/// those errors can reach it. Use [`submit_failure_code`] where the leader
/// errors are already handled, for example by [`classify_guarded`], so that
/// only the uncommitted tail still needs a mapping.
pub fn controller_write_code(error: &RaftError, otherwise: i16) -> i16 {
    match error {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => codes::NOT_CONTROLLER,
        other => submit_failure_code(other, otherwise),
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

/// Fixtures that run [`guarded_operation_on_source`] to each failure it can
/// end in.
///
/// [`guarded_operation_on_source`]: super::forward_to_controller::guarded_operation_on_source
#[cfg(test)]
pub mod test_support {
    use {
        super::super::forward_to_controller::guarded_operation_on_source,
        crate::test_support::FakeMetadataSource,
        krabka_client_core::ClientError,
        krabka_log::LogError,
        krabka_metadata::MetadataError,
        krabka_protocol::{ProtocolError, records::RecordsError},
        krabka_raft::{MetadataReplayError, NodeId, PersistedFormatError, RaftError},
        std::{io, net::SocketAddr, path::PathBuf, time::Duration},
        tokio::time::{Instant, error::Elapsed},
        wincode::error::{ReadError, WriteError},
    };

    /// The errors that [`guarded_operation_on_source`] retries until its
    /// deadline instead of returning them.
    pub const RETRIED: [&str; 2] = ["stale_metadata_image", "uncommitted_tail"];

    /// A variant's name and a constructor for one error of it.
    pub type NamedError = (&'static str, fn() -> RaftError);

    /// A named constructor for each variant of [`RaftError`], one row per
    /// variant. A new variant gets a row here.
    pub fn every_raft_error() -> Vec<NamedError> {
        vec![
            ("storage", || {
                RaftError::from(LogError::Io(io::Error::other("disk")))
            }),
            ("network", || RaftError::from(ClientError::Disconnected)),
            ("protocol", || {
                RaftError::from(ProtocolError::InvalidValue("value"))
            }),
            ("controller_admin", || {
                RaftError::ControllerAdmin("route".to_owned())
            }),
            ("records", || {
                RaftError::from(RecordsError::HeaderTooShort { needed: 1 })
            }),
            ("malformed_control_record", || {
                RaftError::MalformedControlRecord("no key")
            }),
            ("metadata", || {
                RaftError::from(MetadataError::UnknownTopic("orders".to_owned()))
            }),
            ("unsupported", || RaftError::Unsupported("feature")),
            ("not_leader", || RaftError::NotLeader {
                current_leader: None,
            }),
            ("leader_unknown", || RaftError::LeaderUnknown),
            ("change_rejected", || {
                RaftError::ChangeRejected("rejected".to_owned())
            }),
            ("stale_metadata_image", || RaftError::StaleMetadataImage),
            ("uncommitted_tail", || RaftError::UncommittedTail),
            ("cluster_authorization_failed", || {
                RaftError::ClusterAuthorizationFailed
            }),
            ("reconfig_rejected", || {
                RaftError::ReconfigRejected("rejected".to_owned())
            }),
            ("reconfig_in_progress", || RaftError::ReconfigInProgress),
            ("duplicate_voter", || RaftError::DuplicateVoter(NodeId(1))),
            ("voter_not_found", || RaftError::VoterNotFound(NodeId(1))),
            ("invalid_voter_update", || {
                RaftError::InvalidVoterUpdate("update".to_owned())
            }),
            ("unsupported_kraft_version", || {
                RaftError::UnsupportedKraftVersion(0)
            }),
            ("voter_not_caught_up", || RaftError::VoterNotCaughtUp {
                id: NodeId(1),
                lag: 1,
            }),
            ("serde_failed", || {
                RaftError::from(WriteError::LengthEncodingOverflow("length"))
            }),
            ("serde_failed_decode", || {
                RaftError::from(ReadError::InvalidUtf8Code(0))
            }),
            ("persisted_format", || RaftError::PersistedFormat {
                artifact: "quorum-state",
                path: PathBuf::from("quorum-state"),
                problem: PersistedFormatError::MissingVersion,
            }),
            ("unsupported_private_version", || {
                RaftError::UnsupportedPrivateVersion {
                    api_key: 1003,
                    version: 9,
                }
            }),
            ("metadata_replay", || {
                RaftError::from(MetadataReplayError::InvalidControlRecord {
                    offset: 0,
                    reason: "invalid".to_owned(),
                })
            }),
            ("startup", || RaftError::Startup("config".to_owned())),
            ("controller_listener_bind", || {
                RaftError::ControllerListenerBind {
                    addr: SocketAddr::from(([127, 0, 0, 1], 0)),
                    source: io::Error::other("in use"),
                }
            }),
            ("fatal_fault", || RaftError::FatalFault("fault".to_owned())),
            ("shutdown", || RaftError::Shutdown),
        ]
    }

    /// What [`guarded_operation_on_source`] ends in when every attempt of the
    /// operation fails with `error`. Run it on a paused clock: the deadline is
    /// one second away.
    pub async fn guarded_failure<T>(
        error: fn() -> RaftError,
    ) -> Result<Result<T, RaftError>, Elapsed> {
        let source = FakeMetadataSource::builder().build();
        guarded_operation_on_source(
            &source,
            Instant::now() + Duration::from_secs(1),
            |_| async move { Err(error()) },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            classify_guarded, controller_write_code, submit_failure_code,
            test_support::{RETRIED, every_raft_error, guarded_failure},
        },
        crate::codes,
        assert2::check,
        krabka_raft::RaftError,
        std::{future::pending, time::Duration},
        tokio::time::{error::Elapsed, timeout},
    };

    /// Each row: the error, the handler's `otherwise` code, then what
    /// [`submit_failure_code`] and [`controller_write_code`] answer.
    #[yare::parameterized(
        uncommitted_unknown_server_default = {
            RaftError::UncommittedTail,
            codes::UNKNOWN_SERVER_ERROR,
            (codes::NOT_CONTROLLER, codes::NOT_CONTROLLER),
        },
        uncommitted_coordinator_default = {
            RaftError::UncommittedTail,
            codes::COORDINATOR_NOT_AVAILABLE,
            (codes::NOT_CONTROLLER, codes::NOT_CONTROLLER),
        },
        not_leader = {
            RaftError::NotLeader { current_leader: None },
            codes::UNKNOWN_SERVER_ERROR,
            (codes::UNKNOWN_SERVER_ERROR, codes::NOT_CONTROLLER),
        },
        unknown_leader = {
            RaftError::LeaderUnknown,
            codes::COORDINATOR_NOT_AVAILABLE,
            (codes::COORDINATOR_NOT_AVAILABLE, codes::NOT_CONTROLLER),
        },
        refusal_keeps_default = {
            RaftError::ChangeRejected("stale".to_owned()),
            codes::UNKNOWN_SERVER_ERROR,
            (codes::UNKNOWN_SERVER_ERROR, codes::UNKNOWN_SERVER_ERROR),
        },
        // A controller that refuses the version of a forwarded write answers
        // the handler's own failure code, not the retriable NOT_CONTROLLER.
        unsupported_private_version_keeps_default = {
            RaftError::UnsupportedPrivateVersion { api_key: 1003, version: 1 },
            codes::UNKNOWN_SERVER_ERROR,
            codes::UNKNOWN_SERVER_ERROR,
        },
        shutdown_keeps_default = {
            RaftError::Shutdown,
            codes::COORDINATOR_NOT_AVAILABLE,
            (codes::COORDINATOR_NOT_AVAILABLE, codes::COORDINATOR_NOT_AVAILABLE),
        },
    )]
    fn a_failed_controller_write_maps_to_its_error_code(
        error: RaftError,
        otherwise: i16,
        expected: (i16, i16),
    ) {
        check!(
            (
                submit_failure_code(&error, otherwise),
                controller_write_code(&error, otherwise)
            ) == expected
        );
    }

    /// A guarded result: `None` stands for a passed deadline.
    async fn guarded(
        outcome: Option<Result<u8, RaftError>>,
    ) -> Result<Result<u8, RaftError>, Elapsed> {
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

    /// `guarded_operation_on_source` never returns a stale image or an
    /// uncommitted tail. It retries both until the deadline, and it returns
    /// every other error as it is.
    #[tokio::test(start_paused = true)]
    async fn a_guarded_operation_returns_every_error_but_the_two_it_retries() {
        for (name, error) in every_raft_error() {
            let outcome = guarded_failure::<()>(error)
                .await
                .map(|result| result.map_err(|error| format!("{error:?}")))
                .map_err(|_| "deadline");
            let expected = if RETRIED.contains(&name) {
                Err("deadline")
            } else {
                Ok(Err(format!("{:?}", error())))
            };
            check!(outcome == expected, "{name}");
        }
    }

    /// For every error that `guarded_operation_on_source` can return,
    /// `submit_failure_code` answers the handler's own code. A handler that
    /// answered that code directly before `GuardedFailure::code` answers the
    /// same now.
    #[tokio::test(start_paused = true)]
    async fn every_error_a_guarded_operation_returns_keeps_the_handler_code() {
        let mut returned = Vec::new();
        for (name, error) in every_raft_error() {
            if let Ok(Err(error)) = guarded_failure::<()>(error).await {
                check!(
                    [
                        codes::UNKNOWN_SERVER_ERROR,
                        codes::COORDINATOR_NOT_AVAILABLE
                    ]
                    .map(|otherwise| submit_failure_code(&error, otherwise))
                        == [
                            codes::UNKNOWN_SERVER_ERROR,
                            codes::COORDINATOR_NOT_AVAILABLE
                        ],
                    "{name}"
                );
                returned.push(name);
            }
        }
        let expected = every_raft_error()
            .into_iter()
            .map(|(name, _)| name)
            .filter(|name| !RETRIED.contains(name))
            .collect::<Vec<_>>();
        check!(returned == expected);
    }
}
