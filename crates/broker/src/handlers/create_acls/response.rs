//! The `CreateAcls` response rows: one result per creation, the envelope that
//! carries them, the bulk stamp a failed controller submit leaves on the
//! creations it covered.
//!
//! Kafka answers `CreateAcls` positionally, so every path through the handler
//! has to produce exactly one row per request creation. Keeping the row
//! constructors together makes that invariant easy to see.

use {
    super::ValidCreation,
    crate::{
        codes,
        handlers::submit_failure::{controller_write_code, wrong_controller_write_message},
    },
    krabka_protocol::owned::create_acls_response::{AclCreationResult, CreateAclsResponse},
    krabka_raft::{NodeId, RaftError},
};

/// The row for a creation the controller committed.
///
/// Kafka's `AclApis.handleCreateAcls` answers a successful creation with a
/// bare `new AclCreationResult()` and sets the error fields only when the
/// creation failed, so the row carries the generated default for
/// `ErrorMessage`: the empty string, not null.
pub(super) fn acl_success_result() -> AclCreationResult {
    AclCreationResult {
        error_code: codes::NONE,
        error_message: Some(String::new()),
        ..Default::default()
    }
}

pub(super) fn acl_error_result(code: i16, msg: impl Into<String>) -> AclCreationResult {
    AclCreationResult {
        error_code: code,
        error_message: Some(msg.into()),
        ..Default::default()
    }
}

pub(super) fn create_acls_response(results: Vec<AclCreationResult>) -> CreateAclsResponse {
    CreateAclsResponse {
        results,
        ..Default::default()
    }
}

/// Stamps every submitted creation with the error a failed controller write
/// maps to.
///
/// Kafka's controller fails the `createAcls` write with the
/// `NotControllerException` of
/// `ControllerExceptions.newWrongControllerException` when it is not (or is
/// no longer) the active controller, and wraps anything else as an
/// `UnknownServerException`. `ClusterMetadataAuthorizer.createAcls` completes
/// each creation with that exception, and `AclApis.handleCreateAcls` writes
/// `ApiError.fromThrowable(throwable).message()`. `ApiError` keeps the
/// wrong-controller text, which names `QuorumController.latestController()`,
/// and drops the text of an `UnknownServerException` so no internal detail
/// reaches the client.
///
/// `known_leader` is the raft leader this node knows of when the write failed.
pub(super) fn apply_submit_error(
    results: &mut [AclCreationResult],
    to_submit: &[ValidCreation],
    err: &RaftError,
    known_leader: Option<NodeId>,
) {
    let code = controller_write_code(err, codes::UNKNOWN_SERVER_ERROR);
    let message = wrong_controller_write_message(err, known_leader);
    for creation in to_submit {
        if let Some(result) = results.get_mut(creation.result_index) {
            *result = AclCreationResult {
                error_code: code,
                error_message: message.clone(),
                ..Default::default()
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::create_acls::test_support::{OPERATION_READ, creation, validate},
        krabka_metadata::MetadataRecord,
    };

    /// Every submitted creation of a failed controller write, beside a
    /// creation the handler had already refused.
    ///
    /// Kafka's `ClusterMetadataAuthorizer.createAcls` completes each creation
    /// with the controller's exception, and `AclApis.handleCreateAcls` writes
    /// `ApiError.fromThrowable(throwable).message()`: for a controller that is
    /// not active, the message of
    /// `ControllerExceptions.newWrongControllerException`, which names
    /// `QuorumController.latestController()`, the raft leader the controller
    /// knows of. `ApiError` drops the text of an `UnknownServerException`.
    #[yare::parameterized(
        not_leader = {
            RaftError::NotLeader { current_leader: Some(NodeId(2)) },
            Some(NodeId(3)),
            codes::NOT_CONTROLLER,
            Some("The active controller appears to be node 2."),
        },
        not_leader_without_hint = {
            RaftError::NotLeader { current_leader: None },
            Some(NodeId(3)),
            codes::NOT_CONTROLLER,
            Some("No controller appears to be active."),
        },
        leader_unknown = {
            RaftError::LeaderUnknown,
            None,
            codes::NOT_CONTROLLER,
            Some("No controller appears to be active."),
        },
        uncommitted_tail = {
            RaftError::UncommittedTail,
            Some(NodeId(1)),
            codes::NOT_CONTROLLER,
            Some("The active controller appears to be node 1."),
        },
        uncommitted_tail_without_leader = {
            RaftError::UncommittedTail,
            None,
            codes::NOT_CONTROLLER,
            Some("No controller appears to be active."),
        },
        shutdown = { RaftError::Shutdown, Some(NodeId(1)), codes::UNKNOWN_SERVER_ERROR, None },
        change_rejected = {
            RaftError::ChangeRejected("internal detail".into()),
            Some(NodeId(1)),
            codes::UNKNOWN_SERVER_ERROR,
            None,
        },
    )]
    fn a_failed_submit_answers_kafkas_rows(
        error: RaftError,
        known_leader: Option<NodeId>,
        code: i16,
        message: Option<&str>,
    ) {
        let submitted = vec![ValidCreation {
            result_index: 0,
            record: MetadataRecord::V1AccessControlEntry(
                validate(&creation("topic-a", "User:alice", OPERATION_READ))
                    .expect("valid creation"),
            ),
        }];
        let mut results = vec![
            acl_success_result(),
            acl_error_result(codes::INVALID_REQUEST, "already invalid"),
        ];
        apply_submit_error(&mut results, &submitted, &error, known_leader);
        let expected = vec![
            krabka_macros::tagged_wire!(AclCreationResult {
                error_code: code,
                error_message: message.map(str::to_owned),
            }),
            krabka_macros::tagged_wire!(AclCreationResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("already invalid".into()),
            }),
        ];
        assert2::assert!(results == expected);
    }
}
