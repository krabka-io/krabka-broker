//! Construction of the per-user rows of the response.
//!
//! One place builds every row shape the handler can return: an accepted row,
//! a row that carries an error code and a message, and the rewrite of the rows
//! when the metadata submit fails.

use {
    crate::{
        codes,
        handlers::{
            stamp_unset,
            submit_failure::{controller_write_code, wrong_controller_write_message},
        },
    },
    krabka_protocol::owned::alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
    krabka_raft::{NodeId, RaftError},
};

pub(super) fn ok_result(name: String) -> AlterUserScramCredentialsResult {
    AlterUserScramCredentialsResult {
        user: name,
        error_code: 0,
        error_message: None,
        ..Default::default()
    }
}

pub(super) fn err_result(name: String, code: i16, msg: &str) -> AlterUserScramCredentialsResult {
    AlterUserScramCredentialsResult {
        user: name,
        error_code: code,
        error_message: Some(msg.to_owned()),
        ..Default::default()
    }
}

/// Stamps the per-user rows of a request whose write failed with `error`,
/// where `known_leader` is the controller leader this node knows.
///
/// A lost or unknown leader and an uncommitted tail answer `NOT_CONTROLLER`
/// (see [`controller_write_code`]) on every row, sorted by user, with the
/// message of `ControllerExceptions.newWrongControllerException`. Kafka's
/// controller fails the whole write before it validates any user, and
/// `AlterUserScramCredentialsRequest.getErrorResponse` answers one row per
/// distinct user, sorted by name. `results` holds one row per distinct user.
/// Every other failure stamps the rows still unset with krabka's own
/// `submit failed` text.
pub(super) fn apply_submit_error(
    results: &mut [AlterUserScramCredentialsResult],
    error: &RaftError,
    known_leader: Option<NodeId>,
) {
    let code = controller_write_code(error, codes::UNKNOWN_SERVER_ERROR);
    match wrong_controller_write_message(error, known_leader) {
        Some(message) => {
            results.sort_by(|a, b| a.user.cmp(&b.user));
            for result in results {
                result.error_code = code;
                result.error_message = Some(message.clone());
            }
        }
        None => stamp_unset(results, code, &format!("submit failed: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::{
            alter_user_scram_credentials::test_support::expected_result,
            submit_failure::test_support::submit_not_controller_rows,
        },
        assert2::{assert, check},
    };

    #[test]
    fn err_result_carries_code_and_message() {
        let r = err_result("alice".into(), codes::UNACCEPTABLE_CREDENTIAL, "bad");
        assert!(r == expected_result("alice", codes::UNACCEPTABLE_CREDENTIAL, Some("bad")));
    }

    #[test]
    fn ok_result_has_zero_error_code() {
        let r = ok_result("alice".into());
        assert!(r == expected_result("alice", 0, None));
    }

    #[test]
    fn submit_error_rewrites_only_success_rows() {
        let mut results = vec![
            ok_result("alice".into()),
            err_result(
                "bob".into(),
                codes::DUPLICATE_RESOURCE,
                "duplicate resource",
            ),
        ];

        apply_submit_error(&mut results, &RaftError::Shutdown, Some(NodeId(1)));

        let expected = vec![
            expected_result(
                "alice",
                codes::UNKNOWN_SERVER_ERROR,
                Some("submit failed: controller shut down"),
            ),
            expected_result("bob", codes::DUPLICATE_RESOURCE, Some("duplicate resource")),
        ];
        assert!(results == expected);
    }

    /// The rows of a request whose write failed because this node is not the
    /// active controller.
    ///
    /// Kafka's `QuorumController.alterUserScramCredentials` is a controller
    /// write event, and `ScramControlManager` validates each user inside it,
    /// so a controller that is not active fails the whole write with
    /// `ControllerExceptions.newWrongControllerException`.
    /// `ControllerApis.handleAlterUserScramCredentials` hands that to
    /// `requestHelper.handleError`, and
    /// `AlterUserScramCredentialsRequest.getErrorResponse` answers one row per
    /// distinct user, sorted by name, each with `ApiError.fromThrowable(e)`'s
    /// code and message.
    #[test]
    fn a_failed_submit_answers_kafkas_not_controller_rows() {
        for case in submit_not_controller_rows() {
            let mut results = vec![
                ok_result("carol".into()),
                err_result("alice".into(), codes::DUPLICATE_RESOURCE, "duplicate"),
                ok_result("bob".into()),
            ];
            apply_submit_error(&mut results, &(case.error)(), case.known_leader);
            let expected = ["alice", "bob", "carol"]
                .map(|user| expected_result(user, codes::NOT_CONTROLLER, Some(case.error_message)));
            check!(results == expected, "{}", case.name);
        }
    }
}
