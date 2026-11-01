//! Response assembly for `AlterPartitionReassignments`: the per-partition
//! result rows and the whole-request error envelope.

use {
    crate::{
        codes::{COORDINATOR_NOT_AVAILABLE, NOT_CONTROLLER},
        handlers::submit_failure::{controller_write_code, wrong_controller_write_message},
    },
    krabka_protocol::{
        UnknownTaggedFields,
        owned::{
            alter_partition_reassignments_request::AlterPartitionReassignmentsRequest,
            alter_partition_reassignments_response::{
                AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
                ReassignableTopicResponse,
            },
        },
    },
    krabka_raft::{NodeId, RaftError},
};

/// The error code and message that every row still unset answers when the
/// batch's `submit_change` failed with `error`, where `known_leader` is the
/// controller leader this node knows when the write failed.
///
/// Kafka's `ControllerApis.handleAlterPartitionReassignments` answers a failed
/// write through `requestHelper.handleError`, whose
/// `AlterPartitionReassignmentsRequest.getErrorResponse` sets
/// `ApiError.message()` on every partition. A lost or unknown leader and an
/// uncommitted tail answer `NOT_CONTROLLER` (see [`controller_write_code`]),
/// and that row carries the message of
/// `ControllerExceptions.newWrongControllerException` (see
/// [`wrong_controller_write_message`]). Every other code keeps krabka's own
/// `submit failed` text.
pub(super) fn submit_failure_error(
    error: &RaftError,
    known_leader: Option<NodeId>,
) -> (i16, String) {
    let code = controller_write_code(error, COORDINATOR_NOT_AVAILABLE);
    let message = (code == NOT_CONTROLLER)
        .then(|| wrong_controller_write_message(error, known_leader))
        .flatten()
        .unwrap_or_else(|| format!("submit failed: {error}"));
    (code, message)
}

/// The whole response when the batch's `submit_change` failed with `error`
/// because this node is not the active controller, or `None` for any other
/// failure.
///
/// Kafka's `QuorumController.alterPartitionReassignments` is a controller
/// write event, so a controller that is not active fails it before it
/// validates any partition. `AlterPartitionReassignmentsRequest.getErrorResponse`
/// then puts the `NOT_CONTROLLER` code and message at the top level as well as
/// on every partition, which [`whole_request_error`] builds.
pub(super) fn not_controller_response(
    req: &AlterPartitionReassignmentsRequest,
    error: &RaftError,
    known_leader: Option<NodeId>,
) -> Option<AlterPartitionReassignmentsResponse> {
    let (code, message) = submit_failure_error(error, known_leader);
    (code == NOT_CONTROLLER).then(|| whole_request_error(req, code, &message))
}

/// A partition the request altered, or would have.
///
/// Kafka's `ReplicationControlManager.alterPartitionReassignments` sets
/// `ErrorMessage` from `ApiError.NONE.message()`, which is null.
pub(super) fn ok_row(partition_index: i32) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        error_code: 0,
        error_message: None,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

pub(super) fn err_row(
    partition_index: i32,
    code: i16,
    msg: String,
) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        error_code: code,
        error_message: Some(msg),
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

pub(super) fn whole_request_error(
    req: &AlterPartitionReassignmentsRequest,
    code: i16,
    msg: &str,
) -> AlterPartitionReassignmentsResponse {
    let responses: Vec<ReassignableTopicResponse> = req
        .topics
        .iter()
        .map(|t| ReassignableTopicResponse {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| err_row(p.partition_index, code, msg.into()))
                .collect(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        })
        .collect();
    AlterPartitionReassignmentsResponse {
        throttle_time_ms: 0,
        // Kafka's `AlterPartitionReassignmentsRequest.getErrorResponse` leaves
        // this at its schema default, `true`, whatever the request asked.
        allow_replication_factor_change: true,
        error_code: code,
        error_message: Some(msg.to_owned()),
        responses,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            codes::{self, CLUSTER_AUTHORIZATION_FAILED, UNKNOWN_TOPIC_OR_PARTITION},
            handlers::{
                alter_partition_reassignments::test_support::request,
                submit_failure::test_support::submit_not_controller_rows,
            },
        },
        assert2::{assert, check},
        krabka_raft::{NodeId, RaftError},
    };

    /// The whole response when the batch's submit failed because this node is
    /// not the active controller.
    ///
    /// Kafka's `QuorumController.alterPartitionReassignments` is a controller
    /// write event, which fails with
    /// `ControllerExceptions.newWrongControllerException` before it validates
    /// any partition. `ControllerApis.handleAlterPartitionReassignments` hands
    /// that to `requestHelper.handleError`, and
    /// `AlterPartitionReassignmentsRequest.getErrorResponse` sets the code and
    /// `ApiError.message()` at the top level and on every partition. It leaves
    /// `AllowReplicationFactorChange` at its schema default, `true`.
    #[test]
    fn a_failed_submit_answers_kafkas_not_controller_response() {
        let req = request().topic("payments").partition_index(8).call();
        for case in submit_not_controller_rows() {
            let message = case.error_message.to_owned();
            let expected = krabka_macros::unthrottled_wire!(AlterPartitionReassignmentsResponse {
                allow_replication_factor_change: true,
                error_code: codes::NOT_CONTROLLER,
                error_message: Some(message.clone()),
                responses: vec![krabka_macros::tagged_wire!(ReassignableTopicResponse {
                    name: "payments".to_owned(),
                    partitions: vec![err_row(8, codes::NOT_CONTROLLER, message)],
                })],
            });
            check!(
                not_controller_response(&req, &(case.error)(), case.known_leader) == Some(expected),
                "{}",
                case.name
            );
        }
    }

    /// Any other failed submit keeps krabka's per-partition row and answers no
    /// whole-request error.
    #[test]
    fn another_failed_submit_keeps_its_row() {
        let req = request().call();
        check!(
            submit_failure_error(&RaftError::Shutdown, Some(NodeId(1)))
                == (
                    COORDINATOR_NOT_AVAILABLE,
                    "submit failed: controller shut down".to_owned()
                )
        );
        check!(not_controller_response(&req, &RaftError::Shutdown, Some(NodeId(1))).is_none());
    }

    #[test]
    fn row_builders_preserve_non_default_fields() {
        let ok = ok_row(7);
        let expected_ok = krabka_macros::tagged_wire!(ReassignablePartitionResponse {
            partition_index: 7,
            error_code: 0,
            error_message: None,
        });
        assert!(ok == expected_ok);

        let err = err_row(8, UNKNOWN_TOPIC_OR_PARTITION, "missing partition".into());
        let expected_err = krabka_macros::tagged_wire!(ReassignablePartitionResponse {
            partition_index: 8,
            error_code: UNKNOWN_TOPIC_OR_PARTITION,
            error_message: Some("missing partition".into()),
        });
        assert!(err == expected_err);
    }

    #[test]
    fn whole_request_error_preserves_request_shape() {
        let req = request().topic("payments").partition_index(8).call();

        let resp = whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "denied");

        assert!(resp.allow_replication_factor_change);
        assert!(resp.error_code == CLUSTER_AUTHORIZATION_FAILED);
        assert!(resp.error_message.as_deref() == Some("denied"));
        assert!(resp.responses.len() == 1);
        assert!(resp.responses[0].name == "payments");
        assert!(resp.responses[0].partitions.len() == 1);
        let partition = &resp.responses[0].partitions[0];
        assert!(partition.partition_index == 8);
        assert!(partition.error_code == CLUSTER_AUTHORIZATION_FAILED);
        assert!(partition.error_message.as_deref() == Some("denied"));
    }
}
