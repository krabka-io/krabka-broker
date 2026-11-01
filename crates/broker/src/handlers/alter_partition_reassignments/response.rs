//! Response assembly for `AlterPartitionReassignments`: the per-partition
//! result rows and the whole-request error envelope.

use {
    crate::{
        codes::{COORDINATOR_NOT_AVAILABLE, NOT_CONTROLLER},
        handlers::{submit_failure::wrong_controller_write_message, submit_failure_code},
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
/// `ApiError.message()` on every partition. A `NOT_CONTROLLER` row therefore
/// carries the message of `ControllerExceptions.newWrongControllerException`
/// (see [`wrong_controller_write_message`]). Every other code keeps krabka's
/// own `submit failed` text.
pub(super) fn submit_failure_error(
    error: &RaftError,
    known_leader: Option<NodeId>,
) -> (i16, String) {
    let code = submit_failure_code(error, COORDINATOR_NOT_AVAILABLE);
    let message = (code == NOT_CONTROLLER)
        .then(|| wrong_controller_write_message(error, known_leader))
        .flatten()
        .unwrap_or_else(|| format!("submit failed: {error}"));
    (code, message)
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
            handlers::alter_partition_reassignments::test_support::request,
        },
        assert2::{assert, check},
        krabka_raft::{NodeId, RaftError},
    };

    /// The row of partition 0 with `error_code` and `error_message`.
    fn row(error_code: i16, error_message: &str) -> ReassignablePartitionResponse {
        err_row(0, error_code, error_message.to_owned())
    }

    /// The row a partition answers when the batch's submit failed, given the
    /// leader this node knows when the write failed.
    ///
    /// Kafka's `ControllerApis.handleAlterPartitionReassignments` answers a
    /// failed `alterPartitionReassignments` through
    /// `requestHelper.handleError`, and
    /// `AlterPartitionReassignmentsRequest.getErrorResponse` sets
    /// `ApiError.message()` on every partition: for a controller that is not
    /// active, the message of `ControllerExceptions.newWrongControllerException`.
    /// krabka answers a lost or unknown leader `COORDINATOR_NOT_AVAILABLE`
    /// rather than `NOT_CONTROLLER`, so those rows keep krabka's own message.
    #[yare::parameterized(
        not_leader = {
            RaftError::NotLeader { current_leader: Some(NodeId(2)) },
            Some(NodeId(3)),
            row(
                COORDINATOR_NOT_AVAILABLE,
                "submit failed: not leader; current leader: Some(NodeId(2))",
            ),
        },
        not_leader_without_hint = {
            RaftError::NotLeader { current_leader: None },
            Some(NodeId(3)),
            row(COORDINATOR_NOT_AVAILABLE, "submit failed: not leader; current leader: None"),
        },
        leader_unknown = {
            RaftError::LeaderUnknown,
            None,
            row(COORDINATOR_NOT_AVAILABLE, "submit failed: leader unknown (election in progress)"),
        },
        uncommitted_tail = {
            RaftError::UncommittedTail,
            Some(NodeId(1)),
            row(codes::NOT_CONTROLLER, "The active controller appears to be node 1."),
        },
        uncommitted_tail_without_leader = {
            RaftError::UncommittedTail,
            None,
            row(codes::NOT_CONTROLLER, "No controller appears to be active."),
        },
        shutdown = {
            RaftError::Shutdown,
            Some(NodeId(1)),
            row(COORDINATOR_NOT_AVAILABLE, "submit failed: controller shut down"),
        },
    )]
    fn a_failed_submit_answers_kafkas_row(
        error: RaftError,
        known_leader: Option<NodeId>,
        expected: ReassignablePartitionResponse,
    ) {
        let (code, message) = submit_failure_error(&error, known_leader);
        check!(err_row(0, code, message) == expected);
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
