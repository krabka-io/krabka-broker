//! The `DeleteAcls` response rows: the matching-ACL echo Kafka sends back for
//! every entry a filter removed, the per-filter result that carries them, the
//! envelope, the bulk stamp a failed controller submit leaves behind.
//!
//! `kafka-acls --remove` prints the matching-ACL list verbatim, so the wire
//! bytes here are what the operator reads. Keeping the row constructors
//! together makes the one-row-per-filter invariant easy to see.

use {
    crate::{
        codes,
        handlers::{
            acl_wire::{
                operation_to_wire, pattern_type_to_wire, permission_to_wire, resource_type_to_wire,
            },
            stamp_unset,
            submit_failure::{controller_write_code, wrong_controller_write_message},
        },
    },
    krabka_metadata::AclEntry,
    krabka_protocol::owned::delete_acls_response::{
        DeleteAclsFilterResult, DeleteAclsMatchingAcl, DeleteAclsResponse,
    },
    krabka_raft::{NodeId, RaftError},
};

/// The echo of one ACL a filter removed.
///
/// Kafka's `DeleteAclsResponse.matchingAcl` writes `ApiError.NONE`'s code and
/// message for a successful deletion, and that message is null.
pub(super) fn matching_acl_result(e: &AclEntry) -> DeleteAclsMatchingAcl {
    DeleteAclsMatchingAcl {
        error_code: codes::NONE,
        error_message: None,
        resource_type: resource_type_to_wire(e.resource_type),
        resource_name: e.resource_name.clone(),
        pattern_type: pattern_type_to_wire(e.pattern_type),
        principal: e.principal.clone(),
        host: e.host.clone(),
        operation: operation_to_wire(e.operation),
        permission_type: permission_to_wire(e.permission_type),
        ..Default::default()
    }
}

pub(super) fn filter_result(
    error_code: i16,
    error_message: Option<String>,
    matching_acls: Vec<DeleteAclsMatchingAcl>,
) -> DeleteAclsFilterResult {
    DeleteAclsFilterResult {
        error_code,
        error_message,
        matching_acls,
        ..Default::default()
    }
}

pub(super) fn delete_acls_response(
    filter_results: Vec<DeleteAclsFilterResult>,
) -> DeleteAclsResponse {
    DeleteAclsResponse {
        filter_results,
        ..Default::default()
    }
}

/// Stamps the filter results of a request whose deletion write failed with
/// `err`, where `known_leader` is the controller leader this node knows.
///
/// A lost or unknown leader and an uncommitted tail answer `NOT_CONTROLLER`
/// (see [`controller_write_code`]) on every filter, with the message of
/// `ControllerExceptions.newWrongControllerException` and no matching ACL.
/// Kafka's `AclControlManager.deleteAcls` validates and matches each filter
/// inside the controller write, so a controller that is not active fails them
/// all, and `ClusterMetadataAuthorizer.deleteAcls` completes each with an
/// `AclDeleteResult` of that exception. Every other failure stamps the rows
/// still unset with krabka's own `submit failed` text.
pub(super) fn apply_submit_error(
    filter_results: &mut [DeleteAclsFilterResult],
    err: &RaftError,
    known_leader: Option<NodeId>,
) {
    let code = controller_write_code(err, codes::COORDINATOR_NOT_AVAILABLE);
    match wrong_controller_write_message(err, known_leader) {
        Some(message) => {
            for result in filter_results {
                *result = filter_result(code, Some(message.clone()), Vec::new());
            }
        }
        None => stamp_unset(filter_results, code, &format!("submit failed: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::{
            acl_test_support::expected_deleted_orders_acl,
            delete_acls::test_support::{OPERATION_WRITE, PATTERN_TYPE_PREFIXED, acl},
            submit_failure::test_support::submit_not_controller_rows,
        },
        assert2::{assert, check},
        krabka_metadata::{AclOperation, PatternType},
    };

    /// Every filter of a request whose deletion write failed because this
    /// node is not the active controller.
    ///
    /// Kafka's `QuorumController.deleteAcls` is a controller write event, and
    /// `AclControlManager.deleteAcls` validates and matches each filter inside
    /// it, so a controller that is not active fails the whole write with
    /// `ControllerExceptions.newWrongControllerException`.
    /// `ClusterMetadataAuthorizer.deleteAcls` completes every filter with an
    /// `AclDeleteResult` of that exception, which lists no matching ACL, and
    /// `DeleteAclsResponse.filterResult` writes `ApiError.fromThrowable(e)`'s
    /// code and message.
    #[test]
    fn a_failed_submit_answers_kafkas_not_controller_rows() {
        for case in submit_not_controller_rows() {
            let mut results = vec![
                filter_result(codes::NONE, None, vec![matching_acl_result(&acl().call())]),
                filter_result(
                    codes::INVALID_REQUEST,
                    Some("Unknown entryFilter.".into()),
                    Vec::new(),
                ),
            ];
            apply_submit_error(&mut results, &(case.error)(), case.known_leader);
            let refused = filter_result(
                codes::NOT_CONTROLLER,
                Some(case.error_message.into()),
                Vec::new(),
            );
            check!(results == vec![refused.clone(), refused], "{}", case.name);
        }
    }

    #[test]
    fn helpers_preserve_matching_acl_and_submit_error_fields() {
        let matched = matching_acl_result(&acl().call());
        let expected_matched = expected_deleted_orders_acl();
        assert!(matched == expected_matched);

        let mut prefixed_acl = acl()
            .resource_name("orders-")
            .principal("User:bob")
            .operation(AclOperation::Write)
            .call();
        prefixed_acl.pattern_type = PatternType::Prefixed;
        let matched = matching_acl_result(&prefixed_acl);
        assert!(matched.pattern_type == PATTERN_TYPE_PREFIXED);
        assert!(matched.operation == OPERATION_WRITE);

        let mut results = vec![
            filter_result(codes::NONE, None, vec![matched.clone()]),
            filter_result(
                codes::INVALID_REQUEST,
                Some("Unknown entryFilter.".into()),
                Vec::new(),
            ),
        ];
        apply_submit_error(&mut results, &RaftError::Shutdown, Some(NodeId(1)));

        let expected_results = vec![
            krabka_macros::tagged_wire!(DeleteAclsFilterResult {
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: controller shut down".into()),
                matching_acls: vec![matched],
            }),
            krabka_macros::tagged_wire!(DeleteAclsFilterResult {
                error_code: codes::INVALID_REQUEST,
                error_message: Some("Unknown entryFilter.".into()),
                matching_acls: Vec::new(),
            }),
        ];
        assert!(results == expected_results);
    }
}
