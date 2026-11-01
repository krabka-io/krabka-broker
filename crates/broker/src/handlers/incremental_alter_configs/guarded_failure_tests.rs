//! The row of an `IncrementalAlterConfigs` resource whose guarded alter gave
//! up, for each controller failure that ends the retry.

use {
    super::{RESOURCE_TYPE_TOPIC, resource_refusal},
    crate::{
        codes,
        handlers::{
            alter_configs::guarded_resource_outcome, submit_failure::test_support::guarded_failure,
        },
    },
    assert2::check,
    krabka_protocol::owned::{
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
    },
    krabka_raft::{NodeId, RaftError},
};

/// The row of a resource whose guarded alter failed with a controller error.
///
/// Kafka's `ControllerApis.handleIncrementalAlterConfigs` answers a failed
/// `incrementalAlterConfigs` through `requestHelper.handleError`, and
/// `IncrementalAlterConfigsRequest.getErrorResponse` sets
/// `ApiError.message()` on every resource row: for a controller that is not
/// active, the message of `ControllerExceptions.newWrongControllerException`.
/// A guarded alter retries `UncommittedTail` until the deadline, so that row
/// answers `REQUEST_TIMED_OUT` whatever leader this node knows; the case
/// without a known leader is the same row and is left out.
#[yare::parameterized(
    not_leader = {
        || RaftError::NotLeader { current_leader: Some(NodeId(2)) },
        row(codes::NOT_CONTROLLER, Some("The active controller appears to be node 2.")),
    },
    not_leader_without_hint = {
        || RaftError::NotLeader { current_leader: None },
        row(codes::NOT_CONTROLLER, Some("No controller appears to be active.")),
    },
    leader_unknown = {
        || RaftError::LeaderUnknown,
        row(codes::NOT_CONTROLLER, Some("No controller appears to be active.")),
    },
    uncommitted_tail = { || RaftError::UncommittedTail, row(codes::REQUEST_TIMED_OUT, None) },
    shutdown = { || RaftError::Shutdown, row(codes::UNKNOWN_SERVER_ERROR, None) },
)]
#[test_macro(tokio::test(start_paused = true))]
async fn a_failed_alter_carries_kafkas_not_controller_message(
    error: fn() -> RaftError,
    expected: AlterConfigsResourceResponse,
) {
    let resource = AlterConfigsResource {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: "orders".to_owned(),
        ..Default::default()
    };
    let answered =
        guarded_resource_outcome(guarded_failure(error).await, "IncrementalAlterConfigs")
            .unwrap_or_else(|refusal| resource_refusal(&resource, refusal));
    check!(answered == expected);
}

/// The row of topic `orders` with `error_code` and `error_message`.
fn row(error_code: i16, error_message: Option<&str>) -> AlterConfigsResourceResponse {
    AlterConfigsResourceResponse {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: "orders".to_owned(),
        error_code,
        error_message: error_message.map(str::to_owned),
        ..Default::default()
    }
}
