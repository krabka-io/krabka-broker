//! The row of an `AlterConfigs` or `IncrementalAlterConfigs` resource whose
//! guarded alter gave up, for each controller failure that ends the retry.

use {
    super::{RESOURCE_TYPE_TOPIC, guarded_resource_outcome, resource_refusal},
    crate::{
        codes,
        handlers::submit_failure::test_support::{guarded_failure, guarded_not_controller_rows},
    },
    assert2::check,
    krabka_protocol::owned::{
        alter_configs_request, alter_configs_response, incremental_alter_configs_request,
        incremental_alter_configs_response,
    },
};

/// The rows of a resource whose guarded alter failed with a controller error,
/// in both config APIs, which share the guarded alter.
///
/// Kafka's `ControllerApis.handleLegacyAlterConfigs` and
/// `ControllerApis.handleIncrementalAlterConfigs` answer a failed write
/// through `requestHelper.handleError`. `AlterConfigsRequest.getErrorResponse`
/// and `IncrementalAlterConfigsRequest.getErrorResponse` set
/// `ApiError.message()` on every resource row: for a controller that is not
/// active, the message of `ControllerExceptions.newWrongControllerException`.
#[tokio::test(start_paused = true)]
async fn a_failed_alter_carries_kafkas_not_controller_message() {
    let legacy = alter_configs_request::AlterConfigsResource {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: "orders".to_owned(),
        ..Default::default()
    };
    let incremental = incremental_alter_configs_request::AlterConfigsResource {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: "orders".to_owned(),
        ..Default::default()
    };
    for case in guarded_not_controller_rows(codes::UNKNOWN_SERVER_ERROR) {
        let error_message = case.error_message.map(str::to_owned);
        let legacy_row: alter_configs_response::AlterConfigsResourceResponse =
            guarded_resource_outcome(guarded_failure(case.error).await, "AlterConfigs")
                .unwrap_or_else(|refusal| resource_refusal(&legacy, refusal));
        check!(
            legacy_row
                == alter_configs_response::AlterConfigsResourceResponse {
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: "orders".to_owned(),
                    error_code: case.error_code,
                    error_message: error_message.clone(),
                    ..Default::default()
                },
            "{}",
            case.name
        );
        let incremental_row: incremental_alter_configs_response::AlterConfigsResourceResponse =
            guarded_resource_outcome(guarded_failure(case.error).await, "IncrementalAlterConfigs")
                .unwrap_or_else(|refusal| resource_refusal(&incremental, refusal));
        check!(
            incremental_row
                == incremental_alter_configs_response::AlterConfigsResourceResponse {
                    resource_type: RESOURCE_TYPE_TOPIC,
                    resource_name: "orders".to_owned(),
                    error_code: case.error_code,
                    error_message,
                    ..Default::default()
                },
            "{}",
            case.name
        );
    }
}
