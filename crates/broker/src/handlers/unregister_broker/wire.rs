//! The `UnregisterBrokerResponse` shape that every outcome of the handler
//! takes, and the version-aware encode that puts it on the wire.
//!
//! KIP-631 gives the response no per-broker rows, so a success and a refusal
//! differ only in the top-level error code and the optional message. Neither
//! throttles. Field-for-field construction is the contract with the JVM
//! `AdminClient`, so it sits apart from the code that decides which code an
//! outcome gets.

use bytes::Bytes;
use krabka_metadata::NodeId;
use krabka_protocol::owned::unregister_broker_response::UnregisterBrokerResponse;

use crate::{error::BrokerError, handlers::forward_to_controller::wrong_controller_message};

pub(super) fn response(error_code: i16, error_message: Option<String>) -> UnregisterBrokerResponse {
    UnregisterBrokerResponse {
        error_code,
        error_message,
        ..Default::default()
    }
}

/// The refusal of a node that is not the active controller, or `None` when
/// `node` leads. `leader` is the node that this one believes leads.
///
/// Kafka's `ControllerWriteEvent.run` throws the wrong-controller exception
/// before it reads any state, and `UnregisterBrokerRequest.getErrorResponse`
/// copies its code and message.
pub(super) fn not_controller_refusal(
    leader: Option<NodeId>,
    node: NodeId,
) -> Option<UnregisterBrokerResponse> {
    (leader != Some(node)).then(|| {
        response(
            crate::codes::NOT_CONTROLLER,
            Some(wrong_controller_message(leader)),
        )
    })
}

pub(super) fn encode_resp(
    version: i16,
    resp: &UnregisterBrokerResponse,
) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}
