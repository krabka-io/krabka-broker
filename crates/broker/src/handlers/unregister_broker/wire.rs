//! The `UnregisterBrokerResponse` shape that every outcome of the handler
//! takes, and the version-aware encode that puts it on the wire.
//!
//! KIP-631 gives the response no per-broker rows, so a success and a refusal
//! differ only in the top-level error code and the optional message. Neither
//! throttles. Field-for-field construction is the contract with the JVM
//! `AdminClient`, so it sits apart from the code that decides which code an
//! outcome gets.

use bytes::Bytes;
use krabka_protocol::owned::unregister_broker_response::UnregisterBrokerResponse;

use crate::error::BrokerError;

pub(super) fn response(error_code: i16, error_message: Option<String>) -> UnregisterBrokerResponse {
    UnregisterBrokerResponse {
        error_code,
        error_message,
        ..Default::default()
    }
}

pub(super) fn encode_resp(
    version: i16,
    resp: &UnregisterBrokerResponse,
) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(resp, version)
}
