//! Kafka broker/controller lifecycle RPCs served on the controller listener.
//!
//! This root routes an `api_key` to the handler that answers it, and owns what
//! all three handlers share: the Kafka error codes these APIs reply with, the
//! declared API/version table, the leadership guard, and the mapping from a
//! [`RaftError`] to the code a client acts on. Each RPC has its own submodule,
//! with the listener grammar in `listeners` and the response encoders in
//! `response`.

use bytes::Bytes;
use krabka_protocol::owned::{
    broker_heartbeat_request, broker_registration_request, controller_registration_request,
};

mod broker;
mod controller;
mod heartbeat;
mod listeners;
mod response;

use self::{
    broker::broker_registration, controller::controller_registration, heartbeat::broker_heartbeat,
};
use crate::{RaftError, kraft::KraftController};

const SUCCESS: i16 = 0;
const UNKNOWN_SERVER_ERROR: i16 = -1;
const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
const UNSUPPORTED_VERSION: i16 = 35;
const NOT_CONTROLLER: i16 = 41;
const STALE_BROKER_EPOCH: i16 = 77;
const DUPLICATE_BROKER_REGISTRATION: i16 = 101;
const BROKER_ID_NOT_REGISTERED: i16 = 102;
const INCONSISTENT_CLUSTER_ID: i16 = 104;
const UNKNOWN_CONTROLLER_ID: i16 = 116;
const INVALID_REGISTRATION: i16 = 119;

pub(super) const SUPPORTED_APIS: [(i16, i16); 3] = [
    (
        broker_registration_request::API_KEY,
        broker_registration_request::MAX_VERSION,
    ),
    (
        broker_heartbeat_request::API_KEY,
        broker_heartbeat_request::MAX_VERSION,
    ),
    (
        controller_registration_request::API_KEY,
        controller_registration_request::MAX_VERSION,
    ),
];

pub(super) fn is_controller_api(api_key: i16) -> bool {
    SUPPORTED_APIS.iter().any(|&(key, _)| key == api_key)
}

pub(super) async fn dispatch(
    api_key: i16,
    version: i16,
    body: &[u8],
    engine: &KraftController,
    authorized: bool,
) -> Result<Bytes, RaftError> {
    match api_key {
        broker_registration_request::API_KEY => {
            broker_registration(version, body, engine, authorized).await
        }
        broker_heartbeat_request::API_KEY => broker_heartbeat(version, body, engine, authorized),
        controller_registration_request::API_KEY => {
            controller_registration(version, body, engine, authorized).await
        }
        _ => Err(RaftError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue("unknown controller lifecycle API"),
        )),
    }
}

fn is_leader(engine: &KraftController) -> bool {
    engine.watch_leader().borrow().as_ref() == Some(&engine.node_id())
}

fn raft_error_code(error: &RaftError) -> i16 {
    match error {
        RaftError::NotLeader { .. } | RaftError::LeaderUnknown => NOT_CONTROLLER,
        RaftError::Metadata(_) | RaftError::ChangeRejected(_) => INVALID_REGISTRATION,
        _ => UNKNOWN_SERVER_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// The controller answers the lifecycle APIs it declares and nothing else.
    #[test]
    fn only_the_declared_apis_are_controller_apis() {
        for &(key, _) in &SUPPORTED_APIS {
            check!(is_controller_api(key), "declared api {key}");
        }
        // A key nothing declares: Produce is a broker API, never a controller one.
        check!(!is_controller_api(0), "Produce is not a controller api");
        check!(!is_controller_api(i16::MAX));
    }

    /// Each raft failure maps to the error code a Kafka client acts on: a
    /// leadership problem tells it to look elsewhere, a rejected registration
    /// tells it not to retry unchanged.
    #[test]
    fn raft_errors_map_to_the_client_visible_code() {
        check!(raft_error_code(&RaftError::LeaderUnknown) == NOT_CONTROLLER);
        check!(raft_error_code(&RaftError::ChangeRejected("no".into())) == INVALID_REGISTRATION,);
        // Anything else is not something the client can act on specifically.
        check!(raft_error_code(&RaftError::Shutdown) == UNKNOWN_SERVER_ERROR);
    }

    #[test]
    fn lifecycle_api_table_matches_generated_schemas() {
        assert2::assert!(SUPPORTED_APIS == [(62, 4), (63, 2), (70, 0)]);
    }
}
