//! The Kafka registration RPC served by the controller listener itself.
//!
//! This root routes an `api_key` to the handler that answers it, and owns the
//! Kafka error codes it replies with, the declared API table, the leadership
//! guard, and the mapping from a [`RaftError`] to the code a client acts on.
//! The RPC has its own submodule, with the listener grammar in `listeners` and
//! the response encoder in `response`.
//!
//! `BrokerRegistration` and `BrokerHeartbeat` are deliberately absent. Their
//! answers are not a function of the metadata image alone. Registration refuses
//! a new incarnation only while the previous one holds a heartbeat session, and
//! it withdraws the ISR and ELR seats of a broker that cannot prove a clean
//! restart. The heartbeat drives the controller's heartbeat registry, the
//! KIP-112 offline-dir failover, and the controlled-shutdown drain. All of that
//! lives in the broker crate, so both reach the controller listener through the
//! KIP-919 Admin router like the other broker-owned APIs. There is exactly one
//! implementation of each, not a second one here that skips the bookkeeping.

use bytes::Bytes;
use krabka_protocol::owned::controller_registration_request;

mod controller;
mod listeners;
mod response;

use self::controller::controller_registration;
use crate::{RaftError, kraft::KraftController};

const SUCCESS: i16 = 0;
const UNKNOWN_SERVER_ERROR: i16 = -1;
const NOT_CONTROLLER: i16 = 41;
const UNKNOWN_CONTROLLER_ID: i16 = 116;
const INVALID_REGISTRATION: i16 = 119;

/// The lifecycle API keys this module answers. The versions they are served at
/// are declared once, with the rest of the listener's surface, in
/// [`super::api_versions::table`].
pub(super) const SUPPORTED_APIS: [i16; 1] = [controller_registration_request::API_KEY];

pub(super) fn is_controller_api(api_key: i16) -> bool {
    SUPPORTED_APIS.contains(&api_key)
}

pub(super) async fn dispatch(
    api_key: i16,
    version: i16,
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    match api_key {
        controller_registration_request::API_KEY => {
            controller_registration(version, body, engine).await
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
        for key in SUPPORTED_APIS {
            check!(is_controller_api(key), "declared api {key}");
        }
        // A key nothing declares: Produce is a broker API, never a controller one.
        check!(!is_controller_api(0), "Produce is not a controller api");
        check!(!is_controller_api(i16::MAX));
        // `BrokerRegistration` and `BrokerHeartbeat` are answered by the
        // broker's handlers through the Admin router, so this table must not
        // claim them.
        check!(
            !is_controller_api(krabka_protocol::owned::broker_registration_request::API_KEY),
            "BrokerRegistration belongs to the Admin router"
        );
        check!(
            !is_controller_api(krabka_protocol::owned::broker_heartbeat_request::API_KEY),
            "BrokerHeartbeat belongs to the Admin router"
        );
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

    /// The declared keys are the generated ones. The versions these are served
    /// at are asserted with the rest of the advertised table, in
    /// [`super::super::api_versions::table`].
    #[test]
    fn lifecycle_api_keys_match_generated_schemas() {
        assert2::assert!(SUPPORTED_APIS == [70]);
    }
}
