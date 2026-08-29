//! Shaping the `FindCoordinatorResponse` from the resolved coordinator rows.
//!
//! The response has two forms on the wire. Versions 0 through 3 carry a single
//! coordinator in the top-level `node_id`, `host`, `port`, `error_code` and
//! `error_message` fields; version 4 and later carry the per-key `coordinators`
//! array. This module fills both from one row list, so a v0-v3 client reads the
//! first row out of the top-level fields while a v4+ client reads the array.

use bytes::Bytes;
use krabka_protocol::owned::find_coordinator_response::{Coordinator, FindCoordinatorResponse};

use crate::{codes, error::BrokerError, handlers::parse_advertised_host_port as parse_host_port};

pub(super) fn encode_coordinators(
    broker_id: i32,
    advertised: &str,
    version: i16,
    coordinators: Vec<Coordinator>,
) -> Result<Bytes, BrokerError> {
    let (node_id, host, port, error_code, error_message) = coordinators.first().map_or_else(
        || {
            let (host, port) = parse_host_port(advertised);
            (broker_id, host, i32::from(port), codes::NONE, None)
        },
        |first| {
            (
                first.node_id,
                first.host.clone(),
                first.port,
                first.error_code,
                first.error_message.clone(),
            )
        },
    );
    crate::handlers::encode_response(
        &FindCoordinatorResponse {
            throttle_time_ms: 0,
            error_code,
            error_message,
            node_id,
            host,
            port,
            coordinators,
            ..Default::default()
        },
        version,
    )
}

/// Build an error response in which all coordinators carry the given error code.
///
/// The handler uses this function when a top-level failure, for example a
/// bootstrap failure, prevents the per-key lookup.
pub(super) fn encode_error_response(
    broker_id: i32,
    advertised: &str,
    version: i16,
    error_code: i16,
    error_message: Option<&str>,
) -> Result<Bytes, BrokerError> {
    let (host, port) = parse_host_port(advertised);
    let resp = FindCoordinatorResponse {
        throttle_time_ms: 0,
        error_code,
        error_message: error_message.map(str::to_owned),
        node_id: broker_id,
        host,
        port: i32::from(port),
        coordinators: vec![],
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}
