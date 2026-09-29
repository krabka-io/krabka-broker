//! The broker-listener half of `UnregisterController`: `KafkaApis` answers it
//! with `forwardToController`, so this module wraps the request in a KIP-590
//! `Envelope` under the caller's own principal and address, sends it to the
//! active controller, and unwraps the answer.
//!
//! The failure answers are `ForwardingManagerImpl`'s. An `Envelope` refused
//! with any code but `UNSUPPORTED_VERSION` becomes `UNKNOWN_SERVER_ERROR`, an
//! `UNSUPPORTED_VERSION` closes the client connection, and a controller that
//! cannot be reached becomes the `REQUEST_TIMED_OUT` of the forwarding
//! timeout.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        envelope_request,
        envelope_response::{self, EnvelopeResponse},
        unregister_controller_request::API_KEY,
    },
};

use crate::{
    broker::Broker,
    codes,
    envelope::{self, ForwardedPrincipal, ForwardedRequest},
    error::BrokerError,
    handlers::RequestContext,
};

/// `Errors.UNKNOWN_SERVER_ERROR.message()`, which the
/// `UnknownServerException` `ForwardingManagerImpl` builds carries.
const UNKNOWN_SERVER_ERROR_MESSAGE: &str =
    "The server experienced an unexpected error when processing the request.";

/// Forwards the request to the active controller and returns the answer, or
/// `None` when this node is the active controller and answers it itself.
pub(super) async fn to_active_controller(
    broker: &Broker,
    req_bytes: &[u8],
    version: i16,
    ctx: &RequestContext<'_>,
) -> Option<Result<Bytes, BrokerError>> {
    let body = match build(broker, req_bytes, version, ctx) {
        Ok(body) => body,
        Err(error) => return Some(Err(error)),
    };
    let forwarded = broker
        .controller
        .forward_raw(
            envelope_request::API_KEY,
            envelope_request::MIN_VERSION,
            body,
        )
        .await?;
    Some(match forwarded {
        Ok(envelope_response) => unwrap(broker, &envelope_response, version),
        Err(_) => super::encode(version, codes::REQUEST_TIMED_OUT, None),
    })
}

fn build(
    broker: &Broker,
    req_bytes: &[u8],
    version: i16,
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let request_data = envelope::wrap_request(&ForwardedRequest {
        api_key: API_KEY,
        api_version: version,
        correlation_id: 0,
        client_id: ctx.client_id.map(ToOwned::to_owned),
        body: Bytes::copy_from_slice(req_bytes),
        body_flexible: broker.handlers().body_flexible(API_KEY, version),
    });
    let principal = ForwardedPrincipal {
        name: ctx.principal.name.clone(),
        token_authenticated: false,
    };
    let request = envelope::envelope_request(request_data, &principal, ctx.peer.ip())?;
    let mut out = BytesMut::with_capacity(request.encoded_len(envelope_request::MIN_VERSION));
    request.encode(&mut out, envelope_request::MIN_VERSION)?;
    Ok(out.freeze())
}

fn unwrap(broker: &Broker, envelope_bytes: &[u8], version: i16) -> Result<Bytes, BrokerError> {
    let mut cur = envelope_bytes;
    let resp = EnvelopeResponse::decode(&mut cur, envelope_response::MIN_VERSION)?;
    if resp.error_code == codes::UNSUPPORTED_VERSION {
        return Err(BrokerError::UnsupportedApi {
            api_key: API_KEY,
            version,
        });
    }
    if resp.error_code != codes::NONE {
        return super::encode(
            version,
            codes::UNKNOWN_SERVER_ERROR,
            Some(UNKNOWN_SERVER_ERROR_MESSAGE),
        );
    }
    let flexible = broker.handlers().body_flexible(API_KEY, version);
    let data = resp.response_data.unwrap_or_default();
    let (_, body) = envelope::unwrap_response(API_KEY, flexible, &data)?;
    Ok(body)
}
