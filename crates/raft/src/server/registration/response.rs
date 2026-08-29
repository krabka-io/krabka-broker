//! Encoders for the two registration responses the controller returns.
//!
//! Every refusal path in the broker and controller registration handlers ends
//! in one of these, so the shape of a reply is decided in one place rather than
//! at each early return.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Encode,
    owned::{
        broker_registration_response::BrokerRegistrationResponse,
        controller_registration_response::ControllerRegistrationResponse,
    },
};

use crate::RaftError;

pub(super) fn broker_registration_response(
    version: i16,
    error_code: i16,
    broker_epoch: i64,
) -> Result<Bytes, RaftError> {
    encode(
        &BrokerRegistrationResponse {
            error_code,
            broker_epoch,
            ..Default::default()
        },
        version,
    )
}

pub(super) fn controller_registration_response(
    version: i16,
    error_code: i16,
    error_message: Option<String>,
) -> Result<Bytes, RaftError> {
    encode(
        &ControllerRegistrationResponse {
            error_code,
            error_message,
            ..Default::default()
        },
        version,
    )
}

pub(super) fn encode(response: &impl Encode, version: i16) -> Result<Bytes, RaftError> {
    let mut bytes = BytesMut::new();
    response.encode(&mut bytes, version)?;
    Ok(bytes.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::server::registration::{INVALID_REGISTRATION, NOT_CONTROLLER};

    /// The two registration responses carry the error code and the epoch or
    /// message the caller passed, and encode to bytes.
    ///
    /// These are one-line wrappers, which is exactly why nothing tested them:
    /// swapping a field or dropping the encode leaves a function that still
    /// returns `Ok`.
    #[test]
    fn registration_responses_carry_what_they_were_given() {
        use krabka_protocol::{
            Decode as _,
            owned::{broker_registration_response, controller_registration_response},
        };

        let bytes = broker_registration_response(
            broker_registration_response::MAX_VERSION,
            INVALID_REGISTRATION,
            42,
        )
        .expect("encode broker response");
        let mut cursor = &bytes[..];
        let decoded = BrokerRegistrationResponse::decode(
            &mut cursor,
            broker_registration_response::MAX_VERSION,
        )
        .expect("decode broker response");
        check!((decoded.error_code, decoded.broker_epoch) == (INVALID_REGISTRATION, 42));

        let bytes = controller_registration_response(
            controller_registration_response::MAX_VERSION,
            NOT_CONTROLLER,
            Some("not the controller".to_owned()),
        )
        .expect("encode controller response");
        let mut cursor = &bytes[..];
        let decoded = ControllerRegistrationResponse::decode(
            &mut cursor,
            controller_registration_response::MAX_VERSION,
        )
        .expect("decode controller response");
        check!(decoded.error_code == NOT_CONTROLLER);
        check!(decoded.error_message.as_deref() == Some("not the controller"));
    }
}
