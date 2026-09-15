//! The encoder for the controller registration response.
//!
//! Every refusal path in the controller registration handler ends in it, so the
//! shape of a reply is decided in one place rather than at each early return.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{
    Encode, owned::controller_registration_response::ControllerRegistrationResponse,
};

use crate::RaftError;

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

fn encode(response: &impl Encode, version: i16) -> Result<Bytes, RaftError> {
    let mut bytes = BytesMut::new();
    response.encode(&mut bytes, version)?;
    Ok(bytes.freeze())
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::server::registration::NOT_CONTROLLER;

    /// The controller registration response carries the error code and the
    /// message the caller passed, and encodes to bytes.
    ///
    /// This is a one-line wrapper, which is exactly why nothing tested it:
    /// swapping a field or dropping the encode leaves a function that still
    /// returns `Ok`.
    #[test]
    fn the_registration_response_carries_what_it_was_given() {
        use krabka_protocol::{Decode as _, owned::controller_registration_response};

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
