//! Kafka's `AbstractRequest.getErrorResponse`: the response that a request
//! gets when the broker refuses all of it before any handler reads it.
//!
//! Every Kafka request class overrides `getErrorResponse(throttleTimeMs, e)`.
//! Some answer with one top-level code. Others repeat the request's topics,
//! partitions, groups or resources, each row carrying the code. This module
//! ports each override for every api key that Kafka 4.3.1 parses, and it
//! encodes the response at the request's version. A caller passes the decoded
//! failure as a [`RequestFailure`], because the overrides read the exception
//! in three different ways.
//!
//! `getErrorResponse` builds no response in two cases. It returns `null` for
//! a `Produce` with `acks = 0`, and it throws a `NullPointerException` for a
//! `DescribeUserScramCredentials` with a null user list.
//! [`error_response_body`] returns `Ok(None)` for both. A body that does not decode at the requested
//! version, or an api key that Kafka 4.3.1 does not parse, is an error, as
//! Kafka's `RequestContext.parseRequest` throws `InvalidRequestException`
//! before any `getErrorResponse` runs.
//!
//! Three overrides fill rows from a Java `HashMap`: `Produce`,
//! `WriteTxnMarkers` and `AlterUserScramCredentials`. `HashMap` iteration
//! order has no stable meaning on the wire. The first two keep the request's
//! order here, and they merge repeated entries the way the map does. The
//! third sorts as Kafka sorts.

/// Each item of `$items` as a `$row` with the fields given and every other
/// field defaulted: the topic, partition and resource rows that the overrides
/// repeat from a request.
macro_rules! rows {
    ($items:expr, |$item:pat_param| $row:ident { $($field:ident $(: $value:expr)?),* $(,)? }) => {
        ($items)
            .into_iter()
            .map(|$item| $row { $($field $(: $value)?,)* ..Default::default() })
            .collect()
    };
    // rustfmt wraps a long row in a block.
    ($items:expr, |$item:pat_param| { $row:ident { $($body:tt)* } }) => {
        rows!($items, |$item| $row { $($body)* })
    };
}

mod admin;
mod client;
mod control;
mod groups;
#[cfg(test)]
mod tests;
mod txn;

use {
    super::{ApiKeyCode, ApiVersion, decode_request, encode_response},
    crate::{codes::UNKNOWN_SERVER_ERROR, error::BrokerError},
    bytes::Bytes,
    krabka_protocol::{Decode, Encode, ProtocolError, api_key::ApiKey},
};

/// The exception that a request is refused with, as Kafka's
/// `getErrorResponse` overrides read it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestFailure<'a> {
    /// `Errors.forException(e).code()`.
    pub(crate) error_code: i16,
    /// `Errors.message()`: the code's own default message.
    pub(crate) default_message: &'a str,
    /// `Throwable.getMessage()`.
    pub(crate) exception_message: &'a str,
}

impl RequestFailure<'_> {
    /// `Errors.forException(e).message()`, which some overrides put on the
    /// response.
    fn error_message(&self) -> String { self.default_message.to_owned() }

    /// `ApiError.fromThrowable(e).message()`: the exception's message, except
    /// that `UNKNOWN_SERVER_ERROR` hides it and a message equal to the
    /// code's default message is dropped.
    fn api_message(&self) -> Option<String> {
        (self.error_code != UNKNOWN_SERVER_ERROR && self.exception_message != self.default_message)
            .then(|| self.exception_message.to_owned())
    }

    /// `e.getMessage()`, which `UnregisterBroker` and
    /// `DescribeShareGroupOffsets` copy as it is.
    fn exception_message(&self) -> String { self.exception_message.to_owned() }
}

/// What [`error_response_body`] answers: the encoded response body, or `None`
/// where Kafka's `getErrorResponse` builds no response.
pub(crate) type ErrorAnswer = Result<Option<Bytes>, BrokerError>;

/// Decodes the request body of `api_key` at `version`, and encodes the
/// response body that Kafka's `getErrorResponse` builds for it from
/// `failure`, at the same version.
///
/// # Errors
///
/// Fails when `body` does not decode at `version`, when Kafka 4.3.1 does not
/// parse `api_key`, or when the response does not encode.
pub(crate) fn error_response_body(
    api_key: ApiKeyCode,
    version: ApiVersion,
    body: &[u8],
    failure: &RequestFailure<'_>,
) -> ErrorAnswer {
    let request = Request {
        version,
        body,
        failure,
    };
    ApiKey::from_i16(api_key)
        .and_then(|api| {
            client::answer(api, request)
                .or_else(|| groups::answer(api, request))
                .or_else(|| txn::answer(api, request))
                .or_else(|| admin::answer(api, request))
                .or_else(|| control::answer(api, request))
        })
        .unwrap_or(Err(BrokerError::Protocol(ProtocolError::InvalidValue(
            "api key has no Kafka 4.3.1 request to answer",
        ))))
}

/// One refused request: its version, its undecoded body and the failure that
/// the response carries.
#[derive(Clone, Copy)]
struct Request<'a> {
    version: ApiVersion,
    body: &'a [u8],
    failure: &'a RequestFailure<'a>,
}

impl Request<'_> {
    /// Decodes the body as `Req` and encodes the response that `build` makes
    /// of it.
    fn respond<Req, Resp>(self, build: impl FnOnce(Req) -> Resp) -> ErrorAnswer
    where
        Req: for<'de> Decode<'de>,
        Resp: Encode,
    {
        self.maybe_respond(|request| Some(build(request)))
    }

    /// As [`Self::respond`], for an override that may build no response.
    fn maybe_respond<Req, Resp>(self, build: impl FnOnce(Req) -> Option<Resp>) -> ErrorAnswer
    where
        Req: for<'de> Decode<'de>,
        Resp: Encode,
    {
        let request: Req = decode_request(self.body, self.version)?;
        build(request)
            .map(|response| encode_response(&response, self.version))
            .transpose()
    }

    /// The failure's `error_code`.
    fn code(self) -> i16 { self.failure.error_code }
}
