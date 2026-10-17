//! Each api's error response, compared whole against the one its Kafka
//! `getErrorResponse` override builds from the same request.
//!
//! The control and group tables live in child modules of `control` and
//! `groups`, so they share those modules' imports. They run on the helpers
//! here.

mod admin;
mod client;
mod txn;

use {
    super::{RequestFailure, error_response_body},
    crate::codes::{ILLEGAL_SASL_STATE, UNKNOWN_SERVER_ERROR},
    assert2::assert,
    bytes::BytesMut,
    krabka_protocol::{
        Decode, Encode,
        api_key::ApiKey,
        owned::{
            describe_acls_request::DescribeAclsRequest,
            describe_acls_response::DescribeAclsResponse,
            describe_share_group_offsets_request::{
                DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsRequestGroup,
            },
            describe_share_group_offsets_response::{
                DescribeShareGroupOffsetsResponse, DescribeShareGroupOffsetsResponseGroup,
            },
            heartbeat_request::HeartbeatRequest,
            unregister_broker_request::UnregisterBrokerRequest,
            unregister_broker_response::UnregisterBrokerResponse,
        },
        primitives::uuid::Uuid,
    },
    std::fmt::Debug,
};

/// `Errors.ILLEGAL_SASL_STATE.message()`.
const DEFAULT_MESSAGE: &str = "Request is not valid given the current SASL state.";

/// The `IllegalSaslStateException` message, as the authenticator words it.
const EXCEPTION_MESSAGE: &str = "Unexpected Kafka request of type TEST during SASL authentication.";

/// The failure every table row is refused with.
const FAILURE: RequestFailure<'static> = RequestFailure {
    error_code: ILLEGAL_SASL_STATE,
    default_message: DEFAULT_MESSAGE,
    exception_message: EXCEPTION_MESSAGE,
};

/// A topic id that is not the zero id, so a dropped id shows.
pub(super) const TOPIC_ID: Uuid = Uuid([7; 16]);

/// `ApiError.fromThrowable(e).message()` for [`FAILURE`].
fn exception_message() -> String { EXCEPTION_MESSAGE.to_owned() }

/// `Errors.message()` for [`FAILURE`].
pub(super) fn default_message() -> String { DEFAULT_MESSAGE.to_owned() }

/// One table row, which asserts on its own answer when run.
type Case = Box<dyn Fn()>;

/// The row for `request` of `api` at `version`, which expects `expected`
/// back: the decoded response, or `None` for no response.
pub(super) fn case<Req, Resp>(
    api: ApiKey,
    version: i16,
    request: Req,
    expected: Option<Resp>,
) -> Case
where
    Req: Encode + 'static,
    Resp: for<'de> Decode<'de> + PartialEq + Debug + 'static,
{
    Box::new(move || {
        let actual: Option<Resp> = answer(api, version, &request, &FAILURE);
        assert!(actual == expected, "{api:?} v{version}");
    })
}

/// Runs every row.
pub(super) fn check(cases: Vec<Case>) {
    for case in cases {
        case();
    }
}

/// Encodes `request`, answers it as `api` at `version` with `failure`, and
/// decodes the response.
fn answer<Req, Resp>(
    api: ApiKey,
    version: i16,
    request: &Req,
    failure: &RequestFailure<'_>,
) -> Option<Resp>
where
    Req: Encode,
    Resp: for<'de> Decode<'de>,
{
    let mut body = BytesMut::new();
    request
        .encode(&mut body, version)
        .expect("the request encodes");
    error_response_body(api as i16, version, &body, failure)
        .expect("the request is answered")
        .map(|bytes| {
            let mut cursor = &bytes[..];
            let response = Resp::decode(&mut cursor, version).expect("the response decodes");
            assert!(cursor.is_empty(), "{api:?} v{version} left bytes over");
            response
        })
}

/// `ApiError.fromThrowable` drops the message for `UNKNOWN_SERVER_ERROR` and
/// for a message equal to the code's own, and keeps any other.
#[test]
fn api_error_message_follows_api_error_from_throwable() {
    let cases = [
        ("an exception message", FAILURE, Some(exception_message())),
        (
            "the default message",
            RequestFailure {
                exception_message: DEFAULT_MESSAGE,
                ..FAILURE
            },
            None,
        ),
        (
            "an unknown server error",
            RequestFailure {
                error_code: UNKNOWN_SERVER_ERROR,
                ..FAILURE
            },
            None,
        ),
    ];
    for (name, failure, error_message) in cases {
        let response: Option<DescribeAclsResponse> = answer(
            ApiKey::DescribeAcls,
            3,
            &DescribeAclsRequest::default(),
            &failure,
        );
        assert!(
            response
                == Some(DescribeAclsResponse {
                    error_code: failure.error_code,
                    error_message,
                    ..Default::default()
                }),
            "{name}"
        );
    }
}

/// `UnregisterBroker` copies `e.getMessage()` even where `ApiError` would drop
/// it.
#[test]
fn unregister_broker_copies_the_exception_message() {
    let failure = RequestFailure {
        exception_message: DEFAULT_MESSAGE,
        ..FAILURE
    };
    let response: Option<UnregisterBrokerResponse> = answer(
        ApiKey::UnregisterBroker,
        0,
        &UnregisterBrokerRequest::default(),
        &failure,
    );
    assert!(
        response
            == Some(UnregisterBrokerResponse {
                error_code: ILLEGAL_SASL_STATE,
                error_message: Some(default_message()),
                ..Default::default()
            })
    );
}

/// `DescribeShareGroupOffsetsResponse` puts the exception's message on each
/// group, and the code's default message for `UNKNOWN_SERVER_ERROR`.
#[test]
fn describe_share_group_offsets_hides_an_unknown_server_error() {
    let request = DescribeShareGroupOffsetsRequest {
        groups: vec![DescribeShareGroupOffsetsRequestGroup {
            group_id: "g".to_owned(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let cases = [
        (FAILURE, Some(exception_message())),
        (
            RequestFailure {
                error_code: UNKNOWN_SERVER_ERROR,
                ..FAILURE
            },
            Some(default_message()),
        ),
    ];
    for (failure, error_message) in cases {
        let response: Option<DescribeShareGroupOffsetsResponse> =
            answer(ApiKey::DescribeShareGroupOffsets, 1, &request, &failure);
        assert!(
            response
                == Some(DescribeShareGroupOffsetsResponse {
                    groups: vec![DescribeShareGroupOffsetsResponseGroup {
                        group_id: "g".to_owned(),
                        error_code: failure.error_code,
                        error_message,
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
            "code {}",
            failure.error_code
        );
    }
}

/// A request Kafka 4.3.1 cannot parse has no error response: an api key that
/// release lacks, or a body that does not decode at its version.
#[test]
fn an_unparsable_request_is_an_error() {
    let mut heartbeat = BytesMut::new();
    HeartbeatRequest::default()
        .encode(&mut heartbeat, 4)
        .expect("the request encodes");
    let cases: [(&str, i16, i16, &[u8]); 4] = [
        ("an unknown api key", 9_999, 0, &[]),
        (
            "an api key Kafka 4.3.1 lacks",
            ApiKey::GetReplicaLogInfo as i16,
            0,
            &[],
        ),
        (
            "a truncated body",
            ApiKey::Heartbeat as i16,
            4,
            &heartbeat[..2],
        ),
        (
            "an unknown version",
            ApiKey::Heartbeat as i16,
            99,
            &heartbeat,
        ),
    ];
    for (name, api_key, version, body) in cases {
        assert!(
            error_response_body(api_key, version, body, &FAILURE).is_err(),
            "{name}"
        );
    }
}
