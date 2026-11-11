//! Step definitions for the message that `ApiError.fromThrowable` keeps on a
//! whole-request refusal.

use {
    super::{
        DEFAULT_MESSAGE, EXCEPTION_MESSAGE, FAILURE, ILLEGAL_SASL_STATE, RequestFailure,
        UNKNOWN_SERVER_ERROR, answer, exception_message,
    },
    assert2::assert,
    krabka_protocol::{
        api_key::ApiKey,
        owned::{
            describe_acls_request::DescribeAclsRequest,
            describe_acls_response::DescribeAclsResponse,
        },
    },
};

/// The code a scenario refuses the request with.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum Code {
    #[step_param("ILLEGAL_SASL_STATE")]
    IllegalSaslState,
    #[step_param("UNKNOWN_SERVER_ERROR")]
    UnknownServerError,
}

impl Code {
    const fn value(self) -> i16 {
        match self {
            Self::IllegalSaslState => ILLEGAL_SASL_STATE,
            Self::UnknownServerError => UNKNOWN_SERVER_ERROR,
        }
    }
}

/// The message the refusing exception carries.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum Thrown {
    #[step_param("its own message")]
    OwnMessage,
    #[step_param("the code's default message")]
    DefaultMessage,
}

struct RefusedRequest {
    failure: krabka_bdd::Given<RequestFailure<'static>>,
    response: krabka_bdd::Given<Option<DescribeAclsResponse>>,
}

#[rstest::fixture]
fn refused_request() -> RefusedRequest {
    RefusedRequest {
        failure: krabka_bdd::Given::new(
            "failure",
            "Given a request is refused with <code> carrying <message>",
        ),
        response: krabka_bdd::Given::new(
            "response",
            "When the broker answers a DescribeAcls request with that refusal",
        ),
    }
}

#[krabka_bdd::given("a request is refused with {code} carrying {message}")]
fn refused_with(refused_request: &mut RefusedRequest, code: Code, message: Thrown) {
    refused_request.failure.set(RequestFailure {
        error_code: code.value(),
        exception_message: match message {
            Thrown::OwnMessage => EXCEPTION_MESSAGE,
            Thrown::DefaultMessage => DEFAULT_MESSAGE,
        },
        ..FAILURE
    });
}

#[krabka_bdd::when("the broker answers a DescribeAcls request with that refusal")]
fn answers_describe_acls(refused_request: &mut RefusedRequest) {
    let response: Option<DescribeAclsResponse> = answer(
        ApiKey::DescribeAcls,
        3,
        &DescribeAclsRequest::default(),
        refused_request.failure.get(),
    );
    refused_request.response.set(response);
}

/// The response is the whole refusal: `code` and `error_message`, and nothing
/// else.
fn assert_refusal(refused_request: &RefusedRequest, code: Code, error_message: Option<String>) {
    assert!(
        *refused_request.response.get()
            == Some(DescribeAclsResponse {
                error_code: code.value(),
                error_message,
                ..Default::default()
            })
    );
}

#[krabka_bdd::then("the DescribeAcls response carries {code} with the exception's own message")]
fn keeps_own_message(refused_request: &RefusedRequest, code: Code) {
    assert_refusal(refused_request, code, Some(exception_message()));
}

#[krabka_bdd::then("the DescribeAcls response carries {code} with no message")]
fn drops_message(refused_request: &RefusedRequest, code: Code) {
    assert_refusal(refused_request, code, None);
}

krabka_bdd::scenarios!(
    "tests/features/migrated/src/handlers/request_error",
    fixtures = [refused_request: RefusedRequest],
);
