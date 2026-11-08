//! Caller-level command exits are an executable contract.
#![cfg(test)]

use {
    assert2::assert,
    std::{
        cell::Cell,
        future::{Ready, ready},
    },
};

#[derive(Clone, Copy)]
enum ReplyMode {
    Success,
    Refusal,
    TransportFailure,
}
#[derive(Clone, Copy)]
struct Request {
    mode: ReplyMode,
}
struct Reply {
    error_code: i16,
    error_message: Option<String>,
    value: i32,
}
struct Client {
    sends: Cell<usize>,
}
#[derive(Debug, PartialEq, Eq)]
struct Failure;
struct TransportFailure;
impl From<TransportFailure> for Failure {
    fn from(_: TransportFailure) -> Self { Self }
}
impl Client {
    fn send(&self, request: Request) -> Ready<Result<Reply, TransportFailure>> {
        self.sends.set(self.sends.get() + 1);
        ready(match request.mode {
            ReplyMode::Success => Ok(Reply {
                error_code: 0,
                error_message: None,
                value: 42,
            }),
            ReplyMode::Refusal => Ok(Reply {
                error_code: 31,
                error_message: Some("denied".into()),
                value: 42,
            }),
            ReplyMode::TransportFailure => Err(TransportFailure),
        })
    }
}
fn report_error(err: &mut Vec<&'static str>, code: i16, message: Option<&str>) -> i32 {
    assert!(code == 31 && message == Some("denied"));
    err.push("refused");
    -i32::from(code)
}
async fn command(
    client: &Client,
    request: Request,
    continued: &Cell<bool>,
    err: &mut Vec<&'static str>,
) -> Result<i32, Failure> {
    let reply = krabka_macros::guard_response!(client, request, err);
    continued.set(true);
    Ok(reply.value)
}

#[yare::parameterized(
    success = { ReplyMode::Success, Ok(42), true, &[] },
    refusal = { ReplyMode::Refusal, Ok(-31), false, &["refused"] },
    transport_failure = { ReplyMode::TransportFailure, Err(Failure), false, &[] },
)]
#[test_macro(tokio::test)]
async fn command_errors_return_from_the_caller_before_success_interpretation(
    mode: ReplyMode,
    expected: Result<i32, Failure>,
    continues: bool,
    reported: &[&str],
) {
    let client = Client {
        sends: Cell::new(0),
    };
    let continued = Cell::new(false);
    let mut err = Vec::new();
    let result = command(&client, Request { mode }, &continued, &mut err).await;
    assert2::assert!(
        result == expected
            && continued.get() == continues
            && client.sends.get() == 1
            && err == reported
    );
}
