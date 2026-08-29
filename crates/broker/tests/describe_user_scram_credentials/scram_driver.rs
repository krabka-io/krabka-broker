//! The typed driver for `DescribeUserScramCredentials` (`api_key` 50): it
//! authenticates, encodes the request, and flattens the response into the
//! per-user rows the tests assert on.

use std::net::SocketAddr;

use bytes::BytesMut;
use krabka_protocol::{Decode, Encode};

use crate::scram_wire::{round_trip, sasl_plain_authenticate};

/// Drives `DescribeUserScramCredentials` (`api_key=50`) over a SASL/PLAIN
/// connection.
///
/// Returns `(top_level_error, per_user_rows)` where each row is
/// `(user, error_code, credential_infos)` and each `credential_info` is
/// `(mechanism, iterations)`.
pub(crate) async fn drive_describe_user_scram_credentials_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    users_filter: Option<Vec<String>>,
) -> (i16, Vec<(String, i16, Vec<(i8, i32)>)>) {
    use krabka_protocol::owned::{
        describe_user_scram_credentials_request::{DescribeUserScramCredentialsRequest, UserName},
        describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse,
    };

    let req = DescribeUserScramCredentialsRequest {
        users: users_filter.map(|v| {
            v.into_iter()
                .map(|n| UserName {
                    name: n,
                    ..Default::default()
                })
                .collect()
        }),
        ..Default::default()
    };

    let mut stream = sasl_plain_authenticate(addr, user, pass.as_bytes())
        .await
        .expect("SASL authenticate for DescribeUserScramCredentials");

    let mut body = BytesMut::new();
    req.encode(&mut body, 0)
        .expect("encode DescribeUserScramCredentials");

    let resp_bytes = round_trip(&mut stream, 50, 0, 1, true, &body)
        .await
        .expect("DescribeUserScramCredentials round-trip");

    let mut cur: &[u8] = &resp_bytes;
    let resp = DescribeUserScramCredentialsResponse::decode(&mut cur, 0)
        .expect("decode DescribeUserScramCredentialsResponse");

    let per_user: Vec<_> = resp
        .results
        .into_iter()
        .map(|r| {
            let infos: Vec<(i8, i32)> = r
                .credential_infos
                .into_iter()
                .map(|c| (c.mechanism, c.iterations))
                .collect();
            (r.user, r.error_code, infos)
        })
        .collect();

    (resp.error_code, per_user)
}
