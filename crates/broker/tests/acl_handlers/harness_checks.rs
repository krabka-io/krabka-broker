//! Checks on the raw-wire helpers the ACL scenarios rely on.
//!
//! A helper that swallows a mismatch makes every suite built on it blind to
//! that mismatch, so these pin the failures the helpers must report.
use {
    crate::{
        kafka_wire::round_trip,
        support::wire::{decode_response_frame, read_frame, write_frame},
    },
    assert2::assert,
    krabka_protocol::owned::api_versions_response::ApiVersionsResponse,
    std::io,
    tokio::io::{DuplexStream, duplex},
};

/// Answer one request on `server` with a bare response header for `correlation`.
async fn answer_with_correlation(server: &mut DuplexStream, correlation: i32) {
    let _request = read_frame(server).await.expect("read request");
    write_frame(server, &correlation.to_be_bytes(), None)
        .await
        .expect("write response");
}

#[tokio::test]
async fn round_trip_rejects_a_response_for_another_correlation() {
    let (mut client, mut server) = duplex(1024);
    let broker = tokio::spawn(async move { answer_with_correlation(&mut server, 8).await });

    let result = round_trip()
        .stream(&mut client)
        .correlation(7)
        .body(&[])
        .call()
        .await;
    broker.await.expect("answer task");

    let error = result.expect_err("a response for correlation 8 must not answer request 7");
    assert!(error.to_string() == "response correlation id 8 does not match request 7");
}

#[test]
fn decode_response_frame_rejects_a_response_for_another_correlation() {
    let result: io::Result<ApiVersionsResponse> =
        decode_response_frame(&8_i32.to_be_bytes(), 7, 0, false, "ApiVersions decode");

    let error = result.expect_err("a response for correlation 8 must not answer request 7");
    assert!(error.to_string() == "response correlation id 8 does not match request 7");
}

#[test]
fn decode_response_frame_reports_a_truncated_header() {
    let result: io::Result<ApiVersionsResponse> =
        decode_response_frame(&[0, 0], 7, 0, false, "ApiVersions decode");

    let error = result.expect_err("two bytes cannot hold a correlation id");
    assert!(error.to_string() == "response missing correlation id");
}
