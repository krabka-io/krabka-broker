//! `ApiVersions` over a live controller-listener connection: the listener
//! answers Kafka's refusals behind a v0 response header, keeps the connection
//! open after each one, and gives the same bytes that the handshake gets for a
//! request before SASL authentication.

use assert2::check;
use krabka_protocol::{
    Decode, Encode,
    owned::{api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

use super::{
    ConnectionContext, ListenerApiVersions, handle_conn, test_support::single_voter_engine,
};
use crate::ControllerApiVersions as _;

/// A request frame with a v1 header (below v3) or a v2 header (v3 and above,
/// and every version the listener does not serve above that).
fn api_versions_frame(version: i16, correlation_id: i32, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&18i16.to_be_bytes());
    frame.extend_from_slice(&version.to_be_bytes());
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame.extend_from_slice(&1i16.to_be_bytes());
    frame.push(b'c');
    if version >= 3 {
        frame.push(0);
    }
    frame.extend_from_slice(body);
    let mut out = i32::try_from(frame.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(&frame);
    out
}

async fn read_frame(stream: &mut tokio::io::DuplexStream) -> Vec<u8> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.expect("response length");
    let mut frame = vec![0u8; usize::try_from(i32::from_be_bytes(len)).unwrap()];
    stream.read_exact(&mut frame).await.expect("response frame");
    frame
}

fn request_body(version: i16, name: &str) -> Vec<u8> {
    let mut body = bytes::BytesMut::new();
    ApiVersionsRequest {
        client_software_name: name.into(),
        client_software_version: "1.0".into(),
        ..Default::default()
    }
    .encode(&mut body, version)
    .expect("encode");
    body.to_vec()
}

/// One row per request, all on one connection, in order. Each row checks the
/// error code and the table size of the answer, and that the answer is the
/// one the handshake gives before authentication.
#[tokio::test]
async fn controller_listener_answers_api_versions_refusals_and_keeps_the_connection() {
    let (engine, _dir) = single_voter_engine();
    let listener_api_versions = ListenerApiVersions {
        engine: engine.clone(),
        admin_router: None,
    };
    let (mut client, server) = tokio::io::duplex(1 << 16);
    let shutdown = CancellationToken::new();
    let conn = tokio::spawn(handle_conn(
        server,
        engine,
        shutdown.clone(),
        None,
        None,
        ConnectionContext {
            peer: "127.0.0.1:9093".parse().unwrap(),
            principal: None,
            authenticated_via_token: false,
            cluster_alter_authorized: true,
        },
    ));

    // (version, request body, body version of the answer, error code, whether
    // the answer lists the full table)
    let rows: [(i16, Vec<u8>, i16, i16, bool); 7] = [
        (6, vec![0xff], 0, 35, false),
        (i16::MAX, vec![], 0, 35, false),
        (3, request_body(3, ""), 3, 42, false),
        (4, request_body(4, "a b"), 4, 42, false),
        (5, request_body(5, "krabka"), 5, 0, true),
        (4, request_body(4, "krabka"), 4, 0, true),
        (0, vec![], 0, 0, true),
    ];
    for (correlation_id, (version, body, body_version, error_code, full)) in (1..).zip(rows) {
        client
            .write_all(&api_versions_frame(version, correlation_id, &body))
            .await
            .expect("write request");
        let frame = read_frame(&mut client).await;
        check!(frame[..4] == correlation_id.to_be_bytes(), "v{version}");
        let expected = listener_api_versions
            .respond(version, &body)
            .expect("handshake answer");
        check!(
            frame[4..] == expected[..],
            "v{version}: pre-auth answer differs"
        );

        let response =
            ApiVersionsResponse::decode(&mut &frame[4..], body_version).expect("decode answer");
        check!(response.error_code == error_code, "v{version}");
        if full {
            check!(response.api_keys.len() > 1, "v{version}");
        } else if error_code == 35 {
            check!(
                response
                    .api_keys
                    .iter()
                    .map(|key| (key.api_key, key.min_version, key.max_version))
                    .collect::<Vec<_>>()
                    == vec![(18, 0, 5)],
                "v{version}"
            );
        } else {
            check!(response.api_keys.is_empty(), "v{version}");
        }
    }

    shutdown.cancel();
    assert2::assert!(conn.await.expect("connection task").is_ok());
}
