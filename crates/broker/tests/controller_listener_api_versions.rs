//! `ApiVersions` on a `SASL_PLAINTEXT` controller listener, before and after
//! authentication.
//!
//! Kafka's `SocketServer` gives `SaslServerAuthenticator` the same
//! `apiVersionSupplier` that `ControllerApis` answers from. So the answer before
//! authentication is the full controller-listener table with the features, and
//! an unsupported version gets `UNSUPPORTED_VERSION` without closing the
//! connection.

use std::net::SocketAddr;

use assert2::{assert, check};
use krabka_broker::{Broker, BrokerConfig, config::ListenerSpec};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::{ApiVersion, ApiVersionsResponse},
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_handshake_request::SaslHandshakeRequest,
    },
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// A request frame. `flexible` selects the v2 request header.
fn frame(api_key: i16, version: i16, correlation_id: i32, flexible: bool, body: &[u8]) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&api_key.to_be_bytes());
    frame.extend_from_slice(&version.to_be_bytes());
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame.extend_from_slice(&1i16.to_be_bytes());
    frame.push(b'c');
    if flexible {
        frame.push(0);
    }
    frame.extend_from_slice(body);
    let mut out = u32::try_from(frame.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(&frame);
    out
}

/// Sends one request and returns the response frame after the correlation id.
async fn round_trip(stream: &mut TcpStream, request: Vec<u8>, correlation_id: i32) -> Vec<u8> {
    stream.write_all(&request).await.expect("write request");
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.expect("response length");
    let mut response = vec![0u8; u32::from_be_bytes(len) as usize];
    stream
        .read_exact(&mut response)
        .await
        .expect("response frame");
    assert!(response[..4] == correlation_id.to_be_bytes());
    response.split_off(4)
}

fn encode<M: Encode>(message: &M, version: i16) -> Vec<u8> {
    let mut body = bytes::BytesMut::new();
    message.encode(&mut body, version).expect("encode");
    body.to_vec()
}

async fn api_versions(
    stream: &mut TcpStream,
    version: i16,
    correlation_id: i32,
) -> ApiVersionsResponse {
    let body = encode(
        &ApiVersionsRequest {
            client_software_name: "krabka-test".into(),
            client_software_version: "1.0".into(),
            ..Default::default()
        },
        version,
    );
    let response = round_trip(
        stream,
        frame(18, version, correlation_id, version >= 3, &body),
        correlation_id,
    )
    .await;
    ApiVersionsResponse::decode(&mut &response[..], version).expect("decode ApiVersions")
}

async fn start_sasl_controller() -> (krabka_broker::BrokerHandle, SocketAddr, TempDir) {
    let dir = TempDir::new().unwrap();
    let controller = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let controller_addr = controller.local_addr().unwrap();
    let data_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.controller_listen_addr = controller_addr;
    cfg.controller_quorum_voters = vec![(cfg.node_id, controller_addr.to_string())];
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: data_addr,
        advertised: data_addr.to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
        principal_mapper: krabka_broker::SslPrincipalMapper::default(),
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.controller_listener_protocol = ListenerProtocol::SaslPlaintext;
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    cfg.plain_credentials
        .insert("broker".to_string(), "secret".to_string());
    cfg.inter_broker_credentials = Some(krabka_broker::config::InterBrokerCredentials::Plain {
        username: "broker".to_string(),
        password: "secret".to_string(),
    });
    let broker = Broker::start_with_controller_listener(cfg, Some(controller))
        .await
        .expect("start broker");
    (broker, controller_addr, dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sasl_controller_listener_answers_api_versions_before_authentication() {
    let (broker, controller_addr, _dir) = start_sasl_controller().await;

    for version in [0, 3, 4, 5] {
        let mut stream = TcpStream::connect(controller_addr).await.expect("connect");

        // An unsupported version gets Kafka's v0 refusal, and the connection
        // stays open for the next request.
        let refusal = round_trip(&mut stream, frame(18, 6, 1, true, &[0xff]), 1).await;
        check!(
            ApiVersionsResponse::decode(&mut &refusal[..], 0).expect("decode refusal")
                == ApiVersionsResponse {
                    error_code: 35,
                    api_keys: vec![ApiVersion {
                        api_key: 18,
                        min_version: 0,
                        max_version: 5,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            "v{version}"
        );

        let before = api_versions(&mut stream, version, 2).await;

        let handshake = round_trip(
            &mut stream,
            frame(
                17,
                1,
                3,
                false,
                &encode(
                    &SaslHandshakeRequest {
                        mechanism: "PLAIN".into(),
                        ..Default::default()
                    },
                    1,
                ),
            ),
            3,
        )
        .await;
        assert!(handshake[..2] == 0i16.to_be_bytes());
        let authenticate = round_trip(
            &mut stream,
            frame(
                36,
                2,
                4,
                true,
                &encode(
                    &SaslAuthenticateRequest {
                        auth_bytes: bytes::Bytes::from_static(b"\0broker\0secret"),
                        ..Default::default()
                    },
                    2,
                ),
            ),
            4,
        )
        .await;
        // The flexible response header's tagged-fields byte, then error 0.
        assert!(authenticate[..3] == [0, 0, 0]);

        let after = api_versions(&mut stream, version, 5).await;
        check!(before == after, "v{version}");
        check!(before.error_code == 0, "v{version}");
        check!(
            before.api_keys.iter().any(|key| key.api_key == 52),
            "v{version}: Vote is advertised"
        );
        if version >= 3 {
            check!(!before.supported_features.is_empty(), "v{version}");
        }
    }

    broker.shutdown().await;
}
