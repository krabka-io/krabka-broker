//! Fixtures shared by the controller-listener handshake unit tests: frame
//! builders for a Kafka request, readers for the response frame, a
//! `BrokerRaftHandshake` configured for in-process PLAIN, and the encoded
//! request bodies that the SASL rounds send.

use std::{collections::HashMap, sync::Arc};

use krabka_protocol::{
    Encode,
    owned::{
        api_versions_request::ApiVersionsRequest,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_handshake_request::SaslHandshakeRequest,
    },
};
use krabka_raft::RaftHandshakeError;
use krabka_security::{ListenerProtocol, SaslMechanism};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::OnceCell,
    time::{Duration, timeout},
};

use super::{BrokerRaftHandshake, frame::read_kafka_request};

pub(super) fn request_frame(
    api_key: i16,
    api_version: i16,
    corr_id: i32,
    client_id: Option<&[u8]>,
    flexible: bool,
    body: &[u8],
) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&api_key.to_be_bytes());
    frame.extend_from_slice(&api_version.to_be_bytes());
    frame.extend_from_slice(&corr_id.to_be_bytes());
    match client_id {
        Some(id) => {
            let len = i16::try_from(id.len()).expect("client id fits i16");
            frame.extend_from_slice(&len.to_be_bytes());
            frame.extend_from_slice(id);
        }
        None => frame.extend_from_slice(&(-1i16).to_be_bytes()),
    }
    if flexible {
        frame.push(0);
    }
    frame.extend_from_slice(body);

    let mut out = Vec::new();
    let len = u32::try_from(frame.len()).expect("frame fits u32");
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&frame);
    out
}

pub(super) async fn read_request_from_frame(
    frame: Vec<u8>,
) -> Result<(i16, i16, i32, Vec<u8>), RaftHandshakeError> {
    let (mut client, mut server) = tokio::io::duplex(4096);
    client.write_all(&frame).await.expect("write request frame");
    read_kafka_request(&mut server, 4096).await
}

pub(super) async fn read_response_frame(stream: &mut tokio::io::DuplexStream) -> Vec<u8> {
    timeout(Duration::from_secs(1), async {
        let mut size_buf = [0u8; 4];
        stream
            .read_exact(&mut size_buf)
            .await
            .expect("response size");
        let size = u32::from_be_bytes(size_buf) as usize;
        let mut frame = vec![0u8; size];
        stream.read_exact(&mut frame).await.expect("response frame");
        frame
    })
    .await
    .expect("timely response")
}

pub(super) fn sasl_test_config() -> BrokerRaftHandshake {
    let mut plain_credentials = HashMap::new();
    plain_credentials.insert("broker".to_string(), "secret".to_string());
    BrokerRaftHandshake {
        tls_acceptor: None,
        plain_credentials,
        enabled_sasl_mechanisms: vec![SaslMechanism::Plain],
        gssapi: None,
        oauthbearer_validator: krabka_security::OAuthBearerValidator::default(),
        oauthbearer_max_session_lifetime: None,
        protocol: ListenerProtocol::SaslPlaintext,
        controller: Arc::new(OnceCell::new()),
        max_frame_bytes: 4096,
        authorizer: Arc::new(crate::authorizer::AllowAllAuthorizer),
    }
}

pub(super) fn sasl_handshake_body() -> Vec<u8> {
    let mut body = bytes::BytesMut::new();
    SaslHandshakeRequest {
        mechanism: "PLAIN".to_string(),
        ..Default::default()
    }
    .encode(&mut body, 1)
    .expect("encode sasl handshake");
    body.to_vec()
}

pub(super) fn api_versions_body(version: i16) -> Vec<u8> {
    let mut body = bytes::BytesMut::new();
    ApiVersionsRequest {
        client_software_name: "raft-peer".to_string(),
        client_software_version: "1.0".to_string(),
        ..Default::default()
    }
    .encode(&mut body, version)
    .expect("encode api versions");
    body.to_vec()
}

pub(super) fn sasl_authenticate_body() -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(0);
    payload.extend_from_slice(b"broker");
    payload.push(0);
    payload.extend_from_slice(b"secret");

    let mut body = bytes::BytesMut::new();
    SaslAuthenticateRequest {
        auth_bytes: bytes::Bytes::from(payload),
        ..Default::default()
    }
    .encode(&mut body, 2)
    .expect("encode sasl authenticate");
    body.to_vec()
}
