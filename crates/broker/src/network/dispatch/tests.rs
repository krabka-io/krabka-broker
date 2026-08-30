//! Unit tests for the dispatch module root: request peeking, the flexible
//! header metadata the KIP-853 voter RPCs need, and an end-to-end drive of the
//! serve loop over a real socket.

use assert2::{assert, check};
use futures_util::StreamExt;
use tokio::net::TcpStream;

use super::{test_support::DEFAULT_MAX_FRAME_BYTES, *};

fn request_frame(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&[u8]>,
    tagged: Option<u8>,
    body: &[u8],
) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i16(api_key);
    buf.put_i16(api_version);
    buf.put_i32(correlation_id);
    match client_id {
        Some(id) => {
            buf.put_i16(i16::try_from(id.len()).expect("client id length"));
            buf.put_slice(id);
        }
        None => buf.put_i16(-1),
    }
    if let Some(tagged) = tagged {
        buf.put_u8(tagged);
    }
    buf.put_slice(body);
    buf
}

#[test]
fn peek_api_key_reads_first_two_bytes_big_endian() {
    // api_key=18, version=3, corr_id=1 — only first 2 bytes are inspected.
    let mut buf = BytesMut::new();
    buf.put_i16(18);
    buf.put_i16(3);
    buf.put_i32(1);
    assert!(crate::network::request::peek_api_key(&buf).unwrap() == 18);
}

#[test]
fn peek_api_key_rejects_short_frame() {
    let buf = [0u8; 1];
    assert!(crate::network::request::peek_api_key(&buf).is_err());
}

/// KIP-853 RPCs (80/81/82) route through the registry path and are
/// flexible from v0. This guards the metadata used when parsing their
/// flexible request headers.
#[test]
fn raft_voter_rpcs_peek_and_flex_routing() {
    let registry = crate::handlers::registry::build_registry();

    for api_key in [80i16, 81, 82] {
        let mut buf = BytesMut::new();
        buf.put_i16(api_key);
        buf.put_i16(0); // version 0
        buf.put_i32(1); // corr_id
        assert!(crate::network::request::peek_api_key(&buf).unwrap() == api_key);
        assert!(
            registry.body_flexible(api_key, 0),
            "api_key {api_key} is flexible from v0"
        );
    }
}

/// The three KIP-853 controller-plane RPCs, `AddRaftVoter` 80,
/// `RemoveRaftVoter` 81, and `UpdateRaftVoter` 82, must reach their
/// registry handlers. This test drives each RPC over a real socket through
/// the whole serve loop and asserts that it reaches its handler. A
/// `DenyAll` authorizer stops every handler at the ACL gate with
/// `CLUSTER_AUTHORIZATION_FAILED` (31), which differs observably from the
/// unsupported path's 35.
#[tokio::test]
async fn raft_voter_registry_routes_to_real_handlers() {
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            add_raft_voter_request as add_req, add_raft_voter_response as add_resp,
            remove_raft_voter_request as rem_req, remove_raft_voter_response as rem_resp,
            update_raft_voter_request as upd_req, update_raft_voter_response as upd_resp,
        },
    };

    use crate::test_support::DenyAll;

    // Send a flexible (v2-header) request frame carrying `body` for
    // `api_key`/`version` and return the response body with its 5-byte
    // flexible header (corr_id + empty tagged-fields byte) stripped.
    async fn round_trip(
        framed: &mut Framed<TcpStream, tokio_util::codec::LengthDelimitedCodec>,
        api_key: i16,
        version: i16,
        body: &[u8],
    ) -> Vec<u8> {
        let frame = request_frame(api_key, version, 7, None, Some(0), body);
        framed.send(frame.freeze()).await.expect("send request");
        let resp = framed
            .next()
            .await
            .expect("a response frame")
            .expect("response decode");
        resp[5..].to_vec()
    }

    fn encode_default<T: Encode + Default>(version: i16) -> BytesMut {
        let mut body = BytesMut::new();
        T::default().encode(&mut body, version).expect("encode");
        body
    }

    let dir = tempfile::TempDir::new().expect("tempdir");
    let mut cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.authorizer = std::sync::Arc::new(DenyAll);
    let handle = Broker::start(cfg).await.expect("start broker");
    let broker = handle.broker_arc_for_test();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.expect("accept");
        let spec = crate::config::ListenerSpec {
            name: "PLAINTEXT".to_string(),
            bind_addr: addr,
            advertised: "127.0.0.1:9092".to_string(),
            protocol: krabka_security::ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
        };
        serve_connection_stream(broker, stream, spec, peer, None).await;
    });

    let client = TcpStream::connect(addr).await.expect("connect");
    let mut framed = codec::frame(client, DEFAULT_MAX_FRAME_BYTES);

    let add_body = encode_default::<add_req::AddRaftVoterRequest>(add_req::MAX_VERSION);
    let raw = round_trip(&mut framed, 80, add_req::MAX_VERSION, &add_body).await;
    let add = add_resp::AddRaftVoterResponse::decode(&mut &raw[..], add_resp::MAX_VERSION)
        .expect("decode AddRaftVoterResponse");

    let rem_body = encode_default::<rem_req::RemoveRaftVoterRequest>(rem_req::MAX_VERSION);
    let raw = round_trip(&mut framed, 81, rem_req::MAX_VERSION, &rem_body).await;
    let rem = rem_resp::RemoveRaftVoterResponse::decode(&mut &raw[..], rem_resp::MAX_VERSION)
        .expect("decode RemoveRaftVoterResponse");

    let upd_body = encode_default::<upd_req::UpdateRaftVoterRequest>(upd_req::MAX_VERSION);
    let raw = round_trip(&mut framed, 82, upd_req::MAX_VERSION, &upd_body).await;
    let upd = upd_resp::UpdateRaftVoterResponse::decode(&mut &raw[..], upd_resp::MAX_VERSION)
        .expect("decode UpdateRaftVoterResponse");

    // Each real handler denies at the ACL gate; the fall-through path would
    // instead yield UNSUPPORTED_VERSION (and not even decode as this type).
    check!(add.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    check!(rem.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    check!(upd.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);

    drop(framed);
    server.await.expect("serve loop joins on client EOF");
    handle.shutdown().await;
}

#[tokio::test]
async fn version_gate_rejects_every_registered_api_before_dispatch() {
    use krabka_protocol::{Decode, owned::api_versions_response::ApiVersionsResponse};

    let dir = tempfile::TempDir::new().expect("tempdir");
    let cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("start broker");
    let broker = handle.broker_arc_for_test();
    let registry = crate::handlers::registry::build_registry();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("listener addr");
    let server = tokio::spawn(async move {
        let (stream, peer) = listener.accept().await.expect("accept");
        let spec = crate::config::ListenerSpec {
            name: "PLAINTEXT".to_string(),
            bind_addr: addr,
            advertised: "127.0.0.1:9092".to_string(),
            protocol: krabka_security::ListenerProtocol::Plaintext,
            tls_config: None,
            sasl_mechanisms: None,
        };
        serve_connection_stream(broker, stream, spec, peer, None).await;
    });

    let client = TcpStream::connect(addr).await.expect("connect");
    let mut framed = codec::frame(client, DEFAULT_MAX_FRAME_BYTES);
    let mut api_keys: Vec<_> = registry.registered_api_keys().collect();
    api_keys.sort_unstable();

    for (correlation_id, api_key) in api_keys.into_iter().enumerate() {
        let entry = registry.get(api_key).expect("registered entry");
        let version = entry
            .version_range()
            .end()
            .checked_add(1)
            .expect("max version has a successor");
        let flexible = entry.body_flexible(version);
        let frame = request_frame(
            api_key,
            version,
            i32::try_from(correlation_id).expect("correlation id"),
            None,
            flexible.then_some(0),
            &[],
        );
        framed.send(frame.freeze()).await.expect("send request");
        let response = framed
            .next()
            .await
            .expect("response frame")
            .expect("response decode");
        let body = &response[crate::network::response_header_len(api_key, flexible)..];
        if api_key == API_VERSIONS_KEY {
            let decoded =
                ApiVersionsResponse::decode(&mut &body[..], 0).expect("v0 ApiVersions fallback");
            check!(decoded.error_code == codes::UNSUPPORTED_VERSION);
            check!(decoded.api_keys == crate::api_catalog::supported_apis());
        } else {
            check!(
                body == codes::UNSUPPORTED_VERSION.to_be_bytes(),
                "api_key {api_key} version {version}"
            );
        }
    }

    let frame = request_frame(API_VERSIONS_KEY, 99, 99, None, Some(0), &[]);
    framed.send(frame.freeze()).await.expect("send v99 request");
    let response = framed
        .next()
        .await
        .expect("v99 response frame")
        .expect("v99 response decode");
    let decoded =
        ApiVersionsResponse::decode(&mut &response[4..], 0).expect("v99 response uses the v0 body");
    check!(decoded.error_code == codes::UNSUPPORTED_VERSION);
    check!(decoded.api_keys == crate::api_catalog::supported_apis());

    let frame = request_frame(i16::MAX, 0, 100, None, None, &[]);
    framed.send(frame.freeze()).await.expect("send unknown api");
    check!(
        framed.next().await.is_none(),
        "unknown api must close connection"
    );

    server.await.expect("serve loop joins after unknown api");
    handle.shutdown().await;
}
