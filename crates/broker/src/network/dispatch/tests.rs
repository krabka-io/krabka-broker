//! Unit tests for the dispatch module root: request peeking, the flexible
//! header metadata the KIP-853 voter RPCs need, the KIP-219 throttle mute, and
//! an end-to-end drive of the serve loop over a real socket.

use std::time::{Duration, Instant};

use assert2::{assert, check};
use bytes::{BufMut, BytesMut};
use futures_util::StreamExt;
use krabka_metadata::{ClientQuotaRecord, EntityKey, MetadataRecord, QuotaEntity};
use krabka_units::{convert::TimeExt as _, millis};
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
/// connection close on the unsupported path.
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
    // instead close the connection.
    check!(add.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    check!(rem.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
    check!(upd.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);

    drop(framed);
    server.await.expect("serve loop joins on client EOF");
    handle.shutdown().await;
}

#[tokio::test]
async fn unsupported_versions_return_typed_errors_before_dispatch() {
    use krabka_protocol::{Decode, owned::api_versions_response::ApiVersionsResponse};

    let dir = tempfile::TempDir::new().expect("tempdir");
    let cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
    let handle = Broker::start(cfg).await.expect("start broker");
    let broker = handle.broker_arc_for_test();
    let metrics = broker.metrics.clone();
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

    for (correlation_id, api) in crate::api_catalog::supported_apis().into_iter().enumerate() {
        let entry = registry.get(api.api_key).expect("advertised API entry");
        let version = api
            .max_version
            .checked_add(1)
            .expect("maximum API version has a successor");
        let correlation_id = i32::try_from(correlation_id).expect("correlation id");
        let frame = request_frame(
            api.api_key,
            version,
            correlation_id,
            None,
            entry.body_flexible(version).then_some(0),
            &[],
        );
        framed
            .send(frame.freeze())
            .await
            .expect("send max+1 request");
        let response = framed
            .next()
            .await
            .expect("unsupported-version response frame")
            .expect("unsupported-version response decode");
        check!(
            i32::from_be_bytes(response[..4].try_into().expect("response correlation id"))
                == correlation_id,
            "api_key {}",
            api.api_key
        );

        let response_version = if api.api_key == API_VERSIONS_KEY {
            0
        } else {
            entry.nearest_supported_version(version)
        };
        let header_len =
            crate::network::response_header_len(api.api_key, entry.body_flexible(response_version));
        check!(
            response[header_len..]
                .windows(2)
                .any(|bytes| bytes == codes::UNSUPPORTED_VERSION.to_be_bytes()),
            "api_key {} response carries error 35",
            api.api_key
        );

        if api.api_key == API_VERSIONS_KEY {
            let decoded = ApiVersionsResponse::decode(&mut &response[header_len..], 0)
                .expect("max+1 ApiVersions uses the v0 body");
            check!(decoded.error_code == codes::UNSUPPORTED_VERSION);
            check!(decoded.api_keys == crate::api_catalog::supported_apis());
        }
    }

    let frame = request_frame(i16::MAX, 0, 10_000, None, None, &[]);
    framed.send(frame.freeze()).await.expect("send unknown api");
    check!(
        framed.next().await.is_none(),
        "unknown api must close connection"
    );
    let unknown = crate::metrics::ApiKeyLabel {
        api_key: crate::metrics::UNKNOWN_LABEL.into(),
    };
    check!(metrics.api_requests.get_or_create(&unknown).get() == 1);

    server.await.expect("serve loop joins after unknown API");
    handle.shutdown().await;
}

/// Writes a v0 `ApiVersions` request, the cheapest frame that still reaches a
/// real handler through the whole serve loop.
async fn send_api_versions(
    framed: &mut Framed<TcpStream, tokio_util::codec::LengthDelimitedCodec>,
    correlation_id: i32,
) {
    let frame = request_frame(API_VERSIONS_KEY, 0, correlation_id, None, None, &[]).freeze();
    framed.send(frame).await.expect("send ApiVersions");
}

/// Reads the leading correlation id of a response frame.
fn response_correlation_id(frame: &BytesMut) -> i32 {
    i32::from_be_bytes(frame[..4].try_into().expect("response correlation id"))
}

/// KIP-219: a throttled request is answered at once, and the quota is enforced
/// by muting the connection afterwards.
///
/// `request_percentage = 0.0001` gives the KIP-124 bucket a budget of one
/// microsecond of handler time per second, so the first request overruns it by
/// orders of magnitude and earns the configured maximum window. The
/// pre-KIP-219 broker slept for that window *before* writing the response,
/// which is what this pins down: the response has to beat a client timeout far
/// shorter than the window, and the next request must go unserved until the
/// window closes.
#[tokio::test]
async fn throttled_connection_answers_first_and_mutes_afterwards() {
    /// The one `request_percentage` rate this test configures, as a percentage
    /// of one request-handler thread.
    const RATE: f64 = 0.0001;
    /// Stands in for a client `request.timeout.ms` well inside the window.
    const CLIENT_TIMEOUT: Duration = Duration::from_millis(150);
    /// Scheduling slack on the lower bound for the muted read.
    const SLACK: Duration = Duration::from_millis(50);

    let mute_window = millis(600);

    let dir = tempfile::TempDir::new().expect("tempdir");
    let mut cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.quota_throttle_max = mute_window;
    let handle = Broker::start(cfg).await.expect("start broker");
    let broker = handle.broker_arc_for_test();

    // PLAINTEXT authenticates every connection as ANONYMOUS, so that is the
    // principal the request quota is looked up under.
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![QuotaEntity {
                entity_type: "user".into(),
                entity_name: Some("ANONYMOUS".into()),
            }],
            config_key: "request_percentage".into(),
            config_value: Some(RATE),
        })])
        .await
        .expect("seed request quota");
    handle
        .wait_for_image(|image| {
            let key: EntityKey = vec![("user".into(), Some("ANONYMOUS".into()))];
            image
                .client_quotas()
                .get(&key)
                .and_then(|configs| configs.get("request_percentage"))
                == Some(&RATE)
        })
        .await;

    let serve_broker = broker.clone();
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
        serve_connection_stream(serve_broker, stream, spec, peer, None).await;
    });

    let client = TcpStream::connect(addr).await.expect("connect");
    let mut framed = codec::frame(client, DEFAULT_MAX_FRAME_BYTES);

    // The first request trips the quota. Its response must still arrive well
    // inside a client timeout that the throttle window would blow through.
    let sent_at = Instant::now();
    send_api_versions(&mut framed, 1).await;
    let first = tokio::time::timeout(CLIENT_TIMEOUT, framed.next())
        .await
        .expect("throttled response must beat the client timeout, not wait out the window")
        .expect("a response frame")
        .expect("response decode");
    let answered_at = Instant::now();
    check!(response_correlation_id(&first) == 1);
    check!(sent_at.elapsed() < mute_window.to_std());

    // The connection is now muted: the second request sits unread until the
    // window closes, and is then served.
    send_api_versions(&mut framed, 2).await;
    check!(
        tokio::time::timeout(CLIENT_TIMEOUT, framed.next())
            .await
            .is_err(),
        "a muted connection must serve no further request inside the throttle window"
    );
    let second = tokio::time::timeout(Duration::from_secs(5), framed.next())
        .await
        .expect("the mute must lift once the window closes")
        .expect("a response frame")
        .expect("response decode");
    check!(response_correlation_id(&second) == 2);
    // The mute began when the first response was written, marginally before it
    // was read back here, so the lower bound carries a little slack.
    check!(answered_at.elapsed() >= mute_window.to_std().saturating_sub(SLACK));

    drop(framed);
    server.await.expect("serve loop joins on client EOF");
    handle.shutdown().await;
}
