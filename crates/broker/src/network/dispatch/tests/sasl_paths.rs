//! The serve loop's two SASL paths that leave the regular request route: the
//! raw-token exchange after a `SaslHandshake` v0, and the KIP-368 close of a
//! request that arrives after its session expired. The `auth_handlers`
//! integration suite pins what the peer sees on the wire; these pin what the
//! loop itself records and whether it keeps serving.

use {
    super::{RequestHeader, close, close_counts, encoded_request_frame, no_closes},
    crate::{
        handlers::test_support::StartedBroker,
        metrics::{BrokerMetrics, SaslMechanismLabel},
        network::{
            codec::KafkaCodec,
            dispatch::test_support::{ServeLoop, connect_framed, serve_loop},
        },
        test_support::start_broker_with,
    },
    assert2::{assert, check},
    bytes::Bytes,
    futures_util::{SinkExt as _, StreamExt as _},
    krabka_protocol::owned::{
        metadata_request::MetadataRequest, sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_handshake_request::SaslHandshakeRequest,
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::time::Duration,
    tokio::{net::TcpStream, task::JoinHandle, time},
    tokio_util::codec::Framed,
};

const HANDSHAKE: i16 = 17;
const AUTHENTICATE: i16 = 36;
const METADATA: i16 = 3;
const GOOD_TOKEN: &[u8] = b"\0alice\0wonderland";
const BAD_TOKEN: &[u8] = b"\0alice\0looking-glass";

fn handshake(version: i16) -> Bytes {
    encoded_request_frame()
        .body(&SaslHandshakeRequest {
            mechanism: "PLAIN".to_owned(),
            ..Default::default()
        })
        .api_key(HANDSHAKE)
        .api_version(version)
        .call()
}

fn authenticate() -> Bytes {
    encoded_request_frame()
        .body(&SaslAuthenticateRequest {
            auth_bytes: Bytes::from_static(GOOD_TOKEN),
            ..Default::default()
        })
        .api_key(AUTHENTICATE)
        .api_version(2)
        .correlation_id(2)
        .header(RequestHeader::Flexible)
        .call()
}

fn metadata() -> Bytes {
    encoded_request_frame()
        .body(&MetadataRequest {
            topics: Some(Vec::new()),
            ..Default::default()
        })
        .api_key(METADATA)
        .api_version(12)
        .correlation_id(3)
        .header(RequestHeader::Flexible)
        .call()
}

/// A started PLAIN broker, the serve loop of its one SASL connection, and the
/// client end of that connection.
struct SaslConnection {
    started: StartedBroker,
    server: JoinHandle<()>,
    framed: Framed<TcpStream, KafkaCodec>,
}

async fn sasl_connection(max_reauth: Option<krabka_units::Time>) -> SaslConnection {
    let started = start_broker_with(|cfg| {
        cfg.plain_credentials
            .insert("alice".to_owned(), "wonderland".to_owned());
        cfg.connections_max_reauth = max_reauth;
    })
    .await;
    let ServeLoop { addr, serve } = serve_loop(
        started.broker.broker_arc_for_test(),
        "SASL",
        ListenerProtocol::SaslPlaintext,
        Some(vec![SaslMechanism::Plain]),
        None,
        1,
    )
    .await;
    let server = tokio::spawn(serve);
    let framed = connect_framed(addr, "connect").await;
    SaslConnection {
        started,
        server,
        framed,
    }
}

impl SaslConnection {
    async fn round_trip(&mut self, frame: Bytes) -> Option<Bytes> {
        self.framed.send(frame).await.expect("send frame");
        self.framed
            .next()
            .await
            .map(|frame| frame.expect("response frame").freeze())
    }

    /// Closes the client end, joins the serve loop, and returns the metrics it
    /// recorded.
    async fn finish(self) -> BrokerMetrics {
        let Self {
            started,
            server,
            framed,
        } = self;
        drop(framed);
        server
            .await
            .expect("serve loop joins once the connection ends");
        let metrics = started.broker.broker_arc_for_test().metrics.clone();
        started.broker.shutdown().await;
        metrics
    }
}

fn plain_label() -> SaslMechanismLabel {
    SaslMechanismLabel {
        mechanism: "PLAIN".to_owned(),
    }
}

/// After a `SaslHandshake` v0 the next frame is a raw PLAIN token. A good one
/// is answered with an empty raw token, the loop goes on, and the next request
/// is served on the authenticated connection.
#[tokio::test]
async fn a_good_raw_token_authenticates_and_the_loop_keeps_serving() {
    let mut connection = sasl_connection(None).await;
    assert!(let Some(_) = connection.round_trip(handshake(0)).await);

    let token = connection.round_trip(Bytes::from_static(GOOD_TOKEN)).await;
    check!(token == Some(Bytes::new()));
    check!(let Some(_) = connection.round_trip(metadata()).await);

    let metrics = connection.finish().await;
    check!(
        metrics
            .successful_authentication
            .get_or_create(&plain_label())
            .get()
            == 1
    );
    // The client hung up, so the close is the peer's.
    check!(
        close_counts(&metrics)
            == vec![
                close("idle", 0),
                close("sasl_session_expired", 0),
                close("decode_error", 0),
                close("peer_closed", 1),
                close("max_connections", 0),
                close("max_connections_per_ip", 0),
            ]
    );
}

/// A raw token with a bad credential has no response shape to carry an error:
/// the loop closes the connection without writing anything.
#[tokio::test]
async fn a_bad_raw_token_closes_the_connection_without_a_response() {
    let mut connection = sasl_connection(None).await;
    assert!(let Some(_) = connection.round_trip(handshake(0)).await);

    check!(
        connection
            .round_trip(Bytes::from_static(BAD_TOKEN))
            .await
            .is_none()
    );

    let metrics = connection.finish().await;
    check!(
        metrics
            .failed_authentication
            .get_or_create(&plain_label())
            .get()
            == 1
    );
    check!(close_counts(&metrics) == no_closes());
}

/// KIP-368: a request that arrives after the session's window closed, and does
/// not begin a re-authentication, ends the connection with no response, and
/// the close is counted as an expired SASL session.
#[tokio::test]
async fn a_request_after_the_session_expired_closes_as_sasl_session_expired() {
    let mut connection = sasl_connection(Some(krabka_units::millis(1_000))).await;
    assert!(let Some(_) = connection.round_trip(handshake(1)).await);
    assert!(let Some(_) = connection.round_trip(authenticate()).await);
    assert!(let Some(_) = connection.round_trip(metadata()).await);

    time::sleep(Duration::from_millis(1_100)).await;
    check!(connection.round_trip(metadata()).await.is_none());

    let metrics = connection.finish().await;
    check!(
        close_counts(&metrics)
            == vec![
                close("idle", 0),
                close("sasl_session_expired", 1),
                close("decode_error", 0),
                close("peer_closed", 0),
                close("max_connections", 0),
                close("max_connections_per_ip", 0),
            ]
    );
}
