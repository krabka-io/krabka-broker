//! KIP-714 disconnect cleanup through the real serve loop: a connection that
//! pushed telemetry and then closes takes its retained Prometheus series with
//! it, and leaves the series of every other connection in place.
//!
//! The release is wired at the tail of the serve loop, after the read loop
//! ends, so only a test that drives a socket to EOF covers it.

use {
    super::request_frame,
    crate::{
        broker::Broker,
        client_metrics::test_support::{gauge_metric, metrics_data, subscribe_to_cpu},
        codes,
        handlers::test_support::StartedBroker,
        metrics::{ClientMetricsEviction, ClientMetricsEvictionLabel},
        network::{codec::KafkaCodec, dispatch::test_support::plaintext_loop},
        test_support::start_broker_with,
    },
    assert2::assert,
    bytes::BytesMut,
    futures_util::{SinkExt as _, StreamExt as _},
    krabka_protocol::{
        Decode,
        owned::{
            get_telemetry_subscriptions_request::GetTelemetrySubscriptionsRequest,
            get_telemetry_subscriptions_response::GetTelemetrySubscriptionsResponse,
            push_telemetry_request::PushTelemetryRequest,
            push_telemetry_response::PushTelemetryResponse,
        },
    },
    opentelemetry_proto::tonic::metrics::v1::number_data_point,
    prost::Message as _,
    std::{sync::Arc, time::Duration},
    tokio::net::TcpStream,
    tokio_util::codec::Framed,
};

const GET_TELEMETRY_SUBSCRIPTIONS_KEY: i16 = 71;
const PUSH_TELEMETRY_KEY: i16 = 72;
/// Both telemetry APIs are flexible from v0, so the response header carries a
/// tagged-fields byte after the correlation id.
const FLEXIBLE_RESPONSE_HEADER_LEN: usize = 5;
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// One request on `framed`, and the decoded body of its response.
async fn round_trip<Req: krabka_protocol::Encode, Resp: for<'de> Decode<'de>>(
    framed: &mut Framed<TcpStream, KafkaCodec>,
    api_key: i16,
    request: &Req,
) -> Resp {
    let mut body = BytesMut::new();
    request.encode(&mut body, 0).expect("encode request");
    let frame = request_frame()
        .api_key(api_key)
        .api_version(0)
        .correlation_id(1)
        .maybe_tagged(Some(&[0]))
        .body(&body)
        .call()
        .freeze();
    framed.send(frame).await.expect("send request");
    let response = tokio::time::timeout(CLIENT_TIMEOUT, framed.next())
        .await
        .expect("the response must beat the client timeout")
        .expect("a response frame")
        .expect("response decode");
    let mut cursor = &response[FLEXIBLE_RESPONSE_HEADER_LEN..];
    Resp::decode(&mut cursor, 0).expect("decode response body")
}

/// Opens a connection, takes a subscription and pushes one gauge on it, as a
/// Java client's first telemetry cycle does.
async fn connect_and_push(
    broker: &Arc<Broker>,
) -> (tokio::task::JoinHandle<()>, Framed<TcpStream, KafkaCodec>) {
    let (server, mut framed) = plaintext_loop(Arc::clone(broker)).await;
    let subscription: GetTelemetrySubscriptionsResponse = round_trip(
        &mut framed,
        GET_TELEMETRY_SUBSCRIPTIONS_KEY,
        &GetTelemetrySubscriptionsRequest::default(),
    )
    .await;
    assert!(subscription.error_code == codes::NONE);
    let payload = metrics_data(vec![gauge_metric(
        "cpu.utilization",
        number_data_point::Value::AsDouble(0.5),
    )])
    .encode_to_vec();
    let pushed: PushTelemetryResponse =
        round_trip(&mut framed, PUSH_TELEMETRY_KEY, &PushTelemetryRequest {
            client_instance_id: subscription.client_instance_id,
            subscription_id: subscription.subscription_id,
            compression_type: 0,
            metrics: payload.into(),
            ..Default::default()
        })
        .await;
    assert!(pushed.error_code == codes::NONE);
    (server, framed)
}

/// Drops the client end and waits for the serve loop to finish its teardown,
/// which is where the broker releases the connection's client instance.
async fn close(server: tokio::task::JoinHandle<()>, framed: Framed<TcpStream, KafkaCodec>) {
    drop(framed);
    server.await.expect("serve loop joins on client EOF");
}

#[tokio::test]
async fn closing_a_connection_releases_only_its_own_series() {
    let StartedBroker {
        broker: handle,
        dir: _dir,
    } = start_broker_with(|cfg| cfg.client_metrics_enable = true).await;
    subscribe_to_cpu(&handle).await;
    let broker = handle.broker_arc_for_test();
    let collector = &broker.client_metrics.prometheus;
    let disconnects = || {
        collector
            .stats()
            .evicted
            .get_or_create(&ClientMetricsEvictionLabel {
                reason: ClientMetricsEviction::Disconnect,
            })
            .get()
    };

    let (first_server, first) = connect_and_push(&broker).await;
    let (second_server, second) = connect_and_push(&broker).await;
    assert!((collector.retained_series(), disconnects()) == (2, 0));

    close(first_server, first).await;
    assert!((collector.retained_series(), disconnects()) == (1, 1));

    close(second_server, second).await;
    assert!(
        (
            collector.retained_series(),
            collector.stats().retained.get(),
            disconnects()
        ) == (0, 0, 2)
    );
    handle.shutdown().await;
}
