//! KIP-219 end-to-end tests for the serve loop: a throttled request is
//! answered immediately and the quota is enforced afterwards by muting the
//! connection.
//!
//! Every test here drives a real socket through `serve_connection_stream`,
//! because the ordering under test is a property of that loop: the write
//! happens first, and the window the handler charged is applied by refusing
//! to read the next request. The pre-KIP-219 broker slept inside the quota
//! code before writing, which each test distinguishes by giving the response
//! a deadline far shorter than the window it must beat.

use std::time::{Duration, Instant};

use assert2::check;
use bytes::BytesMut;
use futures_util::{SinkExt as _, StreamExt as _};
use krabka_metadata::{ClientQuotaRecord, EntityKey, MetadataRecord, QuotaEntity};
use krabka_protocol::{
    Decode, Encode as _,
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    records::{Record, RecordBatch},
};
use krabka_units::{Time, convert::TimeExt as _, millis};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use super::{super::test_support::DEFAULT_MAX_FRAME_BYTES, request_frame};
use crate::{broker::Broker, network::codec};

/// `Produce` wire `api_key`.
const PRODUCE_KEY: i16 = 0;
/// `Fetch` wire `api_key`.
const FETCH_KEY: i16 = 1;
/// The `Produce` version these tests drive: flexible, and its response carries
/// `throttle_time_ms`.
const PRODUCE_VERSION: i16 = 11;
/// The `Fetch` version these tests drive: flexible, and its response carries
/// `throttle_time_ms` first.
const FETCH_VERSION: i16 = 12;

/// Stands in for a client `request.timeout.ms` well inside a mute window. A
/// response that has to beat it, or a muted read that has to outlast it, is
/// two orders of magnitude away from the real cost of either, so this is loose
/// on a loaded machine and still four times shorter than the shortest window
/// any test here configures.
const CLIENT_TIMEOUT: Duration = Duration::from_millis(250);
/// Scheduling slack on a lower bound measured against a mute window.
const SLACK: Duration = Duration::from_millis(50);
/// An upper bound on a single mute that no scheduling delay is expected to
/// exceed, but that two stacked windows of the same length would.
const ONE_WINDOW_CEILING: Duration = Duration::from_millis(1900);

/// A muted connection is not read from, so a request left in flight over one
/// needs an outer bound that is generous next to the window itself.
const MUTE_LIFT_TIMEOUT: Duration = Duration::from_secs(10);

/// Starts a broker whose quota throttle caps at `throttle_max`, and seeds
/// `quotas` against the ANONYMOUS user.
///
/// PLAINTEXT authenticates every connection as ANONYMOUS, so that is the
/// principal every quota in this module is looked up under. The function
/// returns once the seeded quotas are visible in the metadata image, so a
/// request driven afterwards is guaranteed to see them.
async fn broker_with_anonymous_quotas(
    throttle_max: Time,
    quotas: &[(&str, f64)],
) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let mut cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
    cfg.quota_throttle_max = throttle_max;
    let handle = Broker::start(cfg).await.expect("start broker");

    let records: Vec<MetadataRecord> = quotas
        .iter()
        .map(|(key, value)| {
            MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: vec![QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("ANONYMOUS".into()),
                }],
                config_key: (*key).into(),
                config_value: Some(*value),
            })
        })
        .collect();
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(records)
        .await
        .expect("seed client quotas");
    handle
        .wait_for_image(|image| {
            let key: EntityKey = vec![("user".into(), Some("ANONYMOUS".into()))];
            let Some(configs) = image.client_quotas().get(&key) else {
                return false;
            };
            quotas
                .iter()
                .all(|(name, value)| configs.get(*name) == Some(value))
        })
        .await;
    (handle, dir)
}

/// Accepts one PLAINTEXT connection and serves it through the real dispatch
/// loop. It returns the join handle for the loop and a client already framed
/// against it.
async fn connect_to_serve_loop(
    handle: &crate::broker::BrokerHandle,
) -> (
    tokio::task::JoinHandle<()>,
    Framed<TcpStream, LengthDelimitedCodec>,
) {
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
            principal_mapper: crate::SslPrincipalMapper::default(),
        };
        super::super::serve_connection_stream(broker, stream, spec, peer, None).await;
    });
    let client = TcpStream::connect(addr).await.expect("connect");
    (server, codec::frame(client, DEFAULT_MAX_FRAME_BYTES))
}

/// Writes a v0 `ApiVersions` request, the cheapest frame that still reaches a
/// real handler through the whole serve loop.
async fn send_api_versions(
    framed: &mut Framed<TcpStream, LengthDelimitedCodec>,
    correlation_id: i32,
) {
    let frame = request_frame(super::API_VERSIONS_KEY, 0, correlation_id, None, None, &[]).freeze();
    framed.send(frame).await.expect("send ApiVersions");
}

/// Writes a flexible request frame — a v2 request header, with its trailing
/// tagged-fields byte — carrying an encoded `body`.
async fn send_request(
    framed: &mut Framed<TcpStream, LengthDelimitedCodec>,
    api_key: i16,
    version: i16,
    correlation_id: i32,
    body: &BytesMut,
) {
    let frame = request_frame(api_key, version, correlation_id, None, Some(0), body).freeze();
    framed.send(frame).await.expect("send request");
}

/// Reads the leading correlation id of a response frame.
fn response_correlation_id(frame: &BytesMut) -> i32 {
    i32::from_be_bytes(frame[..4].try_into().expect("response correlation id"))
}

/// Builds a `Produce` request body for `topic` carrying `count` records of
/// `record_bytes` each.
fn produce_body(topic: &str, acks: i16, record_bytes: usize, count: usize) -> BytesMut {
    let value = bytes::Bytes::from(vec![0u8; record_bytes]);
    let records: Vec<Record> = (0..count)
        .map(|i| Record {
            offset_delta: i32::try_from(i).expect("record index"),
            value: Some(value.clone()),
            ..Default::default()
        })
        .collect();
    let request = ProduceRequest {
        acks,
        timeout_ms: 30_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: 0,
                records: Some(
                    RecordBatch {
                        last_offset_delta: i32::try_from(count - 1).expect("record count"),
                        records,
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    request
        .encode(&mut body, PRODUCE_VERSION)
        .expect("encode Produce");
    body
}

/// Builds a consumer `Fetch` request body for partition 0 of `topic`.
fn fetch_body(topic: &str) -> BytesMut {
    let request = FetchRequest {
        replica_id: -1,
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1 << 20,
        topics: vec![FetchTopic {
            topic: topic.to_string(),
            partitions: vec![FetchPartition {
                partition: 0,
                fetch_offset: 0,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut body = BytesMut::new();
    request
        .encode(&mut body, FETCH_VERSION)
        .expect("encode Fetch");
    body
}

/// Strips the flexible (v1) response header — a correlation id plus an empty
/// tagged-fields byte — and decodes the body at `version`.
fn decode_response_body<T: Decode<'static>>(frame: &BytesMut, version: i16) -> T {
    let mut cursor: &[u8] = &frame[5..];
    T::decode(&mut cursor, version).expect("decode response body")
}

/// Reads the response the mute is holding back, and returns how long after
/// `muted_at` it arrived.
///
/// It first pins the mute down: nothing may be served for `CLIENT_TIMEOUT`,
/// which is far shorter than every window configured here.
async fn read_after_mute(
    framed: &mut Framed<TcpStream, LengthDelimitedCodec>,
    muted_at: Instant,
) -> (BytesMut, Duration) {
    check!(
        tokio::time::timeout(CLIENT_TIMEOUT, framed.next())
            .await
            .is_err(),
        "a muted connection must serve no further request inside the throttle window"
    );
    let frame = tokio::time::timeout(MUTE_LIFT_TIMEOUT, framed.next())
        .await
        .expect("the mute must lift once the window closes")
        .expect("a response frame")
        .expect("response decode");
    (frame, muted_at.elapsed())
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
    let mute_window = millis(1000);
    let (handle, _dir) =
        broker_with_anonymous_quotas(mute_window, &[("request_percentage", 0.0001)]).await;
    let (server, mut framed) = connect_to_serve_loop(&handle).await;

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
    let (second, muted_for) = read_after_mute(&mut framed, answered_at).await;
    check!(response_correlation_id(&second) == 2);
    // The mute began when the first response was written, marginally before it
    // was read back here, so the lower bound carries a little slack.
    check!(muted_for >= mute_window.to_std().saturating_sub(SLACK));

    drop(framed);
    server.await.expect("serve loop joins on client EOF");
    handle.shutdown().await;
}

/// KIP-219 on the `Fetch` path, which writes its response as a plan of raw
/// socket writes rather than through the framed sink.
///
/// The records the consumer is waiting on must go out first, and the KIP-124
/// window the fetch charged is applied only once the whole plan is written.
/// Sleeping inside the fetch quota instead would hold the records back for the
/// full window, which the `CLIENT_TIMEOUT` bound rules out.
#[tokio::test]
async fn a_throttled_fetch_writes_its_plan_before_the_mute() {
    let mute_window = millis(1000);
    let (handle, _dir) =
        broker_with_anonymous_quotas(mute_window, &[("request_percentage", 0.0001)]).await;
    let (server, mut framed) = connect_to_serve_loop(&handle).await;

    let sent_at = Instant::now();
    send_request(
        &mut framed,
        FETCH_KEY,
        FETCH_VERSION,
        1,
        &fetch_body("no-such-topic"),
    )
    .await;
    let first = tokio::time::timeout(CLIENT_TIMEOUT, framed.next())
        .await
        .expect("the fetch plan must beat the client timeout, not wait out the window")
        .expect("a response frame")
        .expect("response decode");
    let answered_at = Instant::now();
    check!(response_correlation_id(&first) == 1);
    check!(sent_at.elapsed() < mute_window.to_std());

    // The window the connection is about to be muted for is the one the
    // response reports.
    let fetch: FetchResponse = decode_response_body(&first, FETCH_VERSION);
    let reported = millis(u32::try_from(fetch.throttle_time_ms).expect("a window"));
    check!(reported == mute_window);

    send_api_versions(&mut framed, 2).await;
    let (second, muted_for) = read_after_mute(&mut framed, answered_at).await;
    check!(response_correlation_id(&second) == 2);
    check!(muted_for >= mute_window.to_std().saturating_sub(SLACK));

    drop(framed);
    server.await.expect("serve loop joins on client EOF");
    handle.shutdown().await;
}

/// An `acks=0` produce writes no response frame, and still carries a mute
/// window.
///
/// Kafka answers nothing to `acks=0`, but the handler has charged
/// `producer_byte_rate` all the same, so the quota still has to be enforced.
/// The only way left to enforce it is the channel mute, which this pins down:
/// the next request on the connection goes unserved for the window, and when
/// the connection does speak it is that request's response, never a produce
/// response.
#[tokio::test]
async fn acks_zero_produce_writes_no_response_and_still_mutes() {
    let mute_window = millis(1000);
    // 128 bytes/sec against an 8 KiB produce is about a minute of debt, so the
    // window saturates at the configured maximum.
    let (handle, _dir) =
        broker_with_anonymous_quotas(mute_window, &[("producer_byte_rate", 128.0)]).await;
    let (server, mut framed) = connect_to_serve_loop(&handle).await;

    let produced_at = Instant::now();
    send_request(
        &mut framed,
        PRODUCE_KEY,
        PRODUCE_VERSION,
        1,
        &produce_body("no-such-topic", 0, 1024, 8),
    )
    .await;
    send_api_versions(&mut framed, 2).await;

    // Nothing at all for the window: no produce response, because `acks=0`
    // asks for none, and no `ApiVersions` response, because the produce muted
    // the connection.
    let (frame, muted_for) = read_after_mute(&mut framed, produced_at).await;
    check!(
        response_correlation_id(&frame) == 2,
        "the only response must be the ApiVersions one; acks=0 writes no produce response"
    );
    check!(muted_for >= mute_window.to_std().saturating_sub(SLACK));

    drop(framed);
    server.await.expect("serve loop joins on client EOF");
    handle.shutdown().await;
}

/// A request that trips two quotas is muted once, for the longer window, not
/// once per quota and not for their sum.
///
/// A produce charges both `producer_byte_rate` and `request_percentage`. Both
/// are seeded far below what this request needs, so both saturate at
/// `quota_throttle_max` and the two windows are equal — which makes summing
/// observably different from taking the longest. Kafka reports one
/// `throttle_time_ms` and mutes the channel once, so the response must carry a
/// single window and the connection must be readable again after one.
#[tokio::test]
async fn a_request_tripping_two_quotas_is_muted_once_for_the_longest_window() {
    let mute_window = millis(1000);
    let (handle, _dir) = broker_with_anonymous_quotas(
        mute_window,
        &[
            ("producer_byte_rate", 128.0),
            ("request_percentage", 0.0001),
        ],
    )
    .await;
    let (server, mut framed) = connect_to_serve_loop(&handle).await;

    send_request(
        &mut framed,
        PRODUCE_KEY,
        PRODUCE_VERSION,
        1,
        &produce_body("no-such-topic", 1, 1024, 8),
    )
    .await;
    let first = tokio::time::timeout(CLIENT_TIMEOUT, framed.next())
        .await
        .expect("throttled produce must beat the client timeout, not wait out the window")
        .expect("a response frame")
        .expect("response decode");
    let answered_at = Instant::now();
    check!(response_correlation_id(&first) == 1);

    // One window on the wire, not two summed.
    let produce: ProduceResponse = decode_response_body(&first, PRODUCE_VERSION);
    let reported = millis(u32::try_from(produce.throttle_time_ms).expect("a window"));
    check!(reported == mute_window);

    send_api_versions(&mut framed, 2).await;
    let (second, muted_for) = read_after_mute(&mut framed, answered_at).await;
    check!(response_correlation_id(&second) == 2);
    check!(muted_for >= mute_window.to_std().saturating_sub(SLACK));
    check!(
        muted_for < ONE_WINDOW_CEILING,
        "two quotas must mute for one window, not for their sum"
    );

    drop(framed);
    server.await.expect("serve loop joins on client EOF");
    handle.shutdown().await;
}
