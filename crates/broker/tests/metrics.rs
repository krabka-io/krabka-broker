#![cfg(test)]

//! Prometheus `/metrics` HTTP endpoint.
//!
//! This test boots a broker with a metrics listener on `127.0.0.1:0`, scrapes
//! `/metrics`, exercises the Produce wire path, and verifies that the
//! topic-labelled counters go up. It proves that the chain from the registry,
//! to the handler, to the HTTP endpoint works end to end.
//!
//! It is gated to non-Windows. The broker handle's `metrics_addr()` is for
//! Linux and macOS by convention, which matches the other integration tests.

mod kafka_wire;

mod support;

use {
    crate::support::{
        client::{metrics_config, scrape_metrics},
        fetch::single_partition_fetch,
        records::{batch_from_records, value_record},
        retry::retry_until,
        topics::{creatable_topic, create_topic_request},
        wire::HeaderEncoding as RequestHeaderEncoding,
    },
    assert2::{assert, check},
    bytes::BytesMut,
    krabka_broker::{Broker, BrokerConfig, metrics::PartitionLabel},
    krabka_protocol::{
        Decode, Encode,
        owned::{
            create_topics_response::CreateTopicsResponse,
            produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
            produce_response::ProduceResponse,
        },
        primitives::uuid::Uuid,
    },
    std::{net::SocketAddr, time::Duration},
    tokio::{net::TcpStream, time},
};

const TOPIC: &str = "metrics-it";
const FETCH_VERSION: i16 = 12;
const PRODUCE_VERSION: i16 = 9;
const CREATE_TOPICS_VERSION: i16 = 7;

/// The client id every request header in this suite carries.
const CLIENT_ID: &str = "krabka-metrics-test";

async fn create_topic(addr: SocketAddr) { create_topic_named(addr, TOPIC).await; }

async fn create_topic_named(addr: SocketAddr, topic: &str) {
    let req = create_topic_request(creatable_topic().name((topic).into()).call());
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_TOPICS_VERSION).unwrap();
    let resp = kafka_wire::round_trip()
        .stream(&mut stream)
        .api_key(19)
        .version(CREATE_TOPICS_VERSION)
        .client_id(CLIENT_ID)
        .header(RequestHeaderEncoding::Flexible)
        .body(&body)
        .call()
        .await
        .unwrap();
    let mut cur: &[u8] = &resp;
    let r = CreateTopicsResponse::decode(&mut cur, CREATE_TOPICS_VERSION).unwrap();
    assert!(r.topics[0].error_code == 0, "create: {:?}", r.topics[0]);
}

/// What a produce to one partition answered with.
struct ProduceOutcome {
    /// The partition's error code.
    error_code: i16,
}

async fn produce_to(addr: SocketAddr, topic: &str, partition: i32) -> ProduceOutcome {
    let batch = batch_from_records(vec![value_record(
        0,
        Some(bytes::Bytes::from_static(b"hello")),
    )]);
    let part = PartitionProduceData {
        index: partition,
        records: Some(batch.into()),
        ..Default::default()
    };
    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.into(),
            partition_data: vec![part],
            ..Default::default()
        }],
        ..Default::default()
    };
    let (_body_len, r) = kafka_wire::request_once::<_, ProduceResponse>(
        addr,
        &req,
        (0, PRODUCE_VERSION),
        CLIENT_ID,
        (1, true),
    )
    .await;
    let topic = r.responses.into_iter().next().expect("one topic in resp");
    let part = topic
        .partition_responses
        .into_iter()
        .next()
        .expect("one partition in resp");
    ProduceOutcome {
        error_code: part.error_code,
    }
}

async fn fetch_one(addr: SocketAddr) -> FetchResponse {
    let req = single_partition_fetch()
        .topic(TOPIC.into())
        .topic_id(Uuid::default())
        .call();
    let (_, r): (usize, FetchResponse) =
        kafka_wire::request_once(addr, &req, (1, FETCH_VERSION), CLIENT_ID, (1, true)).await;
    r
}

async fn scrape(addr: SocketAddr) -> String { scrape_metrics(addr).await }

/// Scrapes `/metrics` until `needle` has left the body, and returns that body.
///
/// The series evictor runs on every metadata image change and on its own 30 s
/// tick, so the bound is the harness's usual 30 s awaiter bound, which covers
/// one tick. The 5 s bound this wait had before failed on a runner whose own
/// log said its heartbeats starved beside a compile (#1831). The scrape is
/// repeated at the retry helper's interval rather than in a tight loop, so
/// that the wait itself does not take the cores the evictor needs.
async fn wait_until_absent(addr: SocketAddr, needle: &str) -> String {
    retry_until()
        .attempt(|| scrape(addr))
        .is_complete(|body: &String| !body.contains(needle))
        .timeout(Duration::from_secs(30))
        .call()
        .await
}

use {
    krabka_broker::BrokerHandle,
    krabka_protocol::owned::fetch_response::FetchResponse,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};
struct MetricEndpoint {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    body: Option<String>,
    produced: Option<ProduceOutcome>,
    fetched: Option<FetchResponse>,
}
#[fixture]
fn endpoint() -> MetricEndpoint {
    // Start this scenario with fresh inputs and observations.

    MetricEndpoint {
        broker: None,
        directory: None,
        body: None,
        produced: None,
        fetched: None,
    }
}
async fn start_endpoint(endpoint: &mut MetricEndpoint, directory: TempDir, config: BrokerConfig) {
    endpoint.broker = Some(Broker::start(config).await.expect("metrics broker starts"));
    endpoint.directory = Some(directory);
}
#[given("a broker exposes its default metric families before any client traffic")]
async fn default_metrics(endpoint: &mut MetricEndpoint) {
    let directory = tempfile::tempdir().expect("log directory");
    let config = metrics_config(directory.path().to_path_buf());
    start_endpoint(endpoint, directory, config).await;
}
#[given("a broker exposes metrics and scans partition disk usage once per second")]
async fn disk_metrics(endpoint: &mut MetricEndpoint) {
    let directory = tempfile::tempdir().expect("log directory");
    let mut config = metrics_config(directory.path().to_path_buf());
    config.partition_disk_scan_interval = krabka_units::secs(1);
    start_endpoint(endpoint, directory, config).await;
}
#[given("a broker exposes metrics with client label eviction enabled by its default configuration")]
async fn eviction_metrics(endpoint: &mut MetricEndpoint) {
    let directory = tempfile::tempdir().expect("log directory");
    let mut config = BrokerConfig::for_tests(directory.path().to_path_buf());
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().unwrap());
    start_endpoint(endpoint, directory, config).await;
}
#[when("the operator scrapes the OpenMetrics endpoint")]
async fn scrape_endpoint(endpoint: &mut MetricEndpoint) {
    let address = endpoint
        .broker
        .as_ref()
        .expect("metrics broker")
        .metrics_addr()
        .expect("metrics listener");
    endpoint.body = Some(scrape(address).await);
}
#[then(
    "the initial exposition includes scalar gauges counters replica lag and the OpenMetrics EOF marker"
)]
fn initial_families(endpoint: &MetricEndpoint) {
    let body = endpoint.body.as_ref().expect("metrics scraped");
    for needle in [
        "krabka_broker_partitions_led",
        "krabka_broker_active_controller",
        "krabka_broker_isr_shrinks_total",
        "krabka_broker_isr_expands_total",
        // The replica-lag rollup is a scalar, so it renders before any
        // follower exists and shows that the family reached the registry.
        "krabka_broker_replica_lag_max_records",
    ] {
        assert!(body.contains(needle), "missing {needle} in:\n{body}");
    }
    assert!(body.contains("# EOF"), "no EOF marker in:\n{body}");
}
async fn produce_metric_record(endpoint: &mut MetricEndpoint) {
    let broker = endpoint.broker.as_ref().expect("metrics broker");
    create_topic(broker.listen_addr()).await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    endpoint.produced = Some(produce_to(broker.listen_addr(), TOPIC, 0).await);
}

#[when("the client creates a topic produces one record and fetches it after commitment")]
async fn drive_traffic(endpoint: &mut MetricEndpoint) {
    produce_metric_record(endpoint).await;
    let broker = endpoint.broker.as_ref().expect("metrics broker");
    let address = broker.listen_addr();
    broker.wait_until_high_watermark(TOPIC, 0, 1).await;
    endpoint.fetched = Some(fetch_one(address).await);
    broker
        .wait_for_metrics("partitions_led >=1", |metrics| {
            metrics.partitions_led.get() >= 1
        })
        .await;
}
#[then(
    "the successful traffic appears in topic counters controller event timings and live controller gauges"
)]
fn traffic_families(endpoint: &MetricEndpoint) {
    assert!(
        endpoint
            .produced
            .as_ref()
            .expect("produce completed")
            .error_code
            == 0
    );
    assert!(
        endpoint
            .fetched
            .as_ref()
            .expect("fetch completed")
            .error_code
            == 0
    );
    let body = endpoint
        .body
        .as_ref()
        .expect("post traffic metrics scraped");
    for needle in [
        "krabka_broker_topic_bytes_in_total",
        "krabka_broker_topic_bytes_out_total",
        "krabka_broker_topic_produce_requests_total",
        "krabka_broker_topic_fetch_requests_total",
        "krabka_broker_messages_in_total",
    ] {
        assert!(
            body.contains(needle),
            "post-traffic scrape missing {needle} in:\n{body}"
        );
    }
    // `produce_one` writes a single v2 record into the
    // topic, so `messages_in_total{topic=TOPIC}` must read exactly 1.
    let messages_needle = format!("krabka_broker_messages_in_total{{topic=\"{TOPIC}\"}} 1");
    check!(
        body.contains(&messages_needle),
        "messages_in_total should be 1 for the lone produced record, body:\n{body}"
    );
    check!(
        body.contains(&format!(
            "krabka_broker_topic_produce_requests_total{{topic=\"{TOPIC}\"}}"
        )),
        "produce-requests counter missing topic label:\n{body}"
    );
    check!(
        body.contains(&format!(
            "krabka_broker_topic_fetch_requests_total{{topic=\"{TOPIC}\"}}"
        )),
        "fetch-requests counter missing topic label:\n{body}"
    );
    // partitions_led >= 1 once the partition writer is up.
    let led_line = body
        .lines()
        .find(|l| l.starts_with("krabka_broker_partitions_led "))
        .expect("partitions_led series present");
    let value: i64 = led_line
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert!(
        value >= 1,
        "partitions_led should be >=1, got line: {led_line}"
    );
    // Single-node broker: it's its own controller leader.
    assert!(
        body.contains("krabka_broker_active_controller 1"),
        "active_controller should be 1 on single-node, body:\n{body}"
    );
    // The CreateTopics above went through this combined node's controller,
    // so its event queue has started events and timed their processing.
    for series in [
        "krabka_broker_controller_event_queue_operations_started_total",
        "krabka_broker_controller_event_queue_processing_time_seconds_count",
        "krabka_broker_controller_event_queue_time_seconds_count",
    ] {
        let sample = body
            .lines()
            .find_map(|line| line.strip_prefix(series)?.strip_prefix(' '))
            .and_then(|value| value.parse::<f64>().ok());
        check!(
            sample.is_some_and(|value| value >= 1.0),
            "{series} should count the controller's events, body:\n{body}"
        );
    }
}
#[when("one produced record materializes a local segment and the disk scanner observes its size")]
async fn drive_disk(endpoint: &mut MetricEndpoint) {
    produce_metric_record(endpoint).await;
    let broker = endpoint.broker.as_ref().expect("metrics broker");
    broker
        .wait_for_metrics("partition_disk_bytes >0", |metrics| {
            metrics
                .partition_disk_bytes
                .get_or_create(&PartitionLabel {
                    topic: TOPIC.into(),
                    partition: 0,
                })
                .get()
                > 0
        })
        .await;
}
#[then("successful production exposes topic and partition bytes CPU time and a disk usage gauge")]
fn partition_families(endpoint: &MetricEndpoint) {
    assert!(
        endpoint
            .produced
            .as_ref()
            .expect("produce completed")
            .error_code
            == 0
    );
    let body = endpoint.body.as_ref().expect("partition metrics scraped");
    // Topic-level still present.
    let topic_needle = format!("krabka_broker_topic_bytes_in_total{{topic=\"{TOPIC}\"}}");
    assert!(
        body.contains(&topic_needle),
        "missing topic-level bytes_in in:\n{body}"
    );

    // Partition-level present with non-zero value.
    let partition_needle =
        format!("krabka_broker_partition_bytes_in_total{{topic=\"{TOPIC}\",partition=\"0\"}}");
    assert!(
        body.contains(&partition_needle),
        "missing partition-level bytes_in in:\n{body}"
    );
    // Per-partition CPU micros counter must be emitted with
    // the topic/partition label set after the produce + fetch path
    // runs. Value is timing-dependent (could be 0 if the handler was
    // sub-microsecond), so we only assert presence of the series name,
    // not a specific value.
    let cpu_needle = format!("krabka_broker_partition_cpu_micros_total{{topic=\"{TOPIC}\"");
    assert!(
        body.contains(&cpu_needle),
        "missing partition_cpu_micros_total in:\n{body}"
    );
    // Confirm the value is non-zero.
    let part_line = body
        .lines()
        .find(|l| l.starts_with(&partition_needle))
        .expect("partition_bytes_in line present");
    let value: u64 = part_line
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert!(
        value > 0,
        "partition_bytes_in should be >0 after produce, got line: {part_line}"
    );

    // Disk gauge: emitted for at least one (topic, partition) of TOPIC.
    let disk_prefix = format!("krabka_broker_partition_disk_bytes{{topic=\"{TOPIC}\"");
    assert!(
        body.contains(&disk_prefix),
        "missing partition_disk_bytes for materialized partition in:\n{body}"
    );
}
#[when("the client produces to topic {topic} partition {partition}")]
async fn invalid_destination(endpoint: &mut MetricEndpoint, topic: String, partition: i32) {
    let address = endpoint
        .broker
        .as_ref()
        .expect("metrics broker")
        .listen_addr();
    endpoint.produced = Some(produce_to(address, &topic, partition).await);
}
#[then("the invalid produce destination is refused")]
fn refused(endpoint: &MetricEndpoint) {
    check!(
        endpoint
            .produced
            .as_ref()
            .expect("invalid produce completed")
            .error_code
            != 0
    );
}
#[when(
    "creating the valid topic triggers reconciliation and metrics are observed until the invented topic label disappears"
)]
async fn evict_topic(endpoint: &mut MetricEndpoint) {
    let broker = endpoint.broker.as_ref().expect("metrics broker");
    create_topic_named(broker.listen_addr(), TOPIC).await;
    broker.wait_until_partition_present(TOPIC, 0).await;
    endpoint.body = Some(
        wait_until_absent(
            broker.metrics_addr().expect("metrics listener"),
            "invented-topic",
        )
        .await,
    );
}
#[then("the invented topic does not remain in the metrics body")]
fn topic_evicted(endpoint: &MetricEndpoint) {
    check!(
        !endpoint
            .body
            .as_ref()
            .expect("eviction metrics observed")
            .contains("invented-topic")
    );
}
#[when(
    "creating another topic triggers reconciliation and metrics are observed until partition ninety nine disappears"
)]
async fn evict_partition(endpoint: &mut MetricEndpoint) {
    let broker = endpoint.broker.as_ref().expect("metrics broker");
    create_topic_named(broker.listen_addr(), "eviction-trigger").await;
    broker
        .wait_until_partition_present("eviction-trigger", 0)
        .await;
    let pair = format!("topic=\"{TOPIC}\",partition=\"99\"");
    endpoint.body =
        Some(wait_until_absent(broker.metrics_addr().expect("metrics listener"), &pair).await);
}
#[then("the rejected topic partition pair does not remain in the metrics body")]
fn partition_evicted(endpoint: &MetricEndpoint) {
    let pair = format!("topic=\"{TOPIC}\",partition=\"99\"");
    check!(
        !endpoint
            .body
            .as_ref()
            .expect("eviction metrics observed")
            .contains(&pair)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/metrics.feature",
    name = "Rejected topic and partition labels are removed during reconciliation"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_client_labels_leave_the_metrics_body(endpoint: MetricEndpoint) {
    endpoint.broker.expect("metrics broker").shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/metrics.feature",
    name = "OpenMetrics exposes initial families and records client traffic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_serves_openmetrics_and_counters_tick(endpoint: MetricEndpoint) {
    endpoint.broker.expect("metrics broker").shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/metrics.feature",
    name = "Partition traffic and disk usage appear alongside topic metrics"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_level_metrics_and_disk_gauge_render(endpoint: MetricEndpoint) {
    let _ = time::timeout(
        Duration::from_secs(10),
        endpoint.broker.expect("metrics broker").shutdown(),
    )
    .await;
}
