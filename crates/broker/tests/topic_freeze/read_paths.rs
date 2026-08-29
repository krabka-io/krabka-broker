//! The paths a freeze must leave working: fetch, metadata, metrics, and the
//! offset commits of a consumer that is still draining the frozen topic.
//!
//! "The cluster is up, every read works, and the broker must not accept a new
//! write" is the state this feature exists to give. A freeze that also broke
//! reads would be a deny ACL with extra steps, and one that stopped
//! `OffsetCommit` would strand every group at its last pre-freeze position, so
//! both are asserted rather than assumed.

use assert2::{assert, check};
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, codes};
use krabka_client_core::Client;
use krabka_protocol::{
    krabka::freeze::PATTERN_TYPE_LITERAL,
    owned::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        offset_commit_request::{
            OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    control_plane::freeze_scope,
    support,
    wire::{CONTROL, accepted, create_topic, produce_outcome, refused},
};

/// [`support::start`] with the Prometheus listener bound.
///
/// The harness leaves `metrics_listen_addr` unset, and the one case that
/// scrapes `/metrics` over HTTP needs a socket to scrape.
async fn start_with_metrics() -> (BrokerHandle, Client, tempfile::TempDir) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().expect("a loopback address"));
    let broker = Broker::start(config).await.expect("broker start");
    let client = Client::builder()
        .bootstrap(broker.listen_addr().to_string())
        .client_id("krabka-broker-test")
        .build()
        .await
        .expect("client build");
    (broker, client, tempdir)
}

/// Scrape the `OpenMetrics` body from the broker's `/metrics` endpoint.
async fn scrape(addr: std::net::SocketAddr) -> String {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let request = format!(
        "GET /metrics HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write the scrape request");
    stream.flush().await.expect("flush");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read the body");
    let text = String::from_utf8(buf).expect("a UTF-8 body");
    let body = text.find("\r\n\r\n").map_or(0, |i| i + 4);
    text[body..].to_owned()
}

/// The number of records a fetch from offset zero returns.
async fn fetch_record_count(client: &Client, topic: &str, topic_id: WireUuid) -> usize {
    let response = client
        .send(FetchRequest {
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: topic.into(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("Fetch");
    assert!(response.error_code == codes::NONE, "Fetch: {response:?}");
    let partition = &response.responses[0].partitions[0];
    assert!(
        partition.error_code == codes::NONE,
        "Fetch partition: {partition:?}"
    );
    partition
        .records
        .as_ref()
        .and_then(krabka_protocol::records::RecordsPayload::as_v2)
        .map_or(0, |batches| {
            batches.iter().map(|batch| batch.records.len()).sum()
        })
}

/// A frozen topic stays readable, stays visible, and stays observable.
///
/// "The cluster is up, every read works, and the broker must not accept a new
/// write" is the state this feature exists to give. A freeze that also broke
/// reads would be a deny ACL with extra steps, so the read paths are asserted
/// rather than assumed. The metrics half closes the gap KFC-7's suite found
/// late: both counters were declared, registered and documented, and a live
/// broker scraped zero for them, because nothing on a real request moved them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_metadata_and_the_metrics_endpoint_still_answer_for_a_frozen_topic() {
    let (broker, client, _dir) = start_with_metrics().await;
    let metrics_addr = broker
        .metrics_addr()
        .expect("the metrics listener is bound");
    let frozen = create_topic(&broker, &client, "orders").await;
    let control = create_topic(&broker, &client, CONTROL).await;
    check!(produce_outcome(&broker, &client, "orders", frozen).await == accepted(1));

    freeze_scope(&client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    check!(
        produce_outcome(&broker, &client, "orders", frozen).await
            == refused("literal", "orders", "cutover", 1)
    );

    // The record written before the freeze is still readable, and the topic is
    // still in the metadata a client routes on.
    check!(fetch_record_count(&client, "orders", frozen).await == 1);
    let metadata = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some("orders".into()),
                ..Default::default()
            }]),
            ..Default::default()
        })
        .await
        .expect("Metadata");
    let topic = &metadata.topics[0];
    check!(topic.error_code == codes::NONE, "Metadata: {topic:?}");
    check!(topic.partitions.len() == 1);

    broker
        .wait_for_metrics("topic_freezes_active reaches 1", |m| {
            m.topic_freezes_active.get() == 1
        })
        .await;
    let body = scrape(metrics_addr).await;
    for needle in [
        "krabka_broker_topic_freezes_active 1",
        "krabka_broker_topic_freeze_rejections_total{topic=\"orders\"} 1",
    ] {
        check!(body.contains(needle), "missing {needle} in:\n{body}");
    }

    check!(produce_outcome(&broker, &client, CONTROL, control).await == accepted(1));
    broker.shutdown().await;
}

/// A consumer of a frozen topic can still record where it got to.
///
/// `OffsetCommit` appends to `__consumer_offsets` and not to the frozen topic,
/// and a cutover is exactly when the reader positions matter most: the whole
/// point of freezing rather than deleting is that consumers drain the frozen
/// prefix and commit as they go. A freeze that stopped the commits would strand
/// every group at its last pre-freeze position.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_commit_still_works_against_a_frozen_topic() {
    let p = support::start().await;
    let frozen = create_topic(&p.broker, &p.client, "orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;
    check!(produce_outcome(&p.broker, &p.client, "orders", frozen).await == accepted(1));

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    check!(
        produce_outcome(&p.broker, &p.client, "orders", frozen).await
            == refused("literal", "orders", "cutover", 1)
    );

    for (label, topic, topic_id) in [
        ("the frozen topic", "orders", frozen),
        ("the control topic", CONTROL, control),
    ] {
        let response = p
            .client
            .send(OffsetCommitRequest {
                group_id: "drainers".into(),
                generation_id_or_member_epoch: -1,
                member_id: String::new(),
                topics: vec![OffsetCommitRequestTopic {
                    name: topic.into(),
                    topic_id,
                    partitions: vec![OffsetCommitRequestPartition {
                        partition_index: 0,
                        committed_offset: 1,
                        committed_leader_epoch: -1,
                        committed_metadata: Some(String::new()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .expect("OffsetCommit");
        check!(
            response.topics[0].partitions[0].error_code == codes::NONE,
            "{label}: {response:?}"
        );
    }

    p.broker.shutdown().await;
}
