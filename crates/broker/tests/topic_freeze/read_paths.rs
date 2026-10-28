//! A write freeze leaves reads, discovery, metrics, and consumer commits working.

use {
    crate::{
        control_plane::freeze_scope,
        support::{
            self, InProcess,
            client::{connect_to_broker, scrape_metrics},
            discovery::named_topic_metadata,
            fetch::single_partition_fetch,
            offsets::{offset_commit_partition, offset_commit_topic},
            records::record_count,
        },
        wire::{
            CONTROL, ProduceOutcome, accepted, create_topic, frozen_topic_with_record,
            produce_outcome, refused,
        },
    },
    assert2::{assert, check},
    krabka_broker::{Broker, BrokerConfig, codes},
    krabka_protocol::{
        krabka::freeze::PATTERN_TYPE_LITERAL,
        owned::{
            fetch_response::FetchResponse,
            metadata_response::MetadataResponse,
            offset_commit_request::{OffsetCommitRequest, OffsetCommitRequestPartition},
        },
        primitives::uuid::Uuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct FrozenReads {
    broker: Option<InProcess>,
    frozen: Uuid,
    control: Uuid,
    fetch: Option<FetchResponse>,
    metadata: Option<MetadataResponse>,
    metrics: Option<String>,
    commits: Vec<(String, i16)>,
    control_write: Option<ProduceOutcome>,
}

impl FrozenReads {
    fn broker(&self) -> &InProcess {
        self.broker
            .as_ref()
            .expect("the read-path broker is running")
    }

    async fn shutdown(self) {
        self.broker
            .expect("the read-path broker is running")
            .broker
            .shutdown()
            .await;
    }
}

#[fixture]
fn frozen_reads() -> FrozenReads {
    // Each scenario owns fresh services and observations.
    FrozenReads {
        broker: None,
        ..Default::default()
    }
}

#[given("orders contains a frozen record and the broker exposes metrics")]
async fn frozen_metrics_topic(frozen_reads: &mut FrozenReads) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut config = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    config.metrics_listen_addr = Some("127.0.0.1:0".parse().expect("a loopback address"));
    let broker = Broker::start(config).await.expect("broker start");
    let client = connect_to_broker(&broker).await;
    let (frozen, control) = frozen_topic_with_record(&broker, &client, "orders", "cutover").await;
    frozen_reads.broker = Some(InProcess {
        broker,
        client,
        _tempdir: tempdir,
    });
    frozen_reads.frozen = frozen;
    frozen_reads.control = control;
}

#[given("orders contains a frozen record and the group coordinator is ready")]
async fn frozen_commit_topic(frozen_reads: &mut FrozenReads) {
    let broker = support::start().await;
    broker.broker.wait_until_group_coordinator_ready().await;
    let frozen = create_topic(&broker.broker, &broker.client, "orders").await;
    let control = create_topic(&broker.broker, &broker.client, CONTROL).await;
    check!(produce_outcome(&broker.broker, &broker.client, "orders", frozen).await == accepted(1));
    freeze_scope(&broker.client, PATTERN_TYPE_LITERAL, "orders", "cutover").await;
    check!(
        produce_outcome(&broker.broker, &broker.client, "orders", frozen).await
            == refused("literal", "orders", "cutover", 1)
    );
    frozen_reads.broker = Some(broker);
    frozen_reads.frozen = frozen;
    frozen_reads.control = control;
}

#[when("a consumer fetches orders from offset zero after its freeze")]
async fn fetch_frozen_records(frozen_reads: &mut FrozenReads) {
    let request = single_partition_fetch()
        .topic("orders".into())
        .topic_id(frozen_reads.frozen)
        .call();
    frozen_reads.fetch = Some(
        frozen_reads
            .broker()
            .client
            .send(request)
            .await
            .expect("Fetch"),
    );
}

#[then("the frozen topic still returns its one existing record without an error")]
fn frozen_record_is_readable(frozen_reads: &FrozenReads) {
    let response = frozen_reads.fetch.as_ref().expect("orders was fetched");
    assert!(response.error_code == codes::NONE, "Fetch: {response:?}");
    let partition = &response.responses[0].partitions[0];
    assert!(
        partition.error_code == codes::NONE,
        "Fetch partition: {partition:?}"
    );
    check!(record_count(partition.records.as_ref()) == 1);
}

#[when("a client requests metadata for frozen orders")]
async fn request_frozen_metadata(frozen_reads: &mut FrozenReads) {
    frozen_reads.metadata = Some(
        frozen_reads
            .broker()
            .client
            .send(named_topic_metadata("orders"))
            .await
            .expect("Metadata"),
    );
}

#[then("metadata still exposes the frozen topic and its one partition")]
fn frozen_metadata_is_visible(frozen_reads: &FrozenReads) {
    let topic = &frozen_reads
        .metadata
        .as_ref()
        .expect("Metadata completed")
        .topics[0];
    check!(topic.error_code == codes::NONE, "Metadata: {topic:?}");
    check!(topic.partitions.len() == 1);
}

#[when("an observer scrapes the broker metrics after the freeze is published")]
async fn scrape_freeze_metrics(frozen_reads: &mut FrozenReads) {
    let broker = &frozen_reads.broker().broker;
    broker
        .wait_for_metrics("topic_freezes_active reaches 1", |metrics| {
            metrics.topic_freezes_active.get() == 1
        })
        .await;
    frozen_reads.metrics = Some(
        scrape_metrics(
            broker
                .metrics_addr()
                .expect("the metrics listener is bound"),
        )
        .await,
    );
}

#[then("metrics report one active freeze and one rejected orders write")]
fn freeze_metrics_are_visible(frozen_reads: &FrozenReads) {
    let body = frozen_reads.metrics.as_ref().expect("metrics were scraped");
    for needle in [
        "krabka_broker_topic_freezes_active 1",
        "krabka_broker_topic_freeze_rejections_total{topic=\"orders\"} 1",
    ] {
        check!(body.contains(needle), "missing {needle} in:\n{body}");
    }
}

#[when("drainers commit offset one for frozen orders and the unfrozen control")]
async fn commit_draining_offsets(frozen_reads: &mut FrozenReads) {
    for (label, topic, id) in [
        ("the frozen topic", "orders", frozen_reads.frozen),
        ("the control topic", CONTROL, frozen_reads.control),
    ] {
        let response = frozen_reads
            .broker()
            .client
            .send(OffsetCommitRequest {
                group_id: "drainers".into(),
                generation_id_or_member_epoch: -1,
                member_id: String::new(),
                topics: vec![offset_commit_topic(topic, id, vec![
                    OffsetCommitRequestPartition {
                        committed_leader_epoch: -1,
                        ..offset_commit_partition(0, 1, Some(String::new()))
                    },
                ])],
                ..Default::default()
            })
            .await
            .expect("OffsetCommit");
        frozen_reads.commits.push((
            label.to_owned(),
            response.topics[0].partitions[0].error_code,
        ));
    }
}

#[then("both frozen and unfrozen consumer offset commits succeed")]
fn draining_commits_succeed(frozen_reads: &FrozenReads) {
    assert!(frozen_reads.commits.len() == 2);
    for (label, code) in &frozen_reads.commits {
        check!(*code == codes::NONE, "{label}");
    }
}

#[when("the read-path control topic receives a new record")]
async fn write_read_path_control(frozen_reads: &mut FrozenReads) {
    frozen_reads.control_write = Some(
        produce_outcome(
            &frozen_reads.broker().broker,
            &frozen_reads.broker().client,
            CONTROL,
            frozen_reads.control,
        )
        .await,
    );
}

#[then("the read-path control write is accepted at log end one")]
fn read_path_control_is_writable(frozen_reads: &FrozenReads) {
    check!(
        frozen_reads
            .control_write
            .as_ref()
            .expect("control was written")
            == &accepted(1)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/read_paths.feature",
    name = "fetch metadata and the metrics endpoint still answer for a frozen topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_metadata_and_the_metrics_endpoint_still_answer_for_a_frozen_topic(
    frozen_reads: FrozenReads,
) {
    frozen_reads.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/topic_freeze/read_paths.feature",
    name = "offset commit still works against a frozen topic"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_commit_still_works_against_a_frozen_topic(frozen_reads: FrozenReads) {
    frozen_reads.shutdown().await;
}
