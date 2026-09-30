//! The share-group dead-letter queue (KIP-1191), end to end.
//!
//! At `share.version` 2, a record that a member rejects is written to the
//! group's dead-letter topic before the share partition archives it. The
//! dead-letter record carries six headers that say where the record came from
//! and why it is there, and the key and value of the source record when the
//! group copies them. Kafka archives the record whatever becomes of the write,
//! so a group whose topic cannot be written still moves on.
//!
//! The queue is Kafka trunk's, so each broker here runs with
//! `unstable.api.versions.enable` and `unstable.feature.versions.enable` on.

use std::time::Duration;

use assert2::{assert, check};
use bytes::Bytes;
use krabka_broker::{BrokerConfig, BrokerHandle, api_catalog::UnstableApiVersions};
use krabka_client_core::Client;
use krabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        share_acknowledge_request::{
            AcknowledgePartition, AcknowledgeTopic, AcknowledgementBatch, ShareAcknowledgeRequest,
        },
        update_features_request::{FeatureUpdateKey, UpdateFeaturesRequest},
    },
    records::{Record, RecordBatch, RecordsPayload},
};

use crate::{
    NONE, REJECT,
    harness::{
        bootstrap_share_state, broker_config, broker_test_permit, connect, create_topic, join,
        produce_n, produce_values, topic_id, wait_for_share_init, wire,
    },
    share_rpc::{acquired_count, fetch_until_acquired, share_ack, share_fetch},
};

/// Kafka resource type ids of `GROUP` and `BROKER`.
const RESOURCE_TYPE_GROUP: i8 = 32;
const RESOURCE_TYPE_BROKER: i8 = 4;
/// `config_operation` SET = 0 in the `IncrementalAlterConfigs` wire protocol.
const CONFIG_OP_SET: i8 = 0;

const GROUP: &str = "g1";
const DLQ_TOPIC: &str = "dlq.g1";

/// A broker that serves what krabka implements from Kafka trunk, over a fresh
/// data directory.
fn trunk_config(dir: &tempfile::TempDir) -> BrokerConfig {
    let mut config = broker_config(dir.path().to_path_buf());
    config.features.unstable_api_versions = UnstableApiVersions::Enabled;
    config.features.unstable_feature_versions = krabka_raft::UnstableFeatureVersions::Enabled;
    config
}

/// Finalizes `share.version` at 2, the level of the dead-letter queue.
async fn finalize_share_version_two(client: &Client) {
    let response = client
        .send(UpdateFeaturesRequest {
            feature_updates: vec![FeatureUpdateKey {
                feature: "share.version".into(),
                max_version_level: 2,
                upgrade_type: 1,
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("UpdateFeatures");
    assert!(response.error_code == 0, "{response:?}");
}

/// Creates the dead-letter topic with the topic config that opts it in.
async fn create_dead_letter_topic(broker: &BrokerHandle, client: &Client) {
    create_dead_letter_topic_with(broker, client, &[]).await;
}

/// [`create_dead_letter_topic`], with `extra` topic configs beside the one that
/// opts the topic in.
async fn create_dead_letter_topic_with(
    broker: &BrokerHandle,
    client: &Client,
    extra: &[(&str, &str)],
) {
    let configs = std::iter::once(("errors.deadletterqueue.group.enable", "true"))
        .chain(extra.iter().copied())
        .map(|(name, value)| CreatableTopicConfig {
            name: name.into(),
            value: Some(value.into()),
            ..Default::default()
        })
        .collect();
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: DLQ_TOPIC.into(),
                num_partitions: 1,
                replication_factor: 1,
                configs,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(response.topics[0].error_code == 0, "{response:?}");
    broker.wait_until_partition_present(DLQ_TOPIC, 0).await;
}

/// Sets `configs` on the resource of `resource_type` and `resource_name`.
async fn set_configs(
    client: &Client,
    resource_type: i8,
    resource_name: &str,
    configs: &[(&str, &str)],
) {
    let response = client
        .send(IncrementalAlterConfigsRequest {
            resources: vec![AlterConfigsResource {
                resource_type,
                resource_name: resource_name.into(),
                configs: configs
                    .iter()
                    .map(|(name, value)| AlterableConfig {
                        name: (*name).into(),
                        config_operation: CONFIG_OP_SET,
                        value: Some((*value).into()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("IncrementalAlterConfigs");
    assert!(
        response.responses[0].error_code == 0,
        "{configs:?} rejected: {:?}",
        response.responses[0].error_message
    );
}

/// Gives the group its dead-letter topic, and asks it to copy the records.
async fn point_group_at_dead_letter_topic(client: &Client) {
    set_configs(
        client,
        RESOURCE_TYPE_GROUP,
        GROUP,
        &[
            ("errors.deadletterqueue.topic.name", DLQ_TOPIC),
            ("errors.deadletterqueue.copy.record.enable", "true"),
        ],
    )
    .await;
}

/// The records of the first batch of the dead-letter topic, once there is one.
async fn dead_letter_records(broker: &BrokerHandle, client: &Client) -> Vec<Record> {
    let dlq_id = topic_id(broker, DLQ_TOPIC);
    let response = client
        .send(FetchRequest {
            max_wait_ms: 100,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: DLQ_TOPIC.into(),
                topic_id: wire(dlq_id),
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
    let row = &response.responses[0].partitions[0];
    match row.records.as_ref() {
        Some(RecordsPayload::V2(batches)) => batches
            .iter()
            .flat_map(|batch| batch.records.clone())
            .collect(),
        Some(RecordsPayload::Raw(bytes)) => {
            let mut cursor = bytes.clone();
            let mut records = Vec::new();
            while !cursor.is_empty() {
                let batch = RecordBatch::decode(&mut cursor).expect("decode dead-letter batch");
                records.extend(batch.records);
            }
            records
        }
        _ => Vec::new(),
    }
}

/// Polls the dead-letter topic until it holds `count` records.
async fn wait_for_dead_letters(
    broker: &BrokerHandle,
    client: &Client,
    count: usize,
) -> Vec<Record> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let records = dead_letter_records(broker, client).await;
            if records.len() >= count {
                return records;
            }
            // intentional: bounded poll of a topic that a background task writes;
            // nothing signals the append to the test.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the dead-letter records reached the topic")
}

/// A header of `record` as text.
fn header(record: &Record, key: &str) -> Option<String> {
    record
        .headers
        .iter()
        .find(|header| header.key == key)
        .and_then(|header| header.value.as_ref())
        .map(|value| String::from_utf8_lossy(value).into_owned())
}

/// One `ShareAcknowledge` that rejects each of `ranges`, as `(first, last)`
/// offsets of partition 0, in a batch of its own.
async fn reject_ranges(
    client: &Client,
    member: &str,
    tid: uuid::Uuid,
    epoch: i32,
    ranges: &[(i64, i64)],
) {
    let response = client
        .send(ShareAcknowledgeRequest {
            group_id: Some(GROUP.into()),
            member_id: Some(member.into()),
            share_session_epoch: epoch,
            topics: vec![AcknowledgeTopic {
                topic_id: wire(tid),
                partitions: vec![AcknowledgePartition {
                    partition_index: 0,
                    acknowledgement_batches: ranges
                        .iter()
                        .map(|(first, last)| AcknowledgementBatch {
                            first_offset: *first,
                            last_offset: *last,
                            acknowledge_types: vec![
                                REJECT;
                                usize::try_from(last - first + 1).unwrap()
                            ],
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .await
        .expect("ShareAcknowledge");
    assert!(response.error_code == NONE, "{response:?}");
    assert!(
        response.responses[0].partitions[0].error_code == NONE,
        "{response:?}"
    );
}

/// What the `DeadLetterQueue*` meters of the group count on the broker: the
/// records written, the attempts to produce, and the writes that failed.
fn meters(metrics: &krabka_broker::metrics::BrokerMetrics) -> (u64, u64, u64) {
    let label = krabka_broker::metrics::ShareGroupIdLabel {
        group_id: GROUP.to_owned(),
    };
    (
        metrics.share_group_dlq_records.get_or_create(&label).get(),
        metrics
            .share_group_dlq_produce_requests
            .get_or_create(&label)
            .get(),
        metrics
            .share_group_dlq_failed_produce_requests
            .get_or_create(&label)
            .get(),
    )
}

/// Starts a trunk broker with a source topic `t` of `records` records and a
/// member that holds all of them acquired, then returns what a test drives.
struct Cluster {
    broker: BrokerHandle,
    client: std::sync::Arc<Client>,
    tid: uuid::Uuid,
    member: String,
    _dir: tempfile::TempDir,
}

async fn acquired_records(
    records: i64,
    tweak: impl FnOnce(&mut BrokerConfig),
    configure: impl AsyncFnOnce(&Client, &BrokerHandle),
) -> Cluster {
    acquired_records_from(records, tweak, configure, async |client: &Client, tid| {
        produce_n(client, "t", tid, 0, records).await;
    })
    .await
}

/// [`acquired_records`], with the records that `produce` writes to `t` in place
/// of the small ones that `produce_n` writes.
async fn acquired_records_from(
    records: i64,
    tweak: impl FnOnce(&mut BrokerConfig),
    configure: impl AsyncFnOnce(&Client, &BrokerHandle),
    produce: impl AsyncFnOnce(&Client, uuid::Uuid),
) -> Cluster {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = trunk_config(&dir);
    tweak(&mut config);
    let broker = krabka_broker::Broker::start(config).await.unwrap();
    let client = connect(&broker.listen_addr().to_string()).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, GROUP).await;
    configure(&client, &broker).await;
    produce(&client, tid).await;
    let (member, member_epoch) = join(&client, GROUP, "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;
    let row = fetch_until_acquired(&client, GROUP, &member, tid, 0, 0).await;
    assert!(acquired_count(&row) == records, "{row:?}");
    Cluster {
        broker,
        client,
        tid,
        member,
        _dir: dir,
    }
}

/// Kafka's `initiateDLQAndArchive`, end to end: a rejected batch is written to
/// the group's topic, one record for each offset, with the six headers and the
/// copied key and value, and only then does the SPSO move past it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_batch_is_dead_lettered_then_archived() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records(
        3,
        |_| {},
        async |client, broker| {
            finalize_share_version_two(client).await;
            create_dead_letter_topic(broker, client).await;
            point_group_at_dead_letter_topic(client).await;
        },
    )
    .await;
    let Cluster {
        broker,
        client,
        tid,
        member,
        ..
    } = &cluster;

    let ack = share_ack(client, member, *tid, 1, 0, 2, REJECT).await;
    assert!(ack.error_code == NONE, "reject: {}", ack.error_code);
    let records = wait_for_dead_letters(broker, client, 3).await;
    broker.wait_until_share_spso(GROUP, *tid, 0, 3).await;

    let seen: Vec<_> = records
        .iter()
        .map(|record| {
            (
                record.value.clone(),
                header(record, "__dlq.errors.topic"),
                header(record, "__dlq.errors.partition"),
                header(record, "__dlq.errors.offset"),
                header(record, "__dlq.errors.group"),
                header(record, "__dlq.errors.delivery.count"),
                header(record, "__dlq.errors.message"),
            )
        })
        .collect();
    let expected: Vec<_> = (0..3)
        .map(|offset| {
            (
                Some(Bytes::from(format!("v{offset}"))),
                Some("t".to_owned()),
                Some("0".to_owned()),
                Some(offset.to_string()),
                Some(GROUP.to_owned()),
                Some("1".to_owned()),
                Some("Offset rejected by client.".to_owned()),
            )
        })
        .collect();
    check!(seen == expected);
    let summary = broker.share_state_summary_for_test(GROUP, *tid, 0).await;
    check!(summary.map(|(_, _, spso, _)| spso) == Some(3));
    // Kafka's `recordDLQRecordWrite` and `recordDLQProduce`: the three records
    // of the one round, and one attempt to produce it.
    broker
        .wait_for_metrics("the dead-letter meters", |metrics| {
            meters(metrics) == (3, 1, 0)
        })
        .await;
    cluster.broker.shutdown().await;
}

/// Runs that are not neighbours are a write each, and the rounds of the writes
/// share the produce requests to the leader of the dead-letter partition. One
/// record comes out for each offset, and the meters count each round as a
/// produce however the rounds were packed into requests: Kafka's
/// `recordDLQProduce` marks each handler, not each coalesced request.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_that_are_not_neighbours_are_all_dead_lettered_and_counted() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records(
        5,
        |_| {},
        async |client, broker| {
            finalize_share_version_two(client).await;
            create_dead_letter_topic(broker, client).await;
            point_group_at_dead_letter_topic(client).await;
        },
    )
    .await;
    let Cluster {
        broker,
        client,
        tid,
        member,
        ..
    } = &cluster;

    reject_ranges(client, member, *tid, 1, &[(0, 0), (2, 2), (4, 4)]).await;
    let mut records = wait_for_dead_letters(broker, client, 3).await;
    broker
        .wait_for_metrics("the dead-letter meters", |metrics| {
            meters(metrics) == (3, 3, 0)
        })
        .await;
    // The records of the rounds that were packed into one request are one
    // batch, and the order of the requests is the writers' race.
    records.sort_by_key(|record| header(record, "__dlq.errors.offset"));

    check!(
        records
            .iter()
            .map(|record| (
                header(record, "__dlq.errors.offset"),
                record.value.clone(),
                header(record, "__dlq.errors.delivery.count"),
                header(record, "__dlq.errors.message"),
            ))
            .collect::<Vec<_>>()
            == [0, 2, 4]
                .map(|offset| (
                    Some(offset.to_string()),
                    Some(Bytes::from(format!("v{offset}"))),
                    Some("1".to_owned()),
                    Some("Offset rejected by client.".to_owned()),
                ))
                .to_vec()
    );
    cluster.broker.shutdown().await;
}

/// A write that the topic refuses counts as a failed write, and the run is
/// archived regardless. The topic takes no more than 100 bytes for a message,
/// and a dead-letter record with its six headers is more, so the leader answers
/// `MESSAGE_TOO_LARGE`, which Kafka's `handleProduceResponse` does not retry.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_the_topic_refuses_counts_as_failed_and_still_archives() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records(
        1,
        |_| {},
        async |client, broker| {
            finalize_share_version_two(client).await;
            create_dead_letter_topic_with(broker, client, &[("max.message.bytes", "100")]).await;
            point_group_at_dead_letter_topic(client).await;
        },
    )
    .await;
    let Cluster {
        broker,
        client,
        tid,
        member,
        ..
    } = &cluster;

    let ack = share_ack(client, member, *tid, 1, 0, 0, REJECT).await;
    assert!(ack.error_code == NONE, "reject: {}", ack.error_code);
    broker.wait_until_share_spso(GROUP, *tid, 0, 1).await;

    broker
        .wait_for_metrics("the dead-letter meters", |metrics| {
            meters(metrics) == (0, 1, 1)
        })
        .await;
    check!(dead_letter_records(broker, client).await.is_empty());
    cluster.broker.shutdown().await;
}

/// A record that uses up `share.delivery.count.limit` goes to the queue too,
/// with the delivery count as its cause: here its lock runs out on the second
/// delivery, and the record is written with a delivery count of 2 and Kafka's
/// message for the cause.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_that_uses_up_its_deliveries_is_dead_lettered() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records(
        1,
        |config| {
            config.share_group.record_lock_duration = Duration::from_millis(150);
            config.share_group.max_delivery_attempts = 2;
        },
        async |client, broker| {
            finalize_share_version_two(client).await;
            create_dead_letter_topic(broker, client).await;
            point_group_at_dead_letter_topic(client).await;
        },
    )
    .await;
    let Cluster {
        broker,
        client,
        tid,
        member,
        ..
    } = &cluster;

    // Delivery 1 was taken by the helper. Its lock runs out, and the record is
    // available again for a second delivery, which nobody acknowledges.
    broker
        .wait_until_share_acquired_count(GROUP, *tid, 0, 0)
        .await;
    let second = share_fetch(client, GROUP, member, *tid, 0, 1, 0).await;
    assert!(
        acquired_count(&second) == 1 && second.acquired_records[0].delivery_count == 2,
        "{second:?}"
    );
    let records = wait_for_dead_letters(broker, client, 1).await;
    broker.wait_until_share_spso(GROUP, *tid, 0, 1).await;

    check!(
        (
            header(&records[0], "__dlq.errors.offset"),
            header(&records[0], "__dlq.errors.delivery.count"),
            header(&records[0], "__dlq.errors.message"),
            records[0].value.clone(),
        ) == (
            Some("0".to_owned()),
            Some("2".to_owned()),
            Some("Offset delivery count exceeded the threshold.".to_owned()),
            Some(Bytes::from_static(b"v0")),
        )
    );
    cluster.broker.shutdown().await;
}

/// The queue is off below `share.version` 2, so a reject archives at once and
/// nothing reaches the topic, even with the group and the topic set up: the
/// gate is the finalized level, not the config.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reject_below_share_version_two_writes_nothing() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records(
        2,
        |_| {},
        async |client, broker| {
            create_dead_letter_topic(broker, client).await;
            point_group_at_dead_letter_topic(client).await;
        },
    )
    .await;
    let Cluster {
        broker,
        client,
        tid,
        member,
        ..
    } = &cluster;

    let ack = share_ack(client, member, *tid, 1, 0, 1, REJECT).await;
    assert!(ack.error_code == NONE, "reject: {}", ack.error_code);
    broker.wait_until_share_spso(GROUP, *tid, 0, 2).await;
    // intentional: no signal says that a write that must not happen has not
    // happened yet, so the test gives it time before it reads the topic.
    tokio::time::sleep(Duration::from_millis(500)).await;

    check!(dead_letter_records(broker, client).await.is_empty());
    cluster.broker.shutdown().await;
}

/// A group whose topic cannot be written still moves on: Kafka archives the
/// record when the write fails, and logs the failure. Here the topic does not
/// exist and the cluster does not create it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_write_still_archives_the_record() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records(
        2,
        |_| {},
        async |client, _| {
            finalize_share_version_two(client).await;
            point_group_at_dead_letter_topic(client).await;
        },
    )
    .await;
    let Cluster {
        broker,
        client,
        tid,
        member,
        ..
    } = &cluster;

    let ack = share_ack(client, member, *tid, 1, 0, 1, REJECT).await;
    assert!(ack.error_code == NONE, "reject: {}", ack.error_code);
    broker.wait_until_share_spso(GROUP, *tid, 0, 2).await;

    check!(
        broker
            .controller_image_for_test()
            .topic(DLQ_TOPIC)
            .is_none()
    );
    // A topic that fails validation is refused before any request is made, so
    // no produce is counted, as in Kafka's `dlq`, which fails the future before
    // the handler is queued.
    broker
        .wait_for_metrics("no dead-letter produce", |metrics| {
            meters(metrics) == (0, 0, 0)
        })
        .await;
    cluster.broker.shutdown().await;
}

/// With `errors.deadletterqueue.auto.create.topics.enable`, a missing topic is
/// created with the config that opts it in, and the records are written to it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_missing_topic_is_created_when_the_cluster_allows_it() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records(
        1,
        |_| {},
        async |client, _| {
            finalize_share_version_two(client).await;
            set_configs(
                client,
                RESOURCE_TYPE_BROKER,
                "",
                &[("errors.deadletterqueue.auto.create.topics.enable", "true")],
            )
            .await;
            point_group_at_dead_letter_topic(client).await;
        },
    )
    .await;
    let Cluster {
        broker,
        client,
        tid,
        member,
        ..
    } = &cluster;

    let ack = share_ack(client, member, *tid, 1, 0, 0, REJECT).await;
    assert!(ack.error_code == NONE, "reject: {}", ack.error_code);
    broker.wait_until_partition_present(DLQ_TOPIC, 0).await;
    let records = wait_for_dead_letters(broker, client, 1).await;
    broker.wait_until_share_spso(GROUP, *tid, 0, 1).await;

    check!(records[0].value == Some(Bytes::from_static(b"v0")));
    check!(
        broker
            .controller_image_for_test()
            .topic_config(DLQ_TOPIC)
            .and_then(|configs| configs.get("errors.deadletterqueue.group.enable").cloned())
            == Some("true".to_owned())
    );
    cluster.broker.shutdown().await;
}

/// A source record that fits the read budget can be over the topic's
/// `max.message.bytes` once the six headers and the batch header are added. The
/// copy is best effort, so the record is written with its headers alone rather
/// than lost to the broker's `MESSAGE_TOO_LARGE`.
///
/// The source value is 1,000 bytes, which is about 1,010 bytes as a record and
/// fits the topic's 1,100. With the headers, which take about 180 bytes, and
/// the 61-byte batch header it is about 1,250 bytes as a batch, which does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_copied_record_too_big_for_the_topic_is_written_with_headers_alone() {
    let _permit = broker_test_permit().await;
    let cluster = acquired_records_from(
        1,
        |_| {},
        async |client, broker| {
            finalize_share_version_two(client).await;
            create_dead_letter_topic_with(broker, client, &[("max.message.bytes", "1100")]).await;
            point_group_at_dead_letter_topic(client).await;
        },
        async |client, tid| {
            let values = vec![Bytes::from(vec![b'x'; 1_000])];
            produce_values(client, "t", tid, 0, values).await;
        },
    )
    .await;
    let Cluster {
        broker,
        client,
        tid,
        member,
        ..
    } = &cluster;

    let ack = share_ack(client, member, *tid, 1, 0, 0, REJECT).await;
    assert!(ack.error_code == NONE, "reject: {}", ack.error_code);
    let records = wait_for_dead_letters(broker, client, 1).await;
    broker.wait_until_share_spso(GROUP, *tid, 0, 1).await;

    check!(
        records
            .iter()
            .map(|record| (
                record.key.clone(),
                record.value.clone(),
                header(record, "__dlq.errors.offset"),
                header(record, "__dlq.errors.message"),
            ))
            .collect::<Vec<_>>()
            == vec![(
                None,
                None,
                Some("0".to_owned()),
                Some("Offset rejected by client.".to_owned()),
            )]
    );
    cluster.broker.shutdown().await;
}
