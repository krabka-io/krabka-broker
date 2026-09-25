//! Tests for the internal-topic gate.
//!
//! Kafka's `ReplicaManager.appendToLocalLog` refuses every partition of a
//! `Topic.isInternal` topic with `InvalidTopicException` (17) unless the
//! request's `client_id` is `"__admin_client"`, so the produce path resolves
//! that once per topic beside the freeze and refuses each partition row
//! before it parses the batch.

use std::{path::PathBuf, sync::Arc, time::Duration};

use assert2::check;
use bytes::Bytes;
use krabka_compression::RecordDecompressionPolicy;
use krabka_protocol::{
    owned::produce_response::PartitionProduceResponse,
    records::{Record, RecordBatch},
};

use super::super::{
    BrokerProducePolicy, FramedPartition, PartitionInput, PartitionServices, TimestampPolicy,
    process_partition,
};
use crate::{
    config::BrokerConfig,
    handlers::produce::{
        framing::PartitionPayload,
        test_support::{encode_batch, image_with_topic},
    },
    internal_topics::{is_internal_topic, produce_internal_topics_allowed},
};

/// The same resolve the produce handler runs once per topic, spelled out for
/// the table below rather than copied by hand into every case.
fn internal_topic_denied(config: &BrokerConfig, topic: &str, client_id: &str) -> bool {
    is_internal_topic(config, topic) && !produce_internal_topics_allowed(client_id)
}

/// Kafka's three coordinator topics plus krabka's own broker-owned topics are
/// denied for every `client_id` but the admin-tooling exception; an ordinary
/// topic is never denied, whatever the `client_id`.
#[test]
fn only_the_admin_client_may_produce_to_an_internal_topic() {
    let config = BrokerConfig::for_tests(PathBuf::from("/nonexistent"));
    let cases = [
        (
            "the offsets topic, an ordinary application",
            "__consumer_offsets",
            "my-app",
            true,
        ),
        (
            "the offsets topic, the admin client",
            "__consumer_offsets",
            "__admin_client",
            false,
        ),
        (
            "the transaction state topic, an ordinary application",
            "__transaction_state",
            "my-app",
            true,
        ),
        (
            "the share group state topic, an ordinary application",
            "__share_group_state",
            "my-app",
            true,
        ),
        (
            "an ordinary topic, an ordinary application",
            "orders",
            "my-app",
            false,
        ),
        (
            "an ordinary topic, even the admin client id",
            "orders",
            "__admin_client",
            false,
        ),
        (
            "the offsets topic, the empty client id null decodes to",
            "__consumer_offsets",
            "",
            true,
        ),
    ];
    for (label, topic, client_id, expected) in cases {
        check!(
            internal_topic_denied(&config, topic, client_id) == expected,
            "case: {label}"
        );
    }
}

/// A client Produce to `__consumer_offsets` is refused with
/// `INVALID_TOPIC_EXCEPTION` (17) and appends nothing; the admin client's
/// Produce to the same topic, and an ordinary topic in the same request,
/// both append normally.
///
/// The log-end-offset assertions are the load-bearing ones, the same way
/// they are for the freeze gate: the gate sits ahead of `prepare_batch`, so a
/// refused row must leave the partition exactly as it found it.
#[tokio::test]
async fn a_denied_internal_topic_is_refused_and_its_log_end_offset_does_not_move() {
    let dir = tempfile::tempdir().expect("log root");
    let config = BrokerConfig::for_tests(PathBuf::from("/nonexistent"));
    let image = Arc::new(image_with_topic("__consumer_offsets", &[1]));

    let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
    let txn_coordinator = Arc::new(crate::txn::coordinator::TxnCoordinator::new(
        krabka_audit::NodeId(1),
        Arc::clone(&partitions),
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        50,
        krabka_units::mebibytes(1),
    ));
    let producer_state = Arc::new(crate::producer_state::ProducerState::new());
    let log_dir_status = crate::log_dir_status::LogDirRegistry::default();
    let metrics = crate::metrics::BrokerMetrics::new();

    let part_dir = crate::log_dir::partition_dir(dir.path(), "__consumer_offsets", 0);
    std::fs::create_dir_all(&part_dir).expect("partition directory");
    let log =
        krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default()).expect("open the log");
    let part = crate::broker::spawn_partition(
        "__consumer_offsets".to_string(),
        krabka_ids::PartitionIndex(0),
        dir.path().to_path_buf(),
        log,
        log_dir_status.clone(),
        Arc::clone(&producer_state),
        false,
    );
    let topic_id = image.topic("__consumer_offsets").expect("topic").topic_id;
    let record = image.partition("__consumer_offsets", 0).expect("partition");
    part.install_replication_target(Some(topic_id), record.leader.0, record.leader_epoch.0)
        .await;
    part.install_isr(&record.isr, &record.replicas, record.leader)
        .await;
    part.log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut seed_batch())
        .expect("seed the partition");
    partitions.insert(
        "__consumer_offsets".into(),
        krabka_ids::PartitionIndex(0),
        part,
    );

    let cases = [
        (
            "an ordinary application is refused before any append",
            "my-app",
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::INVALID_TOPIC_EXCEPTION,
                base_offset: -1,
                ..Default::default()
            },
        ),
        (
            "the admin client appends normally",
            "__admin_client",
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::NONE,
                base_offset: 1,
                log_start_offset: 0,
                ..Default::default()
            },
        ),
    ];

    for (label, client_id, want) in cases {
        let resp = process_partition(
            PartitionInput {
                part_data: FramedPartition {
                    index: 0,
                    payload: PartitionPayload::Slice(encode_batch(&seed_batch())),
                },
                topic_compression: None,
                timestamps: TimestampPolicy::default(),
                compacted_topic: false,
                max_message_bytes: krabka_log::DEFAULT_MAX_MESSAGE_SIZE,
                delivery: None,
                schema: None,
                topic_name: "__consumer_offsets".into(),
                freeze: crate::freeze::resolve::FreezeMutationResolution::Admit,
                internal_topic_denied: internal_topic_denied(
                    &config,
                    "__consumer_offsets",
                    client_id,
                ),
                transaction: crate::handlers::produce::producer_checks::TransactionRequest {
                    transactional_id: None,
                    version: 9,
                    producer_id_expiration_ms: 86_400_000,
                },
                acks: 1,
                timeout: Duration::from_secs(5),
            },
            PartitionServices {
                partitions: &partitions,
                txn_coordinator: &txn_coordinator,
                producer_state: &producer_state,
                log_dir_status: &log_dir_status,
                image: &image,
                broker_policy: BrokerProducePolicy {
                    node_id: krabka_audit::NodeId(1),
                    default_min_insync_replicas: 1,
                    is_witness: false,
                },
                record_decompression_policy: RecordDecompressionPolicy::default(),
                metrics: &metrics,
                phases: &crate::metrics::RequestPhases::default(),
                schema_validator: None,
            },
        )
        .await
        .expect("process partition")
        .expect_done();
        check!(resp == want, "case: {label}");
    }

    check!(
        partitions
            .get("__consumer_offsets", krabka_ids::PartitionIndex(0))
            .expect("the partition is registered")
            .log_end_offset()
            == krabka_log::Offset(2),
        "the seed batch plus the admin client's one append; the ordinary \
         application's refused batch must not have landed"
    );
}

// One record, enough to seed a partition or to be refused.
fn seed_batch() -> RecordBatch {
    RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    }
}
