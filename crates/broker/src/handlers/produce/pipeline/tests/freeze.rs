//! Tests for the KFC-9 topic write freeze.
//!
//! A freeze is an authority gate rather than a content gate, so the produce
//! path resolves it once per topic beside the compression, delivery and
//! schema resolves, and refuses each partition row of a frozen topic with
//! `POLICY_VIOLATION` (44) before it parses the batch.

use std::{sync::Arc, time::Duration};

use assert2::check;
use bytes::Bytes;
use krabka_compression::RecordDecompressionPolicy;
use krabka_metadata::{
    MetadataImage, MetadataRecord, PartitionRecord, PatternType, TopicFreezeRecord, TopicRecord,
};
use krabka_protocol::{
    owned::produce_response::PartitionProduceResponse,
    records::{Record, RecordBatch},
};
use uuid::Uuid;

use super::super::{
    BrokerProducePolicy, FramedPartition, PartitionInput, PartitionServices, process_partition,
};
use crate::{
    freeze::resolve::{FreezeVerdict, resolve_topic_freeze},
    handlers::produce::{
        framing::PartitionPayload,
        test_support::{encode_batch, image_with_topic},
    },
};

// Put one live entry in the registry for the resolve below to find.
fn frozen(image: &mut MetadataImage, scope: &str, pattern_type: PatternType, reason: &str) {
    image.apply(&MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
        scope: scope.to_owned(),
        pattern_type,
        frozen: true,
        reason: reason.to_owned(),
        set_by: "User:alice".to_owned(),
        set_at_ms: 1_770_000_000_000,
        proposal_id: Uuid::nil(),
        key_id: String::new(),
        signature: Vec::new(),
    }));
}

fn verdict(scope: &str, pattern_type: PatternType, reason: &str) -> FreezeVerdict {
    FreezeVerdict {
        scope: scope.to_owned(),
        pattern_type,
        reason: reason.to_owned(),
    }
}

// A second single-partition topic led by node 1, so one request can carry
// a frozen topic and an unfrozen control topic at once.
fn add_topic(image: &mut MetadataImage, topic: &str, topic_id: Uuid) {
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: topic.into(),
        topic_id,
        partitions: 1,
        replication_factor: 1,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: topic.into(),
        partition: 0,
        leader: krabka_audit::NodeId(1),
        replicas: vec![krabka_audit::NodeId(1)],
        isr: vec![krabka_audit::NodeId(1)],
        leader_epoch: krabka_metadata::LeaderEpoch(0),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 0,
    }));
}

#[test]
fn the_produce_path_resolves_one_freeze_per_topic() {
    let mut image = image_with_topic("orders", &[1]);
    frozen(&mut image, "orders", PatternType::Literal, "DR cutover");
    frozen(
        &mut image,
        "tenant-a.",
        PatternType::Prefixed,
        "offboarding",
    );

    let cases = [
        (
            "a literal freeze covers the one topic it names",
            "orders",
            Some(verdict("orders", PatternType::Literal, "DR cutover")),
        ),
        (
            "a prefix freeze covers every topic under it",
            "tenant-a.events",
            Some(verdict("tenant-a.", PatternType::Prefixed, "offboarding")),
        ),
        (
            "an unfrozen topic resolves no freeze at all",
            "payments",
            None,
        ),
    ];

    for (label, topic, want) in cases {
        check!(
            resolve_topic_freeze(&image, topic).map(FreezeVerdict::from) == want,
            "case: {label}"
        );
    }
}

/// A frozen topic's partition row comes back with `POLICY_VIOLATION` (44)
/// and a message that names the scope, and an unfrozen control topic in
/// the same request is untouched by it.
///
/// The log-end-offset assertions are the load-bearing ones. The gate sits
/// ahead of `prepare_batch` and ahead of the dedup gate, so a refused row
/// must leave the partition exactly as it found it. A refusal that still
/// appended is the worst failure this feature can have, and the error code
/// alone does not rule it out. Both partitions are seeded with one batch
/// first, so "the offset did not move" is a claim about a log that holds
/// data rather than about an empty log that reads as unmoved either way.
///
/// The payload that is not a record batch at all pins the position rather
/// than only the outcome. `prepare_batch` refuses those bytes with a
/// record-shape code, so a freeze that answers 44 over them is a freeze
/// the broker resolved before it parsed the batch. Every later gate,
/// including the idempotent-sequence one, sits behind that parse.
#[tokio::test]
async fn a_frozen_topic_is_refused_and_its_log_end_offset_does_not_move() {
    let dir = tempfile::tempdir().expect("log root");
    let mut image = image_with_topic("frozen", &[1]);
    add_topic(&mut image, "control", Uuid::from_u128(2));
    frozen(&mut image, "frozen", PatternType::Literal, "DR cutover");
    let image = Arc::new(image);

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

    for topic in ["frozen", "control"] {
        let part_dir = crate::log_dir::partition_dir(dir.path(), topic, 0);
        std::fs::create_dir_all(&part_dir).expect("partition directory");
        let log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default())
            .expect("open the log");
        let part = crate::broker::spawn_partition(
            topic.to_string(),
            krabka_ids::PartitionIndex(0),
            dir.path().to_path_buf(),
            log,
            log_dir_status.clone(),
            Arc::clone(&producer_state),
            false,
        );
        let topic_id = image.topic(topic).expect("topic").topic_id;
        let record = image.partition(topic, 0).expect("partition");
        part.install_replication_target(Some(topic_id), record.leader.0, record.leader_epoch.0)
            .await;
        part.install_isr(&record.isr, &record.replicas, record.leader)
            .await;
        part.log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .append(&mut seed_batch())
            .expect("seed the partition");
        partitions.insert(topic.to_string(), krabka_ids::PartitionIndex(0), part);
    }

    // `base_offset` is spelled out because the refusal happens before any
    // append, and the wire sentinel for that is -1, not the struct default 0.
    let refused = |scope: &str| PartitionProduceResponse {
        index: 0,
        error_code: crate::codes::POLICY_VIOLATION,
        base_offset: -1,
        error_message: Some(verdict(scope, PatternType::Literal, "DR cutover").error_message()),
        ..Default::default()
    };
    let cases = [
        (
            "the frozen topic is refused, and the message names the scope",
            "frozen",
            PartitionPayload::Slice(encode_batch(&seed_batch())),
            refused("frozen"),
        ),
        (
            "a payload that is not a batch is refused before it is parsed",
            "frozen",
            PartitionPayload::Slice(Bytes::from_static(b"not-a-batch")),
            refused("frozen"),
        ),
        (
            "an unfrozen control topic in the same request still appends",
            "control",
            PartitionPayload::Slice(encode_batch(&seed_batch())),
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::NONE,
                base_offset: 1,
                // The one accepted row in the table, so the one row that
                // carries the partition's real log start offset. Nothing has
                // trimmed the control topic, so it is 0 and not the -1 every
                // refusal above keeps from `Default`.
                log_start_offset: 0,
                ..Default::default()
            },
        ),
    ];

    for (label, topic, payload, want) in cases {
        // The handler resolves the freeze inside its per-topic loop, which
        // is what keeps one topic's verdict off another topic's rows.
        let freeze = resolve_topic_freeze(&image, topic);
        let resp = process_partition(
            PartitionInput {
                part_data: FramedPartition { index: 0, payload },
                topic_compression: None,
                max_message_bytes: krabka_log::DEFAULT_MAX_MESSAGE_SIZE,
                delivery: None,
                schema: None,
                topic_name: topic.into(),
                topic_denied: false,
                freeze,
                txn_id_denied: false,
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
        .expect("process partition");
        check!(resp == want, "case: {label}");
    }

    let log_end_offset = |topic: &str| {
        partitions
            .get(topic, krabka_ids::PartitionIndex(0))
            .expect("the partition is registered")
            .log_end_offset()
    };
    check!(
        log_end_offset("frozen") == krabka_log::Offset(1),
        "a refused row must not append: the seed batch is all the log holds"
    );
    check!(
        log_end_offset("control") == krabka_log::Offset(2),
        "the control topic took the append the frozen topic was refused"
    );

    let rejections = |topic: &str| {
        metrics
            .topic_freeze_rejections
            .get_or_create(&crate::metrics::TopicLabel {
                topic: topic.to_string(),
            })
            .get()
    };
    check!(
        rejections("frozen") == 2,
        "the gate counts once per refused partition row"
    );
    check!(rejections("control") == 0);
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
