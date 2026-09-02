//! Tests for the KFC-1 scheduled-delivery gate, including one that drives a
//! real scheduled partition end to end through the per-partition pipeline.

use std::{sync::Arc, time::Duration};

use assert2::check;
use bytes::Bytes;
use krabka_compression::RecordDecompressionPolicy;
use krabka_ids::Offset;
use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::{bytes, millis};
use uuid::Uuid;

use super::*;
use crate::{
    config_keys::{DELIVERY_MAX_DELAY_MS, DELIVERY_MODE_IMMEDIATE, DELIVERY_SCHEDULE_MONOTONIC},
    handlers::produce::{
        framing::{FramedPartition, PartitionPayload},
        leadership::BrokerProducePolicy,
        pipeline::{PartitionInput, PartitionServices, process_partition},
        test_support::{encode_batch, image_with_topic},
    },
};

// ── KFC-1 scheduled delivery ─────────────────────────────────────
//
// On a topic with `delivery.mode=scheduled` a batch's `max_timestamp` is
// the time it becomes visible to a consumer. The produce path rejects two
// kinds of batch with `INVALID_TIMESTAMP` (32), and does nothing at all for
// a topic that delivers immediately.

// The fixed clock reading the pure delivery-gate cases run against, so a
// schedule in a test is exact rather than nearly right.
const SCHEDULE_NOW_MS: i64 = 1_700_000_000_000;

// The topic-config overrides one delivery-gate table row applies.
type DeliveryOverrides = &'static [(&'static str, &'static str)];

fn image_with_delivery(topic: &str, overrides: &[(&str, &str)]) -> MetadataImage {
    let mut image = image_with_topic(topic, &[1]);
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.into(),
        overrides: overrides
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
    }));
    image
}

// A one-record batch that asks to be delivered at `delivery_ms`.
fn batch_delivered_at(delivery_ms: i64) -> RecordBatch {
    RecordBatch {
        base_timestamp: delivery_ms,
        max_timestamp: delivery_ms,
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    }
}

// A scheduled log holding one batch per entry of `deliveries`.
fn scheduled_log(dir: &std::path::Path, deliveries: &[i64]) -> std::sync::Mutex<krabka_log::Log> {
    let mut log = krabka_log::Log::open(
        dir,
        krabka_log::LogConfig {
            delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
            ..krabka_log::LogConfig::default()
        },
    )
    .expect("open the log");
    for delivery_ms in deliveries {
        log.append(&mut batch_delivered_at(*delivery_ms))
            .expect("append a scheduled batch");
    }
    std::sync::Mutex::new(log)
}

#[test]
fn only_a_scheduled_topic_resolves_a_delivery_gate() {
    let cases: [(DeliveryOverrides, Option<DeliveryGate>, &str); 6] = [
        (&[], None, "no topic config at all"),
        (
            &[(DELIVERY_MODE, DELIVERY_MODE_IMMEDIATE)],
            None,
            "the explicit default",
        ),
        (
            // A mode that is not `scheduled` keeps Kafka's behavior, even
            // when the other two keys are set.
            &[
                (DELIVERY_MODE, "later"),
                (DELIVERY_SCHEDULE_MONOTONIC, "true"),
            ],
            None,
            "a corrupt mode alongside the other keys",
        ),
        (
            &[(DELIVERY_MODE, DELIVERY_MODE_SCHEDULED)],
            Some(DeliveryGate {
                max_delay: Some(millis(604_800_000)),
                monotonic: false,
            }),
            "scheduled, with both other keys defaulted",
        ),
        (
            &[
                (DELIVERY_MODE, DELIVERY_MODE_SCHEDULED),
                (DELIVERY_MAX_DELAY_MS, "-1"),
                (DELIVERY_SCHEDULE_MONOTONIC, "true"),
            ],
            Some(DeliveryGate {
                max_delay: None,
                monotonic: true,
            }),
            "scheduled, with the unbounded sentinel",
        ),
        (
            &[
                (DELIVERY_MODE, DELIVERY_MODE_SCHEDULED),
                (DELIVERY_MAX_DELAY_MS, "90000"),
            ],
            Some(DeliveryGate {
                max_delay: Some(millis(90_000)),
                monotonic: false,
            }),
            "scheduled, with an explicit bound",
        ),
    ];

    for (overrides, want, label) in cases {
        let image = image_with_delivery("t", overrides);
        check!(resolve_delivery_gate(&image, "t") == want, "case: {label}");
    }
}

#[test]
fn the_delivery_gate_bounds_only_how_far_ahead_a_batch_is_scheduled() {
    let dir = tempfile::tempdir().expect("log root");
    // An empty partition holds no schedule, so `monotonic` cannot fire and
    // every verdict below is the `delivery.max.delay.ms` verdict alone.
    let log = scheduled_log(dir.path(), &[]);

    let cases = [
        (
            Some(millis(60_000)),
            SCHEDULE_NOW_MS + 59_999,
            false,
            "inside the bound",
        ),
        (
            Some(millis(60_000)),
            SCHEDULE_NOW_MS + 60_000,
            false,
            "exactly at the bound",
        ),
        (
            Some(millis(60_000)),
            SCHEDULE_NOW_MS + 60_001,
            true,
            "one millisecond past the bound",
        ),
        (
            Some(millis(60_000)),
            SCHEDULE_NOW_MS - 86_400_000,
            false,
            "a day in the past is not a delay",
        ),
        (
            Some(<Time as TimeExt>::ZERO),
            SCHEDULE_NOW_MS,
            false,
            "a zero bound still takes the present instant",
        ),
        (
            Some(<Time as TimeExt>::ZERO),
            SCHEDULE_NOW_MS + 1,
            true,
            "a zero bound rejects the next millisecond",
        ),
        (
            None,
            i64::MAX,
            false,
            "the -1 sentinel removes the bound entirely",
        ),
    ];

    for (max_delay, delivery_ms, want, label) in cases {
        let gate = DeliveryGate {
            max_delay,
            monotonic: true,
        };
        check!(
            gate.rejects(delivery_ms, SCHEDULE_NOW_MS, &log) == want,
            "case: {label}"
        );
    }
}

#[test]
fn a_monotonic_gate_rejects_a_batch_that_precedes_the_partitions_schedule() {
    let dir = tempfile::tempdir().expect("log root");
    // The partition's schedule already runs out to SCHEDULE_NOW_MS + 2_000.
    let log = scheduled_log(
        dir.path(),
        &[SCHEDULE_NOW_MS + 1_000, SCHEDULE_NOW_MS + 2_000],
    );

    let cases = [
        (
            true,
            SCHEDULE_NOW_MS + 2_001,
            false,
            "after the largest delivery time the partition holds",
        ),
        (
            true,
            SCHEDULE_NOW_MS + 2_000,
            false,
            "equal to it, which does not run backwards",
        ),
        (
            true,
            SCHEDULE_NOW_MS + 1_999,
            true,
            "one millisecond before it",
        ),
        (
            true,
            SCHEDULE_NOW_MS + 1_500,
            true,
            "between the two batches already scheduled",
        ),
        (
            true,
            SCHEDULE_NOW_MS - 1_000,
            true,
            "in the past, behind the whole schedule",
        ),
        (
            false,
            SCHEDULE_NOW_MS - 1_000,
            false,
            "the same batch with the guard turned off",
        ),
    ];

    for (monotonic, delivery_ms, want, label) in cases {
        let gate = DeliveryGate {
            // Unbounded, so every verdict below is the monotonic verdict.
            max_delay: None,
            monotonic,
        };
        check!(
            gate.rejects(delivery_ms, SCHEDULE_NOW_MS, &log) == want,
            "case: {label}"
        );
    }
}

// Drive `process_partition` against a real scheduled partition: both
// rejections, then the batch that fits the schedule, which must reach the
// log as the producer's own bytes.
//
// The gate reads the broker's clock, so the delivery times here are
// relative to that reading and sit far from either boundary.
#[tokio::test]
async fn a_scheduled_partition_rejects_and_appends_by_delivery_time() {
    use krabka_protocol::owned::produce_response::PartitionProduceResponse;

    let dir = tempfile::tempdir().unwrap();
    let image = Arc::new(image_with_delivery(
        "sched",
        &[
            (DELIVERY_MODE, DELIVERY_MODE_SCHEDULED),
            (DELIVERY_MAX_DELAY_MS, "3600000"),
            (DELIVERY_SCHEDULE_MONOTONIC, "true"),
        ],
    ));
    let delivery = resolve_delivery_gate(&image, "sched");
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

    let part_dir = crate::log_dir::partition_dir(dir.path(), "sched", 0);
    std::fs::create_dir_all(&part_dir).unwrap();
    let log = krabka_log::Log::open(
        &part_dir,
        krabka_log::LogConfig {
            delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
            ..krabka_log::LogConfig::default()
        },
    )
    .unwrap();
    let part = crate::broker::spawn_partition(
        "sched".to_string(),
        krabka_ids::PartitionIndex(0),
        dir.path().to_path_buf(),
        log,
        log_dir_status.clone(),
        Arc::clone(&producer_state),
        false,
    );
    let record = image.partition("sched", 0).expect("partition");
    part.install_replication_target(Some(Uuid::nil()), record.leader.0, record.leader_epoch.0)
        .await;
    part.install_isr(&record.isr, &record.replicas, record.leader)
        .await;

    // Seed offset 0 with a batch that comes due in ten minutes, so the
    // partition already carries a schedule to run backwards from.
    let now_ms = part.delivery.now_ms();
    part.log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut batch_delivered_at(now_ms + 600_000))
        .expect("seed the partition schedule");
    partitions.insert("sched".into(), krabka_ids::PartitionIndex(0), part);

    // The accepted case appends, so it comes last.
    let accepted_delivery_ms = now_ms + 900_000;
    let cases = [
        (
            now_ms + 300_000,
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::INVALID_TIMESTAMP,
                base_offset: -1,
                ..Default::default()
            },
            "before the delivery time the partition already holds",
        ),
        (
            now_ms + 7_200_000,
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::INVALID_TIMESTAMP,
                base_offset: -1,
                ..Default::default()
            },
            "further ahead than delivery.max.delay.ms",
        ),
        (
            accepted_delivery_ms,
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::NONE,
                base_offset: 1,
                // An accepted row carries the partition's real log start
                // offset. Nothing has trimmed this one, so it is 0 and not
                // the -1 the two refusals above keep from `Default`.
                log_start_offset: 0,
                ..Default::default()
            },
            "after the schedule and inside the bound",
        ),
    ];

    for (delivery_ms, want, label) in cases {
        let resp = process_partition(
            PartitionInput {
                schema: None,
                part_data: FramedPartition {
                    index: 0,
                    payload: PartitionPayload::Slice(encode_batch(&batch_delivered_at(
                        delivery_ms,
                    ))),
                },
                topic_compression: None,
                max_message_bytes: krabka_log::DEFAULT_MAX_MESSAGE_SIZE,
                delivery,
                topic_name: "sched".into(),
                freeze: crate::freeze::resolve::FreezeMutationResolution::Admit,
                txn_id_denied: false,
                acks: 1,
                timeout: Duration::from_secs(5),
            },
            PartitionServices {
                schema_validator: None,
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
            },
        )
        .await
        .expect("process partition");
        check!(resp == want, "case: {label}");
    }

    // The accepted batch took the verbatim path: the log holds the
    // producer's own bytes, with only `base_offset` (v2 header bytes 0..8)
    // and `partition_leader_epoch` (bytes 12..16) stamped. Both sit ahead
    // of the CRC's coverage, which is what lets the writer patch them
    // without re-encoding. A scheduled topic must keep that passthrough.
    let accepted_wire = encode_batch(&batch_delivered_at(accepted_delivery_ms));
    let mut want_bytes = accepted_wire.to_vec();
    want_bytes[0..8].copy_from_slice(&1_i64.to_be_bytes());
    want_bytes[12..16].copy_from_slice(&0_i32.to_be_bytes());
    let part = partitions
        .get("sched", krabka_ids::PartitionIndex(0))
        .expect("the partition is registered");
    let stored = part
        .log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .read_raw(Offset(1), Offset(2), bytes(4096))
        .expect("read the appended batch back");
    check!(stored.bytes == Bytes::from(want_bytes));
}
