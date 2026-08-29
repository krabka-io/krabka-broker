//! Tests for the per-partition pipeline's leadership-gate and write-freeze
//! response rows.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{MetadataRecord, PartitionRecord};
use krabka_protocol::{
    owned::produce_response::LeaderIdAndEpoch,
    records::{Record, RecordBatch},
};

use super::*;
use crate::handlers::produce::{
    framing::PartitionPayload,
    test_support::{encode_batch, image_with_topic},
};

#[tokio::test]
async fn process_partition_non_leader_preserves_current_leader_hint() {
    let mut img = image_with_topic("orders", &[2, 3]);
    img.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: "orders".into(),
        partition: 0,
        leader: krabka_audit::NodeId(2),
        replicas: vec![krabka_audit::NodeId(2), krabka_audit::NodeId(3)],
        isr: vec![krabka_audit::NodeId(2), krabka_audit::NodeId(3)],
        leader_epoch: krabka_metadata::LeaderEpoch(17),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    }));
    let image = Arc::new(img);
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
    let payload = encode_batch(&RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"hello")),
            ..Default::default()
        }],
        ..Default::default()
    });

    let resp = process_partition(
        PartitionInput {
            schema: None,
            part_data: FramedPartition {
                index: 0,
                payload: PartitionPayload::Slice(payload),
            },
            topic_compression: None,
            delivery: None,
            topic_name: "orders".into(),
            topic_denied: false,
            freeze: None,
            txn_id_denied: false,
            acks: 1,
            timeout: Duration::from_millis(1),
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
        },
    )
    .await
    .expect("process partition");

    let expected = PartitionProduceResponse {
        index: 0,
        error_code: crate::codes::NOT_LEADER_OR_FOLLOWER,
        base_offset: 0,
        log_append_time_ms: -1,
        log_start_offset: -1,
        record_errors: vec![],
        error_message: None,
        current_leader: LeaderIdAndEpoch {
            leader_id: 2,
            leader_epoch: 17,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        },
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
}

#[tokio::test]
async fn process_partition_leader_without_local_replica_hints_leader() {
    // We ARE the image-designated leader (this_node_id == leader), but the
    // local writer-actor hasn't been spun up (empty registry). This takes
    // the "transient not-leader" branch, whose `current_leader` hint must
    // still carry the real leader id + epoch from the image — not the 0
    // defaults a struct-field-deletion mutant would leave.
    let mut img = image_with_topic("orders", &[2, 3]);
    img.apply(&MetadataRecord::V1Partition(PartitionRecord {
        topic: "orders".into(),
        partition: 0,
        leader: krabka_audit::NodeId(2),
        replicas: vec![krabka_audit::NodeId(2), krabka_audit::NodeId(3)],
        isr: vec![krabka_audit::NodeId(2), krabka_audit::NodeId(3)],
        leader_epoch: krabka_metadata::LeaderEpoch(17),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![],
        partition_epoch: 1,
    }));
    let image = Arc::new(img);
    // Empty registry → `partitions.get(..)` returns None.
    let partitions = Arc::new(crate::partition_registry::PartitionRegistry::new());
    let txn_coordinator = Arc::new(crate::txn::coordinator::TxnCoordinator::new(
        krabka_audit::NodeId(2),
        Arc::clone(&partitions),
        Arc::new(crate::producer_id_manager::ProducerIdManager::new()),
        50,
        krabka_units::mebibytes(1),
    ));
    let producer_state = Arc::new(crate::producer_state::ProducerState::new());
    let log_dir_status = crate::log_dir_status::LogDirRegistry::default();
    let metrics = crate::metrics::BrokerMetrics::new();
    let payload = encode_batch(&RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"hello")),
            ..Default::default()
        }],
        ..Default::default()
    });

    let resp = process_partition(
        PartitionInput {
            schema: None,
            part_data: FramedPartition {
                index: 0,
                payload: PartitionPayload::Slice(payload),
            },
            topic_compression: None,
            delivery: None,
            topic_name: "orders".into(),
            topic_denied: false,
            freeze: None,
            txn_id_denied: false,
            acks: 1,
            timeout: Duration::from_millis(1),
        },
        PartitionServices {
            schema_validator: None,
            partitions: &partitions,
            txn_coordinator: &txn_coordinator,
            producer_state: &producer_state,
            log_dir_status: &log_dir_status,
            image: &image,
            // We are the leader (node 2), but hold no local replica.
            broker_policy: BrokerProducePolicy {
                node_id: krabka_audit::NodeId(2),
                default_min_insync_replicas: 1,
                is_witness: false,
            },
            record_decompression_policy: RecordDecompressionPolicy::default(),
            metrics: &metrics,
        },
    )
    .await
    .expect("process partition");

    let expected = PartitionProduceResponse {
        index: 0,
        error_code: crate::codes::NOT_LEADER_OR_FOLLOWER,
        base_offset: 0,
        log_append_time_ms: -1,
        log_start_offset: -1,
        record_errors: vec![],
        error_message: None,
        current_leader: LeaderIdAndEpoch {
            leader_id: 2,
            leader_epoch: 17,
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        },
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
}

// ── KFC-9 topic write freeze ─────────────────────────────────────
//
// The gate that refuses every partition row of a frozen topic, and the
// per-topic resolve the handler feeds it.
mod freeze;
