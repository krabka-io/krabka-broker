//! The per-partition produce pipeline, which runs one partition's records
//! through every gate in order and returns that partition's response row.

use std::{sync::Arc, time::Duration};

use krabka_compression::RecordDecompressionPolicy;
use krabka_metadata::TopicFreezeRecord;
use krabka_protocol::owned::produce_response::PartitionProduceResponse;

use super::{
    append::{AppendContext, dispatch_prepared},
    delivery::DeliveryGate,
    framing::FramedPartition,
    leadership::{
        BrokerProducePolicy, current_leader_hint, diskless_role_ready, replica_state_matches_image,
        replication_target_matches_image, validate_partition_gate,
    },
    prepare::prepare_batch,
    producer_checks::{handle_duplicate, validate_transactional_produce},
    schema::{SCHEMA_REJECTION_MESSAGE, validate_batch_schemas},
};
use crate::{
    codes,
    error::BrokerError,
    freeze::resolve::FreezeVerdict,
    partition_registry::PartitionRegistry,
    schema_validation::{SchemaGate, SchemaValidator},
};

/// Per-partition produce input, held apart so that the call site can wrap the
/// work in `tokio_metrics::TaskMonitor` and charge only on-CPU poll time to
/// `partition_cpu_micros_total`.
///
/// Wall time spent on the writer queue, on the HW gate under `acks=-1`, or on
/// the txn coordinator does not count toward CPU usage. The work returns the
/// per-partition response on every path. Only `txn_coordinator.put` errors
/// propagate with `?`.
pub(super) struct PartitionInput<'a> {
    pub(super) part_data: FramedPartition,
    pub(super) topic_compression: Option<krabka_compression::CompressionType>,
    /// The topic's KFC-1 delivery settings, resolved once per topic. `None` is
    /// `delivery.mode=immediate`, and skips the delivery gate entirely.
    pub(super) delivery: Option<DeliveryGate>,
    /// The topic's KFC-7 schema-validation settings, resolved once per topic.
    /// `None` is "neither `schema.validation.key` nor
    /// `schema.validation.value` is set", and skips the check entirely.
    pub(super) schema: Option<SchemaGate>,
    pub(super) topic_name: String,
    pub(super) topic_denied: bool,
    /// The topic's KFC-9 write-freeze entry, resolved once per topic. `None`
    /// is a topic that accepts writes, and skips the freeze gate entirely.
    /// It sits beside `topic_denied` because it refuses for the same kind of
    /// reason: nothing about this batch can earn the write.
    pub(super) freeze: Option<&'a TopicFreezeRecord>,
    pub(super) txn_id_denied: bool,
    pub(super) acks: i16,
    pub(super) timeout: Duration,
}

#[derive(Clone, Copy)]
pub(super) struct PartitionServices<'a> {
    pub(super) partitions: &'a Arc<PartitionRegistry>,
    pub(super) txn_coordinator: &'a Arc<crate::txn::coordinator::TxnCoordinator>,
    pub(super) producer_state: &'a Arc<crate::producer_state::ProducerState>,
    pub(super) log_dir_status: &'a crate::log_dir_status::LogDirRegistry,
    pub(super) image: &'a Arc<krabka_metadata::MetadataImage>,
    pub(super) broker_policy: BrokerProducePolicy,
    pub(super) record_decompression_policy: RecordDecompressionPolicy,
    pub(super) metrics: &'a crate::metrics::BrokerMetrics,
    /// The broker's KFC-7 validator. `None` is "no `[schema_registry]`
    /// section", and a topic that asks for validation on such a broker is
    /// rejected rather than admitted unchecked.
    pub(super) schema_validator: Option<&'a Arc<SchemaValidator>>,
}

pub(super) async fn process_partition(
    input: PartitionInput<'_>,
    services: PartitionServices<'_>,
) -> Result<PartitionProduceResponse, BrokerError> {
    let PartitionInput {
        part_data,
        topic_compression,
        delivery,
        schema,
        topic_name,
        topic_denied,
        freeze,
        txn_id_denied,
        acks,
        timeout,
    } = input;
    let topic_name = topic_name.as_str();
    let PartitionServices {
        partitions,
        txn_coordinator,
        producer_state,
        log_dir_status,
        image,
        broker_policy,
        record_decompression_policy,
        metrics,
        schema_validator,
    } = services;
    let idx = part_data.index;
    let mut out = PartitionProduceResponse {
        index: idx,
        ..Default::default()
    };

    if txn_id_denied {
        out.error_code = codes::TRANSACTIONAL_ID_AUTHORIZATION_FAILED;
        return Ok(out);
    }

    if topic_denied {
        out.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
        return Ok(out);
    }

    // ── KFC-9 write freeze ───────────────────────────────────────────
    // Beside the topic ACL denial, and ahead of `prepare_batch`, because a
    // freeze is an authority gate and not a content gate. It ranks with the
    // denial above rather than with the KFC-1 and KFC-7 gates below, so a
    // frozen topic never pays CRC verification or decompression for a batch
    // the broker will never accept.
    //
    // The position has a second consequence, which the tests assert: the gate
    // returns ahead of the idempotent-sequence gate, so a refused batch leaves
    // the producer state untouched and the log end offset unmoved. A refusal
    // that still appended would be the worst failure this feature can have,
    // and the error code alone does not rule it out.
    if let Some(entry) = freeze {
        metrics.record_topic_freeze_rejection(topic_name);
        out.error_code = codes::POLICY_VIOLATION;
        out.error_message = Some(FreezeVerdict::from(entry).error_message());
        return Ok(out);
    }

    // Decide verbatim-passthrough vs owned-decode and extract the HEADER
    // fields the gates below need (producer id/epoch/sequence,
    // last_offset_delta, max_timestamp, attributes). On the verbatim path
    // this verifies the CRC and complete record structure while retaining the
    // original wire bytes. Compressed bodies are transiently decompressed for
    // validation but never re-encoded. The owned fallback fully materializes
    // the records, exactly as before. A null /
    // undecodable field returns INVALID_REQUEST / INVALID_RECORD, preserving
    // the prior error-code ordering (before the leadership gate).
    let prepared = match prepare_batch(
        part_data.payload,
        topic_compression,
        topic_name,
        metrics,
        record_decompression_policy,
    ) {
        Ok(p) => p,
        Err(code) => {
            out.error_code = code;
            return Ok(out);
        }
    };

    // ── KFC-7 schema validation ──────────────────────────────────────
    // Before the leadership gate, so that record-shape rejections keep coming
    // ahead of leadership ones, which is the order every gate above this line
    // already follows. `gate` is `None` on a topic that did not ask, and this
    // whole block is then one `if let` that does not match.
    if let Some(gate) = schema
        && let Err(rejection) = validate_batch_schemas(
            &prepared,
            gate,
            schema_validator,
            topic_name,
            record_decompression_policy,
            metrics,
        )
        .await
    {
        out.error_code = codes::INVALID_RECORD;
        out.error_message = Some(SCHEMA_REJECTION_MESSAGE.to_owned());
        out.record_errors = rejection;
        return Ok(out);
    }

    // ── leadership gate (Kafka: only the LEADER accepts Produce) ──────
    // Only the partition leader may accept a Produce. A Produce misrouted
    // to a non-leader must be rejected so the client refreshes its
    // metadata and re-targets — it must NOT be appended to a local
    // follower replica (the real leader would never see those records and
    // the follower's append would be discarded on its next truncating
    // Fetch from the leader → silent data loss).
    //
    // The authoritative leader is the metadata IMAGE's `partition.leader`,
    // the same source the Fetch handler uses for its KIP-320 / KIP-951
    // `current_leader` hint. We deliberately do NOT gate on the broker's
    // local `leader_partitions` / `is_coordinator_for` set: that set is
    // recomputed on every metadata change and is transiently empty while
    // raft leadership settles on a freshly-booted broker, so it would
    // spuriously reject a legitimate leader's Produces (see the same
    // hazard documented for the transactional path below). The image
    // reflects committed leadership, so a just-elected leader's own image
    // already names it the leader; the only residual window is a follower
    // whose image hasn't yet caught up to a leadership change, which
    // correctly returns NOT_LEADER (the client retries against the new
    // leader) rather than appending to the wrong replica.
    //
    // Partition-level absence in the image (topic exists but this index
    // doesn't, or the topic is unknown) maps to UNKNOWN_TOPIC_OR_PARTITION
    // (3); presence-but-not-leader maps to NOT_LEADER_OR_FOLLOWER (6) with
    // a `current_leader` hint (encodes at Produce v10+, KIP-951) so the
    // client re-routes without a full Metadata round-trip.
    let (part, _) = match validate_partition_gate(
        topic_name,
        idx,
        acks,
        partitions,
        log_dir_status,
        image,
        broker_policy,
    ) {
        Ok(ready) => ready,
        Err(error) => {
            out.error_code = error.code;
            if let Some(leader) = error.current_leader {
                out.current_leader = leader;
            }
            return Ok(out);
        }
    };
    // Hold the transition barrier through dedup, enqueue, append, and ack.
    // Diskless promotion takes the write side before hydrating and rebuilding
    // producer state, so it cannot publish a half-adopted prefix or race an
    // idempotent retry already admitted here.
    let transition = part.lock_produce_transition().await;
    let record = image.partition(topic_name, idx).expect("gate checked");
    let topic_id = image.topic(topic_name).map(|topic| topic.topic_id);
    if !replication_target_matches_image(&transition, topic_id, record)
        || (part.diskless && !diskless_role_ready(&part, record))
    {
        out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        out.current_leader = current_leader_hint(record);
        return Ok(out);
    }
    if !part.diskless {
        let replica_state = part.replica_state.lock().await;
        if !replica_state_matches_image(&replica_state, record) {
            out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
            out.current_leader = current_leader_hint(record);
            return Ok(out);
        }
    }
    let leader_epoch = part
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);

    // ── transactional produce verify (KIP-1319 v2) ──────────
    // This check is more authoritative than idempotent dedup,
    // so it runs first. Non-transactional batches (pid < 0 or
    // is_transactional=false) skip directly to the dedup gate.
    // All header fields below come from `prepared` — sourced from the v2
    // batch HEADER on the verbatim path, or from the decoded owned
    // `RecordBatch` header on the fallback path.
    if prepared.attributes.is_transactional() && part.diskless {
        out.error_code = codes::INVALID_TXN_STATE;
        return Ok(out);
    }
    if let Some(code) =
        validate_transactional_produce(&prepared, txn_coordinator, image, topic_name, idx).await?
    {
        out.error_code = code;
        return Ok(out);
    }

    // ── idempotent-producer dedup gate ───────────────────────
    if let Some(response) = handle_duplicate(
        &prepared,
        producer_state,
        &part,
        topic_name,
        idx,
        acks,
        timeout,
    )
    .await
    {
        return Ok(response);
    }

    // ── KFC-1 scheduled-delivery gate ───────────────────────
    // On a topic with `delivery.mode=scheduled` the batch's `max_timestamp` is
    // the time it becomes visible to a consumer. Two settings reject it, and
    // both use the existing `INVALID_TIMESTAMP` (32) that every client already
    // classifies: a delivery time further ahead than `delivery.max.delay.ms`,
    // and, under `delivery.schedule.monotonic`, one that precedes the largest
    // delivery time the partition already holds.
    //
    // The second is the guard against a schedule that stalls in silence.
    // Visibility is offset-ordered for a classic group, because a group's
    // position is one offset and a record it reads past is unreachable for it
    // forever. So a batch that comes due before an earlier one holds up
    // everything behind it. The topic still looks healthy and the lag is real,
    // which is why the config turns that stall into an error at the producer
    // that caused it.
    //
    // The gate runs after the dedup gate on purpose. An idempotent retry is not
    // a new entry in the schedule, and a partition that accepted a later batch
    // in between would otherwise answer that retry with INVALID_TIMESTAMP
    // instead of the offset it already assigned it.
    if let Some(gate) = delivery
        && gate.rejects(prepared.max_timestamp, part.delivery.now_ms(), &part.log)
    {
        out.error_code = codes::INVALID_TIMESTAMP;
        return Ok(out);
    }

    dispatch_prepared(
        prepared,
        AppendContext {
            partition: &part,
            producer_state,
            topic_name,
            partition_index: idx,
            acks,
            timeout,
            leader_epoch,
        },
    )
    .await
}

#[cfg(test)]
mod tests;
