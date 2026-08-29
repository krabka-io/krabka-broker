//! `Produce` (`api_key=0`).
//!
//! The handler routes each partition's records to that partition's
//! writer-actor and waits for the assigned base offset.
//!
//! There is one `RecordBatch` per (topic, partition) per request. The
//! generated `PartitionProduceData.records` field is `Option<RecordsPayload>`.
//! Versions 0-2 carry a legacy v0/v1 `MessageSet`, which the handler
//! up-converts to a v2 `RecordBatch` before the append. Versions 3+ carry a
//! native v2 `RecordBatch`. The handler fully supports clients that send a
//! single v2 batch per partition, which is the typical modern case.

use std::time::Duration;

use bytes::Bytes;
use krabka_protocol::owned::produce_response::{PartitionProduceResponse, TopicProduceResponse};

use self::{
    authorization::authorize_produce,
    delivery::resolve_delivery_gate,
    framing::decode_produce_request,
    leadership::BrokerProducePolicy,
    pipeline::{PartitionInput, PartitionServices, process_partition},
    response::build_topic_error_response,
    throttle::{finish_produce_response, produce_bytes_by_qos_tier},
    topic_settings::resolve_topic_compression,
};
use crate::{broker::Broker, codes, config_keys::resolve_schema_validation, error::BrokerError};

mod append;
mod authorization;
mod delivery;
mod framing;
mod leadership;
mod owned_decode;
mod pipeline;
mod prepare;
mod producer_checks;
mod response;
mod schema;
mod throttle;
mod topic_settings;

#[cfg(test)]
mod test_support;

/// Kafka `acks` sentinel `-1`, which is producer `acks=all`. The leader must
/// hold the response until the high watermark covers the append, that is,
/// until every in-sync replica has it.
const ACKS_ALL: i16 = -1;

/// Wire sentinel "no offset assigned", which is
/// `ProduceResponse.INVALID_OFFSET`. The handler stamps it on partition rows
/// that failed before any append happened.
const INVALID_OFFSET: i64 = -1;

/// Wire sentinel "leader unknown" for the KIP-951 `current_leader` hint. The
/// handler uses it when the leader's `NodeId` does not fit the wire's `i32`.
const NO_LEADER_ID: i32 = -1;

#[tracing::instrument(
    name = "handle_produce",
    level = "info",
    skip_all,
    fields(api = "Produce", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    body_bytes: Bytes,
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    // KIP-124 request_percentage meters server-side handler time; capture the
    // start so the request throttle can be combined with the byte-rate throttle
    // below (KIP-219).
    let handler_start = std::time::Instant::now();
    let record_decompression_policy = broker.config.record_decompression_policy()?;
    let partitions = broker.partitions.clone();
    let controller = broker.controller.clone();
    let producer_state = broker.producer_state.clone();
    let txn_coordinator = broker.txn_coordinator.clone();
    let log_dir_status = broker.log_dir_status.clone();
    let broker_policy = BrokerProducePolicy {
        node_id: broker.config.node_id,
        default_min_insync_replicas: broker.config.default_min_insync_replicas,
        is_witness: broker.config.is_witness(),
    };
    // ── request decode (header-only on the verbatim-eligible path) ──
    // For v≥3 (native v2 payloads) we decode only the request FRAMING —
    // `transactional_id`, `acks`, `timeout_ms`, and per-topic / per-partition
    // headers plus each partition's `records` field as a zero-copy `Bytes`
    // slice of the request frame — via `produce_framing`. The record BODIES
    // are NOT decoded or decompressed here. Per-partition validation later
    // parses every record and transiently decompresses compressed bodies, but
    // skips owned-record materialization and preserves the original wire
    // bytes whenever no conversion is required. The owned
    // `RecordBatch` is decoded lazily, per partition, ONLY when the
    // verbatim-passthrough predicate fails (legacy magic, control batch,
    // log-append-time, broker-side recompression, multi-batch slice, or a
    // wire-null / undecodable field) — see `process_partition` /
    // `build_produce_data`.
    //
    // Legacy v0-2 requests carry a v0/v1 `MessageSet` that is always
    // up-converted (never passthrough-eligible), so they take the full owned
    // decode and feed every partition the owned path directly.
    let req = decode_produce_request(req_bytes, body_bytes, version)?;
    let timeout = Duration::from_millis(u64::try_from(req.timeout_ms.max(0)).unwrap_or(0));

    // ── ACL preamble ────────────────────────────────────────
    // For transactional Produce (request carries a non-empty
    // `transactional_id`), authorize `Write` on
    // `TransactionalId(transactional_id)` FIRST. On Deny, emit
    // TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53) per-partition on every
    // row of the response (matches Kafka's per-partition error mapping).
    //
    // Then batch-authorize every topic in the request for `Write` (the
    // operation Produce requires). Topics that come back `Deny` will
    // short-circuit the per-partition append below and emit
    // TOPIC_AUTHORIZATION_FAILED on every partition row of that topic.
    // Topic name resolution for v ≥ 13 (topic_id only on the wire) is
    // re-done inline below — but ACLs are keyed by topic
    // *name*, so we resolve the names here too for the authorize call.
    let image = controller.current_image();
    let authorization = authorize_produce(broker, &image, ctx, &req);
    let txn_id_denied = authorization.transactional_id_denied;
    let denied_topics = authorization.denied_topics;

    let mut topic_results: Vec<TopicProduceResponse> = Vec::with_capacity(req.topic_data.len());

    // ── KIP-13: measure total request bytes before consuming the topic_data ──
    // Computed here so the iterator doesn't conflict with `for topic in req.topic_data`
    // below (which moves the vector).
    let produce_bytes_by_qos_tier = produce_bytes_by_qos_tier(&image, &req.topic_data);

    for topic in req.topic_data {
        // v ≤ 12 sends the topic name; v ≥ 13 sends only topic_id and
        // we look it up in the metadata image. KIP-516: an explicit
        // non-zero id that is unknown returns UNKNOWN_TOPIC_ID (100) on
        // every partition row; a mismatched name+id returns
        // INCONSISTENT_TOPIC_ID (103). Only name-only misses fall through
        // to the legacy UNKNOWN_TOPIC_OR_PARTITION path.
        let topic_name = match crate::topic_resolve::resolve(&image, &topic.name, topic.topic_id) {
            Ok(rec) => rec.name.clone(),
            Err(codes::UNKNOWN_TOPIC_OR_PARTITION) => topic.name.clone(),
            Err(code) => {
                topic_results.push(build_topic_error_response(&topic, code));
                continue;
            }
        };

        // Account for the topic in Prometheus before
        // consuming `partition_data`. Sum the per-partition records-field
        // wire length so the bytes-in counter matches the actual bytes
        // received (the producer's compressed bytes on the verbatim path —
        // the true on-the-wire payload, no decompression needed). We count
        // even for authorize-denied / unknown-topic paths since the produce
        // *request* arrived; that mirrors Kafka's BrokerTopicMetrics semantics.
        if !topic_name.is_empty() {
            let mut topic_bytes: u64 = 0;
            // Also tally records-per-batch for
            // `messages_in_total`. V2 payloads expose
            // `records.len()` directly; legacy MessageSet payloads
            // remain opaque here and the upconversion-time
            // accounting already counts those arrivals.
            let mut topic_messages: u64 = 0;
            for p in &topic.partition_data {
                let partition_bytes = p.payload.payload_len() as u64;
                broker
                    .metrics
                    .record_partition_produce(&topic_name, p.index, partition_bytes);
                topic_bytes += partition_bytes;
                topic_messages += p.payload.message_count();
            }
            broker.metrics.record_produce(&topic_name, topic_bytes);
            broker
                .metrics
                .record_produce_messages(&topic_name, topic_messages);
        }

        let mut partition_results: Vec<PartitionProduceResponse> =
            Vec::with_capacity(topic.partition_data.len());

        // If the topic was denied by the ACL preamble, every
        // partition row for it gets TOPIC_AUTHORIZATION_FAILED and the
        // real append is skipped. An empty topic_name (v ≥ 13 with an
        // unknown topic_id) maps to "" in the denied set if and only if
        // its authorize result was Deny; the no-ACL compat shim returns
        // Allow uniformly, so existing tests are unaffected.
        let topic_denied = denied_topics.contains(&topic_name);

        // Resolve the topic's broker-side `compression.type` once. `None`
        // means Kafka's `producer` pass-through (no recompression). A
        // concrete codec forces recompression of any batch whose codec
        // differs — those batches must take the owned path. Mirrors the
        // writer's `config_snapshot().compression_type` gate so the
        // handler's verbatim decision matches the writer's recompression
        // decision exactly.
        let topic_compression = resolve_topic_compression(&image, &topic_name);

        // Resolve the topic's KFC-1 delivery settings once, beside the
        // compression resolve and for the same reason: they are a property of
        // the topic, not of a partition or of a batch. `None` is
        // `delivery.mode=immediate`, the default, and every partition of such a
        // topic then skips the delivery gate without reading a clock, a
        // timestamp, or the log.
        let delivery = resolve_delivery_gate(&image, &topic_name);

        // KFC-7, resolved here for the same reason: schema validation is a
        // property of the topic. `None` is the default, and every partition of
        // such a topic then skips the check without reading a record body.
        let schema = resolve_schema_validation(&image, &topic_name);

        for part_data in topic.partition_data {
            let idx = part_data.index;
            // Time the per-partition handler work for the
            // rebalancer's CpuUsage / CpuCapacity goals via
            // tokio_metrics::TaskMonitor — only on-CPU poll duration is
            // charged (not wall-time spent awaiting the writer queue,
            // HW gate under acks=-1, or txn coordinator).
            let monitor = tokio_metrics::TaskMonitor::new();
            let out = monitor
                .instrument(process_partition(
                    PartitionInput {
                        part_data,
                        topic_compression,
                        delivery,
                        schema,
                        topic_name: topic_name.clone(),
                        topic_denied,
                        txn_id_denied,
                        acks: req.acks,
                        timeout,
                    },
                    PartitionServices {
                        partitions: &partitions,
                        txn_coordinator: &txn_coordinator,
                        producer_state: &producer_state,
                        log_dir_status: &log_dir_status,
                        image: &image,
                        broker_policy,
                        record_decompression_policy,
                        metrics: &broker.metrics,
                        schema_validator: broker.config.schema_validator.as_ref(),
                    },
                ))
                .await?;
            let micros = u64::try_from(monitor.cumulative().total_poll_duration.as_micros())
                .unwrap_or(u64::MAX);
            if !topic_name.is_empty() {
                broker
                    .metrics
                    .record_partition_cpu_micros(&topic_name, idx, micros);
                // Per-partition failure accounting. Bumps
                // once per partition whose response carries a non-zero
                // error code (TOPIC_AUTHORIZATION_FAILED,
                // NOT_ENOUGH_REPLICAS, INVALID_RECORD, etc.) —
                // mirrors JVM's `failedProduceRequestRate.mark()`.
                if out.error_code != 0 {
                    broker.metrics.record_failed_produce(&topic_name);
                }
            }
            partition_results.push(out);
        }

        topic_results.push(TopicProduceResponse {
            name: topic_name,
            topic_id: topic.topic_id,
            partition_responses: partition_results,
            ..Default::default()
        });
    }

    // ── KIP-13 producer_byte_rate + KIP-124 request_percentage ──────
    // Combine the data (byte-rate) and request (handler-time) throttles as
    // their max, surface it in throttle_time_ms, and mute the channel once
    // before responding (KIP-219). The dispatch loop skips request_percentage
    // for Produce so it is charged exactly once, here.
    finish_produce_response(
        broker,
        &image,
        ctx,
        handler_start,
        &produce_bytes_by_qos_tier,
        topic_results,
        version,
    )
    .await
}

/// Whether Kafka requires a response for this Produce request. `acks=0`
/// requests are one-way even when an append or authorization check fails.
pub(crate) fn response_required(
    request_bytes: &[u8],
    body_bytes: Bytes,
    version: i16,
) -> Result<bool, BrokerError> {
    Ok(decode_produce_request(request_bytes, body_bytes, version)?.acks != 0)
}
