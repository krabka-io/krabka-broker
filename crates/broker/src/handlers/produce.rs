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

use std::{
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use krabka_compression::RecordDecompressionPolicy;
use krabka_log::{Log, Offset, VerbatimBatch};
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        produce_request::ProduceRequest,
        produce_response::{
            BatchIndexAndErrorMessage, LeaderIdAndEpoch, PartitionProduceResponse, ProduceResponse,
            TopicProduceResponse,
        },
    },
    primitives::uuid::Uuid as WireUuid,
    records::{
        Attributes, RecordBatch, RecordBatchBorrowed, RecordsPayload, TimestampType,
        ValidatedBatch, count_records_in_v2_batches, produce_framing, validate_one_v2_batch,
    },
};
use krabka_schema_serde::subject::Role;
use krabka_units::{Time, convert::TimeExt};
use tokio::sync::oneshot;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    config_keys::{
        COMPRESSION_TYPE, DELIVERY_MODE, DELIVERY_MODE_SCHEDULED, MIN_INSYNC_REPLICAS,
        parse_compression_type, resolve_delivery_max_delay, resolve_delivery_schedule_monotonic,
        resolve_schema_validation,
    },
    error::BrokerError,
    partition::{Partition, ProduceData, ProduceJob, WriterMessage},
    partition_registry::PartitionRegistry,
    schema_validation::{RejectReason, SchemaGate, SchemaValidator},
};

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

/// Resolve `min.insync.replicas` for a topic from the metadata image.
///
/// On a malformed value the function falls back to the broker default without
/// a message. The `AlterConfigs` validator already rejected the invalid
/// values, so any string here that does not parse means a corrupt metadata
/// image.
fn topic_min_insync_replicas(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    default_min_insync_replicas: i32,
) -> i32 {
    image
        .topic_config(topic)
        .and_then(|m| m.get(MIN_INSYNC_REPLICAS))
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(default_min_insync_replicas)
}

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

fn decode_produce_request(
    request_bytes: &[u8],
    body_bytes: Bytes,
    version: i16,
) -> Result<ProduceFramed, BrokerError> {
    if !(0..3).contains(&version) {
        return Ok(ProduceFramed::from_framing(produce_framing(
            body_bytes, version,
        )?));
    }
    let mut cursor = request_bytes;
    let owned: ProduceRequest =
        krabka_protocol::kafka_3_6_2::owned::produce_request::ProduceRequest::decode(
            &mut cursor,
            version,
        )?
        .into();
    Ok(ProduceFramed::from_owned(owned))
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

struct ProduceAuthorization {
    transactional_id_denied: bool,
    denied_topics: std::collections::HashSet<String>,
}

fn authorize_produce(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    request: &ProduceFramed,
) -> ProduceAuthorization {
    let transactional_id_denied = request.transactional_id.as_deref().is_some_and(|id| {
        !id.is_empty()
            && broker.config.authorizer.authorize(
                image,
                &AuthorizationRequest {
                    principal: context.principal,
                    host: context.peer,
                    resource_type: ResourceType::TransactionalId,
                    resource_name: id,
                    operation: AclOperation::Write,
                },
            ) == AuthorizationResult::Deny
    });
    let topic_names: Vec<String> = request
        .topic_data
        .iter()
        .map(|topic| {
            if !topic.name.is_empty() {
                topic.name.clone()
            } else if topic.topic_id != WireUuid::ZERO {
                image
                    .topic_name_by_id(&uuid::Uuid::from_bytes(topic.topic_id.0))
                    .unwrap_or_default()
                    .to_string()
            } else {
                String::new()
            }
        })
        .collect();
    let denied_topics = authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        context.principal,
        context.peer,
        AclOperation::Write,
        topic_names.iter().map(String::as_str),
    )
    .into_iter()
    .filter(|(_, result)| *result == AuthorizationResult::Deny)
    .map(|(name, _)| name.to_string())
    .collect();
    ProduceAuthorization {
        transactional_id_denied,
        denied_topics,
    }
}

async fn finish_produce_response(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &crate::handlers::RequestContext<'_>,
    handler_start: std::time::Instant,
    bytes_by_qos: &std::collections::BTreeMap<String, u64>,
    topic_results: Vec<TopicProduceResponse>,
    version: i16,
) -> Result<Bytes, BrokerError> {
    let data_delay = bytes_by_qos
        .iter()
        .map(|(tier, bytes)| {
            crate::quota::consume_producer_quota(
                image,
                &broker.quota_buckets,
                &context.principal.name,
                context.client_id,
                tier,
                *bytes,
                broker.config.quota_throttle_max,
            )
        })
        .fold(<Time as TimeExt>::ZERO, Time::max);
    let elapsed_micros = u64::try_from(
        handler_start
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)),
    )
    .expect("elapsed microseconds clamped to u64");
    let request_delay = crate::quota::consume_request_quota(
        image,
        &broker.quota_buckets,
        &context.principal.name,
        context.client_id,
        elapsed_micros,
        broker.config.quota_throttle_max,
    );
    let delay = data_delay.max(request_delay);
    let response = ProduceResponse {
        responses: topic_results,
        throttle_time_ms: crate::quota::throttle_time_ms(delay),
        ..Default::default()
    };
    if delay > <Time as TimeExt>::ZERO {
        tokio::time::sleep(delay.to_std()).await;
    }
    let mut encoded = BytesMut::new();
    if (0..3).contains(&version) {
        let legacy: krabka_protocol::kafka_3_6_2::owned::produce_response::ProduceResponse =
            response.into();
        encoded.reserve(legacy.encoded_len(version));
        legacy.encode(&mut encoded, version)?;
    } else {
        encoded.reserve(response.encoded_len(version));
        response.encode(&mut encoded, version)?;
    }
    Ok(encoded.freeze())
}

/// Per-partition produce input, held apart so that the call site can wrap the
/// work in `tokio_metrics::TaskMonitor` and charge only on-CPU poll time to
/// `partition_cpu_micros_total`.
///
/// Wall time spent on the writer queue, on the HW gate under `acks=-1`, or on
/// the txn coordinator does not count toward CPU usage. The work returns the
/// per-partition response on every path. Only `txn_coordinator.put` errors
/// propagate with `?`.
struct PartitionInput {
    part_data: FramedPartition,
    topic_compression: Option<krabka_compression::CompressionType>,
    /// The topic's KFC-1 delivery settings, resolved once per topic. `None` is
    /// `delivery.mode=immediate`, and skips the delivery gate entirely.
    delivery: Option<DeliveryGate>,
    /// The topic's KFC-7 schema-validation settings, resolved once per topic.
    /// `None` is "neither `schema.validation.key` nor
    /// `schema.validation.value` is set", and skips the check entirely.
    schema: Option<SchemaGate>,
    topic_name: String,
    topic_denied: bool,
    txn_id_denied: bool,
    acks: i16,
    timeout: Duration,
}

#[derive(Clone, Copy)]
struct PartitionServices<'a> {
    partitions: &'a Arc<PartitionRegistry>,
    txn_coordinator: &'a Arc<crate::txn::coordinator::TxnCoordinator>,
    producer_state: &'a Arc<crate::producer_state::ProducerState>,
    log_dir_status: &'a crate::log_dir_status::LogDirRegistry,
    image: &'a Arc<krabka_metadata::MetadataImage>,
    broker_policy: BrokerProducePolicy,
    record_decompression_policy: RecordDecompressionPolicy,
    metrics: &'a crate::metrics::BrokerMetrics,
    /// The broker's KFC-7 validator. `None` is "no `[schema_registry]`
    /// section", and a topic that asks for validation on such a broker is
    /// rejected rather than admitted unchecked.
    schema_validator: Option<&'a Arc<SchemaValidator>>,
}

async fn process_partition(
    input: PartitionInput,
    services: PartitionServices<'_>,
) -> Result<PartitionProduceResponse, BrokerError> {
    let PartitionInput {
        part_data,
        topic_compression,
        delivery,
        schema,
        topic_name,
        topic_denied,
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

fn diskless_role_ready(
    partition: &crate::partition::Partition,
    record: &krabka_metadata::PartitionRecord,
) -> bool {
    krabka_metadata::NodeId(
        partition
            .current_leader
            .load(std::sync::atomic::Ordering::Acquire),
    ) == record.leader
        && partition
            .current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire)
            == record.leader_epoch.0
}

fn replication_target_matches_image(
    target: &crate::partition::ReplicationTarget,
    topic_id: Option<uuid::Uuid>,
    record: &krabka_metadata::PartitionRecord,
) -> bool {
    target.topic_id == topic_id
        && target.leader_node_id == record.leader
        && target.leader_epoch == record.leader_epoch
}

fn replica_state_matches_image(
    state: &crate::replica_state::ReplicaState,
    record: &krabka_metadata::PartitionRecord,
) -> bool {
    state.current_leader_epoch == krabka_ids::LeaderEpoch(record.leader_epoch.0)
        && state.isr.len() == record.isr.len()
        && record.isr.iter().all(|node| state.isr.contains(node))
}

fn current_leader_hint(record: &krabka_metadata::PartitionRecord) -> LeaderIdAndEpoch {
    LeaderIdAndEpoch {
        leader_id: i32::try_from(record.leader.0).unwrap_or(NO_LEADER_ID),
        leader_epoch: record.leader_epoch.0,
        ..Default::default()
    }
}

#[derive(Clone, Copy)]
struct AppendContext<'a> {
    partition: &'a Arc<crate::partition::Partition>,
    producer_state: &'a Arc<crate::producer_state::ProducerState>,
    topic_name: &'a str,
    partition_index: i32,
    acks: i16,
    timeout: Duration,
    leader_epoch: i32,
}

async fn dispatch_prepared(
    prepared: PreparedBatch,
    context: AppendContext<'_>,
) -> Result<PartitionProduceResponse, BrokerError> {
    let mut response = PartitionProduceResponse {
        index: context.partition_index,
        ..Default::default()
    };
    let commit = CommitKey {
        topic: context.topic_name,
        partition: context.partition_index,
        pid: prepared.producer_id,
        epoch: prepared.producer_epoch,
        base_seq: prepared.base_sequence,
        last_offset_delta: prepared.last_offset_delta,
        max_timestamp: prepared.max_timestamp,
    };
    let data = build_produce_data(prepared, context.leader_epoch);
    let (ack_tx, ack_rx) = oneshot::channel();
    let job = WriterMessage::Produce(ProduceJob { data, ack: ack_tx });
    if context.partition.writer_tx.send(job).await.is_err() {
        response.error_code = codes::NOT_LEADER_OR_FOLLOWER;
        return Ok(response);
    }
    match tokio::time::timeout(context.timeout, ack_rx).await {
        Ok(Ok(Ok(base_offset))) => {
            finalize_ack(
                &mut response,
                context.partition,
                context.acks,
                context.timeout,
                base_offset,
                context.producer_state,
                &commit,
            )
            .await;
        }
        Ok(Ok(Err(error))) => response.error_code = codes::from_broker_error(&error),
        Ok(Err(_)) => response.error_code = codes::NOT_LEADER_OR_FOLLOWER,
        Err(_) => response.error_code = codes::REQUEST_TIMED_OUT,
    }
    Ok(response)
}

async fn handle_duplicate(
    batch: &PreparedBatch,
    producer_state: &crate::producer_state::ProducerState,
    partition: &crate::partition::Partition,
    topic_name: &str,
    partition_index: i32,
    acks: i16,
    timeout: Duration,
) -> Option<PartitionProduceResponse> {
    if batch.producer_id < 0 {
        return None;
    }
    let decision = producer_state
        .check(
            topic_name,
            krabka_ids::PartitionIndex(partition_index),
            batch.producer_id,
            batch.producer_epoch,
            batch.base_sequence,
            batch.last_offset_delta,
        )
        .await;
    let (error_code, base_offset) = match decision {
        crate::producer_state::Decision::Duplicate { base_offset } => {
            let error_code = if acks == ACKS_ALL {
                let target = base_offset + i64::from(batch.last_offset_delta) + 1;
                let deadline = std::time::Instant::now() + timeout;
                if partition
                    .await_hw_at_least(Offset(target), deadline)
                    .await
                    .is_ok()
                {
                    codes::NONE
                } else {
                    codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND
                }
            } else {
                codes::NONE
            };
            (error_code, base_offset)
        }
        crate::producer_state::Decision::OutOfOrder => (codes::OUT_OF_ORDER_SEQUENCE_NUMBER, -1),
        crate::producer_state::Decision::Fenced => (codes::INVALID_PRODUCER_EPOCH, -1),
        crate::producer_state::Decision::Append => return None,
    };
    Some(PartitionProduceResponse {
        index: partition_index,
        error_code,
        base_offset,
        ..Default::default()
    })
}

struct PartitionGateError {
    code: i16,
    current_leader: Option<LeaderIdAndEpoch>,
}

/// The broker-wide policy that the per-partition Produce gate applies.
///
/// The gate reads one partition record out of the metadata image and compares
/// it against this node. It needs the node id to decide whether this node
/// leads the partition, the broker default for `min.insync.replicas` for the
/// `acks=all` check, and the witness role, which refuses every client write.
#[derive(Clone, Copy)]
struct BrokerProducePolicy {
    /// This node's id, compared against the image's partition leader.
    node_id: krabka_metadata::NodeId,
    /// Broker default for `min.insync.replicas`. A topic override wins over
    /// it.
    default_min_insync_replicas: i32,
    /// `true` when this node is a data-bearing witness.
    is_witness: bool,
}

fn validate_partition_gate(
    topic_name: &str,
    partition_index: i32,
    acks: i16,
    partitions: &PartitionRegistry,
    log_dir_status: &crate::log_dir_status::LogDirRegistry,
    image: &krabka_metadata::MetadataImage,
    broker_policy: BrokerProducePolicy,
) -> Result<(Arc<crate::partition::Partition>, i32), PartitionGateError> {
    let BrokerProducePolicy {
        node_id: this_node_id,
        default_min_insync_replicas,
        is_witness,
    } = broker_policy;
    let Some(record) = image
        .partition(topic_name, partition_index)
        .filter(|_| !topic_name.is_empty())
    else {
        return Err(PartitionGateError {
            code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            current_leader: None,
        });
    };
    let leader = LeaderIdAndEpoch {
        leader_id: i32::try_from(record.leader.0).unwrap_or(NO_LEADER_ID),
        leader_epoch: record.leader_epoch.0,
        ..Default::default()
    };
    let Some(partition) = partitions.get(topic_name, krabka_ids::PartitionIndex(partition_index))
    else {
        return Err(PartitionGateError {
            code: codes::NOT_LEADER_OR_FOLLOWER,
            current_leader: Some(leader),
        });
    };
    // A witness replicates the partition and counts toward
    // `min.insync.replicas`, but it serves no client traffic, so it accepts no
    // Produce. The guard is explicit because the leader check below reads
    // `record.leader != this_node_id && !partition.diskless`: a diskless
    // partition skips the leader check outright, so without this guard a
    // diskless Produce could land on a witness. NOT_LEADER_OR_FOLLOWER is the
    // code that makes a Kafka client refresh its metadata and produce
    // somewhere else.
    if is_witness {
        return Err(PartitionGateError {
            code: codes::NOT_LEADER_OR_FOLLOWER,
            current_leader: Some(leader),
        });
    }
    if record.leader != this_node_id && !partition.diskless {
        return Err(PartitionGateError {
            code: codes::NOT_LEADER_OR_FOLLOWER,
            current_leader: Some(leader),
        });
    }
    if log_dir_status.is_offline(&partition.log_dir.load()) {
        return Err(PartitionGateError {
            code: codes::KAFKA_STORAGE_ERROR,
            current_leader: None,
        });
    }
    if acks == ACKS_ALL
        && i32::try_from(record.isr.len()).unwrap_or(i32::MAX)
            < topic_min_insync_replicas(image, topic_name, default_min_insync_replicas)
    {
        return Err(PartitionGateError {
            code: codes::NOT_ENOUGH_REPLICAS,
            current_leader: None,
        });
    }
    let leader_epoch = partition
        .current_leader_epoch
        .load(std::sync::atomic::Ordering::Acquire);
    Ok((partition, leader_epoch))
}

async fn validate_transactional_produce(
    batch: &PreparedBatch,
    coordinator: &crate::txn::coordinator::TxnCoordinator,
    image: &krabka_metadata::MetadataImage,
    topic_name: &str,
    partition: i32,
) -> Result<Option<i16>, BrokerError> {
    if !batch.attributes.is_transactional() || batch.producer_id < 0 {
        return Ok(None);
    }
    let transactional_id = coordinator.tid_for_pid(krabka_log::ProducerId(batch.producer_id));
    let Some(entry_mutex) = transactional_id
        .as_ref()
        .and_then(|transactional_id| coordinator.get(transactional_id))
    else {
        return Ok(None);
    };
    let mut entry = entry_mutex.lock().await;
    if entry.has_staged_producer_identity() {
        return Ok(Some(codes::INVALID_TXN_STATE));
    }
    if entry.producer_epoch != batch.producer_epoch {
        return Ok(Some(codes::INVALID_PRODUCER_EPOCH));
    }
    let topic_partition = crate::txn::state::TopicPartition {
        topic: topic_name.to_string(),
        partition: krabka_ids::PartitionIndex(partition),
    };
    let completed = matches!(
        entry.state,
        crate::txn::state::TxnState::CompleteCommit | crate::txn::state::TxnState::CompleteAbort
    );
    if entry.partitions.contains(&topic_partition) && !completed {
        return Ok(None);
    }
    if !entry
        .state
        .can_transition_to(crate::txn::state::TxnState::Ongoing)
    {
        return Ok(Some(codes::INVALID_TXN_STATE));
    }
    if completed {
        entry.partitions.clear();
    }
    entry.state = crate::txn::state::TxnState::Ongoing;
    entry.partitions.insert(topic_partition);
    entry.last_update_ms = crate::txn::util::now_millis();
    let snapshot = entry.clone();
    drop(entry);
    let version = crate::txn::version::resolve_txn_version(image);
    coordinator.put(snapshot, version).await?;
    Ok(None)
}

/// Borrowed bundle of the idempotent-producer dedup identity and the fields
/// that a `producer_state.commit` record needs.
///
/// The struct groups the eight positional `commit` arguments into one value,
/// so the commit call exists in exactly one place, [`finalize_ack`].
struct CommitKey<'a> {
    topic: &'a str,
    partition: i32,
    pid: i64,
    epoch: i16,
    base_seq: i32,
    last_offset_delta: i32,
    max_timestamp: i64,
}

/// Finalize a successful writer append.
///
/// The function applies the `acks=-1` high-watermark durability gate, sets the
/// response `error_code` and `base_offset`, and records the
/// idempotent-producer commit exactly once when `pid >= 0`.
///
/// The behavior per path:
///   * `acks != -1`: NONE, then commit.
///   * `acks == -1`, HW reaches target: NONE, then commit.
///   * `acks == -1`, HW gate times out: `NOT_ENOUGH_REPLICAS_AFTER_APPEND`,
///     then commit. The append is durable on the leader, so the idempotent
///     tracker must advance. A retry is then recognized as a duplicate and
///     not as out-of-order.
///
/// Note that the commit happens on *both* the success and the timeout
/// `acks=-1` sub-paths. The function therefore always commits once it has
/// decided the `error_code` and the `base_offset`.
async fn finalize_ack(
    out: &mut PartitionProduceResponse,
    part: &Arc<Partition>,
    acks: i16,
    timeout: Duration,
    base_offset: Offset,
    producer_state: &Arc<crate::producer_state::ProducerState>,
    key: &CommitKey<'_>,
) {
    let target = base_offset + i64::from(key.last_offset_delta) + 1;
    if acks == ACKS_ALL {
        let deadline = std::time::Instant::now() + timeout;
        out.error_code = match part.await_hw_at_least(target, deadline).await {
            Ok(()) => codes::NONE,
            Err(_timeout) => codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
        };
    } else {
        out.error_code = codes::NONE;
    }
    // Unwrap the assigned `Offset` into the wire `base_offset` response field.
    out.base_offset = base_offset.0;
    // Only record the idempotent-producer commit if the appended batch is still
    // on the leader's log. A failover-rejoin divergence truncation can remove
    // the batch while the acks=all HW gate above is waiting (the gate then times
    // out); recording the truncated batch would make a retry dedup against an
    // offset the log no longer holds, and the retry's HW gate would wait forever
    // for a high watermark that can never reach the vanished offset. Skipping
    // the commit lets the retry re-append fresh instead.
    //
    // This is a best-effort check: a truncation racing in between this read and
    // the commit below could still record a stale entry. That is tolerated
    // because the replicator calls `ProducerState::truncate` after *every* log
    // truncation, so any entry stranded by such a race is dropped by the next
    // truncation/failover. Do not "harden" this by removing the check — the
    // check is what avoids recording the common (already-truncated) case.
    if key.pid >= 0 && part.log_end_offset() < target {
        // Evidence for the on-cluster failover verification: the appended batch
        // was truncated before its dedup commit, so we skip the commit (the
        // retry re-appends). Seeing this fire confirms the Bug-D path executed.
        tracing::warn!(
            topic = key.topic,
            partition = key.partition,
            base_offset = base_offset.0,
            target = target.0,
            leo = part.log_end_offset().0,
            "produce: appended batch truncated before dedup commit; skipping commit so retry re-appends"
        );
    }
    if key.pid >= 0 && part.log_end_offset() >= target {
        producer_state
            .commit(
                key.topic,
                krabka_ids::PartitionIndex(key.partition),
                (key.pid, key.epoch),
                (key.base_seq, key.last_offset_delta),
                // Unwrap the assigned `Offset` into the dedup tracker's `i64`.
                (base_offset.0, key.max_timestamp),
            )
            .await;
    }
}

/// Build a topic-level error response for the KIP-516 id-resolution failures
/// `UNKNOWN_TOPIC_ID` and `INCONSISTENT_TOPIC_ID`.
///
/// Every partition row in the request gets the same error code. The function
/// sets `base_offset` to -1 to signal "no offset assigned". This matches
/// Kafka's behavior on pre-append errors.
fn build_topic_error_response(
    topic: &FramedTopic,
    code: i16,
) -> krabka_protocol::owned::produce_response::TopicProduceResponse {
    use krabka_protocol::owned::produce_response::{
        PartitionProduceResponse, TopicProduceResponse,
    };
    TopicProduceResponse {
        name: topic.name.clone(),
        topic_id: topic.topic_id,
        partition_responses: topic
            .partition_data
            .iter()
            .map(|p| PartitionProduceResponse {
                index: p.index,
                error_code: code,
                base_offset: INVALID_OFFSET,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// One partition's records, as they arrived on the wire and BEFORE any owned
/// decode or decompression.
///
/// The verbatim hot path keeps the producer's exact bytes here. The owned
/// legacy path carries an already-decoded payload.
enum PartitionPayload {
    /// v≥3 native records bytes captured zero-copy from the request frame.
    /// The value is a refcount view and not a copy. Nothing has validated or
    /// decompressed it yet. The per-partition dispatch validates the header
    /// and record structure, then decides between verbatim and owned.
    Slice(Bytes),
    /// Legacy v0-2 payload, or any pre-decoded payload. It always takes the
    /// owned path. The handler up-converts a v0/v1 `MessageSet` and never
    /// passes it through.
    Owned(RecordsPayload),
    /// Wire-null records field → `INVALID_REQUEST`.
    Null,
}

impl PartitionPayload {
    /// Records-field wire length in bytes.
    ///
    /// For the owned form it is `RecordsPayload::payload_len`. For the
    /// verbatim form it is the slice's own length. The KIP-13 bytes-in metrics
    /// and the producer byte-rate quota both use it.
    fn payload_len(&self) -> usize {
        match self {
            Self::Slice(b) => b.len(),
            Self::Owned(p) => p.payload_len(),
            Self::Null => 0,
        }
    }

    /// Number of records across the field's batches, for `messages_in_total`.
    ///
    /// Verbatim slices read each v2 batch header's `records_count` WITHOUT
    /// decompression. Owned payloads sum `records.len()` over their v2
    /// batches.
    fn message_count(&self) -> u64 {
        match self {
            Self::Slice(b) => count_records_in_v2_batches(b),
            Self::Owned(p) => p.as_v2().map_or(0, |batches| {
                batches.iter().map(|b| b.records.len() as u64).sum()
            }),
            Self::Null => 0,
        }
    }
}

/// Header-only framing of a `ProduceRequest`.
///
/// The field names match the owned struct's field names, so the handler body
/// differs only in the records form.
struct ProduceFramed {
    transactional_id: Option<String>,
    acks: i16,
    timeout_ms: i32,
    topic_data: Vec<FramedTopic>,
}

struct FramedTopic {
    name: String,
    topic_id: WireUuid,
    partition_data: Vec<FramedPartition>,
}

struct FramedPartition {
    index: i32,
    payload: PartitionPayload,
}

impl ProduceFramed {
    /// v≥3: build from the header-only `produce_framing` walk. This function
    /// decodes and decompresses no record body.
    fn from_framing(f: krabka_protocol::records::ProduceFraming) -> Self {
        Self {
            transactional_id: f.transactional_id,
            acks: f.acks,
            timeout_ms: f.timeout_ms,
            topic_data: f
                .topics
                .into_iter()
                .map(|t| FramedTopic {
                    name: t.name,
                    topic_id: WireUuid(t.topic_id.0),
                    partition_data: t
                        .partitions
                        .into_iter()
                        .map(|p| FramedPartition {
                            index: p.partition,
                            payload: match p.records {
                                Some(b) => PartitionPayload::Slice(b),
                                None => PartitionPayload::Null,
                            },
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// v0-2: wrap the fully-decoded legacy request. Every partition takes the
    /// owned path, because a legacy `MessageSet` up-conversion is never a
    /// passthrough.
    fn from_owned(req: ProduceRequest) -> Self {
        Self {
            transactional_id: req.transactional_id,
            acks: req.acks,
            timeout_ms: req.timeout_ms,
            topic_data: req
                .topic_data
                .into_iter()
                .map(|t| FramedTopic {
                    name: t.name,
                    topic_id: t.topic_id,
                    partition_data: t
                        .partition_data
                        .into_iter()
                        .map(|p| FramedPartition {
                            index: p.index,
                            payload: match p.records {
                                Some(rp) => PartitionPayload::Owned(rp),
                                None => PartitionPayload::Null,
                            },
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

fn produce_bytes_by_qos_tier(
    image: &krabka_metadata::MetadataImage,
    topics: &[FramedTopic],
) -> std::collections::BTreeMap<String, u64> {
    let mut out = std::collections::BTreeMap::new();
    for topic in topics {
        let topic_name = match crate::topic_resolve::resolve(image, &topic.name, topic.topic_id) {
            Ok(rec) => rec.name.as_str(),
            Err(_) => topic.name.as_str(),
        };
        let qos_tier = crate::config_keys::resolve_qos_tier(image, topic_name).to_string();
        let topic_bytes: u64 = topic
            .partition_data
            .iter()
            .map(|p| p.payload.payload_len() as u64)
            .sum();
        *out.entry(qos_tier).or_default() += topic_bytes;
    }
    out
}

/// Resolve a topic's broker-side `compression.type` from the metadata image.
///
/// `None` means Kafka's `producer` pass-through, with no recompression.
/// `Some(codec)` forces recompression of the batches whose codec differs. The
/// result matches the resolution that the partition writer applies through its
/// `LogConfig::compression_type`.
fn resolve_topic_compression(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<krabka_compression::CompressionType> {
    image
        .topic_config(topic)
        .and_then(|m| m.get(COMPRESSION_TYPE))
        .and_then(|v| parse_compression_type(v).ok())
        .flatten()
}

/// The KFC-1 produce-time delivery settings of one topic.
///
/// [`resolve_delivery_gate`] builds this once per topic, and only for a topic
/// whose `delivery.mode` is `scheduled`. An immediate topic resolves to `None`,
/// so no partition of it reads a clock, takes the log mutex, or looks at a
/// batch timestamp.
///
/// On a scheduled topic a batch's `max_timestamp` is its delivery time. Both
/// rejections read that one v2 header field, which
/// [`validate_one_v2_batch`] already extracted into
/// [`ValidatedHeader::max_timestamp`]. Neither decodes a record, decompresses a
/// body, or changes the verbatim-passthrough decision, so a scheduled topic
/// keeps the same zero-copy append an immediate topic gets.
#[derive(Debug, Clone, Copy, PartialEq)]
struct DeliveryGate {
    /// `delivery.max.delay.ms`: the largest delay accepted, measured forward
    /// from produce time. `None` is the `-1` sentinel and removes the bound.
    max_delay: Option<Time>,
    /// `delivery.schedule.monotonic`: reject a batch that would make the
    /// partition's schedule run backwards.
    monotonic: bool,
}

impl DeliveryGate {
    /// Whether this batch earns `INVALID_TIMESTAMP` (32).
    ///
    /// `delivery_ms` is the batch's `max_timestamp`, `produced_at_ms` is the
    /// broker's clock reading for this produce, and `log` is the target
    /// partition's log.
    fn rejects(self, delivery_ms: i64, produced_at_ms: i64, log: &Mutex<Log>) -> bool {
        self.exceeds_max_delay(delivery_ms, produced_at_ms)
            || (self.monotonic && schedule_runs_backwards(log, delivery_ms))
    }

    /// Whether `delivery_ms` sits further ahead of `produced_at_ms` than
    /// `delivery.max.delay.ms` allows.
    ///
    /// The bound is one-sided. It limits how far ahead a producer may schedule
    /// a batch and says nothing about a delivery time in the past, which comes
    /// due at once.
    fn exceeds_max_delay(self, delivery_ms: i64, produced_at_ms: i64) -> bool {
        self.max_delay
            .is_some_and(|delay| delivery_ms.saturating_sub(produced_at_ms) > delay.millis_i64())
    }
}

/// Whether `log` already holds a delivery time later than `delivery_ms`.
///
/// This is the `delivery.schedule.monotonic` test. KFC-1 defines it against
/// "the largest delivery time already in the partition", and the log answers
/// that as an existence query: one record scheduled strictly after this batch
/// is one record this batch would hold up. Delivery is offset-ordered for a
/// classic group, so such a batch stalls the partition's schedule instead of
/// overtaking, and the config turns that silent stall into an error the
/// producer that caused it can see.
///
/// Every batch still waiting has a delivery time above the broker's activation
/// cutoff and every batch already delivered has one at or below it, so whenever
/// the partition holds a waiting batch at all, the largest delivery time in it
/// *is* the largest waiting one.
///
/// [`Log::offset_for_timestamp`] skips a segment whose own cached maximum sits
/// below the target, so a schedule that runs forward — the accepted case —
/// costs one integer comparison per segment and no disk read. Only a rejected
/// batch pays for an index lookup and a bounded scan.
fn schedule_runs_backwards(log: &Mutex<Log>, delivery_ms: i64) -> bool {
    let Some(later) = delivery_ms.checked_add(1) else {
        // Nothing can be scheduled after `i64::MAX`.
        return false;
    };
    // Recover a poisoned guard rather than fail the produce. The log data stays
    // consistent enough to read a timestamp out of, and the partition writer
    // takes the same view of a poisoned lock.
    log.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .offset_for_timestamp(later)
        .is_some()
}

/// Resolve a topic's KFC-1 delivery settings from the metadata image.
///
/// `None` means `delivery.mode=immediate`, the default and Kafka's behavior:
/// the produce path then does no delivery work for the topic at all. The two
/// settings come from [`resolve_delivery_max_delay`] and
/// [`resolve_delivery_schedule_monotonic`], which fall back to their defaults
/// on a corrupt value exactly as the other produce-side config reads do.
fn resolve_delivery_gate(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<DeliveryGate> {
    let scheduled = image
        .topic_config(topic)
        .and_then(|configs| configs.get(DELIVERY_MODE))
        .map(String::as_str)
        == Some(DELIVERY_MODE_SCHEDULED);
    scheduled.then(|| DeliveryGate {
        max_delay: resolve_delivery_max_delay(image, topic),
        monotonic: resolve_delivery_schedule_monotonic(image, topic),
    })
}

/// All the per-batch HEADER fields that the broker's produce gates need.
///
/// The gates are the leadership epoch stamp, the transactional verify, the
/// idempotent dedup, and the `acks=-1` HW target. The struct holds these
/// fields without materializing owned records. On the verbatim path they come
/// from the v2 batch header through
/// [`validate_one_v2_batch`]. On the owned fallback they come from the decoded
/// [`RecordBatch`] header. The values are identical on both paths.
#[derive(Debug)]
struct PreparedBatch {
    attributes: Attributes,
    last_offset_delta: i32,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    /// The append source. It is either the producer's verbatim bytes on the
    /// passthrough path, or the decoded owned batch on the fallback path. On
    /// the verbatim path the writer stamps the leader epoch at append time. On
    /// the owned path the code below stamps it onto the owned batch.
    source: PreparedSource,
}

#[derive(Debug)]
enum PreparedSource {
    /// Validated, single, CRC-checked v2 batch. The writer appends the
    /// producer's exact bytes after every declared record was parsed.
    Verbatim(Bytes),
    /// Decoded owned batch. This is the complete fallback path. When the
    /// producer compressed the batch, `RecordBatch::decode` decompressed it
    /// here.
    Owned(RecordBatch),
}

impl PreparedBatch {
    fn from_header(header: ValidatedHeader, bytes: Bytes) -> Self {
        Self {
            attributes: header.attributes,
            last_offset_delta: header.last_offset_delta,
            max_timestamp: header.max_timestamp,
            producer_id: header.producer_id,
            producer_epoch: header.producer_epoch,
            base_sequence: header.base_sequence,
            source: PreparedSource::Verbatim(bytes),
        }
    }

    fn from_owned(batch: RecordBatch) -> Self {
        Self {
            attributes: batch.attributes,
            last_offset_delta: batch.last_offset_delta,
            max_timestamp: batch.max_timestamp,
            producer_id: batch.producer_id,
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            source: PreparedSource::Owned(batch),
        }
    }
}

/// Decide the append shape for one partition's records and extract the header
/// fields that the gates need without materializing owned records on the
/// verbatim path.
///
/// The verbatim-passthrough predicate holds only when ALL of these hold. It
/// matches the writer's recompression gate exactly:
///   1. the records are a v≥3 native-v2 slice, not legacy and not a wire-null
///      field;
///   2. the slice is exactly one complete, CRC-valid v2 batch whose body
///      contains exactly the declared structurally valid records;
///   3. `timestamp_type == CreateTime`; a client-supplied log-append-time
///      batch is invalid;
///   4. there is no broker-side recompression. The topic's `compression.type`
///      is `producer` pass-through, which is `None`, OR it equals the batch's
///      own codec.
///
/// On any miss the function decodes the records into an owned `RecordBatch`.
/// That is the complete fallback. The verbatim path transiently decompresses
/// compressed bodies only to validate their record structure, then discards
/// that buffer and retains the original compressed wire bytes.
/// [`decode_owned_batch`] up-converts the legacy v0-2 payloads.
///
/// The function returns the response error *code* on a bad field, either
/// `INVALID_REQUEST` or `INVALID_RECORD`.
fn prepare_batch(
    payload: PartitionPayload,
    topic_compression: Option<krabka_compression::CompressionType>,
    topic_name: &str,
    metrics: &crate::metrics::BrokerMetrics,
    policy: RecordDecompressionPolicy,
) -> Result<PreparedBatch, i16> {
    let bytes = match payload {
        // Legacy / pre-decoded payload: always owned.
        PartitionPayload::Owned(rp) => {
            let batch = decode_owned_batch(rp, topic_name, metrics, policy)?;
            validate_owned_client_batch(&batch)?;
            return Ok(PreparedBatch::from_owned(batch));
        }
        PartitionPayload::Null => return Err(codes::INVALID_REQUEST),
        PartitionPayload::Slice(b) => b,
    };

    // Owned fallback for a v≥3 records slice that the verbatim predicate
    // rejects. Routes the raw field bytes through `RecordsPayload::from_bytes`
    // — which dispatches v2 (parse every batch) vs legacy (v0/v1 `MessageSet`,
    // kept opaque) by the magic byte — then through `decode_owned_batch`, the
    // SAME pipeline the request decoder used before this change. This is what
    // up-converts a v1 `MessageSet` carried over a v≥3 produce (older
    // message-format clients) and surfaces INVALID_RECORD on malformed bytes.
    let owned_fallback = |bytes: Bytes| -> Result<PreparedBatch, i16> {
        match RecordsPayload::from_bytes_with_policy(bytes, policy) {
            Ok(rp) => decode_owned_batch(rp, topic_name, metrics, policy).and_then(|batch| {
                validate_owned_client_batch(&batch)?;
                Ok(PreparedBatch::from_owned(batch))
            }),
            Err(_) => Err(codes::INVALID_RECORD),
        }
    };
    // Extract the header fields into owned values up front so the borrow of
    // `bytes` (via the `ValidatedBatch`) ends before any `owned_fallback(bytes)`
    // move or the final `Verbatim(bytes)` construction.
    let validated = match validate_one_v2_batch(&bytes) {
        Ok(batch) if batch.total_len == bytes.len() => batch,
        _ => return owned_fallback(bytes),
    };
    let header = ValidatedHeader::from(&validated);
    let attributes = header.attributes;
    validate_client_batch_header(header)?;

    // (4) No recompression: producer pass-through, or target == current codec.
    if let Some(target) = topic_compression
        && target != attributes.compression()
    {
        return owned_fallback(bytes);
    }
    validated
        .validate_records(policy)
        .map_err(|_| codes::INVALID_RECORD)?;

    Ok(PreparedBatch::from_header(header, bytes))
}

/// The KIP-467 `error_message` a schema rejection carries.
///
/// The per-record `record_errors` say which records failed and why; this is
/// the partition-level line a client shows when it does not read them, and it
/// is what a pre-v8 client would have seen had the field existed for it.
const SCHEMA_REJECTION_MESSAGE: &str = "one or more records failed schema validation";

/// The largest number of per-record errors one rejected batch reports.
///
/// A batch can hold thousands of records and a producer that framed none of
/// them would otherwise make the broker build a response larger than the
/// request. The first few name the problem; the producer does not need the
/// rest to act.
const MAX_RECORD_ERRORS: usize = 8;

/// Check every validated field of every record in `prepared` against the
/// registry.
///
/// `Ok(())` admits the batch. `Err(record_errors)` rejects it whole, which is
/// what the batch's own CRC requires: the broker appends the producer's exact
/// bytes, so it cannot drop one record without re-encoding the batch. The
/// returned rows name the offending records.
///
/// # The second decode
///
/// The verbatim path materializes no records — it walks them to check their
/// structure and throws each one away — so there is no key or value here to
/// look at. This decodes the batch again, for inspection only, and then
/// discards the decoded view and leaves `prepared` untouched. The log still
/// holds exactly what the producer wrote. The cost is a second CRC pass and,
/// on a compressed batch, a second decompression, paid only on a topic that
/// asked for validation.
async fn validate_batch_schemas(
    prepared: &PreparedBatch,
    gate: SchemaGate,
    validator: Option<&Arc<SchemaValidator>>,
    topic_name: &str,
    policy: RecordDecompressionPolicy,
    metrics: &crate::metrics::BrokerMetrics,
) -> Result<(), Vec<BatchIndexAndErrorMessage>> {
    let Some(validator) = validator else {
        // The topic asked for validation and this broker has no registry to
        // ask. Admitting the record would make the topic's setting a lie, so
        // this fails closed, and it fails the same way for every record in the
        // batch rather than naming one.
        let reason = RejectReason::RegistryUnavailable(
            "no [schema_registry] section is configured on this broker".to_owned(),
        );
        metrics.record_schema_validation_rejection(topic_name, reason.label());
        return Err(vec![BatchIndexAndErrorMessage {
            batch_index: 0,
            batch_index_error_message: Some(reason.to_string()),
            ..Default::default()
        }]);
    };

    let check = SchemaCheck {
        validator,
        gate,
        topic_name,
        metrics,
    };
    let mut errors = Vec::new();
    match &prepared.source {
        PreparedSource::Owned(batch) => {
            for (index, record) in batch.records.iter().enumerate() {
                check
                    .record(
                        index,
                        record.key.as_deref(),
                        record.value.as_deref(),
                        &mut errors,
                    )
                    .await;
                if errors.len() >= MAX_RECORD_ERRORS {
                    break;
                }
            }
        }
        PreparedSource::Verbatim(bytes) => {
            let mut cursor: &[u8] = bytes;
            // `prepare_batch` already proved this decodes; a failure here is
            // not reachable through it, and treating it as "cannot validate"
            // is the safe reading if it ever became reachable.
            let Ok(batch) = RecordBatchBorrowed::decode_borrow_with_policy(&mut cursor, policy)
            else {
                let reason = RejectReason::Unframed("batch did not decode".to_owned());
                metrics.record_schema_validation_rejection(topic_name, reason.label());
                return Err(vec![BatchIndexAndErrorMessage {
                    batch_index: 0,
                    batch_index_error_message: Some(reason.to_string()),
                    ..Default::default()
                }]);
            };
            for (index, record) in batch.iter().enumerate() {
                // `prepare_batch`'s `validate_records` walk already parsed
                // every record, so this is not reachable through it. It fails
                // closed anyway: this is a different walk from that one, and if
                // the two ever disagree, a validated topic must not admit a
                // record the broker could not read. Breaking without a row
                // would leave `errors` empty and admit the batch.
                let Ok(record) = record else {
                    let reason = RejectReason::Unframed("record did not decode".to_owned());
                    metrics.record_schema_validation_rejection(topic_name, reason.label());
                    errors.push(BatchIndexAndErrorMessage {
                        batch_index: i32::try_from(index).unwrap_or(i32::MAX),
                        batch_index_error_message: Some(reason.to_string()),
                        ..Default::default()
                    });
                    break;
                };
                check
                    .record(index, record.key, record.value, &mut errors)
                    .await;
                if errors.len() >= MAX_RECORD_ERRORS {
                    break;
                }
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

/// Everything the per-record check needs that does not change between
/// records, held together so that the check takes one argument for its
/// context and one for the record.
#[derive(Clone, Copy)]
struct SchemaCheck<'a> {
    validator: &'a Arc<SchemaValidator>,
    gate: SchemaGate,
    topic_name: &'a str,
    metrics: &'a crate::metrics::BrokerMetrics,
}

impl SchemaCheck<'_> {
    /// Check one record's key and value, appending a row for each that failed.
    ///
    /// A null field is skipped rather than rejected. A null key is ordinary,
    /// and a null value is a tombstone, which a compacted topic needs —
    /// rejecting one would make schema validation and compaction mutually
    /// exclusive.
    async fn record(
        self,
        index: usize,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        errors: &mut Vec<BatchIndexAndErrorMessage>,
    ) {
        let batch_index = i32::try_from(index).unwrap_or(i32::MAX);
        for (wanted, role, field) in [
            (self.gate.key, Role::Key, key),
            (self.gate.value, Role::Value, value),
        ] {
            if !wanted {
                continue;
            }
            let Some(field) = field else { continue };
            if let Err(reason) = self
                .validator
                .check(self.topic_name, role, self.gate.mode, field, self.metrics)
                .await
            {
                self.metrics
                    .record_schema_validation_rejection(self.topic_name, reason.label());
                errors.push(BatchIndexAndErrorMessage {
                    batch_index,
                    batch_index_error_message: Some(reason.to_string()),
                    ..Default::default()
                });
            }
        }
    }
}

/// The v2 batch header fields that the gates need, copied out of a borrowed
/// [`ValidatedBatch`] so that the code can move the verbatim `Bytes`
/// afterward.
#[derive(Debug, Clone, Copy)]
struct ValidatedHeader {
    base_offset: i64,
    attributes: Attributes,
    last_offset_delta: i32,
    records_count: i32,
    max_timestamp: i64,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
}

impl From<&ValidatedBatch<'_>> for ValidatedHeader {
    fn from(v: &ValidatedBatch<'_>) -> Self {
        Self {
            base_offset: v.header.base_offset.get(),
            attributes: Attributes(v.header.attributes.get()),
            last_offset_delta: v.header.last_offset_delta.get(),
            records_count: v.header.records_count.get(),
            max_timestamp: v.header.max_timestamp.get(),
            producer_id: v.header.producer_id.get(),
            producer_epoch: v.header.producer_epoch.get(),
            base_sequence: v.header.base_sequence.get(),
        }
    }
}

/// Apply Kafka's client-origin v2 batch-header invariants without decoding
/// the record body. Every field is covered by the batch CRC that
/// [`validate_one_v2_batch`] checked before this function runs.
fn validate_client_batch_header(batch: ValidatedHeader) -> Result<(), i16> {
    validate_client_batch_fields(
        batch.attributes,
        batch.base_offset,
        batch.last_offset_delta,
        batch.records_count,
        batch.producer_id,
        batch.base_sequence,
    )
}

fn validate_owned_client_batch(batch: &RecordBatch) -> Result<(), i16> {
    let records_count = i32::try_from(batch.records.len()).map_err(|_| codes::INVALID_RECORD)?;
    validate_client_batch_fields(
        batch.attributes,
        batch.base_offset,
        batch.last_offset_delta,
        records_count,
        batch.producer_id,
        batch.base_sequence,
    )
}

fn validate_client_batch_fields(
    attributes: Attributes,
    base_offset: i64,
    last_offset_delta: i32,
    records_count: i32,
    producer_id: i64,
    base_sequence: i32,
) -> Result<(), i16> {
    let offset_count = last_offset_delta.checked_add(1);
    if base_offset != 0
        || offset_count.is_none_or(|count| count <= 0 || count != records_count)
        || records_count <= 0
        || attributes.is_control_batch()
        || (producer_id >= 0 && base_sequence < 0)
    {
        return Err(codes::INVALID_RECORD);
    }
    if attributes.timestamp_type() != TimestampType::CreateTime {
        return Err(codes::INVALID_TIMESTAMP);
    }
    Ok(())
}

/// Decode or up-convert a legacy or pre-decoded `RecordsPayload` into one
/// owned record batch.
///
/// The function up-converts a v0/v1 `MessageSet` and counts it once. A v2
/// sequence with anything other than one batch gives `INVALID_RECORD`, as does
/// a failed up-conversion.
fn decode_owned_batch(
    payload: RecordsPayload,
    topic_name: &str,
    metrics: &crate::metrics::BrokerMetrics,
    policy: RecordDecompressionPolicy,
) -> Result<RecordBatch, i16> {
    match payload {
        RecordsPayload::V2(batches) => exactly_one_v2_batch(batches),
        RecordsPayload::Raw(bytes) => match RecordsPayload::from_bytes_with_policy(bytes, policy) {
            Ok(RecordsPayload::V2(batches)) => exactly_one_v2_batch(batches),
            Ok(RecordsPayload::Raw(_) | RecordsPayload::Legacy(_)) | Err(_) => {
                Err(codes::INVALID_RECORD)
            }
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            Ok(RecordsPayload::FileRegions(_)) => Err(codes::INVALID_RECORD),
        },
        #[cfg(any(
            target_os = "linux",
            target_os = "macos",
            target_os = "ios",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "freebsd",
            target_os = "dragonfly",
        ))]
        RecordsPayload::FileRegions(_) => Err(codes::INVALID_REQUEST),
        RecordsPayload::Legacy(bytes) => {
            match krabka_records_legacy::legacy_to_v2_with_policy(&bytes, policy) {
                Ok(rb) => {
                    if !topic_name.is_empty() {
                        metrics.record_produce_message_conversion(topic_name);
                    }
                    let mut rb = rb;
                    rb.base_offset = 0;
                    rb.last_offset_delta = i32::try_from(rb.records.len())
                        .map_err(|_| codes::INVALID_RECORD)?
                        .checked_sub(1)
                        .ok_or(codes::INVALID_RECORD)?;
                    for (offset, record) in rb.records.iter_mut().enumerate() {
                        record.offset_delta =
                            i32::try_from(offset).map_err(|_| codes::INVALID_RECORD)?;
                    }
                    Ok(rb)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "legacy_to_v2 failed");
                    Err(codes::INVALID_RECORD)
                }
            }
        }
    }
}

fn exactly_one_v2_batch(mut batches: Vec<RecordBatch>) -> Result<RecordBatch, i16> {
    if batches.len() != 1 {
        return Err(codes::INVALID_RECORD);
    }
    Ok(batches.pop().expect("length checked"))
}

/// Build the writer's [`ProduceData`] from a prepared batch and stamp the
/// leader epoch.
///
/// Verbatim batches carry the producer's exact bytes. Owned batches carry the
/// decoded `RecordBatch`, whose `partition_leader_epoch` the caller has
/// already stamped.
fn build_produce_data(prepared: PreparedBatch, leader_epoch: i32) -> ProduceData {
    let is_transactional = prepared.attributes.is_transactional();
    match prepared.source {
        PreparedSource::Verbatim(bytes) => ProduceData::Verbatim(VerbatimBatch {
            last_offset_delta: prepared.last_offset_delta,
            max_timestamp: prepared.max_timestamp,
            // Wrap the atomic-loaded raw epoch into the log seam's `LeaderEpoch`.
            leader_epoch: krabka_log::LeaderEpoch(leader_epoch),
            // Wrap the produce path's decode-side `i64` into the log seam's `ProducerId`.
            producer_id: krabka_log::ProducerId(prepared.producer_id),
            producer_epoch: prepared.producer_epoch,
            base_sequence: prepared.base_sequence,
            is_transactional,
            bytes,
        }),
        PreparedSource::Owned(mut batch) => {
            // The writer stamps the verbatim path's epoch in-place at append;
            // the owned batch carries it as a struct field instead.
            batch.partition_leader_epoch = leader_epoch;
            ProduceData::Owned(batch)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc, time::Duration};

    use assert2::{assert, check};
    use bytes::{Bytes, BytesMut};
    use krabka_compression::{CompressionType, RecordDecompressionPolicy};
    use krabka_ids::Offset;
    use krabka_metadata::{
        BrokerConfigRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicConfigRecord,
        TopicRecord,
    };
    use krabka_protocol::{
        owned::produce_response::{LeaderIdAndEpoch, PartitionProduceResponse},
        records::{Attributes, Record, RecordBatch, RecordsPayload},
    };
    use krabka_units::{Time, bytes, convert::TimeExt, fraction, millis, secs};
    use uuid::Uuid;

    use super::{
        BrokerProducePolicy, DeliveryGate, FramedPartition, FramedTopic, MIN_INSYNC_REPLICAS,
        PartitionInput, PartitionPayload, PartitionServices, PreparedBatch, PreparedSource,
        build_topic_error_response, decode_owned_batch, diskless_role_ready, prepare_batch,
        process_partition, produce_bytes_by_qos_tier, replica_state_matches_image,
        replication_target_matches_image, resolve_delivery_gate, resolve_topic_compression,
        topic_min_insync_replicas, validate_batch_schemas, validate_partition_gate,
    };
    use crate::config_keys::{
        DELIVERY_MAX_DELAY_MS, DELIVERY_MODE, DELIVERY_MODE_IMMEDIATE, DELIVERY_MODE_SCHEDULED,
        DELIVERY_SCHEDULE_MONOTONIC,
    };

    fn image_with_topic(topic: &str, isr: &[u64]) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: topic.into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(isr.len().max(1)).unwrap(),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: topic.into(),
            partition: 0,
            leader: krabka_audit::NodeId(*isr.first().unwrap_or(&1)),
            replicas: isr.iter().copied().map(krabka_audit::NodeId).collect(),
            isr: isr.iter().copied().map(krabka_audit::NodeId).collect(),
            leader_epoch: krabka_metadata::LeaderEpoch(0),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        img
    }

    #[test]
    fn replication_target_must_match_the_complete_image_identity() {
        let image = image_with_topic("orders", &[1, 2]);
        let record = image.partition("orders", 0).expect("partition");
        let current = crate::partition::ReplicationTarget {
            topic_id: Some(Uuid::nil()),
            leader_node_id: record.leader,
            leader_epoch: record.leader_epoch,
        };
        assert!(replication_target_matches_image(
            &current,
            Some(Uuid::nil()),
            record
        ));

        assert!(!replication_target_matches_image(
            &crate::partition::ReplicationTarget {
                leader_epoch: krabka_metadata::LeaderEpoch(record.leader_epoch.0 + 1),
                ..current
            },
            Some(Uuid::nil()),
            record
        ));
        assert!(!replication_target_matches_image(
            &current,
            Some(Uuid::new_v4()),
            record
        ));
    }

    #[test]
    fn replica_state_must_install_the_image_epoch_and_exact_isr() {
        let image = image_with_topic("orders", &[1, 2]);
        let record = image.partition("orders", 0).expect("partition");
        let mut state = crate::replica_state::ReplicaState::new();
        assert!(!replica_state_matches_image(&state, record));

        state.install_isr(
            &record.isr,
            &record.replicas,
            record.leader,
            std::time::Instant::now(),
        );
        assert!(replica_state_matches_image(&state, record));

        state.current_leader_epoch = krabka_ids::LeaderEpoch(record.leader_epoch.0 + 1);
        assert!(!replica_state_matches_image(&state, record));
    }

    #[tokio::test]
    async fn diskless_produce_waits_for_installed_role_and_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let image = image_with_topic("orders", &[1]);
        let record = image.partition("orders", 0).expect("partition");
        let log = krabka_log::Log::open(
            crate::log_dir::partition_dir(dir.path(), "orders", 0),
            krabka_log::LogConfig::default(),
        )
        .unwrap();
        let partition = crate::broker::spawn_partition(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            dir.path().to_path_buf(),
            log,
            crate::log_dir_status::LogDirRegistry::default(),
            Arc::new(crate::producer_state::ProducerState::new()),
            true,
        );

        assert!(!diskless_role_ready(&partition, record));
        partition
            .install_leader_change(record.leader.0, record.leader_epoch.0)
            .await;
        assert!(diskless_role_ready(&partition, record));
        partition
            .install_leader_change(record.leader.0, record.leader_epoch.0 + 1)
            .await;
        assert!(!diskless_role_ready(&partition, record));
    }

    /// Spawn one local partition and run the Produce gate over it.
    ///
    /// Returns `None` when the gate admits the write, or the complete gate
    /// error when it refuses. The gate itself is synchronous, but
    /// `spawn_partition` starts the writer-actor task, so the callers still
    /// need a Tokio runtime.
    fn produce_gate(
        image: &MetadataImage,
        node_id: krabka_audit::NodeId,
        is_witness: bool,
        diskless: bool,
    ) -> Option<(i16, Option<LeaderIdAndEpoch>)> {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = krabka_log::Log::open(
            crate::log_dir::partition_dir(dir.path(), "orders", 0),
            krabka_log::LogConfig::default(),
        )
        .expect("open log");
        let partitions = crate::partition_registry::PartitionRegistry::new();
        partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            crate::broker::spawn_partition(
                "orders".into(),
                krabka_ids::PartitionIndex(0),
                dir.path().to_path_buf(),
                log,
                crate::log_dir_status::LogDirRegistry::default(),
                Arc::new(crate::producer_state::ProducerState::new()),
                diskless,
            ),
        );
        validate_partition_gate(
            "orders",
            0,
            1,
            &partitions,
            &crate::log_dir_status::LogDirRegistry::default(),
            image,
            BrokerProducePolicy {
                node_id,
                default_min_insync_replicas: 1,
                is_witness,
            },
        )
        .err()
        .map(|error| (error.code, error.current_leader))
    }

    #[tokio::test]
    async fn witness_refuses_every_produce_including_a_diskless_partition() {
        // Node 1 leads `orders` at epoch 0 and node 2 follows. A refused row
        // carries the real leader, so a Kafka client re-targets without a full
        // Metadata round-trip.
        let image = image_with_topic("orders", &[1, 2]);
        let refused = Some((
            crate::codes::NOT_LEADER_OR_FOLLOWER,
            Some(LeaderIdAndEpoch {
                leader_id: 1,
                leader_epoch: 0,
                ..Default::default()
            }),
        ));
        for (name, node_id, is_witness, diskless, want) in [
            (
                "witness leads a classic partition",
                1,
                true,
                false,
                refused.clone(),
            ),
            // The leader check reads `leader != this_node && !diskless`, so a
            // diskless partition skips it. The witness guard is the only thing
            // that refuses these two rows.
            (
                "witness leads a diskless partition",
                1,
                true,
                true,
                refused.clone(),
            ),
            (
                "witness follows a diskless partition",
                2,
                true,
                true,
                refused.clone(),
            ),
            (
                "plain broker leads a classic partition",
                1,
                false,
                false,
                None,
            ),
            (
                "plain broker follows a diskless partition",
                2,
                false,
                true,
                None,
            ),
            (
                "plain broker follows a classic partition",
                2,
                false,
                false,
                refused.clone(),
            ),
        ] {
            let got = produce_gate(&image, krabka_audit::NodeId(node_id), is_witness, diskless);
            assert!(got == want, "{name}: got {got:?}, want {want:?}");
        }
    }

    #[tokio::test]
    async fn witness_on_another_node_leaves_this_brokers_produce_gate_alone() {
        // Node 2 carries `broker.witness=true` in the image. This node is 1
        // and it leads the partition, so the gate must still admit the write.
        let mut image = image_with_topic("orders", &[1, 2]);
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_audit::NodeId(2),
            config_name: crate::config_keys::BROKER_WITNESS.into(),
            config_value: Some(crate::config_keys::WITNESS_TRUE.into()),
        }));
        let got = produce_gate(&image, krabka_audit::NodeId(1), false, false);
        assert!(got.is_none(), "got {got:?}");
    }

    fn set_min_isr(img: &mut MetadataImage, topic: &str, n: i32) {
        let mut o = BTreeMap::new();
        o.insert(MIN_INSYNC_REPLICAS.into(), n.to_string());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: topic.into(),
            overrides: o,
        }));
    }

    fn set_qos_tier(img: &mut MetadataImage, topic: &str, tier: &str) {
        let mut o = BTreeMap::new();
        o.insert(crate::config_keys::QOS_TIER.into(), tier.into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: topic.into(),
            overrides: o,
        }));
    }

    fn framed_topic(name: &str, payload_lens: &[usize]) -> FramedTopic {
        FramedTopic {
            name: name.into(),
            topic_id: krabka_protocol::primitives::uuid::Uuid::ZERO,
            partition_data: payload_lens
                .iter()
                .enumerate()
                .map(|(idx, len)| FramedPartition {
                    index: i32::try_from(idx).unwrap(),
                    payload: PartitionPayload::Slice(Bytes::from(vec![0; *len])),
                })
                .collect(),
        }
    }

    fn encode_batch(batch: &RecordBatch) -> Bytes {
        let mut buf = BytesMut::new();
        batch.encode(&mut buf).expect("encode record batch");
        buf.freeze()
    }

    #[test]
    fn topic_min_isr_defaults_to_one_when_unset() {
        let img = image_with_topic("t", &[1, 2, 3]);
        assert!(topic_min_insync_replicas(&img, "t", 1) == 1);
    }

    #[test]
    fn topic_min_isr_reads_override_when_set() {
        let mut img = image_with_topic("t", &[1, 2, 3]);
        set_min_isr(&mut img, "t", 3);
        assert!(topic_min_insync_replicas(&img, "t", 1) == 3);
    }

    #[test]
    fn topic_min_isr_uses_broker_fallback_unless_valid_override_exists() {
        let cases = [(None, 2), (Some(3), 3)];

        for (override_value, expected) in cases {
            let mut img = image_with_topic("t", &[1, 2, 3]);
            if let Some(value) = override_value {
                set_min_isr(&mut img, "t", value);
            }

            assert!(topic_min_insync_replicas(&img, "t", 2) == expected);
        }
    }

    #[test]
    fn topic_min_isr_default_one_on_unknown_topic() {
        let img = MetadataImage::new(Uuid::nil());
        assert!(
            topic_min_insync_replicas(&img, "ghost", 1) == 1,
            "missing topic_config must default to 1, not crash"
        );
    }

    #[test]
    fn topic_min_isr_default_one_on_malformed_value() {
        let mut img = image_with_topic("t", &[1, 2, 3]);
        let mut o = BTreeMap::new();
        o.insert(MIN_INSYNC_REPLICAS.into(), "not-a-number".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: o,
        }));
        assert!(
            topic_min_insync_replicas(&img, "t", 1) == 1,
            "unparseable value must fall back to permissive default 1"
        );
    }

    #[test]
    fn topic_min_isr_handles_topic_config_without_min_isr_key() {
        // Topic has *some* override (e.g. retention.ms) but no
        // min.insync.replicas — still defaults to 1.
        let mut img = image_with_topic("t", &[1, 2, 3]);
        let mut o = BTreeMap::new();
        o.insert("retention.ms".into(), "60000".into());
        img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: o,
        }));
        assert!(topic_min_insync_replicas(&img, "t", 1) == 1);
    }

    #[test]
    fn produce_bytes_by_qos_tier_groups_topic_payload_bytes() {
        let mut img = image_with_topic("gold-topic", &[1]);
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "default-topic".into(),
            topic_id: Uuid::from_u128(2),
            partitions: 1,
            replication_factor: 1,
        }));
        set_qos_tier(&mut img, "gold-topic", "gold");

        let topics = vec![
            framed_topic("gold-topic", &[10, 15]),
            framed_topic("default-topic", &[7]),
            framed_topic("gold-topic", &[5]),
        ];

        let grouped = produce_bytes_by_qos_tier(&img, &topics);

        let expected: BTreeMap<String, u64> = BTreeMap::from([
            ("gold".to_string(), 30),
            (crate::config_keys::DEFAULT_QOS_TIER.to_string(), 7),
        ]);
        assert!(grouped == expected);
    }

    #[test]
    fn build_topic_error_response_preserves_topic_and_partition_fields() {
        use krabka_protocol::owned::produce_response::{
            LeaderIdAndEpoch, PartitionProduceResponse, TopicProduceResponse,
        };
        let topic_id = krabka_protocol::primitives::uuid::Uuid([7; 16]);
        let topic = FramedTopic {
            name: "orders".into(),
            topic_id,
            partition_data: vec![
                FramedPartition {
                    index: 0,
                    payload: PartitionPayload::Null,
                },
                FramedPartition {
                    index: 4,
                    payload: PartitionPayload::Slice(Bytes::from_static(b"not-a-batch")),
                },
            ],
        };

        let resp = build_topic_error_response(&topic, crate::codes::UNKNOWN_TOPIC_ID);

        let error_partition = |index: i32| PartitionProduceResponse {
            index,
            error_code: crate::codes::UNKNOWN_TOPIC_ID,
            base_offset: -1,
            log_append_time_ms: -1,
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
            current_leader: LeaderIdAndEpoch {
                leader_id: -1,
                leader_epoch: -1,
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            },
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        let expected = TopicProduceResponse {
            name: "orders".to_string(),
            topic_id,
            partition_responses: vec![error_partition(0), error_partition(4)],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn resolve_topic_compression_distinguishes_producer_and_forced_codecs() {
        let cases = [
            // "producer" keeps the producer's codec → no forced compression.
            ("producer", None),
            // A concrete codec forces recompression to that codec.
            ("zstd", Some(CompressionType::Zstd)),
        ];
        for (config_value, want) in cases {
            let mut img = image_with_topic("t", &[1]);
            let mut overrides = BTreeMap::new();
            overrides.insert(
                crate::config_keys::COMPRESSION_TYPE.into(),
                config_value.into(),
            );
            img.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
                topic: "t".into(),
                overrides,
            }));
            assert!(
                resolve_topic_compression(&img, "t") == want,
                "compression.type {config_value:?}"
            );
        }
    }

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
    fn scheduled_log(
        dir: &std::path::Path,
        deliveries: &[i64],
    ) -> std::sync::Mutex<krabka_log::Log> {
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
        partitions.insert("sched".to_string(), krabka_ids::PartitionIndex(0), part);

        // The accepted case appends, so it comes last.
        let accepted_delivery_ms = now_ms + 900_000;
        let cases = [
            (
                now_ms + 300_000,
                PartitionProduceResponse {
                    index: 0,
                    error_code: crate::codes::INVALID_TIMESTAMP,
                    ..Default::default()
                },
                "before the delivery time the partition already holds",
            ),
            (
                now_ms + 7_200_000,
                PartitionProduceResponse {
                    index: 0,
                    error_code: crate::codes::INVALID_TIMESTAMP,
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
                    delivery,
                    topic_name: "sched".into(),
                    topic_denied: false,
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

    #[test]
    fn decode_owned_batch_preserves_non_default_header_and_record_fields() {
        let batch = RecordBatch {
            last_offset_delta: 1,
            max_timestamp: 9876,
            producer_id: 22,
            producer_epoch: 3,
            base_sequence: 11,
            records: vec![
                Record {
                    value: Some(Bytes::from_static(b"a")),
                    ..Default::default()
                },
                Record {
                    value: Some(Bytes::from_static(b"b")),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let decoded = decode_owned_batch(
            RecordsPayload::V2(vec![batch]),
            "orders",
            &crate::metrics::BrokerMetrics::new(),
            RecordDecompressionPolicy::default(),
        )
        .expect("decode owned batch");

        check!(decoded.last_offset_delta == 1);
        check!(decoded.max_timestamp == 9876);
        check!(decoded.producer_id == 22);
        check!(decoded.producer_epoch == 3);
        check!(decoded.base_sequence == 11);
        assert!(decoded.records.len() == 2);
        check!(decoded.records[0].value.as_deref() == Some(&b"a"[..]));
        check!(decoded.records[1].value.as_deref() == Some(&b"b"[..]));
    }

    #[test]
    fn decode_owned_batch_rejects_empty_v2_payload() {
        let err = decode_owned_batch(
            RecordsPayload::V2(Vec::new()),
            "orders",
            &crate::metrics::BrokerMetrics::new(),
            RecordDecompressionPolicy::default(),
        )
        .unwrap_err();
        assert!(err == crate::codes::INVALID_RECORD);
    }

    #[test]
    fn legacy_produce_offsets_are_reassigned_consecutively() {
        let records = vec![
            krabka_records_legacy::ParsedRecord {
                offset: Offset(10),
                timestamp: Some(100),
                key: None,
                value: Some(Bytes::from_static(b"a")),
            },
            krabka_records_legacy::ParsedRecord {
                offset: Offset(20),
                timestamp: Some(200),
                key: None,
                value: Some(Bytes::from_static(b"b")),
            },
        ];
        let mut legacy = BytesMut::new();
        krabka_records_legacy::encode_flat_message_set(
            records,
            krabka_records_legacy::Magic::V1,
            &mut legacy,
        );

        let prepared = prepare_batch(
            PartitionPayload::Owned(RecordsPayload::Legacy(legacy.freeze())),
            None,
            "orders",
            &crate::metrics::BrokerMetrics::new(),
            RecordDecompressionPolicy::default(),
        )
        .unwrap();
        match prepared.source {
            PreparedSource::Owned(batch) => {
                check!(batch.base_offset == 0);
                check!(batch.last_offset_delta == 1);
                check!(batch.records[0].offset_delta == 0);
                check!(batch.records[1].offset_delta == 1);
            }
            PreparedSource::Verbatim(_) => panic!("expected one converted owned batch"),
        }
    }

    #[test]
    fn record_decompression_policy_limits_owned_and_verbatim_produce() {
        let policy = RecordDecompressionPolicy::new(fraction(1.0), bytes(1), bytes(32)).unwrap();
        let metrics = crate::metrics::BrokerMetrics::new();

        let v2 = RecordBatch {
            attributes: Attributes::default().with_compression(CompressionType::Lz4),
            records: vec![Record {
                value: Some(Bytes::from(vec![b'x'; 4096])),
                ..Default::default()
            }],
            ..Default::default()
        };
        let wire = encode_batch(&v2);
        let error = prepare_batch(
            PartitionPayload::Slice(wire.clone()),
            None,
            "t",
            &metrics,
            policy,
        )
        .unwrap_err();
        assert!(error == crate::codes::INVALID_RECORD);

        let error = prepare_batch(
            PartitionPayload::Slice(wire.clone()),
            Some(CompressionType::Zstd),
            "t",
            &metrics,
            policy,
        )
        .unwrap_err();
        assert!(error == crate::codes::INVALID_RECORD);
        assert!(
            prepare_batch(
                PartitionPayload::Slice(wire),
                Some(CompressionType::Zstd),
                "t",
                &metrics,
                RecordDecompressionPolicy::default(),
            )
            .is_ok()
        );

        let records = vec![krabka_records_legacy::ParsedRecord {
            offset: Offset(0),
            timestamp: Some(1),
            key: None,
            value: Some(Bytes::from(vec![b'x'; 4096])),
        }];
        let mut legacy = BytesMut::new();
        krabka_records_legacy::encode_compressed_message_set(
            &records,
            krabka_records_legacy::Magic::V1,
            CompressionType::Lz4,
            &mut legacy,
        )
        .unwrap();
        let error = decode_owned_batch(
            RecordsPayload::Legacy(legacy.freeze()),
            "t",
            &metrics,
            policy,
        )
        .unwrap_err();
        assert!(error == crate::codes::INVALID_RECORD);
    }

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

    /// An idempotent retry, `Decision::Duplicate`, under `acks=all` waits
    /// again for the HW to reach the duplicate's *last offset + 1* before it
    /// claims success.
    ///
    /// The duplicate spans offsets 0..=2, so the durability target is 3, which
    /// is `base_offset 0 + last_offset_delta 2 + 1`. When the HW is stuck at
    /// 2, the wait times out and gives `NOT_ENOUGH_REPLICAS_AFTER_APPEND`. The
    /// `+ 1` matters. A mutant that flips it to `- 1` would target offset 1,
    /// which HW 2 already satisfies, and would wrongly return `NONE`.
    #[tokio::test]
    async fn duplicate_acks_all_waits_for_last_offset_plus_one() {
        use krabka_protocol::owned::produce_response::PartitionProduceResponse;

        let dir = tempfile::tempdir().unwrap();
        let image = Arc::new(image_with_topic("orders", &[1]));
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

        // Materialize the local leader replica for "orders"-0.
        let part_dir = crate::log_dir::partition_dir(dir.path(), "orders", 0);
        std::fs::create_dir_all(&part_dir).unwrap();
        let log = krabka_log::Log::open(&part_dir, krabka_log::LogConfig::default()).unwrap();
        let part = crate::broker::spawn_partition(
            "orders".to_string(),
            krabka_ids::PartitionIndex(0),
            dir.path().to_path_buf(),
            log,
            log_dir_status.clone(),
            Arc::clone(&producer_state),
            false,
        );
        let record = image.partition("orders", 0).expect("partition");
        part.install_replication_target(Some(Uuid::nil()), record.leader.0, record.leader_epoch.0)
            .await;
        part.install_isr(&record.isr, &record.replicas, record.leader)
            .await;
        // Push LEO to 3 so the HW can be clamped to 2 (one below the target).
        {
            let mut batch = RecordBatch {
                last_offset_delta: 2,
                records: (0..3)
                    .map(|i| Record {
                        offset_delta: i,
                        value: Some(Bytes::from_static(b"v")),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            };
            part.log
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .append(&mut batch)
                .expect("seed source records");
        }
        assert!(part.log_end_offset() == krabka_log::Offset(3));
        part.set_follower_hw(krabka_log::Offset(2)).await;
        assert!(part.high_watermark().await == krabka_log::Offset(2));
        partitions.insert("orders".to_string(), krabka_ids::PartitionIndex(0), part);

        // Pre-seed the dedup tracker so the incoming batch is a Duplicate whose
        // recorded base_offset is 0 and span is 0..=2.
        let pid: i64 = 7777;
        producer_state
            .commit(
                "orders",
                krabka_ids::PartitionIndex(0),
                (pid, 0),
                (0, 2),
                (0, 0),
            )
            .await;

        // Incoming (retried) batch: same pid/epoch/base_sequence/span.
        let payload = encode_batch(&RecordBatch {
            producer_id: pid,
            producer_epoch: 0,
            base_sequence: 0,
            last_offset_delta: 2,
            records: (0..3)
                .map(|i| Record {
                    offset_delta: i,
                    value: Some(Bytes::from_static(b"v")),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        });

        let resp: PartitionProduceResponse = process_partition(
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
                txn_id_denied: false,
                acks: -1,
                timeout: Duration::from_millis(50),
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

        check!(resp.base_offset == 0);
        check!(
            resp.error_code == crate::codes::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            "HW 2 < target 3 must time out; a `-1` mutant would target offset 1 and return NONE"
        );
    }

    #[test]
    fn consume_producer_quota_tuple_match_overage_throttles() {
        use krabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                },
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app-x".into()),
                },
            ],
            config_key: "producer_byte_rate".into(),
            config_value: Some(1024.0),
        }));
        let buckets = crate::quota::QuotaBuckets::new();
        // Tuple match → 4096 bytes overage at 1024 B/s → throttle > 0.
        let delay_match = crate::quota::consume_producer_quota(
            &img,
            &buckets,
            "alice",
            "app-x",
            "default",
            4096,
            secs(1),
        );
        assert!(
            delay_match > <Time as TimeExt>::ZERO,
            "tuple quota match should throttle on overage; got {delay_match:?}"
        );
        // No tuple match for client_id="other"; no (user=alice)-only quota exists.
        let buckets2 = crate::quota::QuotaBuckets::new();
        let delay_other = crate::quota::consume_producer_quota(
            &img,
            &buckets2,
            "alice",
            "other",
            "default",
            4096,
            secs(1),
        );
        assert!(
            delay_other == <Time as TimeExt>::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }

    // ── verbatim passthrough predicate (prepare_batch + build_produce_data) ──
    //
    // These drive the zero-copy dispatch end to end: `prepare_batch` validates
    // the v2 batch and decides verbatim-vs-owned; `build_produce_data` maps the
    // result to the writer's `ProduceData`, stamping the leader epoch.
    mod verbatim {
        use assert2::{assert, check};
        use bytes::{Bytes, BytesMut};
        use krabka_compression::{CompressionType, RecordDecompressionPolicy};
        use krabka_protocol::records::{
            Attributes, CRC_COVERAGE_START, HEADER_LEN, Record, RecordBatch, RecordsPayload,
            TimestampType,
        };

        use super::super::{
            PartitionPayload, PreparedSource, ProduceData, build_produce_data, prepare_batch,
        };

        fn encode(b: &RecordBatch) -> Bytes {
            let mut buf = BytesMut::new();
            b.encode(&mut buf).unwrap();
            buf.freeze()
        }

        fn refresh_batch_crc(encoded: &mut [u8]) {
            let crc = crc32c::crc32c(&encoded[CRC_COVERAGE_START..]);
            encoded[CRC_COVERAGE_START - 4..CRC_COVERAGE_START].copy_from_slice(&crc.to_be_bytes());
        }

        fn plain_batch() -> RecordBatch {
            RecordBatch {
                base_offset: 0,
                partition_leader_epoch: -1,
                last_offset_delta: 0,
                max_timestamp: 42,
                producer_id: -1,
                records: vec![Record {
                    value: Some(Bytes::from_static(b"hello")),
                    ..Default::default()
                }],
                ..Default::default()
            }
        }

        #[test]
        fn message_count_reports_v2_record_total() {
            // Multi-record batch so the count can't be mistaken for a constant.
            let batch = RecordBatch {
                last_offset_delta: 2,
                records: vec![
                    Record {
                        value: Some(Bytes::from_static(b"a")),
                        ..Default::default()
                    },
                    Record {
                        value: Some(Bytes::from_static(b"b")),
                        ..Default::default()
                    },
                    Record {
                        value: Some(Bytes::from_static(b"c")),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            };
            let wire = encode(&batch);
            // A null field and a non-v2 (zeroed) slice both contribute zero.
            let cases = [
                (PartitionPayload::Slice(wire), 3, "v2 slice with 3 records"),
                (PartitionPayload::Null, 0, "null records field"),
                (
                    PartitionPayload::Slice(Bytes::from_static(&[0u8; 64])),
                    0,
                    "non-v2 zeroed slice",
                ),
            ];
            for (payload, want, label) in cases {
                assert!(payload.message_count() == want, "case: {label}");
            }
        }

        /// Run the full dispatch over a v≥3 records slice: first
        /// `prepare_batch`, then `build_produce_data` with the given leader
        /// epoch.
        fn dispatch_slice(
            slice: Bytes,
            topic_compression: Option<CompressionType>,
            leader_epoch: i32,
        ) -> ProduceData {
            let m = crate::metrics::BrokerMetrics::new();
            let prepared = prepare_batch(
                PartitionPayload::Slice(slice),
                topic_compression,
                "t",
                &m,
                RecordDecompressionPolicy::default(),
            )
            .unwrap();
            build_produce_data(prepared, leader_epoch)
        }

        #[test]
        fn passthrough_when_all_conditions_hold() {
            let b = plain_batch();
            let wire = encode(&b);
            let data = dispatch_slice(wire.clone(), None, 7);
            match data {
                ProduceData::Verbatim(v) => {
                    check!(&v.bytes[..] == &wire[..]);
                    check!(v.leader_epoch == 7);
                    check!(v.max_timestamp == 42);
                    check!(v.last_offset_delta == 0);
                }
                ProduceData::Owned(_) => panic!("expected Verbatim"),
                ProduceData::OwnedCommitMarker { .. } | ProduceData::OwnedControl(_) => {
                    panic!("expected producer data")
                }
            }
        }

        #[test]
        fn passthrough_when_target_codec_equals_current() {
            // Topic forces lz4; batch is already lz4 → no recompression needed.
            let mut b = plain_batch();
            b.attributes = b.attributes.with_compression(CompressionType::Lz4);
            let wire = encode(&b);
            let data = dispatch_slice(wire, Some(CompressionType::Lz4), 1);
            assert!(matches!(data, ProduceData::Verbatim(_)));
        }

        #[test]
        fn fallback_when_null_field() {
            // A wire-null records field is rejected as INVALID_REQUEST.
            let m = crate::metrics::BrokerMetrics::new();
            let err = prepare_batch(
                PartitionPayload::Null,
                None,
                "t",
                &m,
                RecordDecompressionPolicy::default(),
            )
            .unwrap_err();
            assert!(err == crate::codes::INVALID_REQUEST);
        }

        #[test]
        fn fallback_on_recompression_to_different_codec() {
            // Batch uncompressed, topic forces zstd → must recompress (owned).
            let b = plain_batch();
            let wire = encode(&b);
            let data = dispatch_slice(wire, Some(CompressionType::Zstd), 0);
            assert!(matches!(data, ProduceData::Owned(_)));
        }

        #[test]
        fn rejects_client_log_append_time() {
            let mut b = plain_batch();
            b.attributes = b
                .attributes
                .with_timestamp_type(TimestampType::LogAppendTime);
            let wire = encode(&b);
            let err = prepare_batch(
                PartitionPayload::Slice(wire),
                None,
                "t",
                &crate::metrics::BrokerMetrics::new(),
                RecordDecompressionPolicy::default(),
            )
            .unwrap_err();
            assert!(err == crate::codes::INVALID_TIMESTAMP);
        }

        #[test]
        fn rejects_client_control_batch() {
            let mut b = plain_batch();
            b.attributes = Attributes::default().with_control(true);
            let wire = encode(&b);
            let err = prepare_batch(
                PartitionPayload::Slice(wire),
                None,
                "t",
                &crate::metrics::BrokerMetrics::new(),
                RecordDecompressionPolicy::default(),
            )
            .unwrap_err();
            assert!(err == crate::codes::INVALID_RECORD);
        }

        #[test]
        fn rejects_invalid_client_batch_metadata_on_header_and_owned_paths() {
            let mut invalid_base_offset = plain_batch();
            invalid_base_offset.base_offset = 1;

            let mut invalid_offset_range = plain_batch();
            invalid_offset_range.last_offset_delta = -1;

            let mut inconsistent_count = plain_batch();
            inconsistent_count.last_offset_delta = 1;

            let mut empty = plain_batch();
            empty.records.clear();

            let mut invalid_sequence = plain_batch();
            invalid_sequence.producer_id = 7;
            invalid_sequence.producer_epoch = 0;
            invalid_sequence.base_sequence = -1;

            for (name, batch) in [
                ("invalid base offset", invalid_base_offset),
                ("invalid offset range", invalid_offset_range),
                ("inconsistent count", inconsistent_count),
                ("empty batch", empty),
                ("invalid producer sequence", invalid_sequence),
            ] {
                let payloads = [
                    PartitionPayload::Slice(encode(&batch)),
                    PartitionPayload::Owned(RecordsPayload::V2(vec![batch])),
                ];
                for payload in payloads {
                    let err = prepare_batch(
                        payload,
                        None,
                        "t",
                        &crate::metrics::BrokerMetrics::new(),
                        RecordDecompressionPolicy::default(),
                    )
                    .unwrap_err();
                    assert!(err == crate::codes::INVALID_RECORD, "case: {name}");
                }
            }
        }

        #[test]
        fn fallback_on_corrupt_crc_slice() {
            let b = plain_batch();
            let mut wire = encode(&b).to_vec();
            // Corrupt a body byte → CRC validation fails → owned fallback.
            let hdr_len = krabka_protocol::records::HEADER_LEN;
            wire[hdr_len] ^= 0xFF;
            // A corrupt CRC also fails the owned `RecordBatch::decode`, so the
            // fallback surfaces INVALID_RECORD (the prior decode-error code).
            let m = crate::metrics::BrokerMetrics::new();
            let err = prepare_batch(
                PartitionPayload::Slice(Bytes::from(wire)),
                None,
                "t",
                &m,
                RecordDecompressionPolicy::default(),
            )
            .unwrap_err();
            assert!(err == crate::codes::INVALID_RECORD);
        }

        #[test]
        fn rejects_crc_valid_malformed_record_body() {
            let mut wire = encode(&plain_batch()).to_vec();
            wire[HEADER_LEN] = 0; // zero-length first record body
            refresh_batch_crc(&mut wire);

            let error = prepare_batch(
                PartitionPayload::Slice(Bytes::from(wire)),
                None,
                "t",
                &crate::metrics::BrokerMetrics::new(),
                RecordDecompressionPolicy::default(),
            )
            .unwrap_err();
            assert!(error == crate::codes::INVALID_RECORD);
        }

        #[test]
        fn fallback_on_multiple_batches_in_slice() {
            // Kafka v2 records fields contain exactly one batch. A second
            // batch is invalid and must never be silently discarded.
            let b = plain_batch();
            let mut two = BytesMut::new();
            b.encode(&mut two).unwrap();
            b.encode(&mut two).unwrap();
            let err = prepare_batch(
                PartitionPayload::Slice(two.freeze()),
                None,
                "t",
                &crate::metrics::BrokerMetrics::new(),
                RecordDecompressionPolicy::default(),
            )
            .unwrap_err();
            assert!(err == crate::codes::INVALID_RECORD);
        }

        #[test]
        fn transactional_batch_can_pass_through() {
            let mut b = plain_batch();
            b.producer_id = 100;
            b.producer_epoch = 0;
            b.base_sequence = 0;
            b.attributes = b.attributes.with_transactional(true);
            let wire = encode(&b);
            let data = dispatch_slice(wire, None, 0);
            match data {
                ProduceData::Verbatim(v) => {
                    assert!(v.is_transactional);
                    assert!(v.producer_id == krabka_log::ProducerId(100));
                }
                ProduceData::Owned(_) => panic!("transactional data batch should pass through"),
                ProduceData::OwnedCommitMarker { .. } | ProduceData::OwnedControl(_) => {
                    panic!("expected producer data")
                }
            }
        }

        /// A producer-LZ4-compressed batch stays verbatim after structural
        /// validation, even when its decompressed form is 100 KiB and its
        /// compressed wire bytes are tiny.
        ///
        /// The stored `Verbatim.bytes` equal the compressed wire bytes, which
        /// are much smaller than the decompressed payload. The header fields
        /// `last_offset_delta` and `max_timestamp` come straight from the v2
        /// header. This test pins the no-reencoding guarantee.
        #[test]
        fn lz4_batch_passes_through_without_reencoding() {
            // 100 KiB of highly-compressible payload across many records.
            let big = vec![b'A'; 100 * 1024];
            let mut b = RecordBatch {
                last_offset_delta: 0,
                max_timestamp: 7_777,
                producer_id: -1,
                ..RecordBatch::default()
            };
            b.attributes = b.attributes.with_compression(CompressionType::Lz4);
            b.records.push(Record {
                value: Some(Bytes::from(big.clone())),
                ..Default::default()
            });
            let wire = encode(&b);
            // The compressed wire bytes must be far smaller than the raw payload,
            // so an accidental re-encode to an uncompressed batch is obvious.
            assert!(
                wire.len() < big.len() / 4,
                "lz4 wire ({} B) should be much smaller than raw ({} B)",
                wire.len(),
                big.len()
            );

            let data = dispatch_slice(wire.clone(), None, 3);
            match data {
                ProduceData::Verbatim(v) => {
                    // Stored bytes are the COMPRESSED wire bytes — verbatim, not
                    // re-encoded from decompressed records ("must stay compressed").
                    // Header fields came from the v2 header, no record decode.
                    check!(&v.bytes[..] == &wire[..]);
                    check!(v.bytes.len() == wire.len());
                    check!(v.bytes.len() < big.len());
                    check!(v.max_timestamp == 7_777);
                    check!(v.last_offset_delta == 0);
                    check!(v.leader_epoch == 3);
                }
                ProduceData::Owned(_) => {
                    panic!("lz4 producer batch must pass through verbatim")
                }
                ProduceData::OwnedCommitMarker { .. } | ProduceData::OwnedControl(_) => {
                    panic!("expected producer data")
                }
            }
        }

        /// HEADER fields drive the idempotent dedup over the verbatim path.
        ///
        /// `prepare_batch` exposes `producer_id`, `producer_epoch`,
        /// `base_sequence`, and `last_offset_delta`. It reads them from the v2
        /// header without materializing owned records. The values match what
        /// an owned decode of the same bytes would give.
        #[test]
        fn header_fields_drive_dedup_on_verbatim_path() {
            let mut b = plain_batch();
            b.producer_id = 4242;
            b.producer_epoch = 9;
            b.base_sequence = 17;
            b.last_offset_delta = 2;
            b.max_timestamp = 555;
            b.records.extend([
                Record {
                    value: Some(Bytes::from_static(b"second")),
                    ..Default::default()
                },
                Record {
                    value: Some(Bytes::from_static(b"third")),
                    ..Default::default()
                },
            ]);
            // Force lz4 so validation must decompress while the append still
            // retains the producer's exact compressed bytes.
            b.attributes = b.attributes.with_compression(CompressionType::Lz4);
            let wire = encode(&b);

            let m = crate::metrics::BrokerMetrics::new();
            let prepared = prepare_batch(
                PartitionPayload::Slice(wire.clone()),
                None,
                "t",
                &m,
                RecordDecompressionPolicy::default(),
            )
            .unwrap();
            assert!(matches!(prepared.source, PreparedSource::Verbatim(_)));
            check!(prepared.producer_id == 4242);
            check!(prepared.producer_epoch == 9);
            check!(prepared.base_sequence == 17);
            check!(prepared.last_offset_delta == 2);
            check!(prepared.max_timestamp == 555);

            // Cross-check: an owned decode of the same compressed bytes yields
            // the same header identity (proving the header read is correct).
            let mut cur: &[u8] = &wire;
            let owned = RecordBatch::decode(&mut cur).unwrap();
            check!(owned.producer_id == prepared.producer_id);
            check!(owned.producer_epoch == prepared.producer_epoch);
            check!(owned.base_sequence == prepared.base_sequence);
            check!(owned.last_offset_delta == prepared.last_offset_delta);
        }
    }

    /// A record the broker cannot decode must not be admitted by a validated
    /// topic.
    ///
    /// This is defence in depth, not a live hole: `prepare_batch` runs
    /// `validate_records` over every record before this function is reached,
    /// so a batch that gets here has already had each record parsed. The two
    /// walks are different code, though, and the batch-level decode a few
    /// lines above this one already fails closed for exactly that reason.
    /// Before the fix, the per-record arm broke out of the loop without
    /// recording anything, so `errors` stayed empty and the batch was
    /// *admitted* — the one outcome a validation feature must never produce.
    #[tokio::test]
    async fn a_record_that_does_not_decode_is_rejected_not_admitted() {
        let mut batch = RecordBatch {
            last_offset_delta: 0,
            producer_id: -1,
            ..RecordBatch::default()
        };
        // A null value is a tombstone, which the checker skips, so this record
        // passes without a registry call. That matters: if it recorded an
        // error of its own, `errors` would be non-empty and the test would
        // pass whether or not the decode failure was handled.
        batch.records.push(Record {
            value: None,
            ..Default::default()
        });
        let whole = encode_batch(&batch);
        // Claim one more record than the bytes carry. The v2 header is intact
        // and self-consistent, so the batch-level decode succeeds; the walk
        // then yields the real record and fails on the phantom one. The record
        // count is the i32 at offset 57 of the 61-byte header.
        let mut bytes = whole.to_vec();
        bytes[57..61].copy_from_slice(&2i32.to_be_bytes());
        // The batch-level decode verifies the CRC, so it has to be restored
        // over the edited count or this never reaches the per-record walk.
        // The CRC covers the header from offset 21 and then the record bytes.
        let crc = crc32c::crc32c_append(crc32c::crc32c(&bytes[21..61]), &bytes[61..]);
        bytes[17..21].copy_from_slice(&crc.to_be_bytes());
        let bytes = Bytes::from(bytes);

        let prepared = PreparedBatch {
            attributes: batch.attributes,
            last_offset_delta: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            source: PreparedSource::Verbatim(bytes),
        };
        // No request reaches the registry: the record fails to decode before
        // any field is checked, so the address is never dialled.
        let validator = Arc::new(
            crate::schema_validation::SchemaValidator::new(
                "http://127.0.0.1:1".to_owned(),
                false,
                100,
                secs(60),
                secs(5),
            )
            .expect("validator"),
        );
        let gate = crate::schema_validation::SchemaGate {
            key: false,
            value: true,
            mode: crate::schema_validation::ValidationMode::Id,
        };
        let metrics = crate::metrics::BrokerMetrics::new();

        let got = validate_batch_schemas(
            &prepared,
            gate,
            Some(&validator),
            "orders",
            RecordDecompressionPolicy::default(),
            &metrics,
        )
        .await;

        assert!(let Err(errors) = &got);
        check!(errors.len() == 1);
        // Index 1, the record that did not decode — not 0, which would mean the
        // batch-level arm above had fired instead and this test proved nothing.
        check!(errors[0].batch_index == 1);
        check!(
            errors[0]
                .batch_index_error_message
                .as_deref()
                .is_some_and(|m| m.contains("record did not decode")),
            "{:?}",
            errors[0].batch_index_error_message
        );
    }
}
