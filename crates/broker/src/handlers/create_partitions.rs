//! `CreatePartitions` (`api_key=37`), which serves
//! `kafka-topics --alter --partitions N`.
//!
//! When the caller omits `assignments`, the automatic replica placement
//! matches the `CreateTopics` path: [`crate::site_placement`] spreads the
//! replicas over the sites and pins leadership to the preferred site. An
//! explicit, validated `assignments` list, with one entry per *new*
//! partition, overrides the placement, and the handler uses it verbatim. That
//! matches the JVM flow
//! `kafka-topics --alter --partitions N --replica-assignment 0:1,1:2,...`.
//!
//! This file holds the request loop. Each stage it walks through lives in its
//! own submodule: `admission` for the quota and authorization preamble,
//! `assignment` for the replica placement, `apply` for the metadata records
//! and the local materialization, and `response` for the encoding and the
//! throttle.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        create_partitions_request::CreatePartitionsRequest,
        create_partitions_response::CreatePartitionsTopicResult,
    },
};
use krabka_raft::RaftError;

mod admission;
mod apply;
mod assignment;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use self::{
    admission::{denied_topics, partition_mutation_count},
    apply::{MaterializeContext, materialize_new_partitions, partition_records},
    assignment::resolve_new_partition_assignments,
    response::{create_partitions_response, encode_response, finish_response},
};
use crate::{
    broker::Broker,
    codes,
    config_keys::resolve_preferred_leader_site,
    error::BrokerError,
    handlers::create_topics::{diskless_wal_placement_error, site_broker_views},
};

#[tracing::instrument(
    name = "handle_create_partitions",
    level = "info",
    skip_all,
    fields(api = "CreatePartitions", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur: &[u8] = req_bytes;
    let req = CreatePartitionsRequest::decode(&mut cur, version)?;

    let node_id = broker.config.node_id;
    let partitions_map = broker.partitions.clone();
    let producer_state = broker.producer_state.clone();
    let log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    let log_dir_status = broker.log_dir_status.clone();
    let hot_tail = broker.hot_tail.clone();
    let wal_shards = broker.wal_shards.clone();

    let image = broker.controller.current_image();

    // KIP-599: count partition mutations before running handler logic so that
    // even invalid/rejected requests consume quota (bad-faith clients can't
    // escape throttling by sending malformed RPCs).
    let mutation_count = partition_mutation_count(&req, &image);
    let quota = crate::quota::apply_controller_mutation_quota_mode(
        &image,
        &broker.quota_buckets,
        ctx.principal.name.as_str(),
        ctx.client_id,
        mutation_count,
        broker.config.controller_mutation_quota_window,
        broker.config.quota_throttle_max,
        version >= 3,
    );
    if quota.is_rejected() {
        let results = req
            .topics
            .iter()
            .map(|topic| CreatePartitionsTopicResult {
                name: topic.name.clone(),
                error_code: codes::THROTTLING_QUOTA_EXCEEDED,
                ..Default::default()
            })
            .collect();
        return encode_response(
            &create_partitions_response(results, crate::quota::throttle_time_ms(quota.delay())),
            version,
        );
    }

    // ── ACL preamble ────────────────────────────────────────
    // Batch-authorize every topic name for `Alter`. Topics that come
    // back `Deny` short-circuit the partition-change loop and emit
    // TOPIC_AUTHORIZATION_FAILED on that topic row.
    let denied_topics = denied_topics(
        broker.config.authorizer.as_ref(),
        &image,
        ctx.principal,
        ctx.peer,
        &req,
    );

    let mut results: Vec<CreatePartitionsTopicResult> = Vec::with_capacity(req.topics.len());
    let preferred_site = resolve_preferred_leader_site(&image);

    for t in req.topics {
        let mut out = CreatePartitionsTopicResult {
            name: t.name.clone(),
            ..Default::default()
        };

        // Per-topic ACL check.
        if denied_topics.contains(&t.name) {
            out.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
            results.push(out);
            continue;
        }

        let Some(topic_rec) = image.topic(&t.name).cloned() else {
            out.error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
            out.error_message = Some(format!("unknown topic `{}`", t.name));
            results.push(out);
            continue;
        };

        let existing = topic_rec.partitions;
        let diskless = crate::config_keys::resolve_diskless(image.topic_config(&t.name));
        if t.count <= existing {
            out.error_code = codes::INVALID_PARTITIONS;
            out.error_message = Some(format!(
                "topic `{}` already has {} partitions; cannot decrease to {}",
                t.name, existing, t.count
            ));
            results.push(out);
            continue;
        }

        let brokers = site_broker_views(&image, node_id);
        let rf = topic_rec.replication_factor;
        let new_count = t.count;
        let new_partition_indices: Vec<i32> = (existing..new_count).collect();
        let new_partition_count = new_partition_indices.len();
        let new_assignments = match resolve_new_partition_assignments(
            t.assignments.as_ref(),
            &brokers,
            existing,
            new_partition_count,
            rf,
            preferred_site,
        ) {
            Ok(a) => a,
            Err((code, msg)) => {
                out.error_code = code;
                out.error_message = Some(msg);
                results.push(out);
                continue;
            }
        };

        if diskless
            && let Some(reason) =
                diskless_wal_placement_error(&image, &broker.config, existing, &new_assignments)
        {
            out.error_code = codes::INVALID_CONFIG;
            out.error_message = Some(reason);
            results.push(out);
            continue;
        }

        if req.validate_only {
            results.push(out);
            continue;
        }

        // Build batch: one V1Partition per new index. Under KIP-631 framing the
        // topic's partition count IS the number of PartitionRecords (the
        // `TopicRecord` carries no count), so CreatePartitions appends only the
        // new partition records — no `V1Topic` rewrite. The image derives the
        // grown count from the partitions map as these apply. (Re-submitting a
        // `V1Topic` would round-trip back to the pre-grow count and be rejected
        // by the strict-expansion `validate` on the apply path.)
        let records = partition_records(&t.name, &new_partition_indices, &new_assignments);

        match broker.controller.submit_change(records).await {
            Ok(_) => {
                materialize_new_partitions(
                    MaterializeContext {
                        partitions: &partitions_map,
                        log_dirs: &log_dirs,
                        log_config: &log_config,
                        log_dir_status: &log_dir_status,
                        producer_state: &producer_state,
                        producer_id_expiration: broker.config.producer_id_expiration,
                        max_produce_group: broker.config.max_produce_group,
                        partition_writer_queue_depth: broker.config.partition_writer_queue_depth,
                        diskless_wal_local_replica_count: broker
                            .config
                            .diskless_wal_local_replica_count,
                        node_id,
                        diskless,
                        topic_id: topic_rec.topic_id,
                        hot_tail: &hot_tail,
                        wal_shards: &wal_shards,
                        controller: &broker.controller,
                    },
                    &t.name,
                    &new_partition_indices,
                    &new_assignments,
                )
                .await;
            }
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                out.error_code = codes::NOT_CONTROLLER;
            }
            Err(e) => {
                tracing::error!(topic = %t.name, error = %e,
                    "CreatePartitions submit_change failed");
                out.error_code = codes::UNKNOWN_SERVER_ERROR;
            }
        }

        results.push(out);
    }

    // KIP-599: report the controller_mutation_rate throttle after response
    // assembly. It sets throttle_time_ms and records the window for the
    // connection loop's post-send mute (KIP-219).
    finish_response(broker, ctx, quota.delay(), results, version)
}
