//! `CreateTopics` (`api_key=19`). Routes through `Controller::submit_change`
//! so every topic/partition creation goes through the metadata quorum before
//! the partition directories are materialized on disk.
//!
//! Automatic replica placement is site-aware. See [`crate::site_placement`]
//! for the site spread and the leadership pinning it gives. An explicit
//! `assignments` field still wins, as it does in Kafka.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        create_topics_request::CreateTopicsRequest, create_topics_response::CreatableTopicResult,
    },
    primitives::uuid::Uuid as ProtoUuid,
};
use krabka_raft::RaftError;
use uuid::Uuid;

mod authorization;
mod materialize;
mod mutation_quota;
mod placement;
mod records;
mod response;

#[cfg(test)]
mod tests;

pub(crate) use self::placement::{round_robin_replicas, site_broker_views};
use self::{
    authorization::cluster_create_denied,
    materialize::{TopicMaterialization, materialize_topic},
    mutation_quota::mutation_count,
    placement::resolve_assignments,
    records::{topic_config_overrides, topic_records},
    response::{create_topics_response, encode_response, finish_response, topic_error_result},
};
use crate::{
    broker::Broker,
    codes,
    config_keys::{self, resolve_preferred_leader_site},
    error::BrokerError,
};

/// Leader epoch that a freshly created partition starts at. The committed
/// `PartitionRecord` and the handler-side leader-cache install must agree.
const INITIAL_LEADER_EPOCH: i32 = 0;

#[tracing::instrument(
    name = "handle_create_topics",
    level = "info",
    skip_all,
    fields(api = "CreateTopics", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    // ── ACL preamble ────────────────────────────────────────
    // Whole-request Cluster Create gate. On Deny, return
    // CLUSTER_AUTHORIZATION_FAILED on every topic row and short-circuit.
    let mut cursor = req_bytes;
    let req = CreateTopicsRequest::decode(&mut cursor, version)?;
    let image = broker.controller.current_image();
    if cluster_create_denied(broker, &image, ctx) {
        let results = req
            .topics
            .iter()
            .map(|topic| {
                topic_error_result(
                    topic.name.clone(),
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    Some("create-topics denied".into()),
                )
            })
            .collect();
        return encode_response(&create_topics_response(results, 0), version);
    }

    let controller = broker.controller.clone();
    let node_id = broker.config.node_id;
    let log_dirs = broker.config.all_log_dirs();
    let log_config = broker.config.log_config.clone();
    let log_dir_status = broker.log_dir_status.clone();
    let partitions_map = broker.partitions.clone();
    let producer_state = broker.producer_state.clone();
    let hot_tail = broker.hot_tail.clone();
    let wal_shards = broker.wal_shards.clone();

    // KIP-599: count mutations before running handler logic so that even
    // invalid requests consume quota (bad-faith clients can't escape by
    // sending malformed RPCs). num_partitions == -1 means "use cluster
    // default"; count it as 1 for accounting.
    let mutation_count = mutation_count(&req);
    let quota = crate::quota::apply_controller_mutation_quota_mode(
        &image,
        &broker.quota_buckets,
        &ctx.principal.name,
        ctx.client_id,
        mutation_count,
        broker.config.controller_mutation_quota_window,
        broker.config.quota_throttle_max,
        version >= 6,
    );
    if quota.is_rejected() {
        let results = req
            .topics
            .iter()
            .map(|topic| {
                topic_error_result(topic.name.clone(), codes::THROTTLING_QUOTA_EXCEEDED, None)
            })
            .collect();
        return encode_response(
            &create_topics_response(results, crate::quota::throttle_time_ms(quota.delay())),
            version,
        );
    }

    let mut results: Vec<CreatableTopicResult> = Vec::with_capacity(req.topics.len());
    let preferred_site = resolve_preferred_leader_site(&image);

    for topic_req in req.topics {
        let name = topic_req.name.clone();
        let partition_count = topic_req.num_partitions;

        // Kafka validates a topic's configs before it looks at placement, so a
        // rejected config wins over INVALID_PARTITIONS on the same topic.
        let config_overrides = topic_config_overrides(&topic_req);
        if let Err(reason) = config_keys::validate_topic_config_map(&config_overrides) {
            results.push(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(reason),
            ));
            continue;
        }

        // Reject invalid partition counts before attempting automatic placement.
        // Manual assignments use -1 for both count and replication factor.
        if topic_req.assignments.is_empty() && partition_count <= 0 {
            results.push(topic_error_result(name, codes::INVALID_PARTITIONS, None));
            continue;
        }

        // Read the current broker set from the controller's image, with the
        // site and the witness role of each broker. `site_broker_views` sorts
        // by node id for determinism, and it covers the race in which the
        // self-registration record has not reached the local image yet.
        let brokers = site_broker_views(&image, node_id);

        let assignments = match resolve_assignments(&topic_req, &brokers, preferred_site) {
            Ok(assignments) => assignments,
            Err(code) => {
                results.push(topic_error_result(name, code, None));
                continue;
            }
        };

        if assignments.is_empty() {
            // The placement cannot satisfy the request. RF above the broker
            // count is the common cause. Surface INVALID_REPLICATION_FACTOR
            // per Apache Kafka semantics.
            results.push(topic_error_result(
                name,
                codes::INVALID_REPLICATION_FACTOR,
                None,
            ));
            continue;
        }

        let topic_id = Uuid::new_v4();

        // Build the batch: one TopicRecord + N PartitionRecords.
        let records = topic_records(&topic_req, topic_id, &assignments, &config_overrides);
        let diskless = crate::broker::diskless_topic_config(Some(&config_overrides));

        let result = controller.submit_change(records).await;

        let error_code = match result {
            Ok(_) => {
                materialize_topic(
                    TopicMaterialization {
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
                        topic_id,
                        hot_tail: &hot_tail,
                        wal_shards: &wal_shards,
                        controller: &controller,
                    },
                    &name,
                    &assignments,
                )
                .await;
                codes::NONE
            }
            Err(RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))) => {
                codes::TOPIC_ALREADY_EXISTS
            }
            Err(RaftError::Metadata(krabka_metadata::MetadataError::InvalidRecord(_))) => {
                // E.g., `partitions <= 0` rejected by image::validate.
                codes::INVALID_PARTITIONS
            }
            Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => codes::NOT_CONTROLLER,
            Err(e) => {
                tracing::error!(topic = %name, error = %e, "CreateTopics submit_change failed");
                codes::UNKNOWN_SERVER_ERROR
            }
        };

        // Convert uuid::Uuid → krabka_protocol::primitives::uuid::Uuid.
        let proto_uuid = ProtoUuid(topic_id.into_bytes());

        let mut result = CreatableTopicResult {
            name,
            topic_id: proto_uuid,
            error_code,
            ..Default::default()
        };

        if error_code == codes::NONE {
            result.num_partitions = i32::try_from(assignments.len()).unwrap_or(i32::MAX);
            result.replication_factor = assignments
                .first()
                .and_then(|replicas| i16::try_from(replicas.len()).ok())
                .unwrap_or(-1);
            // KIP-525 (v5+): return an empty configs list to satisfy
            // clients that unconditionally call `configs().stream()`.
            result.configs = Some(Vec::new());
        }
        results.push(result);
    }

    finish_response(broker, ctx, results, quota.delay(), version).await
}
