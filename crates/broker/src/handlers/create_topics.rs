//! `CreateTopics` (`api_key=19`). Routes through `Controller::submit_change`
//! so every topic/partition creation goes through the metadata quorum before
//! the partition directories are materialized on disk.
//!
//! Automatic replica placement is site-aware. See [`crate::site_placement`]
//! for the site spread and the leadership pinning it gives. An explicit
//! `assignments` field still wins, as it does in Kafka.
//!
//! KIP-525: a v5+ row reports what the topic was created as -- its partition
//! count, its replication factor, and its whole effective configuration, the
//! list [`crate::handlers::describe_configs`] answers a `TOPIC` resource with.
//! A client that reads it needs no follow-up `DescribeConfigs`, which is what
//! Terraform's `kafka_topic`, Connect's `TopicAdmin` and Streams'
//! `InternalTopicManager` do. Kafka gates the whole disclosure on a second,
//! per-topic ACL check -- `DescribeConfigs` on `Topic(name)` -- and a denial
//! withholds it behind `topicConfigErrorCode` without failing the create.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        create_topics_request::CreateTopicsRequest,
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
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
    authorization::{cluster_create_denied, describe_configs_denied},
    materialize::{TopicMaterialization, materialize_topic},
    mutation_quota::mutation_count,
    placement::resolve_assignments,
    records::{topic_config_overrides, topic_records},
    response::{
        create_topics_response, effective_topic_configs, encode_response, finish_response,
        topic_error_result,
    },
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

pub(crate) fn diskless_wal_placement_error(
    image: &krabka_metadata::MetadataImage,
    config: &crate::config::BrokerConfig,
    first_partition: i32,
    assignments: &[Vec<krabka_raft::NodeId>],
) -> Option<String> {
    let mut brokers = image
        .brokers()
        .map(|broker| (broker.node_id, broker.rack.clone()))
        .collect::<Vec<_>>();
    if brokers.is_empty() {
        brokers.push((config.node_id, config.rack.clone()));
    }
    brokers.sort_by_key(|(node_id, _)| node_id.0);

    let required = config.diskless_wal_local_replica_count;
    assignments
        .iter()
        .enumerate()
        .find_map(|(offset, assignment)| {
            let leader = *assignment.first()?;
            let available = crate::wal::quorum::placement::select_voters_from_sorted_racks(
                &brokers, leader, required,
            )
            .len();
            (available != required).then(|| {
                let partition =
                    first_partition.saturating_add(i32::try_from(offset).unwrap_or(i32::MAX));
                format!(
                    "diskless WAL partition {partition} leader {} has {available} eligible \
                     rack-distinct voters, but {required} are required; configure `broker.rack` \
                     on every voter and provide at least {required} distinct racks",
                    leader.0
                )
            })
        })
}

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
        return encode_response(&cluster_denied_response(&req), version);
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
    // KIP-108: a validate-only request runs every check and commits nothing,
    // so the policy below sees it exactly as it sees a committing one.
    let validate_only = req.validate_only;

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

        // `validate_topic_config_map` sees the key/value pairs alone and
        // cannot see the broker's own configuration. A diskless topic needs
        // one thing from it: an object-store backend. Without
        // `remote_storage_backend` there is no `DisklessReadHandle`, so the
        // broker starts neither the WAL index projection nor the object
        // flusher. The topic would still accept writes through its WAL
        // quorum, but nothing would ever move them to the object tier and
        // nothing would ever trim the local logs, so the durability model the
        // flag advertises would not exist and local storage would grow without
        // bound. Refuse at creation rather than accept a topic that cannot
        // work.
        //
        // This reads *this* broker's configuration, and the topic is
        // cluster-wide, so it is a guard against the common
        // one-configuration-fleet mistake rather than a cluster-wide
        // guarantee: a fleet where only some brokers carry a backend can still
        // create a topic that some of them cannot serve. Catching the default
        // configuration is the case worth having.
        let diskless = config_keys::resolve_diskless(Some(&config_overrides));
        if diskless && broker.config.remote_storage_backend.is_none() {
            results.push(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(format!(
                    "{}=true requires an object-store tier, but this broker has no \
                     `remote_storage_backend` configured; the diskless WAL could never flush \
                     or trim",
                    config_keys::DISKLESS
                )),
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
        let brokers = site_broker_views(&image, broker.config.is_broker().then_some(node_id));

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

        if diskless
            && let Some(reason) =
                diskless_wal_placement_error(&image, &broker.config, 0, &assignments)
        {
            results.push(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(reason),
            ));
            continue;
        }

        // The committing path learns of a name collision from
        // `submit_change`, which decides it inside the quorum and is the
        // race-safe answer. A dry run never gets there, so ask the image
        // directly: Kafka's `validateOnly` reports `TopicExistsException` for
        // an existing name, and it reports it ahead of the topic policy.
        if validate_only && image.topic(&name).is_some() {
            results.push(topic_error_result(name, codes::TOPIC_ALREADY_EXISTS, None));
            continue;
        }

        // KIP-108: the operator-declared topic policy, on the effective
        // partition count and replication factor the placement resolved and
        // on the topic's own config overrides. Kafka calls
        // `CreateTopicPolicy.validate` here too: after config validation, and
        // before the records are generated.
        if let Err(reason) = crate::topic_policy::check(
            &broker.config.topic_policy,
            &name,
            Some(assignments.len()),
            assignments.first().map(Vec::len),
            &config_overrides,
        ) {
            results.push(topic_error_result(
                name,
                codes::POLICY_VIOLATION,
                Some(reason),
            ));
            continue;
        }

        let topic_id = Uuid::new_v4();

        // A validate-only request has now passed every check the committing
        // path runs, and commits nothing.
        let error_code = if validate_only {
            codes::NONE
        } else {
            // Build the batch: one TopicRecord + N PartitionRecords.
            let records = topic_records(&topic_req, topic_id, &assignments, &config_overrides);

            match controller.submit_change(records).await {
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
                            partition_writer_queue_depth: broker
                                .config
                                .partition_writer_queue_depth,
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
                Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => {
                    codes::NOT_CONTROLLER
                }
                Err(e) => {
                    tracing::error!(topic = %name, error = %e, "CreateTopics submit_change failed");
                    codes::UNKNOWN_SERVER_ERROR
                }
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
            disclose_created_topic(
                broker,
                ctx,
                &image,
                version,
                &CreatedTopic {
                    controller: &controller,
                    assignments: &assignments,
                    overrides: &config_overrides,
                },
                &mut result,
            );
        }
        results.push(result);
    }

    finish_response(broker, ctx, results, validate_only, quota.delay(), version)
}

/// Fill in what KIP-525 discloses about a topic the create just made: its
/// partition count, its replication factor and, on v5+, its whole effective
/// configuration.
///
/// Split out of [`handle`] because the disclosure is one decision with two
/// outcomes -- told or withheld -- and reads none of the create's own state
/// beyond the row it fills.
/// What the create decided about one topic, as the KIP-525 disclosure reads
/// it: where to resolve the effective configuration from, the replica
/// assignment the row's counts come from, and the override map the request
/// carried.
struct CreatedTopic<'a> {
    controller: &'a std::sync::Arc<dyn crate::metadata_source::MetadataSource>,
    assignments: &'a [Vec<krabka_raft::NodeId>],
    overrides: &'a std::collections::BTreeMap<String, String>,
}

fn disclose_created_topic(
    broker: &Broker,
    ctx: &crate::handlers::RequestContext<'_>,
    image: &krabka_metadata::MetadataImage,
    version: i16,
    created: &CreatedTopic<'_>,
    result: &mut CreatableTopicResult,
) {
    // KIP-525 (v5+): the row carries what the topic was created as, so
    // a client needs no follow-up DescribeConfigs. Kafka gates the
    // whole disclosure -- the effective configs, the partition count
    // and the replication factor -- on `DescribeConfigs` on
    // `Topic(name)`, and stamps `topicConfigErrorCode` when the
    // principal may not be told. The create itself already happened
    // either way. Below v5 none of those fields are on the wire, so
    // the check is not worth an authorizer call.
    let describable = version < 5 || !describe_configs_denied(broker, image, ctx, &result.name);
    if describable {
        result.num_partitions = i32::try_from(created.assignments.len()).unwrap_or(i32::MAX);
        result.replication_factor = created
            .assignments
            .first()
            .and_then(|replicas| i16::try_from(replicas.len()).ok())
            .unwrap_or(-1);
        if version >= 5 {
            // The overrides the create wrote -- or, on a `validate_only`
            // row, would have written -- resolved against the current
            // image. This is Kafka's
            // `computeEffectiveTopicConfigs(creationConfigs)`: it builds
            // the row from the request's own map, and `validateOnly`
            // discards the records alone.
            result.configs = Some(effective_topic_configs(
                &created.controller.current_image(),
                &result.name,
                created.overrides,
            ));
        }
    } else {
        // Kafka leaves the partition count and the replication factor
        // at -1 here too: `AdminClient` fails every accessor on the
        // create result once `topicConfigErrorCode` is set, so a value
        // in either field would never be read.
        result.configs = Some(Vec::new());
        result.topic_config_error_code = codes::TOPIC_AUTHORIZATION_FAILED;
    }
}

/// Every topic row answered `CLUSTER_AUTHORIZATION_FAILED`, which is what a
/// request that fails the whole-request `Cluster` `Create` gate gets: the
/// handler learns nothing else about the topics, so no row can carry a
/// different verdict.
fn cluster_denied_response(req: &CreateTopicsRequest) -> CreateTopicsResponse {
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
    create_topics_response(results, 0)
}
