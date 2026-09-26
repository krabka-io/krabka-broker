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
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreatableTopicResult,
    },
    primitives::uuid::Uuid as ProtoUuid,
};
use krabka_raft::RaftError;
use uuid::Uuid;

mod authorization;
mod materialize;
mod name;
mod placement;
mod records;
mod response;

#[cfg(test)]
mod tests;

pub(crate) use self::placement::{
    InitialLeadership, automatic_leaderships, manual_leaderships, placement_failure_message,
    round_robin_replicas, site_broker_views, validate_manual_partition_assignment,
};
use self::{
    authorization::{authorize_create_topics, describe_configs_denied},
    materialize::{TopicMaterialization, materialize_topic},
    name::{CLUSTER_METADATA_TOPIC, topic_name_error},
    placement::resolve_assignments,
    records::{null_config_error, topic_config_overrides, topic_records},
    response::{
        create_topics_response, effective_topic_configs, encode_response, finish_response,
        topic_error_result,
    },
};
use crate::{
    authorizer::AuthorizationResult,
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
    leaderships: &[InitialLeadership],
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
    leaderships
        .iter()
        .enumerate()
        .find_map(|(offset, leadership)| {
            let leader = leadership.leader;
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
#[allow(clippy::too_many_lines)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    // ── ACL preamble ────────────────────────────────────────
    let mut cursor = req_bytes;
    let req = CreateTopicsRequest::decode(&mut cursor, version)?;
    let image = broker.controller.current_image();

    // Kafka removes duplicate names from the request before authorizing
    // (`ControllerApis.createTopics`): every row that shares a duplicated
    // name leaves the request, and the name answers one INVALID_REQUEST row
    // after the controller's rows.
    let duplicate_names = duplicate_names(&req.topics);

    // Cluster `Create` is a shortcut; on Deny, `authorize_create_topics`
    // falls back to topic-level `Create` per surviving name, so a principal
    // with only a topic-scoped ACL can still create the topics it covers.
    // Duplicate and protected names are excluded here exactly as Kafka
    // excludes them from `allowedTopicNames` before authorizing.
    let candidate_names: Vec<&str> = req
        .topics
        .iter()
        .map(|topic| topic.name.as_str())
        .filter(|name| {
            !duplicate_names.iter().any(|duplicate| duplicate == *name)
                && *name != CLUSTER_METADATA_TOPIC
        })
        .collect();
    let denied_names: std::collections::HashSet<String> =
        authorize_create_topics(broker, &image, ctx, candidate_names.iter().copied())
            .into_iter()
            .filter(|(_, result)| *result == AuthorizationResult::Deny)
            .map(|(name, _)| name.to_owned())
            .collect();

    // Kafka appends the rows the request answers without the controller
    // after the controller's own rows: one per duplicated name, then the
    // protected raft metadata topic and the topics denied `Create`. The
    // controller sees the rest, in request order.
    let mut trailing: Vec<CreatableTopicResult> = duplicate_names
        .iter()
        .map(|name| {
            topic_error_result(
                name.clone(),
                codes::INVALID_REQUEST,
                Some("Duplicate topic name.".into()),
            )
        })
        .collect();
    let mut effective: Vec<CreatableTopic> = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        if duplicate_names.contains(&topic.name) {
            continue;
        }
        if topic.name == CLUSTER_METADATA_TOPIC {
            trailing.push(topic_error_result(
                topic.name.clone(),
                codes::INVALID_REQUEST,
                Some(format!(
                    "Creation of internal topic {CLUSTER_METADATA_TOPIC} is prohibited."
                )),
            ));
        } else if denied_names.contains(&topic.name) {
            // Kafka answers a denial TOPIC_AUTHORIZATION_FAILED with this
            // exact message, never CLUSTER_AUTHORIZATION_FAILED: the cluster
            // check is only a shortcut past the per-topic lookup.
            trailing.push(topic_error_result(
                topic.name.clone(),
                codes::TOPIC_AUTHORIZATION_FAILED,
                Some("Authorization failed.".into()),
            ));
        } else {
            effective.push(topic.clone());
        }
    }

    // Kafka's `validateTotalNumberOfPartitions` refuses the whole request
    // when the topics the controller sees add up to more partitions than one
    // metadata batch may carry. The failure answers every requested row, as
    // `CreateTopicsRequest.getErrorResponse` does, and creates nothing.
    if total_partitions(&effective, broker.config.num_partitions) > MAX_PARTITIONS_PER_REQUEST {
        let results = req
            .topics
            .iter()
            .map(|topic| {
                topic_error_result(
                    topic.name.clone(),
                    codes::POLICY_VIOLATION,
                    Some(TOO_MANY_PARTITIONS.into()),
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

    // KIP-599: Kafka's controller charges each topic with the partitions it
    // creates, after every other check on it has passed
    // (`ReplicationControlManager.createTopic`). A strict version (v6+)
    // refuses the topic that finds the bucket negative, and every topic
    // after it.
    let mut quota = crate::quota::ControllerMutationQuota::new(&crate::quota::QuotaRequest {
        image: &image,
        buckets: &broker.quota_buckets,
        principal: &ctx.principal.name,
        client_id: ctx.client_id,
        window: broker.config.controller_mutation_quota_window,
        strict: version >= 6,
    });

    let mut results: Vec<CreatableTopicResult> = Vec::with_capacity(req.topics.len());
    let preferred_site = resolve_preferred_leader_site(&image);
    // KIP-108: a validate-only request runs every check and commits nothing,
    // so the policy below sees it exactly as it sees a committing one.
    let validate_only = req.validate_only;

    for mut topic_req in effective {
        let name = topic_req.name.clone();

        // Kafka checks the name before anything else. The name becomes part
        // of the partition directory path, so no later step may see a name
        // that this check refuses.
        if let Some((code, message)) = topic_name_error(&image, &name) {
            results.push(topic_error_result(name, code, Some(message)));
            continue;
        }

        // Kafka answers an existing topic TOPIC_ALREADY_EXISTS before it
        // looks at the configs, the counts, the placement or the policy, so
        // `kafka-topics --create --if-not-exists` succeeds whatever else the
        // request carries.
        if image.topic(&name).is_some() {
            results.push(topic_exists_result(name));
            continue;
        }

        // Kafka's `computeConfigChanges` refuses a config with a null value
        // before it validates the others.
        if let Some(message) = null_config_error(&topic_req) {
            results.push(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(message),
            ));
            continue;
        }

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

        // Kafka's `ReplicationControlManager.createTopic` checks the
        // replication factor first and the partition count second. Each may
        // be -1, which KIP-464 resolves to the broker's `num.partitions` or
        // `default.replication.factor`. A manual assignment requires -1 for
        // both, and `resolve_assignments` checks that.
        if topic_req.assignments.is_empty() {
            if let Some((code, message)) = invalid_topic_shape(&topic_req) {
                results.push(topic_error_result(name, code, Some(message.to_owned())));
                continue;
            }
            topic_req.num_partitions =
                resolve_default(topic_req.num_partitions, broker.config.num_partitions);
            topic_req.replication_factor = resolve_default(
                topic_req.replication_factor,
                broker.config.default_replication_factor,
            );
        }

        // Read the current broker set from the controller's image, with the
        // site and the witness role of each broker. `site_broker_views` sorts
        // by node id for determinism, and it covers the race in which the
        // self-registration record has not reached the local image yet.
        // The automatic placement never picks an unavailable broker. A manual
        // assignment may name one, because Kafka checks only that the broker
        // is registered, and the ISR below leaves it out.
        let unavailable = super::offline_replicas::unavailable_brokers(broker, &image).await;
        let manual = !topic_req.assignments.is_empty();
        let no_exclusion = std::collections::HashSet::new();
        let brokers = site_broker_views(
            &image,
            broker.config.is_broker().then_some(node_id),
            if manual { &no_exclusion } else { &unavailable },
        );

        let assignments = match resolve_assignments(&topic_req, &brokers, preferred_site) {
            Ok(assignments) => assignments,
            Err((code, message)) => {
                results.push(topic_error_result(name, code, Some(message)));
                continue;
            }
        };

        if assignments.is_empty() {
            // The placement cannot satisfy the request. RF above the broker
            // count is the common cause. Surface INVALID_REPLICATION_FACTOR
            // with the message of Kafka's replica placer.
            results.push(topic_error_result(
                name,
                codes::INVALID_REPLICATION_FACTOR,
                Some(placement_failure_message(
                    topic_req.replication_factor,
                    brokers.len(),
                )),
            ));
            continue;
        }

        let leaderships = if manual {
            match manual_leaderships(
                &assignments,
                &unavailable,
                &config_keys::witness_node_ids(&image),
                0,
            ) {
                Ok(leaderships) => leaderships,
                Err(message) => {
                    results.push(topic_error_result(
                        name,
                        codes::INVALID_REPLICA_ASSIGNMENT,
                        Some(message),
                    ));
                    continue;
                }
            }
        } else {
            automatic_leaderships(&assignments)
        };

        if diskless
            && let Some(reason) =
                diskless_wal_placement_error(&image, &broker.config, 0, &leaderships)
        {
            results.push(topic_error_result(
                name,
                codes::INVALID_CONFIG,
                Some(reason),
            ));
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

        // KIP-599: charge the partitions this topic creates.
        let partition_count = u64::try_from(assignments.len()).unwrap_or(u64::MAX);
        if quota.record(partition_count).is_err() {
            results.push(topic_error_result(
                name,
                codes::THROTTLING_QUOTA_EXCEEDED,
                Some(THROTTLING_QUOTA_EXCEEDED_MESSAGE.into()),
            ));
            continue;
        }

        let topic_id = Uuid::new_v4();

        // A validate-only request has now passed every check the committing
        // path runs, and commits nothing.
        let failure = if validate_only {
            None
        } else {
            // Build the batch: one TopicRecord + N PartitionRecords.
            let records = topic_records(
                &topic_req,
                topic_id,
                &assignments,
                &leaderships,
                &config_overrides,
            );

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
                        &leaderships,
                    )
                    .await;
                    None
                }
                // Another request created the name after this one read the
                // image. The quorum decides that race, and the row is the one
                // Kafka's existence check answers.
                Err(RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))) => {
                    Some(topic_exists_result(name.clone()))
                }
                Err(RaftError::Metadata(krabka_metadata::MetadataError::InvalidRecord(_))) => {
                    // E.g., `partitions <= 0` rejected by image::validate.
                    Some(topic_error_result(
                        name.clone(),
                        codes::INVALID_PARTITIONS,
                        None,
                    ))
                }
                Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown) => Some(
                    topic_error_result(name.clone(), codes::NOT_CONTROLLER, None),
                ),
                Err(e) => {
                    tracing::error!(topic = %name, error = %e, "CreateTopics submit_change failed");
                    Some(topic_error_result(
                        name.clone(),
                        codes::UNKNOWN_SERVER_ERROR,
                        None,
                    ))
                }
            }
        };
        if let Some(failure) = failure {
            results.push(failure);
            continue;
        }

        let mut result = CreatableTopicResult {
            name,
            topic_id: ProtoUuid(topic_id.into_bytes()),
            error_code: codes::NONE,
            ..Default::default()
        };
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
        results.push(result);
    }
    results.extend(trailing);

    finish_response(broker, ctx, results, validate_only, quota.delay(), version)
}

/// Kafka's `maxRecordsPerBatch` (`controller.max.records.per.batch`, default
/// 10000): the most partitions one `CreateTopics` request may create.
const MAX_PARTITIONS_PER_REQUEST: u64 = 10_000;

/// The message of Kafka's `validateTotalNumberOfPartitions` refusal.
const TOO_MANY_PARTITIONS: &str = "Too many partitions in request.";

/// Kafka's `Errors.THROTTLING_QUOTA_EXCEEDED.message()`, which
/// `StrictControllerMutationQuota.record` puts on the exception.
const THROTTLING_QUOTA_EXCEEDED_MESSAGE: &str = "The throttling quota has been exceeded.";

/// The names that more than one request row carries, in the order of their
/// first row.
fn duplicate_names(topics: &[CreatableTopic]) -> Vec<String> {
    let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for topic in topics {
        *counts.entry(topic.name.as_str()).or_insert(0) += 1;
    }
    let mut seen = std::collections::HashSet::new();
    topics
        .iter()
        .map(|topic| topic.name.as_str())
        .filter(|name| counts[name] > 1 && seen.insert(*name))
        .map(str::to_owned)
        .collect()
}

/// The partitions a request asks for, as Kafka's
/// `validateTotalNumberOfPartitions` counts them: an assignment list counts
/// its length, -1 counts `num.partitions`, and a count that is not positive
/// counts nothing.
fn total_partitions(topics: &[CreatableTopic], default_num_partitions: i32) -> u64 {
    topics
        .iter()
        .map(|topic| {
            let count = if !topic.assignments.is_empty() {
                i64::try_from(topic.assignments.len()).unwrap_or(i64::MAX)
            } else if topic.num_partitions == -1 {
                i64::from(default_num_partitions)
            } else {
                i64::from(topic.num_partitions.max(0))
            };
            u64::try_from(count).unwrap_or(0)
        })
        .fold(0, u64::saturating_add)
}

/// Kafka's row for a topic that already exists: zero topic id, and the
/// message `ReplicationControlManager.createTopics` gives it.
fn topic_exists_result(name: String) -> CreatableTopicResult {
    let message = format!("Topic '{name}' already exists.");
    topic_error_result(name, codes::TOPIC_ALREADY_EXISTS, Some(message))
}

/// Kafka's refusal of a requested topic shape without a manual assignment:
/// `INVALID_REPLICATION_FACTOR` for a replication factor of 0 or below -1,
/// else `INVALID_PARTITIONS` for a partition count of 0 or below -1. The
/// messages are Kafka's.
fn invalid_topic_shape(
    topic: &krabka_protocol::owned::create_topics_request::CreatableTopic,
) -> Option<(i16, &'static str)> {
    if topic.replication_factor < -1 || topic.replication_factor == 0 {
        Some((
            codes::INVALID_REPLICATION_FACTOR,
            "Replication factor must be larger than 0, or -1 to use the default value.",
        ))
    } else if topic.num_partitions < -1 || topic.num_partitions == 0 {
        Some((
            codes::INVALID_PARTITIONS,
            "Number of partitions was set to an invalid non-positive value.",
        ))
    } else {
        None
    }
}

/// KIP-464: a requested value of -1 means the broker default.
fn resolve_default<T: From<i8> + PartialEq>(requested: T, default: T) -> T {
    if requested == T::from(-1) {
        default
    } else {
        requested
    }
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
