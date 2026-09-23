//! Creation of the internal topics that a streams topology needs, through the
//! `CreateTopics` path that serves a client.
//!
//! Kafka's coordinator returns the topics to create with the heartbeat
//! response, and `KafkaApis` checks `Create` on the whole set before it hands
//! them to `AutoTopicCreationManager.createStreamsInternalTopics`: `Create`
//! on the `Cluster` once, else `Create` on each topic. A caller that fails
//! both gets none of the topics created and a `MISSING_INTERNAL_TOPICS`
//! status detail naming them, so a principal with only group `Read` can never
//! make the broker create a topic on its behalf. An authorized creation sends
//! a `CreateTopics` request to the controller with the principal of the
//! caller, which validates the configs, applies the topic policy and places
//! the replicas. A failure goes into an error cache for twice the heartbeat
//! interval, and the `MISSING_INTERNAL_TOPICS` status of the next heartbeats
//! names it.

use std::collections::BTreeMap;

use dashmap::DashMap;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        common::streams_group_heartbeat_response::status::Status,
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
};

use crate::{
    broker::Broker,
    codes,
    coordinator::unified::streams::topology::{InternalTopicSpec, status as topo_status},
    error::BrokerError,
    handlers::{acl_denied, acl_wire::CLUSTER_RESOURCE_NAME},
};

/// How many failures the `MISSING_INTERNAL_TOPICS` status names, as Kafka's
/// `KafkaApis` does.
const MAX_ERRORS_TO_INCLUDE: usize = 3;

/// `DefaultAutoTopicCreationManager.DEFAULT_TOPIC_ERROR_CACHE_CAPACITY`.
const ERROR_CACHE_CAPACITY: usize = 1_000;

/// The per-broker state of Kafka's `DefaultAutoTopicCreationManager` for the
/// streams internal topics: the topics whose creation is in flight, and the
/// failures that the next heartbeats report.
#[derive(Debug, Default)]
pub(crate) struct StreamsInternalTopics {
    /// Topic name -> the request that is creating it right now.
    in_flight: DashMap<String, ()>,
    /// Topic name -> the error of the last failed creation and the instant it
    /// expires, in milliseconds.
    errors: DashMap<String, (String, i64)>,
}

impl StreamsInternalTopics {
    /// The topics of `specs` that no other request is creating and that no
    /// unexpired error holds back.
    fn take_creatable(&self, specs: &[InternalTopicSpec], now_ms: i64) -> Vec<InternalTopicSpec> {
        specs
            .iter()
            .filter(|spec| {
                let backing_off = self
                    .errors
                    .get(&spec.name)
                    .is_some_and(|entry| now_ms < entry.value().1);
                !backing_off && self.in_flight.insert(spec.name.clone(), ()).is_none()
            })
            .cloned()
            .collect()
    }

    /// Records the outcome of one creation. Kafka's `ExpiringErrorCache.put`
    /// caches the failure with its time to live, drops the entries that have
    /// expired, and keeps the cache at its capacity.
    fn finish(&self, name: &str, error: Option<String>, now_ms: i64, ttl_ms: i64) {
        self.in_flight.remove(name);
        let Some(error) = error else {
            self.errors.remove(name);
            return;
        };
        self.errors
            .insert(name.to_string(), (error, now_ms.saturating_add(ttl_ms)));
        self.errors
            .retain(|_, (_, expires_at_ms)| now_ms < *expires_at_ms);
        while self.errors.len() > ERROR_CACHE_CAPACITY {
            let Some(earliest) = self
                .errors
                .iter()
                .min_by_key(|entry| entry.value().1)
                .map(|entry| entry.key().clone())
            else {
                break;
            };
            self.errors.remove(&earliest);
        }
    }

    /// The unexpired errors of `names`, in name order.
    fn errors_of(&self, names: &[String], now_ms: i64) -> Vec<(String, String)> {
        let mut errors: Vec<(String, String)> = names
            .iter()
            .filter_map(|name| {
                self.errors
                    .get(name)
                    .filter(|entry| now_ms < entry.value().1)
                    .map(|entry| (name.clone(), entry.value().0.clone()))
            })
            .collect();
        errors.sort();
        errors
    }
}

/// Creates the internal topics of `specs` and adds the failures to the
/// `MISSING_INTERNAL_TOPICS` status of `response`.
///
/// # Errors
///
/// Returns an error when the `CreateTopics` request cannot be encoded or
/// decoded.
pub(super) async fn create_internal_topics(
    broker: &Broker,
    ctx: &crate::handlers::RequestContext<'_>,
    response: &mut StreamsGroupHeartbeatResponse,
    specs: &[InternalTopicSpec],
) -> Result<(), BrokerError> {
    // Kafka checks `Create` on the whole `topicsToCreate` set before any of
    // them is created: `Create` on the `Cluster` once, else `Create` on each
    // topic name. A topic that fails the per-topic fallback holds back every
    // topic in this heartbeat, not only itself -- the next heartbeat's
    // `MISSING_INTERNAL_TOPICS` status tries the whole set again.
    let unauthorized = create_unauthorized(broker, ctx, specs);
    if !unauthorized.is_empty() {
        let detail = format!(
            "Unauthorized to CREATE on topics {}.",
            unauthorized.join(", ")
        );
        append_missing_internal_topics_detail(response, &detail);
        return Ok(());
    }

    let creator = &broker.streams_internal_topics;
    let now_ms = crate::time_util::now_ms();
    // Kafka caches a failure for twice the heartbeat interval of the group,
    // which the response carries.
    let ttl_ms = 2 * i64::from(response.heartbeat_interval_ms.max(0));
    let creatable = creator.take_creatable(specs, now_ms);
    if !creatable.is_empty() {
        let results = create_topics(broker, ctx, &creatable).await;
        // Kafka caches the failures when the creation answers, so the time
        // the controller took does not eat into the back-off.
        let cached_at_ms = crate::time_util::now_ms();
        for spec in &creatable {
            creator.finish(
                &spec.name,
                results.get(&spec.name).cloned(),
                cached_at_ms,
                ttl_ms,
            );
        }
    }

    let names: Vec<String> = specs.iter().map(|spec| spec.name.clone()).collect();
    let errors = creator.errors_of(&names, crate::time_util::now_ms());
    if errors.is_empty() {
        return Ok(());
    }
    let shown = errors
        .iter()
        .take(MAX_ERRORS_TO_INCLUDE)
        .map(|(topic, error)| format!("{topic} ({error})"))
        .collect::<Vec<_>>()
        .join(", ");
    let detail = if errors.len() > MAX_ERRORS_TO_INCLUDE {
        format!("{shown} and {} more", errors.len() - MAX_ERRORS_TO_INCLUDE)
    } else {
        shown
    };
    for status in response.status.iter_mut().flatten() {
        if status.status_code == topo_status::MISSING_INTERNAL_TOPICS {
            status.status_detail = format!("{}; Creation failed: {detail}.", status.status_detail);
        }
    }
    Ok(())
}

/// Kafka's `CREATE` gate before `createStreamsInternalTopics`: `Create` on
/// the `Cluster` once, else `Create` on each topic name of `specs`. Returns
/// the names creation is not authorized for, in name order -- empty when the
/// whole request may create every one of `specs`, whether because the
/// `Cluster` grant covers them or because each one has its own.
fn create_unauthorized(
    broker: &Broker,
    ctx: &crate::handlers::RequestContext<'_>,
    specs: &[InternalTopicSpec],
) -> Vec<String> {
    let image = broker.controller.current_image();
    let cluster_create_denied = acl_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        ResourceType::Cluster,
        CLUSTER_RESOURCE_NAME,
        AclOperation::Create,
    );
    if !cluster_create_denied {
        return Vec::new();
    }
    let mut unauthorized: Vec<String> = specs
        .iter()
        .filter(|spec| {
            acl_denied(
                broker.config.authorizer.as_ref(),
                &image,
                ctx,
                ResourceType::Topic,
                &spec.name,
                AclOperation::Create,
            )
        })
        .map(|spec| spec.name.clone())
        .collect();
    unauthorized.sort_unstable();
    unauthorized
}

/// Kafka's `MISSING_INTERNAL_TOPICS` status append: joins `detail` onto the
/// existing status with `"; "`, or adds a new status holding only `detail`
/// when the response carries none yet.
fn append_missing_internal_topics_detail(
    response: &mut StreamsGroupHeartbeatResponse,
    detail: &str,
) {
    let statuses = response.status.get_or_insert_with(Vec::new);
    if let Some(status) = statuses
        .iter_mut()
        .find(|status| status.status_code == topo_status::MISSING_INTERNAL_TOPICS)
    {
        status.status_detail = format!("{}; {detail}", status.status_detail);
    } else {
        statuses.push(Status {
            status_code: topo_status::MISSING_INTERNAL_TOPICS,
            status_detail: detail.to_string(),
            ..Default::default()
        });
    }
}

/// Sends one `CreateTopics` request for `specs` with the principal of the
/// caller, and returns the error message of each topic that it did not
/// create.
async fn create_topics(
    broker: &Broker,
    ctx: &crate::handlers::RequestContext<'_>,
    specs: &[InternalTopicSpec],
) -> BTreeMap<String, String> {
    let version = krabka_protocol::owned::create_topics_request::MAX_VERSION;
    let request = CreateTopicsRequest {
        topics: specs
            .iter()
            .map(|spec| CreatableTopic {
                name: spec.name.clone(),
                num_partitions: spec.partitions,
                // Kafka's `toCreatableTopic` sends the replication factor of
                // the topology, and the default of the broker when it has
                // none.
                replication_factor: if spec.replication_factor > 0 {
                    spec.replication_factor
                } else {
                    broker
                        .config
                        .streams_group
                        .internal_topic_replication_factor
                },
                configs: spec
                    .configs
                    .iter()
                    .map(|(name, value)| CreatableTopicConfig {
                        name: name.clone(),
                        value: Some(value.clone()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let failures = |message: &str| {
        specs
            .iter()
            .map(|spec| (spec.name.clone(), message.to_string()))
            .collect::<BTreeMap<_, _>>()
    };
    let Ok(bytes) = crate::handlers::encode_response(&request, version) else {
        return failures("the request could not be encoded");
    };
    let response =
        match crate::handlers::create_topics::handle(broker, version, 0, &bytes, ctx).await {
            Ok(bytes) => bytes,
            Err(error) => return failures(&error.to_string()),
        };
    let mut cursor: &[u8] = &response;
    let Ok(response) = CreateTopicsResponse::decode(&mut cursor, version) else {
        return failures("the response could not be decoded");
    };
    response
        .topics
        .into_iter()
        .filter(|topic| {
            topic.error_code != codes::NONE && topic.error_code != codes::TOPIC_ALREADY_EXISTS
        })
        .map(|topic| {
            let message = topic
                .error_message
                .unwrap_or_else(|| format!("error code {}", topic.error_code));
            (topic.name, message)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn spec(name: &str) -> InternalTopicSpec {
        InternalTopicSpec {
            name: name.into(),
            partitions: 1,
            replication_factor: 0,
            configs: BTreeMap::new(),
        }
    }

    /// Kafka's `DefaultAutoTopicCreationManager`: a topic whose creation is in
    /// flight or whose last failure has not expired is not created again, and
    /// the failure of a creation holds for its time to live.
    #[test]
    fn the_cache_holds_a_failure_for_its_time_to_live() {
        let creator = StreamsInternalTopics::default();
        let specs = [spec("a"), spec("b")];

        // Both go out, and a second request finds them in flight.
        check!(creator.take_creatable(&specs, 0) == specs.to_vec());
        check!(creator.take_creatable(&specs, 0) == vec![]);

        // `a` failed and `b` was created.
        creator.finish("a", Some("no brokers".into()), 100, 1_000);
        creator.finish("b", None, 100, 1_000);
        check!(
            creator.errors_of(&["a".into(), "b".into()], 200)
                == vec![("a".into(), "no brokers".into())]
        );
        check!(creator.take_creatable(&specs, 200) == vec![spec("b")]);
        creator.finish("b", None, 200, 1_000);

        // The failure of `a` expires, so the next heartbeat creates it again
        // and no longer reports it.
        check!(creator.errors_of(&["a".into()], 1_101) == vec![]);
        check!(creator.take_creatable(&specs, 1_101) == specs.to_vec());
    }

    /// The cache keeps at most `ERROR_CACHE_CAPACITY` failures, as Kafka's
    /// `ExpiringErrorCache` does, and drops the ones that expired.
    #[test]
    fn the_cache_is_bounded() {
        let creator = StreamsInternalTopics::default();
        for index in 0..=ERROR_CACHE_CAPACITY {
            creator.finish(&format!("t{index}"), Some("no brokers".into()), 100, 1_000);
        }
        check!(creator.errors.len() == ERROR_CACHE_CAPACITY);

        creator.finish("late", Some("no brokers".into()), 2_000, 1_000);
        check!(creator.errors.len() == 1);
    }
}
