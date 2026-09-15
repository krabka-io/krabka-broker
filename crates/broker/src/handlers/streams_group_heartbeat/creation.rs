//! Creation of the internal topics that a streams topology needs, through the
//! `CreateTopics` path that serves a client.
//!
//! Kafka's coordinator returns the topics to create with the heartbeat
//! response, and `KafkaApis` hands them to
//! `AutoTopicCreationManager.createStreamsInternalTopics`, which sends a
//! `CreateTopics` request to the controller with the principal of the caller.
//! The controller then validates the configs, applies the topic policy and
//! places the replicas. A failure goes into an error cache for twice the
//! heartbeat interval, and the `MISSING_INTERNAL_TOPICS` status of the next
//! heartbeats names it.

use std::collections::BTreeMap;

use dashmap::DashMap;
use krabka_protocol::{
    Decode,
    owned::{
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
};

/// How many failures the `MISSING_INTERNAL_TOPICS` status names, as Kafka's
/// `KafkaApis` does.
const MAX_ERRORS_TO_INCLUDE: usize = 3;

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

    fn finish(&self, name: &str, error: Option<String>, expires_at_ms: i64) {
        self.in_flight.remove(name);
        match error {
            Some(error) => {
                self.errors.insert(name.to_string(), (error, expires_at_ms));
            }
            None => {
                self.errors.remove(name);
            }
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
    let creator = &broker.streams_internal_topics;
    let now_ms = crate::time_util::now_ms();
    // Kafka caches a failure for twice the heartbeat interval of the group.
    let expires_at_ms = now_ms.saturating_add(
        2 * i64::try_from(broker.config.streams_group.heartbeat_interval.as_millis())
            .unwrap_or(i64::MAX),
    );
    let creatable = creator.take_creatable(specs, now_ms);
    if !creatable.is_empty() {
        let results = create_topics(broker, ctx, &creatable).await;
        for spec in &creatable {
            creator.finish(&spec.name, results.get(&spec.name).cloned(), expires_at_ms);
        }
    }

    let names: Vec<String> = specs.iter().map(|spec| spec.name.clone()).collect();
    let errors = creator.errors_of(&names, now_ms);
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
