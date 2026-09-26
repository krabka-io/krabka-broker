//! The rows `Metadata` answers for a requested topic that does not exist, and
//! the topic auto-creation behind them.
//!
//! Kafka's `KafkaApis.getTopicMetadata` answers each missing topic that the
//! principal may describe with a row that carries no partitions and the zero
//! id. A name that `Topic.validate` refuses gets `INVALID_TOPIC_EXCEPTION`
//! (17); any other name gets `UNKNOWN_TOPIC_OR_PARTITION` (3). When the
//! request allows auto-creation and `auto.create.topics.enable` is on,
//! `DefaultAutoTopicCreationManager.createTopics` first sends a
//! `CreateTopics` request for the valid names with the requester's principal,
//! and still answers each of them with 3: the topic is not described until a
//! later request finds it in the metadata image. It lists the invalid names
//! ahead of the created ones.

use krabka_log::topic_name::validate_topic_name;
use krabka_protocol::{
    Decode,
    owned::{
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        metadata_response::MetadataResponseTopic,
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{broker::Broker, codes, handlers::RequestContext};

/// The `CreateTopics` version the auto-creation request uses.
///
/// Version 5 takes the `-1` partition count and replication factor that ask
/// for the broker defaults (KIP-464), as Kafka's auto-creation request does.
/// It is also the last version whose `ControllerMutationQuota` is permissive,
/// which is the quota `KafkaApis.getTopicMetadata` hands auto-creation
/// (`newPermissiveQuotaFor`): the mutation is recorded but never refused.
const AUTO_CREATE_VERSION: i16 = 5;

/// The rows for `names`, the missing topics that the principal may describe
/// and, when `auto_create` is set, may create. With `auto_create` set this
/// also creates the valid names.
pub(super) async fn missing_topic_rows(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    names: &[&str],
    auto_create: bool,
) -> Vec<MetadataResponseTopic> {
    let row = |error_code, name: &str| MetadataResponseTopic {
        error_code,
        name: Some(name.to_owned()),
        topic_id: WireUuid::ZERO,
        is_internal: crate::internal_topics::is_internal_topic(&broker.config, name),
        ..Default::default()
    };
    let error_code = |name: &str| {
        if validate_topic_name(name).is_ok() {
            codes::UNKNOWN_TOPIC_OR_PARTITION
        } else {
            codes::INVALID_TOPIC_EXCEPTION
        }
    };
    if !auto_create {
        return names
            .iter()
            .map(|name| row(error_code(name), name))
            .collect();
    }
    let (creatable, invalid): (Vec<&str>, Vec<&str>) = names
        .iter()
        .partition(|name| validate_topic_name(name).is_ok());
    if !creatable.is_empty() {
        create_topics(broker, ctx, &creatable).await;
    }
    invalid
        .into_iter()
        .map(|name| row(codes::INVALID_TOPIC_EXCEPTION, name))
        .chain(
            creatable
                .into_iter()
                .map(|name| row(codes::UNKNOWN_TOPIC_OR_PARTITION, name)),
        )
        .collect()
}

/// Sends one `CreateTopics` request for `names` with the broker defaults and
/// the principal of `ctx`. A failure is logged, not answered: Kafka's
/// auto-creation logs the controller's errors and answers the `Metadata`
/// request the same way whatever they are.
async fn create_topics(broker: &Broker, ctx: &RequestContext<'_>, names: &[&str]) {
    let request = CreateTopicsRequest {
        topics: names
            .iter()
            .map(|name| CreatableTopic {
                name: (*name).to_owned(),
                num_partitions: -1,
                replication_factor: -1,
                ..Default::default()
            })
            .collect(),
        timeout_ms: 30_000,
        ..Default::default()
    };
    let Ok(bytes) = crate::handlers::encode_response(&request, AUTO_CREATE_VERSION) else {
        tracing::warn!(?names, "auto topic creation request could not be encoded");
        return;
    };
    // A context of its own, with the requester's identity, so the
    // controller-mutation charge `CreateTopics` defers onto its context never
    // throttles this `Metadata` response. Kafka throttles `Metadata` by the
    // request quota only.
    let create_ctx = RequestContext::new(
        ctx.principal,
        ctx.peer,
        ctx.client_id,
        ctx.connection_id,
        false,
        ctx.connection_listener_name,
    );
    let response = match crate::handlers::create_topics::handle(
        broker,
        AUTO_CREATE_VERSION,
        0,
        &bytes,
        &create_ctx,
    )
    .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!(?names, %error, "auto topic creation failed");
            return;
        }
    };
    let mut cursor: &[u8] = &response;
    match CreateTopicsResponse::decode(&mut cursor, AUTO_CREATE_VERSION) {
        Ok(response) => {
            for topic in response
                .topics
                .iter()
                .filter(|topic| topic.error_code != codes::NONE)
            {
                tracing::warn!(
                    topic = %topic.name,
                    error_code = topic.error_code,
                    error_message = ?topic.error_message,
                    "auto topic creation failed"
                );
            }
        }
        Err(error) => {
            tracing::warn!(?names, %error, "auto topic creation response could not be decoded");
        }
    }
}
