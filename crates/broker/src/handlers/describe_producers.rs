//! `DescribeProducers` (`api_key=61`, KIP-664) shows the producer state of a
//! set of partitions.
//!
//! This admin RPC returns the in-memory producer-state snapshot of the broker
//! for a set of `(topic, partition)` pairs. JVM `Admin.describeProducers` and
//! `kafka-transactions --describe-producers` use it to debug stuck idempotent
//! or transactional producers.
//!
//! ## ACL
//!
//! Kafka's `KafkaApis.checkValidTopic` validates the topic name with
//! `Topic.validate` before it authorizes anything: a malformed name (empty,
//! too long, or holding a character other than ASCII alphanumerics, `.`,
//! `_` and `-`) gives every requested partition `INVALID_TOPIC_EXCEPTION
//! (17)` with `Topic.validate`'s message, regardless of the principal's
//! ACLs. Only a name that passes that check goes on to the `Read` check on
//! `Topic(name)`, which mirrors `Fetch`, per KIP-664. On a Deny, every
//! partition of that topic carries `TOPIC_AUTHORIZATION_FAILED (29)`. An
//! unknown topic or an out-of-range partition gives a per-partition
//! `UNKNOWN_TOPIC_OR_PARTITION (3)`.
//!
//! ## Field semantics
//!
//! `producer_id`, `producer_epoch`, `last_sequence`, and `last_timestamp`
//! come from `crate::producer_state`. The partition log supplies the current
//! transaction start offset and the last coordinator epoch recovered from a
//! durable transaction marker. Their schema sentinel is `-1` when there is no
//! open transaction or no marker has established a coordinator epoch yet.

use bytes::Bytes;
use krabka_log::topic_name::validate_topic_name;
use krabka_metadata::AclOperation;
use krabka_protocol::{
    Decode,
    owned::{
        describe_producers_request::DescribeProducersRequest,
        describe_producers_response::{
            DescribeProducersResponse, PartitionResponse, ProducerState, TopicResponse,
        },
    },
};

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
};

#[tracing::instrument(
    name = "handle_describe_producers",
    level = "info",
    skip_all,
    fields(api = "DescribeProducers", version, req_bytes = req_bytes.len()),
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
    let req = DescribeProducersRequest::decode(&mut cur, version)?;

    let image = broker.controller.current_image();

    // Kafka's `checkValidTopic` runs `Topic.validate` before it authorizes
    // anything, so a malformed name never reaches the authorizer. Only the
    // names that pass go into the batch `Read` check below.
    let topic_decisions = authorize_topics(
        broker.config.authorizer.as_ref(),
        &*image,
        ctx.principal,
        ctx.peer,
        AclOperation::Read,
        req.topics
            .iter()
            .filter(|t| validate_topic_name(t.name.as_str()).is_ok())
            .map(|t| t.name.as_str()),
    );

    let mut topics_out: Vec<TopicResponse> = Vec::with_capacity(req.topics.len());
    for topic_req in &req.topics {
        let mut parts_out: Vec<PartitionResponse> =
            Vec::with_capacity(topic_req.partition_indexes.len());

        if let Err(invalid) = validate_topic_name(topic_req.name.as_str()) {
            // Kafka answers INVALID_TOPIC_EXCEPTION on every requested
            // partition of a malformed name, regardless of the principal's
            // ACLs, before it even looks the topic up.
            let message = invalid.to_string();
            for &idx in &topic_req.partition_indexes {
                parts_out.push(PartitionResponse {
                    partition_index: idx,
                    error_code: codes::INVALID_TOPIC_EXCEPTION,
                    error_message: Some(message.clone()),
                    active_producers: Vec::new(),
                    ..Default::default()
                });
            }
            topics_out.push(TopicResponse {
                name: topic_req.name.clone(),
                partitions: parts_out,
                ..Default::default()
            });
            continue;
        }

        let allow = topic_decisions
            .get(topic_req.name.as_str())
            .copied()
            .unwrap_or(AuthorizationResult::Deny);

        if allow == AuthorizationResult::Deny {
            // KIP-664: per-partition TOPIC_AUTHORIZATION_FAILED on every
            // requested partition of a denied topic.
            for &idx in &topic_req.partition_indexes {
                parts_out.push(PartitionResponse {
                    partition_index: idx,
                    error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                    error_message: Some("Topic authorization failed.".into()),
                    active_producers: Vec::new(),
                    ..Default::default()
                });
            }
            topics_out.push(TopicResponse {
                name: topic_req.name.clone(),
                partitions: parts_out,
                ..Default::default()
            });
            continue;
        }

        // Topic-existence + per-partition-bounds check. The image
        // exposes `partition(name, idx) -> Option<&PartitionRecord>`
        // which combines both checks in one lookup.
        for &idx in &topic_req.partition_indexes {
            if image.partition(topic_req.name.as_str(), idx).is_none() {
                parts_out.push(PartitionResponse {
                    partition_index: idx,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: Some("This server does not host this topic-partition.".into()),
                    active_producers: Vec::new(),
                    ..Default::default()
                });
                continue;
            }

            let partition_index = krabka_ids::PartitionIndex(idx);
            let Some(partition) = broker
                .partitions
                .get(topic_req.name.as_str(), partition_index)
            else {
                parts_out.push(PartitionResponse {
                    partition_index: idx,
                    error_code: codes::NOT_LEADER_OR_FOLLOWER,
                    error_message: None,
                    active_producers: Vec::new(),
                    ..Default::default()
                });
                continue;
            };

            let snapshot = broker
                .producer_state
                .snapshot(topic_req.name.as_str(), partition_index)
                .await;
            let log = partition.log.lock().map_err(|_| {
                BrokerError::Replication(format!(
                    "DescribeProducers: log lock poisoned for {}-{idx}",
                    topic_req.name
                ))
            })?;
            let active_producers: Vec<ProducerState> = snapshot
                .into_iter()
                .map(|(producer_id, entry)| {
                    let (coordinator_epoch, transaction_start) =
                        log.producer_transaction_state(krabka_log::ProducerId(producer_id));
                    ProducerState {
                        producer_id,
                        producer_epoch: i32::from(entry.epoch),
                        last_sequence: entry.last_sequence,
                        last_timestamp: entry.last_timestamp,
                        coordinator_epoch,
                        current_txn_start_offset: transaction_start.map_or(-1, |offset| offset.0),
                        ..Default::default()
                    }
                })
                .collect();

            parts_out.push(PartitionResponse {
                partition_index: idx,
                error_code: codes::NONE,
                error_message: None,
                active_producers,
                ..Default::default()
            });
        }

        topics_out.push(TopicResponse {
            name: topic_req.name.clone(),
            partitions: parts_out,
            ..Default::default()
        });
    }

    let resp = DescribeProducersResponse {
        throttle_time_ms: 0,
        topics: topics_out,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_protocol::owned::describe_producers_request::TopicRequest;
    use krabka_security::Principal;

    use super::*;
    use crate::{
        authorizer::AllowAllAuthorizer,
        broker::Broker,
        test_support::{DenyAll, peer, principal},
    };

    const VERSION: i16 = 0;

    crate::test_support::wire_helpers!(
        DescribeProducersRequest,
        DescribeProducersResponse,
        version = VERSION,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer_no_audit as start_broker;

    fn request(topic: &str, partition_indexes: &[i32]) -> DescribeProducersRequest {
        DescribeProducersRequest {
            topics: vec![TopicRequest {
                name: topic.into(),
                partition_indexes: partition_indexes.to_vec(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    async fn drive(
        broker: &Broker,
        req: &DescribeProducersRequest,
        principal: &Principal,
        peer: &SocketAddr,
    ) -> DescribeProducersResponse {
        let ctx = test_context(principal, peer);
        let req_bytes = encode_request(req);
        let bytes = handle(broker, VERSION, 123, &req_bytes, &ctx)
            .await
            .expect("handle");
        decode_response(&bytes)
    }

    /// Kafka's `checkValidTopic` answers `INVALID_TOPIC_EXCEPTION` (17) for
    /// a malformed topic name on every requested partition, before it ever
    /// consults the authorizer, and with `Topic.validate`'s message. A
    /// well-formed but missing topic still falls through to the existence
    /// check, and gets `UNKNOWN_TOPIC_OR_PARTITION` (3) with Kafka's message.
    /// This holds under both a deny-everything and an allow-everything
    /// authorizer, since an invalid name never reaches either.
    #[tokio::test]
    async fn handle_validates_topic_name_before_authorizing() {
        type Case<'a> = (
            &'a str,
            Arc<dyn crate::authorizer::Authorizer>,
            i16,
            Option<String>,
        );

        let too_long = "a".repeat(krabka_log::topic_name::MAX_TOPIC_NAME_LENGTH + 1);
        let cases: Vec<Case<'_>> = vec![
            (
                "",
                Arc::new(AllowAllAuthorizer),
                codes::INVALID_TOPIC_EXCEPTION,
                Some(krabka_log::topic_name::InvalidTopicName::Empty.to_string()),
            ),
            (
                "",
                Arc::new(DenyAll),
                codes::INVALID_TOPIC_EXCEPTION,
                Some(krabka_log::topic_name::InvalidTopicName::Empty.to_string()),
            ),
            (
                "a/b",
                Arc::new(AllowAllAuthorizer),
                codes::INVALID_TOPIC_EXCEPTION,
                Some(
                    krabka_log::topic_name::InvalidTopicName::IllegalCharacter("a/b".into())
                        .to_string(),
                ),
            ),
            (
                "a/b",
                Arc::new(DenyAll),
                codes::INVALID_TOPIC_EXCEPTION,
                Some(
                    krabka_log::topic_name::InvalidTopicName::IllegalCharacter("a/b".into())
                        .to_string(),
                ),
            ),
            (
                too_long.as_str(),
                Arc::new(AllowAllAuthorizer),
                codes::INVALID_TOPIC_EXCEPTION,
                Some(
                    krabka_log::topic_name::InvalidTopicName::TooLong(too_long.clone()).to_string(),
                ),
            ),
            (
                "missing-but-valid",
                Arc::new(AllowAllAuthorizer),
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                Some("This server does not host this topic-partition.".into()),
            ),
            (
                "missing-but-valid",
                Arc::new(DenyAll),
                codes::TOPIC_AUTHORIZATION_FAILED,
                Some("Topic authorization failed.".into()),
            ),
        ];

        for (name, authorizer, expected_code, expected_message) in cases {
            let (broker_handle, _dir) = start_broker(authorizer).await;
            let broker = broker_handle.broker_arc_for_test();
            let p = principal("alice");
            let peer = peer();
            let req = request(name, &[0]);

            let resp = drive(&broker, &req, &p, &peer).await;

            let expected = DescribeProducersResponse {
                throttle_time_ms: 0,
                topics: vec![TopicResponse {
                    name: name.to_owned(),
                    partitions: vec![PartitionResponse {
                        partition_index: 0,
                        error_code: expected_code,
                        error_message: expected_message.clone(),
                        active_producers: Vec::new(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            assert!(resp == expected, "topic {name:?}");
            broker_handle.shutdown().await;
        }
    }

    /// An invalid name gives `INVALID_TOPIC_EXCEPTION` on every requested
    /// partition, not only the first.
    #[tokio::test]
    async fn handle_invalid_topic_name_marks_every_requested_partition() {
        let (broker_handle, _dir) = start_broker(Arc::new(AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let p = principal("alice");
        let peer = peer();
        let req = request("a/b", &[0, 1, 4]);

        let resp = drive(&broker, &req, &p, &peer).await;

        let message =
            krabka_log::topic_name::InvalidTopicName::IllegalCharacter("a/b".into()).to_string();
        let expected = DescribeProducersResponse {
            throttle_time_ms: 0,
            topics: vec![TopicResponse {
                name: "a/b".into(),
                partitions: [0, 1, 4]
                    .into_iter()
                    .map(|partition_index| PartitionResponse {
                        partition_index,
                        error_code: codes::INVALID_TOPIC_EXCEPTION,
                        error_message: Some(message.clone()),
                        active_producers: Vec::new(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }
}
