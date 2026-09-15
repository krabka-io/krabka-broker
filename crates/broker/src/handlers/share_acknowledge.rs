//! `ShareAcknowledge` (`api_key` 79), from KIP-932.
//!
//! This is the acknowledge-only counterpart of `ShareFetch`. It acknowledges
//! records that a member acquired earlier, and acquires no new ones.
//!
//! For every requested partition that this broker leads, the handler applies
//! each acknowledgement batch to the `(group, topic, partition)`
//! [`AcquisitionState`] machine, and persists the result. Accept advances the
//! SPSO, Release offers the records again, and Reject and Gap archive them.
//!
//! A partition that this broker does not lead gets `NOT_LEADER_OR_FOLLOWER`.
//! An acknowledge that targets records the member does not currently hold
//! fails that partition row with `INVALID_RECORD_STATE`.
//!
//! `network::dispatch` intercepts this request inline, so the handler receives
//! the per-connection principal and the peer `SocketAddr` for the group `Read`
//! and per-topic `Read` ACL gates.

use std::time::Instant;

use bytes::Bytes;
use krabka_log::Offset;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        share_acknowledge_request::ShareAcknowledgeRequest,
        share_acknowledge_response::{
            LeaderIdAndEpoch, PartitionData, ShareAcknowledgeResponse,
            ShareAcknowledgeTopicResponse,
        },
    },
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{group_read_denied, share_fetch::apply_one_ack},
};

#[tracing::instrument(
    name = "handle_share_acknowledge",
    level = "info",
    skip_all,
    fields(api = "ShareAcknowledge", version, req_bytes = req_bytes.len()),
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
    let req = ShareAcknowledgeRequest::decode(&mut cur, version)?;

    let cfg = broker.config.share_group.clone();
    let lock_timeout_ms = i32::try_from(cfg.record_lock_duration.as_millis()).unwrap_or(i32::MAX);

    if !cfg.enable {
        return encode_error_response(version, codes::UNSUPPORTED_VERSION);
    }

    let group = req.group_id.clone().unwrap_or_default();
    let member = req.member_id.clone().unwrap_or_default();

    // Kafka's `KafkaApis.handleShareAcknowledgeRequest` checks `Read` on the
    // group after the feature gate, and before the member, the share session
    // and the topic checks.
    let image = broker.controller.current_image();
    if group_read_denied(broker.config.authorizer.as_ref(), &image, ctx, &group) {
        return encode_error_response(version, codes::GROUP_AUTHORIZATION_FAILED);
    }

    let released = match broker.share_partition_leaders.update_acknowledge_session(
        &group,
        &member,
        req.share_session_epoch,
    ) {
        Ok(released) => released,
        Err(code) => return encode_error_response(version, code),
    };

    let now = Instant::now();
    let responses = process_topics(broker, &req, ctx, &cfg, &group, &member, now).await;
    broker
        .share_partition_leaders
        .release_session_partitions(&group, &member, &released)
        .await;

    let resp = ShareAcknowledgeResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        error_message: None,
        acquisition_lock_timeout_ms: lock_timeout_ms,
        responses,
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

async fn process_topics(
    broker: &Broker,
    req: &ShareAcknowledgeRequest,
    ctx: &crate::handlers::RequestContext<'_>,
    cfg: &crate::coordinator::unified::share::config::ShareGroupConfig,
    group: &str,
    member: &str,
    now: Instant,
) -> Vec<ShareAcknowledgeTopicResponse> {
    let mgr = &broker.share_partition_leaders;
    let image = broker.controller.current_image();
    let mut responses = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let topic_id = uuid::Uuid::from_bytes(topic.topic_id.0);
        // Kafka's `KafkaApis.getAcknowledgeBatchesFromShareAcknowledgeRequest`
        // answers UNKNOWN_TOPIC_ID on every partition of a topic id that
        // `metadataCache.topicIdsToNames()` does not hold, the zero id
        // included. `handleAcknowledgements` checks `Read` only for the
        // partitions that resolved.
        let Some(topic_name) = mgr.topic_name_for(topic_id) else {
            responses.push(ShareAcknowledgeTopicResponse {
                topic_id: topic.topic_id,
                partitions: topic
                    .partitions
                    .iter()
                    .map(|ap| PartitionData {
                        partition_index: ap.partition_index,
                        error_code: codes::UNKNOWN_TOPIC_ID,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            });
            continue;
        };

        let denied = broker.config.authorizer.authorize(
            &*image,
            &AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::Topic,
                resource_name: &topic_name,
                operation: AclOperation::Read,
            },
        ) == AuthorizationResult::Deny;

        let mut parts: Vec<PartitionData> = Vec::with_capacity(topic.partitions.len());
        for ap in &topic.partitions {
            let mut out = PartitionData {
                partition_index: ap.partition_index,
                ..Default::default()
            };

            if denied {
                out.error_code = codes::TOPIC_AUTHORIZATION_FAILED;
                parts.push(out);
                continue;
            }

            if !mgr.topic_leader_is_self(topic_id, ap.partition_index) {
                let (leader_id, leader_epoch) = mgr.current_leader_of(topic_id, ap.partition_index);
                out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
                out.current_leader = LeaderIdAndEpoch {
                    leader_id,
                    leader_epoch,
                    ..Default::default()
                };
                parts.push(out);
                continue;
            }

            // A failed state read fails the partition and caches nothing.
            let cell = match mgr.get_or_load(group, topic_id, ap.partition_index).await {
                Ok(cell) => cell,
                Err(code) => {
                    out.error_code = code;
                    parts.push(out);
                    continue;
                }
            };
            let mut st = cell.lock().await;
            // The acknowledgement is durable before the answer, or it is rolled
            // back and the write error is the partition error, as Kafka's
            // `SharePartition.rollbackOrProcessStateUpdates` does.
            out.error_code = mgr
                .apply_durably(group, topic_id, ap.partition_index, &cell, &mut st, |st| {
                    let mut err = codes::NONE;
                    for batch in &ap.acknowledgement_batches {
                        // A renew-ack RENEWs each batch's lock instead of
                        // acknowledging.
                        let res = if req.is_renew_ack {
                            st.renew(
                                member,
                                Offset(batch.first_offset),
                                Offset(batch.last_offset),
                                now,
                                cfg.record_lock_duration,
                            )
                        } else {
                            apply_one_ack(
                                st,
                                member,
                                batch.first_offset,
                                batch.last_offset,
                                &batch.acknowledge_types,
                                now,
                            )
                        };
                        if let Err(code) = res {
                            err = code;
                        }
                    }
                    err
                })
                .await;
            parts.push(out);
        }

        responses.push(ShareAcknowledgeTopicResponse {
            topic_id: topic.topic_id,
            partitions: parts,
            ..Default::default()
        });
    }
    responses
}

/// Encodes a `ShareAcknowledgeResponse` that carries a top-level error and no
/// per-partition row. The error is a feature-gate, authorization, or session
/// failure.
///
/// This is Kafka's `ShareAcknowledgeRequest.getErrorResponse`, which sets only
/// the throttle time and the error code. So the acquisition lock timeout keeps
/// its default, 0.
fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes, BrokerError> {
    let resp = ShareAcknowledgeResponse {
        throttle_time_ms: 0,
        error_code,
        error_message: None,
        acquisition_lock_timeout_ms: 0,
        responses: Vec::new(),
        ..Default::default()
    };
    crate::handlers::encode_response(&resp, version)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use assert2::assert;
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::{
            create_topics_request::{CreatableTopic, CreateTopicsRequest},
            share_acknowledge_request::{AcknowledgePartition, AcknowledgeTopic},
            share_acknowledge_response,
        },
        primitives::uuid::Uuid as ProtoUuid,
    };
    use krabka_security::Principal;

    use super::*;

    crate::test_support::wire_helpers!(
        ShareAcknowledgeRequest,
        ShareAcknowledgeResponse,
        version = share_acknowledge_response::MAX_VERSION,
        client_id = "client-a"
    );

    fn request(topic_id: ProtoUuid, partitions: &[i32]) -> ShareAcknowledgeRequest {
        ShareAcknowledgeRequest {
            group_id: Some("g1".into()),
            member_id: Some("member-1".into()),
            share_session_epoch: 0,
            topics: vec![AcknowledgeTopic {
                topic_id,
                partitions: partitions
                    .iter()
                    .map(|partition_index| AcknowledgePartition {
                        partition_index: *partition_index,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    async fn start_broker(share_enabled: bool) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.share_group.enable = share_enabled;
        })
        .await
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    #[test]
    fn encode_error_response_preserves_top_level_fields() {
        let resp = encode_error_response(
            share_acknowledge_response::MAX_VERSION,
            codes::UNSUPPORTED_VERSION,
        )
        .expect("encode");
        let resp = decode_response(&resp);

        let expected = ShareAcknowledgeResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            acquisition_lock_timeout_ms: 0,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[tokio::test]
    async fn handle_disabled_feature_returns_top_level_unsupported_version() {
        let version = share_acknowledge_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(false).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req_bytes = encode_request(&request(ProtoUuid([7; 16]), &[0]));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = ShareAcknowledgeResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            acquisition_lock_timeout_ms: 0,
            responses: Vec::new(),
            node_endpoints: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Denies `Read` on every topic and allows everything else, so that topic
    /// creation still works and only the per-topic gate refuses.
    #[derive(Debug)]
    struct DenyTopicRead;

    impl crate::authorizer::Authorizer for DenyTopicRead {
        fn authorize(
            &self,
            _source: &dyn crate::authorizer::AclSource,
            request: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            if request.resource_type == ResourceType::Topic
                && request.operation == AclOperation::Read
            {
                AuthorizationResult::Deny
            } else {
                AuthorizationResult::Allow
            }
        }
    }

    /// The topic id that one request row carries.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TopicRef {
        /// The id of a topic that exists.
        Known,
        /// A non-zero id that no topic has.
        Unknown,
        /// The zero id.
        Zero,
    }

    async fn create_topic(broker: &crate::broker::BrokerHandle, name: &str) -> ProtoUuid {
        let client = krabka_client_core::Client::builder()
            .bootstrap(broker.listen_addr().to_string())
            .client_id("share-acknowledge-resolution-test")
            .build()
            .await
            .expect("client build");
        let response = client
            .send(CreateTopicsRequest {
                topics: vec![CreatableTopic {
                    name: name.to_string(),
                    num_partitions: 1,
                    replication_factor: 1,
                    ..Default::default()
                }],
                timeout_ms: 5_000,
                ..Default::default()
            })
            .await
            .expect("CreateTopics");
        assert!(response.topics[0].error_code == codes::NONE, "{response:?}");
        broker.wait_until_partition_present(name, 0).await;
        let image = broker.controller_image_for_test();
        let topic = image.topic(name).expect("created topic in the image");
        ProtoUuid(topic.topic_id.into_bytes())
    }

    /// Open a share session for `member` on partition 0 of `topic_id`,
    /// acknowledge it at epoch 1 with no batch, and return the decoded
    /// response. Kafka answers `NONE` for a partition that it may acknowledge
    /// and that carries no batch.
    async fn acknowledge(
        broker: &crate::broker::BrokerHandle,
        version: i16,
        member: &str,
        topic_id: ProtoUuid,
    ) -> ShareAcknowledgeResponse {
        let shared = broker.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = crate::test_support::request_context(&principal, &peer, "client-a");
        let id = uuid::Uuid::from_bytes(topic_id.0);
        shared
            .share_partition_leaders
            .update_fetch_session(
                "g1",
                member,
                ctx.connection_id,
                0,
                &maplit::hashset! {(id, 0)},
                &std::collections::HashSet::new(),
                false,
                false,
            )
            .expect("open share session");
        let mut request = request(topic_id, &[0]);
        request.member_id = Some(member.into());
        request.share_session_epoch = 1;
        let req_bytes = crate::test_support::encode_request(&request, version);
        let resp = handle(&shared, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        crate::test_support::decode_response(&resp, version)
    }

    /// Run one case per (version, topic reference) on `broker`, each in its
    /// own share session. `known_error` is the code for a topic that exists.
    async fn drive(
        broker: &crate::broker::BrokerHandle,
        known: ProtoUuid,
        known_error: i16,
    ) -> (
        Vec<(i16, TopicRef, ShareAcknowledgeResponse)>,
        Vec<(i16, TopicRef, ShareAcknowledgeResponse)>,
    ) {
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for version in [
            share_acknowledge_response::MIN_VERSION,
            share_acknowledge_response::MAX_VERSION,
        ] {
            for (topic, topic_id, error_code) in [
                (TopicRef::Known, known, known_error),
                (
                    TopicRef::Unknown,
                    ProtoUuid([8; 16]),
                    codes::UNKNOWN_TOPIC_ID,
                ),
                (TopicRef::Zero, ProtoUuid::ZERO, codes::UNKNOWN_TOPIC_ID),
            ] {
                let member = format!("member-{version}-{topic:?}");
                let response = acknowledge(broker, version, &member, topic_id).await;
                actual.push((version, topic, response));
                let row = PartitionData {
                    partition_index: 0,
                    error_code,
                    ..Default::default()
                };
                expected.push((
                    version,
                    topic,
                    ShareAcknowledgeResponse {
                        // The wire carries the lock timeout from v2 on. An
                        // older version decodes the field's default, 0.
                        acquisition_lock_timeout_ms: if version >= 2 { 30_000 } else { 0 },
                        responses: vec![ShareAcknowledgeTopicResponse {
                            topic_id,
                            partitions: vec![row],
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ));
            }
        }
        (actual, expected)
    }

    #[tokio::test]
    async fn partition_row_error_follows_topic_id() {
        let (broker_handle, _dir) = start_broker(true).await;
        let known = create_topic(&broker_handle, "ack-resolution").await;

        let (actual, expected) = drive(&broker_handle, known, codes::NONE).await;

        assert!(actual == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka answers `UNKNOWN_TOPIC_ID` before it authorizes the topic. A
    /// principal with no topic `Read` grant sees 29 for a topic that exists and
    /// 100 for an id that does not resolve.
    #[tokio::test]
    async fn unresolved_id_answers_before_topic_authorization() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.share_group.enable = true;
            cfg.authorizer = std::sync::Arc::new(DenyTopicRead);
        })
        .await;
        let known = create_topic(&broker_handle, "ack-resolution").await;

        let (actual, expected) =
            drive(&broker_handle, known, codes::TOPIC_AUTHORIZATION_FAILED).await;

        assert!(actual == expected);
        broker_handle.shutdown().await;
    }
}
