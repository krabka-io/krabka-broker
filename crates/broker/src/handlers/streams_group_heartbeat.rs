//! `StreamsGroupHeartbeat` (`api_key` 88), the KIP-1071 streams rebalance
//! protocol. The handler routes the request to the per-group streams actor in
//! `GroupCoordinator`.
//!
//! It mirrors the KIP-932 share-group heartbeat handler
//! ([`super::share_group_heartbeat`]): decode, gate, `mark_streams` and
//! `get_or_create_streams`, send a `Heartbeat` actor message, await the
//! oneshot, then encode.
//!
//! Two gates gate it: the finalized `streams.version >= 1` feature, which is
//! KIP-1071 early access, AND the `streams_group.enable` config kill-switch.
//! BOTH must allow the request.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker, codes, coordinator::unified::streams::actor::StreamsGroupActorMessage,
    error::BrokerError, handlers::group_read_denied, time_util::now_ms,
};

mod creation;
mod topic_authz;
mod validation;

pub(crate) use self::creation::StreamsInternalTopics;

#[tracing::instrument(
    name = "handle_streams_group_heartbeat",
    level = "info",
    skip_all,
    fields(api = "StreamsGroupHeartbeat", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let streams_enabled = broker.config.streams_group.enable;
    let image = broker.controller.current_image();
    let ng = broker.group_coordinator.clone();
    {
        let mut cur: &[u8] = req_bytes;
        let req = StreamsGroupHeartbeatRequest::decode(&mut cur, version)?;

        // KafkaApis answers UNSUPPORTED_VERSION before the group ACL when the
        // streams protocol is off: KIP-1071 gates it on a finalized
        // streams.version >= 1 (early access, default-disabled), and krabka
        // also on the `streams_group.enable` config kill-switch.
        if !crate::features::feature_enabled(&image, crate::features::STREAMS_VERSION, 1)
            || !streams_enabled
        {
            return crate::handlers::encode_response(&error(codes::UNSUPPORTED_VERSION), version);
        }

        // ── ACL preamble ────────────────────────────────────────────
        // `Read` on `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        if group_read_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            &req.group_id,
        ) {
            return crate::handlers::encode_response(
                &error(codes::GROUP_AUTHORIZATION_FAILED),
                version,
            );
        }

        // Kafka's `KafkaApis.handleStreamsGroupHeartbeat` reads the topology
        // straight off the wire, before the group coordinator ever sees the
        // request: a topology that names a Kafka internal topic or an
        // invalid topic name is `STREAMS_INVALID_TOPOLOGY`, and a required
        // topic (source, repartition sink, repartition source or changelog)
        // that `Describe` denies fails the whole request with
        // `TOPIC_AUTHORIZATION_FAILED` -- no partial disclosure, and the
        // group coordinator never runs.
        if let Some(topology) = req.topology.as_ref() {
            let required = topic_authz::required_topics(topology);
            if let Some(message) = topic_authz::invalid_topology_message(broker, &required) {
                return crate::handlers::encode_response(
                    &crate::coordinator::unified::streams::actor::response::error_resp(
                        codes::STREAMS_INVALID_TOPOLOGY,
                        Some(message),
                    ),
                    version,
                );
            }
            if !required.is_empty() && topic_authz::describe_denied(broker, &image, ctx, &required)
            {
                return crate::handlers::encode_response(
                    &error(codes::TOPIC_AUTHORIZATION_FAILED),
                    version,
                );
            }
        }

        // Kafka's `GroupCoordinatorService` checks the request before it
        // schedules the write on the coordinator, so a refused request changes
        // no group and never gets NOT_COORDINATOR.
        if let Some((error_code, message)) = validation::request_error(&req) {
            return crate::handlers::encode_response(
                &crate::coordinator::unified::streams::actor::response::error_resp(
                    error_code,
                    Some(message),
                ),
                version,
            );
        }

        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
            return crate::handlers::encode_response(&error(error_code), version);
        }

        // Kafka creates a streams group only on a join, in place of nothing or of
        // an empty classic group (a KIP-1071 cold upgrade converts it here), and
        // answers GROUP_ID_NOT_FOUND to anything else.
        if let Some(message) = ng
            .streams_group_lookup_error(&req.group_id, req.member_epoch, now_ms())
            .await?
        {
            return crate::handlers::encode_response(
                &crate::coordinator::unified::streams::actor::response::error_resp(
                    codes::GROUP_ID_NOT_FOUND,
                    Some(message),
                ),
                version,
            );
        }

        ng.mark_streams(&req.group_id);
        let handle = ng.get_or_create_streams(&req.group_id);
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(StreamsGroupActorMessage::Heartbeat {
                request: Box::new(req),
                client_id: ctx.client_id.to_owned(),
                client_host: ctx.client_host(),
                reply: tx,
            })
            .await
            .is_err()
        {
            return crate::handlers::encode_response(
                &error(codes::COORDINATOR_LOAD_IN_PROGRESS),
                version,
            );
        }
        let Ok(result) = rx.await else {
            return crate::handlers::encode_response(&error(codes::UNKNOWN_SERVER_ERROR), version);
        };
        let mut resp = result.response;
        // KafkaApis hands the internal topics that the coordinator asks for to
        // the `CreateTopics` path, with the principal of the caller.
        if !result.creatable_topics.is_empty() {
            creation::create_internal_topics(broker, ctx, &mut resp, &result.creatable_topics)
                .await?;
        }
        crate::handlers::encode_response(&resp, version)
    }
}

/// Kafka's `StreamsGroupHeartbeatRequest.getErrorResponse`: the error code
/// and the defaults of the generated response data, whose status list is
/// empty.
fn error(code: i16) -> StreamsGroupHeartbeatResponse {
    crate::coordinator::unified::streams::actor::response::error_resp(code, None)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
    use krabka_protocol::owned::streams_group_heartbeat_response;
    use krabka_security::Principal;

    /// A valid join of member `m1` with a one-subtopology topology.
    fn request(group_id: &str) -> StreamsGroupHeartbeatRequest {
        use krabka_protocol::owned::streams_group_heartbeat_request::{Subtopology, Topology};

        StreamsGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: "m1".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            active_tasks: Some(vec![]),
            standby_tasks: Some(vec![]),
            warmup_tasks: Some(vec![]),
            topology: Some(Topology {
                epoch: 1,
                subtopologies: vec![Subtopology {
                    subtopology_id: "0".into(),
                    source_topics: vec!["in".into()],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Creates a one-partition topic on the test broker, so that a streams
    /// topology can read it.
    async fn create_source_topic(broker: &Broker, name: &str) {
        use krabka_metadata::{LeaderEpoch, PartitionRecord, TopicRecord};

        let node_id = krabka_audit::NodeId(broker.config.node_id.0);
        broker
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: name.into(),
                    topic_id: uuid::Uuid::new_v4(),
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: name.into(),
                    partition: 0,
                    leader: node_id,
                    replicas: vec![node_id],
                    isr: vec![node_id],
                    leader_epoch: LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                }),
            ])
            .await
            .expect("create the source topic");
    }

    /// Kafka creates the internal topics of a streams topology through
    /// `CreateTopics` with the principal of the caller, so the controller
    /// validates the configs and places the replicas, and it names a failed
    /// creation in the `MISSING_INTERNAL_TOPICS` status. Each row joins one
    /// member with a changelog topic on a one-broker cluster and compares the
    /// created topic and the status detail.
    #[tokio::test]
    async fn handle_creates_the_internal_topics_through_create_topics() {
        use krabka_protocol::owned::common::streams_group_heartbeat_request::{
            key_value::KeyValue, topic_info::TopicInfo,
        };

        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let config = |name: &str, value: &str| KeyValue {
            key: name.into(),
            value: value.into(),
            ..Default::default()
        };
        // (group id, the changelog topic of the topology, the expected
        // (replication factor, configs) of the created topic or None, the
        // expected end of the status detail)
        create_source_topic(&broker, "in").await;
        let rows = [
            (
                "rf-unset",
                TopicInfo {
                    name: "rf-unset-changelog".into(),
                    topic_configs: vec![config("cleanup.policy", "compact")],
                    ..Default::default()
                },
                Some((1, vec![("cleanup.policy", "compact")])),
                &"Internal topics are missing: rf-unset-changelog".to_string(),
            ),
            (
                "rf-too-high",
                TopicInfo {
                    name: "rf-too-high-changelog".into(),
                    replication_factor: 3,
                    ..Default::default()
                },
                None,
                &"Internal topics are missing: rf-too-high-changelog; Creation failed: \
                  rf-too-high-changelog (Unable to replicate the partition 3 time(s): The \
                  target replication factor of 3 cannot be reached because only 1 broker(s) \
                  are registered or some brokers have all their log directories cordoned.)."
                    .to_string(),
            ),
            (
                "bad-config",
                TopicInfo {
                    name: "bad-config-changelog".into(),
                    topic_configs: vec![config("cleanup.policy", "bogus")],
                    ..Default::default()
                },
                None,
                &format!(
                    "Internal topics are missing: bad-config-changelog; Creation failed: \
                     bad-config-changelog ({}).",
                    crate::config_keys::validate_topic_config_map(&maplit::btreemap! {
                        "cleanup.policy".to_string() => "bogus".to_string()
                    })
                    .expect_err("an unknown cleanup policy is refused")
                ),
            ),
        ];

        for (group_id, changelog, created, detail) in rows {
            let mut req = request(group_id);
            let topology = req.topology.as_mut().expect("the join carries a topology");
            topology.subtopologies[0].state_changelog_topics = vec![changelog.clone()];

            let bytes = handle(&broker, version, 1, &encode_request(&req), &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&bytes);

            assert!(resp.error_code == codes::NONE, "{group_id}: {resp:?}");
            let status = resp.status.unwrap_or_default();
            assert!(
                status
                    .iter()
                    .map(|s| s.status_detail.clone())
                    .collect::<Vec<_>>()
                    == vec![detail.clone()],
                "{group_id}: {status:?}"
            );
            let image = broker.controller.current_image();
            let topic = image.topic(&changelog.name);
            match created {
                Some((replication_factor, configs)) => {
                    let topic = topic.expect("the topic was created");
                    assert!(topic.replication_factor == replication_factor, "{group_id}");
                    let expected: std::collections::BTreeMap<String, String> = configs
                        .into_iter()
                        .map(|(name, value)| (name.to_string(), value.to_string()))
                        .collect();
                    assert!(
                        image.topic_config(&changelog.name) == Some(&expected),
                        "{group_id}"
                    );
                }
                None => assert!(topic.is_none(), "{group_id}"),
            }
        }
        broker_handle.shutdown().await;
    }

    /// Kafka's `handleStreamsGroupHeartbeat` reads the topology straight off
    /// the wire before the group coordinator sees it: a required topic (here,
    /// the one source topic) that names a Kafka internal topic or an invalid
    /// name is refused with `STREAMS_INVALID_TOPOLOGY` and Kafka's message,
    /// and no group is created.
    #[tokio::test]
    async fn handle_refuses_a_topology_naming_a_prohibited_or_invalid_topic() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);

        // (group id, source topic, the expected error message)
        let rows = [
            (
                "internal-topic",
                "__consumer_offsets",
                "Use of Kafka internal topics __consumer_offsets in a Kafka Streams topology is \
                 prohibited.",
            ),
            (
                "invalid-name",
                "bad topic",
                "Topic names bad topic are not valid topic names.",
            ),
        ];

        for (group_id, source_topic, message) in rows {
            let mut req = request(group_id);
            req.topology
                .as_mut()
                .expect("the join carries a topology")
                .subtopologies[0]
                .source_topics = vec![source_topic.into()];

            let bytes = handle(&broker, version, 1, &encode_request(&req), &ctx)
                .await
                .expect("handle");

            assert!(
                decode_response(&bytes)
                    == crate::coordinator::unified::streams::actor::response::error_resp(
                        codes::STREAMS_INVALID_TOPOLOGY,
                        Some(message.into()),
                    ),
                "{group_id}"
            );
            assert!(
                broker.group_coordinator.find_streams(group_id).is_none(),
                "{group_id}"
            );
        }
        broker_handle.shutdown().await;
    }

    /// Kafka's `filterByAuthorized(DESCRIBE, TOPIC, requiredTopics)`: a
    /// principal that can `Read` the group but not `Describe` one of the
    /// topology's required topics gets `TOPIC_AUTHORIZATION_FAILED` (29) for
    /// the whole request, and the group coordinator never runs, so no group
    /// is created.
    #[tokio::test]
    async fn handle_refuses_a_required_topic_denied_for_describe() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker_with_grants().await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        // `Group:Read` only: the source topic `in` gets no `Describe`.
        let principal = crate::test_support::principal("Group:Read");
        let ctx = context(&principal, &peer);

        let bytes = handle(
            &broker,
            version,
            1,
            &encode_request(&request("describe-denied")),
            &ctx,
        )
        .await
        .expect("handle");

        assert!(
            decode_response(&bytes)
                == crate::coordinator::unified::streams::actor::response::error_resp(
                    codes::TOPIC_AUTHORIZATION_FAILED,
                    None,
                )
        );
        assert!(
            broker
                .group_coordinator
                .find_streams("describe-denied")
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    /// Kafka's `CREATE` gate before `createStreamsInternalTopics`: `Create`
    /// on the `Cluster` once, else `Create` on each topic. A principal with
    /// only `Read` on the group and `Describe` on the topics gets none of
    /// the internal topics created, and the `MISSING_INTERNAL_TOPICS` status
    /// names them as unauthorized instead -- unlike the no-ACL-check path
    /// this replaces, which let any principal with group `Read` make the
    /// broker create arbitrary topics.
    #[tokio::test]
    async fn handle_reports_topics_unauthorized_to_create_in_status() {
        use krabka_protocol::owned::common::streams_group_heartbeat_request::topic_info::TopicInfo;

        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker_with_grants().await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        create_source_topic(&broker, "in").await;
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        // `Read` on the group and `Describe` on the topics, but no `Create`
        // anywhere.
        let principal = crate::test_support::principal("Group:Read+Topic:Describe");
        let ctx = context(&principal, &peer);

        let mut req = request("no-create-grant");
        let topology = req.topology.as_mut().expect("the join carries a topology");
        topology.subtopologies[0].state_changelog_topics = vec![TopicInfo {
            name: "no-create-grant-changelog".into(),
            ..Default::default()
        }];

        let bytes = handle(&broker, version, 1, &encode_request(&req), &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&bytes);

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        let status = resp.status.unwrap_or_default();
        assert!(
            status
                .iter()
                .map(|s| s.status_detail.clone())
                .collect::<Vec<_>>()
                == vec![
                    "Internal topics are missing: no-create-grant-changelog; Unauthorized to \
                     CREATE on topics no-create-grant-changelog."
                        .to_string()
                ],
            "{status:?}"
        );
        let image = broker.controller.current_image();
        assert!(image.topic("no-create-grant-changelog").is_none());
        broker_handle.shutdown().await;
    }

    /// Kafka creates a streams group only on a join, and answers
    /// `GROUP_ID_NOT_FOUND` to a heartbeat or a leave for a group that does not
    /// exist and to any heartbeat for a group of another type
    /// (`getOrCreateStreamsGroup`, `getStreamsGroupOrThrow`, `streamsGroup`).
    /// Each row sends one request for its own group id, compares the whole
    /// response, and checks whether a streams group exists afterwards.
    #[tokio::test]
    async fn handle_finds_or_creates_the_streams_group_as_kafka_does() {
        use crate::coordinator::unified::{
            actor::GroupKindTag, streams::actor::response::error_resp,
        };

        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        broker.group_coordinator.mark_share("share");
        let _consumer = broker
            .group_coordinator
            .get_or_create_group("consumer", GroupKindTag::Consumer);
        let heartbeat = |group_id: &str, member_epoch| StreamsGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: "m1".into(),
            member_epoch,
            ..Default::default()
        };
        let not_found =
            |message: String| Some(error_resp(codes::GROUP_ID_NOT_FOUND, Some(message)));
        // (group id, request, expected error response or None for success,
        // a streams group exists afterwards)
        let rows = [
            (
                "absent-heartbeat",
                heartbeat("absent-heartbeat", 3),
                not_found("Streams group absent-heartbeat not found.".into()),
                false,
            ),
            (
                "absent-leave",
                heartbeat("absent-leave", -1),
                not_found("Group absent-leave not found.".into()),
                false,
            ),
            (
                "share",
                request("share"),
                not_found("Group share is not a streams group.".into()),
                false,
            ),
            (
                "consumer",
                request("consumer"),
                not_found("Group consumer is not a streams group.".into()),
                false,
            ),
            ("absent-join", request("absent-join"), None, true),
        ];

        for (group_id, req, expected, exists) in rows {
            let bytes = handle(&broker, version, 1, &encode_request(&req), &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&bytes);
            match expected {
                Some(expected) => assert!(resp == expected, "{group_id}"),
                None => assert!(resp.error_code == codes::NONE, "{group_id}: {resp:?}"),
            }
            assert!(
                broker.group_coordinator.find_streams(group_id).is_some() == exists,
                "{group_id}"
            );
        }
        broker_handle.shutdown().await;
    }

    /// Kafka's `GroupCoordinatorService` refuses an invalid request before the
    /// coordinator runs it, so the response carries only the error and the
    /// group is not created.
    #[tokio::test]
    async fn handle_refuses_an_invalid_request_and_creates_no_group() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req = StreamsGroupHeartbeatRequest {
            member_id: String::new(),
            ..request("invalid-join")
        };

        let bytes = handle(&broker, version, 1, &encode_request(&req), &ctx)
            .await
            .expect("handle");

        assert!(
            decode_response(&bytes)
                == crate::coordinator::unified::streams::actor::response::error_resp(
                    codes::INVALID_REQUEST,
                    Some("MemberId can't be empty.".into()),
                )
        );
        assert!(
            broker
                .group_coordinator
                .find_streams("invalid-join")
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    fn encode_request(req: &StreamsGroupHeartbeatRequest) -> Bytes {
        crate::test_support::encode_request(req, streams_group_heartbeat_response::MAX_VERSION)
    }

    fn decode_response(bytes: &Bytes) -> StreamsGroupHeartbeatResponse {
        crate::test_support::decode_response(bytes, streams_group_heartbeat_response::MAX_VERSION)
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    fn context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "streams-client")
    }

    async fn start_broker(
        streams_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
            cfg.streams_group.enable = streams_enabled;
        })
        .await
    }

    /// A broker whose authorizer grants exactly the operations named in the
    /// caller's principal (see [`crate::test_support::GrantsInPrincipalName`]),
    /// for the tests that drive a specific ACL gate rather than allow
    /// everything.
    async fn start_broker_with_grants() -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::test_support::GrantsInPrincipalName);
            cfg.streams_group.enable = true;
        })
        .await
    }

    async fn finalize_streams_version(broker: &Broker) {
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crate::features::STREAMS_VERSION.into(),
                level: 1,
            })])
            .await
            .expect("submit streams.version");

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if broker
                    .controller
                    .current_image()
                    .finalized_feature(crate::features::STREAMS_VERSION)
                    == Some(1)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("streams.version visible");
    }

    #[tokio::test]
    async fn handle_unfinalized_feature_returns_unsupported_version_with_read_allowed() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req_bytes = encode_request(&request("streams-app-disabled-feature"));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_disabled_config_returns_unsupported_version_when_feature_finalized() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(false).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req_bytes = encode_request(&request("streams-app-disabled-config"));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_persists_request_client_identity() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);

        let bytes = handle(
            &broker,
            version,
            1,
            &encode_request(&request("identity-group")),
            &ctx,
        )
        .await
        .expect("StreamsGroupHeartbeat handler");
        assert!(decode_response(&bytes).error_code == 0);

        let actor = broker
            .group_coordinator
            .get_or_create_streams("identity-group");
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(StreamsGroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe streams group");
        let view = rx.await.expect("streams group view");

        assert!(view.members.len() == 1);
        assert!(view.members[0].client_id == "streams-client");
        assert!(view.members[0].client_host == "/127.0.0.1");

        let peer: SocketAddr = "127.0.0.2:9093".parse().unwrap();
        let ctx = crate::test_support::request_context(&principal, &peer, "streams-client-b");
        let req = StreamsGroupHeartbeatRequest {
            group_id: "identity-group".into(),
            member_id: view.members[0].member_id.clone(),
            member_epoch: view.members[0].member_epoch,
            ..Default::default()
        };
        let bytes = handle(&broker, version, 2, &encode_request(&req), &ctx)
            .await
            .expect("StreamsGroupHeartbeat identity refresh");
        assert!(decode_response(&bytes).error_code == 0);

        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(StreamsGroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe refreshed streams group");
        let view = rx.await.expect("refreshed streams group view");
        assert!(view.members[0].client_id == "streams-client-b");
        assert!(view.members[0].client_host == "/127.0.0.2");

        broker_handle.shutdown().await;
    }

    use super::*;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        use krabka_protocol::owned::streams_group_heartbeat_response::{
            self, StreamsGroupHeartbeatResponse,
        };

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "streams-client");

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

        let bytes = crate::handlers::encode_response(
            &error(codes::GROUP_AUTHORIZATION_FAILED),
            streams_group_heartbeat_response::MAX_VERSION,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = StreamsGroupHeartbeatResponse::decode(
            &mut cur,
            streams_group_heartbeat_response::MAX_VERSION,
        )
        .unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }
}
