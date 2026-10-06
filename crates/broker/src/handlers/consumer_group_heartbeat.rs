//! `ConsumerGroupHeartbeat` (`api_key` 68), from the KIP-848 next-gen consumer
//! group protocol. It routes the request to the per-group actor in
//! `GroupCoordinator`.

use bytes::Bytes;
use krabka_protocol::owned::{
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker,
    codes,
    coordinator::unified::{
        actor::{GroupActorMessage, GroupKindTag},
        regex_resolver::ImageTopicRegexResolver,
    },
    error::BrokerError,
    handlers::{group_read_denied, group_version_disabled},
};

#[tracing::instrument(
    name = "handle_consumer_group_heartbeat",
    level = "info",
    skip_all,
    fields(api = "ConsumerGroupHeartbeat", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let coordinator = broker.group_coordinator.clone();
    // Read the offset BEFORE the image, not after: if a record commits in
    // between, `image` may reflect it while `metadata_offset` does not. The
    // regex resolution the actor may make from `image` is stamped with
    // `metadata_offset`, and an offset older than its image only makes a
    // later refresh happen once too often, never once too rarely.
    let metadata_offset = broker.controller.current_metadata_offset();
    let image = broker.controller.current_image();
    let mut cur: &[u8] = req_bytes;
    let req: ConsumerGroupHeartbeatRequest =
        crate::handlers::decode_group_request(&mut cur, version)?;

    // ── Protocol gate ───────────────────────────────────────────
    // Kafka's `handleConsumerGroupHeartbeat` checks whether the
    // consumer-group protocol is available BEFORE any ACL check, so a
    // disabled protocol answers `UNSUPPORTED_VERSION` even to a caller
    // with no ACLs on the group at all. KIP-848 / KIP-584: the next-gen
    // protocol is gated on a finalized group.version >= 1. Below that —
    // including UNFINALIZED, which means disabled — reject so the client
    // falls back to the classic protocol.
    // `isConsumerGroupProtocolEnabled` also needs `consumer` among the
    // configured rebalance protocols, and answers the same way without it.
    if group_version_disabled(&image) || !coordinator.config.next_gen_enabled() {
        return reply(version, codes::UNSUPPORTED_VERSION, None);
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
        return reply(version, codes::GROUP_AUTHORIZATION_FAILED, None);
    }

    // `Describe` on every distinct name in `subscribed_topic_names`
    // (Kafka's `filterByAuthorized(request.context, DESCRIBE, TOPIC,
    // subscribedTopicSet)`). Any denial fails the WHOLE heartbeat with
    // `TOPIC_AUTHORIZATION_FAILED` (29); the group is never touched, so an
    // unauthorized caller cannot learn the denied topic's id or
    // partitions by being admitted as a member. This runs before
    // `group_coordinator_error` -- Kafka authorizes the request before it
    // ever reaches coordinator routing, so an unauthorized subscription
    // must not be masked by `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE`.
    if crate::handlers::subscribed_names_describe_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        req.subscribed_topic_names.as_deref(),
    ) {
        return reply(version, codes::TOPIC_AUTHORIZATION_FAILED, None);
    }

    // `GroupCoordinatorService.consumerGroupHeartbeat` validates the
    // request before it routes it to a coordinator shard.
    if let Err(refused) = validate_request(&req, version, &coordinator.config) {
        return crate::handlers::encode_response(&*refused, version);
    }

    // `subscribed_topic_regex` (KIP-848 v1+): the actor resolves the
    // group's patterns against the image read above, with this principal's
    // `Describe` decisions, when a heartbeat needs them resolved or
    // refreshed -- Kafka's `TopicRegexResolver.resolveRegularExpressions`
    // with the request context of the heartbeat. A pattern that fails to
    // compile is rejected by the actor's own
    // `check_subscribed_topic_regex` with `INVALID_REGULAR_EXPRESSION`
    // before any member state changes.
    let regex_resolver = std::sync::Arc::new(ImageTopicRegexResolver::new(
        image,
        metadata_offset,
        broker.config.authorizer.clone(),
        ctx.principal.clone(),
        *ctx.peer,
    ));

    if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
        return reply(version, error_code, None);
    }

    // Kafka creates a consumer group only on a join, and answers
    // GROUP_ID_NOT_FOUND for a missing group on any other epoch and for a
    // share or streams group, without touching any group.
    if let Some(message) = coordinator.consumer_group_lookup_error(&req.group_id, req.member_epoch)
    {
        return reply(version, codes::GROUP_ID_NOT_FOUND, Some(message));
    }

    // Route to the one actor for this id, spawning a consumer-kind actor if
    // the id is brand-new. Both RPC families reach the same actor; a classic
    // group rejects a next-gen heartbeat from inside the actor's `Heartbeat`
    // arm (replying `GROUP_ID_NOT_FOUND`), which is where the per-group kind
    // lock now lives.
    let handle = coordinator.get_or_create_group(&req.group_id, GroupKindTag::Consumer);
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::Heartbeat {
            request: req,
            client_id: ctx.client_id.unwrap_or_default().to_owned(),
            client_host: ctx.client_host(),
            regex_resolver,
            reply: tx,
        })
        .await
        .is_err()
    {
        return reply(version, codes::COORDINATOR_LOAD_IN_PROGRESS, None);
    }
    let resp = rx
        .await
        .unwrap_or_else(|_| error(codes::UNKNOWN_SERVER_ERROR));
    crate::handlers::encode_response(&resp, version)
}

fn error(code: i16) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code: code,
        ..Default::default()
    }
}

/// The encoded early refusal: `error(code)` carrying `message`.
fn reply(version: i16, code: i16, message: Option<String>) -> Result<Bytes, BrokerError> {
    crate::handlers::encode_response(
        &ConsumerGroupHeartbeatResponse {
            error_message: message,
            ..error(code)
        },
        version,
    )
}

/// The version from which a consumer must generate its own member id,
/// Kafka's `CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION`.
const CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION: i16 = 1;
const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;
const LEAVE_GROUP_STATIC_MEMBER_EPOCH: i32 = -2;

/// Kafka's `GroupCoordinatorService.throwIfConsumerGroupHeartbeatRequestIsInvalid`.
///
/// A refusal carries only the error code and Kafka's message.
fn validate_request(
    req: &ConsumerGroupHeartbeatRequest,
    version: i16,
    config: &crate::coordinator::unified::config::NextGenConfig,
) -> Result<(), Box<ConsumerGroupHeartbeatResponse>> {
    let invalid = |message: &str| {
        Box::new(ConsumerGroupHeartbeatResponse {
            error_code: codes::INVALID_REQUEST,
            error_message: Some(message.to_string()),
            ..Default::default()
        })
    };
    // `Utils.throwIfEmptyString`: a present value that trims to nothing.
    let blank = |value: Option<&str>| value.is_some_and(|value| value.trim().is_empty());

    if (version >= CONSUMER_GENERATED_MEMBER_ID_REQUIRED_VERSION
        || req.member_epoch > 0
        || req.member_epoch == LEAVE_GROUP_MEMBER_EPOCH)
        && blank(Some(&req.member_id))
    {
        return Err(invalid("MemberId can't be empty."));
    }
    if blank(Some(&req.group_id)) {
        return Err(invalid("GroupId can't be empty."));
    }
    if blank(req.instance_id.as_deref()) {
        return Err(invalid("InstanceId can't be empty."));
    }
    if blank(req.rack_id.as_deref()) {
        return Err(invalid("RackId can't be empty."));
    }

    if req.member_epoch == 0 {
        if req.rebalance_timeout_ms == -1 {
            return Err(invalid(
                "RebalanceTimeoutMs must be provided in first request.",
            ));
        }
        if req
            .topic_partitions
            .as_ref()
            .is_none_or(|partitions| !partitions.is_empty())
        {
            return Err(invalid("TopicPartitions must be empty when (re-)joining."));
        }
        if req.subscribed_topic_names.is_none() && req.subscribed_topic_regex.is_none() {
            return Err(invalid(
                "Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when \
                 (re-)joining.",
            ));
        }
    } else if req.member_epoch == LEAVE_GROUP_STATIC_MEMBER_EPOCH {
        if req.instance_id.is_none() {
            return Err(invalid("InstanceId can't be null."));
        }
    } else if req.member_epoch < LEAVE_GROUP_STATIC_MEMBER_EPOCH {
        return Err(invalid("MemberEpoch is invalid."));
    }

    if let Some(name) = req
        .server_assignor
        .as_deref()
        .filter(|name| !config.assignor_enabled(name))
    {
        let supported: Vec<&str> = config
            .assignors
            .iter()
            .map(|assignor| assignor.name())
            .collect();
        return Err(Box::new(ConsumerGroupHeartbeatResponse {
            error_code: codes::UNSUPPORTED_ASSIGNOR,
            error_message: Some(format!(
                "ServerAssignor {name} is not supported. Supported assignors: {}.",
                supported.join(", ")
            )),
            ..Default::default()
        }));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use bytes::BytesMut;
    use krabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord};
    use krabka_protocol::{Decode, Encode};

    const VERSION: i16 = krabka_protocol::owned::consumer_group_heartbeat_request::MAX_VERSION;

    fn request(group_id: &str) -> Bytes {
        let req = ConsumerGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: "member-a".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
            topic_partitions: Some(vec![]),
            subscribed_topic_names: Some(vec!["topic-a".into()]),
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(req.encoded_len(VERSION));
        req.encode(&mut buf, VERSION)
            .expect("encode ConsumerGroupHeartbeatRequest");
        buf.freeze()
    }

    crate::test_support::response_helpers!(
        ConsumerGroupHeartbeatResponse,
        version = VERSION,
        client_id = "consumer-group-heartbeat-test"
    );

    /// Start a broker with `authorizer` and wait until its group coordinator
    /// serves `__consumer_offsets`.
    async fn start_broker(
        authorizer: Arc<dyn crate::authorizer::Authorizer>,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let (handle, dir) = crate::test_support::start_broker_with_authorizer(
            crate::test_support::controller_peer_allowed(authorizer),
        )
        .await;
        handle.wait_until_group_coordinator_ready().await;
        (handle, dir)
    }

    fn image_with_group_version(level: i16) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::group_version::GROUP_VERSION_FEATURE.into(),
            level,
        }));
        image
    }

    fn anonymous_principal() -> krabka_security::Principal {
        crate::test_support::principal("ANONYMOUS")
    }

    #[test]
    fn group_version_gate_distinguishes_disabled_and_enabled_images() {
        let fresh = MetadataImage::new(uuid::Uuid::nil());
        assert!(group_version_disabled(&fresh));

        let enabled = image_with_group_version(1);
        assert!(!group_version_disabled(&enabled));

        let disabled = image_with_group_version(0);
        assert!(group_version_disabled(&disabled));
    }

    /// Kafka's `throwIfConsumerGroupHeartbeatRequestIsInvalid`, row by row:
    /// (label, version, request, whole expected refusal or `None`).
    #[test]
    fn validate_request_follows_kafka() {
        use krabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions;

        let join = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
            subscribed_topic_names: Some(vec!["t".into()]),
            topic_partitions: Some(vec![]),
            ..Default::default()
        };
        let steady = ConsumerGroupHeartbeatRequest {
            member_epoch: 3,
            rebalance_timeout_ms: -1,
            subscribed_topic_names: None,
            topic_partitions: None,
            ..join.clone()
        };
        let invalid = |message: &str| {
            Some(ConsumerGroupHeartbeatResponse {
                error_code: codes::INVALID_REQUEST,
                error_message: Some(message.into()),
                ..Default::default()
            })
        };
        let rows = [
            ("valid join", 1, join.clone(), None),
            ("valid steady", 1, steady.clone(), None),
            (
                "v0 join mints the member id",
                0,
                ConsumerGroupHeartbeatRequest {
                    member_id: String::new(),
                    ..join.clone()
                },
                None,
            ),
            (
                "v1 empty member id",
                1,
                ConsumerGroupHeartbeatRequest {
                    member_id: " ".into(),
                    ..join.clone()
                },
                invalid("MemberId can't be empty."),
            ),
            (
                "v0 leave with empty member id",
                0,
                ConsumerGroupHeartbeatRequest {
                    member_id: String::new(),
                    member_epoch: -1,
                    ..steady.clone()
                },
                invalid("MemberId can't be empty."),
            ),
            (
                "empty group id",
                1,
                ConsumerGroupHeartbeatRequest {
                    group_id: String::new(),
                    ..join.clone()
                },
                invalid("GroupId can't be empty."),
            ),
            (
                "empty instance id",
                1,
                ConsumerGroupHeartbeatRequest {
                    instance_id: Some(String::new()),
                    ..join.clone()
                },
                invalid("InstanceId can't be empty."),
            ),
            (
                "empty rack id",
                1,
                ConsumerGroupHeartbeatRequest {
                    rack_id: Some(String::new()),
                    ..join.clone()
                },
                invalid("RackId can't be empty."),
            ),
            (
                "join without rebalance timeout",
                1,
                ConsumerGroupHeartbeatRequest {
                    rebalance_timeout_ms: -1,
                    ..join.clone()
                },
                invalid("RebalanceTimeoutMs must be provided in first request."),
            ),
            (
                "join with null owned partitions",
                1,
                ConsumerGroupHeartbeatRequest {
                    topic_partitions: None,
                    ..join.clone()
                },
                invalid("TopicPartitions must be empty when (re-)joining."),
            ),
            (
                "join with owned partitions",
                1,
                ConsumerGroupHeartbeatRequest {
                    topic_partitions: Some(vec![TopicPartitions::default()]),
                    ..join.clone()
                },
                invalid("TopicPartitions must be empty when (re-)joining."),
            ),
            (
                "join without a subscription",
                1,
                ConsumerGroupHeartbeatRequest {
                    subscribed_topic_names: None,
                    ..join.clone()
                },
                invalid(
                    "Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when \
                     (re-)joining.",
                ),
            ),
            (
                "static leave without instance id",
                1,
                ConsumerGroupHeartbeatRequest {
                    member_epoch: -2,
                    ..steady.clone()
                },
                invalid("InstanceId can't be null."),
            ),
            (
                "epoch below -2",
                1,
                ConsumerGroupHeartbeatRequest {
                    member_epoch: -3,
                    ..steady.clone()
                },
                invalid("MemberEpoch is invalid."),
            ),
            (
                "unknown assignor",
                1,
                ConsumerGroupHeartbeatRequest {
                    server_assignor: Some("sticky".into()),
                    ..join.clone()
                },
                Some(ConsumerGroupHeartbeatResponse {
                    error_code: codes::UNSUPPORTED_ASSIGNOR,
                    error_message: Some(
                        "ServerAssignor sticky is not supported. Supported assignors: uniform, \
                         range."
                            .into(),
                    ),
                    ..Default::default()
                }),
            ),
        ];
        let config = crate::coordinator::unified::config::NextGenConfig::default();
        for (label, version, request, want) in rows {
            let got = validate_request(&request, version, &config)
                .err()
                .map(|e| *e);
            assert!(got == want, "{label}");
        }
    }

    #[test]
    fn error_response_preserves_error_code() {
        let resp = error(codes::GROUP_AUTHORIZATION_FAILED);
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }

    use super::*;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        use krabka_protocol::owned::consumer_group_heartbeat_response::{
            self, ConsumerGroupHeartbeatResponse,
        };

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = crate::test_support::principal("ANONYMOUS");
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));

        let ctx = crate::test_support::request_context(&principal, &peer, "consumer-client");

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

        let bytes = crate::handlers::encode_response(
            &error(codes::GROUP_AUTHORIZATION_FAILED),
            consumer_group_heartbeat_response::MAX_VERSION,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = ConsumerGroupHeartbeatResponse::decode(
            &mut cur,
            consumer_group_heartbeat_response::MAX_VERSION,
        )
        .unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }

    #[test]
    fn group_read_denied_allows_allow_all_authorizer() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = anonymous_principal();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "consumer-client");

        assert!(!group_read_denied(
            &crate::authorizer::AllowAllAuthorizer,
            &image,
            &ctx,
            "g"
        ));
    }

    /// Finalizes `group.version >= 1`, the [`group_version_disabled`] gate.
    /// Every test below the protocol-gate reordering needs this first, or
    /// the protocol gate — now checked *before* the group ACL — answers
    /// `UNSUPPORTED_VERSION` before the behavior under test ever runs.
    async fn finalize_group_version(broker: &crate::broker::Broker) {
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: krabka_metadata::group_version::GROUP_VERSION_FEATURE.into(),
                level: 1,
            })])
            .await
            .expect("finalize group.version");
    }

    #[tokio::test]
    async fn handle_group_read_denied_preserves_error_response() {
        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let (broker_handle, _dir) = start_broker(Arc::new(authorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_group_version(&broker).await;
        let principal = anonymous_principal();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = test_context(&principal, &peer);
        let req = request("denied-group");

        let bytes = handle(&broker, VERSION, 5, &req, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        let resp = decode_response(&bytes);

        assert!(
            resp.error_code == codes::GROUP_AUTHORIZATION_FAILED,
            "{resp:?}"
        );

        broker_handle.shutdown().await;
    }

    /// The protocol gate — `UNSUPPORTED_VERSION` when `group.version` is not
    /// finalized — runs BEFORE the group ACL check, matching Kafka's
    /// `handleConsumerGroupHeartbeat`. A principal denied `Read` on the group
    /// still gets `UNSUPPORTED_VERSION`, not `GROUP_AUTHORIZATION_FAILED`,
    /// when the protocol itself is unavailable.
    #[tokio::test]
    async fn handle_protocol_gate_precedes_group_acl() {
        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let (broker_handle, _dir) = start_broker(Arc::new(authorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        // `start_broker`'s bootstrap seeds every feature at its release
        // default for a modern metadata.version, which finalizes
        // group.version >= 1 automatically. Explicitly downgrade it back to
        // 0 (unfinalized/disabled) so this test observes the protocol gate.
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: krabka_metadata::group_version::GROUP_VERSION_FEATURE.into(),
                level: 0,
            })])
            .await
            .expect("disable group.version");
        let principal = anonymous_principal();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = test_context(&principal, &peer);
        let req = request("denied-group");

        let bytes = handle(&broker, VERSION, 5, &req, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        let resp = decode_response(&bytes);

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_persists_request_client_identity() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_group_version(&broker).await;
        let principal = anonymous_principal();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = test_context(&principal, &peer);

        let bytes = handle(&broker, VERSION, 5, &request("identity-group"), &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        assert!(decode_response(&bytes).error_code == 0);

        let actor = broker
            .group_coordinator
            .get_or_create_group("identity-group", GroupKindTag::Consumer);
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe consumer group");
        let view = rx.await.expect("consumer group view");

        assert!(view.members.len() == 1);
        assert!(view.members[0].client_id == "consumer-group-heartbeat-test");
        assert!(view.members[0].client_host == "/127.0.0.1");

        let member_id = view.members[0].member_id.clone();
        let member_epoch = view.members[0].member_epoch;
        let peer = std::net::SocketAddr::from(([127, 0, 0, 2], 9093));
        let ctx = crate::test_support::request_context(&principal, &peer, "consumer-client-b");
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "identity-group".into(),
            member_id,
            member_epoch,
            rebalance_timeout_ms: 30_000,
            topic_partitions: Some(vec![]),
            subscribed_topic_names: Some(vec!["topic-a".into()]),
            ..Default::default()
        };
        let req = crate::test_support::encode_request(&req, VERSION);

        let bytes = handle(&broker, VERSION, 6, &req, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat identity refresh");
        assert!(decode_response(&bytes).error_code == 0);

        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe refreshed consumer group");
        let view = rx.await.expect("refreshed consumer group view");
        assert!(view.members[0].client_id == "consumer-client-b");
        assert!(view.members[0].client_host == "/127.0.0.2");

        broker_handle.shutdown().await;
    }

    /// Kafka's `getOrMaybeCreateConsumerGroup` and `consumerGroupLeave`: only a
    /// join creates a consumer group, a missing group answers
    /// `GROUP_ID_NOT_FOUND` on any other epoch, and a share or streams group is
    /// not a consumer group. Each row sends one heartbeat for its own group id
    /// and compares the whole response; `None` expects an accepted join.
    #[tokio::test]
    async fn handle_creates_consumer_group_only_on_join_as_kafka_does() {
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_group_version(&broker).await;
        let coordinator = &broker.group_coordinator;
        coordinator.mark_share("share");
        coordinator.mark_streams("streams");
        let _share_actor = coordinator.get_or_create_share("share-actor");
        let principal = anonymous_principal();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = test_context(&principal, &peer);
        let not_found = |message: &str| {
            Some(ConsumerGroupHeartbeatResponse {
                error_code: codes::GROUP_ID_NOT_FOUND,
                error_message: Some(message.into()),
                ..Default::default()
            })
        };
        // (group id, member epoch, expected response)
        let rows = [
            (
                "absent-heartbeat",
                3,
                not_found("Consumer group absent-heartbeat not found."),
            ),
            (
                "absent-leave",
                -1,
                not_found("Group absent-leave not found."),
            ),
            (
                "absent-static-leave",
                -2,
                not_found("Group absent-static-leave not found."),
            ),
            (
                "share",
                0,
                not_found("Group share is not a consumer group."),
            ),
            (
                "share-actor",
                0,
                not_found("Group share-actor is not a consumer group."),
            ),
            (
                "streams",
                3,
                not_found("Group streams is not a consumer group."),
            ),
            (
                "streams",
                -1,
                not_found("Group streams is not a consumer group."),
            ),
            ("joined", 0, None),
        ];

        for (group_id, member_epoch, expected) in rows {
            let req = crate::test_support::encode_request(
                &ConsumerGroupHeartbeatRequest {
                    group_id: group_id.into(),
                    member_id: "m1".into(),
                    instance_id: (member_epoch == -2).then(|| "i1".into()),
                    member_epoch,
                    rebalance_timeout_ms: if member_epoch == 0 { 30_000 } else { -1 },
                    topic_partitions: (member_epoch == 0).then(Vec::new),
                    subscribed_topic_names: Some(vec!["topic-a".into()]),
                    ..Default::default()
                },
                VERSION,
            );
            let bytes = handle(&broker, VERSION, 1, &req, &ctx)
                .await
                .expect("ConsumerGroupHeartbeat handler");
            let resp = decode_response(&bytes);
            match expected {
                Some(expected) => assert!(resp == expected, "{group_id}: {resp:?}"),
                None => assert!(resp.error_code == codes::NONE, "{group_id}: {resp:?}"),
            }
        }

        // Only the join left a consumer group behind.
        assert!(coordinator.find("joined").is_some());
        for group_id in [
            "absent-heartbeat",
            "absent-leave",
            "absent-static-leave",
            "share",
        ] {
            assert!(coordinator.find(group_id).is_none(), "{group_id}");
        }
        broker_handle.shutdown().await;
    }

    // ── Explicit-name and regex `Describe` checks (issue #716) ─────────

    use crate::handlers::group_heartbeat_test_support::{
        alice, describe_acl, group_read_acl, topic_with_partitions,
    };

    /// Table-driven cases for
    /// [`crate::handlers::subscribed_names_describe_denied`]: whether each ACL
    /// configuration over `subscribed_topic_names` denies the whole heartbeat,
    /// per Kafka's `filterByAuthorized(.., DESCRIBE, TOPIC, ..)`.
    #[tokio::test]
    async fn subscribed_names_describe_denied_table() {
        for (label, granted, names, expected_denied) in [
            ("no subscription", vec![], None, false),
            ("empty subscription", vec![], Some(vec![]), false),
            (
                "single name, fully authorized",
                vec!["orders"],
                Some(vec!["orders"]),
                false,
            ),
            (
                "single name, not authorized",
                vec![],
                Some(vec!["orders"]),
                true,
            ),
            (
                "two names, one denied",
                vec!["orders"],
                Some(vec!["orders", "shipments"]),
                true,
            ),
            (
                "two names, both authorized",
                vec!["orders", "shipments"],
                Some(vec!["orders", "shipments"]),
                false,
            ),
        ] {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            for name in &granted {
                image.apply(&describe_acl(name));
            }
            let (broker_handle, _dir) = start_broker(Arc::new(
                crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
            ))
            .await;
            let broker = broker_handle.broker_arc_for_test();
            let principal = alice();
            let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
            let ctx = crate::test_support::request_context(&principal, &peer, "c");
            let req = ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                subscribed_topic_names: names.map(|ns| ns.into_iter().map(String::from).collect()),
                ..Default::default()
            };

            assert!(
                crate::handlers::subscribed_names_describe_denied(
                    broker.config.authorizer.as_ref(),
                    &image,
                    &ctx,
                    req.subscribed_topic_names.as_deref(),
                ) == expected_denied,
                "{label}"
            );
            broker_handle.shutdown().await;
        }
    }

    /// A `SubscribedTopicNames` entry this principal cannot `Describe` fails
    /// the whole heartbeat with `TOPIC_AUTHORIZATION_FAILED` (29), and the
    /// group is left with no member — Kafka never builds the coordinator
    /// record in this case, so an attacker cannot use group membership to
    /// learn a denied topic's id or partitions.
    #[tokio::test]
    async fn handle_subscribed_name_describe_denied_refuses_whole_heartbeat_no_member_created() {
        // Deliberately no Describe grant for "topic-a".
        let (broker_handle, _dir) = start_broker(Arc::new(
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
        ))
        .await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_group_version(&broker).await;
        broker
            .controller
            .submit_change(vec![group_read_acl("g")])
            .await
            .expect("grant group Read");
        let principal = alice();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "c");
        let req = crate::test_support::encode_request(
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                rebalance_timeout_ms: 30_000,
                topic_partitions: Some(vec![]),
                subscribed_topic_names: Some(vec!["topic-a".into()]),
                ..Default::default()
            },
            VERSION,
        );

        let bytes = handle(&broker, VERSION, 7, &req, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        let resp = decode_response(&bytes);
        assert!(
            resp.error_code == codes::TOPIC_AUTHORIZATION_FAILED,
            "{resp:?}"
        );

        let actor = broker
            .group_coordinator
            .get_or_create_group("g", GroupKindTag::Consumer);
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(GroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe consumer group");
        let view = rx.await.expect("consumer group view");
        assert!(view.members.is_empty(), "{view:?}");

        broker_handle.shutdown().await;
    }

    /// The security-fix regression case end to end: a regex subscription
    /// matches two topics, but the principal may only `Describe` one of
    /// them. The assignment the heartbeat returns holds only the authorized
    /// topic's partitions — Kafka's
    /// `TopicRegexResolver.filterTopicDescribeAuthorizedTopics`, exercised
    /// through the whole handler → actor → reconciler path.
    #[tokio::test]
    async fn handle_regex_subscription_assigns_only_describe_authorized_topics() {
        let (broker_handle, _dir) = start_broker(Arc::new(
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
        ))
        .await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_group_version(&broker).await;
        let allowed_id = uuid::Uuid::from_u128(1);
        let denied_id = uuid::Uuid::from_u128(2);
        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![group_read_acl("g"), describe_acl("orders-eu")];
        records.extend(topic_with_partitions("orders-eu", allowed_id, 2, node));
        records.extend(topic_with_partitions("orders-us", denied_id, 2, node));
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create topics");
        let principal = alice();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "c");
        let req = crate::test_support::encode_request(
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "regex-member".into(),
                rebalance_timeout_ms: 30_000,
                topic_partitions: Some(vec![]),
                subscribed_topic_regex: Some("^orders-.*".into()),
                ..Default::default()
            },
            VERSION,
        );

        let bytes = handle(&broker, VERSION, 7, &req, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        let resp = decode_response(&bytes);
        assert!(resp.error_code == codes::NONE, "{resp:?}");
        let assigned: std::collections::HashSet<uuid::Uuid> = resp
            .assignment
            .into_iter()
            .flat_map(|a| a.topic_partitions)
            .map(|tp| uuid::Uuid::from_bytes(tp.topic_id.0))
            .collect();
        assert!(assigned == std::collections::HashSet::from([allowed_id]));

        broker_handle.shutdown().await;
    }

    // ── Regex resolution that follows the metadata ─────────────────────

    /// The topics a member of group `g` holds, as its heartbeats report them.
    ///
    /// The response carries the assignment only when it changed, so the set
    /// keeps the last one.
    struct RegexMember {
        broker: Arc<crate::broker::Broker>,
        member_id: &'static str,
        member_epoch: i32,
        assigned: std::collections::HashSet<uuid::Uuid>,
    }

    impl RegexMember {
        fn new(broker: Arc<crate::broker::Broker>, member_id: &'static str) -> Self {
            Self {
                broker,
                member_id,
                member_epoch: 0,
                assigned: std::collections::HashSet::new(),
            }
        }

        /// Sends one heartbeat as `alice`, with `regex` only when it is the
        /// join or when the pattern changes, as the Java client does.
        async fn heartbeat(&mut self, regex: Option<&str>) {
            let principal = alice();
            let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
            let ctx = crate::test_support::request_context(&principal, &peer, "c");
            let joining = self.member_epoch == 0;
            let req = crate::test_support::encode_request(
                &ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: self.member_id.into(),
                    member_epoch: self.member_epoch,
                    rebalance_timeout_ms: if joining { 30_000 } else { -1 },
                    topic_partitions: joining.then(Vec::new),
                    subscribed_topic_regex: regex.map(str::to_owned),
                    ..Default::default()
                },
                VERSION,
            );
            let bytes = handle(&self.broker, VERSION, 7, &req, &ctx)
                .await
                .expect("ConsumerGroupHeartbeat handler");
            let resp = decode_response(&bytes);
            assert!(resp.error_code == codes::NONE, "{resp:?}");
            self.member_epoch = resp.member_epoch;
            if let Some(assignment) = resp.assignment {
                self.assigned = assignment
                    .topic_partitions
                    .into_iter()
                    .map(|tp| uuid::Uuid::from_bytes(tp.topic_id.0))
                    .collect();
            }
        }

        /// Heartbeats without a pattern until the member holds exactly
        /// `expected`, or fails after ten seconds.
        async fn heartbeat_until_holding(&mut self, expected: &[uuid::Uuid]) {
            let expected: std::collections::HashSet<uuid::Uuid> =
                expected.iter().copied().collect();
            for _ in 0..200 {
                self.heartbeat(None).await;
                if self.assigned == expected {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            panic!(
                "member {} holds {:?} and never held {expected:?}",
                self.member_id, self.assigned
            );
        }
    }

    /// A broker whose group `g` and topics `topics` (each with two partitions
    /// and a `Describe` grant for alice) exist, and the broker's handle.
    async fn broker_with_described_topics(
        topics: &[(&str, uuid::Uuid)],
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        let (broker_handle, dir) = start_broker(Arc::new(
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
        ))
        .await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_group_version(&broker).await;
        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![group_read_acl("g")];
        for (name, id) in topics {
            records.push(describe_acl(name));
            records.extend(topic_with_partitions(name, *id, 2, node));
        }
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create topics");
        (broker_handle, dir)
    }

    /// Kafka refreshes the resolution of a group's regular expressions when the
    /// metadata image has new topics (`lastMetadataImageWithNewTopics`), at the
    /// next heartbeat of any member. A Java client sends its pattern only when
    /// it joins, so a topic created after the join reaches the member only
    /// through that refresh.
    #[tokio::test]
    async fn a_topic_created_after_the_join_is_assigned_to_a_member_that_never_resends_its_pattern()
    {
        let first = uuid::Uuid::from_u128(1);
        let second = uuid::Uuid::from_u128(2);
        let (broker_handle, _dir) = broker_with_described_topics(&[("orders-eu", first)]).await;
        let broker = broker_handle.broker_arc_for_test();
        let mut member = RegexMember::new(broker.clone(), "m1");

        member.heartbeat(Some("^orders-.*")).await;
        assert!(member.assigned == std::collections::HashSet::from([first]));

        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![describe_acl("orders-us")];
        records.extend(topic_with_partitions("orders-us", second, 2, node));
        broker
            .controller
            .submit_change(records)
            .await
            .expect("create a second topic");

        member.heartbeat_until_holding(&[first, second]).await;
        broker_handle.shutdown().await;
    }

    /// The resolution is made with the `Describe` grants of the heartbeat that
    /// refreshes it, so a grant that is revoked takes the topic from the member
    /// at a later heartbeat, again without the pattern.
    #[tokio::test]
    async fn revoking_describe_on_a_resolved_topic_removes_it_at_a_later_heartbeat() {
        let allowed = uuid::Uuid::from_u128(1);
        let revoked = uuid::Uuid::from_u128(2);
        let (broker_handle, _dir) =
            broker_with_described_topics(&[("orders-eu", allowed), ("orders-us", revoked)]).await;
        let broker = broker_handle.broker_arc_for_test();
        let mut member = RegexMember::new(broker.clone(), "m1");

        member.heartbeat(Some("^orders-.*")).await;
        assert!(member.assigned == std::collections::HashSet::from([allowed, revoked]));

        broker
            .controller
            .submit_change(vec![MetadataRecord::V1DeleteAccessControlEntry(
                krabka_metadata::AclEntryFilter {
                    resource_type: Some(krabka_metadata::ResourceType::Topic),
                    resource_name: Some("orders-us".into()),
                    ..Default::default()
                },
            )])
            .await
            .expect("revoke the grant");

        member.heartbeat_until_holding(&[allowed]).await;
        broker_handle.shutdown().await;
    }
}
