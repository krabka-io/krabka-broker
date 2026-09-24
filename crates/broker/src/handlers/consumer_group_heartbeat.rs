//! `ConsumerGroupHeartbeat` (`api_key` 68), from the KIP-848 next-gen consumer
//! group protocol. It routes the request to the per-group actor in
//! `GroupCoordinator`.

use std::collections::HashSet;

use bytes::Bytes;
use krabka_metadata::AclOperation;
use krabka_protocol::{
    Decode,
    owned::{
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    },
};
use tokio::sync::oneshot;

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::unified::actor::{GroupActorMessage, GroupKindTag},
    error::BrokerError,
    handlers::group_read_denied,
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
    let image = broker.controller.current_image();
    {
        let mut cur: &[u8] = req_bytes;
        let req = ConsumerGroupHeartbeatRequest::decode(&mut cur, version)?;

        // ── Protocol gate ───────────────────────────────────────────
        // Kafka's `handleConsumerGroupHeartbeat` checks whether the
        // consumer-group protocol is available BEFORE any ACL check, so a
        // disabled protocol answers `UNSUPPORTED_VERSION` even to a caller
        // with no ACLs on the group at all. KIP-848 / KIP-584: the next-gen
        // protocol is gated on a finalized group.version >= 1. Below that —
        // including UNFINALIZED, which means disabled — reject so the client
        // falls back to the classic protocol.
        if group_version_disabled(&image) {
            return crate::handlers::encode_response(&error(codes::UNSUPPORTED_VERSION), version);
        }

        if next_gen_config_disabled(coordinator.config.next_gen_enabled()) {
            return crate::handlers::encode_response(&error(codes::GROUP_ID_NOT_FOUND), version);
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

        // `Describe` on every distinct name in `subscribed_topic_names`
        // (Kafka's `filterByAuthorized(request.context, DESCRIBE, TOPIC,
        // subscribedTopicSet)`). Any denial fails the WHOLE heartbeat with
        // `TOPIC_AUTHORIZATION_FAILED` (29); the group is never touched, so an
        // unauthorized caller cannot learn the denied topic's id or
        // partitions by being admitted as a member. This runs before
        // `group_coordinator_error` -- Kafka authorizes the request before it
        // ever reaches coordinator routing, so an unauthorized subscription
        // must not be masked by `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE`.
        if subscribed_names_describe_denied(broker, &image, ctx, &req) {
            return crate::handlers::encode_response(
                &error(codes::TOPIC_AUTHORIZATION_FAILED),
                version,
            );
        }

        // `subscribed_topic_regex` (KIP-848 v1+): resolve it against every
        // topic the image currently knows about and precompute the subset of
        // matches this principal may `Describe` right now. The actor stores
        // this set on the member and the reconciler ANDs it against the live
        // regex match before assignment — Kafka's
        // `TopicRegexResolver.filterTopicDescribeAuthorizedTopics`. A pattern
        // that fails to compile here is left alone: the actor's own
        // `check_subscribed_topic_regex` rejects it with
        // `INVALID_REGULAR_EXPRESSION` before any member state changes.
        let regex_authorized_topics =
            regex_subscription_describe_authorized(broker, &image, ctx, &req);

        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
            return crate::handlers::encode_response(&error(error_code), version);
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
                client_id: ctx.client_id.to_owned(),
                client_host: ctx.client_host(),
                regex_authorized_topics,
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
        let resp = rx
            .await
            .unwrap_or_else(|_| error(codes::UNKNOWN_SERVER_ERROR));
        crate::handlers::encode_response(&resp, version)
    }
}

fn group_version_disabled(image: &krabka_metadata::MetadataImage) -> bool {
    !crate::features::feature_enabled(
        image,
        krabka_metadata::group_version::GROUP_VERSION_FEATURE,
        1,
    )
}

fn next_gen_config_disabled(next_gen_enabled: bool) -> bool {
    !next_gen_enabled
}

/// `true` when `req.subscribed_topic_names` is non-empty and at least one of
/// its distinct names is `Describe`-denied for `ctx.principal`. A `None` or
/// empty list means Kafka's `subscribedTopicSet` is empty, which is
/// vacuously fully authorized.
fn subscribed_names_describe_denied(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    req: &ConsumerGroupHeartbeatRequest,
) -> bool {
    let Some(names) = req.subscribed_topic_names.as_ref() else {
        return false;
    };
    if names.is_empty() {
        return false;
    }
    let unique: HashSet<&str> = names.iter().map(String::as_str).collect();
    authorize_topics(
        broker.config.authorizer.as_ref(),
        image,
        ctx.principal,
        ctx.peer,
        AclOperation::Describe,
        unique,
    )
    .into_values()
    .any(|result| result == AuthorizationResult::Deny)
}

/// A cached result of [`regex_subscription_describe_authorized`], keyed by
/// `(group_id, member_id, principal)` in
/// [`crate::coordinator::unified::GroupCoordinator::regex_authz_cache`].
///
/// `metadata_offset` is `broker.controller.current_metadata_offset()` at the
/// time `authorized` was computed. Every metadata-log record — a topic
/// create/delete or an ACL change alike — advances that offset, so comparing
/// it on the next heartbeat is a cheap, sufficient test for "could the
/// authorized set possibly be different now". A cache hit therefore requires
/// both the pattern and the offset to match; either one to change forces a
/// fresh, fail-closed recompute.
#[derive(Debug, Clone)]
pub(crate) struct RegexAuthzCacheEntry {
    pattern: String,
    metadata_offset: i64,
    authorized: HashSet<String>,
}

/// Resolves `req.subscribed_topic_regex` against every topic name `image`
/// currently knows about, and returns the subset of matches that
/// `ctx.principal` may `Describe` right now.
///
/// The reconciler ANDs a live regex match against this set (fail-closed): a
/// topic this call never authorized is excluded even if it starts matching
/// later, whether because it is new (a race between this snapshot and the
/// actor's own, later `MetadataProvider` read) or because the member's state
/// was rebuilt from a raft seed that carries no authorization decision at
/// all (`apply_seed`). Both cases default to "not yet authorized" rather
/// than "not yet denied".
///
/// Returns an empty set when there is no pattern or it fails to compile — a
/// heartbeat with a pattern that does not compile never reaches assignment:
/// the actor's own `check_subscribed_topic_regex` rejects it with
/// `INVALID_REGULAR_EXPRESSION` first, so no filtering is needed here.
/// `Some("")` is a pattern like any other: `Regex::new("")` compiles and
/// matches every topic name, so it goes through the same authorization walk
/// as any other pattern rather than being read as "no regex".
///
/// This recomputes only when something relevant to the decision could have
/// changed since the last heartbeat from the same `(group_id, member_id,
/// principal)`: the pattern itself, or `broker.controller.current_metadata_offset()`
/// (which advances on every topic and every ACL change). Otherwise it reuses
/// the cached result, at [`RegexAuthzCacheEntry`], with no call to the
/// authorizer at all — the expensive part on a remote/OPA-backed
/// `Authorizer`, and one that would otherwise run on every heartbeat a
/// regex-subscribed member sends.
fn regex_subscription_describe_authorized(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    req: &ConsumerGroupHeartbeatRequest,
) -> HashSet<String> {
    let Some(pattern) = req.subscribed_topic_regex.as_deref() else {
        broker
            .group_coordinator
            .regex_authz_cache
            .remove(&cache_key(req, ctx));
        return HashSet::new();
    };

    let cache_key = cache_key(req, ctx);
    let metadata_offset = broker.controller.current_metadata_offset();
    if let Some(cached) = broker.group_coordinator.regex_authz_cache.get(&cache_key)
        && cached.pattern == pattern
        && cached.metadata_offset == metadata_offset
    {
        return cached.authorized.clone();
    }

    let Ok(re) = regex::Regex::new(pattern) else {
        return HashSet::new();
    };
    let matched: Vec<&str> = image
        .topics()
        .map(|topic| topic.name.as_str())
        .filter(|name| re.is_match(name))
        .collect();
    let authorized: HashSet<String> = if matched.is_empty() {
        HashSet::new()
    } else {
        authorize_topics(
            broker.config.authorizer.as_ref(),
            image,
            ctx.principal,
            ctx.peer,
            AclOperation::Describe,
            matched,
        )
        .into_iter()
        .filter(|(_, result)| *result == AuthorizationResult::Allow)
        .map(|(name, _)| name.to_string())
        .collect()
    };

    broker.group_coordinator.regex_authz_cache.insert(
        cache_key,
        RegexAuthzCacheEntry {
            pattern: pattern.to_string(),
            metadata_offset,
            authorized: authorized.clone(),
        },
    );
    authorized
}

/// The [`GroupCoordinator::regex_authz_cache`] key for `req`'s member under
/// `ctx.principal`. Principal is part of the key, not just `(group_id,
/// member_id)`, because a first-join heartbeat may carry an empty
/// `member_id` (the raw-RPC fallback `first_join_member_id` mints a
/// server-side id for) — without the principal, two distinct callers racing
/// that fallback in the same group could otherwise read back each other's
/// cached authorization.
fn cache_key(
    req: &ConsumerGroupHeartbeatRequest,
    ctx: &crate::handlers::RequestContext<'_>,
) -> (String, String, String) {
    (
        req.group_id.clone(),
        req.member_id.clone(),
        ctx.principal.name.clone(),
    )
}

fn error(code: i16) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse {
        error_code: code,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use bytes::BytesMut;
    use krabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord};
    use krabka_protocol::Encode;

    const VERSION: i16 = krabka_protocol::owned::consumer_group_heartbeat_request::MAX_VERSION;

    fn request(group_id: &str) -> Bytes {
        let req = ConsumerGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
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

    use crate::test_support::start_broker_with_authorizer as start_broker;

    fn image_with_group_version(level: i16) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: krabka_metadata::group_version::GROUP_VERSION_FEATURE.into(),
            level,
        }));
        image
    }

    fn anonymous_principal() -> krabka_security::Principal {
        krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        }
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

    #[test]
    fn next_gen_config_gate_inverts_enabled_flag() {
        assert!(!next_gen_config_disabled(true));
        assert!(next_gen_config_disabled(false));
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
        let principal = krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
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
        // group.version is deliberately left UNFINALIZED.
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

    // ── Explicit-name and regex `Describe` checks (issue #716) ─────────

    fn describe_acl(name: &str) -> MetadataRecord {
        MetadataRecord::V1AccessControlEntry(krabka_metadata::AclEntry {
            resource_type: krabka_metadata::ResourceType::Topic,
            resource_name: name.into(),
            pattern_type: krabka_metadata::PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: krabka_metadata::AclOperation::Describe,
            permission_type: krabka_metadata::PermissionType::Allow,
        })
    }

    fn group_read_acl(name: &str) -> MetadataRecord {
        MetadataRecord::V1AccessControlEntry(krabka_metadata::AclEntry {
            resource_type: krabka_metadata::ResourceType::Group,
            resource_name: name.into(),
            pattern_type: krabka_metadata::PatternType::Literal,
            principal: "User:alice".into(),
            host: "*".into(),
            operation: krabka_metadata::AclOperation::Read,
            permission_type: krabka_metadata::PermissionType::Allow,
        })
    }

    fn topic_record(name: &str, topic_id: uuid::Uuid, partitions: i32) -> MetadataRecord {
        MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
            name: name.into(),
            topic_id,
            partitions,
            replication_factor: 1,
        })
    }

    /// `topic_record`'s `partitions` field is only the requested count: a
    /// `TopicRecord`'s `partitions` in the image is a derived cache that
    /// starts at 0 and is restored only by the `V1Partition` records that
    /// follow it in log order (`krabka_metadata::MetadataImage::apply`'s own
    /// doc comment on the `V1Topic` arm says so). A test that submits a bare
    /// `topic_record` and then expects the reconciler to assign the topic's
    /// partitions needs one of these per partition index too, or
    /// `ImageMetadataProvider::snapshot`'s `partitions_per_topic` reads back
    /// 0 and the assignor has nothing to hand out.
    fn partition_records(name: &str, partitions: i32) -> Vec<MetadataRecord> {
        (0..partitions)
            .map(|partition| {
                MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                    topic: name.into(),
                    partition,
                    leader: krabka_metadata::NodeId(1),
                    replicas: vec![krabka_metadata::NodeId(1)],
                    isr: vec![krabka_metadata::NodeId(1)],
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 0,
                })
            })
            .collect()
    }

    fn alice() -> krabka_security::Principal {
        krabka_security::Principal {
            name: "alice".into(),
            auth_method: krabka_security::AuthMethod::SaslPlain,
            groups: vec![],
        }
    }

    /// Table-driven cases for [`subscribed_names_describe_denied`]: whether
    /// each ACL configuration over `subscribed_topic_names` denies the whole
    /// heartbeat, per Kafka's `filterByAuthorized(.., DESCRIBE, TOPIC, ..)`.
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
                subscribed_names_describe_denied(&broker, &image, &ctx, &req) == expected_denied,
                "{label}"
            );
            broker_handle.shutdown().await;
        }
    }

    /// `Some("")` is a valid regex that matches every topic name, not "no
    /// regex" -- it must go through the same Describe authorization walk as
    /// any other pattern. This is the #716 regression case: treating an
    /// empty pattern as absent skipped authorization entirely and returned
    /// every topic as a free pass.
    #[tokio::test]
    async fn regex_subscription_describe_authorized_treats_empty_pattern_as_a_real_regex() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&describe_acl("orders"));
        image.apply(&topic_record("orders", uuid::Uuid::from_u128(1), 1));
        image.apply(&topic_record("payments", uuid::Uuid::from_u128(2), 1));
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
            subscribed_topic_regex: Some(String::new()),
            ..Default::default()
        };

        let authorized = regex_subscription_describe_authorized(&broker, &image, &ctx, &req);

        assert!(
            authorized == std::collections::HashSet::from(["orders".to_string()]),
            "{authorized:?}"
        );
        broker_handle.shutdown().await;
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
        let mut records = vec![
            group_read_acl("g"),
            describe_acl("orders-eu"),
            topic_record("orders-eu", allowed_id, 2),
            topic_record("orders-us", denied_id, 2),
        ];
        records.extend(partition_records("orders-eu", 2));
        records.extend(partition_records("orders-us", 2));
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
                rebalance_timeout_ms: 30_000,
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

    // ── Regex Describe-authorization caching (Codex P1 follow-up) ──────

    /// Wraps an authorizer and counts every [`crate::authorizer::Authorizer::authorize`]
    /// call, so a test can assert that a heartbeat did or did not reach the
    /// authorizer at all — the thing the cache in
    /// [`super::regex_subscription_describe_authorized`] exists to avoid on a
    /// heartbeat where nothing relevant changed.
    #[derive(Debug)]
    struct CountingAuthorizer {
        inner: crate::authorizer::SimpleAclAuthorizer,
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl crate::authorizer::Authorizer for CountingAuthorizer {
        fn authorize(
            &self,
            source: &dyn crate::authorizer::AclSource,
            req: &crate::authorizer::AuthorizationRequest<'_>,
        ) -> crate::authorizer::AuthorizationResult {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.authorize(source, req)
        }
    }

    /// (a) Two heartbeats with an unchanged `subscribed_topic_regex`, the same
    /// cluster topic set, and the same ACL state must call the authorizer only
    /// on the first of the two — the second is a pure cache hit in
    /// [`super::regex_subscription_describe_authorized`].
    #[tokio::test]
    async fn regex_subscription_describe_authorized_caches_across_unchanged_heartbeats() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let authorizer = Arc::new(CountingAuthorizer {
            inner: crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
            calls: calls.clone(),
        });
        let (broker_handle, _dir) = start_broker(authorizer).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_group_version(&broker).await;
        let mut records = vec![group_read_acl("g"), describe_acl("orders-eu")];
        records.push(topic_record("orders-eu", uuid::Uuid::from_u128(1), 2));
        records.extend(partition_records("orders-eu", 2));
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create topic");
        let principal = alice();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "c");

        // A real KIP-848 client generates and sends its own member id on the
        // very first heartbeat (`member_epoch: 0`), and keeps sending that
        // same id on every heartbeat after. Doing the same here, instead of
        // relying on the raw-RPC empty-id fallback, keeps the member id
        // stable across the two heartbeats below, so the cache key -- and
        // this test -- reflect a real steady-state client, not a client that
        // changes identity between its join and its very next heartbeat.
        let member_id = uuid::Uuid::new_v4().to_string();
        let req = crate::test_support::encode_request(
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.clone(),
                member_epoch: 0,
                rebalance_timeout_ms: 30_000,
                subscribed_topic_regex: Some("^orders-.*".into()),
                ..Default::default()
            },
            VERSION,
        );
        let before_first = calls.load(std::sync::atomic::Ordering::SeqCst);
        let bytes = handle(&broker, VERSION, 7, &req, &ctx)
            .await
            .expect("first heartbeat");
        let resp = decode_response(&bytes);
        assert!(resp.error_code == codes::NONE, "{resp:?}");
        // The first heartbeat must consult the authorizer for the `Group`
        // Read check (uncached, and not part of this fix) AND at least once
        // more for "orders-eu" under `subscribed_topic_regex`.
        let after_first = calls.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            after_first - before_first >= 2,
            "the first heartbeat must consult the authorizer for the regex match too: \
                before={before_first} after={after_first}"
        );

        // Steady-state heartbeat: same member, same epoch, same pattern,
        // nothing in the cluster or its ACLs has changed.
        let req2 = crate::test_support::encode_request(
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.clone(),
                member_epoch: resp.member_epoch,
                rebalance_timeout_ms: 30_000,
                subscribed_topic_regex: Some("^orders-.*".into()),
                ..Default::default()
            },
            VERSION,
        );
        let bytes2 = handle(&broker, VERSION, 8, &req2, &ctx)
            .await
            .expect("steady-state heartbeat");
        let resp2 = decode_response(&bytes2);
        assert!(resp2.error_code == codes::NONE, "{resp2:?}");
        // Only the uncached `Group` Read check calls the authorizer on an
        // unchanged heartbeat -- the regex cache hit adds zero calls.
        let after_second = calls.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            after_second - after_first == 1,
            "an unchanged heartbeat must not call the authorizer for the regex match again: \
                after_first={after_first} after_second={after_second}"
        );

        broker_handle.shutdown().await;
    }

    /// (b) A heartbeat sent after the cluster's topic set changes — here, a
    /// new topic starts matching the member's regex and its principal already
    /// holds `Describe` on it — must recompute (the authorizer is consulted
    /// again) and the new topic must show up in the very next assignment.
    #[tokio::test]
    async fn regex_subscription_describe_authorized_recomputes_after_topic_set_changes() {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let authorizer = Arc::new(CountingAuthorizer {
            inner: crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new()),
            calls: calls.clone(),
        });
        let (broker_handle, _dir) = start_broker(authorizer).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_group_version(&broker).await;
        let orders_eu = uuid::Uuid::from_u128(1);
        let orders_us = uuid::Uuid::from_u128(2);
        let mut records = vec![
            group_read_acl("g"),
            // Describe is granted for both names up front; only
            // "orders-us"'s TOPIC record is missing so far, so the regex
            // cannot yet match it.
            describe_acl("orders-eu"),
            describe_acl("orders-us"),
            topic_record("orders-eu", orders_eu, 2),
        ];
        records.extend(partition_records("orders-eu", 2));
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create the first topic");
        let principal = alice();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "c");

        // A real KIP-848 client keeps the same member id across heartbeats
        // (see the analogous comment in the caching test above), so pin one
        // down here too instead of taking the raw-RPC empty-id fallback.
        let member_id = uuid::Uuid::new_v4().to_string();
        let req = crate::test_support::encode_request(
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.clone(),
                member_epoch: 0,
                rebalance_timeout_ms: 30_000,
                subscribed_topic_regex: Some("^orders-.*".into()),
                ..Default::default()
            },
            VERSION,
        );
        let bytes = handle(&broker, VERSION, 7, &req, &ctx)
            .await
            .expect("first heartbeat");
        let resp = decode_response(&bytes);
        assert!(resp.error_code == codes::NONE, "{resp:?}");
        let assigned: std::collections::HashSet<uuid::Uuid> = resp
            .assignment
            .clone()
            .into_iter()
            .flat_map(|a| a.topic_partitions)
            .map(|tp| uuid::Uuid::from_bytes(tp.topic_id.0))
            .collect();
        assert!(
            assigned == std::collections::HashSet::from([orders_eu]),
            "{assigned:?}"
        );
        let calls_before_new_topic = calls.load(std::sync::atomic::Ordering::SeqCst);

        // The cluster's topic set changes: "orders-us" now exists, and this
        // principal's Describe grant already covers it.
        let mut new_topic_records = vec![topic_record("orders-us", orders_us, 2)];
        new_topic_records.extend(partition_records("orders-us", 2));
        broker
            .controller
            .submit_change(new_topic_records)
            .await
            .expect("create the second topic");

        let req2 = crate::test_support::encode_request(
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.clone(),
                member_epoch: resp.member_epoch,
                rebalance_timeout_ms: 30_000,
                subscribed_topic_regex: Some("^orders-.*".into()),
                ..Default::default()
            },
            VERSION,
        );
        let bytes2 = handle(&broker, VERSION, 8, &req2, &ctx)
            .await
            .expect("heartbeat after the topic-set change");
        let resp2 = decode_response(&bytes2);
        assert!(resp2.error_code == codes::NONE, "{resp2:?}");
        assert!(
            calls.load(std::sync::atomic::Ordering::SeqCst) > calls_before_new_topic,
            "a topic-set change must force a fresh authorizer consult"
        );
        let assigned2: std::collections::HashSet<uuid::Uuid> = resp2
            .assignment
            .into_iter()
            .flat_map(|a| a.topic_partitions)
            .map(|tp| uuid::Uuid::from_bytes(tp.topic_id.0))
            .collect();
        assert!(
            assigned2 == std::collections::HashSet::from([orders_eu, orders_us]),
            "{assigned2:?}"
        );

        broker_handle.shutdown().await;
    }
}
