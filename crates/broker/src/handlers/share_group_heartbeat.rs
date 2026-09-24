//! `ShareGroupHeartbeat` (`api_key` 76), KIP-932 share-group membership. The
//! handler routes the request to the per-group share actor in
//! `GroupCoordinator`.

use std::collections::HashSet;

use bytes::Bytes;
use krabka_metadata::AclOperation;
use krabka_protocol::{
    Decode,
    owned::{
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
};
use tokio::sync::oneshot;

use crate::{
    authorizer::{AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    coordinator::unified::share::actor::ShareGroupActorMessage,
    error::BrokerError,
    handlers::group_read_denied,
};

#[tracing::instrument(
    name = "handle_share_group_heartbeat",
    level = "info",
    skip_all,
    fields(api = "ShareGroupHeartbeat", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let share_enabled = broker.config.share_group.enable;
    let ng = broker.group_coordinator.clone();
    {
        let mut cur: &[u8] = req_bytes;
        let req = ShareGroupHeartbeatRequest::decode(&mut cur, version)?;

        // ── Protocol gate ───────────────────────────────────────────
        // Kafka's `handleShareGroupHeartbeat` checks whether share groups are
        // enabled BEFORE any ACL check, so a disabled feature answers
        // `UNSUPPORTED_VERSION` even to a caller with no ACLs on the group at
        // all.
        if !share_enabled {
            return crate::handlers::encode_response(&error(codes::UNSUPPORTED_VERSION), version);
        }

        // ── ACL preamble ────────────────────────────────────────────
        // KIP-932 share groups still gate membership on `Read` on
        // `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
        let image = broker.controller.current_image();
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

        // ── Malformed-request check ────────────────────────────────
        // Kafka's `ShareGroupHeartbeatRequestManager` validates the request
        // shape -- a non-zero `member_epoch` (rejoin, steady-state, or leave)
        // must carry a non-empty `member_id` -- between the group ACL check
        // and topic filtering. Only `member_epoch == 0` (first join) allows
        // an empty id, which the actor mints a fresh member id for. Running
        // this before `subscribed_names_describe_denied` matters: a malformed
        // request with a Describe-denied topic must answer `INVALID_REQUEST`,
        // not `TOPIC_AUTHORIZATION_FAILED`.
        if req.member_epoch != 0 && req.member_id.is_empty() {
            return crate::handlers::encode_response(&error(codes::INVALID_REQUEST), version);
        }

        // `Describe` on every distinct name in `subscribed_topic_names`
        // (Kafka's `filterByAuthorized(request.context, DESCRIBE, TOPIC,
        // subscribedTopicSet)`). Any denial fails the WHOLE heartbeat with
        // `TOPIC_AUTHORIZATION_FAILED` (29); the group is never touched, so an
        // unauthorized caller cannot learn a denied topic's id or partitions
        // by being admitted as a member. This runs before
        // `group_coordinator_error` -- Kafka authorizes the request before it
        // ever reaches coordinator routing, so an unauthorized subscription
        // must not be masked by `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE`.
        if subscribed_names_describe_denied(broker, &image, ctx, &req) {
            return crate::handlers::encode_response(
                &error(codes::TOPIC_AUTHORIZATION_FAILED),
                version,
            );
        }

        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
            return crate::handlers::encode_response(&error(error_code), version);
        }

        ng.mark_share(&req.group_id);
        let handle = ng.get_or_create_share(&req.group_id);
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(ShareGroupActorMessage::Heartbeat {
                request: req,
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
        let resp = rx
            .await
            .unwrap_or_else(|_| error(codes::UNKNOWN_SERVER_ERROR));
        crate::handlers::encode_response(&resp, version)
    }
}

/// `true` when `req.subscribed_topic_names` is non-empty and at least one of
/// its distinct names is `Describe`-denied for `ctx.principal`. A `None` or
/// empty list means Kafka's `subscribedTopicSet` is empty, which is
/// vacuously fully authorized.
///
/// Unlike `ConsumerGroupHeartbeat`, KIP-932's `ShareGroupHeartbeatRequest`
/// carries no `SubscribedTopicRegex` field, so there is no regex-subscription
/// counterpart to authorize here.
fn subscribed_names_describe_denied(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    req: &ShareGroupHeartbeatRequest,
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

fn error(code: i16) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse {
        error_code: code,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_metadata::{MetadataImage, MetadataRecord};
    use krabka_protocol::{UnknownTaggedFields, owned::share_group_heartbeat_response};
    use krabka_security::{AuthMethod, Principal};

    use super::*;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        use krabka_protocol::owned::share_group_heartbeat_response::{
            self, ShareGroupHeartbeatResponse,
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

        let ctx = crate::test_support::request_context(&principal, &peer, "share-client");

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));
        assert!(!group_read_denied(
            &crate::authorizer::AllowAllAuthorizer,
            &image,
            &ctx,
            "g"
        ));

        let bytes = crate::handlers::encode_response(
            &error(codes::GROUP_AUTHORIZATION_FAILED),
            share_group_heartbeat_response::MAX_VERSION,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = ShareGroupHeartbeatResponse::decode(
            &mut cur,
            share_group_heartbeat_response::MAX_VERSION,
        )
        .unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
        assert!(cur.is_empty(), "response decoder consumed all bytes");
    }

    crate::test_support::wire_helpers!(
        ShareGroupHeartbeatRequest,
        ShareGroupHeartbeatResponse,
        version = share_group_heartbeat_response::MAX_VERSION,
        client_id = "client-a"
    );

    use crate::test_support::start_broker_with_authorizer as start_broker;

    fn anonymous_principal() -> Principal {
        Principal {
            name: "ANONYMOUS".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        }
    }

    fn alice() -> Principal {
        Principal {
            name: "alice".into(),
            auth_method: AuthMethod::SaslPlain,
            groups: vec![],
        }
    }

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

    /// A `V1Topic` record plus one `V1Partition` per index, assigned to
    /// `node`. The KIP-631 wire framing does not carry `TopicRecord.partitions`
    /// -- a decoded `V1Topic` round-trips back at `partitions == 0`, and the
    /// real count comes from the `V1Partition` records that follow it -- so a
    /// topic meant to be assignable needs both, unlike [`topic_record`] alone
    /// (used only where a test never reaches the assignor).
    fn topic_with_partitions(
        name: &str,
        topic_id: uuid::Uuid,
        partitions: i32,
        node: krabka_raft::NodeId,
    ) -> Vec<MetadataRecord> {
        let replicas = vec![node];
        let mut records = vec![topic_record(name, topic_id, partitions)];
        records.extend((0..partitions).map(|partition| {
            MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                topic: name.into(),
                partition,
                leader: node,
                replicas: replicas.clone(),
                isr: replicas.clone(),
                leader_epoch: krabka_metadata::LeaderEpoch(0),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![],
                partition_epoch: 0,
            })
        }));
        records
    }

    fn request(group_id: &str, subscribed: Vec<&str>) -> ShareGroupHeartbeatRequest {
        ShareGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(subscribed.into_iter().map(String::from).collect()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn handle_disabled_feature_returns_unsupported_version() {
        let version = share_group_heartbeat_response::MAX_VERSION;
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        cfg.share_group.enable = false;
        let broker_handle = Broker::start(cfg).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let principal = anonymous_principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = request("g1", vec!["t1"]);

        let resp = handle(&broker, version, 1, &encode_request(&req), &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = ShareGroupHeartbeatResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            error_message: None,
            member_id: None,
            member_epoch: 0,
            heartbeat_interval_ms: 0,
            assignment: None,
            unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// The protocol gate -- `UNSUPPORTED_VERSION` when `share_group.enable`
    /// is off -- runs BEFORE the group ACL check, matching Kafka's
    /// `handleShareGroupHeartbeat`. A principal denied `Read` on the group
    /// still gets `UNSUPPORTED_VERSION`, not `GROUP_AUTHORIZATION_FAILED`,
    /// when the protocol itself is unavailable.
    #[tokio::test]
    async fn handle_protocol_gate_precedes_group_acl() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
                std::collections::HashSet::new(),
            ));
            cfg.share_group.enable = false;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = anonymous_principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = request("denied-group", vec!["t1"]);

        let bytes = handle(
            &broker,
            share_group_heartbeat_response::MAX_VERSION,
            5,
            &encode_request(&req),
            &ctx,
        )
        .await
        .expect("ShareGroupHeartbeat handler");
        let resp = decode_response(&bytes);

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_group_read_denied_preserves_error_response() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
                std::collections::HashSet::new(),
            ));
            cfg.share_group.enable = true;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = anonymous_principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = request("denied-group", vec!["t1"]);

        let bytes = handle(
            &broker,
            share_group_heartbeat_response::MAX_VERSION,
            5,
            &encode_request(&req),
            &ctx,
        )
        .await
        .expect("ShareGroupHeartbeat handler");
        let resp = decode_response(&bytes);

        assert!(
            resp.error_code == codes::GROUP_AUTHORIZATION_FAILED,
            "{resp:?}"
        );

        broker_handle.shutdown().await;
    }

    /// A non-zero `member_epoch` with an empty `member_id` is malformed
    /// (Kafka rejects it before topic filtering) and answers
    /// `INVALID_REQUEST` even when the request also names a Describe-denied
    /// topic -- the malformed-request check must win, not
    /// `TOPIC_AUTHORIZATION_FAILED`.
    #[tokio::test]
    async fn handle_malformed_member_id_precedes_topic_authorization() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
                std::collections::HashSet::new(),
            ));
            cfg.share_group.enable = true;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        broker
            .controller
            .submit_change(vec![group_read_acl("g")])
            .await
            .expect("grant group Read");
        let principal = alice();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "c");
        // No Describe grant for "topic-b" -- if the malformed-request check
        // did not run first, this would answer `TOPIC_AUTHORIZATION_FAILED`.
        let req = ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: String::new(),
            member_epoch: 5,
            subscribed_topic_names: Some(vec!["topic-b".into()]),
            ..Default::default()
        };

        let bytes = handle(
            &broker,
            share_group_heartbeat_response::MAX_VERSION,
            9,
            &crate::test_support::encode_request(&req, share_group_heartbeat_response::MAX_VERSION),
            &ctx,
        )
        .await
        .expect("ShareGroupHeartbeat handler");
        let resp = decode_response(&bytes);
        assert!(resp.error_code == codes::INVALID_REQUEST, "{resp:?}");

        broker_handle.shutdown().await;
    }

    // ── Explicit-name `Describe` checks (issue #720) ────────────────────

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
            let req = ShareGroupHeartbeatRequest {
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

    /// A `SubscribedTopicNames` entry this principal cannot `Describe` fails
    /// the whole heartbeat with `TOPIC_AUTHORIZATION_FAILED` (29), and the
    /// group is left with no member -- Kafka never builds the coordinator
    /// record in this case, so an attacker cannot use group membership to
    /// learn a denied topic's id or partitions.
    #[tokio::test]
    async fn handle_subscribed_name_describe_denied_refuses_whole_heartbeat_no_member_created() {
        // Deliberately no Describe grant for "topic-b".
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
                std::collections::HashSet::new(),
            ));
            cfg.share_group.enable = true;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        broker
            .controller
            .submit_change(vec![group_read_acl("g"), describe_acl("topic-a")])
            .await
            .expect("grant group Read and Describe(topic-a)");
        let principal = alice();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "c");
        let req = crate::test_support::encode_request(
            &request("g", vec!["topic-a", "topic-b"]),
            share_group_heartbeat_response::MAX_VERSION,
        );

        let bytes = handle(
            &broker,
            share_group_heartbeat_response::MAX_VERSION,
            7,
            &req,
            &ctx,
        )
        .await
        .expect("ShareGroupHeartbeat handler");
        let resp = decode_response(&bytes);
        assert!(
            resp.error_code == codes::TOPIC_AUTHORIZATION_FAILED,
            "{resp:?}"
        );

        let actor = broker.group_coordinator.get_or_create_share("g");
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(ShareGroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe share group");
        let view = rx.await.expect("share group view");
        assert!(view.members.is_empty(), "{view:?}");

        broker_handle.shutdown().await;
    }

    /// The end-to-end authorized case: both subscribed topics are
    /// `Describe`-authorized and the group is `Read`-authorized, so the
    /// heartbeat succeeds and creates a member.
    #[tokio::test]
    async fn handle_all_authorized_creates_member() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::SimpleAclAuthorizer::new(
                std::collections::HashSet::new(),
            ));
            cfg.share_group.enable = true;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let allowed_id = uuid::Uuid::from_u128(1);
        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![group_read_acl("g"), describe_acl("topic-a")];
        records.extend(topic_with_partitions("topic-a", allowed_id, 1, node));
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create topic");
        let principal = alice();
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "c");
        let req = crate::test_support::encode_request(
            &request("g", vec!["topic-a"]),
            share_group_heartbeat_response::MAX_VERSION,
        );

        let bytes = handle(
            &broker,
            share_group_heartbeat_response::MAX_VERSION,
            7,
            &req,
            &ctx,
        )
        .await
        .expect("ShareGroupHeartbeat handler");
        let resp = decode_response(&bytes);
        assert!(resp.error_code == codes::NONE, "{resp:?}");

        let actor = broker.group_coordinator.get_or_create_share("g");
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(ShareGroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe share group");
        let view = rx.await.expect("share group view");
        assert!(view.members.len() == 1, "{view:?}");

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_persists_request_client_identity() {
        let version = share_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer);
            cfg.share_group.enable = true;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = anonymous_principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);
        let req = ShareGroupHeartbeatRequest {
            group_id: "identity-group".into(),
            member_id: String::new(),
            member_epoch: 0,
            subscribed_topic_names: Some(Vec::new()),
            ..Default::default()
        };

        let bytes = handle(&broker, version, 1, &encode_request(&req), &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");
        assert!(decode_response(&bytes).error_code == 0);

        let actor = broker
            .group_coordinator
            .get_or_create_share("identity-group");
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(ShareGroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe share group");
        let view = rx.await.expect("share group view");

        assert!(view.members.len() == 1);
        assert!(view.members[0].client_id == "client-a");
        assert!(view.members[0].client_host == "/127.0.0.1");

        let peer: SocketAddr = "127.0.0.2:9093".parse().unwrap();
        let ctx = crate::test_support::request_context(&principal, &peer, "client-b");
        let req = ShareGroupHeartbeatRequest {
            group_id: "identity-group".into(),
            member_id: view.members[0].member_id.clone(),
            member_epoch: view.members[0].member_epoch,
            subscribed_topic_names: Some(Vec::new()),
            ..Default::default()
        };
        let bytes = handle(&broker, version, 2, &encode_request(&req), &ctx)
            .await
            .expect("ShareGroupHeartbeat identity refresh");
        assert!(decode_response(&bytes).error_code == 0);

        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(ShareGroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe refreshed share group");
        let view = rx.await.expect("refreshed share group view");
        assert!(view.members[0].client_id == "client-b");
        assert!(view.members[0].client_host == "/127.0.0.2");

        broker_handle.shutdown().await;
    }
}
