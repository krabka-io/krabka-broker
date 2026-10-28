//! `ShareGroupHeartbeat` (`api_key` 76), KIP-932 share-group membership. The
//! handler routes the request to the per-group share actor in
//! `GroupCoordinator`.

#[cfg(test)]
use crate::config::BrokerConfig;
#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        broker::Broker,
        codes,
        coordinator::unified::{GroupCoordinator, GroupType, share::actor::ShareGroupActorMessage},
        error::BrokerError,
        features::share_groups_enabled,
        handlers::{
            ApiVersion, ErrorResponse as _, RequestContext,
            acl_gates::{group_protocol_refusal, subscribed_names_response},
            group_coordinator_error, group_coordinator_error_response,
            share_fetch::member_id_is_valid,
        },
        task_util::{AskError, ask},
    },
    krabka_protocol::owned::{
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
    std::sync::Arc,
};

/// Kafka's `ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH`.
const LEAVE_GROUP_MEMBER_EPOCH: i32 = -1;

pub async fn handle(
    broker: &Broker,
    req: ShareGroupHeartbeatRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<ShareGroupHeartbeatResponse, BrokerError> {
    let ng = Arc::clone(&broker.group_coordinator);

    // ── Protocol gate ───────────────────────────────────────────
    // Kafka's `handleShareGroupHeartbeat` checks whether share groups are
    // enabled BEFORE any ACL check, so a disabled feature answers
    // `UNSUPPORTED_VERSION` even to a caller with no ACLs on the group at
    // all. They are enabled by a finalized `share.version` of 1.
    let image = broker.controller.current_image();

    // ── ACL preamble ────────────────────────────────────────────
    // KIP-932 share groups still gate membership on `Read` on
    // `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
    if let Some(error_code) = group_protocol_refusal(
        share_groups_enabled(&image),
        broker,
        &image,
        ctx,
        &req.group_id,
    ) {
        return Ok(reply(error_code, None));
    }

    // Kafka's `KafkaApis.isMemberIdValid`: the member id must be set and
    // at most 36 characters long. The share consumer mints its own id, so
    // even a first join must carry one. `getErrorResponse` sets only the
    // code. This runs before the topic `Describe` check, so a malformed
    // request that names a denied topic answers `INVALID_REQUEST`.
    if !member_id_is_valid(&req.member_id) {
        return Ok(reply(codes::INVALID_REQUEST, None));
    }

    // Refuse unauthorized subscriptions before coordinator routing.
    if let Some(response) = subscribed_names_response(
        broker,
        &image,
        ctx,
        req.subscribed_topic_names.as_deref(),
        |code| reply(code, None),
    ) {
        return Ok(response);
    }

    // Kafka's `GroupCoordinatorService.throwIfShareGroupHeartbeatRequestIsInvalid`
    // runs before the operation reaches a coordinator shard.
    if let Some(message) = invalid_request_message(&req) {
        return Ok(reply(codes::INVALID_REQUEST, Some(message.to_owned())));
    }

    if let Some(response) = group_coordinator_error_response(broker, &req.group_id, reply) {
        return Ok(response);
    }

    // Kafka creates a share group only on a join, and answers
    // GROUP_ID_NOT_FOUND for a missing group on any other epoch and for a
    // group of another type, without touching any group.
    if let Some(message) = share_group_lookup_error(&ng, &req.group_id, req.member_epoch) {
        return Ok(reply(codes::GROUP_ID_NOT_FOUND, Some(message)));
    }

    ng.mark_share(&req.group_id);
    let handle = ng.get_or_create_share(&req.group_id);
    let group_id = req.group_id.clone();
    let asked = ask(&handle.tx, |reply| ShareGroupActorMessage::Heartbeat {
        request: req,
        client_id: ctx.client_id.unwrap_or_default().to_owned(),
        client_host: ctx.client_host(),
        reply,
    })
    .await;
    Ok(match asked {
        Ok(resp) => resp,
        Err(AskError::Closed) => reply(codes::COORDINATOR_LOAD_IN_PROGRESS, None),
        Err(AskError::Dropped) => {
            ShareGroupHeartbeatResponse::error(stopped_actor_code(broker, &group_id), None)
        }
    })
}

/// The `GROUP_ID_NOT_FOUND` message for a heartbeat that must not reach a
/// share group, or `None` when the heartbeat may go to the share actor.
///
/// It follows Kafka's `GroupMetadataManager`: `getOrMaybeCreateShareGroup`
/// creates a missing group only when `member_epoch == 0` and refuses a group of
/// another type, and `shareGroupLeave` looks the group up through
/// `shareGroup`, which refuses a missing group with the generic message.
///
/// A classic or consumer group lives in the `groups` registry with no type
/// lock, and a streams group keeps its offset home there too, so a hit in
/// either registry is a group of another type.
fn share_group_lookup_error(
    coordinator: &GroupCoordinator,
    group_id: &str,
    member_epoch: i32,
) -> Option<String> {
    let not_share = || Some(format!("Group {group_id} is not a share group."));
    match coordinator.group_type(group_id) {
        Some(GroupType::Share) => return None,
        Some(_) => return not_share(),
        None => {}
    }
    if coordinator.find(group_id).is_some() || coordinator.find_streams(group_id).is_some() {
        return not_share();
    }
    if member_epoch == 0 || coordinator.find_share(group_id).is_some() {
        return None;
    }
    Some(if member_epoch == LEAVE_GROUP_MEMBER_EPOCH {
        format!("Group {group_id} not found.")
    } else {
        format!("Share group {group_id} not found.")
    })
}

/// Kafka's `Utils.throwIfEmptyString` test: a set value that Java's
/// `String.trim` reduces to nothing, which strips every character at or below
/// U+0020 from both ends.
fn blank(value: &str) -> bool { value.chars().all(|c| c <= ' ') }

/// The `INVALID_REQUEST` message of Kafka's
/// `GroupCoordinatorService.throwIfShareGroupHeartbeatRequestIsInvalid`, or
/// `None` for a well-formed request.
fn invalid_request_message(req: &ShareGroupHeartbeatRequest) -> Option<&'static str> {
    if blank(&req.member_id) {
        return Some("MemberId can't be empty.");
    }
    if blank(&req.group_id) {
        return Some("GroupId can't be empty.");
    }
    if req.rack_id.as_deref().is_some_and(blank) {
        return Some("RackId can't be empty.");
    }
    if req.member_epoch == 0 {
        if req
            .subscribed_topic_names
            .as_ref()
            .is_none_or(Vec::is_empty)
        {
            return Some("SubscribedTopicNames must be set in first request.");
        }
    } else if req.member_epoch < LEAVE_GROUP_MEMBER_EPOCH {
        return Some("MemberEpoch is invalid.");
    }
    None
}

/// The early refusal: `code` carrying `message`.
fn reply(code: i16, message: Option<String>) -> ShareGroupHeartbeatResponse {
    ShareGroupHeartbeatResponse::error(code, message)
}

/// The code of a heartbeat that the group's actor dropped unanswered.
///
/// The actor stops when this broker unloads the group's offsets partition,
/// and when a write fails, and the heartbeats still in its mailbox stop with
/// it. Kafka fails an operation on a shard that unloads with
/// `NOT_COORDINATOR`, so the routing check runs again: it answers
/// `NOT_COORDINATOR` once another broker leads the partition. A broker that
/// still serves the group answers `COORDINATOR_NOT_AVAILABLE`, which the
/// client retries, and the retry starts a new actor from the committed state
/// of the group.
fn stopped_actor_code(broker: &Broker, group_id: &str) -> i16 {
    group_coordinator_error(broker, group_id).unwrap_or(codes::COORDINATOR_NOT_AVAILABLE)
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::{
            authorizer::{AllowAllAuthorizer, Authorizer},
            coordinator::unified::share::actor::{ShareDescribeView, ShareGroupActorHandle},
            handlers::{
                encode_response,
                group_heartbeat_test_support::{
                    RequestIdentity, acl_authorizer, alice_peer, describe_acl, grant_group_read,
                    group_read_acl, subscribed_names_describe_denied_table as denied_table_fixture,
                    topic_with_partitions,
                },
                group_read_denied,
            },
            task_util::ask,
            test_support::{
                ControllerPeerAllowed, finalize_share_version, request_context, start_broker_with,
                tolerate_starved_heartbeats,
            },
        },
        assert2::assert,
        krabka_metadata::MetadataImage,
        krabka_protocol::{Decode, owned::share_group_heartbeat_response},
        std::{net::SocketAddr, sync::Arc},
    };

    const VERSION: i16 = share_group_heartbeat_response::MAX_VERSION;

    /// Starts a broker with `authorizer` installed and the share-test
    /// heartbeat session.
    async fn start_broker(authorizer: Arc<dyn Authorizer>) -> HandlerStartedBroker {
        start_broker_with(|cfg| {
            cfg.authorizer = authorizer;
            tolerate_starved_heartbeats(cfg);
        })
        .await
    }

    #[bon::builder]
    fn heartbeat_request(
        group_id: String,
        member_id: String,
        member_epoch: i32,
    ) -> ShareGroupHeartbeatRequest {
        ShareGroupHeartbeatRequest {
            group_id,
            member_id,
            member_epoch,
            subscribed_topic_names: Some(vec!["t".into()]),
            ..Default::default()
        }
    }

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        let authorizer = acl_authorizer();
        let image = MetadataImage::new(uuid::Uuid::nil());
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["ANONYMOUS"];
            client ["share-client"];
        };

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));
        assert!(!group_read_denied(&AllowAllAuthorizer, &image, &ctx, "g"));

        let bytes = encode_response(
            &ShareGroupHeartbeatResponse::error(codes::GROUP_AUTHORIZATION_FAILED, None),
            VERSION,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = ShareGroupHeartbeatResponse::decode(&mut cur, VERSION).unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
        assert!(cur.is_empty(), "response decoder consumed all bytes");
    }

    krabka_macros::wire_codec_helpers! {
        test_context ["client-a"];
    }

    struct ShareGroupSnapshot {
        actor: Arc<ShareGroupActorHandle>,
        view: ShareDescribeView,
    }

    async fn share_group_view(broker: &Broker, group: &str) -> ShareGroupSnapshot {
        let actor = broker.group_coordinator.get_or_create_share(group);
        let view = ask(&actor.tx, |reply| ShareGroupActorMessage::Describe {
            reply,
        })
        .await
        .expect("share group view");
        ShareGroupSnapshot { actor, view }
    }

    fn request(group_id: &str, subscribed: Vec<&str>) -> ShareGroupHeartbeatRequest {
        ShareGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: "member-1".into(),
            member_epoch: 0,
            subscribed_topic_names: Some(subscribed.into_iter().map(String::from).collect()),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn handle_disabled_feature_returns_unsupported_version() {
        let version = VERSION;
        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
        tolerate_starved_heartbeats(&mut cfg);
        let broker_handle = Broker::start(cfg).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        finalize_share_version(&broker, 0).await;
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["ANONYMOUS"];
        };
        let req = request("g1", vec!["t1"]);

        let resp = handle(&broker, req, version, &ctx).await.expect("handle");

        let expected = ShareGroupHeartbeatResponse {
            error_code: codes::UNSUPPORTED_VERSION,
            ..Default::default()
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// The protocol gate -- `UNSUPPORTED_VERSION` when `share.version` is
    /// finalized at 0 -- runs BEFORE the group ACL check, matching Kafka's
    /// `handleShareGroupHeartbeat`. A principal denied `Read` on the group
    /// still gets `UNSUPPORTED_VERSION`, not `GROUP_AUTHORIZATION_FAILED`,
    /// when the protocol itself is unavailable.
    async fn handle_denied_group_heartbeat(broker: &Broker) -> ShareGroupHeartbeatResponse {
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["ANONYMOUS"];
        };
        let req = request("denied-group", vec!["t1"]);
        handle(broker, req, VERSION, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler")
    }

    #[tokio::test]
    async fn handle_protocol_gate_precedes_group_acl() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker(Arc::new(acl_authorizer()))];
        };
        finalize_share_version(&broker, 0).await;
        let resp = handle_denied_group_heartbeat(&broker).await;

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_group_read_denied_preserves_error_response() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker(Arc::new(acl_authorizer()))];
        };
        let resp = handle_denied_group_heartbeat(&broker).await;

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
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker(Arc::new(acl_authorizer()))];
        };
        grant_group_read(&broker, "g").await;
        let RequestIdentity { principal, peer } = alice_peer();
        let ctx = request_context(&principal, &peer, "c");
        // No Describe grant for "topic-b" -- if the malformed-request check
        // did not run first, this would answer `TOPIC_AUTHORIZATION_FAILED`.
        let req = ShareGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: String::new(),
            member_epoch: 5,
            subscribed_topic_names: Some(vec!["topic-b".into()]),
            ..Default::default()
        };

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");
        assert!(resp.error_code == codes::INVALID_REQUEST, "{resp:?}");

        broker_handle.shutdown().await;
    }

    #[derive(Default)]
    struct ShareHeartbeatScenario {
        started: Option<HandlerStartedBroker>,
        request: Option<ShareGroupHeartbeatRequest>,
        response: Option<ShareGroupHeartbeatResponse>,
    }
    #[rstest::fixture]
    fn share_heartbeat() -> ShareHeartbeatScenario {
        let started = None;
        ShareHeartbeatScenario {
            started,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "an authorized share coordinator and a joining member-1 heartbeat for group g"
    )]
    async fn joining_share_heartbeat(share_heartbeat: &mut ShareHeartbeatScenario) {
        share_heartbeat.started = Some(start_broker(Arc::new(AllowAllAuthorizer)).await);
        share_heartbeat.request = Some(
            heartbeat_request()
                .group_id("g".into())
                .member_id("member-1".into())
                .member_epoch(0)
                .call(),
        );
    }
    #[rstest_bdd_macros::given("the share heartbeat field {field} is {value}")]
    fn share_heartbeat_field(
        share_heartbeat: &mut ShareHeartbeatScenario,
        field: String,
        value: String,
    ) {
        let request = share_heartbeat.request.as_mut().unwrap();
        let text = match value.as_str() {
            "empty" => String::new(),
            "blank" => "  ".into(),
            "space-and-tab" => " \t".into(),
            "37 characters" => "m".repeat(37),
            _ => value.clone(),
        };
        match field.as_str() {
            "member ID" => request.member_id = text,
            "group ID" => request.group_id = text,
            "rack ID" => request.rack_id = Some(text),
            "subscription" => {
                request.subscribed_topic_names = match value.as_str() {
                    "null" => None,
                    "empty" => Some(Vec::new()),
                    _ => panic!("unknown share subscription"),
                }
            }
            "member epoch" => request.member_epoch = value.parse().unwrap(),
            _ => panic!("unknown share heartbeat field: {field}"),
        }
    }
    #[rstest_bdd_macros::given(
        "the share coordinator contains a classic group a consumer group and a streams marker"
    )]
    async fn other_share_group_kinds(share_heartbeat: &mut ShareHeartbeatScenario) {
        use crate::coordinator::unified::actor::GroupKindTag;
        let started = start_broker(Arc::new(AllowAllAuthorizer)).await;
        started.broker.wait_until_group_coordinator_ready().await;
        let broker = started.broker.broker_arc_for_test();
        let coordinator = &broker.group_coordinator;
        let _classic = coordinator.get_or_create_classic("classic");
        let _consumer = coordinator.get_or_create_group("consumer", GroupKindTag::Consumer);
        coordinator.mark_streams("streams");
        share_heartbeat.started = Some(started);
    }
    #[rstest_bdd_macros::given("share member m1 sends epoch {epoch} for group {group}")]
    fn share_group_creation_request(
        share_heartbeat: &mut ShareHeartbeatScenario,
        epoch: i32,
        group: String,
    ) {
        share_heartbeat.request = Some(
            heartbeat_request()
                .group_id(group)
                .member_id("m1".into())
                .member_epoch(epoch)
                .call(),
        );
    }
    #[rstest_bdd_macros::when("the share coordinator handles the member heartbeat")]
    async fn handle_share_heartbeat(share_heartbeat: &mut ShareHeartbeatScenario) {
        let broker = share_heartbeat
            .started
            .as_ref()
            .unwrap()
            .broker
            .broker_arc_for_test();
        krabka_macros::request_test_identity! {bind(ctx);user ["ANONYMOUS"];};
        share_heartbeat.response = Some(
            handle(
                &broker,
                share_heartbeat.request.as_ref().unwrap().clone(),
                VERSION,
                &ctx,
            )
            .await
            .expect("ShareGroupHeartbeat handler"),
        );
    }
    #[rstest_bdd_macros::then(
        "the malformed share heartbeat returns code 42 message {message} and creates no share group"
    )]
    async fn check_malformed_share_heartbeat(
        share_heartbeat: &mut ShareHeartbeatScenario,
        message: String,
    ) {
        let error_message = if message == "null" {
            None
        } else {
            Some(message)
        };
        assert!(
            share_heartbeat.response.as_ref().unwrap()
                == &ShareGroupHeartbeatResponse {
                    error_code: codes::INVALID_REQUEST,
                    error_message,
                    ..Default::default()
                }
        );
        assert!(
            share_heartbeat
                .started
                .as_ref()
                .unwrap()
                .broker
                .broker_arc_for_test()
                .group_coordinator
                .share_group_ids()
                .is_empty()
        );
        share_heartbeat
            .started
            .take()
            .unwrap()
            .broker
            .shutdown()
            .await;
    }
    #[rstest_bdd_macros::then(
        "the share coordinator returns error {code} message {message} and creates only accepted joins"
    )]
    async fn check_share_group_creation(
        share_heartbeat: &mut ShareHeartbeatScenario,
        code: i16,
        message: String,
    ) {
        let response = share_heartbeat.response.as_ref().unwrap();
        if code == codes::NONE {
            assert!(response.error_code == codes::NONE);
        } else {
            assert!(
                response
                    == &ShareGroupHeartbeatResponse {
                        error_code: code,
                        error_message: Some(message),
                        ..Default::default()
                    }
            );
        }
        let expected = if code == codes::NONE {
            vec!["joined".to_owned()]
        } else {
            Vec::new()
        };
        assert!(
            share_heartbeat
                .started
                .as_ref()
                .unwrap()
                .broker
                .broker_arc_for_test()
                .group_coordinator
                .share_group_ids()
                == expected
        );
        share_heartbeat
            .started
            .take()
            .unwrap()
            .broker
            .shutdown()
            .await;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_heartbeat/heartbeat.feature",
        name = "Malformed member identity join subscription and epoch fields cannot create a group"
    )]
    #[tokio::test]
    async fn handle_refuses_malformed_heartbeats_as_kafka_does(
        share_heartbeat: ShareHeartbeatScenario,
        field: String,
        value: String,
        message: String,
    ) {
        let _ = (share_heartbeat, field, value, message);
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_heartbeat/heartbeat.feature",
        name = "Only a join can create a share group and other group kinds remain distinct"
    )]
    #[tokio::test]
    async fn handle_creates_share_group_only_on_join_as_kafka_does(
        share_heartbeat: ShareHeartbeatScenario,
        epoch: i32,
        group: String,
        code: i16,
        message: String,
    ) {
        let _ = (share_heartbeat, epoch, group, code, message);
    }

    #[test]
    fn subscribed_names_describe_denied_table() { denied_table_fixture(); }

    // ── Explicit-name `Describe` checks (issue #720) ────────────────────

    /// A `SubscribedTopicNames` entry this principal cannot `Describe` fails
    /// the whole heartbeat with `TOPIC_AUTHORIZATION_FAILED` (29), and the
    /// group is left with no member -- Kafka never builds the coordinator
    /// record in this case, so an attacker cannot use group membership to
    /// learn a denied topic's id or partitions.
    #[tokio::test]
    async fn handle_subscribed_name_describe_denied_refuses_whole_heartbeat_no_member_created() {
        // Deliberately no Describe grant for "topic-b".
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker(Arc::new(acl_authorizer()))];
        };
        broker
            .controller
            .submit_change(vec![group_read_acl("g"), describe_acl("topic-a")])
            .await
            .expect("grant group Read and Describe(topic-a)");
        let RequestIdentity { principal, peer } = alice_peer();
        let ctx = request_context(&principal, &peer, "c");
        let req = request("g", vec!["topic-a", "topic-b"]);

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");
        assert!(
            resp.error_code == codes::TOPIC_AUTHORIZATION_FAILED,
            "{resp:?}"
        );

        let ShareGroupSnapshot {
            actor: _actor,
            view,
        } = share_group_view(&broker, "g").await;
        assert!(view.members.is_empty(), "{view:?}");

        broker_handle.shutdown().await;
    }

    /// The end-to-end authorized case: both subscribed topics are
    /// `Describe`-authorized and the group is `Read`-authorized, so the
    /// heartbeat succeeds and creates a member.
    #[tokio::test]
    async fn handle_all_authorized_creates_member() {
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker(Arc::new(ControllerPeerAllowed(acl_authorizer()))).await;
        broker_handle.wait_until_group_coordinator_ready().await;
        let broker = broker_handle.broker_arc_for_test();
        let allowed_id = uuid::Uuid::from_u128(1);
        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![group_read_acl("g"), describe_acl("topic-a")];
        records.extend(
            topic_with_partitions()
                .name("topic-a")
                .topic_id(allowed_id)
                .partitions(1)
                .node(node)
                .call(),
        );
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create topic");
        let RequestIdentity { principal, peer } = alice_peer();
        let ctx = request_context(&principal, &peer, "c");
        let req = request("g", vec!["topic-a"]);

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");
        assert!(resp.error_code == codes::NONE, "{resp:?}");

        let ShareGroupSnapshot {
            actor: _actor,
            view,
        } = share_group_view(&broker, "g").await;
        assert!(view.members.len() == 1, "{view:?}");

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_persists_request_client_identity() {
        let version = VERSION;
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker(Arc::new(AllowAllAuthorizer)).await;
        broker_handle.wait_until_group_coordinator_ready().await;
        let broker = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["ANONYMOUS"];
        };
        let heartbeat_req = heartbeat_request()
            .group_id("identity-group".into())
            .member_id("member-1".into())
            .member_epoch(0)
            .call();

        let resp = handle(&broker, heartbeat_req, version, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");
        assert!(resp.error_code == 0);

        let actor = broker
            .group_coordinator
            .get_or_create_share("identity-group");
        let view = ask(&actor.tx, |reply| ShareGroupActorMessage::Describe {
            reply,
        })
        .await
        .expect("share group view");

        assert!(view.members.len() == 1);
        assert!(view.members[0].client_id == "client-a");
        assert!(view.members[0].client_host == "/127.0.0.1");

        let peer: SocketAddr = "127.0.0.2:9093".parse().unwrap();
        let ctx = request_context(&principal, &peer, "client-b");
        let req = heartbeat_request()
            .group_id("identity-group".into())
            .member_id(view.members[0].member_id.clone())
            .member_epoch(view.members[0].member_epoch)
            .call();
        let resp = handle(&broker, req, version, &ctx)
            .await
            .expect("ShareGroupHeartbeat identity refresh");
        assert!(resp.error_code == 0);

        let view = ask(&actor.tx, |reply| ShareGroupActorMessage::Describe {
            reply,
        })
        .await
        .expect("refreshed share group view");
        assert!(view.members[0].client_id == "client-b");
        assert!(view.members[0].client_host == "/127.0.0.2");

        broker_handle.shutdown().await;
    }
}
