//! `ShareGroupHeartbeat` (`api_key` 76), KIP-932 share-group membership. The
//! handler routes the request to the per-group share actor in
//! `GroupCoordinator`.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        broker::Broker,
        codes,
        coordinator::unified::{
            GroupCoordinator, GroupType, replay_policy::LEAVE_GROUP_MEMBER_EPOCH,
            share::actor::ShareGroupActorMessage,
        },
        error::BrokerError,
        features::share_groups_enabled,
        handlers::{
            ApiVersion, ErrorResponse as _, RequestContext,
            acl_gates::{group_protocol_refusal, subscribed_names_response},
            coordinator_routing::java_blank,
            group_actor_error_code, group_coordinator_error_response,
            share_fetch::member_id_is_valid,
        },
        task_util::ask,
    },
    krabka_protocol::owned::{
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
    std::sync::Arc,
};

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
        Err(error) => reply(group_actor_error_code(broker, &group_id, error), None),
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

/// The `INVALID_REQUEST` message of Kafka's
/// `GroupCoordinatorService.throwIfShareGroupHeartbeatRequestIsInvalid`, or
/// `None` for a well-formed request.
fn invalid_request_message(req: &ShareGroupHeartbeatRequest) -> Option<&'static str> {
    if java_blank(&req.member_id) {
        return Some("MemberId can't be empty.");
    }
    if java_blank(&req.group_id) {
        return Some("GroupId can't be empty.");
    }
    if req.rack_id.as_deref().is_some_and(java_blank) {
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
                    RequestIdentity, acl_authorizer, alice_peer, describe_acl, group_read_acl,
                    subscribed_names_describe_denied_table as denied_table_fixture,
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
        std::{convert::Infallible, net::SocketAddr, str::FromStr, sync::Arc},
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
        krabka_broker_macros::request_test_identity! {
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

    krabka_broker_macros::wire_codec_helpers! {
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

    /// A broker that authorizes with ACLs, and the answer to the heartbeat
    /// the scenario sends it as `User:alice`.
    struct ShareHeartbeatGates {
        started: krabka_bdd::Given<HandlerStartedBroker>,
        response: krabka_bdd::Given<ShareGroupHeartbeatResponse>,
    }

    #[rstest::fixture]
    fn share_heartbeat_gates() -> ShareHeartbeatGates {
        ShareHeartbeatGates {
            started: krabka_bdd::Given::new("started", "Given a principal holds <grants>"),
            response: krabka_bdd::Given::new("response", "When ... sends a heartbeat ..."),
        }
    }

    /// The ACLs `User:alice` holds on a broker that authorizes with ACLs.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
    enum Grants {
        #[step_param("no ACLs")]
        Nothing,
        #[step_param("Read on group g")]
        GroupRead,
        #[step_param("Read on group g and Describe on topic-a")]
        GroupReadAndDescribe,
    }

    /// The whole-response refusal a heartbeat gets.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
    enum Refusal {
        #[step_param("UNSUPPORTED_VERSION")]
        UnsupportedVersion,
        #[step_param("GROUP_AUTHORIZATION_FAILED")]
        GroupAuthorizationFailed,
        #[step_param("INVALID_REQUEST")]
        InvalidRequest,
        #[step_param("TOPIC_AUTHORIZATION_FAILED")]
        TopicAuthorizationFailed,
    }

    impl Refusal {
        fn code(self) -> i16 {
            match self {
                Self::UnsupportedVersion => codes::UNSUPPORTED_VERSION,
                Self::GroupAuthorizationFailed => codes::GROUP_AUTHORIZATION_FAILED,
                Self::InvalidRequest => codes::INVALID_REQUEST,
                Self::TopicAuthorizationFailed => codes::TOPIC_AUTHORIZATION_FAILED,
            }
        }
    }

    /// The subscribed topic names of a heartbeat, written `topic-a and topic-b`.
    struct Subscription(Vec<String>);

    impl FromStr for Subscription {
        type Err = Infallible;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            Ok(Self(text.split(" and ").map(str::to_owned).collect()))
        }
    }

    #[krabka_bdd::given("a principal holds {grants}")]
    async fn principal_grants(share_heartbeat_gates: &mut ShareHeartbeatGates, grants: Grants) {
        let started = start_broker(Arc::new(ControllerPeerAllowed(acl_authorizer()))).await;
        started.broker.wait_until_group_coordinator_ready().await;
        let records = match grants {
            Grants::Nothing => Vec::new(),
            Grants::GroupRead => vec![group_read_acl("g")],
            Grants::GroupReadAndDescribe => vec![group_read_acl("g"), describe_acl("topic-a")],
        };
        if !records.is_empty() {
            started
                .broker
                .broker_arc_for_test()
                .controller
                .submit_change(records)
                .await
                .expect("grant the principal's ACLs");
        }
        share_heartbeat_gates.started.set(started);
    }

    #[krabka_bdd::given("share.version is finalized at {level}")]
    async fn share_version_level(share_heartbeat_gates: &ShareHeartbeatGates, level: i16) {
        finalize_share_version(
            &share_heartbeat_gates
                .started
                .get()
                .broker
                .broker_arc_for_test(),
            level,
        )
        .await;
    }

    #[krabka_bdd::given("topic-a exists")]
    async fn topic_a_exists(share_heartbeat_gates: &ShareHeartbeatGates) {
        let handle = &share_heartbeat_gates.started.get().broker;
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(
                topic_with_partitions()
                    .name("topic-a")
                    .topic_id(uuid::Uuid::from_u128(1))
                    .partitions(1)
                    .node(krabka_raft::NodeId(handle.node_id()))
                    .call(),
            )
            .await
            .expect("create topic-a");
    }

    /// Sends `req` as `User:alice` and keeps the answer.
    async fn send_as_alice(
        share_heartbeat_gates: &mut ShareHeartbeatGates,
        req: ShareGroupHeartbeatRequest,
    ) {
        let RequestIdentity { principal, peer } = alice_peer();
        let ctx = request_context(&principal, &peer, "c");
        let broker = share_heartbeat_gates
            .started
            .get()
            .broker
            .broker_arc_for_test();
        let response = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ShareGroupHeartbeat handler");
        share_heartbeat_gates.response.set(response);
    }

    #[krabka_bdd::when("a member sends a first heartbeat for group g subscribing to {topics}")]
    async fn first_heartbeat(
        share_heartbeat_gates: &mut ShareHeartbeatGates,
        topics: Subscription,
    ) {
        let req = ShareGroupHeartbeatRequest {
            subscribed_topic_names: Some(topics.0),
            ..request("g", Vec::new())
        };
        send_as_alice(share_heartbeat_gates, req).await;
    }

    #[krabka_bdd::when(
        "a member with an empty member id sends a heartbeat at epoch {epoch} for group g subscribing to topic-b"
    )]
    async fn empty_member_id_heartbeat(
        share_heartbeat_gates: &mut ShareHeartbeatGates,
        epoch: i32,
    ) {
        let req = ShareGroupHeartbeatRequest {
            member_id: String::new(),
            member_epoch: epoch,
            ..request("g", vec!["topic-b"])
        };
        send_as_alice(share_heartbeat_gates, req).await;
    }

    #[krabka_bdd::then("the heartbeat is refused with {refusal}")]
    fn heartbeat_refused(share_heartbeat_gates: &ShareHeartbeatGates, refusal: Refusal) {
        let expected = ShareGroupHeartbeatResponse {
            error_code: refusal.code(),
            ..Default::default()
        };
        assert!(*share_heartbeat_gates.response.get() == expected);
    }

    #[krabka_bdd::then("the heartbeat is accepted")]
    fn heartbeat_accepted(share_heartbeat_gates: &ShareHeartbeatGates) {
        let response = share_heartbeat_gates.response.get();
        assert!((response.error_code, &response.error_message) == (codes::NONE, &None));
    }

    #[krabka_bdd::then("group g has {count} member(s)")]
    async fn group_member_count(share_heartbeat_gates: &ShareHeartbeatGates, count: usize) {
        let broker = share_heartbeat_gates
            .started
            .get()
            .broker
            .broker_arc_for_test();
        let ShareGroupSnapshot {
            actor: _actor,
            view,
        } = share_group_view(&broker, "g").await;
        assert!(view.members.len() == count, "{view:?}");
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_heartbeat/heartbeat.feature",
        name = "A heartbeat gets UNSUPPORTED_VERSION while share groups are off"
    )]
    #[tokio::test]
    async fn share_version_zero_precedes_the_group_acl(
        share_heartbeat_gates: ShareHeartbeatGates,
        case: String,
        grants: String,
    ) {
        let _ = (share_heartbeat_gates, case, grants);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_heartbeat/heartbeat.feature",
        name = "A principal without group Read is refused"
    )]
    #[tokio::test]
    async fn group_read_is_required(share_heartbeat_gates: ShareHeartbeatGates) {
        let _ = share_heartbeat_gates;
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_heartbeat/heartbeat.feature",
        name = "An empty member id is invalid even with an undescribable topic"
    )]
    #[tokio::test]
    async fn member_id_precedes_topic_authorization(share_heartbeat_gates: ShareHeartbeatGates) {
        let _ = share_heartbeat_gates;
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_heartbeat/heartbeat.feature",
        name = "One undescribable topic refuses the heartbeat and adds no member"
    )]
    #[tokio::test]
    async fn an_undescribable_topic_refuses_the_heartbeat(
        share_heartbeat_gates: ShareHeartbeatGates,
    ) {
        let _ = share_heartbeat_gates;
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_share_heartbeat/heartbeat.feature",
        name = "Describable topics admit the member"
    )]
    #[tokio::test]
    async fn describable_topics_admit_the_member(share_heartbeat_gates: ShareHeartbeatGates) {
        let _ = share_heartbeat_gates;
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
        krabka_broker_macros::request_test_identity! {bind(ctx);user ["ANONYMOUS"];};
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

    #[tokio::test]
    async fn handle_persists_request_client_identity() {
        let version = VERSION;
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker(Arc::new(AllowAllAuthorizer)).await;
        broker_handle.wait_until_group_coordinator_ready().await;
        let broker = broker_handle.broker_arc_for_test();
        krabka_broker_macros::request_test_identity! {
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
