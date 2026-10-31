//! `ConsumerGroupHeartbeat` (`api_key` 68), from the KIP-848 next-gen consumer
//! group protocol. It routes the request to the per-group actor in
//! `GroupCoordinator`.

#[cfg(test)]
use super::group_heartbeat_test_support::{HandlerStartedBroker, LiveBrokerFixture};
use {
    crate::{
        broker::Broker,
        codes,
        coordinator::unified::{
            actor::{GroupActorMessage, GroupKindTag},
            config::NextGenConfig,
            regex_resolver::ImageTopicRegexResolver,
        },
        error::BrokerError,
        handlers::{
            ApiVersion, ErrorResponse as _, RequestContext,
            acl_gates::{group_protocol_refusal, subscribed_names_response},
            coordinator_routing::{group_actor_error_code, java_blank},
            group_coordinator_error, group_version_disabled,
        },
        task_util::ask,
    },
    krabka_protocol::owned::{
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    },
    std::sync::Arc,
};

pub async fn handle(
    broker: &Broker,
    req: ConsumerGroupHeartbeatRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<ConsumerGroupHeartbeatResponse, BrokerError> {
    let coordinator = Arc::clone(&broker.group_coordinator);
    // Read the offset BEFORE the image, not after: if a record commits in
    // between, `image` may reflect it while `metadata_offset` does not. The
    // regex resolution the actor may make from `image` is stamped with
    // `metadata_offset`, and an offset older than its image only makes a
    // later refresh happen once too often, never once too rarely.
    let metadata_offset = broker.controller.current_metadata_offset();
    let image = broker.controller.current_image();

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

    // ── ACL preamble ────────────────────────────────────────────
    // `Read` on `Group(group_id)`. On Deny → whole-response
    // `error_code = GROUP_AUTHORIZATION_FAILED (30)`.
    if let Some(error_code) = group_protocol_refusal(
        !group_version_disabled(&image) && coordinator.config.next_gen_enabled(),
        broker,
        &image,
        ctx,
        &req.group_id,
    ) {
        return Ok(reply(error_code, None));
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

    // `GroupCoordinatorService.consumerGroupHeartbeat` validates the
    // request before it routes it to a coordinator shard.
    if let Err(refused) = validate_request(&req, version, &coordinator.config) {
        return Ok(*refused);
    }

    // `subscribed_topic_regex` (KIP-848 v1+): the actor resolves the
    // group's patterns against the image read above, with this principal's
    // `Describe` decisions, when a heartbeat needs them resolved or
    // refreshed -- Kafka's `TopicRegexResolver.resolveRegularExpressions`
    // with the request context of the heartbeat. A pattern that fails to
    // compile is rejected by the actor's own
    // `check_subscribed_topic_regex` with `INVALID_REGULAR_EXPRESSION`
    // before any member state changes.
    let regex_resolver = Arc::new(ImageTopicRegexResolver::new(
        image,
        metadata_offset,
        Arc::clone(&broker.config.authorizer),
        ctx.principal.clone(),
        *ctx.peer,
    ));

    if let Some(error_code) = group_coordinator_error(broker, &req.group_id) {
        return Ok(reply(error_code, None));
    }

    // Kafka creates a consumer group only on a join, and answers
    // GROUP_ID_NOT_FOUND for a missing group on any other epoch and for a
    // share or streams group, without touching any group.
    if let Some(message) = coordinator.consumer_group_lookup_error(&req.group_id, req.member_epoch)
    {
        return Ok(reply(codes::GROUP_ID_NOT_FOUND, Some(message)));
    }

    // Route to the one actor for this id, spawning a consumer-kind actor if
    // the id is brand-new. Both RPC families reach the same actor; a classic
    // group rejects a next-gen heartbeat from inside the actor's `Heartbeat`
    // arm (replying `GROUP_ID_NOT_FOUND`), which is where the per-group kind
    // lock now lives.
    let handle = coordinator.get_or_create_group(&req.group_id, GroupKindTag::Consumer);
    let asked = ask(&handle.tx, |reply| GroupActorMessage::Heartbeat {
        request: req,
        client_id: ctx.client_id.unwrap_or_default().to_owned(),
        client_host: ctx.client_host(),
        regex_resolver,
        reply,
    })
    .await;
    Ok(asked.unwrap_or_else(|error| reply(group_actor_error_code(error), None)))
}

/// The early refusal: `code` carrying `message`.
fn reply(code: i16, message: Option<String>) -> ConsumerGroupHeartbeatResponse {
    ConsumerGroupHeartbeatResponse::error(code, message)
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
    config: &NextGenConfig,
) -> Result<(), Box<ConsumerGroupHeartbeatResponse>> {
    let invalid = |message: &str| {
        Box::new(ConsumerGroupHeartbeatResponse::error(
            codes::INVALID_REQUEST,
            Some(message.to_owned()),
        ))
    };
    // `Utils.throwIfEmptyString`: a present value that trims to nothing.
    let blank = |value: Option<&str>| value.is_some_and(java_blank);

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

    use {
        super::*,
        crate::{
            authorizer::AllowAllAuthorizer,
            broker::{Broker, BrokerHandle},
            coordinator::unified::config::NextGenConfig,
            handlers::{
                encode_response,
                group_heartbeat_test_support::{
                    GroupFeatureLevel, RequestIdentity, acl_authorizer, alice_peer,
                    check_group_read_denied, describe_acl, grant_group_read, group_read_acl,
                    image_with_group_version, set_group_version,
                    subscribed_names_describe_denied_table as subscribed_names_describe_denied_cases,
                    topic_with_partitions, versioned_group_broker,
                },
                group_read_denied,
            },
            task_util::ask,
            test_support::{KafkaErrorCode, request_context, start_group_broker},
        },
        assert2::assert,
        krabka_metadata::{MetadataImage, MetadataRecord},
        krabka_protocol::{Decode, owned::consumer_group_heartbeat_request},
        std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration},
        tokio::time,
    };

    const VERSION: i16 = consumer_group_heartbeat_request::MAX_VERSION;

    fn request(group_id: &str) -> ConsumerGroupHeartbeatRequest {
        subscription_request(group_id, "member-a", 0)
    }

    fn subscription_request(
        group_id: &str,
        member_id: &str,
        member_epoch: i32,
    ) -> ConsumerGroupHeartbeatRequest {
        ConsumerGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: member_id.into(),
            member_epoch,
            rebalance_timeout_ms: 30_000,
            topic_partitions: Some(vec![]),
            subscribed_topic_names: Some(vec!["topic-a".into()]),
            ..Default::default()
        }
    }

    krabka_macros::wire_codec_helpers! {
        test_context ["consumer-group-heartbeat-test"];
    }

    #[test]
    fn group_version_gate_distinguishes_disabled_and_enabled_images() {
        let fresh = MetadataImage::new(uuid::Uuid::nil());
        assert!(group_version_disabled(&fresh));

        let enabled = image_with_group_version(GroupFeatureLevel(1));
        assert!(!group_version_disabled(&enabled));

        let disabled = image_with_group_version(GroupFeatureLevel(0));
        assert!(group_version_disabled(&disabled));
    }

    #[derive(Default)]
    struct ConsumerHeartbeatScenario {
        request: Option<ConsumerGroupHeartbeatRequest>,
        version: i16,
        validation: Vec<Option<ConsumerGroupHeartbeatResponse>>,
        live: Option<LiveBrokerFixture>,
        response: Option<ConsumerGroupHeartbeatResponse>,
    }
    #[rstest::fixture]
    fn consumer_heartbeat() -> ConsumerHeartbeatScenario {
        let request = None;
        ConsumerHeartbeatScenario {
            request,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given("a consumer {phase} heartbeat uses API version {version}")]
    fn consumer_heartbeat_phase(
        consumer_heartbeat: &mut ConsumerHeartbeatScenario,
        phase: String,
        version: i16,
    ) {
        let join = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 30_000,
            subscribed_topic_names: Some(vec!["t".into()]),
            topic_partitions: Some(vec![]),
            ..Default::default()
        };
        consumer_heartbeat.request = Some(match phase.as_str() {
            "joining" => join,
            "steady" => ConsumerGroupHeartbeatRequest {
                member_epoch: 3,
                rebalance_timeout_ms: -1,
                subscribed_topic_names: None,
                topic_partitions: None,
                ..join
            },
            "leaving" => ConsumerGroupHeartbeatRequest {
                member_epoch: -1,
                rebalance_timeout_ms: -1,
                subscribed_topic_names: None,
                topic_partitions: None,
                ..join
            },
            _ => panic!("unknown consumer heartbeat phase: {phase}"),
        });
        consumer_heartbeat.version = version;
    }
    #[rstest_bdd_macros::given("the consumer heartbeat field {field} is {value}")]
    fn consumer_heartbeat_field(
        consumer_heartbeat: &mut ConsumerHeartbeatScenario,
        field: String,
        value: String,
    ) {
        use krabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions;
        let request = consumer_heartbeat.request.as_mut().unwrap();
        let text = match value.as_str() {
            "empty" => String::new(),
            "blank" => " ".into(),
            _ => value.clone(),
        };
        match field.as_str() {
            "member ID" => request.member_id = text,
            "group ID" => request.group_id = text,
            "instance ID" => request.instance_id = Some(text),
            "rack ID" => request.rack_id = Some(text),
            "rebalance timeout" => request.rebalance_timeout_ms = value.parse().unwrap(),
            "owned partitions" => {
                request.topic_partitions = match value.as_str() {
                    "null" => None,
                    "nonempty" => Some(vec![TopicPartitions::default()]),
                    _ => panic!("unknown owned partitions"),
                }
            }
            "subscription" => {
                assert!(value == "null");
                request.subscribed_topic_names = None;
            }
            "member epoch" => request.member_epoch = value.parse().unwrap(),
            "server assignor" => request.server_assignor = Some(text),
            _ => panic!("unknown consumer heartbeat field: {field}"),
        }
    }
    #[rstest_bdd_macros::when(
        "the consumer heartbeat is validated with default coordinator settings"
    )]
    fn validate_consumer_heartbeat(consumer_heartbeat: &mut ConsumerHeartbeatScenario) {
        consumer_heartbeat.validation.push(
            validate_request(
                consumer_heartbeat.request.as_ref().unwrap(),
                consumer_heartbeat.version,
                &NextGenConfig::default(),
            )
            .err()
            .map(|error| *error),
        );
    }
    #[rstest_bdd_macros::then("consumer heartbeat validation accepts the request")]
    fn check_valid_consumer_heartbeat(consumer_heartbeat: &ConsumerHeartbeatScenario) {
        assert!(consumer_heartbeat.validation == vec![None]);
    }
    #[rstest_bdd_macros::then(
        "consumer heartbeat validation refuses with code {code} message {message}"
    )]
    fn check_invalid_consumer_heartbeat(
        consumer_heartbeat: &ConsumerHeartbeatScenario,
        code: i16,
        message: String,
    ) {
        assert!(
            consumer_heartbeat.validation
                == vec![Some(ConsumerGroupHeartbeatResponse {
                    error_code: code,
                    error_message: Some(message),
                    ..Default::default()
                })]
        );
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_consumer_heartbeat_validation",fixtures=[consumer_heartbeat:ConsumerHeartbeatScenario]);
    #[rstest_bdd_macros::given(
        "the consumer coordinator has a share marker a live share actor and a streams marker"
    )]
    async fn consumer_coordinator_group_kinds(consumer_heartbeat: &mut ConsumerHeartbeatScenario) {
        let fixture =
            versioned_group_broker(Arc::new(AllowAllAuthorizer), GroupFeatureLevel(1)).await;
        let coordinator = &fixture.broker.group_coordinator;
        coordinator.mark_share("share");
        coordinator.mark_streams("streams");
        let _share_actor = coordinator.get_or_create_share("share-actor");
        consumer_heartbeat.live = Some(fixture);
    }
    #[rstest_bdd_macros::given("consumer member m1 sends epoch {epoch} for group {group}")]
    fn consumer_group_creation_request(
        consumer_heartbeat: &mut ConsumerHeartbeatScenario,
        epoch: i32,
        group: String,
    ) {
        consumer_heartbeat.request = Some(ConsumerGroupHeartbeatRequest {
            group_id: group,
            member_id: "m1".into(),
            instance_id: (epoch == -2).then(|| "i1".into()),
            member_epoch: epoch,
            rebalance_timeout_ms: if epoch == 0 { 30_000 } else { -1 },
            topic_partitions: (epoch == 0).then(Vec::new),
            subscribed_topic_names: Some(vec!["topic-a".into()]),
            ..Default::default()
        });
    }
    #[rstest_bdd_macros::when("the consumer coordinator handles the member heartbeat")]
    async fn handle_consumer_group_creation(consumer_heartbeat: &mut ConsumerHeartbeatScenario) {
        let broker = &consumer_heartbeat.live.as_ref().unwrap().broker;
        krabka_macros::request_test_identity! {bind(ctx);user ["ANONYMOUS"];};
        consumer_heartbeat.response = Some(
            handle(
                broker,
                consumer_heartbeat.request.as_ref().unwrap().clone(),
                VERSION,
                &ctx,
            )
            .await
            .expect("ConsumerGroupHeartbeat handler"),
        );
    }
    #[rstest_bdd_macros::then(
        "the consumer coordinator returns error {code} message {message} and creates only accepted joins"
    )]
    async fn check_consumer_group_creation(
        consumer_heartbeat: &mut ConsumerHeartbeatScenario,
        code: i16,
        message: String,
    ) {
        let response = consumer_heartbeat.response.as_ref().unwrap();
        if code == codes::NONE {
            assert!(response.error_code == codes::NONE);
        } else {
            assert!(
                response
                    == &ConsumerGroupHeartbeatResponse {
                        error_code: code,
                        error_message: Some(message),
                        ..Default::default()
                    }
            );
        }
        let fixture = consumer_heartbeat.live.as_ref().unwrap();
        let coordinator = &fixture.broker.group_coordinator;
        assert!(coordinator.find("joined").is_some() == (code == codes::NONE));
        for group in [
            "absent-heartbeat",
            "absent-leave",
            "absent-static-leave",
            "share",
        ] {
            assert!(coordinator.find(group).is_none(), "{group}");
        }
        consumer_heartbeat
            .live
            .take()
            .unwrap()
            .handle
            .shutdown()
            .await;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_consumer_group_creation/creation.feature",
        name = "Only joins can create consumer groups and other group kinds are refused"
    )]
    #[tokio::test]
    async fn handle_creates_consumer_group_only_on_join_as_kafka_does(
        consumer_heartbeat: ConsumerHeartbeatScenario,
        epoch: i32,
        group: String,
        code: i16,
        message: String,
    ) {
        let _ = (consumer_heartbeat, epoch, group, code, message);
    }

    /// Kafka's `String.trim` blank: a control character is blank and a
    /// no-break space is not.
    #[rstest::rstest]
    #[case::control_char("\u{1}", Some("MemberId can't be empty."))]
    #[case::no_break_space("\u{A0}", None)]
    fn member_id_blankness_follows_java_trim(
        #[case] member_id: &str,
        #[case] refusal: Option<&str>,
    ) {
        let request = subscription_request("g", member_id, 0);

        let outcome = validate_request(&request, VERSION, &NextGenConfig::default());

        assert!(
            outcome.err().map(|error| *error)
                == refusal.map(|message| ConsumerGroupHeartbeatResponse {
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some(message.to_owned()),
                    ..Default::default()
                })
        );
    }

    #[test]
    fn error_response_preserves_error_code() {
        let resp = ConsumerGroupHeartbeatResponse::error(codes::GROUP_AUTHORIZATION_FAILED, None);
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        check_group_read_denied().call();

        let bytes = encode_response(
            &ConsumerGroupHeartbeatResponse::error(codes::GROUP_AUTHORIZATION_FAILED, None),
            VERSION,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = ConsumerGroupHeartbeatResponse::decode(&mut cur, VERSION).unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }

    #[test]
    fn group_read_denied_allows_allow_all_authorizer() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["ANONYMOUS"];
            client ["consumer-client"];
        };

        assert!(!group_read_denied(&AllowAllAuthorizer, &image, &ctx, "g"));
    }

    async fn check_denied_group_response(broker: &Broker, expected: KafkaErrorCode) {
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["ANONYMOUS"];
        };
        let resp = handle(broker, request("denied-group"), VERSION, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        assert!(resp.error_code == expected.0, "{resp:?}");
    }

    #[tokio::test]
    async fn handle_group_read_denied_preserves_error_response() {
        let authorizer = acl_authorizer();
        let LiveBrokerFixture {
            handle: broker_handle,
            directory: _dir,
            broker,
        } = versioned_group_broker(Arc::new(authorizer), GroupFeatureLevel(1)).await;
        check_denied_group_response(&broker, KafkaErrorCode(codes::GROUP_AUTHORIZATION_FAILED))
            .await;

        broker_handle.shutdown().await;
    }

    /// The protocol gate — `UNSUPPORTED_VERSION` when `group.version` is not
    /// finalized — runs BEFORE the group ACL check, matching Kafka's
    /// `handleConsumerGroupHeartbeat`. A principal denied `Read` on the group
    /// still gets `UNSUPPORTED_VERSION`, not `GROUP_AUTHORIZATION_FAILED`,
    /// when the protocol itself is unavailable.
    #[tokio::test]
    async fn handle_protocol_gate_precedes_group_acl() {
        let authorizer = acl_authorizer();
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_group_broker(Arc::new(authorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        // `start_broker`'s bootstrap seeds every feature at its release
        // default for a modern metadata.version, which finalizes
        // group.version >= 1 automatically. Explicitly downgrade it back to
        // 0 (unfinalized/disabled) so this test observes the protocol gate.
        set_group_version(&broker, GroupFeatureLevel(0)).await;
        check_denied_group_response(&broker, KafkaErrorCode(codes::UNSUPPORTED_VERSION)).await;

        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_persists_request_client_identity() {
        let LiveBrokerFixture {
            handle: broker_handle,
            directory: _dir,
            broker,
        } = versioned_group_broker(Arc::new(AllowAllAuthorizer), GroupFeatureLevel(1)).await;
        krabka_macros::request_test_identity! {
            bind(principal, peer, ctx);
            user ["ANONYMOUS"];
        };

        let resp = handle(&broker, request("identity-group"), VERSION, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        assert!(resp.error_code == 0);

        let actor = broker
            .group_coordinator
            .get_or_create_group("identity-group", GroupKindTag::Consumer);
        let view = ask(&actor.tx, |reply| GroupActorMessage::Describe { reply })
            .await
            .expect("consumer group view");

        assert!(view.members.len() == 1);
        assert!(view.members[0].client_id == "consumer-group-heartbeat-test");
        assert!(view.members[0].client_host == "/127.0.0.1");

        let member_id = view.members[0].member_id.clone();
        let member_epoch = view.members[0].member_epoch;
        let peer = SocketAddr::from(([127, 0, 0, 2], 9093));
        let ctx = request_context(&principal, &peer, "consumer-client-b");
        let req = subscription_request("identity-group", &member_id, member_epoch);

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat identity refresh");
        assert!(resp.error_code == 0);

        let view = ask(&actor.tx, |reply| GroupActorMessage::Describe { reply })
            .await
            .expect("refreshed consumer group view");
        assert!(view.members[0].client_id == "consumer-client-b");
        assert!(view.members[0].client_host == "/127.0.0.2");

        broker_handle.shutdown().await;
    }

    #[test]
    fn subscribed_names_describe_denied_table() { subscribed_names_describe_denied_cases(); }

    // ── Explicit-name and regex `Describe` checks (issue #716) ─────────

    /// A `SubscribedTopicNames` entry this principal cannot `Describe` fails
    /// the whole heartbeat with `TOPIC_AUTHORIZATION_FAILED` (29), and the
    /// group is left with no member — Kafka never builds the coordinator
    /// record in this case, so an attacker cannot use group membership to
    /// learn a denied topic's id or partitions.
    #[tokio::test]
    async fn handle_subscribed_name_describe_denied_refuses_whole_heartbeat_no_member_created() {
        // Deliberately no Describe grant for "topic-a".
        let LiveBrokerFixture {
            handle: broker_handle,
            directory: _dir,
            broker,
        } = versioned_group_broker(Arc::new(acl_authorizer()), GroupFeatureLevel(1)).await;
        grant_group_read(&broker, "g").await;
        let RequestIdentity { principal, peer } = alice_peer();
        let ctx = request_context(&principal, &peer, "c");
        let req = subscription_request("g", "", 0);

        let resp = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        assert!(
            resp.error_code == codes::TOPIC_AUTHORIZATION_FAILED,
            "{resp:?}"
        );

        let actor = broker
            .group_coordinator
            .get_or_create_group("g", GroupKindTag::Consumer);
        let view = ask(&actor.tx, |reply| GroupActorMessage::Describe { reply })
            .await
            .expect("consumer group view");
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
        let LiveBrokerFixture {
            handle: broker_handle,
            directory: _dir,
            broker,
        } = versioned_group_broker(Arc::new(acl_authorizer()), GroupFeatureLevel(1)).await;
        let allowed_id = uuid::Uuid::from_u128(1);
        let denied_id = uuid::Uuid::from_u128(2);
        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![group_read_acl("g"), describe_acl("orders-eu")];
        records.extend(
            topic_with_partitions()
                .name("orders-eu")
                .topic_id(allowed_id)
                .node(node)
                .call(),
        );
        records.extend(
            topic_with_partitions()
                .name("orders-us")
                .topic_id(denied_id)
                .node(node)
                .call(),
        );
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create topics");
        let RequestIdentity { principal, peer } = alice_peer();
        let ctx = request_context(&principal, &peer, "c");
        let req = ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "regex-member".into(),
            rebalance_timeout_ms: 30_000,
            topic_partitions: Some(vec![]),
            subscribed_topic_regex: Some("^orders-.*".into()),
            ..Default::default()
        };

        let joined = handle(&broker, req, VERSION, &ctx)
            .await
            .expect("ConsumerGroupHeartbeat handler");
        assert!(joined.error_code == codes::NONE, "{joined:?}");
        // Kafka writes the resolution after the join's batch, and the member's
        // next heartbeat computes the target that holds its topics.
        let resp = handle(
            &broker,
            ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "regex-member".into(),
                member_epoch: joined.member_epoch,
                rebalance_timeout_ms: -1,
                ..Default::default()
            },
            VERSION,
            &ctx,
        )
        .await
        .expect("ConsumerGroupHeartbeat handler");
        assert!(resp.error_code == codes::NONE, "{resp:?}");
        let assigned: HashSet<uuid::Uuid> = resp
            .assignment
            .into_iter()
            .flat_map(|a| a.topic_partitions)
            .map(|tp| uuid::Uuid::from_bytes(tp.topic_id.0))
            .collect();
        assert!(assigned == HashSet::from([allowed_id]));

        broker_handle.shutdown().await;
    }

    // ── Regex resolution that follows the metadata ─────────────────────

    /// The topics a member of group `g` holds, as its heartbeats report them.
    ///
    /// The response carries the assignment only when it changed, so the set
    /// keeps the last one.
    struct RegexMember {
        broker: Arc<Broker>,
        member_id: &'static str,
        member_epoch: i32,
        assigned: HashSet<uuid::Uuid>,
    }

    impl RegexMember {
        fn new(broker: Arc<Broker>, member_id: &'static str) -> Self {
            Self {
                broker,
                member_id,
                member_epoch: 0,
                assigned: HashSet::new(),
            }
        }

        /// Sends one heartbeat as `alice`, with `regex` only when it is the
        /// join or when the pattern changes, as the Java client does.
        async fn heartbeat(&mut self, regex: Option<&str>) {
            let RequestIdentity { principal, peer } = alice_peer();
            let ctx = request_context(&principal, &peer, "c");
            let joining = self.member_epoch == 0;
            let req = ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: self.member_id.into(),
                member_epoch: self.member_epoch,
                rebalance_timeout_ms: if joining { 30_000 } else { -1 },
                topic_partitions: joining.then(Vec::new),
                subscribed_topic_regex: regex.map(str::to_owned),
                ..Default::default()
            };
            let resp = handle(&self.broker, req, VERSION, &ctx)
                .await
                .expect("ConsumerGroupHeartbeat handler");
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
            let expected: HashSet<uuid::Uuid> = expected.iter().copied().collect();
            for _ in 0..200 {
                self.heartbeat(None).await;
                if self.assigned == expected {
                    return;
                }
                time::sleep(Duration::from_millis(50)).await;
            }
            panic!(
                "member {} holds {:?} and never held {expected:?}",
                self.member_id, self.assigned
            );
        }
    }

    /// A topic the test broker creates, with the id it creates it under.
    struct DescribedTopic<'a> {
        name: &'a str,
        id: uuid::Uuid,
    }

    /// A running broker and the temporary directory that holds its data.
    struct StartedBroker {
        handle: BrokerHandle,
        dir: tempfile::TempDir,
    }

    /// A broker whose group `g` and topics `topics` (each with two partitions
    /// and a `Describe` grant for alice) exist, and the broker's handle.
    async fn broker_with_described_topics(topics: &[DescribedTopic<'_>]) -> StartedBroker {
        let LiveBrokerFixture {
            handle: broker_handle,
            directory: dir,
            broker,
        } = versioned_group_broker(Arc::new(acl_authorizer()), GroupFeatureLevel(1)).await;
        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![group_read_acl("g")];
        for DescribedTopic { name, id } in topics {
            records.push(describe_acl(name));
            records.extend(
                topic_with_partitions()
                    .name(name)
                    .topic_id(*id)
                    .node(node)
                    .call(),
            );
        }
        broker
            .controller
            .submit_change(records)
            .await
            .expect("grant ACLs and create topics");
        StartedBroker {
            handle: broker_handle,
            dir,
        }
    }

    /// A joined member and the broker resources kept until shutdown.
    struct JoinedRegexMember {
        handle: BrokerHandle,
        directory: tempfile::TempDir,
        broker: Arc<Broker>,
        member: RegexMember,
    }

    /// Kafka refreshes the resolution of a group's regular expressions when the
    /// metadata image has new topics (`lastMetadataImageWithNewTopics`), at the
    /// next heartbeat of any member. A Java client sends its pattern only when
    /// it joins, so a topic created after the join reaches the member only
    /// through that refresh.
    async fn joined_regex_member(topics: &[DescribedTopic<'_>]) -> JoinedRegexMember {
        let StartedBroker {
            handle,
            dir: directory,
        } = broker_with_described_topics(topics).await;
        let broker = handle.broker_arc_for_test();
        let mut member = RegexMember::new(Arc::clone(&broker), "m1");
        member.heartbeat(Some("^orders-.*")).await;
        JoinedRegexMember {
            handle,
            directory,
            broker,
            member,
        }
    }
    #[tokio::test]
    async fn a_topic_created_after_the_join_is_assigned_to_a_member_that_never_resends_its_pattern()
    {
        let first = uuid::Uuid::from_u128(1);
        let second = uuid::Uuid::from_u128(2);
        let JoinedRegexMember {
            handle: broker_handle,
            directory: _dir,
            broker,
            mut member,
        } = joined_regex_member(&[DescribedTopic {
            name: "orders-eu",
            id: first,
        }])
        .await;
        member.heartbeat_until_holding(&[first]).await;

        let node = krabka_raft::NodeId(broker_handle.node_id());
        let mut records = vec![describe_acl("orders-us")];
        records.extend(
            topic_with_partitions()
                .name("orders-us")
                .topic_id(second)
                .node(node)
                .call(),
        );
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
        let JoinedRegexMember {
            handle: broker_handle,
            directory: _dir,
            broker,
            mut member,
        } = joined_regex_member(&[
            DescribedTopic {
                name: "orders-eu",
                id: allowed,
            },
            DescribedTopic {
                name: "orders-us",
                id: revoked,
            },
        ])
        .await;
        member.heartbeat_until_holding(&[allowed, revoked]).await;

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
