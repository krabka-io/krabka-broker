//! Handler tests for the request-level checks that `ShareFetch` and
//! `ShareAcknowledge` run before the share session.
//!
//! Kafka's `KafkaApis.handleShareFetchRequest` and
//! `KafkaApis.handleShareAcknowledgeRequest` refuse a null group id with
//! `INVALID_REQUEST`, then check `Read` on the group, then refuse a member id
//! that `isMemberIdValid` does not accept (null, empty, or longer than 36
//! characters) with `INVALID_REQUEST`. They do not ask the group coordinator
//! whether the member exists.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        authorizer::AuthorizationResult,
        broker::Broker,
        codes,
        handlers::{RequestContext, test_support::StartedBroker},
        test_support::{
            decode_response, encode_request, start_broker_no_audit_with,
            tolerate_starved_heartbeats, try_dispatch_context,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::{
        owned,
        owned::{
            share_acknowledge_request::ShareAcknowledgeRequest,
            share_acknowledge_response::{self, ShareAcknowledgeResponse},
            share_fetch_request::{API_KEY, ShareFetchRequest},
            share_fetch_response::{self, ShareFetchResponse},
        },
    },
    std::sync::Arc,
};

/// The share group that the principal may read.
const READABLE: &str = "readable-group";

/// The acquisition lock timeout of the test broker's share-group config.
const LOCK_TIMEOUT_MS: i32 = 30_000;

/// Allows `Read` on [`READABLE`] only, and every other operation.
#[derive(Debug)]
struct ReadOneGroup;

krabka_macros::test_authorizer!(ReadOneGroup, request; {
    if request.resource_type == ResourceType::Group
        && request.operation == AclOperation::Read
        && request.resource_name != READABLE
    {
        AuthorizationResult::Deny
    } else {
        AuthorizationResult::Allow
    }
});

async fn start() -> StartedBroker {
    let HandlerStartedBroker { broker, dir } = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(ReadOneGroup);
        tolerate_starved_heartbeats(cfg);
    })
    .await;
    StartedBroker { broker, dir }
}

/// The top-level shape Kafka answers: rows and a lock timeout only when the
/// request got past every check.
fn fetch_response(error_code: i16) -> ShareFetchResponse {
    ShareFetchResponse {
        error_code,
        acquisition_lock_timeout_ms: if error_code == codes::NONE {
            LOCK_TIMEOUT_MS
        } else {
            0
        },
        ..Default::default()
    }
}

fn acknowledge_response(error_code: i16) -> ShareAcknowledgeResponse {
    ShareAcknowledgeResponse {
        error_code,
        acquisition_lock_timeout_ms: if error_code == codes::NONE {
            LOCK_TIMEOUT_MS
        } else {
            0
        },
        ..Default::default()
    }
}

/// Dispatches one `ShareFetch` request and returns the encoded response.
async fn dispatch_share_fetch(
    broker: &Broker,
    fetch: &ShareFetchRequest,
    version: i16,
    ctx: &RequestContext<'_>,
) -> Bytes {
    try_dispatch_context(
        broker,
        API_KEY,
        version,
        &encode_request(fetch, version),
        ctx,
    )
    .await
    .expect("handle share fetch")
}

#[derive(Default)]
struct ShareRequestIdentityScenario {
    started: Option<StartedBroker>,
    group: Option<String>,
    member: Option<String>,
    fetch: Option<ShareFetchResponse>,
    acknowledgement: Option<ShareAcknowledgeResponse>,
}
#[rstest::fixture]
fn share_request_identity() -> ShareRequestIdentityScenario {
    let started = None;
    ShareRequestIdentityScenario {
        started,
        ..Default::default()
    }
}
#[rstest_bdd_macros::given("only share group readable-group grants the consumer Read permission")]
async fn share_request_authorization(share_request_identity: &mut ShareRequestIdentityScenario) {
    share_request_identity.started = Some(start().await);
}
#[rstest_bdd_macros::given("share requests name group {group} and member {member}")]
fn share_request_ids(
    share_request_identity: &mut ShareRequestIdentityScenario,
    group: String,
    member: String,
) {
    share_request_identity.group = if group == "null" { None } else { Some(group) };
    share_request_identity.member = match member.as_str() {
        "null" => None,
        "empty" => Some(String::new()),
        "36 characters" => Some("m".repeat(36)),
        "37 characters" => Some("m".repeat(37)),
        _ => panic!("unknown share member input: {member}"),
    };
}
#[rstest_bdd_macros::when(
    "the consumer sends ShareFetch at epoch 0 followed by ShareAcknowledge at epoch 1"
)]
async fn handle_share_request_ids(share_request_identity: &mut ShareRequestIdentityScenario) {
    let broker = share_request_identity
        .started
        .as_ref()
        .unwrap()
        .broker
        .broker_arc_for_test();
    krabka_macros::request_test_identity! {bind(user,address,ctx);user ["share-consumer"];client ["share-client"];};
    let fetch_version = share_fetch_response::MAX_VERSION;
    let acknowledge_version = share_acknowledge_response::MAX_VERSION;
    let fetch = ShareFetchRequest {
        group_id: share_request_identity.group.clone(),
        member_id: share_request_identity.member.clone(),
        share_session_epoch: 0,
        ..Default::default()
    };
    let fetched = dispatch_share_fetch(&broker, &fetch, fetch_version, &ctx).await;
    share_request_identity.fetch = Some(decode_response::<ShareFetchResponse>(
        &fetched,
        fetch_version,
    ));
    let acknowledge = ShareAcknowledgeRequest {
        group_id: share_request_identity.group.clone(),
        member_id: share_request_identity.member.clone(),
        share_session_epoch: 1,
        ..Default::default()
    };
    let acknowledged = try_dispatch_context(
        &broker,
        owned::share_acknowledge_request::API_KEY,
        acknowledge_version,
        &encode_request(&acknowledge, acknowledge_version),
        &ctx,
    )
    .await
    .expect("handle share acknowledge");
    share_request_identity.acknowledgement = Some(decode_response::<ShareAcknowledgeResponse>(
        &acknowledged,
        acknowledge_version,
    ));
}
#[rstest_bdd_macros::then(
    "the complete ShareFetch response has error {fetch} and ShareAcknowledge has error {acknowledge}"
)]
async fn check_share_request_ids(
    share_request_identity: &mut ShareRequestIdentityScenario,
    fetch: i16,
    acknowledge: i16,
) {
    assert!(share_request_identity.fetch.as_ref().unwrap() == &fetch_response(fetch));
    assert!(
        share_request_identity.acknowledgement.as_ref().unwrap()
            == &acknowledge_response(acknowledge)
    );
    share_request_identity
        .started
        .take()
        .unwrap()
        .broker
        .shutdown()
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/handler_share_request_identity/identity.feature",
    name = "Null group authorization and member validity are checked in Kafka order"
)]
#[tokio::test]
async fn group_and_member_ids_are_checked_in_kafka_order(
    share_request_identity: ShareRequestIdentityScenario,
    group: String,
    member: String,
    fetch: i16,
    acknowledge: i16,
) {
    let _ = (share_request_identity, group, member, fetch, acknowledge);
}

/// Kafka's `ShareFetch` does not look the member up in the group
/// coordinator, so a group with a live share actor that does not know the
/// member still serves it.
#[tokio::test]
async fn a_member_the_group_does_not_know_still_fetches() {
    let StartedBroker { broker, dir: _dir } = start().await;
    let shared = broker.broker_arc_for_test();
    krabka_macros::request_test_identity!(bind(user, address, ctx); user ["share-consumer"]; client ["share-client"];);
    let version = share_fetch_response::MAX_VERSION;
    let _actor = shared.group_coordinator.get_or_create_share(READABLE);

    let fetch = ShareFetchRequest {
        group_id: Some(READABLE.into()),
        member_id: Some("not-a-member".into()),
        share_session_epoch: 0,
        ..Default::default()
    };
    let response = dispatch_share_fetch(&shared, &fetch, version, &ctx).await;

    assert!(
        decode_response::<ShareFetchResponse>(&response, version) == fetch_response(codes::NONE)
    );
    broker.shutdown().await;
}
