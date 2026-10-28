#![cfg(test)]

//! KIP-559: the `JoinGroup` v7+ and `SyncGroup` v5+ responses carry
//! `protocol_type` and `protocol_name`, so an L7 proxy can identify the
//! group-coordination exchange without remembering the earlier one.
//!
//! `Client::send` negotiates to the broker `MAX_VERSION`, which is v9 for
//! `JoinGroup` and v5 for `SyncGroup`, so these tests always exercise the
//! KIP-559 fields.

use {
    crate::support::classic::{
        ClassicJoinSetup, ClassicTimeouts, classic_join_request, classic_join_request_from,
        classic_sync_request, join_protocol, sync_assignment,
    },
    assert2::{assert, check},
    bytes::Bytes,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
};
mod support;

use krabka_protocol::owned::{
    join_group_response::JoinGroupResponse, sync_group_response::SyncGroupResponse,
};

const GROUP: &str = "kip559-grp";
const PROTOCOL_TYPE: &str = "consumer";
const PROTOCOL_NAME: &str = "range";

/// Boots one broker and waits until its group coordinator serves
/// `__consumer_offsets`. No broker creates that topic when it starts.
async fn start() -> support::InProcess {
    let p = support::start().await;
    p.broker.wait_until_group_coordinator_ready().await;
    p
}

fn join_setup() -> ClassicJoinSetup {
    ClassicJoinSetup {
        group_id: GROUP.into(),
        timeouts: ClassicTimeouts {
            rebalance: krabka_units::millis(1_500),
            ..Default::default()
        },
        protocol_type: PROTOCOL_TYPE.into(),
        protocols: vec![join_protocol(PROTOCOL_NAME, Bytes::new())],
        ..Default::default()
    }
}

struct ProtocolDiscovery {
    broker: Option<InProcess>,
    bootstrap: Option<JoinGroupResponse>,
    joined: Option<JoinGroupResponse>,
    synchronized: Option<SyncGroupResponse>,
}

#[fixture]
fn protocol() -> ProtocolDiscovery {
    // Start this scenario with fresh inputs and observations.

    ProtocolDiscovery {
        broker: None,
        bootstrap: None,
        joined: None,
        synchronized: None,
    }
}

#[given("a ready coordinator for proxy-visible group negotiation")]
async fn ready_coordinator(protocol: &mut ProtocolDiscovery) {
    protocol.broker = Some(start().await);
}

#[when("a member joins the consumer range group")]
async fn join_consumer(protocol: &mut ProtocolDiscovery) {
    let p = protocol.broker.as_ref().expect("a coordinator was started");
    let bootstrap = p
        .client
        .send(classic_join_request_from(join_setup()))
        .await
        .expect("JoinGroup bootstrap");
    let mut rejoin = join_setup();
    rejoin.member_id.clone_from(&bootstrap.member_id);
    let joined = p
        .client
        .send(
            classic_join_request()
                .group_id(rejoin.group_id)
                .member_id(rejoin.member_id)
                .timeouts(rejoin.timeouts)
                .protocol_type(rejoin.protocol_type)
                .protocols(rejoin.protocols)
                .call(),
        )
        .await
        .expect("JoinGroup");
    protocol.bootstrap = Some(bootstrap);
    protocol.joined = Some(joined);
}

#[then("membership bootstrap supplies a nonempty member id")]
fn assigned_member(protocol: &ProtocolDiscovery) {
    let bootstrap = protocol
        .bootstrap
        .as_ref()
        .expect("membership bootstrap completed");
    assert!(bootstrap.error_code == 79, "expected MEMBER_ID_REQUIRED");
    assert!(!bootstrap.member_id.is_empty());
}

#[then("successful JoinGroup identifies consumer and range")]
fn joined_protocol(protocol: &ProtocolDiscovery) {
    let joined = protocol.joined.as_ref().expect("JoinGroup completed");
    check!(joined.error_code == 0, "JoinGroup must succeed: {joined:?}");
    check!(
        joined.protocol_type.as_deref() == Some(PROTOCOL_TYPE),
        "JoinGroup must echo protocol_type: {joined:?}"
    );
    check!(
        joined.protocol_name.as_deref() == Some(PROTOCOL_NAME),
        "JoinGroup must echo protocol_name: {joined:?}"
    );
}

#[when("the joined member synchronizes its assignment")]
async fn sync_member(protocol: &mut ProtocolDiscovery) {
    let p = protocol.broker.as_ref().expect("a coordinator was started");
    let joined = protocol.joined.as_ref().expect("a member joined");
    let member_id = protocol
        .bootstrap
        .as_ref()
        .expect("membership bootstrap completed")
        .member_id
        .clone();
    protocol.synchronized = Some(
        p.client
            .send(
                classic_sync_request()
                    .group_id(GROUP.into())
                    .generation_id(joined.generation_id)
                    .member_id(member_id.clone())
                    .maybe_protocol_type(Some(Some(PROTOCOL_TYPE.into())))
                    .maybe_protocol_name(Some(Some(PROTOCOL_NAME.into())))
                    .assignments(vec![sync_assignment(
                        member_id,
                        Bytes::from_static(b"asgn"),
                    )])
                    .call(),
            )
            .await
            .expect("SyncGroup"),
    );
}

#[then("successful SyncGroup identifies consumer and range")]
fn synchronized_protocol(protocol: &ProtocolDiscovery) {
    let response = protocol.synchronized.as_ref().expect("SyncGroup completed");
    check!(
        response.error_code == 0,
        "SyncGroup must succeed: {response:?}"
    );
    check!(
        response.protocol_type.as_deref() == Some(PROTOCOL_TYPE),
        "SyncGroup must echo protocol_type: {response:?}"
    );
    check!(
        response.protocol_name.as_deref() == Some(PROTOCOL_NAME),
        "SyncGroup must echo protocol_name: {response:?}"
    );
}

#[when("another member tries to join using the stream protocol")]
async fn join_incompatible(protocol: &mut ProtocolDiscovery) {
    let p = protocol.broker.as_ref().expect("a coordinator was started");
    protocol.joined = Some(
        p.client
            .send(
                classic_join_request()
                    .group_id(join_setup().group_id)
                    .member_id(join_setup().member_id)
                    .timeouts(join_setup().timeouts)
                    .protocol_type("stream".into())
                    .protocols(vec![join_protocol("doesnt-matter", Bytes::new())])
                    .call(),
            )
            .await
            .expect("JoinGroup"),
    );
}

#[then("JoinGroup returns only INCONSISTENT_GROUP_PROTOCOL without protocol identity")]
fn join_refusal(protocol: &ProtocolDiscovery) {
    let response = protocol
        .joined
        .as_ref()
        .expect("the incompatible join completed");
    assert!(
        *response
            == JoinGroupResponse {
                error_code: 23,
                protocol_name: None,
                ..Default::default()
            },
        "expected a bare INCONSISTENT_GROUP_PROTOCOL (23), got {response:?}"
    );
}

#[when("an unknown member tries to synchronize the current generation")]
async fn sync_unknown_member(protocol: &mut ProtocolDiscovery) {
    let p = protocol.broker.as_ref().expect("a coordinator was started");
    let generation = protocol
        .joined
        .as_ref()
        .expect("a member joined")
        .generation_id;
    protocol.synchronized = Some(
        p.client
            .send(
                classic_sync_request()
                    .group_id(GROUP.into())
                    .generation_id(generation)
                    .member_id("ghost-member".into())
                    .maybe_protocol_type(Some(Some(PROTOCOL_TYPE.into())))
                    .maybe_protocol_name(Some(Some(PROTOCOL_NAME.into())))
                    .call(),
            )
            .await
            .expect("SyncGroup"),
    );
}

#[then("SyncGroup returns only UNKNOWN_MEMBER_ID without protocol identity or assignment")]
fn sync_refusal(protocol: &ProtocolDiscovery) {
    let response = protocol
        .synchronized
        .as_ref()
        .expect("the unknown member synchronization completed");
    check!(
        *response
            == SyncGroupResponse {
                error_code: 25,
                protocol_type: None,
                protocol_name: None,
                assignment: Bytes::default(),
                ..Default::default()
            }
    );
}

async fn stop_coordinator(protocol: ProtocolDiscovery) {
    protocol
        .broker
        .expect("a coordinator was started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip559_l7_proxy_fields.feature",
    name = "JoinGroup exposes its negotiated protocol to a stateless proxy"
)]
#[tokio::test]
async fn join_group_response_carries_protocol_type_and_name_on_success(
    protocol: ProtocolDiscovery,
) {
    stop_coordinator(protocol).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip559_l7_proxy_fields.feature",
    name = "SyncGroup exposes its negotiated protocol to a stateless proxy"
)]
#[tokio::test]
async fn sync_group_response_carries_protocol_type_and_name_on_success(
    protocol: ProtocolDiscovery,
) {
    stop_coordinator(protocol).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip559_l7_proxy_fields.feature",
    name = "A refused incompatible join does not advertise a negotiated protocol"
)]
#[tokio::test]
async fn join_group_inconsistent_protocol_error_carries_no_protocol_fields(
    protocol: ProtocolDiscovery,
) {
    stop_coordinator(protocol).await;
}

#[scenario(
    path = "tests/features/migrated/tests/kip559_l7_proxy_fields.feature",
    name = "A refused synchronization does not advertise a protocol or assignment"
)]
#[tokio::test]
async fn sync_group_error_carries_no_protocol_fields(protocol: ProtocolDiscovery) {
    stop_coordinator(protocol).await;
}
