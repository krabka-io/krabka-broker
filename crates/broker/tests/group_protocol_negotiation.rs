#![cfg(test)]

//! KIP-429 batch-4 T4A: broker-side `JoinGroup` protocol-set negotiation tests.

mod support;

use {
    crate::support::{
        classic::{ClassicTimeouts, classic_join_request, join_protocol},
        client::connect_owned,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        join_group_request::JoinGroupRequest, join_group_response::JoinGroupResponse,
    },
    std::{iter, mem, time::Duration},
    tokio::time,
};

// Kafka error codes consumed by these tests.
const ERR_NONE: i16 = 0;
const ERR_INCONSISTENT_GROUP_PROTOCOL: i16 = 23;
const ERR_MEMBER_ID_REQUIRED: i16 = 79;

/// Boots a single-broker no-auth test cluster. It returns the handle, a shared
/// bootstrap address string, and the tempdir guard.
///
/// Tests build a client per member with `connect_client`, because the broker
/// processes the requests on one TCP connection in sequence. Two concurrent
/// `JoinGroup` waits over one `Client` would deadlock the second member behind
/// the first member's `INITIAL_REBALANCE_DELAY` wait.
use crate::support::start_group_coordinator as start_broker;

/// Builds a fresh `Client`, and therefore a fresh TCP connection, against
/// `bootstrap`. Each member in a concurrent test gets its own client.
async fn connect_client(bootstrap: &str, client_id: &str) -> Client {
    connect_owned(bootstrap, client_id, "client build").await
}

/// Builds a `JoinGroup` request that proposes `protocols` in caller order,
/// with `protocol_type` and `member_id`. `session_timeout` and
/// `rebalance_timeout` are short, so a stuck rebalance fails the test quickly
/// instead of holding the runtime.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ProtocolJoinSetup<'a> {
    #[default("g")]
    group_id: &'a str,
    member_id: &'a str,
    #[default("consumer")]
    protocol_type: &'a str,
    protocols: &'a [ProposedProtocol<'a>],
}

/// A protocol a member proposes: its name and its metadata.
#[derive(Clone, Copy)]
struct ProposedProtocol<'a> {
    name: &'a str,
    metadata: &'a [u8],
}

impl<'a> ProposedProtocol<'a> {
    const fn new(name: &'a str, metadata: &'a [u8]) -> Self { Self { name, metadata } }
}

#[bon::builder]
fn join_group_request(
    #[builder(default = "g")] group_id: &'_ str,
    #[builder(default)] member_id: &'_ str,
    #[builder(default = "consumer")] protocol_type: &'_ str,
    #[builder(default)] protocols: &'_ [ProposedProtocol<'_>],
) -> JoinGroupRequest {
    let setup = ProtocolJoinSetup {
        group_id,
        member_id,
        protocol_type,
        protocols,
    };
    JoinGroupRequest {
        group_instance_id: None,
        ..classic_join_request()
            .group_id(setup.group_id.to_owned())
            .member_id(setup.member_id.to_owned())
            .timeouts(ClassicTimeouts {
                rebalance: krabka_units::millis(60_000),
                ..Default::default()
            })
            .protocol_type(setup.protocol_type.to_owned())
            .protocols(
                setup
                    .protocols
                    .iter()
                    .map(|protocol| {
                        join_protocol(
                            protocol.name.to_owned(),
                            Bytes::copy_from_slice(protocol.metadata),
                        )
                    })
                    .collect(),
            )
            .call()
    }
}

/// First-round `JoinGroup` with an empty `member_id`. The broker replies with
/// `MEMBER_ID_REQUIRED (79)` and the member id that it generated (KIP-394).
/// This function asserts both, and returns the member id for the second
/// round.
async fn bootstrap_member_id(
    client: &Client,
    group_id: &str,
    protocol_type: &str,
    protocols: &[ProposedProtocol<'_>],
) -> String {
    let req = join_group_request()
        .group_id(group_id)
        .protocol_type(protocol_type)
        .protocols(protocols)
        .call();
    let resp = client
        .send(req)
        .await
        .expect("first JoinGroup must round-trip");
    assert!(
        resp.error_code == ERR_MEMBER_ID_REQUIRED,
        "first JoinGroup (empty member_id) must return MEMBER_ID_REQUIRED (79), got {resp:?}"
    );
    assert!(
        !resp.member_id.is_empty(),
        "broker must return a non-empty generated member_id on MEMBER_ID_REQUIRED"
    );
    resp.member_id
}

/// Second-round `JoinGroup` with the member id that the broker supplied. It
/// returns the raw response, so the caller can assert on `error_code` and
/// `protocol_name`.
async fn second_join(
    client: &Client,
    group_id: &str,
    member_id: &str,
    protocol_type: &str,
    protocols: &[ProposedProtocol<'_>],
) -> JoinGroupResponse {
    let req = join_group_request()
        .group_id(group_id)
        .member_id(member_id)
        .protocol_type(protocol_type)
        .protocols(protocols)
        .call();
    client
        .send(req)
        .await
        .expect("second JoinGroup must round-trip")
}

/// Two-step `JoinGroup` against a group that is already stable, or that has a
/// single member. It bootstraps a member id, then joins again at once.
///
/// The second call blocks for up to `INITIAL_REBALANCE_DELAY`, about 3 s,
/// before the broker completes the rebalance and returns NONE. It returns the
/// full response.
async fn full_join(
    client: &Client,
    group_id: &str,
    protocol_type: &str,
    protocols: &[ProposedProtocol<'_>],
) -> JoinGroupResponse {
    let member_id = bootstrap_member_id(client, group_id, protocol_type, protocols).await;
    second_join(client, group_id, &member_id, protocol_type, protocols).await
}

use {
    krabka_broker::BrokerHandle, rstest_bdd::datatable::Rows, rstest_bdd_macros::DataTableRow,
    tempfile::TempDir,
};
#[derive(DataTableRow)]
struct ProtocolMember {
    client: String,
    protocols: String,
}
struct RacingMember {
    client_id: String,
    protocols: Vec<String>,
    timeout_context: &'static str,
    join_context: &'static str,
}
struct ProtocolNegotiation {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    bootstrap: String,
    members: Vec<RacingMember>,
    responses: Vec<JoinGroupResponse>,
    clients: Vec<Client>,
    mismatch: Option<JoinGroupResponse>,
}
impl ProtocolNegotiation {
    async fn shutdown(self) {
        drop(self.clients);
        self.broker.expect("group broker started").shutdown().await;
    }
}
#[rstest::fixture]
fn negotiation() -> ProtocolNegotiation {
    // Start this scenario with fresh inputs and observations.

    ProtocolNegotiation {
        broker: None,
        directory: None,
        bootstrap: String::new(),
        members: Vec::new(),
        responses: Vec::new(),
        clients: Vec::new(),
        mismatch: None,
    }
}
#[rstest_bdd_macros::given("a classic group coordinator is ready")]
async fn group_protocol_broker(negotiation: &mut ProtocolNegotiation) {
    let (broker, bootstrap, directory) = start_broker().await;
    negotiation.broker = Some(broker);
    negotiation.bootstrap = bootstrap;
    negotiation.directory = Some(directory);
}
#[rstest_bdd_macros::given(
    "consumer members propose these ordered protocol names with empty metadata:"
)]
fn group_protocol_members(
    negotiation: &mut ProtocolNegotiation,
    #[datatable] rows: Rows<ProtocolMember>,
) {
    negotiation.members = rows
        .into_iter()
        .map(|row| RacingMember {
            client_id: row.client,
            protocols: row.protocols.split(',').map(str::to_owned).collect(),
            timeout_context: "member second JoinGroup timed out",
            join_context: "member JoinGroup task panic",
        })
        .collect();
}
#[rstest_bdd_macros::when("the members join group {group} concurrently on separate connections")]
async fn negotiate_group_protocol(negotiation: &mut ProtocolNegotiation, group: String) {
    negotiation.responses = race_consumer_joins(
        &negotiation.bootstrap,
        &group,
        mem::take(&mut negotiation.members),
    )
    .await;
}
#[rstest_bdd_macros::then("the sorted member error codes are exactly {codes}")]
fn group_protocol_errors(negotiation: &ProtocolNegotiation, codes: String) {
    let expected: Vec<i16> = codes
        .split(',')
        .map(|code| code.parse().expect("expected error code"))
        .collect();
    let mut actual: Vec<_> = negotiation
        .responses
        .iter()
        .map(|response| response.error_code)
        .collect();
    actual.sort_unstable();
    assert!(
        actual == expected,
        "member responses: {:?}",
        negotiation.responses
    );
}
#[rstest_bdd_macros::then("all {count} members succeed with selected protocol {protocol}")]
fn group_protocol_chosen(negotiation: &ProtocolNegotiation, count: usize, protocol: String) {
    assert!(negotiation.responses.len() == count);
    for response in &negotiation.responses {
        assert!(
            response.error_code == ERR_NONE,
            "member must succeed: {response:?}"
        );
        assert!(
            response.protocol_name.as_deref() == Some(protocol.as_str()),
            "selected protocol: {response:?}"
        );
    }
}
#[rstest_bdd_macros::when(
    "member-a establishes group cg-type-mismatch with consumer protocol type and range"
)]
async fn establish_consumer_protocol_type(negotiation: &mut ProtocolNegotiation) {
    let client = connect_client(&negotiation.bootstrap, "member-a").await;
    negotiation.responses.push(
        full_join(&client, "cg-type-mismatch", "consumer", &[
            ProposedProtocol::new("range", b""),
        ])
        .await,
    );
    negotiation.clients.push(client);
}
#[rstest_bdd_macros::when(
    "member-b first joins cg-type-mismatch with stream protocol type and range"
)]
async fn mismatch_consumer_protocol_type(negotiation: &mut ProtocolNegotiation) {
    let client = connect_client(&negotiation.bootstrap, "member-b").await;
    negotiation.mismatch = Some(
        client
            .send(
                join_group_request()
                    .group_id("cg-type-mismatch")
                    .protocol_type("stream")
                    .protocols(&[ProposedProtocol::new("range", b"")])
                    .call(),
            )
            .await
            .expect("JoinGroup must round-trip"),
    );
    negotiation.clients.push(client);
}
#[rstest_bdd_macros::then(
    "the mismatched member receives exactly the error 23 default response without a protocol or member id"
)]
fn mismatched_group_protocol(negotiation: &ProtocolNegotiation) {
    let response = negotiation
        .mismatch
        .as_ref()
        .expect("mismatched join attempted");
    assert!(
        response
            == &JoinGroupResponse {
                error_code: ERR_INCONSISTENT_GROUP_PROTOCOL,
                protocol_name: None,
                ..JoinGroupResponse::default()
            },
        "mismatched response: {response:?}"
    );
}
// Connect every member, then bootstrap every id, then spawn every second join.
// Awaiting the tasks in input order preserves the original racing window.
async fn race_consumer_joins(
    bootstrap: &str,
    group: &str,
    members: Vec<RacingMember>,
) -> Vec<JoinGroupResponse> {
    let count = members.len();
    let mut clients = Vec::with_capacity(count);
    for member in &members {
        clients.push(connect_client(bootstrap, &member.client_id).await);
    }
    let mut member_ids = Vec::with_capacity(count);
    for (client, member) in clients.iter().zip(&members) {
        let protocols: Vec<_> = member
            .protocols
            .iter()
            .map(|name| ProposedProtocol::new(name, b""))
            .collect();
        member_ids.push(bootstrap_member_id(client, group, "consumer", &protocols).await);
    }
    let groups: Vec<_> = iter::repeat_with(|| group.to_owned()).take(count).collect();
    let mut joins = Vec::with_capacity(count);
    for (((client, member_id), group), member) in
        clients.into_iter().zip(member_ids).zip(groups).zip(members)
    {
        let RacingMember {
            protocols,
            timeout_context,
            join_context,
            ..
        } = member;
        let task = tokio::spawn(async move {
            let protocols: Vec<_> = protocols
                .iter()
                .map(|name| ProposedProtocol::new(name, b""))
                .collect();
            time::timeout(
                Duration::from_secs(10),
                second_join(&client, &group, &member_id, "consumer", &protocols),
            )
            .await
            .expect(timeout_context)
        });
        joins.push((task, join_context));
    }
    let mut responses = Vec::with_capacity(count);
    for (join, context) in joins {
        responses.push(join.await.expect(context));
    }
    responses
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/group_protocol_negotiation.feature",
    name = "empty intersection returns inconsistent group protocol"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_intersection_returns_inconsistent_group_protocol(negotiation: ProtocolNegotiation) {
    negotiation.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/group_protocol_negotiation.feature",
    name = "vote picks cooperative when majority"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vote_picks_cooperative_when_majority(negotiation: ProtocolNegotiation) {
    negotiation.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/group_protocol_negotiation.feature",
    name = "vote ties broken lexicographically"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vote_ties_broken_lexicographically(negotiation: ProtocolNegotiation) {
    negotiation.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/group_protocol_negotiation.feature",
    name = "single member picks its first protocol"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_member_picks_its_first_protocol(negotiation: ProtocolNegotiation) {
    negotiation.shutdown().await;
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/group_protocol_negotiation.feature",
    name = "protocol type mismatch rejected"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_type_mismatch_rejected(negotiation: ProtocolNegotiation) {
    negotiation.shutdown().await;
}
