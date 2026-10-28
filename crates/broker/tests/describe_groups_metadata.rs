#![cfg(test)]

//! Byte-exactness pin for `DescribeGroups` (`api_key=15`) on a CLASSIC
//! consumer group.
//!
//! For each member, the response must carry the `JoinGroup` protocol-metadata
//! bytes as `member_metadata`. At the group level, it must carry the SELECTED
//! protocol name as `protocol_data`. Both matter for wire byte-exactness with
//! Apache Kafka, which `kafka-consumer-groups --describe --members` and
//! `AdminClient.describeClassicGroups` rely on.
//!
//! The test drives a real classic `JoinGroup`, `SyncGroup`, and
//! `DescribeGroups` sequence against the in-process broker over
//! `krabka_client_core::Client`. It mirrors `group_protocol_negotiation.rs`,
//! for the two-step `MEMBER_ID_REQUIRED` flow and the
//! `INITIAL_REBALANCE_DELAY` wait, and `unit.rs`, for the `SyncGroup` shape.

mod support;

use {
    crate::support::{
        classic::{
            ClassicTimeouts, GenerationId, classic_join_request, classic_sync_request,
            join_protocol, sync_assignment,
        },
        client::connect_owned,
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::DescribeGroupsResponse, join_group_request::JoinGroupRequest,
        join_group_response::JoinGroupResponse, sync_group_response::SyncGroupResponse,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{str, time::Duration},
    tempfile::TempDir,
    tokio::time,
};

fn decode_hex(value: &str) -> Bytes {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    assert!(
        remainder.is_empty(),
        "subscription and assignment hex must contain whole bytes"
    );
    pairs
        .iter()
        .map(|pair| {
            u8::from_str_radix(str::from_utf8(pair).expect("ASCII subscription hex"), 16)
                .expect("subscription byte")
        })
        .collect::<Vec<_>>()
        .into()
}

fn join_request(group_id: &str, member_id: &str, metadata: Bytes) -> JoinGroupRequest {
    JoinGroupRequest {
        group_instance_id: None,
        ..classic_join_request()
            .group_id(group_id.to_owned())
            .member_id(member_id.to_owned())
            .timeouts(ClassicTimeouts {
                session: krabka_units::millis(10_000),
                ..Default::default()
            })
            .protocol_type("consumer".to_owned())
            .protocols(vec![join_protocol("range".to_owned(), metadata)])
            .call()
    }
}

#[derive(Default)]
struct ClassicDescription {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Client>,
    group: String,
    metadata: Bytes,
    first: Option<JoinGroupResponse>,
    joined: Option<JoinGroupResponse>,
    synced: Option<SyncGroupResponse>,
    described: Option<DescribeGroupsResponse>,
}

#[fixture]
fn classic() -> ClassicDescription {
    // Start this scenario with fresh inputs and observations.

    ClassicDescription {
        broker: None,
        directory: None,
        client: None,
        group: String::new(),
        metadata: Bytes::new(),
        first: None,
        joined: None,
        synced: None,
        described: None,
    }
}

#[given("a ready classic coordinator for group {group} with range subscription hex {metadata}")]
async fn coordinator(classic: &mut ClassicDescription, group: String, metadata: String) {
    let (broker, bootstrap, directory) = support::start_group_coordinator().await;
    classic.client =
        Some(connect_owned(bootstrap, "describe-metadata-member", "client build").await);
    classic.broker = Some(broker);
    classic.directory = Some(directory);
    classic.group = group;
    classic.metadata = decode_hex(&metadata);
}

#[when("the consumer requests a member id and rejoins using that id")]
async fn join(classic: &mut ClassicDescription) {
    let client = classic.client.as_ref().unwrap();
    let first = client
        .send(join_request(&classic.group, "", classic.metadata.clone()))
        .await
        .expect("first JoinGroup");
    classic.joined = Some(
        time::timeout(
            Duration::from_secs(10),
            client.send(join_request(
                &classic.group,
                &first.member_id,
                classic.metadata.clone(),
            )),
        )
        .await
        .expect("second JoinGroup timed out")
        .expect("second JoinGroup"),
    );
    classic.first = Some(first);
}

#[then(
    "the first join requires a nonempty member id and the rejoin elects that member with range protocol"
)]
fn membership(classic: &ClassicDescription) {
    let first = classic.first.as_ref().unwrap();
    assert!(first.error_code == 79);
    assert!(!first.member_id.is_empty());
    let joined = classic.joined.as_ref().unwrap();
    check!(joined.error_code == 0);
    check!(joined.protocol_name.as_deref() == Some("range"));
    check!(joined.leader == first.member_id);
}

#[when("the leader synchronizes assignment hex {assignment} and describes the group")]
async fn sync_describe(classic: &mut ClassicDescription, assignment: String) {
    let client = classic.client.as_ref().unwrap();
    let first = classic.first.as_ref().unwrap();
    let joined = classic.joined.as_ref().unwrap();
    classic.synced = Some(
        client
            .send(
                classic_sync_request()
                    .group_id(classic.group.clone())
                    .generation_id(GenerationId(joined.generation_id))
                    .member_id(first.member_id.clone())
                    .assignments(vec![sync_assignment(
                        first.member_id.clone(),
                        decode_hex(&assignment),
                    )])
                    .call(),
            )
            .await
            .expect("SyncGroup"),
    );
    classic.described = Some(
        client
            .send(DescribeGroupsRequest {
                groups: vec![classic.group.clone()],
                ..Default::default()
            })
            .await
            .expect("DescribeGroups"),
    );
}

#[then(
    "the sole described consumer uses range, metadata hex {metadata}, and assignment hex {assignment}"
)]
fn described(classic: &ClassicDescription, metadata: String, assignment: String) {
    let assignment = decode_hex(&assignment);
    let synced = classic.synced.as_ref().unwrap();
    assert!(synced.error_code == 0);
    assert!(synced.assignment == assignment);
    let response = classic.described.as_ref().unwrap();
    assert!(response.groups.len() == 1);
    let group = &response.groups[0];
    check!(group.error_code == 0);
    check!(group.protocol_type == "consumer");
    check!(group.protocol_data == "range");
    assert!(group.members.len() == 1);
    let member = &group.members[0];
    assert!(member.member_metadata == decode_hex(&metadata));
    assert!(member.member_assignment == assignment);
}

#[scenario(
    path = "tests/features/migrated/tests/describe_groups_metadata.feature",
    name = "describe groups reports member metadata and protocol name"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_groups_reports_member_metadata_and_protocol_name(classic: ClassicDescription) {
    drop(classic.client);
    classic.broker.unwrap().shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/describe_groups_metadata.feature",
    name = "describe groups matches real kafka range subscription"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_groups_matches_real_kafka_range_subscription(classic: ClassicDescription) {
    drop(classic.client);
    classic.broker.unwrap().shutdown().await;
}
