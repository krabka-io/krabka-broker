//! The capture itself: the host-side `DescribeGroups` call, the assertions that
//! calibrate Krabka's handler against the JVM answer, and the test that walks
//! both images in turn.

use {
    crate::{
        CLASSIC_IMAGE, GROUP, HOST_BOOTSTRAP, NEXT_GEN_GROUP, NEXT_GEN_IMAGE, TYPELESS_GROUP,
        groups_docker::{ContainerGuard, docker_pull, docker_run_kafka},
        groups_fixture::{group_json, write_fixture},
        groups_setup::{prepare_classic_groups, prepare_next_gen_group, wait_for_broker},
        support::client::connect_owned,
    },
    assert2::assert,
    krabka_protocol::owned::{
        describe_groups_request::DescribeGroupsRequest,
        describe_groups_response::{DescribeGroupsResponse, DescribedGroup},
    },
};

async fn describe_real_groups(groups: &[&str]) -> DescribeGroupsResponse {
    let client = connect_owned(HOST_BOOTSTRAP, "cap", "client build against real kafka").await;
    let response = client
        .send(DescribeGroupsRequest {
            groups: groups.iter().map(|group| (*group).to_owned()).collect(),
            include_authorized_operations: false,
            ..Default::default()
        })
        .await
        .expect("DescribeGroups against real kafka");
    client.close();
    response
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct GroupOracle {
    guard: Option<ContainerGuard>,
    response: Option<DescribeGroupsResponse>,
}

#[fixture]
fn oracle() -> GroupOracle {
    // Start this scenario with fresh inputs and observations.

    GroupOracle {
        guard: None,
        response: None,
    }
}

#[given("cp-kafka 7.4.0 hosts classic range group g and offset-only group simple-typeless")]
fn classic_oracle(oracle: &mut GroupOracle) {
    drop(oracle.guard.take());
    docker_pull(CLASSIC_IMAGE);
    docker_run_kafka(CLASSIC_IMAGE, false);
    oracle.guard = Some(ContainerGuard);
    wait_for_broker("kafka-broker-api-versions");
    prepare_classic_groups();
}

#[given("Apache Kafka 4.0.0 hosts consumer-protocol group g-next")]
fn next_oracle(oracle: &mut GroupOracle) {
    drop(oracle.guard.take());
    docker_pull(NEXT_GEN_IMAGE);
    docker_run_kafka(NEXT_GEN_IMAGE, true);
    oracle.guard = Some(ContainerGuard);
    wait_for_broker("/opt/kafka/bin/kafka-broker-api-versions.sh");
    prepare_next_gen_group();
}

#[when("DescribeGroups requests groups {groups} without authorized operations")]
async fn describe(oracle: &mut GroupOracle, groups: String) {
    oracle.response = Some(describe_real_groups(&groups.split(',').collect::<Vec<_>>()).await);
}

fn group<'a>(oracle: &'a GroupOracle, id: &str) -> &'a DescribedGroup {
    let response = oracle.response.as_ref().unwrap();
    response
        .groups
        .iter()
        .find(|group| group.group_id == id)
        .unwrap_or_else(|| panic!("group {id} missing: {response:?}"))
}

#[then(
    "classic group g has error {error}, protocol {protocol}, assignor {assignor}, state {state}, and one version-{version} metadata member"
)]
fn classic(
    oracle: &GroupOracle,
    error: i16,
    protocol: String,
    assignor: String,
    state: String,
    version: u16,
) {
    let classic = group(oracle, GROUP);
    assert!(classic.error_code == error);
    assert!(classic.protocol_type == protocol);
    assert!(classic.protocol_data == assignor);
    assert!(classic.group_state == state);
    assert!(classic.members.len() == 1);
    let member = &classic.members[0];
    assert!(!member.member_metadata.is_empty());
    assert!(&member.member_metadata[..2] == version.to_be_bytes());
}

#[then("offset-only group simple-typeless has error {error} and empty protocol and assignor")]
fn typeless(oracle: &GroupOracle, error: i16) {
    let typeless = group(oracle, TYPELESS_GROUP);
    assert!(typeless.error_code == error);
    assert!(typeless.protocol_type.is_empty());
    assert!(typeless.protocol_data.is_empty());
}

#[when("the classic and typeless responses are captured as real_kafka_classic.json")]
fn persist_classic(oracle: &GroupOracle) {
    let fixture = serde_json::json!({
        "provenance": {
            "image": CLASSIC_IMAGE,
            "api_key": 15,
            "note": "Real cp-kafka DescribeGroups. cp/JVM is the authority for protocol_type / protocol_data / member_metadata.",
        },
        "classic_consumer_group": group_json(group(oracle, GROUP)),
        "typeless_group": group_json(group(oracle, TYPELESS_GROUP)),
    });
    write_fixture(
        "real_kafka_classic.json",
        &serde_json::to_string_pretty(&fixture).unwrap(),
    );
}

#[then(
    "consumer group g-next has error {error}, state {state}, no classic protocol or members, and message {message}"
)]
fn next_generation(oracle: &GroupOracle, error: i16, state: String, message: String) {
    let next_gen = group(oracle, NEXT_GEN_GROUP);
    assert!(next_gen.error_code == error);
    assert!(next_gen.error_message.as_deref() == Some(message.as_str()));
    assert!(next_gen.protocol_type.is_empty());
    assert!(next_gen.protocol_data.is_empty());
    assert!(next_gen.group_state == state);
    assert!(next_gen.members.is_empty());
}

#[when("the next-generation response is captured as real_kafka_next_gen.json")]
fn persist_next(oracle: &GroupOracle) {
    let fixture = serde_json::json!({
        "provenance": {
            "image": NEXT_GEN_IMAGE,
            "api_key": 15,
            "note": "Real Apache Kafka next-generation consumer-group DescribeGroups authority capture.",
        },
        "next_gen_consumer_group": group_json(group(oracle, NEXT_GEN_GROUP)),
    });
    write_fixture(
        "real_kafka_next_gen.json",
        &serde_json::to_string_pretty(&fixture).unwrap(),
    );
}

#[ignore = "requires Docker; captures classic and next-generation real-Kafka DescribeGroups metadata"]
#[scenario(
    path = "tests/features/migrated/tests/describe_groups_jvm/groups_capture.feature",
    name = "capture real kafka describe groups"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn capture_real_kafka_describe_groups(oracle: GroupOracle) { drop(oracle); }
