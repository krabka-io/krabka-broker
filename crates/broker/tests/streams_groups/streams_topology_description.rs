//! KIP-1331 topology descriptions over the wire, on a broker that runs as the
//! Apache Kafka system tests run it: trunk's API versions, and Kafka's
//! in-memory topology description plugin named by its Kafka key in
//! `[server_properties]`.
//!
//! This is the broker side of `StreamsTopologyDescriptionPluginTest` in
//! Kafka's system tests: one of two members that join together is asked for
//! the description, its push is stored and described, and a group that is
//! deleted and created again is asked again.

use {
    crate::{
        streams_harness::{
            connect, create_topic, finalize_streams_version, first_join, follow_up, topology,
        },
        support::{GroupCoordinatorBoot, heartbeat_config, start_group_coordinator_with_config},
    },
    assert2::assert,
    krabka_broker::file_config::FileConfig,
    krabka_client_core::Client,
    krabka_protocol::owned::{
        common::{
            streams_group_describe_response as described,
            streams_group_topology_description_update_request as pushed,
        },
        delete_groups_request::DeleteGroupsRequest,
        streams_group_describe_request::StreamsGroupDescribeRequest,
        streams_group_describe_response::DescribedGroup,
        streams_group_topology_description_update_request::StreamsGroupTopologyDescriptionUpdateRequest,
        streams_group_topology_description_update_response::StreamsGroupTopologyDescriptionUpdateResponse,
    },
};

/// The `[server_properties]` that the system-test adapter passes through.
const TRUNK_WITH_PLUGIN: &str = r#"
[server_properties]
"unstable.api.versions.enable" = "true"
"group.streams.topology.description.plugin.class" = "org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin"
"#;

const GROUP: &str = "topology-description-app";
const SOURCE: &str = "topology-description-input";

async fn boot_with_plugin() -> GroupCoordinatorBoot {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = heartbeat_config(dir.path());
    let file: FileConfig = toml::from_str(TRUNK_WITH_PLUGIN).expect("parse the properties");
    file.apply_to(&mut config).expect("apply the properties");
    Box::pin(
        start_group_coordinator_with_config()
            .config(config)
            .directory(dir)
            .call(),
    )
    .await
}

/// The description a Streams client pushes for [`topology`]: one source node.
fn pushed_description() -> pushed::topology_description::TopologyDescription {
    pushed::topology_description::TopologyDescription {
        subtopologies: vec![
            pushed::topology_description_subtopology::TopologyDescriptionSubtopology {
                subtopology_id: "0".into(),
                nodes: vec![pushed::topology_description_node::TopologyDescriptionNode {
                    name: "KSTREAM-SOURCE-0000000000".into(),
                    node_type: 1,
                    source_topics: vec![SOURCE.into()],
                    ..Default::default()
                }],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

fn described_description() -> described::topology_description::TopologyDescription {
    described::topology_description::TopologyDescription {
        subtopologies: vec![
            described::topology_description_subtopology::TopologyDescriptionSubtopology {
                subtopology_id: "0".into(),
                nodes: vec![
                    described::topology_description_node::TopologyDescriptionNode {
                        name: "KSTREAM-SOURCE-0000000000".into(),
                        node_type: 1,
                        source_topics: vec![SOURCE.into()],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

/// The group's describe row, with the topology description asked for.
async fn describe_with_description(client: &Client) -> DescribedGroup {
    let response = client
        .send(StreamsGroupDescribeRequest {
            group_ids: vec![GROUP.into()],
            include_topology_description: true,
            ..Default::default()
        })
        .await
        .expect("StreamsGroupDescribe");
    let [group] = <[DescribedGroup; 1]>::try_from(response.groups).expect("one described group");
    assert!(group.error_code == 0, "{group:?}");
    group
}

use {
    krabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::sync::Arc,
};

#[derive(Default)]
struct TopologyDescriptionScenario {
    booted: Option<GroupCoordinatorBoot>,
    client: Option<Arc<Client>>,
    joins: Vec<StreamsGroupHeartbeatResponse>,
    descriptions: Vec<DescribedGroup>,
    pushed: Option<StreamsGroupTopologyDescriptionUpdateResponse>,
    heartbeats: Vec<StreamsGroupHeartbeatResponse>,
    left: Option<StreamsGroupHeartbeatResponse>,
    deleted_codes: Option<Vec<i16>>,
}

#[fixture]
fn topology_description_case() -> TopologyDescriptionScenario {
    // Preserve full descriptions and independently observed member responses.
    TopologyDescriptionScenario::default()
}

impl TopologyDescriptionScenario {
    fn client(&self) -> &Client { self.client.as_ref().expect("topology plugin client") }
}

#[given(
    "a trunk streams broker runs the in-memory topology description plugin with one source partition"
)]
async fn topology_plugin_broker(topology_description_case: &mut TopologyDescriptionScenario) {
    let booted = Box::pin(boot_with_plugin()).await;
    let client = connect(&booted.bootstrap).await;
    finalize_streams_version(&client).await;
    create_topic(&client, SOURCE, 1).await;
    topology_description_case.client = Some(client);
    topology_description_case.booted = Some(booted);
}

#[when("a new member joins the topology-description group")]
async fn topology_description_join(topology_description_case: &mut TopologyDescriptionScenario) {
    topology_description_case.joins.push(
        topology_description_case
            .client()
            .send(first_join(GROUP, topology(SOURCE, vec![])))
            .await
            .expect("streams join"),
    );
}

#[then("both topology-description joins succeed and only the first member is asked to push")]
fn one_topology_description_reporter(topology_description_case: &TopologyDescriptionScenario) {
    assert!(topology_description_case.joins.len() == 2);
    let first = &topology_description_case.joins[0];
    let second = &topology_description_case.joins[1];
    assert!(
        first.error_code == 0 && second.error_code == 0,
        "{first:?} {second:?}"
    );
    assert!(
        (
            first.topology_description_required,
            second.topology_description_required
        ) == (true, false)
    );
}

#[then("the latest join succeeds and requests a topology description")]
fn topology_description_required(topology_description_case: &TopologyDescriptionScenario) {
    let response = topology_description_case
        .joins
        .last()
        .expect("join answered");
    assert!(response.error_code == 0, "{response:?}");
    assert!(response.topology_description_required);
}

#[when("the administrator describes the group including its topology description")]
async fn inspect_topology_description(topology_description_case: &mut TopologyDescriptionScenario) {
    topology_description_case
        .descriptions
        .push(describe_with_description(topology_description_case.client()).await);
}

#[then("the topology description has status one and no stored description")]
fn no_stored_topology_description(topology_description_case: &TopologyDescriptionScenario) {
    let response = topology_description_case
        .descriptions
        .last()
        .expect("group described");
    assert!(
        (
            response.topology_description_status,
            response.topology_description.as_ref()
        ) == (1, None)
    );
}

#[when("the selected first member pushes the source-node description at topology epoch zero")]
async fn push_topology_description(topology_description_case: &mut TopologyDescriptionScenario) {
    topology_description_case.pushed = Some(
        topology_description_case
            .client()
            .send(StreamsGroupTopologyDescriptionUpdateRequest {
                group_id: GROUP.into(),
                member_id: topology_description_case.joins[0].member_id.clone(),
                topology_epoch: 0,
                topology_description: pushed_description(),
                ..Default::default()
            })
            .await
            .expect("StreamsGroupTopologyDescriptionUpdate"),
    );
}

#[then("the topology description push returns the complete successful default response")]
fn successful_topology_description_push(topology_description_case: &TopologyDescriptionScenario) {
    assert!(
        topology_description_case
            .pushed
            .as_ref()
            .expect("push answered")
            == &StreamsGroupTopologyDescriptionUpdateResponse::default()
    );
}

#[then("the group description changes only to status three with the exact pushed source topology")]
fn stored_topology_description(topology_description_case: &TopologyDescriptionScenario) {
    assert!(topology_description_case.descriptions.len() == 2);
    assert!(
        topology_description_case.descriptions[1]
            == DescribedGroup {
                topology_description_status: 3,
                topology_description: Some(described_description()),
                ..topology_description_case.descriptions[0].clone()
            }
    );
}

#[when("both joined members heartbeat after the topology description push")]
async fn heartbeat_topology_description_members(
    topology_description_case: &mut TopologyDescriptionScenario,
) {
    for member in &topology_description_case.joins {
        topology_description_case.heartbeats.push(
            topology_description_case
                .client()
                .send(
                    follow_up()
                        .group(GROUP)
                        .member_id(&member.member_id)
                        .epoch(member.member_epoch)
                        .call(),
                )
                .await
                .expect("heartbeat"),
        );
    }
}

#[then("both later heartbeats succeed without requesting another topology push")]
fn topology_description_not_requested(topology_description_case: &TopologyDescriptionScenario) {
    assert!(topology_description_case.heartbeats.len() == 2);
    for response in &topology_description_case.heartbeats {
        assert!(response.error_code == 0, "{response:?}");
        assert!(!response.topology_description_required);
    }
}

#[when("the topology-description group's member leaves permanently")]
async fn leave_topology_description_group(
    topology_description_case: &mut TopologyDescriptionScenario,
) {
    topology_description_case.left = Some(
        topology_description_case
            .client()
            .send(
                follow_up()
                    .group(GROUP)
                    .member_id(&topology_description_case.joins[0].member_id)
                    .epoch(-1)
                    .call(),
            )
            .await
            .expect("leave"),
    );
}

#[then("the topology-description leave succeeds")]
fn topology_description_left(topology_description_case: &TopologyDescriptionScenario) {
    assert!(
        topology_description_case
            .left
            .as_ref()
            .expect("leave answered")
            .error_code
            == 0
    );
}

#[when("the empty topology-description group is deleted")]
async fn delete_topology_description_group(
    topology_description_case: &mut TopologyDescriptionScenario,
) {
    let response = topology_description_case
        .client()
        .send(DeleteGroupsRequest {
            groups_names: vec![GROUP.into()],
            ..Default::default()
        })
        .await
        .expect("DeleteGroups");
    topology_description_case.deleted_codes = Some(
        response
            .results
            .iter()
            .map(|result| result.error_code)
            .collect(),
    );
}

#[then("exactly one topology-description group deletion succeeds")]
fn topology_description_deleted(topology_description_case: &TopologyDescriptionScenario) {
    assert!(
        topology_description_case
            .deleted_codes
            .as_ref()
            .expect("delete answered")
            == &vec![0]
    );
}

#[scenario(
    path = "tests/features/migrated/tests/streams_groups/streams_topology_description.feature",
    name = "one member is asked and its push is described"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_member_is_asked_and_its_push_is_described(
    topology_description_case: TopologyDescriptionScenario,
) {
    topology_description_case
        .booted
        .take()
        .expect("plugin broker")
        .broker
        .shutdown()
        .await;
    drop(topology_description_case);
}

#[scenario(
    path = "tests/features/migrated/tests/streams_groups/streams_topology_description.feature",
    name = "a deleted and recreated group is asked again"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_deleted_and_recreated_group_is_asked_again(
    topology_description_case: TopologyDescriptionScenario,
) {
    topology_description_case
        .booted
        .take()
        .expect("plugin broker")
        .broker
        .shutdown()
        .await;
    drop(topology_description_case);
}
