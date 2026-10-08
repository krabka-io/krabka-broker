//! The KIP-1331 steps of the streams actor, driven through its handle with no
//! connected `MetadataSource`: a joining member installs the topology, which
//! is all the solicitation needs.

use std::sync::Arc;

use assert2::assert;
use krabka_protocol::owned::{
    common::streams_group_topology_description_update_request::{
        topology_description_node::TopologyDescriptionNode,
        topology_description_subtopology::TopologyDescriptionSubtopology,
    },
    streams_group_heartbeat_request::{Subtopology as WireSubtopology, Topology},
};

use super::*;
use crate::coordinator::unified::{
    StreamsGroupSeed,
    offsets_log::fake::InMemoryOffsetsLog,
    streams::{
        actor::{
            StreamsGroupActorHandle, StreamsGroupActorMessage,
            test_support::{coordinator_with_log, describe, heartbeat_result_at, undelayed},
        },
        description::{Node, NodeKind, Subtopology, TopologyDescriptionPlugin},
        persistence::DescriptionEpochs,
        topology::to_stored_topology,
    },
};

fn coordinator(plugin: TopologyDescriptionPlugin) -> Arc<GroupCoordinator> {
    coordinator_with_log(
        StreamsGroupConfig {
            topology_description_plugin: plugin,
            ..undelayed()
        },
        Arc::new(InMemoryOffsetsLog::default()),
    )
}

/// A join of `member_id` with the one-subtopology topology at epoch 0.
fn join(member_id: &str) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".into(),
        member_id: member_id.into(),
        member_epoch: 0,
        topology: Some(Topology {
            epoch: 0,
            subtopologies: vec![WireSubtopology {
                subtopology_id: "0".into(),
                source_topics: vec!["in".into()],
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The next heartbeat of a member that joined at `member_epoch`.
fn steady(member_id: &str, member_epoch: i32) -> StreamsGroupHeartbeatRequest {
    StreamsGroupHeartbeatRequest {
        group_id: "app".into(),
        member_id: member_id.into(),
        member_epoch,
        ..Default::default()
    }
}

async fn heartbeat(
    handle: &StreamsGroupActorHandle,
    request: StreamsGroupHeartbeatRequest,
    version: i16,
) -> StreamsGroupHeartbeatResponse {
    let response = heartbeat_result_at(handle, request, version).await.response;
    assert!(response.error_code == codes::NONE, "{response:?}");
    response
}

async fn push(handle: &StreamsGroupActorHandle, push: DescriptionPush) -> PushAnswer {
    crate::task_util::ask(&handle.tx, |reply| {
        StreamsGroupActorMessage::PushDescription {
            push: Box::new(push),
            reply,
        }
    })
    .await
    .expect("a push answer")
}

/// The source node a Streams client describes for the topology of [`join`].
fn wire_description(node_type: i8) -> WireDescription {
    WireDescription {
        subtopologies: vec![TopologyDescriptionSubtopology {
            subtopology_id: "0".into(),
            nodes: vec![TopologyDescriptionNode {
                name: "KSTREAM-SOURCE-0000000000".into(),
                node_type,
                source_topics: vec!["in".into()],
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn description_push(member_id: &str, topology_epoch: i32, node_type: i8) -> DescriptionPush {
    DescriptionPush {
        member_id: member_id.into(),
        topology_epoch,
        description: wire_description(node_type),
    }
}

/// Each row joins two members of a fresh group, one after the other, and
/// sends a heartbeat from the first. Only a broker with a plugin asks, only
/// at version 1, and only the first member: the back-off window holds the
/// others off, as Kafka's `armIfNotActive` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_member_of_a_group_is_asked_for_the_description() {
    let rows = [
        (
            "no plugin",
            TopologyDescriptionPlugin::None,
            1,
            [false, false, false],
        ),
        (
            "version 0",
            TopologyDescriptionPlugin::InMemory,
            0,
            [false, false, false],
        ),
        (
            "in-memory plugin",
            TopologyDescriptionPlugin::InMemory,
            1,
            [true, false, false],
        ),
    ];
    for (row, plugin, version, want) in rows {
        let coordinator = coordinator(plugin);
        let handle = coordinator.get_or_create_streams("app");

        let first = heartbeat(&handle, join("a"), version).await;
        let second = heartbeat(&handle, join("b"), version).await;
        let again = heartbeat(&handle, steady("a", first.member_epoch), version).await;

        let asked = [
            first.topology_description_required,
            second.topology_description_required,
            again.topology_description_required,
        ];
        assert!(asked == want, "{row}");
    }
}

/// A push stores the description and records its epoch: a describe serves
/// it, the group metadata record carries the epoch, and no heartbeat asks
/// again. Any member of the group may push, not only the one asked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_is_stored_and_ends_the_asking() {
    let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
    let handle = coordinator.get_or_create_streams("app");
    let first = heartbeat(&handle, join("a"), 1).await;
    assert!(first.topology_description_required);
    let second = heartbeat(&handle, join("b"), 1).await;
    assert!(describe(&handle).await.topology_description.is_none());

    let answer = push(&handle, description_push("b", 0, 1)).await;

    assert!(answer == (codes::NONE, None));
    let expected = TopologyDescription {
        subtopologies: vec![Subtopology {
            id: "0".into(),
            nodes: vec![Node {
                name: "KSTREAM-SOURCE-0000000000".into(),
                kind: NodeKind::Source {
                    topics: vec!["in".into()],
                },
                successors: vec![],
            }],
        }],
        global_stores: vec![],
    };
    assert!(describe(&handle).await.topology_description == Some(expected));
    let seed = coordinator
        .cached_streams_seed("app")
        .expect("a cached seed");
    assert!(
        seed.description_epochs
            == DescriptionEpochs {
                stored: 0,
                failed: -1
            }
    );
    for (member_id, member_epoch) in [("a", first.member_epoch), ("b", second.member_epoch)] {
        let response = heartbeat(&handle, steady(member_id, member_epoch), 1).await;
        assert!(!response.topology_description_required, "{member_id}");
    }
}

/// The pushes Kafka's `validateStreamsGroupTopologyDescriptionUpdate` and
/// converter refuse, with their messages. None of them stores anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_push_the_group_does_not_accept_stores_nothing() {
    let rows = [
        (
            "an unknown member",
            description_push("z", 0, 1),
            (
                codes::UNKNOWN_MEMBER_ID,
                Some("Member z is not a member of group app.".to_owned()),
            ),
        ),
        (
            "another topology epoch",
            description_push("a", 3, 1),
            (
                codes::INVALID_REQUEST,
                Some(
                    "Topology epoch 3 does not match the group's current topology epoch 0."
                        .to_owned(),
                ),
            ),
        ),
        (
            "an unknown node type",
            description_push("a", 0, 9),
            (
                codes::INVALID_REQUEST,
                Some("Unknown topology node type: 9".to_owned()),
            ),
        ),
    ];
    for (row, refused, want) in rows {
        let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
        let handle = coordinator.get_or_create_streams("app");
        heartbeat(&handle, join("a"), 1).await;

        assert!(push(&handle, refused).await == want, "{row}");
        assert!(
            describe(&handle).await.topology_description.is_none(),
            "{row}"
        );
        let seed = coordinator
            .cached_streams_seed("app")
            .expect("a cached seed");
        assert!(
            seed.description_epochs == DescriptionEpochs::default(),
            "{row}"
        );
    }

    let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
    let empty = coordinator.get_or_create_streams("app");
    assert!(
        push(&empty, description_push("a", 0, 1)).await
            == (
                codes::GROUP_ID_NOT_FOUND,
                Some("Group app not found.".to_owned())
            )
    );
}

/// A group loaded from the log keeps the epoch its record stored, so its
/// members are not asked again. The in-memory plugin did not survive the
/// load, so a describe finds no description, as Kafka's reference plugin
/// loses its map with the broker.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_loaded_group_is_not_asked_for_a_stored_epoch() {
    let epochs = |stored, failed| DescriptionEpochs { stored, failed };
    // (row, the loaded epochs, the topology epoch, whether the member is asked)
    let rows = [
        ("stored", epochs(0, -1), 0, false),
        ("failed for good", epochs(-1, 0), 0, false),
        ("uncertain", epochs(-2, -1), 0, true),
        ("stored at an older epoch", epochs(0, -1), 1, true),
    ];
    for (row, description_epochs, topology_epoch, asked) in rows {
        let mut request = join("a");
        let topology = request
            .topology
            .as_mut()
            .expect("a join sends its topology");
        topology.epoch = topology_epoch;
        let loaded = to_stored_topology(topology);
        let coordinator = coordinator(TopologyDescriptionPlugin::InMemory);
        let handle = coordinator.get_or_create_streams("app");
        handle
            .tx
            .send(StreamsGroupActorMessage::Seed(StreamsGroupSeed {
                group_epoch: 2,
                description_epochs,
                topology: Some(loaded),
                ..StreamsGroupSeed::default()
            }))
            .await
            .expect("the actor runs");

        let response = heartbeat(&handle, request, 1).await;

        assert!(response.topology_description_required == asked, "{row}");
        assert!(
            describe(&handle).await.topology_description.is_none(),
            "{row}"
        );
    }
}
