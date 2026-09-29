use std::sync::Arc;

use assert2::assert;
use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
use krabka_protocol::owned::{
    common::streams_group_topology_description_update_request::{
        topology_description::TopologyDescription,
        topology_description_node::TopologyDescriptionNode,
        topology_description_subtopology::TopologyDescriptionSubtopology,
    },
    streams_group_topology_description_update_response::MAX_VERSION,
};

use super::*;

crate::test_support::wire_helpers!(
    StreamsGroupTopologyDescriptionUpdateRequest,
    StreamsGroupTopologyDescriptionUpdateResponse,
    version = MAX_VERSION,
    client_id = "streams-client"
);

fn request(group_id: &str) -> StreamsGroupTopologyDescriptionUpdateRequest {
    StreamsGroupTopologyDescriptionUpdateRequest {
        group_id: group_id.into(),
        member_id: "m1".into(),
        topology_epoch: 1,
        topology_description: TopologyDescription {
            subtopologies: vec![TopologyDescriptionSubtopology {
                subtopology_id: "0".into(),
                nodes: vec![TopologyDescriptionNode {
                    name: "KSTREAM-SOURCE-0000000000".into(),
                    node_type: 1,
                    source_topics: vec!["in".into()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
        ..Default::default()
    }
}

fn response(
    error_code: i16,
    message: Option<&str>,
) -> StreamsGroupTopologyDescriptionUpdateResponse {
    StreamsGroupTopologyDescriptionUpdateResponse {
        error_code,
        error_message: message.map(str::to_owned),
        ..Default::default()
    }
}

async fn set_streams_version(broker: &Broker, level: i16) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: crate::features::STREAMS_VERSION.into(),
            level,
        })])
        .await
        .expect("submit streams.version");
}

#[tokio::test]
async fn handle_answers_as_a_trunk_broker_without_a_plugin() {
    let rows = [
        (
            "streams disabled by config",
            false,
            1,
            "Group:Read",
            response(codes::UNSUPPORTED_VERSION, None),
        ),
        (
            "streams.version not finalized",
            true,
            0,
            "Group:Read",
            response(codes::UNSUPPORTED_VERSION, None),
        ),
        (
            "group Read denied",
            true,
            1,
            "Group:Describe",
            response(codes::GROUP_AUTHORIZATION_FAILED, None),
        ),
        (
            "no plugin configured",
            true,
            1,
            "Group:Read",
            response(
                codes::UNSUPPORTED_VERSION,
                Some("The broker has no streams group topology description plugin configured."),
            ),
        ),
    ];
    for (case, enable, streams_version, grants, want) in rows {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.streams_group.enable = enable;
            cfg.authorizer = Arc::new(crate::test_support::ControllerPeerAllowed(
                crate::test_support::GrantsInPrincipalName,
            ));
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        set_streams_version(&broker, streams_version).await;
        let principal = crate::test_support::principal(grants);
        let peer = crate::test_support::peer();
        let ctx = test_context(&principal, &peer);

        let answer = handle(
            &broker,
            MAX_VERSION,
            1,
            &encode_request(&request("app")),
            &ctx,
        )
        .await
        .expect("an answer");

        assert!(decode_response(&answer) == want, "{case}");
        broker_handle.shutdown().await;
    }
}
