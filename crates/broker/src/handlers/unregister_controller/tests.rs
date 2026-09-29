use std::sync::Arc;

use assert2::assert;
use krabka_metadata::{
    ControllerRegistrationRecord, FeatureLevelRecord, MetadataImage, MetadataRecord, NodeId,
    voters::{KRaftVersionRange, Voter, VoterSet},
};
use krabka_protocol::owned::unregister_controller_response::MAX_VERSION;

use super::*;

fn encode_request(req: &UnregisterControllerRequest) -> Bytes {
    crate::test_support::encode_request(req, MAX_VERSION)
}

fn decode_response(bytes: &Bytes) -> UnregisterControllerResponse {
    crate::test_support::decode_response(bytes, MAX_VERSION)
}

fn registration(node_id: u64) -> MetadataRecord {
    MetadataRecord::V1ControllerRegistration(ControllerRegistrationRecord {
        node_id: NodeId(node_id),
        incarnation_id: uuid::Uuid::from_u128(u128::from(node_id)),
        zk_migration_ready: false,
        endpoints: Vec::new(),
        features: std::collections::BTreeMap::new(),
    })
}

fn metadata_version(level: i16) -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
        level,
    })
}

/// An image with voter 1, registered controllers 1 and 7, at `level`.
fn image_at(level: i16) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1Voters(krabka_metadata::VotersRecord {
        voters: VoterSet::from_voters([Voter {
            id: NodeId(1),
            directory_id: uuid::Uuid::nil(),
            endpoints: Vec::new(),
            kraft_version: KRaftVersionRange::default(),
        }]),
    }));
    for record in [metadata_version(level), registration(1), registration(7)] {
        image.apply(&record);
    }
    image
}

fn expected(error_code: i16, message: &str) -> (i16, Option<String>) {
    (error_code, Some(message.to_owned()))
}

/// Trunk's checks in trunk's order: a voter is refused before the
/// `metadata.version` gate, and the gate before the registration lookup.
#[test]
fn refusals_follow_trunks_order() {
    let unsupported = Some(expected(
        codes::UNSUPPORTED_VERSION,
        "The current MetadataVersion is too old to support controller unregistration.",
    ));
    let voter = Some(expected(
        codes::INVALID_REQUEST,
        "Cannot unregister controller 1 because it is part of the voter set.",
    ));
    let cases = [
        ("voter at 4.4-IV1", 32, 1, voter.clone()),
        ("voter at 4.4-IV2", 33, 1, voter),
        ("registered at 4.4-IV1", 32, 7, unsupported.clone()),
        ("unknown at 4.4-IV1", 32, 9, unsupported),
        ("registered at 4.4-IV2", 33, 7, None),
        (
            "unknown at 4.4-IV2",
            33,
            9,
            Some(expected(
                codes::CONTROLLER_ID_NOT_REGISTERED,
                "Controller ID 9 is not currently registered.",
            )),
        ),
        (
            "negative at 4.4-IV2",
            33,
            -1,
            Some(expected(
                codes::CONTROLLER_ID_NOT_REGISTERED,
                "Controller ID -1 is not currently registered.",
            )),
        ),
    ];
    for (case, level, controller_id, want) in cases {
        assert!(refusal(&image_at(level), controller_id) == want, "{case}");
    }
}

#[test]
fn wrong_controller_message_names_the_leader_when_there_is_one() {
    assert!(
        wrong_controller_message(Some(NodeId(3))) == "The active controller appears to be node 3."
    );
    assert!(wrong_controller_message(None) == "No controller appears to be active.");
}

async fn submit(broker: &Broker, records: Vec<MetadataRecord>) {
    broker
        .controller
        .submit_change(records)
        .await
        .expect("submit metadata records");
}

async fn send(
    broker: &Broker,
    principal: &str,
    controller_id: i32,
) -> UnregisterControllerResponse {
    let principal = crate::test_support::principal(principal);
    let peer = crate::test_support::peer();
    let ctx = crate::handlers::RequestContext::new(
        &principal,
        &peer,
        "kafka-cluster",
        CONTROLLER_ADMIN_CONNECTION_ID,
        false,
        "CONTROLLER",
    );
    let body = encode_request(&UnregisterControllerRequest {
        controller_id,
        ..Default::default()
    });
    decode_response(
        &handle(broker, MAX_VERSION, 1, &body, &ctx)
            .await
            .expect("an answer"),
    )
}

/// The issue's two rows, a registered and an unknown controller, with the
/// `metadata.version` gate on either side of 4.4-IV2 and the `Alter` gate.
/// The pinned krabka-metadata table stops at level 32, but the image does not
/// check a finalized level, so the served rows finalize 33 through an
/// ordinary `FeatureLevelRecord`.
#[tokio::test]
async fn handle_unregisters_a_registered_controller_as_trunk_does() {
    let (handle, _dir) = crate::test_support::start_broker_with(|cfg| {
        cfg.authorizer = Arc::new(crate::test_support::ControllerPeerAllowed(
            crate::test_support::GrantsInPrincipalName,
        ));
    })
    .await;
    let broker = handle.broker_arc_for_test();
    let operator = "Cluster:Alter";
    submit(&broker, vec![registration(7), metadata_version(32)]).await;

    let cases = [
        (
            "no Alter grant",
            "none",
            7,
            response(
                codes::CLUSTER_AUTHORIZATION_FAILED,
                Some("Request UnregisterController needs ALTER permission."),
            ),
        ),
        (
            "below 4.4-IV2",
            operator,
            7,
            response(
                codes::UNSUPPORTED_VERSION,
                Some(
                    "The current MetadataVersion is too old to support controller unregistration.",
                ),
            ),
        ),
    ];
    for (case, principal, controller_id, want) in cases {
        assert!(
            send(&broker, principal, controller_id).await == want,
            "{case}"
        );
    }
    assert!(
        broker
            .controller
            .current_image()
            .controller(NodeId(7))
            .is_some()
    );

    submit(&broker, vec![metadata_version(33)]).await;
    let cases = [
        (
            "the active controller, a voter",
            1,
            response(
                codes::INVALID_REQUEST,
                Some("Cannot unregister controller 1 because it is part of the voter set."),
            ),
        ),
        (
            "an unknown controller",
            9,
            response(
                codes::CONTROLLER_ID_NOT_REGISTERED,
                Some("Controller ID 9 is not currently registered."),
            ),
        ),
        (
            "a registered controller",
            7,
            response(codes::NONE, Some("")),
        ),
        (
            "the same controller again",
            7,
            response(
                codes::CONTROLLER_ID_NOT_REGISTERED,
                Some("Controller ID 7 is not currently registered."),
            ),
        ),
    ];
    for (case, controller_id, want) in cases {
        assert!(
            send(&broker, operator, controller_id).await == want,
            "{case}"
        );
    }
    assert!(
        broker
            .controller
            .current_image()
            .controller(NodeId(7))
            .is_none()
    );
    handle.shutdown().await;
}
