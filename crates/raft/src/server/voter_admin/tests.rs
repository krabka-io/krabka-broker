//! Behaviour tests for the voter-reconfiguration handlers: what each
//! request must carry before the quorum sees it, and what a refusal says.

use assert2::check;
use krabka_protocol::Decode;

use super::*;
use crate::server::test_support::{single_voter_engine, wait_for_leader};

/// A wire listener set is usable only when every entry is named, hosted
/// and on a real port, no name repeats, and there is at least one.
#[test]
fn wire_listeners_must_be_usable_and_uniquely_named() {
    type Row<'a> = (&'a str, Vec<(&'a str, &'a str, u16)>, bool);
    let cases: Vec<Row<'_>> = vec![
        (
            "one usable listener",
            vec![("CONTROLLER", "host", 9093)],
            true,
        ),
        (
            "two, differently named",
            vec![("CONTROLLER", "host", 9093), ("PLAINTEXT", "host", 9092)],
            true,
        ),
        ("none at all", vec![], false),
        ("a nameless listener", vec![("", "host", 9093)], false),
        ("a hostless listener", vec![("CONTROLLER", "", 9093)], false),
        ("port zero", vec![("CONTROLLER", "host", 0)], false),
        (
            "a repeated name",
            vec![("CONTROLLER", "host", 9093), ("CONTROLLER", "other", 9094)],
            false,
        ),
        (
            "one good listener followed by a bad one",
            vec![("CONTROLLER", "host", 9093), ("", "host", 9094)],
            false,
        ),
    ];
    for (what, listeners, usable) in cases {
        check!(valid_wire_listeners(listeners.clone()) == usable, "{what}");
    }
}

/// A reconfiguration request is refused before it reaches the quorum when
/// it names the wrong cluster, a negative voter, or a zero directory id.
///
/// Each is a separate arm returning `INVALID_REQUEST` with a reason, and a
/// request that slipped past them would be applied to the voter set --
/// a zero directory id in particular names no real incarnation.
#[tokio::test]
async fn a_malformed_reconfiguration_is_refused_before_the_quorum_sees_it() {
    use krabka_protocol::{
        Encode as _,
        owned::{
            remove_raft_voter_request::{self, RemoveRaftVoterRequest},
            remove_raft_voter_response::RemoveRaftVoterResponse,
        },
    };

    const INVALID_REQUEST: i16 = 42;
    let version = remove_raft_voter_request::MAX_VERSION;
    let (engine, _dir) = single_voter_engine();
    wait_for_leader(&engine).await;
    let cluster_id = engine.current_image().cluster_id().to_string();

    let real_directory = krabka_protocol::primitives::uuid::Uuid([7u8; 16]);
    // (what it is, cluster id sent, voter id, directory id)
    let cases: Vec<(
        &str,
        Option<String>,
        i32,
        krabka_protocol::primitives::uuid::Uuid,
    )> = vec![
        (
            "another cluster's id",
            Some("00000000-0000-0000-0000-0000000000ff".to_owned()),
            2,
            real_directory,
        ),
        (
            "a negative voter id",
            Some(cluster_id.clone()),
            -1,
            real_directory,
        ),
        (
            "a zero directory id",
            Some(cluster_id.clone()),
            2,
            krabka_protocol::primitives::uuid::Uuid::ZERO,
        ),
    ];

    for (what, request_cluster_id, voter_id, voter_directory_id) in cases {
        let request = RemoveRaftVoterRequest {
            cluster_id: request_cluster_id,
            voter_id,
            voter_directory_id,
            ..Default::default()
        };
        let mut body = BytesMut::new();
        request.encode(&mut body, version).expect("encode request");
        let bytes = remove_raft_voter_response(version, &body.freeze(), &engine)
            .await
            .expect("a refusal is still a response");
        let mut cursor = &bytes[..];
        let decoded = RemoveRaftVoterResponse::decode(&mut cursor, version).expect("decode");
        check!(decoded.error_code == INVALID_REQUEST, "{what}: error code");
        check!(decoded.error_message.is_some(), "{what}: says why");
    }

    // Omitting the cluster id entirely is allowed: the field is optional,
    // and only a mismatch is a refusal.
    let request = RemoveRaftVoterRequest {
        cluster_id: None,
        voter_id: 2,
        voter_directory_id: real_directory,
        ..Default::default()
    };
    let mut body = BytesMut::new();
    request.encode(&mut body, version).expect("encode request");
    let bytes = remove_raft_voter_response(version, &body.freeze(), &engine)
        .await
        .expect("response");
    let mut cursor = &bytes[..];
    let decoded = RemoveRaftVoterResponse::decode(&mut cursor, version).expect("decode");
    check!(
        decoded.error_code != INVALID_REQUEST,
        "a well-formed request reaches the quorum, got {}",
        decoded.error_code
    );
}

/// `AddRaftVoter` refuses a candidate it cannot place: an unusable
/// listener set is as disqualifying as a bad id.
///
/// A voter with no reachable endpoint cannot be fetched from, so admitting
/// one puts a member in the quorum that can never catch up -- and the
/// majority it counts toward is computed from the set, not from who
/// answers.
#[tokio::test]
async fn adding_a_voter_needs_an_id_and_a_reachable_listener() {
    use krabka_protocol::{
        Encode as _,
        owned::{
            add_raft_voter_request::{self, AddRaftVoterRequest, Listener},
            add_raft_voter_response::AddRaftVoterResponse,
        },
    };

    const INVALID_REQUEST: i16 = 42;
    let version = add_raft_voter_request::MAX_VERSION;
    let (engine, _dir) = single_voter_engine();
    wait_for_leader(&engine).await;

    let good_listener = || Listener {
        name: "CONTROLLER".to_owned(),
        host: "host-b".to_owned(),
        port: 9093,
        ..Default::default()
    };
    let directory = krabka_protocol::primitives::uuid::Uuid([9u8; 16]);

    // (what it is, voter id, directory id, listeners)
    let cases: Vec<(
        &str,
        i32,
        krabka_protocol::primitives::uuid::Uuid,
        Vec<Listener>,
    )> = vec![
        ("a negative voter id", -1, directory, vec![good_listener()]),
        (
            "a zero directory id",
            2,
            krabka_protocol::primitives::uuid::Uuid::ZERO,
            vec![good_listener()],
        ),
        ("no listeners at all", 2, directory, vec![]),
        (
            "a listener on port zero",
            2,
            directory,
            vec![Listener {
                port: 0,
                ..good_listener()
            }],
        ),
        (
            "a listener with no host",
            2,
            directory,
            vec![Listener {
                host: String::new(),
                ..good_listener()
            }],
        ),
    ];

    for (what, voter_id, voter_directory_id, listeners) in cases {
        let request = AddRaftVoterRequest {
            cluster_id: None,
            voter_id,
            voter_directory_id,
            listeners,
            ..Default::default()
        };
        let mut body = BytesMut::new();
        request.encode(&mut body, version).expect("encode");
        let bytes = add_raft_voter_response(version, &body.freeze(), &engine)
            .await
            .expect("a refusal is still a response");
        let mut cursor = &bytes[..];
        let decoded = AddRaftVoterResponse::decode(&mut cursor, version).expect("decode");
        check!(decoded.error_code == INVALID_REQUEST, "{what}");
        check!(decoded.error_message.is_some(), "{what}: says why");
    }
}

/// `UpdateRaftVoter` additionally requires the caller to name the cluster,
/// to be at the leader's epoch, and to advertise a coherent
/// `kraft.version` range.
///
/// The epoch check is what stops a stale caller rewriting a voter's
/// endpoints against a quorum that has since moved on, and an inverted
/// version range advertises support for nothing.
#[tokio::test]
async fn updating_a_voter_needs_the_cluster_the_epoch_and_a_coherent_range() {
    use krabka_protocol::{
        Encode as _,
        owned::{
            update_raft_voter_request::{
                self, KRaftVersionFeature, Listener, UpdateRaftVoterRequest,
            },
            update_raft_voter_response::UpdateRaftVoterResponse,
        },
    };

    // `UpdateRaftVoter` reports a malformed request as 141, where the add
    // and remove paths use 42.
    const INVALID_UPDATE: i16 = 141;
    let version = update_raft_voter_request::MAX_VERSION;
    let (engine, _dir) = single_voter_engine();
    wait_for_leader(&engine).await;
    let cluster_id = engine.current_image().cluster_id().to_string();
    let epoch = i32::try_from(engine.quorum_state().await.expect("quorum").leader_epoch)
        .expect("epoch fits i32");
    let directory = krabka_protocol::primitives::uuid::Uuid([9u8; 16]);
    let listener = || Listener {
        name: "CONTROLLER".to_owned(),
        host: "host-b".to_owned(),
        port: 9093,
        ..Default::default()
    };
    let range = |min: i16, max: i16| KRaftVersionFeature {
        min_supported_version: min,
        max_supported_version: max,
        ..Default::default()
    };

    // (what it is, cluster id, epoch offered, version range)
    let cases: Vec<(&str, Option<String>, i32, KRaftVersionFeature)> = vec![
        ("no cluster id at all", None, epoch, range(0, 1)),
        (
            "another cluster's id",
            Some("00000000-0000-0000-0000-0000000000ff".to_owned()),
            epoch,
            range(0, 1),
        ),
        (
            "an epoch the quorum has left behind",
            Some(cluster_id.clone()),
            epoch + 1,
            range(0, 1),
        ),
        (
            "an inverted kraft.version range",
            Some(cluster_id.clone()),
            epoch,
            range(2, 1),
        ),
    ];

    for (what, request_cluster_id, current_leader_epoch, k_raft_version_feature) in cases {
        let request = UpdateRaftVoterRequest {
            cluster_id: request_cluster_id,
            voter_id: 1,
            voter_directory_id: directory,
            current_leader_epoch,
            k_raft_version_feature,
            listeners: vec![listener()],
            ..Default::default()
        };
        let mut body = BytesMut::new();
        request.encode(&mut body, version).expect("encode");
        let bytes = update_raft_voter_response(version, &body.freeze(), &engine)
            .await
            .expect("response");
        let mut cursor = &bytes[..];
        let decoded = UpdateRaftVoterResponse::decode(&mut cursor, version).expect("decode");
        check!(decoded.error_code == INVALID_UPDATE, "{what}");
    }
}
