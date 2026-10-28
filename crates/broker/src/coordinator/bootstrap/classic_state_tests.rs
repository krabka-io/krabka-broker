//! Tests for the rebuild of a classic group's members and state from a
//! persisted `GroupMetadata` value.

use {
    super::{
        apply::apply_group_metadata,
        replay::{Replayed, apply_record, finalize},
        test_support::{bare_coordinator, classic_group_record},
    },
    crate::{
        codes,
        coordinator::{
            persistence::{self, GroupMetadataValue, MemberMetadata},
            unified::{
                actor::{GroupActorHandle, GroupActorMessage, JoinResult, JoinResultMember},
                classic_state::{
                    ClassicGroup as ClassicState, GroupState as ClassicGroupState, Member,
                },
                regex_resolver::no_topic_regex_resolver,
            },
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_protocol::{
        owned::join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        records::RecordBatch,
    },
    std::{
        collections::HashMap,
        time::{Duration, Instant},
    },
    tokio::sync::oneshot,
};

fn stored_member(
    group_instance_id: Option<&str>,
    rebalance_timeout_ms: i32,
    session_timeout_ms: i32,
) -> MemberMetadata {
    MemberMetadata {
        member_id: "m1".into(),
        group_instance_id: group_instance_id.map(str::to_owned),
        client_id: "c".into(),
        client_host: "h".into(),
        rebalance_timeout_ms,
        session_timeout_ms,
        subscription: Bytes::from_static(b"sub"),
        assignment: Bytes::from_static(b"asn"),
    }
}

fn stored_group(
    protocol_type: &str,
    protocol_name: Option<&str>,
    members: Vec<MemberMetadata>,
) -> GroupMetadataValue {
    GroupMetadataValue {
        protocol_type: protocol_type.into(),
        generation: 5,
        protocol_name: protocol_name.map(str::to_owned),
        leader: (!members.is_empty()).then(|| "m1".to_owned()),
        current_state_timestamp_ms: 0,
        members,
    }
}

// The member `m1` of `stored_member`, as replay should rebuild it.
fn loaded_member(
    group_instance_id: Option<&str>,
    rebalance_timeout: Duration,
    session_timeout: Duration,
) -> Member {
    Member {
        id: "m1".into(),
        group_instance_id: group_instance_id.map(str::to_owned),
        client_id: "c".into(),
        host: "h".into(),
        session_timeout,
        rebalance_timeout,
        last_heartbeat: Instant::now(),
        protocol_metadata: Bytes::from_static(b"sub"),
        protocols: vec![("range".into(), Bytes::from_static(b"sub"))],
        assignment: Some(Bytes::from_static(b"asn")),
        is_new: false,
    }
}

/// Independently expected stable group rebuilt from the row's stored member fields.
fn loaded_group(
    instance_id: Option<&str>,
    rebalance_timeout: Duration,
    session_timeout: Duration,
) -> ClassicState {
    ClassicState {
        state: ClassicGroupState::Stable,
        protocol_type: Some("consumer".into()),
        generation_id: 5,
        leader_id: Some("m1".into()),
        protocol_name: Some("range".into()),
        members: HashMap::from([(
            "m1".to_owned(),
            loaded_member(instance_id, rebalance_timeout, session_timeout),
        )]),
        static_members: instance_id
            .into_iter()
            .map(|id| (id.to_owned(), "m1".to_owned()))
            .collect(),
        ..ClassicState::new("g")
    }
}

// Kafka's `GroupMetadataManager.replay(GroupMetadataKey, GroupMetadataValue)`:
// every loaded member supports the selected protocol with its stored
// subscription, a stored rebalance timeout of -1 becomes the session timeout,
// and an empty protocol type becomes no protocol type.

// Queues a `JoinGroup` v5 from `member_id` that proposes `range` with
// `metadata`, and returns the channel its reply arrives on.
async fn queue_join(
    handle: &GroupActorHandle,
    member_id: &str,
    metadata: &'static [u8],
) -> oneshot::Receiver<JoinResult> {
    let (reply, joined) = oneshot::channel();
    handle
        .tx
        .send(GroupActorMessage::ClassicJoin {
            req: JoinGroupRequest {
                group_id: "g".into(),
                session_timeout_ms: 30_000,
                rebalance_timeout_ms: 60_000,
                member_id: member_id.into(),
                protocol_type: "consumer".into(),
                protocols: vec![JoinGroupRequestProtocol {
                    name: "range".into(),
                    metadata: Bytes::from_static(metadata),
                    ..Default::default()
                }],
                ..Default::default()
            },
            version: 5,
            client_id: "client-a".into(),
            client_host: "/127.0.0.1".into(),
            regex_resolver: no_topic_regex_resolver(),
            reply,
        })
        .await
        .expect("the group actor is running");
    joined
}

// After a coordinator restart, a new member that proposes the replayed
// group's protocol joins it, and the rebalance completes once the replayed
// member joins again. Kafka's replay gives the loaded member the group's
// protocol, so its `supportsProtocols` check lets the new member in.
#[tokio::test]
async fn a_replayed_group_admits_a_new_member_that_proposes_its_protocol() {
    let coordinator = bare_coordinator();
    let (key, value) = classic_group_record("g", "m1");
    let mut replayed = Replayed::default();
    apply_record(
        &coordinator,
        &mut replayed,
        persistence::parse_key(&key).unwrap(),
        &value,
        &RecordBatch::default(),
    )
    .unwrap();
    finalize(&coordinator, replayed).await;
    let handle = coordinator
        .find("g")
        .expect("the replayed group has an actor");

    // KIP-394: a new member's first `JoinGroup` gets the member id to use.
    let asked = queue_join(&handle, "", b"new").await.await.unwrap();
    assert!(
        asked
            == JoinResult {
                error_code: codes::MEMBER_ID_REQUIRED,
                member_id: asked.member_id.clone(),
                ..JoinResult::default()
            }
    );
    let new_member = asked.member_id;
    let new_member_joined = queue_join(&handle, &new_member, b"new").await;
    let leader = queue_join(&handle, "m1", b"old").await.await.unwrap();
    let follower = new_member_joined.await.unwrap();

    let next_generation = JoinResult {
        error_code: codes::NONE,
        generation_id: 4,
        protocol_type: Some("consumer".into()),
        protocol_name: Some("range".into()),
        leader: "m1".into(),
        ..JoinResult::default()
    };
    let mut members = vec![
        JoinResultMember {
            member_id: "m1".into(),
            group_instance_id: None,
            metadata: Bytes::from_static(b"old"),
        },
        JoinResultMember {
            member_id: new_member.clone(),
            group_instance_id: None,
            metadata: Bytes::from_static(b"new"),
        },
    ];
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    assert!(
        leader
            == JoinResult {
                member_id: "m1".into(),
                members,
                ..next_generation.clone()
            }
    );
    assert!(
        follower
            == JoinResult {
                member_id: new_member,
                ..next_generation
            }
    );
}

use rstest_bdd_macros::{given, scenarios, then, when};
#[derive(Default)]
struct ClassicLoad {
    stored: Option<GroupMetadataValue>,
    loaded: Option<ClassicState>,
    expected: Option<ClassicState>,
}
#[rstest::fixture]
fn classic_load() -> ClassicLoad {
    // Start this scenario with fresh inputs and observations.

    ClassicLoad::default()
}
#[given(
    "a persisted classic consumer member has instance {instance} rebalance timeout {rebalance} and session timeout {session} milliseconds"
)]
fn persisted_classic_member(
    classic_load: &mut ClassicLoad,
    instance: String,
    rebalance: i32,
    session: i32,
) {
    let instance = if instance == "absent" {
        None
    } else {
        Some(instance.as_str())
    };
    classic_load.stored = Some(stored_group("consumer", Some("range"), vec![
        stored_member(instance, rebalance, session),
    ]));
    let rebalance = if rebalance == -1 { session } else { rebalance };
    classic_load.expected = Some(loaded_group(
        instance,
        Duration::from_millis(u64::try_from(rebalance).expect("rebalance milliseconds")),
        Duration::from_millis(u64::try_from(session).expect("session milliseconds")),
    ));
}
#[given("persisted classic group g is empty with an empty protocol type and generation 5")]
fn persisted_empty_classic_group(classic_load: &mut ClassicLoad) {
    classic_load.stored = Some(stored_group("", None, vec![]));
    classic_load.expected = Some(ClassicState {
        generation_id: 5,
        ..ClassicState::new("g")
    });
}
#[when("bootstrap applies the classic group metadata record")]
fn replay_classic_group(classic_load: &mut ClassicLoad) {
    let mut loaded = ClassicState::new("g");
    apply_group_metadata(
        &mut loaded,
        classic_load
            .stored
            .take()
            .expect("persisted group prepared"),
        0,
    );
    classic_load.loaded = Some(loaded);
}
#[then(
    "the loaded classic group preserves all stored member and protocol fields with Kafka timeout defaults"
)]
fn loaded_classic_group(classic_load: &ClassicLoad) {
    let loaded = classic_load.loaded.as_ref().expect("group replayed");
    let mut expected = classic_load
        .expected
        .as_ref()
        .expect("expected group")
        .clone();
    for (identifier, member) in &mut expected.members {
        if let Some(replayed) = loaded.members.get(identifier) {
            member.last_heartbeat = replayed.last_heartbeat;
        }
    }
    assert!(loaded == &expected);
}
scenarios!("tests/features/migrated/src/coordinator/bootstrap/classic_state_tests.feature",fixtures=[classic_load:ClassicLoad]);
