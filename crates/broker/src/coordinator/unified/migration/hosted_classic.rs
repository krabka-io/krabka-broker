//! Serving hosted classic members from the consumer-group reconciler.
//!
//! A classic member hosted in a consumer group keeps speaking the classic
//! `Heartbeat`, `JoinGroup`, and `SyncGroup` RPCs. This module maps those onto
//! the consumer-group machinery as Kafka's `GroupMetadataManager` does: the
//! `JoinGroup` reconciles the member like a heartbeat, the `SyncGroup` returns
//! the partitions the member is assigned and writes nothing, and the
//! `Heartbeat` asks the member to rejoin while it has not reconciled to the
//! group's target assignment.

use {
    super::assignment::{consumer_assignment_blob, embedded_protocol_version},
    crate::{
        codes,
        coordinator::unified::{
            actor::{JoinResult, SyncResult},
            consumer_state::{ClassicMemberFacade, GroupState as ConsumerState, MemberState},
            persistence_next_gen::MemberAssignmentState,
            reconciler::ReconcileInput,
        },
    },
    bytes::Bytes,
    krabka_protocol::owned::{
        heartbeat_request::HeartbeatRequest, sync_group_request::SyncGroupRequest,
    },
    std::{
        collections::HashSet,
        time::{Duration, Instant},
    },
};

/// The classic protocol type that a consumer group serves.
const CONSUMER_PROTOCOL_TYPE: &str = "consumer";

/// Kafka's `validateConsumerGroupMember`: the member a classic `Heartbeat` or
/// `SyncGroup` names. A static member is looked up by its instance id, and a
/// request whose member id is not the one that holds the instance id is
/// fenced.
fn validate_member<'a>(
    state: &'a ConsumerState,
    member_id: &str,
    instance_id: Option<&str>,
) -> Result<&'a MemberState, i16> {
    let Some(instance_id) = instance_id else {
        return state.members.get(member_id).ok_or(codes::UNKNOWN_MEMBER_ID);
    };
    let member = state
        .current_member_for_instance(instance_id)
        .and_then(|id| state.members.get(id))
        .ok_or(codes::UNKNOWN_MEMBER_ID)?;
    if member.member_id != member_id {
        return Err(codes::FENCED_INSTANCE_ID);
    }
    Ok(member)
}

/// A consumer-group member that uses the classic protocol, and its classic
/// facade.
struct HostedClassicMember<'a> {
    member: &'a MemberState,
    facade: &'a ClassicMemberFacade,
}

/// The checks that Kafka's `classicGroupHeartbeatToConsumerGroup` and
/// `classicGroupSyncToConsumerGroup` share: the member exists and is not
/// fenced (`validateConsumerGroupMember`), it uses the classic protocol
/// (`throwIfMemberDoesNotUseClassicProtocol`, `UNKNOWN_MEMBER_ID`), and the
/// request's generation is its member epoch (`throwIfGenerationIdUnmatched`,
/// `ILLEGAL_GENERATION`).
fn validate_classic_member<'a>(
    state: &'a ConsumerState,
    member_id: &str,
    instance_id: Option<&str>,
    generation_id: i32,
) -> Result<HostedClassicMember<'a>, i16> {
    let member = validate_member(state, member_id, instance_id)?;
    let facade = member.classic.as_ref().ok_or(codes::UNKNOWN_MEMBER_ID)?;
    if member.member_epoch != generation_id {
        return Err(codes::ILLEGAL_GENERATION);
    }
    Ok(HostedClassicMember { member, facade })
}

/// Kafka's `classicGroupHeartbeatToConsumerGroup`.
///
/// After the member checks, the heartbeat refreshes the member's session and
/// answers `REBALANCE_IN_PROGRESS`, which asks the client to send `JoinGroup`
/// again, when the member must rejoin to reconcile:
///
/// 1. the target assignment epoch moved past the member epoch;
/// 2. the member has partitions to revoke; or
/// 3. the member waits on partitions that their previous owners released.
///
/// Otherwise it answers `NONE`. It writes nothing.
pub fn serve_classic_heartbeat(state: &mut ConsumerState, request: &HeartbeatRequest) -> i16 {
    let (member_id, rejoin) = match validate_classic_member(
        state,
        &request.member_id,
        request.group_instance_id.as_deref(),
        request.generation_id,
    ) {
        Ok(HostedClassicMember { member, .. }) => (
            member.member_id.clone(),
            member.member_epoch < state.target.epoch
                || member.assignment_state == MemberAssignmentState::UnrevokedPartitions
                || (member.assignment_state == MemberAssignmentState::UnreleasedPartitions
                    && !state.waiting_on_unreleased_partition(&member.member_id)),
        ),
        Err(code) => return code,
    };
    if let Some(member) = state.members.get_mut(&member_id) {
        member.last_seen = Instant::now();
    }
    if rejoin {
        codes::REBALANCE_IN_PROGRESS
    } else {
        codes::NONE
    }
}

/// Kafka's `classicGroupSyncToConsumerGroup`.
///
/// The member's `JoinGroup` already reconciled it, so the sync only answers:
/// after the member checks, the protocol type and name must be the ones its
/// `JoinGroup` was answered with (`throwIfClassicProtocolUnmatched`,
/// `INCONSISTENT_GROUP_PROTOCOL`), and a member whose epoch the target
/// assignment moved past must rejoin first (`throwIfRebalanceInProgress`,
/// `REBALANCE_IN_PROGRESS`), unless it is revoking partitions.
///
/// The answer echoes the request's protocol type and name and carries the
/// member's assigned partitions (`prepareAssignment`) as a
/// `ConsumerProtocolAssignment` blob, at the version of the metadata of the
/// member's first protocol. The sync writes nothing and changes nothing.
pub fn serve_classic_sync(
    state: &ConsumerState,
    request: &SyncGroupRequest,
    image: &ReconcileInput,
) -> SyncResult {
    let refused = |error_code| SyncResult {
        error_code,
        ..SyncResult::default()
    };
    let HostedClassicMember { member, facade } = match validate_classic_member(
        state,
        &request.member_id,
        request.group_instance_id.as_deref(),
        request.generation_id,
    ) {
        Ok(found) => found,
        Err(code) => return refused(code),
    };
    let first_protocol = facade.supported_protocols.first();
    if request
        .protocol_type
        .as_deref()
        .is_some_and(|protocol_type| protocol_type != CONSUMER_PROTOCOL_TYPE)
        || request
            .protocol_name
            .as_deref()
            .is_some_and(|name| first_protocol.is_none_or(|(first, _)| first.as_str() != name))
    {
        return refused(codes::INCONSISTENT_GROUP_PROTOCOL);
    }
    if state.target.epoch > member.member_epoch
        && member.assignment_state != MemberAssignmentState::UnrevokedPartitions
    {
        return refused(codes::REBALANCE_IN_PROGRESS);
    }
    // `prepareAssignment` reads the version off the first protocol's
    // metadata, and a blob without a version is an `IllegalStateException`.
    let Some(version) = first_protocol
        .and_then(|(_, metadata)| embedded_protocol_version(metadata))
        .filter(|version| *version >= 0)
    else {
        return refused(codes::UNKNOWN_SERVER_ERROR);
    };
    let Some(assignment) = consumer_assignment_blob(&member.assigned_partitions, image, version)
    else {
        return refused(codes::UNKNOWN_SERVER_ERROR);
    };
    SyncResult {
        error_code: codes::NONE,
        assignment,
        protocol_type: request.protocol_type.clone(),
        protocol_name: request.protocol_name.clone(),
    }
}

/// Kafka's `ConsumerGroup.supportsClassicProtocols` for a group with members:
/// the protocol type is `consumer`, and one of `protocols` is supported by
/// every member that uses the classic protocol.
pub fn supports_classic_protocols(
    state: &ConsumerState,
    protocol_type: &str,
    protocols: &HashSet<&str>,
) -> bool {
    if protocol_type != CONSUMER_PROTOCOL_TYPE {
        return false;
    }
    if state.members.is_empty() {
        return !protocols.is_empty();
    }
    protocols.iter().any(|name| {
        state
            .members
            .values()
            .filter_map(|member| member.classic.as_ref())
            .all(|facade| {
                facade
                    .supported_protocols
                    .iter()
                    .any(|(supported, _)| supported == name)
            })
    })
}

/// A classic `JoinGroup` for a member of a consumer group, as Kafka's
/// `classicGroupJoinToConsumerGroup` builds its `updatedMember` from it.
pub struct ClassicMemberRegistration {
    pub member_id: String,
    pub subscription_topics: HashSet<String>,
    /// The rack of the member's `ConsumerProtocolSubscription`, if any.
    pub rack_id: Option<String>,
    pub protocols: Vec<(String, Bytes)>,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout: Duration,
    /// The member's rebalance timeout. The caller resolves Kafka's `-1`
    /// sentinel (`maybeUpdateRebalanceTimeoutMs(ofSentinel(..))`) to the
    /// stored timeout.
    pub rebalance_timeout: Duration,
    pub instance_id: Option<String>,
}

/// Creates or updates a classic member of a consumer group, as Kafka's
/// `classicGroupJoinToConsumerGroup` builds its `updatedMember`.
///
/// The member keeps its epochs, its assignment, its server assignor
/// (`maybeUpdateServerAssignorName` of nothing), and its pattern, which the
/// caller drops through Kafka's regex update. An absent instance id or rack
/// keeps the stored one (`maybeUpdate*`). The topic names,
/// client and classic metadata are replaced. A new member starts at epoch 0,
/// as Kafka's `getOrMaybeCreateMember` creates it.
pub fn upsert_classic_member(state: &mut ConsumerState, registration: ClassicMemberRegistration) {
    let ClassicMemberRegistration {
        member_id,
        subscription_topics,
        rack_id,
        protocols,
        client_id,
        client_host,
        session_timeout,
        rebalance_timeout,
        instance_id,
    } = registration;
    let existing = state.members.get(&member_id);
    let member = MemberState {
        member_id: member_id.clone(),
        instance_id: instance_id.or_else(|| {
            let m = existing?;
            m.instance_id.clone()
        }),
        rack_id: rack_id.or_else(|| {
            let m = existing?;
            m.rack_id.clone()
        }),
        client_id,
        client_host,
        subscribed_topic_names: subscription_topics,
        subscribed_topic_regex: existing.and_then(|m| m.subscribed_topic_regex.clone()),
        server_assignor: existing.and_then(|m| m.server_assignor.clone()),
        rebalance_timeout,
        member_epoch: existing.map_or(0, |m| m.member_epoch),
        previous_member_epoch: existing.map_or(0, |m| m.previous_member_epoch),
        assignment_state: existing.map_or(MemberAssignmentState::Stable, |m| m.assignment_state),
        assigned_partitions: existing.map_or_default(|m| m.assigned_partitions.clone()),
        partitions_pending_revocation: existing
            .map_or_default(|m| m.partitions_pending_revocation.clone()),
        assignment_epochs: existing.map_or_default(|m| m.assignment_epochs.clone()),
        last_seen: Instant::now(),
        classic: Some(ClassicMemberFacade {
            supported_protocols: protocols,
            session_timeout,
        }),
    };
    state.add_or_update_member(member);
}

/// Builds the `JoinGroup` result for a hosted classic member, as Kafka's
/// `classicGroupJoinToConsumerGroup` does.
///
/// The coordinator computes the assignment, so the result names no leader and
/// lists no members. A classic client then joins as a follower and sends an
/// empty `SyncGroup`, which returns its assignment. A client that finds its own
/// id in `leader` runs its assignor over the member list. An entry with no
/// subscription metadata makes the Java client fail to parse it.
///
/// The generation is the member epoch, and the protocol name is the member's
/// first protocol. The client sends both back in `SyncGroup`, and the
/// generation in `Heartbeat` and `OffsetCommit`, whose fence compares it with
/// the member epoch.
pub fn build_hosted_classic_join_result(member: &MemberState) -> JoinResult {
    JoinResult {
        error_code: codes::NONE,
        generation_id: member.member_epoch,
        protocol_type: Some(CONSUMER_PROTOCOL_TYPE.into()),
        protocol_name: member
            .classic
            .as_ref()
            .and_then(|facade| facade.supported_protocols.first())
            .map(|(name, _)| name.clone()),
        member_id: member.member_id.clone(),
        ..JoinResult::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::coordinator::unified::actor::test_support::topic_reconcile_input,
        assert2::check, krabka_protocol::primitives::uuid::Uuid, std::collections::HashMap,
    };

    const TOPIC: Uuid = Uuid([7; 16]);
    const TARGET_EPOCH: i32 = 5;

    /// A member at the target epoch that holds partition 0 of `t`, with a
    /// classic facade whose first protocol, `range`, carries a version-1
    /// subscription prefix, or no facade when `classic` is unset.
    fn member(id: &str, classic: bool) -> MemberState {
        MemberState {
            member_id: id.into(),
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: ["t".to_owned()].into(),
            subscribed_topic_regex: None,
            server_assignor: None,
            rebalance_timeout: Duration::from_secs(60),
            member_epoch: TARGET_EPOCH,
            previous_member_epoch: TARGET_EPOCH - 1,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: [(TOPIC, vec![0])].into(),
            partitions_pending_revocation: HashMap::new(),
            assignment_epochs: HashMap::new(),
            last_seen: Instant::now(),
            classic: classic.then(|| ClassicMemberFacade {
                supported_protocols: vec![
                    ("range".into(), Bytes::from_static(&[0, 1, 0, 0])),
                    ("roundrobin".into(), Bytes::from_static(&[0, 1, 0, 0])),
                ],
                session_timeout: Duration::from_secs(45),
            }),
        }
    }

    /// A group at target epoch 5 with `members`, each targeted at what it
    /// holds plus partition 1 for `m`.
    fn group(members: Vec<MemberState>) -> ConsumerState {
        let mut state = ConsumerState::new("g");
        state.group_epoch = TARGET_EPOCH;
        state.target.epoch = TARGET_EPOCH;
        for member in members {
            let target = if member.member_id == "m" {
                [(TOPIC, vec![0, 1])].into()
            } else {
                member.assigned_partitions.clone()
            };
            state
                .target
                .per_member
                .insert(member.member_id.clone(), target);
            state.add_or_update_member(member);
        }
        state
    }

    fn image() -> ReconcileInput { topic_reconcile_input("t", TOPIC, 2) }

    fn sync_request(member_id: &str, generation_id: i32) -> SyncGroupRequest {
        SyncGroupRequest {
            group_id: "g".into(),
            member_id: member_id.into(),
            generation_id,
            protocol_type: Some("consumer".into()),
            protocol_name: Some("range".into()),
            ..SyncGroupRequest::default()
        }
    }

    fn refused(error_code: i16) -> SyncResult {
        SyncResult {
            error_code,
            ..SyncResult::default()
        }
    }

    /// Kafka's `classicGroupSyncToConsumerGroup`: (label, the group's
    /// members, the request) to the whole answer. A served sync returns the
    /// partitions the member is ASSIGNED, not its target, at the version of
    /// its first protocol, and echoes the request's protocol type and name.
    /// Kafka's `checkAssignmentVersion`: the assignment is written at the
    /// version of the member's first protocol, a version above the highest
    /// the schema knows as the highest, and a negative version is an error.
    #[test]
    fn classic_sync_writes_the_assignment_at_the_members_protocol_version() {
        for (prefix, want) in [(0_i16, Some(0_i16)), (3, Some(3)), (7, Some(3)), (-1, None)] {
            let mut m = member("m", true);
            let mut metadata = prefix.to_be_bytes().to_vec();
            metadata.extend([0, 0, 0, 0]);
            m.classic = Some(ClassicMemberFacade {
                supported_protocols: vec![("range".into(), Bytes::from(metadata))],
                session_timeout: Duration::from_secs(45),
            });
            let state = group(vec![m]);

            let got = serve_classic_sync(&state, &sync_request("m", TARGET_EPOCH), &image());

            let want = want.map_or_else(
                || refused(codes::UNKNOWN_SERVER_ERROR),
                |version| SyncResult {
                    error_code: codes::NONE,
                    assignment: consumer_assignment_blob(
                        &[(TOPIC, vec![0])].into(),
                        &image(),
                        version,
                    )
                    .unwrap(),
                    protocol_type: Some("consumer".into()),
                    protocol_name: Some("range".into()),
                },
            );
            check!(got == want, "version prefix {prefix}");
        }
    }

    /// Kafka's `classicGroupHeartbeatToConsumerGroup`: (label, the member's
    /// epoch and state, whether another member still holds the partition the
    /// member waits for) to the answer. The member rejoins when the target
    /// moved past it, when it revokes, and when the partition it waits for
    /// is free.
    #[test]
    fn classic_heartbeat_waits_for_large_reordered_assignments_and_refreshes_sessions() {
        for partition_count in [1_000, 2_000, 4_000, 8_000] {
            for pending_revocation in [false, true] {
                let last_partition = partition_count - 1;
                let assigned: Vec<_> = (0..last_partition).rev().collect();
                let mut target: Vec<_> = (0..partition_count).rev().collect();
                target.push(0);
                let mut m = member("m", true);
                m.assignment_state = MemberAssignmentState::UnreleasedPartitions;
                m.assigned_partitions = [(TOPIC, assigned)].into();
                m.stamp_assignment_epochs(TARGET_EPOCH);
                let mut other = member("other", false);
                other.assigned_partitions.clear();
                let holdings = if pending_revocation {
                    &mut other.partitions_pending_revocation
                } else {
                    &mut other.assigned_partitions
                };
                holdings.insert(TOPIC, vec![last_partition]);
                let mut state = group(vec![m, other]);
                state
                    .target
                    .per_member
                    .insert("m".into(), [(TOPIC, target)].into());
                let request = HeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m".into(),
                    generation_id: TARGET_EPOCH,
                    ..HeartbeatRequest::default()
                };
                let before = state.members["m"].clone();

                for (release, want) in [(false, codes::NONE), (true, codes::REBALANCE_IN_PROGRESS)]
                {
                    if release {
                        let other = state.members.get_mut("other").unwrap();
                        other.assigned_partitions.clear();
                        other.partitions_pending_revocation.clear();
                    }
                    let previous_seen = Instant::now()
                        .checked_sub(Duration::from_secs(60))
                        .expect("the fixture clock supports a minute before now");
                    state.members.get_mut("m").unwrap().last_seen = previous_seen;

                    check!(
                        serve_classic_heartbeat(&mut state, &request) == want,
                        "partitions={partition_count}, pending={pending_revocation}, release={release}"
                    );
                    let after = &state.members["m"];
                    check!(after.last_seen > previous_seen);
                    check!(
                        (
                            &after.assigned_partitions,
                            &after.partitions_pending_revocation,
                            &after.assignment_epochs,
                            after.member_epoch,
                            after.previous_member_epoch,
                            after.assignment_state,
                        ) == (
                            &before.assigned_partitions,
                            &before.partitions_pending_revocation,
                            &before.assignment_epochs,
                            before.member_epoch,
                            before.previous_member_epoch,
                            before.assignment_state,
                        )
                    );
                }
            }
        }
    }

    /// The member checks of the classic `Heartbeat` are the sync's: (label,
    /// request) to the error.
    /// Kafka's `supportsClassicProtocols`: (label, protocol type, protocols)
    /// to whether a group whose classic members both support `range`, and one
    /// of them `roundrobin`, accepts them.
    #[test]
    fn classic_protocols_must_be_supported_by_every_classic_member() {
        let mut narrow = member("narrow", true);
        narrow.classic = Some(ClassicMemberFacade {
            supported_protocols: vec![("range".into(), Bytes::from_static(&[0, 1]))],
            session_timeout: Duration::from_secs(45),
        });
        let state = group(vec![member("m", true), narrow, member("native", false)]);
        for (label, protocol_type, protocols, want) in [
            ("a shared protocol", "consumer", vec!["range"], true),
            (
                "one shared among others",
                "consumer",
                vec!["sticky", "range"],
                true,
            ),
            (
                "a protocol one member lacks",
                "consumer",
                vec!["roundrobin"],
                false,
            ),
            ("another protocol type", "connect", vec!["range"], false),
        ] {
            let protocols: HashSet<&str> = protocols.into_iter().collect();

            check!(
                supports_classic_protocols(&state, protocol_type, &protocols) == want,
                "{label}"
            );
        }
    }
    use {
        rstest_bdd_macros::{given, scenarios, then, when},
        serde::Deserialize,
        serde_json::{Value, json},
    };
    #[derive(Default)]
    struct HostedClassic {
        state: Option<ConsumerState>,
        request: Option<SyncGroupRequest>,
        sync: Option<SyncResult>,
        heartbeat: Option<i16>,
        member_epoch: Option<i32>,
    }
    #[rstest::fixture]
    fn hosted_classic() -> HostedClassic {
        // Start this scenario with fresh inputs and observations.
        HostedClassic::default()
    }

    #[when("the consumer coordinator serves the hosted classic SyncGroup request")]
    fn hosted_sync(hosted_classic: &mut HostedClassic) {
        hosted_classic.sync = Some(serve_classic_sync(
            hosted_classic
                .state
                .as_ref()
                .expect("consumer group prepared"),
            hosted_classic
                .request
                .as_ref()
                .expect("sync request prepared"),
            &image(),
        ));
    }
    #[given(
        "hosted classic member m is at epoch {epoch} assignment state {assignment} with its waiting partition held elsewhere {held}"
    )]
    fn hosted_reconciliation_state(
        hosted_classic: &mut HostedClassic,
        epoch: i32,
        assignment: String,
        held: bool,
    ) {
        let mut classic = member("m", true);
        classic.member_epoch = epoch;
        classic.assignment_state = match assignment.as_str() {
            "stable" => MemberAssignmentState::Stable,
            "unrevoked" => MemberAssignmentState::UnrevokedPartitions,
            "unreleased" => MemberAssignmentState::UnreleasedPartitions,
            _ => panic!("unknown assignment state"),
        };
        let mut other = member("other", false);
        other.assigned_partitions = if held {
            [(TOPIC, vec![1])].into()
        } else {
            HashMap::new()
        };
        hosted_classic.state = Some(group(vec![classic, other]));
        hosted_classic.member_epoch = Some(epoch);
    }
    #[given(
        "consumer group g hosts classic member m at static instance instance-1 and native member native"
    )]
    fn static_hosted_members(hosted_classic: &mut HostedClassic) {
        let mut classic = member("m", true);
        classic.instance_id = Some("instance-1".into());
        hosted_classic.state = Some(group(vec![classic, member("native", false)]));
    }
    #[when("classic member {member_id} instance {instance} heartbeats at generation {generation}")]
    fn hosted_classic_heartbeat(
        hosted_classic: &mut HostedClassic,
        member_id: String,
        instance: String,
        generation: String,
    ) {
        let generation = match generation.as_str() {
            "member" => hosted_classic.member_epoch.expect("member epoch"),
            "target" => TARGET_EPOCH,
            "zero" => 0,
            _ => panic!("unknown heartbeat generation"),
        };
        let request = HeartbeatRequest {
            group_id: "g".into(),
            member_id,
            generation_id: generation,
            group_instance_id: if instance == "absent" {
                None
            } else {
                Some(instance)
            },
            ..HeartbeatRequest::default()
        };
        hosted_classic.heartbeat = Some(serve_classic_heartbeat(
            hosted_classic
                .state
                .as_mut()
                .expect("consumer group prepared"),
            &request,
        ));
    }
    #[then("the hosted classic heartbeat returns Kafka error code {code}")]
    fn hosted_heartbeat_result(hosted_classic: &HostedClassic, code: i16) {
        check!(hosted_classic.heartbeat == Some(code));
    }
    use crate::coordinator::unified::test_support::wire_bytes;

    #[derive(Deserialize)]
    struct HostedProtocolInput {
        name: String,
        metadata: String,
    }
    #[derive(Deserialize)]
    struct HostedFacadeInput {
        session_ms: u64,
        protocols: Vec<HostedProtocolInput>,
    }
    #[derive(Deserialize)]
    struct HostedMemberInput {
        member: String,
        instance: Option<String>,
        rack: Option<String>,
        client: String,
        host: String,
        subscriptions: Vec<String>,
        regex: Option<String>,
        assignor: Option<String>,
        rebalance_ms: u64,
        epoch: i32,
        previous_epoch: i32,
        state: String,
        assigned: HashMap<String, Vec<i32>>,
        revoking: HashMap<String, Vec<i32>>,
        assignment_epochs: HashMap<String, HashMap<i32, i32>>,
        classic: Option<HostedFacadeInput>,
    }
    #[derive(Deserialize)]
    struct HostedSyncRequest {
        member: String,
        instance: Option<String>,
        generation: i32,
        protocol_type: Option<String>,
        protocol_name: Option<String>,
    }
    fn assigned_topic_ranks(topics: HashMap<String, Vec<i32>>) -> HashMap<Uuid, Vec<i32>> {
        topics
            .into_iter()
            .map(|(topic, partitions)| {
                (
                    Uuid([topic.parse::<u8>().expect("topic rank"); 16]),
                    partitions,
                )
            })
            .collect()
    }
    #[given(
        "consumer group g at target epoch 5 has topic t with two partitions and these complete members"
    )]
    fn hosted_member_data(hosted_classic: &mut HostedClassic, docstring: String) {
        let members = serde_json::from_str::<Vec<HostedMemberInput>>(&docstring)
            .expect("complete consumer group members")
            .into_iter()
            .map(|input| MemberState {
                member_id: input.member,
                instance_id: input.instance,
                rack_id: input.rack,
                client_id: input.client,
                client_host: input.host,
                subscribed_topic_names: input.subscriptions.into_iter().collect(),
                subscribed_topic_regex: input.regex,
                server_assignor: input.assignor,
                rebalance_timeout: Duration::from_millis(input.rebalance_ms),
                member_epoch: input.epoch,
                previous_member_epoch: input.previous_epoch,
                assignment_state: match input.state.as_str() {
                    "Stable" => MemberAssignmentState::Stable,
                    "UnrevokedPartitions" => MemberAssignmentState::UnrevokedPartitions,
                    "UnreleasedPartitions" => MemberAssignmentState::UnreleasedPartitions,
                    _ => panic!("unknown consumer member state"),
                },
                assigned_partitions: assigned_topic_ranks(input.assigned),
                partitions_pending_revocation: assigned_topic_ranks(input.revoking),
                assignment_epochs: input
                    .assignment_epochs
                    .into_iter()
                    .map(|(topic, epochs)| {
                        (Uuid([topic.parse::<u8>().expect("topic rank"); 16]), epochs)
                    })
                    .collect(),
                last_seen: Instant::now(),
                classic: input.classic.map(|classic| ClassicMemberFacade {
                    session_timeout: Duration::from_millis(classic.session_ms),
                    supported_protocols: classic
                        .protocols
                        .into_iter()
                        .map(|protocol| {
                            (
                                protocol.name,
                                Bytes::from(wire_bytes(&[&protocol.metadata])),
                            )
                        })
                        .collect(),
                }),
            })
            .collect();
        hosted_classic.state = Some(group(members));
    }
    #[given("the hosted classic SyncGroup request has these fields")]
    fn hosted_sync_request_data(hosted_classic: &mut HostedClassic, docstring: String) {
        let input = serde_json::from_str::<HostedSyncRequest>(&docstring)
            .expect("complete SyncGroup request identity");
        hosted_classic.request = Some(SyncGroupRequest {
            group_id: "g".into(),
            member_id: input.member,
            group_instance_id: input.instance,
            generation_id: input.generation,
            protocol_type: input.protocol_type,
            protocol_name: input.protocol_name,
            ..Default::default()
        });
    }
    #[then("the complete hosted classic SyncGroup response is")]
    fn hosted_sync_expected_data(hosted_classic: &HostedClassic, docstring: String) {
        let expected = serde_json::from_str::<Value>(&docstring)
            .expect("complete expected SyncGroup response");
        let response = hosted_classic.sync.as_ref().expect("SyncGroup completed");
        let assignment = hex::encode(&response.assignment);
        check!(
            json!({"error":response.error_code,"assignment":assignment,"protocol_type":response.protocol_type,"protocol_name":response.protocol_name})
                == expected
        );
    }
    scenarios!("tests/features/migrated/src/coordinator/unified/migration/hosted_classic.feature",fixtures=[hosted_classic:HostedClassic]);
}
