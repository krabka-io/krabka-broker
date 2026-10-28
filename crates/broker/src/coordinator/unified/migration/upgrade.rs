//! The classic-to-next-gen direction of a KIP-848 live migration.
//!
//! It holds the subscription decoder, the admission predicate a classic group
//! must pass, the state translation that re-expresses its members as hosted
//! classic members of a consumer group, and the atomic record batch that makes
//! the flip durable.

use {
    super::assignment::embedded_protocol_version,
    crate::coordinator::unified::{
        actor::{PendingRecords, full_pending_records},
        classic_state::ClassicGroup as ClassicState,
        consumer_state::{ClassicMemberFacade, GroupState as ConsumerState, MemberState},
        persistence_next_gen::MemberAssignmentState,
        reconciler::{self, ReconcileInput},
    },
    krabka_protocol::{
        Decode,
        owned::{
            consumer_protocol_assignment::{
                ConsumerProtocolAssignment, MAX_VERSION as ASSIGNMENT_MAX_VERSION,
            },
            consumer_protocol_subscription::{
                ConsumerProtocolSubscription, MAX_VERSION as SUBSCRIPTION_MAX_VERSION,
            },
        },
        primitives::uuid::Uuid,
    },
    krabka_verified::{
        GroupMigrationDirection, GroupMigrationRecordAction, classic_upgrade_epoch,
        group_migration_record_plan,
    },
    std::{collections::HashMap, time::Instant},
};

/// Decodes a classic member's `protocol_metadata` blob as a
/// `ConsumerProtocolSubscription`, as Kafka's
/// `ConsumerProtocol.deserializeConsumerProtocolSubscription` does.
///
/// The blob carries a leading `i16` version, then the schema body. That
/// version is the "consumer" embedded-protocol version negotiation, which is
/// separate from the per-field version gates in the
/// `ConsumerProtocolSubscription` schema. A version above the highest the
/// schema knows is read as the highest (`checkSubscriptionVersion`).
///
/// This function returns `None` where Kafka throws a `SchemaException`: a blob
/// too short to hold a version, a negative version, or a body that does not
/// decode.
pub fn decode_consumer_subscription(metadata: &[u8]) -> Option<ConsumerProtocolSubscription> {
    let version = embedded_protocol_version(metadata)?;
    if version < 0 {
        return None;
    }
    let mut cur = metadata.get(2..)?;
    ConsumerProtocolSubscription::decode(&mut cur, version.min(SUBSCRIPTION_MAX_VERSION)).ok()
}

/// Kafka's `GroupMetadataManager.validateOnlineUpgrade` for a non-empty
/// classic group that a `ConsumerGroupHeartbeat` joins.
///
/// An empty classic group never reaches this: Kafka's
/// `maybeDeleteEmptyClassicGroup` replaces it whatever the policy and the
/// protocol type. The error is the `GROUP_ID_NOT_FOUND` message Kafka sends.
pub fn validate_online_upgrade(
    state: &ClassicState,
    upgrade_enabled: bool,
    consumer_max_size: usize,
) -> Result<(), String> {
    let group_id = &state.group_id;
    if !upgrade_enabled {
        return Err(format!(
            "Cannot upgrade classic group {group_id} to consumer group because online upgrade is \
             disabled."
        ));
    }
    if state.protocol_type.as_deref() != Some("consumer") {
        return Err(format!(
            "Cannot upgrade classic group {group_id} to consumer group because the group does \
             not use the consumer embedded protocol."
        ));
    }
    if state.members.len() > consumer_max_size {
        return Err(format!(
            "Cannot upgrade classic group {group_id} to consumer group because the group size \
             exceeds the consumer group maximum size."
        ));
    }
    Ok(())
}

/// Why `ConsumerGroup.fromClassicGroup` cannot carry a classic member over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UnconvertibleMember {
    /// The member's subscription or assignment does not decode: Kafka's
    /// `SchemaException`.
    Malformed,
    /// The member's assignment carries user data from a custom assignor,
    /// which a consumer-protocol member cannot keep: Kafka's
    /// `UnsupportedVersionException`.
    CustomAssignorUserData,
}

/// Kafka's `toTopicPartitionMap` of a classic member's
/// `ConsumerProtocolAssignment`, with the refusals of
/// `ConsumerGroup.fromClassicGroup`: the partitions of each topic that `image`
/// knows, by topic ID. An absent or empty assignment, which a member that
/// never synced holds, assigns nothing.
fn decode_consumer_assignment(
    blob: Option<&[u8]>,
    image: &ReconcileInput,
) -> Result<HashMap<Uuid, Vec<i32>>, UnconvertibleMember> {
    let mut assigned: HashMap<Uuid, Vec<i32>> = HashMap::new();
    let Some(blob) = blob.filter(|blob| !blob.is_empty()) else {
        return Ok(assigned);
    };
    let version = embedded_protocol_version(blob)
        .filter(|version| *version >= 0)
        .ok_or(UnconvertibleMember::Malformed)?;
    let mut cur = blob.get(2..).ok_or(UnconvertibleMember::Malformed)?;
    let assignment =
        ConsumerProtocolAssignment::decode(&mut cur, version.min(ASSIGNMENT_MAX_VERSION))
            .map_err(|_| UnconvertibleMember::Malformed)?;
    if assignment
        .user_data
        .as_ref()
        .is_some_and(|user_data| !user_data.is_empty())
    {
        return Err(UnconvertibleMember::CustomAssignorUserData);
    }
    for topic in assignment.assigned_partitions {
        if let Some(topic_id) = image.topic_id_by_name.get(&topic.topic) {
            let partitions = assigned.entry(*topic_id).or_default();
            partitions.extend(topic.partitions);
            partitions.sort_unstable();
            partitions.dedup();
        }
    }
    Ok(assigned)
}

/// Converts a classic group into a consumer group that **hosts its classic
/// members** during a KIP-848 upgrade, as Kafka's
/// `ConsumerGroup.fromClassicGroup` does.
///
/// The group epoch and the target assignment epoch are the classic
/// generation, with an unknown assignment time (0). Each classic member
/// becomes a stable [`MemberState`] at the generation, previous epoch
/// included, with a [`ClassicMemberFacade`], the topic names and rack of its
/// `ConsumerProtocolSubscription`, and its last assignment, read from its
/// `ConsumerProtocolAssignment` against `image`, as both its assignment (at
/// the generation) and its target. The group records the metadata hash of its
/// subscribed topics.
///
/// # Errors
///
/// The `GROUP_ID_NOT_FOUND` message of Kafka's `convertToConsumerGroup` when
/// a member's assignment or subscription does not decode, or its assignment
/// carries user data from a custom assignor. Members are read in member-id
/// order, each assignment before its subscription, as Kafka reads them.
///
/// The caller has run [`validate_online_upgrade`]. Committed offsets live on
/// the kind-agnostic `Group` container, and this function does not change
/// them.
pub fn convert_classic_to_consumer(
    classic: &ClassicState,
    image: &ReconcileInput,
) -> Result<ConsumerState, String> {
    let group_id = &classic.group_id;
    let mut members: Vec<_> = classic.members.values().collect();
    members.sort_unstable_by(|a, b| a.id.cmp(&b.id));
    let mut decoded = Vec::with_capacity(members.len());
    for m in members {
        let converted =
            decode_consumer_assignment(m.assignment.as_deref(), image).and_then(|assigned| {
                decode_consumer_subscription(&m.protocol_metadata)
                    .map(|subscription| (m, subscription, assigned))
                    .ok_or(UnconvertibleMember::Malformed)
            });
        match converted {
            Ok(member) => decoded.push(member),
            Err(UnconvertibleMember::Malformed) => {
                return Err(format!(
                    "Cannot upgrade classic group {group_id} to consumer group because the \
                     embedded consumer protocol is malformed."
                ));
            }
            Err(UnconvertibleMember::CustomAssignorUserData) => {
                return Err(format!(
                    "Cannot upgrade classic group {group_id} to consumer group because an \
                     unsupported custom assignor is in use. Please refer to the documentation \
                     or switch to a default assignor before re-attempting the upgrade."
                ));
            }
        }
    }
    let mut state = ConsumerState::new(group_id.clone());
    let generation = classic_upgrade_epoch(
        classic.protocol_type.as_deref() == Some("consumer"),
        true,
        classic.generation_id,
    )
    .ok_or_else(|| {
        format!(
            "Cannot upgrade classic group {group_id} to consumer group because the group does \
             not use the consumer embedded protocol."
        )
    })?;
    state.group_epoch = generation;
    state.target.epoch = generation;
    for (m, subscription, assigned) in decoded {
        let facade = ClassicMemberFacade {
            supported_protocols: m.protocols.clone(),
            session_timeout: m.session_timeout,
        };
        let assignment_epochs = assigned
            .iter()
            .map(|(topic_id, partitions)| {
                (
                    *topic_id,
                    partitions.iter().map(|p| (*p, generation)).collect(),
                )
            })
            .collect();
        state
            .target
            .per_member
            .insert(m.id.clone(), assigned.clone());
        state.add_or_update_member(MemberState {
            member_id: m.id.clone(),
            instance_id: m.group_instance_id.clone(),
            rack_id: subscription.rack_id.filter(|rack| !rack.is_empty()),
            client_id: m.client_id.clone(),
            client_host: m.host.clone(),
            subscribed_topic_names: subscription.topics.into_iter().collect(),
            subscribed_topic_regex: None,
            server_assignor: None,
            rebalance_timeout: m.rebalance_timeout,
            member_epoch: generation,
            previous_member_epoch: generation,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: assigned,
            partitions_pending_revocation: HashMap::new(),
            assignment_epochs,
            last_seen: Instant::now(),
            classic: Some(facade),
        });
    }
    let hash = reconciler::metadata_hash(&state, image);
    state.record_metadata_hash(hash);
    // A converted group's metadata is fresh, as Kafka's `fromClassicGroup`
    // computes the hash from the current image; the heartbeat that converts
    // it refreshes nothing more.
    Ok(state)
}

/// The atomic record batch for an upgrade. It tombstones the classic k2
/// `GroupMetadata` and writes the full next-gen record set for the converted
/// group. Both go into one batch, so the flip is all-or-nothing.
pub fn upgrade_pending_records(state: &ConsumerState) -> PendingRecords {
    let plan = group_migration_record_plan(GroupMigrationDirection::Upgrade, state.members.len());
    let mut pending = full_pending_records(state);
    pending.classic_group_metadata_tombstone =
        plan.classic_group == GroupMigrationRecordAction::Tombstone;
    assert2::debug_assert!(pending.group_metadata.is_some());
    assert2::debug_assert!(pending.target_metadata.is_some());
    pending.assert_member_record_count(plan.member_count);
    pending
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            actor::test_support::subscription_blob,
            classic_state::{ClassicGroup, Member},
        },
        assert2::{assert, check},
        bytes::{BufMut, Bytes, BytesMut},
        krabka_protocol::{Encode, owned::consumer_protocol_assignment::TopicPartition},
        std::time::Duration,
    };

    fn consumer_member(id: &str, metadata: Bytes) -> Member {
        let mut m = Member::new(
            id,
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), metadata.clone())],
        );
        m.protocol_metadata = metadata;
        m
    }

    /// A conversion row: its label, each member's subscription and
    /// assignment, and the refusal it expects.
    /// A member's `ConsumerProtocolSubscription` blob and, once it has
    /// synced, its `ConsumerProtocolAssignment` blob.
    /// A subscription row: its label, the blob, and the topics it decodes to.
    /// A `ConsumerProtocolAssignment` blob of `version` for partition 0 of
    /// `t1`, with `user_data`.
    fn assignment_blob(version: i16, user_data: Option<&'static [u8]>) -> Bytes {
        let assignment = ConsumerProtocolAssignment {
            assigned_partitions: vec![TopicPartition {
                topic: "t1".into(),
                partitions: vec![0],
                ..Default::default()
            }],
            user_data: user_data.map(Bytes::from_static),
            ..Default::default()
        };
        let mut out = BytesMut::new();
        out.put_i16(version);
        assignment.encode(&mut out, version.clamp(0, 3)).unwrap();
        out.freeze()
    }

    /// Kafka's `ConsumerGroup.fromClassicGroup` refusals, through
    /// `convertToConsumerGroup`: (label, the members' subscription and
    /// assignment) to whether the group converts, or the message of the
    /// `GROUP_ID_NOT_FOUND` that refuses it.
    /// Kafka's `checkSubscriptionVersion`: a blob too short for a version and
    /// a negative version do not decode, and a version above the highest the
    /// schema knows is read as the highest.
    #[test]
    fn convert_preserves_members_subscriptions_and_facade() {
        let mut g = ClassicGroup::new("g");
        g.protocol_type = Some("consumer".into());
        g.generation_id = 3;
        let mut source_m1 = consumer_member("m1", subscription_blob(&["t1"]));
        source_m1.group_instance_id = Some("instance-1".into());
        source_m1.assignment = Some(assignment_blob(0, None));
        g.add_member(source_m1.clone());
        g.add_member(consumer_member("m2", subscription_blob(&["t1", "t2"])));

        let state = convert_classic_to_consumer(&g, &ReconcileInput::default()).unwrap();
        assert!(state.group_id == "g");
        assert!(state.group_epoch == 3); // seeded from classic generation
        assert!(state.members.len() == 2);
        let m1 = &state.members["m1"];
        assert!(m1.is_classic());
        assert!(m1.subscribed_topic_names.contains("t1"));
        assert!(m1.instance_id == source_m1.group_instance_id);
        assert!(m1.client_id == source_m1.client_id);
        assert!(m1.client_host == source_m1.host);
        assert!(m1.rebalance_timeout == source_m1.rebalance_timeout);
        check!(
            m1.classic
                == Some(ClassicMemberFacade {
                    supported_protocols: source_m1.protocols.clone(),
                    session_timeout: source_m1.session_timeout,
                })
        );
        // m2 subscribed to both topics.
        let m2 = &state.members["m2"];
        assert!(m2.subscribed_topic_names.len() == 2);
        // Kafka's `fromClassicGroup`: the target is the last assignment, at
        // the generation.
        assert!((state.target.epoch, state.target_is_stale()) == (3, false));
    }

    #[test]
    fn conversion_clamps_only_negative_epochs_and_retry_is_byte_identical() {
        let mut g = ClassicGroup::new("g");
        g.protocol_type = Some("consumer".into());
        g.generation_id = -1;
        g.add_member(consumer_member("m1", subscription_blob(&["t1"])));

        let first = convert_classic_to_consumer(&g, &ReconcileInput::default()).unwrap();
        let second = convert_classic_to_consumer(&g, &ReconcileInput::default()).unwrap();
        check!(first.group_epoch == 0);
        let first_batch = upgrade_pending_records(&first).to_batch("g", 7).unwrap();
        let second_batch = upgrade_pending_records(&second).to_batch("g", 7).unwrap();
        check!(first_batch.records.len() == 6);
        assert!(first_batch.records == second_batch.records);

        g.generation_id = i32::MAX;
        assert!(
            convert_classic_to_consumer(&g, &ReconcileInput::default())
                .unwrap()
                .group_epoch
                == i32::MAX
        );
    }

    /// Kafka's `validateOnlineUpgrade`, row by row: (label, protocol type,
    /// member count, upgrade enabled, consumer max size, expected result).
    use {
        rstest_bdd::{Slot, datatable::Rows},
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
    };
    #[derive(Default)]
    struct OnlineUpgrade {
        classic: Option<ClassicGroup>,
        result: Option<Result<(), String>>,
        subscription: Vec<u8>,
        topics: Slot<Option<Vec<String>>>,
    }
    #[derive(DataTableRow)]
    struct UpgradeMember {
        subscription: String,
        assignment: String,
        userdata: String,
    }
    #[rstest::fixture]
    fn upgrade() -> OnlineUpgrade {
        // Start this scenario with fresh inputs and observations.
        OnlineUpgrade::default()
    }
    #[given("classic consumer group g has these persisted member subscriptions and assignments")]
    fn upgrade_members(upgrade: &mut OnlineUpgrade, #[datatable] rows: Rows<UpgradeMember>) {
        let mut group = ClassicGroup::new("g");
        group.protocol_type = Some("consumer".into());
        for (index, row) in rows.into_iter().enumerate() {
            let subscription = match row.subscription.as_str() {
                "t1" => subscription_blob(&["t1"]),
                "malformed-negative" => Bytes::from_static(&[0xff, 0xff, 0x01]),
                "malformed-zero" => Bytes::from_static(&[0]),
                _ => panic!("unknown subscription fixture"),
            };
            let assignment = match row.assignment.as_str() {
                "absent" => None,
                "empty" => Some(Bytes::new()),
                "malformed" => Some(Bytes::from_static(b"garbage")),
                version => Some(assignment_blob(
                    version.parse().expect("assignment schema version"),
                    match row.userdata.as_str() {
                        "null" => None,
                        "empty" => Some(b""),
                        "sticky" => Some(b"sticky"),
                        _ => panic!("unknown assignment user data"),
                    },
                )),
            };
            let mut member = consumer_member(&format!("m{index}"), subscription);
            member.assignment = assignment;
            group.add_member(member);
        }
        upgrade.classic = Some(group);
    }
    #[when("the coordinator converts the classic group to consumer membership")]
    fn convert_upgrade(upgrade: &mut OnlineUpgrade) {
        upgrade.result = Some(
            convert_classic_to_consumer(
                upgrade.classic.as_ref().expect("classic group prepared"),
                &ReconcileInput::default(),
            )
            .map(|_| ()),
        );
    }
    #[then("online conversion is {outcome} with the exact Kafka reason")]
    fn upgrade_result(upgrade: &OnlineUpgrade, outcome: String) {
        let expected=match outcome.as_str() {
 "allowed"=>Ok(()),
 "malformed"=>Err("Cannot upgrade classic group g to consumer group because the embedded consumer protocol is malformed.".to_owned()),
 "custom-assignor"=>Err("Cannot upgrade classic group g to consumer group because an unsupported custom assignor is in use. Please refer to the documentation or switch to a default assignor before re-attempting the upgrade.".to_owned()),
 "disabled"=>Err("Cannot upgrade classic group g to consumer group because online upgrade is disabled.".to_owned()),
 "wrong-protocol"=>Err("Cannot upgrade classic group g to consumer group because the group does not use the consumer embedded protocol.".to_owned()),
 "too-large"=>Err("Cannot upgrade classic group g to consumer group because the group size exceeds the consumer group maximum size.".to_owned()),
 _=>panic!("unknown upgrade result"),
 };
        check!(upgrade.result.as_ref() == Some(&expected));
    }
    #[given("a consumer protocol subscription has encoding {encoding}")]
    fn encoded_upgrade_subscription(upgrade: &mut OnlineUpgrade, encoding: String) {
        upgrade.subscription = match encoding.as_str() {
            "empty" => vec![],
            "short" => vec![0],
            "negative-version" => vec![0xff, 0xff, 0, 0, 0, 0],
            "above-schema" => {
                let mut bytes = BytesMut::new();
                bytes.put_i16(99);
                ConsumerProtocolSubscription {
                    topics: vec!["t1".into()],
                    ..Default::default()
                }
                .encode(&mut bytes, 3)
                .expect("encode subscription at highest schema");
                bytes.to_vec()
            }
            _ => panic!("unknown consumer subscription encoding"),
        };
    }
    #[when("the coordinator decodes the embedded consumer subscription")]
    fn decode_upgrade_subscription(upgrade: &mut OnlineUpgrade) {
        upgrade.topics.set(
            decode_consumer_subscription(&upgrade.subscription)
                .map(|subscription| subscription.topics),
        );
    }
    #[then("the embedded subscription resolves to {topics}")]
    fn upgrade_subscription_topics(upgrade: &OnlineUpgrade, topics: String) {
        let expected = if topics == "undecodable" {
            None
        } else {
            Some(vec![topics])
        };
        check!(upgrade.topics.get() == Some(expected));
    }
    #[given("classic group g uses protocol {protocol} with {members} consumer members")]
    fn eligible_upgrade_group(upgrade: &mut OnlineUpgrade, protocol: String, members: usize) {
        let mut group = ClassicGroup::new("g");
        group.protocol_type = Some(protocol);
        for index in 0..members {
            group.add_member(consumer_member(
                &format!("m{index}"),
                subscription_blob(&["t"]),
            ));
        }
        upgrade.classic = Some(group);
    }
    #[when(
        "the coordinator validates online upgrade enabled {enabled} with maximum size {maximum}"
    )]
    fn validate_upgrade_policy(upgrade: &mut OnlineUpgrade, enabled: bool, maximum: usize) {
        upgrade.result = Some(validate_online_upgrade(
            upgrade.classic.as_ref().expect("classic group prepared"),
            enabled,
            maximum,
        ));
    }
    scenarios!("tests/features/migrated/src/coordinator/unified/migration/upgrade.feature",fixtures=[upgrade:OnlineUpgrade]);
}
