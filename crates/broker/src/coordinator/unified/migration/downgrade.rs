//! The next-gen-to-classic direction of a KIP-848 live migration.
//!
//! It re-expresses a consumer group's hosted classic members as classic
//! members, seeding each one from the server-computed target so no partition
//! looks revoked across the flip, and builds the atomic record batch that
//! tombstones the next-gen records and writes the classic one.

use krabka_verified::{
    GroupMigrationDirection, GroupMigrationRecordAction, consumer_downgrade_epoch,
    group_migration_record_plan,
};

use super::assignment::member_target_assignment;
use crate::coordinator::unified::{
    actor::{PendingRecords, classic_group_metadata_record},
    classic_state::{ClassicGroup as ClassicState, Member as ClassicMember, select_protocol},
    consumer_state::GroupState as ConsumerState,
    reconciler::ReconcileInput,
};

/// Can this consumer group be downgraded to a classic group?
///
/// Every remaining member must carry a classic facade. A native consumer
/// member has no classic protocol list or session timeout to restore and makes
/// the current group unrepresentable as classic state.
pub(crate) fn consumer_is_convertible(state: &ConsumerState) -> bool {
    consumer_downgrade_epoch(
        state
            .members
            .values()
            .all(|member| member.classic.is_some()),
        state.group_epoch,
    )
    .is_some()
}

/// Converts a consumer group back into a classic group during a KIP-848
/// downgrade.
///
/// Every member becomes a classic [`ClassicMember`] again, restored from its
/// [`ClassicMemberFacade`]. Its assignment seed is the server-computed target,
/// translated to a `ConsumerProtocolAssignment` blob, so the member keeps its
/// partitions across the flip with no false revoke. Committed offsets live on
/// the kind-agnostic `Group` container, and this function does not change
/// them.
///
/// Precondition: every member is a hosted classic member, that is
/// `classic.is_some()`. That holds once the last native consumer-protocol
/// member has left.
pub(crate) fn convert_consumer_to_classic(
    state: &ConsumerState,
    image: &ReconcileInput,
) -> ClassicState {
    let mut classic = ClassicState::new(state.group_id.clone());
    classic.protocol_type = Some("consumer".into());
    for (mid, m) in &state.members {
        let facade = m
            .classic
            .as_ref()
            .expect("downgrade precondition: all members are hosted classic members");
        // Seed from the server-computed TARGET, not `assigned_partitions`: a
        // hosted classic member's `assigned_partitions` only fills in as a
        // NATIVE consumer acks epochs over heartbeats, which a hosted classic
        // member never does. Its real partitions live in `target.per_member`,
        // so reading the target keeps them across the downgrade.
        let seed = member_target_assignment(state, mid, image);
        let mut cm = ClassicMember::new(
            mid.clone(),
            m.client_id.clone(),
            m.client_host.clone(),
            facade.session_timeout,
            m.rebalance_timeout,
            facade.supported_protocols.clone(),
        )
        .with_instance_id(m.instance_id.clone());
        cm.assignment = Some(seed);
        classic.add_member(cm);
    }
    if let Some(name) = select_protocol(&classic.members) {
        classic.complete_rebalance(&name);
        // Drive to Stable so a downgraded member's first Heartbeat/SyncGroup
        // reads its seed assignment instead of REBALANCE_IN_PROGRESS.
        let assignments: std::collections::HashMap<String, bytes::Bytes> = classic
            .members
            .iter()
            .filter_map(|(id, m)| m.assignment.clone().map(|a| (id.clone(), a)))
            .collect();
        classic.install_assignments(assignments);
    }
    // Set generation_id LAST so neither complete_rebalance (+1) nor
    // install_assignments overrides the consumer group's epoch.
    classic.generation_id = consumer_downgrade_epoch(
        state
            .members
            .values()
            .all(|member| member.classic.is_some()),
        state.group_epoch,
    )
    .expect("downgrade precondition: every member has a classic facade");
    classic
}

/// The atomic record batch for a downgrade.
///
/// It tombstones the consumer group's group-level next-gen k3 `GroupMetadata`
/// and k6 `TargetAssignmentMetadata`, and every member's k5, k7, and k8. It
/// then writes the classic k2 `GroupMetadata` for the re-expressed classic
/// group. All of that is in one batch, so the flip is all-or-nothing, and
/// bootstrap replay sees a clean next-gen drop followed by a classic write.
/// Log order decides.
///
/// The k6 tombstone matters under log compaction. `__consumer_offsets` is
/// compacted. Without the tombstone, a surviving post-upgrade k6 write, which
/// is group-level and never tombstoned per member, would outlive the collected
/// k3. It would then re-create a next-gen seed on replay and bring the
/// downgraded group back as next-gen.
pub(crate) fn downgrade_pending_records(
    consumer: &ConsumerState,
    classic: &ClassicState,
) -> PendingRecords {
    let plan =
        group_migration_record_plan(GroupMigrationDirection::Downgrade, consumer.members.len());
    let mut pending = PendingRecords {
        next_gen_group_metadata_tombstone: plan.next_gen_group
            == GroupMigrationRecordAction::Tombstone,
        next_gen_target_metadata_tombstone: plan.next_gen_target
            == GroupMigrationRecordAction::Tombstone,
        classic_group_metadata: (plan.classic_group == GroupMigrationRecordAction::Write)
            .then(|| classic_group_metadata_record(classic)),
        ..Default::default()
    };
    if plan.member_metadata == GroupMigrationRecordAction::Tombstone {
        for mid in consumer.members.keys() {
            pending.member_metadata.push((mid.clone(), None));
            pending.target_per_member.push((mid.clone(), None));
            pending.current_per_member.push((mid.clone(), None));
        }
    }
    assert2::debug_assert!(pending.member_metadata.len() == plan.member_count);
    assert2::debug_assert!(pending.target_per_member.len() == plan.member_count);
    assert2::debug_assert!(pending.current_per_member.len() == plan.member_count);
    pending
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::Buf;
    use krabka_protocol::{
        Decode, owned::consumer_protocol_assignment::ConsumerProtocolAssignment,
        primitives::uuid::Uuid,
    };

    use super::*;

    #[test]
    fn downgrade_requires_every_member_to_have_a_classic_facade() {
        use std::{
            collections::{HashMap, HashSet},
            time::{Duration, Instant},
        };

        use crate::coordinator::unified::{
            consumer_state::{CompiledRegex, MemberState},
            persistence_next_gen::MemberAssignmentState,
        };

        let mut state = ConsumerState::new("g");
        assert!(consumer_is_convertible(&state));
        state.add_or_update_member(MemberState {
            member_id: "native".into(),
            instance_id: None,
            rack_id: None,
            client_id: "c".into(),
            client_host: "h".into(),
            subscribed_topic_names: HashSet::default(),
            subscribed_topic_regex: None,
            compiled_regex: CompiledRegex::Absent,
            server_assignor: None,
            rebalance_timeout: Duration::from_secs(30),
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            last_seen: Instant::now(),
            classic: None,
        });
        assert!(!consumer_is_convertible(&state));
    }

    #[test]
    fn downgrade_re_expresses_members_as_classic() {
        use std::time::{Duration, Instant};

        use crate::coordinator::unified::{
            classic_state::GroupState as ClassicGroupState,
            consumer_state::{ClassicMemberFacade, GroupState, MemberState},
            persistence_next_gen::MemberAssignmentState,
        };

        let t1 = Uuid([1; 16]);
        let image = ReconcileInput {
            topic_id_by_name: [("orders".to_string(), t1)].into(),
            ..Default::default()
        };
        let mut state = GroupState::new("g");
        state.group_epoch = 7;
        let m = MemberState {
            member_id: "m1".into(),
            instance_id: Some("inst-a".into()),
            rack_id: None,
            client_id: "c".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: ["orders".to_string()].into(),
            subscribed_topic_regex: None,
            compiled_regex: crate::coordinator::unified::consumer_state::CompiledRegex::Absent,
            server_assignor: None,
            rebalance_timeout: Duration::from_mins(1),
            member_epoch: 7,
            previous_member_epoch: 6,
            assignment_state: MemberAssignmentState::Stable,
            // A hosted classic member never acks epochs, so its
            // `assigned_partitions` stays EMPTY — its real partitions live in
            // the group's target (set below). The downgrade must seed from the
            // target, not from this empty map.
            assigned_partitions: std::collections::HashMap::new(),
            partitions_pending_revocation: std::collections::HashMap::new(),
            last_seen: Instant::now(),
            classic: Some(ClassicMemberFacade {
                generation_id: 7,
                supported_protocols: vec![("range".into(), bytes::Bytes::from_static(b"meta"))],
                session_timeout: Duration::from_secs(30),
                last_synced_assignment: bytes::Bytes::new(),
                awaiting_sync: false,
            }),
        };
        state.add_or_update_member(m);
        // The member's real partitions are in the server target, not in
        // `assigned_partitions`.
        state.target.epoch = 7;
        state
            .target
            .per_member
            .insert("m1".into(), [(t1, vec![0, 1])].into());

        let classic = convert_consumer_to_classic(&state, &image);
        assert!(classic.group_id == "g");
        assert!(classic.generation_id == 7);
        let member = classic.members.get("m1").expect("member preserved");
        assert!(member.group_instance_id.as_deref() == Some("inst-a"));
        assert!(member.session_timeout == Duration::from_secs(30));
        let asn = member.assignment.clone().expect("seed assignment");
        let mut cur = &asn[..];
        let version = cur.get_i16();
        assert!(version == 0);
        let decoded = ConsumerProtocolAssignment::decode(&mut cur, 0).unwrap();
        check!(decoded.assigned_partitions[0].topic == "orders");
        check!(decoded.assigned_partitions[0].partitions == vec![0, 1]);
        // Group must land in Stable so the first Heartbeat/SyncGroup after
        // downgrade does not trigger a spurious full rebalance.
        check!(classic.state == ClassicGroupState::Stable);
        // Seed assignment is still intact after stabilization.
        let asn2 = member
            .assignment
            .clone()
            .expect("seed assignment still set after stabilize");
        check!(asn2 == asn);
        // complete_rebalance must have set the protocol metadata coherently.
        check!(classic.protocol_name.as_deref() == Some("range"));
        check!(classic.leader_id.as_deref() == Some("m1"));

        let first = downgrade_pending_records(&state, &classic).to_batch("g", 7);
        let second = downgrade_pending_records(&state, &classic).to_batch("g", 7);
        check!(first.records.len() == 6);
        assert!(first.records == second.records);
    }
}
