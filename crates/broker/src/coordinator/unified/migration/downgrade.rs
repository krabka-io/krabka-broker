//! The next-gen-to-classic direction of a KIP-848 live migration.
//!
//! It re-expresses a consumer group's hosted classic members as classic
//! members, seeding each one from the server-computed target so no partition
//! looks revoked across the flip, and builds the atomic record batch that
//! tombstones the next-gen records and writes the classic one.

use super::assignment::member_target_assignment;
use crate::coordinator::unified::{
    actor::{PendingRecords, classic_group_metadata_record},
    classic_state::{ClassicGroup as ClassicState, Member as ClassicMember, select_protocol},
    consumer_state::GroupState as ConsumerState,
    reconciler::ReconcileInput,
};

/// Can this consumer group be downgraded to a classic group?
///
/// The answer is always `true` in Kafka. A server-managed consumer group can
/// always be re-expressed as a classic group: members become classic members,
/// and the server target becomes the seed assignment. This function exists for
/// symmetry with the upgrade path.
pub(crate) fn consumer_is_convertible() -> bool {
    true
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
    classic.generation_id = state.group_epoch.max(0);
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
    let mut pending = PendingRecords {
        next_gen_group_metadata_tombstone: true,
        next_gen_target_metadata_tombstone: true,
        classic_group_metadata: Some(classic_group_metadata_record(classic)),
        ..Default::default()
    };
    for mid in consumer.members.keys() {
        pending.member_metadata.push((mid.clone(), None));
        pending.target_per_member.push((mid.clone(), None));
        pending.current_per_member.push((mid.clone(), None));
    }
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
    fn consumer_group_always_downgradable() {
        assert!(consumer_is_convertible());
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
    }
}
