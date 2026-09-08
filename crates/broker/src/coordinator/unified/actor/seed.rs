//! Hydration of a next-gen consumer group from the records replayed off
//! `__consumer_offsets`.
//!
//! A coordinator failover rebuilds a group from its persisted k3/k5/k7/k8
//! records, and this is where that seed becomes live [`GroupState`]: members,
//! their epochs, their target and current assignments, and the
//! `ClassicMemberFacade` of any classic member the group hosts after a KIP-848
//! upgrade.
//!
//! A hosted classic member's `last_synced_assignment` is not a persisted field.
//! Nothing in Kafka's schema holds it, so it is rebuilt here from the member's
//! k8 current assignment, which is what `SyncGroup` handed the member, in the
//! same translation the live path uses. A member whose current assignment no
//! longer matches its k7 target therefore still owes a re-sync after the
//! failover, exactly as it did before it.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use bytes::Bytes;
use krabka_protocol::primitives::uuid::Uuid;

use super::{FALLBACK_REBALANCE_TIMEOUT_MS, FALLBACK_SESSION_TIMEOUT_MS};
use crate::coordinator::unified::{
    GroupSeed,
    consumer_state::{ClassicMemberFacade, GroupState, MemberState},
    migration::target_to_consumer_assignment,
    persistence_next_gen::{AssignedTopicPartitions, MemberAssignmentState},
    reconciler::ReconcileInput,
};

/// Rebuilds every hosted classic member's `last_synced_assignment` from the
/// records Kafka's own schema defines.
///
/// `SyncGroup` hands a hosted classic member the blob for its target and
/// records that target as the member's current assignment, so the member's k8
/// record IS what it last synced. Translating it back with the metadata image
/// reproduces the blob byte for byte, which is what
/// `migration::serve_classic_heartbeat` compares against the member's current
/// target. `awaiting_sync` follows the same comparison.
fn restore_classic_sync_state(state: &mut GroupState, image: &ReconcileInput) {
    let restored: Vec<(String, Bytes, bool)> = state
        .members
        .iter()
        .filter(|(_, member)| member.is_classic())
        .map(|(member_id, member)| {
            let synced = target_to_consumer_assignment(&member.assigned_partitions, image);
            let target = state
                .target
                .per_member
                .get(member_id)
                .cloned()
                .unwrap_or_default();
            let owes_sync = synced != target_to_consumer_assignment(&target, image);
            (member_id.clone(), synced, owes_sync)
        })
        .collect();
    for (member_id, synced, owes_sync) in restored {
        if let Some(facade) = state
            .members
            .get_mut(&member_id)
            .and_then(|member| member.classic.as_mut())
        {
            facade.last_synced_assignment = synced;
            facade.awaiting_sync = owes_sync;
        }
    }
}

fn topic_partition_map(partitions: Vec<AssignedTopicPartitions>) -> HashMap<Uuid, Vec<i32>> {
    partitions
        .into_iter()
        .map(|tp| (tp.topic_id, tp.partitions))
        .collect()
}

pub(super) fn apply_seed(state: &mut GroupState, seed: GroupSeed, image: &ReconcileInput) {
    state.group_epoch = seed.group_epoch;
    state.target.epoch = seed.target_epoch;
    let group_generation = seed.group_epoch;
    for (mid, meta) in seed.members {
        let mut sub = std::collections::HashSet::new();
        for n in meta.subscribed_topic_names {
            sub.insert(n);
        }
        // KIP-848 migration: a k5 record carrying a `classic` block describes a
        // classic-protocol member hosted in an upgraded group. Rebuild its
        // `ClassicMemberFacade` so the member keeps speaking
        // `JoinGroup`/`SyncGroup`/`Heartbeat` after a coordinator failover; a
        // native consumer-protocol member has `classic == None`.
        let classic = meta.classic.as_ref().map(|c| ClassicMemberFacade {
            generation_id: group_generation,
            supported_protocols: c.supported_protocols.clone(),
            session_timeout: Duration::from_millis(
                u64::try_from(c.session_timeout_ms.max(0)).unwrap_or(FALLBACK_SESSION_TIMEOUT_MS),
            ),
            // Both fields are rebuilt from the member's k7 target and k8
            // current assignment once those are in place, below.
            last_synced_assignment: Bytes::new(),
            awaiting_sync: true,
        });
        state.add_or_update_member(MemberState {
            member_id: mid.clone(),
            instance_id: meta.instance_id,
            rack_id: meta.rack_id,
            client_id: meta.client_id,
            client_host: meta.client_host,
            subscribed_topic_names: sub,
            subscribed_topic_regex: meta.subscribed_topic_regex,
            compiled_regex: crate::coordinator::unified::consumer_state::CompiledRegex::Absent,
            server_assignor: meta.server_assignor,
            rebalance_timeout: Duration::from_millis(
                u64::try_from(meta.rebalance_timeout_ms.max(0))
                    .unwrap_or(FALLBACK_REBALANCE_TIMEOUT_MS),
            ),
            member_epoch: 0,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            last_seen: Instant::now(),
            classic,
        });
    }
    for (mid, cur) in seed.current_per_member {
        if let Some(m) = state.members.get_mut(&mid) {
            m.member_epoch = cur.member_epoch;
            m.previous_member_epoch = cur.previous_member_epoch;
            m.assignment_state = cur.state;
            for tp in cur.assigned_partitions {
                m.assigned_partitions.insert(tp.topic_id, tp.partitions);
            }
            for tp in cur.partitions_pending_revocation {
                m.partitions_pending_revocation
                    .insert(tp.topic_id, tp.partitions);
            }
        }
    }
    // The k7 target the group last installed. Without it the group would come
    // back with `target.epoch` set but no per-member target at all, so the
    // first RPC after the failover would hand a member an empty assignment.
    for (mid, target) in seed.target_per_member {
        state
            .target
            .per_member
            .insert(mid, topic_partition_map(target.topic_partitions));
    }
    restore_classic_sync_state(state, image);
    state.dirty = false;
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::{
        codes,
        coordinator::unified::{
            migration::serve_classic_heartbeat,
            persistence_next_gen::{
                ClassicMemberMetadata, CurrentMemberAssignmentValue, MemberMetadataValue,
                TargetAssignmentMemberValue,
            },
        },
    };

    const TOPIC: Uuid = Uuid([7; 16]);

    fn image() -> ReconcileInput {
        ReconcileInput {
            topic_id_by_name: [("t".to_string(), TOPIC)].into(),
            partitions_per_topic: [(TOPIC, 2)].into(),
            ..ReconcileInput::default()
        }
    }

    /// The records a coordinator failover replays for a group that hosts one
    /// classic member: a k5 with the classic sub-state, a k7 target of both
    /// partitions of `t`, and a k8 current assignment of `assigned`.
    fn hosted_classic_seed(assigned: Vec<i32>) -> GroupSeed {
        GroupSeed {
            group_epoch: 5,
            target_epoch: 5,
            members: [(
                "m".to_string(),
                MemberMetadataValue {
                    instance_id: None,
                    rack_id: None,
                    client_id: "c".to_string(),
                    client_host: "/127.0.0.1".to_string(),
                    subscribed_topic_names: vec!["t".to_string()],
                    subscribed_topic_regex: None,
                    server_assignor: None,
                    rebalance_timeout_ms: 60_000,
                    classic: Some(ClassicMemberMetadata {
                        session_timeout_ms: 30_000,
                        supported_protocols: vec![(
                            "range".to_string(),
                            Bytes::from_static(b"meta"),
                        )],
                    }),
                },
            )]
            .into(),
            target_per_member: [(
                "m".to_string(),
                TargetAssignmentMemberValue {
                    topic_partitions: vec![AssignedTopicPartitions {
                        topic_id: TOPIC,
                        partitions: vec![0, 1],
                    }],
                },
            )]
            .into(),
            current_per_member: [(
                "m".to_string(),
                CurrentMemberAssignmentValue {
                    member_epoch: 5,
                    previous_member_epoch: 4,
                    state: MemberAssignmentState::Stable,
                    assigned_partitions: vec![AssignedTopicPartitions {
                        topic_id: TOPIC,
                        partitions: assigned,
                    }],
                    partitions_pending_revocation: vec![],
                },
            )]
            .into(),
        }
    }

    #[test]
    fn seed_restores_the_per_member_target() {
        let mut state = GroupState::new("g");
        apply_seed(&mut state, hosted_classic_seed(vec![0, 1]), &image());

        let restored: HashMap<Uuid, Vec<i32>> = [(TOPIC, vec![0, 1])].into();
        check!(state.target.epoch == 5);
        check!(state.target.per_member.get("m") == Some(&restored));
    }

    /// The behaviour the deleted krabka-private tag existed for: a hosted
    /// classic member that had synced its target keeps a quiet heartbeat
    /// across a coordinator failover, and one whose target moved on is still
    /// told to rejoin and re-sync. Both answers are rebuilt from the k7 and k8
    /// records alone.
    #[test]
    fn seeded_hosted_classic_heartbeat_signals_a_resync_only_when_the_assignment_lags() {
        for (assigned, want) in [
            (vec![0, 1], codes::NONE),
            (vec![0], codes::REBALANCE_IN_PROGRESS),
            (vec![], codes::REBALANCE_IN_PROGRESS),
        ] {
            let mut state = GroupState::new("g");
            apply_seed(&mut state, hosted_classic_seed(assigned.clone()), &image());
            let got = serve_classic_heartbeat(&mut state, "m", &image());
            assert!(got == want, "assigned = {assigned:?}");
        }
    }
}
