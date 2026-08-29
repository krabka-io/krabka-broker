//! Hydration of a next-gen consumer group from the records replayed off
//! `__consumer_offsets`.
//!
//! A coordinator failover rebuilds a group from its persisted k3/k5/k7/k8
//! records, and this is where that seed becomes live [`GroupState`]: members,
//! their epochs, their current assignments, and the `ClassicMemberFacade` of
//! any classic member the group hosts after a KIP-848 upgrade.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use super::{FALLBACK_REBALANCE_TIMEOUT_MS, FALLBACK_SESSION_TIMEOUT_MS};
use crate::coordinator::unified::{
    GroupSeed,
    consumer_state::{ClassicMemberFacade, GroupState, MemberState},
    persistence_next_gen::MemberAssignmentState,
};

pub(super) fn apply_seed(state: &mut GroupState, seed: GroupSeed) {
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
            last_synced_assignment: c.last_synced_assignment.clone(),
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
    state.dirty = false;
}
