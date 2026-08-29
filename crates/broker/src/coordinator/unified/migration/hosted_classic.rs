//! Serving hosted classic members from the consumer-group reconciler.
//!
//! A classic member hosted in an upgraded consumer group keeps speaking the
//! classic `Heartbeat`, `JoinGroup`, and `SyncGroup` RPCs. This module maps
//! those onto the next-gen machinery: the member's server-side target,
//! translated to a `ConsumerProtocolAssignment` blob, is the assignment it
//! should hold. The "does this member owe a re-sync?" signal is whether that
//! translated target differs from `facade.last_synced_assignment`, which is
//! purely derived from the reconciler's output, so it needs no separate epoch
//! bookkeeping.

use std::{collections::HashSet, time::Instant};

use bytes::Bytes;

use super::assignment::member_target_assignment;
use crate::{
    codes,
    coordinator::unified::{
        actor::{JoinResult, JoinResultMember, SyncResult},
        consumer_state::{ClassicMemberFacade, GroupState as ConsumerState, MemberState},
        persistence_next_gen::MemberAssignmentState,
        reconciler::ReconcileInput,
    },
};

/// Classic `Heartbeat` for a hosted member. It refreshes liveness, and signals
/// a re-sync while the member's current target differs from what it last
/// synced. `REBALANCE_IN_PROGRESS` tells a classic client to send `JoinGroup`
/// and `SyncGroup` again to pick up the changed assignment. The code is `NONE`
/// once the member is in sync.
pub(crate) fn serve_classic_heartbeat(
    state: &mut ConsumerState,
    member_id: &str,
    image: &ReconcileInput,
) -> i16 {
    let Some(m) = state.members.get(member_id) else {
        return codes::UNKNOWN_MEMBER_ID;
    };
    let current = member_target_assignment(state, member_id, image);
    let owes = m
        .classic
        .as_ref()
        .is_none_or(|c| c.last_synced_assignment != current);
    if let Some(m) = state.members.get_mut(member_id) {
        m.last_seen = Instant::now();
    }
    if owes {
        codes::REBALANCE_IN_PROGRESS
    } else {
        codes::NONE
    }
}

/// Classic `SyncGroup` for a hosted member. It returns the member's current
/// target, translated to a `ConsumerProtocolAssignment` blob, and records that
/// blob as `last_synced_assignment`, so later heartbeats report `NONE`.
pub(crate) fn serve_classic_sync(
    state: &mut ConsumerState,
    member_id: &str,
    image: &ReconcileInput,
) -> SyncResult {
    if !state.members.contains_key(member_id) {
        return SyncResult {
            error_code: codes::UNKNOWN_MEMBER_ID,
            ..Default::default()
        };
    }
    let blob = member_target_assignment(state, member_id, image);
    if let Some(m) = state.members.get_mut(member_id)
        && let Some(c) = m.classic.as_mut()
    {
        c.last_synced_assignment = blob.clone();
        c.awaiting_sync = false;
    }
    SyncResult {
        error_code: codes::NONE,
        assignment: blob,
        protocol_type: Some("consumer".into()),
        protocol_name: None,
    }
}

/// Upserts a hosted classic member from a classic `JoinGroup` into the
/// consumer group.
///
/// A rejoin of an existing member refreshes its facade and its subscription,
/// and keeps its `assigned_partitions` and `last_synced_assignment`. A new
/// member arrives with a fresh facade, where `awaiting_sync = true`.
///
/// `add_or_update_member` marks the group dirty if and only if the
/// subscription is new or changed, so the caller reconciles and persists only
/// when it needs to.
pub(crate) struct ClassicMemberRegistration {
    pub member_id: String,
    pub subscription_topics: HashSet<String>,
    pub protocols: Vec<(String, Bytes)>,
    pub client_id: String,
    pub client_host: String,
    pub session_timeout: std::time::Duration,
    pub rebalance_timeout: std::time::Duration,
    pub instance_id: Option<String>,
}

pub(crate) fn upsert_classic_member(
    state: &mut ConsumerState,
    registration: ClassicMemberRegistration,
) {
    let ClassicMemberRegistration {
        member_id,
        subscription_topics,
        protocols,
        client_id,
        client_host,
        session_timeout,
        rebalance_timeout,
        instance_id,
    } = registration;
    // Preserve a rejoining member's existing assignment + last-synced blob so a
    // rejoin with an unchanged subscription is a pure no-op (no spurious revoke
    // and no re-sync signal). A new member starts fresh, awaiting its first sync.
    let existing = state.members.get(&member_id);
    let assigned_partitions = existing
        .map(|m| m.assigned_partitions.clone())
        .unwrap_or_default();
    let partitions_pending_revocation = existing
        .map(|m| m.partitions_pending_revocation.clone())
        .unwrap_or_default();
    let last_synced_assignment = existing
        .and_then(|m| m.classic.as_ref())
        .map(|c| c.last_synced_assignment.clone())
        .unwrap_or_default();
    let member_epoch = existing.map_or(state.group_epoch, |m| m.member_epoch);
    let previous_member_epoch = existing.map_or(0, |m| m.previous_member_epoch);
    let assignment_state = existing.map_or(MemberAssignmentState::Stable, |m| m.assignment_state);

    let facade = ClassicMemberFacade {
        generation_id: state.group_epoch,
        supported_protocols: protocols,
        session_timeout,
        last_synced_assignment,
        awaiting_sync: existing.is_none(),
    };
    state.add_or_update_member(MemberState {
        member_id,
        instance_id,
        rack_id: None,
        client_id,
        client_host,
        subscribed_topic_names: subscription_topics,
        subscribed_topic_regex: None,
        compiled_regex: crate::coordinator::unified::consumer_state::CompiledRegex::Absent,
        server_assignor: None,
        rebalance_timeout,
        member_epoch,
        previous_member_epoch,
        assignment_state,
        assigned_partitions,
        partitions_pending_revocation,
        last_seen: Instant::now(),
        classic: Some(facade),
    });
}

/// Builds the `JoinGroup` result for a hosted classic member. The group is
/// server-assigned, so the member is its own leader of a single-member view at
/// `generation = group_epoch`. The real assignment arrives on the next
/// `SyncGroup`.
pub(crate) fn build_hosted_classic_join_result(
    state: &ConsumerState,
    member_id: &str,
    protocol_name: Option<String>,
) -> JoinResult {
    JoinResult {
        error_code: codes::NONE,
        generation_id: state.group_epoch,
        protocol_type: Some("consumer".into()),
        protocol_name,
        leader: member_id.to_string(),
        member_id: member_id.to_string(),
        members: vec![JoinResultMember {
            member_id: member_id.to_string(),
            group_instance_id: state
                .members
                .get(member_id)
                .and_then(|m| m.instance_id.clone()),
            metadata: Bytes::new(),
        }],
    }
}
