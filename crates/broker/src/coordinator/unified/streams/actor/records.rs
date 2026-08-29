//! Projection of the actor's state onto the persisted `__consumer_offsets`
//! record values, and hydration back from a seed.
//!
//! One heartbeat can change the group epoch, the topology, the target
//! assignment, and several members at once, so the actor collects the whole
//! change as a [`PendingStreamsRecords`] and writes it as one batch. The same
//! per-member projections feed the last-known-good
//! [`StreamsGroupSeed`] handed to the coordinator cache, and
//! [`apply_seed`] is their inverse for bootstrap replay and respawn.

use super::ActorState;
use crate::coordinator::unified::{
    GroupCoordinator, StreamsGroupSeed,
    offsets_log::OffsetsLog,
    streams::{
        persistence::{
            PendingStreamsRecords, StreamsEndpoint, StreamsGroupCurrentMemberAssignmentValue,
            StreamsGroupMemberMetadataValue, StreamsGroupMetadataValue,
            StreamsGroupTargetAssignmentMemberValue, StreamsGroupTargetAssignmentMetadataValue,
        },
        state::{
            StoredTopologyHandle, StreamsGroupState, StreamsGroupStatePhase,
            StreamsMemberAssignmentState, StreamsMemberState,
        },
    },
};

/// Builds a `PendingStreamsRecords` for the changes to `affected_members`.
///
/// The result always holds the current group epoch. It holds the topology and
/// the partition metadata when both are present, and the target metadata once
/// the actor has installed the target, that is, when `epoch > 0`.
pub(super) fn snapshot_pending_after_change(
    actor: &ActorState,
    affected_members: &[String],
) -> PendingStreamsRecords {
    let state = &actor.state;
    let mut pending = PendingStreamsRecords {
        group_metadata: Some(StreamsGroupMetadataValue {
            epoch: state.group_epoch,
        }),
        ..Default::default()
    };
    if let Some(topology) = &actor.topology {
        pending.topology = Some(topology.clone());
    }
    if let Some(pm) = &actor.partition_metadata {
        pending.partition_metadata = Some(pm.clone());
    }
    if state.target.epoch > 0 {
        pending.target_metadata = Some(StreamsGroupTargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
    }
    for mid in affected_members {
        if let Some(m) = state.members.get(mid) {
            pending
                .member_metadata
                .push((mid.clone(), Some(member_metadata_value(m))));
            pending
                .current_per_member
                .push((mid.clone(), Some(current_assignment_value(m))));
            if let Some(tv) = target_member_value(state, mid) {
                pending.target_per_member.push((mid.clone(), Some(tv)));
            }
        }
    }
    pending
}

fn member_metadata_value(m: &StreamsMemberState) -> StreamsGroupMemberMetadataValue {
    StreamsGroupMemberMetadataValue {
        instance_id: m.instance_id.clone(),
        rack_id: m.rack_id.clone(),
        client_id: m.client_id.clone(),
        client_host: m.client_host.clone(),
        process_id: m.process_id.clone(),
        user_endpoint: m
            .user_endpoint
            .as_ref()
            .map(|(host, port)| StreamsEndpoint {
                host: host.clone(),
                port: *port,
            }),
        client_tags: m.client_tags.clone(),
        rebalance_timeout_ms: m.rebalance_timeout_ms,
        topology_epoch: m.topology_epoch,
    }
}

fn current_assignment_value(m: &StreamsMemberState) -> StreamsGroupCurrentMemberAssignmentValue {
    StreamsGroupCurrentMemberAssignmentValue {
        member_epoch: m.member_epoch,
        previous_member_epoch: m.previous_member_epoch,
        state: m.assignment_state.as_i8(),
        active: m.active.clone(),
        standby: m.standby.clone(),
        warmup: m.warmup.clone(),
        active_pending_revocation: m.active_pending_revocation.clone(),
    }
}

fn target_member_value(
    state: &StreamsGroupState,
    member_id: &str,
) -> Option<StreamsGroupTargetAssignmentMemberValue> {
    let active = state.target.active.get(member_id).cloned();
    let standby = state.target.standby.get(member_id).cloned();
    let warmup = state.target.warmup.get(member_id).cloned();
    if active.is_none() && standby.is_none() && warmup.is_none() {
        return None;
    }
    Some(StreamsGroupTargetAssignmentMemberValue {
        active: active.unwrap_or_default(),
        standby: standby.unwrap_or_default(),
        warmup: warmup.unwrap_or_default(),
    })
}

pub(super) async fn flush_pending(
    actor: &ActorState,
    pending: PendingStreamsRecords,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
) -> Result<(), crate::error::BrokerError> {
    if pending.is_empty() {
        return Ok(());
    }
    let batch = pending.into_batch(&actor.state.group_id, now_ms);
    offsets_log.append(&actor.state.group_id, batch).await?;
    coordinator.update_streams_cache(&actor.state.group_id, snapshot_seed(actor));
    Ok(())
}

/// Snapshots the full actor state into a `StreamsGroupSeed` for the cache and
/// for a respawned actor. The result matches what bootstrap replay produces.
fn snapshot_seed(actor: &ActorState) -> StreamsGroupSeed {
    let state = &actor.state;
    let mut members = std::collections::HashMap::new();
    let mut target_per_member = std::collections::HashMap::new();
    let mut current_per_member = std::collections::HashMap::new();
    for (mid, m) in &state.members {
        members.insert(mid.clone(), member_metadata_value(m));
        current_per_member.insert(mid.clone(), current_assignment_value(m));
        if let Some(tv) = target_member_value(state, mid) {
            target_per_member.insert(mid.clone(), tv);
        }
    }
    StreamsGroupSeed {
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        topology: actor.topology.clone(),
        partition_metadata: actor.partition_metadata.clone(),
        members,
        target_per_member,
        current_per_member,
    }
}

/// Hydrates the actor from a `StreamsGroupSeed`, on bootstrap replay or on a
/// respawn.
pub(super) fn apply_seed(actor: &mut ActorState, seed: StreamsGroupSeed) {
    let state = &mut actor.state;
    state.group_epoch = seed.group_epoch;
    state.target.epoch = seed.assignment_epoch;
    state.assignment_epoch = seed.assignment_epoch;
    if let Some(topology) = &seed.topology {
        state.topology = Some(StoredTopologyHandle {
            epoch: topology.epoch,
        });
        state.topology_epoch = topology.epoch;
    }
    actor.topology = seed.topology;
    actor.partition_metadata = seed.partition_metadata;

    for (mid, meta) in seed.members {
        let mut m = StreamsMemberState::joining(mid.clone(), meta.client_id, meta.client_host);
        m.instance_id = meta.instance_id;
        m.rack_id = meta.rack_id;
        m.process_id = meta.process_id;
        m.user_endpoint = meta.user_endpoint.map(|ep| (ep.host, ep.port));
        m.client_tags = meta.client_tags;
        m.rebalance_timeout_ms = meta.rebalance_timeout_ms;
        m.topology_epoch = meta.topology_epoch;
        state.members.insert(mid, m);
    }
    for (mid, cur) in seed.current_per_member {
        if let Some(m) = state.members.get_mut(&mid) {
            m.member_epoch = cur.member_epoch;
            m.previous_member_epoch = cur.previous_member_epoch;
            m.assignment_state =
                StreamsMemberAssignmentState::from_i8(cur.state).unwrap_or_default();
            m.active = cur.active;
            m.standby = cur.standby;
            m.warmup = cur.warmup;
            m.active_pending_revocation = cur.active_pending_revocation;
        }
    }
    for (mid, tv) in seed.target_per_member {
        state.target.active.insert(mid.clone(), tv.active);
        state.target.standby.insert(mid.clone(), tv.standby);
        state.target.warmup.insert(mid, tv.warmup);
    }
    state.phase = if state.members.is_empty() {
        StreamsGroupStatePhase::Empty
    } else if actor.topology.is_some() {
        StreamsGroupStatePhase::Stable
    } else {
        StreamsGroupStatePhase::NotReady
    };
    state.dirty = false;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;

    use super::*;
    use crate::coordinator::unified::streams::persistence::StreamsGroupTopologyValue;

    #[test]
    fn seed_hydrates_state() {
        let mut actor = ActorState::new("g".into());
        let mut members = std::collections::HashMap::new();
        members.insert(
            "m1".to_string(),
            StreamsGroupMemberMetadataValue {
                instance_id: Some("i1".into()),
                rack_id: Some("r1".into()),
                client_id: "c1".into(),
                client_host: "/127.0.0.1".into(),
                process_id: "p1".into(),
                user_endpoint: Some(StreamsEndpoint {
                    host: "h".into(),
                    port: 9092,
                }),
                client_tags: vec![],
                rebalance_timeout_ms: 60_000,
                topology_epoch: 2,
            },
        );
        let mut current = std::collections::HashMap::new();
        current.insert(
            "m1".to_string(),
            StreamsGroupCurrentMemberAssignmentValue {
                member_epoch: 4,
                previous_member_epoch: 3,
                state: 0,
                active: BTreeMap::from([("0".to_string(), vec![0, 1])]),
                standby: BTreeMap::new(),
                warmup: BTreeMap::new(),
                active_pending_revocation: BTreeMap::new(),
            },
        );
        let mut target = std::collections::HashMap::new();
        target.insert(
            "m1".to_string(),
            StreamsGroupTargetAssignmentMemberValue {
                active: BTreeMap::from([("0".to_string(), vec![0, 1])]),
                standby: BTreeMap::new(),
                warmup: BTreeMap::new(),
            },
        );
        let seed = StreamsGroupSeed {
            group_epoch: 4,
            assignment_epoch: 4,
            topology: Some(StreamsGroupTopologyValue {
                epoch: 2,
                subtopologies: vec![],
            }),
            partition_metadata: None,
            members,
            target_per_member: target,
            current_per_member: current,
        };
        apply_seed(&mut actor, seed);

        check!(actor.state.group_epoch == 4);
        check!(actor.state.target.epoch == 4);
        check!(actor.state.topology_epoch == 2);
        let m = actor.state.members.get("m1").expect("member restored");
        check!(m.member_epoch == 4);
        check!(m.previous_member_epoch == 3);
        check!(m.process_id == "p1");
        check!(m.active == BTreeMap::from([("0".to_string(), vec![0, 1])]));
        check!(actor.state.target.active["m1"] == BTreeMap::from([("0".to_string(), vec![0, 1])]));
        check!(actor.state.phase == StreamsGroupStatePhase::Stable);
    }
}
