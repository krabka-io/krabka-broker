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
            INITIAL_EPOCH, StoredTopologyHandle, StreamsGroupState, StreamsGroupStatePhase,
            StreamsMemberState,
        },
    },
};

/// Builds a `PendingStreamsRecords` for the changes to `affected_members`.
///
/// The result always holds the current group epoch. It holds the topology and
/// the partition metadata when both are present, and the target metadata once
/// the actor has installed a target, that is, when its epoch is past
/// [`INITIAL_EPOCH`]: Kafka's `TargetAssignmentBuilder` writes the record, and a
/// new group writes none while the initial rebalance delay holds its
/// assignment back. After a
/// reconcile that installed a new target, it holds the records of every
/// member, because the new target changed the assignment of all of them.
pub(super) fn snapshot_pending_after_change(
    actor: &mut ActorState,
    affected_members: &[String],
) -> PendingStreamsRecords {
    let all_members: Vec<String>;
    let affected_members = if std::mem::take(&mut actor.target_changed) {
        let mut ids: Vec<String> = actor.state.members.keys().cloned().collect();
        ids.sort_unstable();
        all_members = ids;
        all_members.as_slice()
    } else {
        affected_members
    };
    let state = &actor.state;
    let mut pending = PendingStreamsRecords {
        group_metadata: Some(StreamsGroupMetadataValue {
            epoch: state.group_epoch,
            metadata_hash: actor.metadata_hash,
            description: actor.description_epochs,
        }),
        ..Default::default()
    };
    if let Some(topology) = &actor.topology {
        pending.topology = Some(topology.clone());
    }
    if state.target.epoch > INITIAL_EPOCH {
        pending.target_metadata = Some(StreamsGroupTargetAssignmentMetadataValue {
            assignment_epoch: state.target.epoch,
        });
    }
    crate::coordinator::unified::persistence::snapshot_members!(pending, state, affected_members;
        member_metadata_value, current_assignment_value; |mid, m| {
            if let Some(tv) = target_member_value(state, mid) {
                pending.target_per_member.push((mid.clone(), Some(tv)));
            }
        }
    );
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
        state: m.assignment_state.into(),
        active: m.active.clone(),
        standby: m.standby.clone(),
        warmup: m.warmup.clone(),
        active_pending_revocation: m.active_pending_revocation.clone(),
        standby_pending_revocation: m.standby_pending_revocation.clone(),
        warmup_pending_revocation: m.warmup_pending_revocation.clone(),
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

crate::coordinator::unified::persistence::flush_pending_records! {
    actor: &ActorState, pending: PendingStreamsRecords;
    offsets_log, coordinator, now_ms;
    group &actor.state.group_id;
    encode pending.into_batch(&actor.state.group_id, now_ms);
    cache coordinator.update_streams_cache(&actor.state.group_id, snapshot_seed(actor));
}

/// Persist the reconciler's current delta after a timer or configuration change.
pub(super) async fn reconcile_and_flush(
    actor: &mut ActorState,
    config: &super::super::config::StreamsGroupConfig,
    metadata_source: Option<&std::sync::Arc<dyn crate::metadata_source::MetadataSource>>,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<(), crate::error::BrokerError> {
    super::reconciliation::reconcile(actor, config, metadata_source);
    let pending = snapshot_pending_after_change(actor, &[]);
    flush_pending(
        actor,
        pending,
        offsets_log,
        coordinator,
        super::chrono_now_ms(),
    )
    .await
}

/// Snapshots the full actor state into a `StreamsGroupSeed` for the cache and
/// for a respawned actor. The result matches what bootstrap replay produces.
pub(super) fn snapshot_seed(actor: &ActorState) -> StreamsGroupSeed {
    let state = &actor.state;
    crate::coordinator::unified::seeds::snapshot_member_maps! { state;
        members, current_per_member, target_per_member;
        member_metadata_value, current_assignment_value; |mid, m| {
            if let Some(tv) = target_member_value(state, mid) {
                target_per_member.insert(mid.clone(), tv);
            }
        }
    }
    StreamsGroupSeed {
        group_epoch: state.group_epoch,
        metadata_hash: actor.metadata_hash,
        description_epochs: actor.description_epochs,
        assignment_epoch: state.target.epoch,
        topology: actor.topology.clone(),
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
    actor.metadata_hash = seed.metadata_hash;
    actor.description_epochs = seed.description_epochs;
    state.target.epoch = seed.assignment_epoch;
    state.assignment_epoch = seed.assignment_epoch;
    if let Some(topology) = &seed.topology {
        state.topology = Some(StoredTopologyHandle {
            epoch: topology.epoch,
        });
        state.topology_epoch = topology.epoch;
    }
    actor.topology = seed.topology;

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
    crate::coordinator::unified::seeds::hydrate_member_epochs!(state, seed; m, cur {
            m.assignment_state = cur.state.into();
            m.active = cur.active;
            m.standby = cur.standby;
            m.warmup = cur.warmup;
            m.active_pending_revocation = cur.active_pending_revocation;
            m.standby_pending_revocation = cur.standby_pending_revocation;
            m.warmup_pending_revocation = cur.warmup_pending_revocation;
            // The member got this assignment before the load.
            m.sent_tasks = [m.active.clone(), m.standby.clone(), m.warmup.clone()];
    });
    for (mid, tv) in seed.target_per_member {
        if !tv.active.is_empty() {
            state.target.active.insert(mid.clone(), tv.active);
        }
        if !tv.standby.is_empty() {
            state.target.standby.insert(mid.clone(), tv.standby);
        }
        if !tv.warmup.is_empty() {
            state.target.warmup.insert(mid, tv.warmup);
        }
    }
    state.arm_loaded_rebalance_timeouts(std::time::Instant::now());
    state.phase = if actor.topology.is_none() {
        StreamsGroupStatePhase::NotReady
    } else {
        StreamsGroupStatePhase::Reconciling
    };
    state.refresh_phase();
    state.dirty = false;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use assert2::check;

    use super::*;
    use crate::coordinator::unified::streams::persistence::{
        DescriptionEpochs, StreamsGroupTopologyValue,
    };

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
            crate::coordinator::unified::test_support::stable_streams_assignment(
                (4, 3),
                maplit::btreemap! {"0".to_string() => vec![0, 1]},
            ),
        );
        let mut target = std::collections::HashMap::new();
        target.insert(
            "m1".to_string(),
            StreamsGroupTargetAssignmentMemberValue {
                active: maplit::btreemap! {"0".to_string() => vec![0, 1]},
                standby: BTreeMap::new(),
                warmup: BTreeMap::new(),
            },
        );
        let seed = StreamsGroupSeed {
            group_epoch: 4,
            metadata_hash: 11,
            description_epochs: DescriptionEpochs {
                stored: 2,
                failed: -1,
            },
            assignment_epoch: 4,
            topology: Some(StreamsGroupTopologyValue {
                epoch: 2,
                subtopologies: vec![],
            }),
            members,
            target_per_member: target,
            current_per_member: current,
        };
        apply_seed(&mut actor, seed);

        check!(actor.state.group_epoch == 4);
        check!(actor.metadata_hash == 11);
        check!(
            actor.description_epochs
                == DescriptionEpochs {
                    stored: 2,
                    failed: -1,
                }
        );
        check!(actor.state.target.epoch == 4);
        check!(actor.state.topology_epoch == 2);
        let m = actor.state.members.get("m1").expect("member restored");
        check!(m.member_epoch == 4);
        check!(m.previous_member_epoch == 3);
        check!(m.process_id == "p1");
        check!(m.active == maplit::btreemap! {"0".to_string() => vec![0, 1]});
        check!(
            actor.state.target.active["m1"] == maplit::btreemap! {"0".to_string() => vec![0, 1]}
        );
        check!(actor.state.phase == StreamsGroupStatePhase::Stable);
    }
}
