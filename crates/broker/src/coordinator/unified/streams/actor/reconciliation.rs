//! Recomputation of a streams group's target assignment.
//!
//! Reconciliation is the one place that bumps the group epoch. It resolves the
//! stored topology against the current [`MetadataImage`], creates the internal
//! topics the topology needs, records any blocking topology status, and then
//! runs the assignor to produce the new active, standby, and warmup target.
//!
//! [`MetadataImage`]: krabka_metadata::MetadataImage

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use super::ActorState;
use crate::{
    coordinator::unified::streams::{
        assignor::{self, AssignorInput, AssignorMember},
        config::StreamsGroupConfig,
        persistence::StreamsGroupTopologyValue,
        state::{
            StreamsGroupState, StreamsGroupStatePhase, StreamsMemberState, StreamsTargetAssignment,
        },
        topology,
    },
    metadata_source::MetadataSource,
};

/// Recomputes the target assignment when the group is dirty.
///
/// With no connected [`MetadataSource`], as in the unit tests, or before any
/// member supplies a topology, the group stays `NotReady` with an empty
/// target. Members still advance their epoch but get no tasks.
///
/// Otherwise the function configures the topology against the current image
/// ([`topology::configure_topics`]), records the internal topics that the
/// heartbeat must create, and runs the assignor. A topology that cannot be
/// configured leaves the group `NotReady`: the heartbeat checks the topology
/// before it gets here, so that only a metadata change between the two checks
/// reaches that path.
pub(super) fn reconcile(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
) {
    if !actor.state.dirty {
        return;
    }
    let target_epoch = actor.state.target.epoch;
    reconcile_dirty(actor, config, metadata_source);
    if actor.state.target.epoch != target_epoch {
        actor.target_changed = true;
    }
}

/// Configures the topology of a seeded group against the current image
/// without a new target, as Kafka's first heartbeat after a load does.
///
/// The function records the topology status and the internal topics that the
/// image does not hold, so that the heartbeat asks for them again. It runs
/// once per actor, and a reconcile makes it unnecessary.
pub(super) fn configure_after_load(actor: &mut ActorState, source: &Arc<dyn MetadataSource>) {
    if actor.configured {
        return;
    }
    let Some(topology) = actor.topology.clone() else {
        return;
    };
    actor.configured = true;
    let image = source.current_image();
    // A topology that cannot be configured gets its error from the heartbeat
    // check, so it records nothing here.
    let Ok(configured) = topology::configure_topics(&topology, &image) else {
        return;
    };
    actor.creatable_topics = topology::internal_topic_specs(&configured);
    actor.state.status = configured.status;
}

fn reconcile_dirty(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
) {
    let (Some(source), Some(topology)) = (metadata_source, actor.topology.clone()) else {
        // No metadata source or no topology yet: cannot assign. Bump the epoch
        // and install an empty target so members still advance (to an empty
        // assignment) and the group sits in NotReady.
        install_empty_target(&mut actor.state, StreamsGroupStatePhase::NotReady);
        return;
    };

    let image = source.current_image();
    actor.configured = true;
    actor.metadata_hash = topology::metadata_hash(&topology, &image);
    actor.creatable_topics.clear();
    actor.partition_metadata = Some(topology::partition_metadata(&topology, &image));

    let configured = match topology::configure_topics(&topology, &image) {
        Ok(configured) => configured,
        Err(error) => {
            tracing::warn!(
                group_id = %actor.state.group_id,
                %error,
                "streams topology cannot be configured",
            );
            actor.state.status = None;
            install_empty_target(&mut actor.state, StreamsGroupStatePhase::NotReady);
            return;
        }
    };

    // The heartbeat hands the internal topics that the image does not hold to
    // `CreateTopics`, as Kafka's `KafkaApis` does.
    actor.creatable_topics = topology::internal_topic_specs(&configured);
    actor.state.status.clone_from(&configured.status);

    if !configured.is_ready() {
        install_empty_target(&mut actor.state, StreamsGroupStatePhase::NotReady);
        return;
    }

    // Build assignor inputs, compute the target, and install it.
    compute_and_install_target(actor, config, &topology, &configured.number_of_tasks());
}

/// Runs the assignor over the resolved topology and installs its output as the
/// new target.
///
/// The function bumps the group epoch and installs the target, which computes
/// the active revoke-split. It sets the phase to `Reconciling` while any
/// member still owns un-revoked active tasks, and to `Stable` otherwise.
pub(super) fn compute_and_install_target(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    topology: &StreamsGroupTopologyValue,
    num_tasks: &BTreeMap<String, i32>,
) {
    let members: Vec<AssignorMember> = actor
        .state
        .members
        .values()
        .map(|m| AssignorMember {
            member_id: m.member_id.clone(),
            process_id: m.process_id.clone(),
            rack_id: m.rack_id.clone(),
            current_active: m.active.clone(),
            current_standby: m.standby.clone(),
            current_warmup: m.warmup.clone(),
            task_lag: task_lag(m),
        })
        .collect();

    let stateful: BTreeSet<String> = topology
        .subtopologies
        .iter()
        .filter(|s| !s.state_changelog_topics.is_empty())
        .map(|s| s.subtopology_id.clone())
        .collect();

    let input = AssignorInput {
        tasks: topology::task_set(num_tasks),
        stateful,
        num_standby_replicas: config.num_standby_replicas,
        num_warmup_replicas: config.num_warmup_replicas,
        acceptable_recovery_lag: config.acceptable_recovery_lag,
        kind: config.assignor,
    };
    let assignment = assignor::assign(&members, &input);

    let target = StreamsTargetAssignment {
        epoch: 0,
        active: assignment.active,
        standby: assignment.standby,
        warmup: assignment.warmup,
    };
    if !actor.state.bump_epoch() {
        return;
    }
    actor.state.install_target(target);
    // A computed target ends `NotReady`; the members then reconcile toward it.
    actor.state.phase = StreamsGroupStatePhase::Reconciling;
    actor.state.refresh_phase();
    actor.state.dirty = false;
}

/// Bumps the group epoch, installs an empty target assignment, and moves the
/// group to `phase`. Members still advance to the new, empty assignment epoch
/// on their next `advance_member_epoch`. The function clears `dirty`.
fn install_empty_target(state: &mut StreamsGroupState, phase: StreamsGroupStatePhase) {
    if !state.bump_epoch() {
        return;
    }
    state.install_target(StreamsTargetAssignment::default());
    state.phase = phase;
    state.dirty = false;
}

/// Per-task changelog lag for the assignor: `end_offset - offset`, keyed by
/// `(subtopology, partition)`. The map holds an entry only where the member
/// reported both endpoints.
fn task_lag(m: &StreamsMemberState) -> BTreeMap<(String, i32), i64> {
    let mut lag = BTreeMap::new();
    for (key, &end) in &m.task_end_offsets {
        if let Some(&pos) = m.task_offsets.get(key) {
            // Lag is the delta between two offsets — a record count (i64),
            // compared against `acceptable_recovery_lag`, not an offset.
            lag.insert(key.clone(), end.0 - pos.0);
        }
    }
    lag
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_log::Offset;

    use super::*;

    #[test]
    fn task_lag_is_end_minus_offset_only_when_both_reported() {
        let mut m = StreamsMemberState::joining("m1", "client", "/127.0.0.1");
        // Two tasks with both endpoints reported → lag = end - offset.
        m.task_end_offsets = maplit::btreemap! {
        ("sub-a".to_string(), 0) => Offset(10),
        ("sub-a".to_string(), 1) => Offset(5),
        // A task with an end offset but NO reported position is dropped.
        ("sub-b".to_string(), 0) => Offset(99)};
        m.task_offsets = maplit::btreemap! {
        ("sub-a".to_string(), 0) => Offset(3),
        ("sub-a".to_string(), 1) => Offset(5)};
        let lag = task_lag(&m);
        // 10 - 3 = 7 (kills `-`→`+` which is 13, and `-`→`/` which is 3).
        check!(lag[&("sub-a".to_string(), 0)] == 7);
        // 5 - 5 = 0 (kills `-`→`/` which would be 1).
        check!(lag[&("sub-a".to_string(), 1)] == 0);
        // sub-b has no reported position, so it is absent (pins the filter and
        // kills the fixed-map replacements that inject sub-b / xyzzy keys).
        check!(!lag.contains_key(&("sub-b".to_string(), 0)));
        check!(lag.len() == 2);
    }
}
