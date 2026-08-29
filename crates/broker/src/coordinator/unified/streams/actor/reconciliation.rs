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
            StreamsGroupState, StreamsGroupStatePhase, StreamsMemberAssignmentState,
            StreamsMemberState, StreamsTargetAssignment,
        },
        topology::{self, status as topo_status},
    },
    metadata_source::MetadataSource,
};

/// Recomputes the target assignment when the group is dirty.
///
/// With no connected [`MetadataSource`], as in the unit tests, or before any
/// member supplies a topology, the group stays `NotReady` with an empty
/// target. Members still advance their epoch but get no tasks.
///
/// Otherwise the function validates the topology, derives the task counts and
/// the partition metadata, makes sure the internal topics exist, and runs the
/// assignor.
pub(super) async fn reconcile(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
) {
    if !actor.state.dirty {
        return;
    }

    let (Some(source), Some(topology)) = (metadata_source, actor.topology.clone()) else {
        // No metadata source or no topology yet: cannot assign. Bump the epoch
        // and install an empty target so members still advance (to an empty
        // assignment) and the group sits in NotReady.
        install_empty_target(&mut actor.state, StreamsGroupStatePhase::NotReady);
        return;
    };

    let image = source.current_image();

    // 1. Validation status (missing source / copartition mismatch).
    let mut status = topology::validate_topology(&topology, &image);

    // 2. Derive task counts + the external-topic partition snapshot.
    let derived = topology::derive_tasks(&topology, &image);
    actor.partition_metadata = Some(derived.partition_metadata.clone());

    // 3. Materialize required internal topics; any still-missing → status.
    let specs = topology::required_internal_topics(&topology, &derived.num_tasks);
    if !specs.is_empty() {
        match topology::ensure_internal_topics(
            source,
            &specs,
            config.internal_topic_replication_factor,
        )
        .await
        {
            Ok(still_missing) => {
                if !still_missing.is_empty() {
                    status.push((
                        topo_status::MISSING_INTERNAL_TOPICS,
                        format!(
                            "internal topics not yet created: {}",
                            still_missing.join(", ")
                        ),
                    ));
                }
            }
            Err(e) => {
                status.push((
                    topo_status::MISSING_INTERNAL_TOPICS,
                    format!("internal-topic creation failed: {e}"),
                ));
            }
        }
    }

    // Preserve any non-topology status (e.g. SHUTDOWN_APPLICATION) the actor
    // already recorded; topology-derived status replaces the rest.
    let preserved: Vec<(i8, String)> = actor
        .state
        .status
        .iter()
        .filter(|(c, _)| *c == topo_status::SHUTDOWN_APPLICATION)
        .cloned()
        .collect();

    let blocking = status.iter().any(|(c, _)| {
        *c == topo_status::MISSING_SOURCE_TOPICS
            || *c == topo_status::INCORRECTLY_PARTITIONED_TOPICS
            || *c == topo_status::MISSING_INTERNAL_TOPICS
    });

    status.extend(preserved);
    actor.state.status = status;

    if blocking {
        install_empty_target(&mut actor.state, StreamsGroupStatePhase::NotReady);
        return;
    }

    // 4. Build assignor inputs, compute the target, and install it.
    compute_and_install_target(actor, config, &topology, &derived.num_tasks);
}

/// Runs the assignor over the resolved topology and installs its output as the
/// new target.
///
/// The function bumps the group epoch and installs the target, which computes
/// the active revoke-split. It sets the phase to `Reconciling` while any
/// member still owns un-revoked active tasks, and to `Stable` otherwise.
fn compute_and_install_target(
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
    actor.state.bump_epoch();
    actor.state.install_target(target);

    let pending_revocation = actor
        .state
        .members
        .values()
        .any(|m| m.assignment_state == StreamsMemberAssignmentState::UnrevokedActiveTasks);
    actor.state.phase = if pending_revocation {
        StreamsGroupStatePhase::Reconciling
    } else {
        StreamsGroupStatePhase::Stable
    };
    actor.state.dirty = false;
}

/// Bumps the group epoch, installs an empty target assignment, and moves the
/// group to `phase`. Members still advance to the new, empty assignment epoch
/// on their next `advance_member_epoch`. The function clears `dirty`.
fn install_empty_target(state: &mut StreamsGroupState, phase: StreamsGroupStatePhase) {
    state.bump_epoch();
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
