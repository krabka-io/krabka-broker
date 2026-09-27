//! Recomputation of a share group's target assignment. It runs the share-group
//! assignor over the current membership and the latest metadata snapshot, and
//! it sits apart from the actor loop because it is pure, synchronous
//! state-machine work with no log or persister access.

use std::collections::{BTreeMap, HashMap, HashSet};

use krabka_protocol::primitives::uuid::Uuid;

use crate::coordinator::unified::{
    actor::MetadataProvider,
    assignor::{
        GroupSpec, MemberSubscription, SubscriptionShape, TopicMetadata, subscription_type,
    },
    reconciler::ReconcileInput,
    share::{
        assignor::ShareGroupAssignor,
        state::{ShareGroupState, ShareMemberState},
    },
};

/// Bumps the group epoch and recomputes the target assignment when Kafka's
/// `GroupMetadataManager.shareGroupHeartbeat` would.
///
/// The epoch bumps when membership or a subscription changed, when the
/// subscribed topics changed in the metadata image (Kafka's metadata hash),
/// or when initialized partitions of a subscribed topic are not assigned yet
/// (`initializedAssignmentPending`). The target is recomputed whenever the
/// group epoch is ahead of the target epoch, from the previous target and
/// over the initialized partitions only (`withTopicAssignablePartitionsMap`).
/// Returns `false` when the group epoch is exhausted.
pub(super) fn reconcile(state: &mut ShareGroupState, metadata: &dyn MetadataProvider) -> bool {
    let input = metadata.snapshot();
    let subscribed = subscribed_metadata(state, &input);
    let metadata_changed = state.subscribed_metadata.as_ref() != Some(&subscribed);
    let pending = state.target.epoch >= state.group_epoch && initialized_assignment_pending(state);
    if (state.dirty || metadata_changed || pending) && !state.bump_epoch() {
        return false;
    }
    state.subscribed_metadata = Some(subscribed);
    state.dirty = false;
    if state.target.epoch >= state.group_epoch {
        return true;
    }

    let members: Vec<MemberSubscription> = state
        .members
        .values()
        .map(|m| MemberSubscription {
            member_id: m.member_id.clone(),
            rack_id: m.rack_id.clone(),
            subscribed_topic_ids: resolve_subscribed_topic_ids(m, &input),
            assigned_partitions: state
                .target
                .per_member
                .get(&m.member_id)
                .cloned()
                .unwrap_or_default(),
        })
        .collect();
    // Kafka's `ModernGroup.subscriptionType`: share groups subscribe by name
    // only.
    let shapes: Vec<SubscriptionShape<'_>> = state
        .members
        .values()
        .map(|m| SubscriptionShape {
            topic_names: &m.subscribed_topic_names,
            topic_regex: None,
            regex_topic_names: Vec::new(),
        })
        .collect();
    let group = GroupSpec {
        subscription_type: subscription_type(&shapes),
        members,
    };
    let topics = TopicMetadata {
        partitions_per_topic: input.partitions_per_topic,
        partition_racks: input.partition_racks,
    };
    let mut assignable: HashMap<Uuid, HashSet<i32>> = HashMap::new();
    for (topic_id, partition) in &state.initialized {
        assignable.entry(*topic_id).or_default().insert(*partition);
    }
    let assignment = ShareGroupAssignor.assign(&group, &topics, Some(&assignable));
    state.install_target(assignment);
    true
}

/// The subscribed topics of the group as the image shows them: each
/// subscribed name the image holds, with its topic id and partition count.
fn subscribed_metadata(
    state: &ShareGroupState,
    input: &ReconcileInput,
) -> BTreeMap<String, ([u8; 16], i32)> {
    state
        .members
        .values()
        .flat_map(|m| m.subscribed_topic_names.iter())
        .filter_map(|name| {
            let topic_id = input.topic_id_by_name.get(name)?;
            let partitions = input
                .partitions_per_topic
                .get(topic_id)
                .copied()
                .unwrap_or(0);
            Some((name.clone(), (topic_id.0, partitions)))
        })
        .collect()
}

/// Kafka's `GroupMetadataManager.initializedAssignmentPending`: whether a
/// subscribed topic has initialized partitions that differ from the ones the
/// target assigns.
fn initialized_assignment_pending(state: &ShareGroupState) -> bool {
    if state.members.is_empty() || state.initialized.is_empty() {
        return false;
    }
    let subscribed: HashSet<&str> = state
        .members
        .values()
        .flat_map(|m| m.subscribed_topic_names.iter().map(String::as_str))
        .collect();
    if subscribed.is_empty() {
        return false;
    }
    let mut assigned: HashMap<Uuid, HashSet<i32>> = HashMap::new();
    for member in state.target.per_member.values() {
        for (topic_id, partitions) in member {
            assigned
                .entry(*topic_id)
                .or_default()
                .extend(partitions.iter().copied());
        }
    }
    let mut initialized: HashMap<Uuid, HashSet<i32>> = HashMap::new();
    for (topic_id, partition) in &state.initialized {
        initialized.entry(*topic_id).or_default().insert(*partition);
    }
    initialized.iter().any(|(topic_id, partitions)| {
        state
            .topic_names
            .get(topic_id)
            .is_some_and(|name| subscribed.contains(name.as_str()))
            && assigned.get(topic_id) != Some(partitions)
    })
}

/// Resolve a share member's effective topic-id subscription. Share groups
/// support exact-name subscriptions only, with no regex, so this is a simple
/// name → id lookup against the current metadata.
fn resolve_subscribed_topic_ids(member: &ShareMemberState, input: &ReconcileInput) -> Vec<Uuid> {
    member
        .subscribed_topic_names
        .iter()
        .filter_map(|n| input.topic_id_by_name.get(n).copied())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use assert2::{assert, check};

    use super::{
        MetadataProvider, ReconcileInput, ShareGroupState, ShareMemberState, Uuid, reconcile,
    };

    #[derive(Debug)]
    struct Metadata {
        topic: Uuid,
        partitions: i32,
    }

    impl MetadataProvider for Metadata {
        fn snapshot(&self) -> ReconcileInput {
            ReconcileInput {
                topic_id_by_name: [("t".to_owned(), self.topic)].into(),
                partitions_per_topic: [(self.topic, self.partitions)].into(),
                ..Default::default()
            }
        }
    }

    /// Kafka's share heartbeat: the epoch bumps for a join, a metadata change
    /// and newly initialized partitions, only initialized partitions are
    /// assigned, and a retry with nothing new keeps the epoch.
    #[test]
    fn epoch_bumps_and_target_follow_initialized_partitions() {
        let topic = Uuid([5; 16]);
        let mut state = ShareGroupState::new("g");
        state.add_or_update_member(ShareMemberState::joining(
            "m",
            "client",
            "host",
            HashSet::from(["t".to_owned()]),
        ));
        // (step, partitions in the image, partition initialized before the
        // step, expected group epoch, expected target of m)
        let steps: [(&str, i32, Option<i32>, i32, Vec<i32>); 6] = [
            ("join with nothing initialized", 1, None, 1, vec![]),
            ("partition 0 initialized", 1, Some(0), 2, vec![0]),
            ("retry", 1, None, 2, vec![0]),
            ("topic grows", 2, None, 3, vec![0]),
            ("partition 1 initialized", 2, Some(1), 4, vec![0, 1]),
            ("retry after growth", 2, None, 4, vec![0, 1]),
        ];
        for (step, partitions, initialized, epoch, target) in steps {
            if let Some(partition) = initialized {
                state.mark_initialized((topic, partition));
                state.topic_names.insert(topic, "t".to_owned());
            }
            check!(
                reconcile(&mut state, &Metadata { topic, partitions }),
                "{step}"
            );
            check!(state.group_epoch == epoch, "{step}");
            let got = state
                .target
                .per_member
                .get("m")
                .and_then(|m| m.get(&topic))
                .cloned()
                .unwrap_or_default();
            check!(got == target, "{step}");
        }
    }

    #[test]
    fn metadata_change_fails_closed_at_epoch_limit() {
        let topic = Uuid([6; 16]);
        let mut state = ShareGroupState::new("g");
        state.group_epoch = i32::MAX;
        state.target.epoch = i32::MAX;
        state.members.insert(
            "m".to_owned(),
            ShareMemberState::joining("m", "client", "host", HashSet::from(["t".to_owned()])),
        );

        assert!(!reconcile(
            &mut state,
            &Metadata {
                topic,
                partitions: 1,
            },
        ));
        check!(state.group_epoch == i32::MAX);
        assert!(state.target.per_member.is_empty());
    }
}
