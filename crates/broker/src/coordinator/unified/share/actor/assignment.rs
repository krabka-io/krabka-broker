//! Recomputation of a share group's target assignment. It runs the share-group
//! assignor over the current membership and the latest metadata snapshot, and
//! it sits apart from the actor loop because it is pure, synchronous
//! state-machine work with no log or persister access.

use krabka_protocol::primitives::uuid::Uuid;

use crate::coordinator::unified::{
    actor::MetadataProvider,
    assignor::{MemberSubscription, TopicMetadata},
    reconciler::ReconcileInput,
    share::{
        assignor::ShareGroupAssignor,
        state::{ShareGroupState, ShareMemberState},
    },
};

/// Recompute the target assignment after a membership, subscription, or topic
/// metadata change. The assignment comparison is intentional: metadata has no
/// separate actor message, so a steady heartbeat must detect a changed topic
/// partition set without advancing the epoch when nothing changed.
pub(super) fn reconcile(state: &mut ShareGroupState, metadata: &dyn MetadataProvider) -> bool {
    let input = metadata.snapshot();
    let subscriptions: Vec<MemberSubscription> = state
        .members
        .values()
        .map(|m| MemberSubscription {
            member_id: m.member_id.clone(),
            rack_id: m.rack_id.clone(),
            subscribed_topic_ids: resolve_subscribed_topic_ids(m, &input),
        })
        .collect();
    let topics = TopicMetadata {
        partitions_per_topic: input.partitions_per_topic.clone(),
        partition_racks: input.partition_racks,
    };
    let assignment = ShareGroupAssignor.assign(&subscriptions, &topics);
    if !state.dirty && assignment == state.target.per_member {
        return true;
    }
    if !state.bump_epoch() {
        return false;
    }
    state.install_target(assignment);
    state.dirty = false;
    true
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

    #[test]
    fn clean_heartbeat_detects_metadata_change_and_exact_retry_is_stable() {
        let topic = Uuid([5; 16]);
        let mut state = ShareGroupState::new("g");
        state.add_or_update_member(ShareMemberState::joining(
            "m",
            "client",
            "host",
            HashSet::from(["t".to_owned()]),
        ));
        check!(reconcile(
            &mut state,
            &Metadata {
                topic,
                partitions: 1,
            },
        ));
        state.advance_member_epoch("m");
        check!(state.group_epoch == 1);

        // Membership is clean, but the metadata image grew the topic. The
        // adapter must install the new target and advance exactly once.
        check!(reconcile(
            &mut state,
            &Metadata {
                topic,
                partitions: 2,
            },
        ));
        check!(state.group_epoch == 2);
        assert!(state.target.per_member["m"][&topic] == vec![0, 1]);

        // An exact retry sees the installed target and does not churn epochs.
        check!(reconcile(
            &mut state,
            &Metadata {
                topic,
                partitions: 2,
            },
        ));
        assert!(state.group_epoch == 2);
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
