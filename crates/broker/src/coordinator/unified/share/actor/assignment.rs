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

/// Recompute the target assignment when the group is dirty. It builds the
/// assignor inputs from the latest metadata snapshot, runs the share-group
/// assignor, bumps the epoch, and installs the new target.
pub(super) fn reconcile(state: &mut ShareGroupState, metadata: &dyn MetadataProvider) {
    if !state.dirty {
        return;
    }
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
    state.bump_epoch();
    state.install_target(assignment);
    state.dirty = false;
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
