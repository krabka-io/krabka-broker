//! Conversion between the live share-group state and the [`ShareGroupSeed`]
//! that bootstrap replay produces. Both directions of that round trip live
//! together so a change to one stays matched by the other.

use std::collections::{HashMap, HashSet};

use krabka_protocol::primitives::uuid::Uuid;

use super::records::state_partition_metadata_from;
use crate::coordinator::unified::{
    ShareGroupSeed,
    share::{
        persistence::{
            ShareGroupCurrentMemberAssignmentValue, ShareGroupMemberMetadataValue,
            ShareGroupTargetAssignmentMemberValue,
        },
        state::{ShareGroupState, ShareMemberState},
    },
};

pub(super) fn apply_seed(state: &mut ShareGroupState, seed: ShareGroupSeed) {
    state.group_epoch = seed.group_epoch;
    state.target.epoch = seed.target_epoch;
    for (mid, meta) in seed.members {
        let subs: HashSet<String> = meta.subscribed_topic_names.into_iter().collect();
        let mut m = ShareMemberState::joining(mid.clone(), meta.client_id, meta.client_host, subs);
        m.rack_id = meta.rack_id;
        state.members.insert(mid, m);
    }
    for (mid, cur) in seed.current_per_member {
        if let Some(m) = state.members.get_mut(&mid) {
            m.member_epoch = cur.member_epoch;
            for (tid, parts) in cur.assigned_partitions {
                m.assigned_partitions.insert(tid, parts);
            }
        }
    }
    for (mid, tv) in seed.target_per_member {
        let entry: HashMap<Uuid, Vec<i32>> = tv.topic_partitions.into_iter().collect();
        state.target.per_member.insert(mid, entry);
    }
    // KIP-932: rehydrate the already-Initialized share-state set so the
    // lifecycle hook skips partitions whose state survived the restart.
    state.initialized.clear();
    for (topic_id, partitions) in &seed.state_partition_metadata.initialized {
        let tid = Uuid(*topic_id.as_bytes());
        for p in partitions {
            state.initialized.insert((tid, *p));
        }
    }
    state.dirty = false;
}

/// Snapshot a `ShareGroupState` into a `ShareGroupSeed` that can restore
/// a freshly-respawned actor. It mirrors what bootstrap replay produces.
pub(super) fn snapshot_seed(state: &ShareGroupState) -> ShareGroupSeed {
    let mut members = HashMap::new();
    let mut target_per_member = HashMap::new();
    let mut current_per_member = HashMap::new();
    for (mid, m) in &state.members {
        members.insert(
            mid.clone(),
            ShareGroupMemberMetadataValue {
                rack_id: m.rack_id.clone(),
                client_id: m.client_id.clone(),
                client_host: m.client_host.clone(),
                subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
            },
        );
        current_per_member.insert(
            mid.clone(),
            ShareGroupCurrentMemberAssignmentValue {
                member_epoch: m.member_epoch,
                assigned_partitions: m
                    .assigned_partitions
                    .iter()
                    .map(|(tid, parts)| (*tid, parts.clone()))
                    .collect(),
            },
        );
        if let Some(target) = state.target.per_member.get(mid) {
            target_per_member.insert(
                mid.clone(),
                ShareGroupTargetAssignmentMemberValue {
                    topic_partitions: target
                        .iter()
                        .map(|(tid, parts)| (*tid, parts.clone()))
                        .collect(),
                },
            );
        }
    }
    ShareGroupSeed {
        group_epoch: state.group_epoch,
        target_epoch: state.target.epoch,
        members,
        target_per_member,
        current_per_member,
        // KIP-932 lifecycle: project the live Initialized set back into the
        // persisted record so the cache (and a respawned actor) stay consistent
        // with what the lifecycle hook wrote to the log.
        state_partition_metadata: state_partition_metadata_from(state),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn snapshot_seed_round_trips_through_apply() {
        // A populated state → snapshot_seed → apply_seed reconstructs members,
        // epochs, and assignments (the bootstrap-replay invariant).
        let id = Uuid([7; 16]);
        let mut state = ShareGroupState::new("g");
        let mut m = ShareMemberState::joining(
            "m1",
            "c1",
            "/127.0.0.1",
            ["t".to_string()].into_iter().collect(),
        );
        m.member_epoch = 3;
        m.assigned_partitions.insert(id, vec![0, 1]);
        state.members.insert("m1".into(), m);
        state.group_epoch = 3;
        state.target.epoch = 3;
        state
            .target
            .per_member
            .insert("m1".into(), [(id, vec![0, 1])].into());

        let seed = snapshot_seed(&state);
        let mut restored = ShareGroupState::new("g");
        apply_seed(&mut restored, seed);

        assert!(restored.group_epoch == 3);
        assert!(restored.target.epoch == 3);
        let rm = restored.members.get("m1").expect("member restored");
        check!(rm.member_epoch == 3);
        check!(rm.assigned_partitions[&id] == vec![0, 1]);
        check!(restored.target.per_member["m1"][&id] == vec![0, 1]);
    }
}
