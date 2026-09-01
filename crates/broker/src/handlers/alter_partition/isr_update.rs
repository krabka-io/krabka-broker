//! Validation and application of one partition's ISR proposal.
//!
//! Each partition row of an `AlterPartition` request is decided on its own:
//! leader-epoch fencing, the non-empty-subset-of-replicas rule, and the
//! KIP-903 broker-epoch eligibility check run in that order, and the first
//! failure decides the row's error code. The row either contributes one
//! `PartitionRecord` change or an error response, so the whole per-row
//! decision belongs in one module.

use krabka_metadata::{MetadataRecord, PartitionRecord};
use krabka_protocol::{
    UnknownTaggedFields, owned::alter_partition_response::PartitionData as RespPartitionData,
};
use krabka_verified::isr::{IsrAdmission, isr_admission};

use crate::codes;

/// Validates and applies the ISR proposal of one partition. It returns the
/// per-partition response data, and on success it appends to `changes`.
///
/// `new_isr_i32` carries the v2 `new_isr` field, and `new_isr_with_epochs`
/// carries the v3 field. A v3 request leaves `new_isr` empty and fills
/// `new_isr_with_epochs`. When `new_isr` is empty, this function therefore
/// takes the broker IDs from `new_isr_with_epochs`.
pub(super) fn handle_partition(
    image: &krabka_metadata::MetadataImage,
    topic_name: Option<&str>,
    partition_index: i32,
    req_leader_epoch: i32,
    new_isr_i32: &[i32],
    new_isr_with_epochs: &[krabka_protocol::owned::alter_partition_request::BrokerState],
    changes: &mut Vec<MetadataRecord>,
) -> RespPartitionData {
    let Some(topic_name) = topic_name else {
        return error_part(
            partition_index,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            0,
            0,
            &[],
        );
    };
    let Some(part_rec) = image.partition(topic_name, partition_index) else {
        return error_part(
            partition_index,
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            0,
            0,
            &[],
        );
    };

    let leader_i32 = i32::try_from(part_rec.leader.0).unwrap_or(0);
    let current_isr_i32: Vec<i32> = part_rec
        .isr
        .iter()
        .map(|n| i32::try_from(n.0).unwrap_or(0))
        .collect();

    // Resolve the effective ISR from the request. Protocol v2 sends
    // `new_isr: Vec<i32>`; v3 sends `new_isr_with_epochs` instead and
    // leaves `new_isr` empty. Fall back to extracting broker_ids from
    // `new_isr_with_epochs` when the v2 field is absent.

    let fallback_isr_i32: Vec<i32>;
    let effective_isr_i32: &[i32] = if new_isr_i32.is_empty() && !new_isr_with_epochs.is_empty() {
        fallback_isr_i32 = new_isr_with_epochs.iter().map(|bs| bs.broker_id).collect();
        &fallback_isr_i32
    } else {
        new_isr_i32
    };

    // Validate proposed ISR: non-empty + subset of replicas.
    let proposed_isr: Option<Vec<krabka_metadata::NodeId>> = effective_isr_i32
        .iter()
        .map(|&n| u64::try_from(n).ok().map(krabka_metadata::NodeId))
        .collect();
    let replicas_set: std::collections::HashSet<krabka_metadata::NodeId> =
        part_rec.replicas.iter().copied().collect();
    let proposed_subset = proposed_isr
        .as_ref()
        .is_some_and(|isr| isr.iter().all(|n| replicas_set.contains(n)));

    // KIP-903: fence ineligible replicas. A broker in the proposed ISR is
    // ineligible if it is not currently registered, or if its stamped broker
    // epoch is non-sentinel (-1) and disagrees with the controller's
    // registration epoch. Any ineligible replica fails the whole partition.
    let replicas_eligible = new_isr_with_epochs.iter().all(|bstate| {
        let node = krabka_metadata::NodeId(u64::try_from(bstate.broker_id).unwrap_or(u64::MAX));
        let registered = image.broker_epoch(node);
        registered.is_some()
            && (bstate.broker_epoch == -1 || registered == Some(bstate.broker_epoch))
    });
    let proposed_isr = match isr_admission(
        req_leader_epoch == part_rec.leader_epoch,
        !effective_isr_i32.is_empty(),
        proposed_subset,
        replicas_eligible,
    ) {
        IsrAdmission::FencedLeaderEpoch => {
            return error_part(
                partition_index,
                codes::FENCED_LEADER_EPOCH,
                leader_i32,
                part_rec.leader_epoch.0,
                &current_isr_i32,
            );
        }
        IsrAdmission::InvalidProposal => {
            return error_part(
                partition_index,
                codes::INVALID_REQUEST,
                leader_i32,
                part_rec.leader_epoch.0,
                &current_isr_i32,
            );
        }
        IsrAdmission::IneligibleReplica => {
            return error_part(
                partition_index,
                codes::INELIGIBLE_REPLICA,
                leader_i32,
                part_rec.leader_epoch.0,
                &current_isr_i32,
            );
        }
        IsrAdmission::Admit => proposed_isr.expect("verified ISR proposal contains valid IDs"),
    };

    // Success: submit the ISR change.
    let new_partition_epoch = part_rec.partition_epoch + 1;
    changes.push(MetadataRecord::V1Partition(PartitionRecord {
        topic: topic_name.to_string(),
        partition: partition_index,
        leader: part_rec.leader,
        replicas: part_rec.replicas.clone(),
        isr: proposed_isr,
        leader_epoch: part_rec.leader_epoch,
        adding_replicas: part_rec.adding_replicas.clone(),
        removing_replicas: part_rec.removing_replicas.clone(),
        directories: part_rec.directories.clone(),
        partition_epoch: new_partition_epoch,
    }));

    RespPartitionData {
        partition_index,
        error_code: codes::NONE,
        leader_id: leader_i32,
        leader_epoch: part_rec.leader_epoch.0,
        isr: effective_isr_i32.to_vec(),
        leader_recovery_state: 0,
        partition_epoch: new_partition_epoch,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

fn error_part(
    partition_index: i32,
    error_code: i16,
    leader_id: i32,
    leader_epoch: i32,
    isr: &[i32],
) -> RespPartitionData {
    RespPartitionData {
        partition_index,
        error_code,
        leader_id,
        leader_epoch,
        isr: isr.to_vec(),
        leader_recovery_state: 0,
        partition_epoch: 0,
        unknown_tagged_fields: UnknownTaggedFields::default(),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::handlers::alter_partition::test_support::{
        PartitionFixture, bs, image_with, image_with_partition,
    };

    #[test]
    fn matching_epochs_succeed() {
        let image = image_with(&[(1, 10), (2, 20), (3, 30)]);
        let mut changes = Vec::new();
        let isr = vec![bs(1, 10), bs(2, 20), bs(3, 30)];
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(resp.error_code == codes::NONE, "got {}", resp.error_code);
        assert!(changes.len() == 1);
    }

    #[test]
    fn success_response_preserves_non_default_partition_fields() {
        let image = image_with_partition(
            &PartitionFixture {
                partition: 7,
                leader: 2,
                replicas: &[2, 4, 6],
                isr: &[2, 4],
                leader_epoch: 9,
                partition_epoch: 11,
            },
            &[(2, 20), (4, 40), (6, 60)],
        );
        let mut changes = Vec::new();
        let resp = handle_partition(&image, Some("t"), 7, 9, &[2, 4], &[], &mut changes);

        let expected = RespPartitionData {
            partition_index: 7,
            error_code: codes::NONE,
            leader_id: 2,
            leader_epoch: 9,
            isr: vec![2, 4],
            leader_recovery_state: 0,
            partition_epoch: 12,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(changes.len() == 1);
        let MetadataRecord::V1Partition(record) = &changes[0] else {
            panic!("wrong change variant");
        };
        assert!(record.partition == 7);
        assert!(record.partition_epoch == 12);
    }

    #[test]
    fn error_response_preserves_non_default_partition_fields() {
        let image = image_with_partition(
            &PartitionFixture {
                partition: 7,
                leader: 2,
                replicas: &[2, 4, 6],
                isr: &[2, 4],
                leader_epoch: 9,
                partition_epoch: 11,
            },
            &[(2, 20), (4, 40), (6, 60)],
        );
        let mut changes = Vec::new();
        let resp = handle_partition(&image, Some("t"), 7, 8, &[2, 4], &[], &mut changes);

        let expected = RespPartitionData {
            partition_index: 7,
            error_code: codes::FENCED_LEADER_EPOCH,
            leader_id: 2,
            leader_epoch: 9,
            isr: vec![2, 4],
            leader_recovery_state: 0,
            partition_epoch: 0,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(changes.is_empty());
    }

    #[test]
    fn explicit_v2_isr_wins_when_epoch_states_are_also_present() {
        let image = image_with(&[(1, 10), (2, 20), (3, 30)]);
        let mut changes = Vec::new();
        let resp = handle_partition(&image, Some("t"), 0, 5, &[1, 2], &[bs(3, 30)], &mut changes);
        let expected = RespPartitionData {
            partition_index: 0,
            error_code: codes::NONE,
            leader_id: 1,
            leader_epoch: 5,
            isr: vec![1, 2],
            leader_recovery_state: 0,
            partition_epoch: 1,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        assert!(changes.len() == 1);
        let MetadataRecord::V1Partition(record) = &changes[0] else {
            panic!("wrong change variant");
        };
        assert!(record.isr == vec![krabka_metadata::NodeId(1), krabka_metadata::NodeId(2)]);
    }

    #[test]
    fn stale_epoch_is_ineligible() {
        let image = image_with(&[(1, 10), (2, 20), (3, 30)]);
        let mut changes = Vec::new();
        let isr = vec![bs(1, 10), bs(2, 20), bs(3, 29)]; // 29 != image 30
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(
            resp.error_code == codes::INELIGIBLE_REPLICA,
            "got {}",
            resp.error_code
        );
        assert!(changes.is_empty());
    }

    #[test]
    fn unregistered_replica_is_ineligible() {
        let image = image_with(&[(1, 10), (2, 20)]); // broker 3 never registered
        let mut changes = Vec::new();
        let isr = vec![bs(1, 10), bs(2, 20), bs(3, -1)];
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(
            resp.error_code == codes::INELIGIBLE_REPLICA,
            "got {}",
            resp.error_code
        );
        assert!(changes.is_empty());
    }

    #[test]
    fn sentinel_epoch_skips_epoch_check() {
        let image = image_with(&[(1, 10), (2, 20), (3, 30)]);
        let mut changes = Vec::new();
        let isr = vec![bs(1, -1), bs(2, -1), bs(3, -1)]; // -1 = don't check
        let resp = handle_partition(&image, Some("t"), 0, 5, &[], &isr, &mut changes);
        assert!(resp.error_code == codes::NONE, "got {}", resp.error_code);
        assert!(changes.len() == 1);
    }

    #[test]
    fn v2_no_epochs_path_unaffected() {
        let image = image_with(&[(1, 10), (2, 20)]);
        let mut changes = Vec::new();
        // v2: new_isr populated, new_isr_with_epochs empty -> no epoch fencing.
        let resp = handle_partition(&image, Some("t"), 0, 5, &[1, 2, 3], &[], &mut changes);
        assert!(resp.error_code == codes::NONE, "got {}", resp.error_code);
        assert!(changes.len() == 1);
    }

    #[test]
    fn negative_replica_id_is_invalid_even_when_node_zero_is_a_replica() {
        let image = image_with_partition(
            &PartitionFixture {
                partition: 0,
                leader: 1,
                replicas: &[0, 1],
                isr: &[1],
                leader_epoch: 5,
                partition_epoch: 0,
            },
            &[(0, 0), (1, 10)],
        );
        let mut changes = Vec::new();
        let resp = handle_partition(&image, Some("t"), 0, 5, &[-1], &[], &mut changes);

        assert!(resp.error_code == codes::INVALID_REQUEST);
        assert!(changes.is_empty());
    }
}
