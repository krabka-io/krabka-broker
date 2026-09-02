//! Per-row planning for `AlterPartitionReassignments`: it turns one alter row
//! into the `PartitionRecord` the controller submits, or into a wire error
//! code.
//!
//! The logic here is pure. The start path validates the requested target and
//! writes the union of the current and the target replica sets, and the cancel
//! path reverts an in-flight reassignment to the replicas it started from.

use std::collections::HashSet;

use krabka_metadata::{MetadataImage, PartitionRecord};
use krabka_raft::NodeId;

use crate::codes::{
    ELIGIBLE_LEADERS_NOT_AVAILABLE, INVALID_REPLICA_ASSIGNMENT, INVALID_REQUEST,
    NO_REASSIGNMENT_IN_PROGRESS, POLICY_VIOLATION, UNKNOWN_TOPIC_OR_PARTITION,
};

/// Per-row rejection: a Kafka wire error code and a readable message.
type RowError = (i16, String);

/// Process one (topic, partition, `target_opt`) row from an
/// `AlterPartitionReassignments` request.
///
/// `cancel_approved` is KFC-9's answer for this row: whether an approved
/// break-glass proposal covers a cancel of this partition. The caller resolves
/// it against the metadata image, so the per-row decision stays in this pure
/// function. It is `true` on a broker that gates nothing, and it is read only
/// on the cancel path.
///
/// The return values are:
///   - `Ok(Some(PartitionRecord))`: submit this intermediate record
///   - `Ok(None)`: do nothing, because the row is already at target or the
///     alter is empty
///   - `Err((wire_code, message))`: reject this row
pub(crate) fn process_one_partition(
    image: &MetadataImage,
    topic: &str,
    partition: i32,
    target: Option<&[i32]>,
    allow_rf_change: bool,
    cancel_approved: bool,
) -> Result<Option<PartitionRecord>, RowError> {
    let pr = image
        .partition(topic, partition)
        .ok_or((UNKNOWN_TOPIC_OR_PARTITION, "unknown partition".into()))?;

    match target {
        None => cancel_path(pr, cancel_approved),
        Some(target_slice) => {
            validate_target(target_slice, image, allow_rf_change, pr)?;
            start_path(pr, target_slice)
        }
    }
}

/// The row message that an unapproved cancel carries when the caller has no
/// refusal text of its own to put there.
///
/// The handler replaces it with the gate's own text, which names the proposal
/// that nearly authorized the cancel. This constant is what a caller that
/// resolved the gate elsewhere still gets.
const CANCEL_NEEDS_APPROVAL: &str = "a reassignment cancel needs an approved break-glass proposal";

fn validate_target(
    target: &[i32],
    image: &MetadataImage,
    allow_rf_change: bool,
    pr: &PartitionRecord,
) -> Result<(), RowError> {
    if target.is_empty() {
        return Err((INVALID_REPLICA_ASSIGNMENT, "empty target".into()));
    }
    // Duplicates.
    let mut seen: std::collections::HashSet<i32> = std::collections::HashSet::new();
    for &n in target {
        if !seen.insert(n) {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("duplicate replica {n}")));
        }
    }
    // Every node id must be a registered broker.
    for &n in target {
        let Ok(node_id) = u64::try_from(n) else {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("negative broker {n}")));
        };
        if image.broker(NodeId(node_id)).is_none() {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("unknown broker {n}")));
        }
    }
    // RF-change check.
    if !allow_rf_change {
        let current_target_len = pr
            .replicas
            .iter()
            .filter(|n| !pr.removing_replicas.contains(n))
            .count();
        if target.len() != current_target_len {
            return Err((
                INVALID_REPLICA_ASSIGNMENT,
                format!(
                    "rf change disallowed: target len {} != current target len {}",
                    target.len(),
                    current_target_len,
                ),
            ));
        }
    }
    Ok(())
}

fn cancel_path(pr: &PartitionRecord, approved: bool) -> Result<Option<PartitionRecord>, RowError> {
    // KFC-9: the two-person rule is an authority gate, so it answers before any
    // question about the partition's own state. Reading the reassignment first
    // would make "does this need an approval" depend on state that a
    // concurrent reassignment can change between the check and the append.
    if !approved {
        return Err((POLICY_VIOLATION, CANCEL_NEEDS_APPROVAL.into()));
    }
    if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
        return Err((NO_REASSIGNMENT_IN_PROGRESS, "nothing to cancel".into()));
    }
    let reverted_replicas: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let reverted_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let Some(leader) = eligible_leader(pr.leader, &reverted_replicas, &reverted_isr) else {
        return Err((
            ELIGIBLE_LEADERS_NOT_AVAILABLE,
            "no eligible leader after cancel".into(),
        ));
    };
    let leader_changes = leader != pr.leader;
    let (partition_epoch, leader_epoch) = crate::metadata_epoch::next_partition_change(
        pr.partition_epoch,
        pr.leader_epoch,
        leader_changes,
    )
    .ok_or((
        INVALID_REQUEST,
        "partition metadata epoch is exhausted".into(),
    ))?;
    if !krabka_verified::reassignment_plan_admission(
        (true, approved, true),
        (false, false),
        (true, true),
    ) {
        return Err((
            INVALID_REQUEST,
            "reassignment cancel admission failed".into(),
        ));
    }
    let new_directories =
        crate::reassignment::remap_directories(&pr.replicas, &pr.directories, &reverted_replicas);
    Ok(Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader,
        replicas: reverted_replicas,
        isr: reverted_isr,
        leader_epoch,
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: new_directories,
        partition_epoch,
    }))
}

fn start_path(pr: &PartitionRecord, target: &[i32]) -> Result<Option<PartitionRecord>, RowError> {
    let target_set: Vec<NodeId> = target
        .iter()
        .map(|&id| NodeId(u64::try_from(id).expect("target validated as non-negative")))
        .collect();
    let current_target: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.removing_replicas.contains(n))
        .copied()
        .collect();
    let mut new_replicas = Vec::new();
    let mut new = Vec::new();
    let mut old = Vec::new();
    let mut classified = HashSet::new();
    for &node in current_target.iter().chain(&target_set) {
        if !classified.insert(node) {
            continue;
        }
        let membership = krabka_verified::reassignment_set_membership(
            current_target.contains(&node),
            target_set.contains(&node),
        );
        if membership.in_union {
            new_replicas.push(node);
        }
        if membership.adding {
            new.push(node);
        }
        if membership.removing {
            old.push(node);
        }
    }
    if old.is_empty() && new.is_empty() {
        return Ok(None); // already at target — no-op
    }
    let Some(leader) = eligible_leader(pr.leader, &new_replicas, &pr.isr) else {
        return Err((
            ELIGIBLE_LEADERS_NOT_AVAILABLE,
            "no eligible leader for reassignment start".into(),
        ));
    };
    let leader_changes = leader != pr.leader;
    let new_directories =
        crate::reassignment::remap_directories(&pr.replicas, &pr.directories, &new_replicas);
    let (partition_epoch, leader_epoch) = crate::metadata_epoch::next_partition_change(
        pr.partition_epoch,
        pr.leader_epoch,
        leader_changes,
    )
    .ok_or((
        INVALID_REQUEST,
        "partition metadata epoch is exhausted".into(),
    ))?;
    if !krabka_verified::reassignment_plan_admission(
        (false, false, false),
        (true, true),
        (true, true),
    ) {
        return Err((
            INVALID_REQUEST,
            "reassignment start admission failed".into(),
        ));
    }
    Ok(Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader,
        replicas: new_replicas,
        isr: pr.isr.clone(),
        leader_epoch,
        adding_replicas: new,
        removing_replicas: old,
        directories: new_directories,
        partition_epoch,
    }))
}

fn eligible_leader(current: NodeId, replicas: &[NodeId], isr: &[NodeId]) -> Option<NodeId> {
    if replicas.contains(&current) && isr.contains(&current) {
        Some(current)
    } else {
        replicas.iter().find(|node| isr.contains(node)).copied()
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::LeaderEpoch;
    use uuid::Uuid;

    use super::*;
    use crate::handlers::alter_partition_reassignments::test_support::{img_with, img_with_epoch};

    #[test]
    fn validate_target_rejects_negative_broker_id() {
        let image = img_with(&[1], &[1], &[], &[], 1);
        let partition = image.partition("foo", 0).expect("seeded partition");
        let error = validate_target(&[-1], &image, true, partition).expect_err("negative broker");
        assert!(error.0 == INVALID_REPLICA_ASSIGNMENT);
        assert!(error.1.contains("negative broker"));
    }

    #[test]
    fn validate_target_rejects_duplicate_and_unknown_brokers() {
        let image = img_with(&[1], &[1], &[], &[], 1);
        let partition = image.partition("foo", 0).expect("seeded partition");
        for (target, message) in [
            (vec![1, 1], "duplicate replica"),
            (vec![99], "unknown broker"),
        ] {
            let error = validate_target(&target, &image, true, partition).expect_err(message);
            assert!(error.0 == INVALID_REPLICA_ASSIGNMENT);
            assert!(error.1.contains(message));
        }
    }

    #[test]
    fn noop_when_already_at_target() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2, 3]), true, true).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn start_writes_union_replicas() {
        let img = img_with_epoch(&[1, 2, 3], &[1, 2, 3], &[], &[], 1, 11);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 4]), true, true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5), // unchanged on start
            adding_replicas: vec![NodeId(4)],
            removing_replicas: vec![NodeId(2), NodeId(3)],
            directories: vec![Uuid::nil(); 4],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn start_rejects_an_exhausted_partition_epoch() {
        let image = img_with_epoch(&[1, 2, 3], &[1, 2, 3], &[], &[], 1, i32::MAX);
        let error = process_one_partition(&image, "foo", 0, Some(&[1, 4]), true, true)
            .expect_err("exhausted epoch must fail closed");
        assert!(error.0 == INVALID_REQUEST);
    }

    #[test]
    fn replaces_existing_in_flight_reassignment() {
        // Currently in flight: replicas=[1,2,3,4], adding=[4], removing=[2,3].
        // current_target = [1,4]. New alter target = [5,6].
        // Expected: replicas=[1,4,5,6], adding=[5,6], removing=[1,4].
        let img = img_with(&[1, 2, 3, 4], &[1, 2, 3], &[4], &[2, 3], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[5, 6]), true, true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(4), NodeId(5), NodeId(6)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![NodeId(5), NodeId(6)],
            removing_replicas: vec![NodeId(1), NodeId(4)],
            directories: vec![Uuid::nil(); 4],
            partition_epoch: 1,
        };
        assert!(res == expected);
    }

    #[test]
    fn replacing_reassignment_never_keeps_a_leader_outside_the_union() {
        let img = img_with_epoch(&[1, 2, 3, 4], &[1, 3, 4], &[4], &[1, 2], 1, 11);
        let res = process_one_partition(&img, "foo", 0, Some(&[4, 5]), true, true)
            .expect("replacement is valid")
            .expect("replacement mutates");

        assert!(res.replicas == vec![NodeId(3), NodeId(4), NodeId(5)]);
        assert!(res.adding_replicas == vec![NodeId(5)]);
        assert!(res.removing_replicas == vec![NodeId(3)]);
        assert!(res.leader == NodeId(3));
        assert!(res.isr.contains(&res.leader));
        assert!(res.leader_epoch == LeaderEpoch(6));
        assert!(res.partition_epoch == 12);
    }

    #[test]
    fn start_rejects_when_the_planned_union_has_no_isr_leader() {
        let img = img_with(&[1, 2, 3, 4], &[1, 2, 3], &[4], &[1, 2, 3], 1);
        let error = process_one_partition(&img, "foo", 0, Some(&[5]), true, true)
            .expect_err("union has no ISR member");

        assert!(error.0 == ELIGIBLE_LEADERS_NOT_AVAILABLE);
    }

    #[test]
    fn start_handoff_rejects_an_exhausted_leader_epoch_and_is_retryable() {
        let mut image = img_with(&[1, 2, 3, 4], &[1, 3, 4], &[4], &[1, 2], 1);
        let mut record = image.partition("foo", 0).expect("seeded partition").clone();
        record.leader_epoch = LeaderEpoch(i32::MAX);
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(record));

        for _ in 0..2 {
            let error = process_one_partition(&image, "foo", 0, Some(&[4, 5]), true, true)
                .expect_err("exhausted leader epoch must fail without mutation");
            assert!(error.0 == INVALID_REQUEST);
            assert!(image.partition("foo", 0).unwrap().leader == NodeId(1));
        }
    }

    #[test]
    fn rf_change_rejected_when_disabled() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[1, 2]), false, true).unwrap_err();
        assert!(err.0 == INVALID_REPLICA_ASSIGNMENT);
    }

    #[test]
    fn rf_change_allowed_when_enabled() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2]), true, true)
            .expect("ok")
            .expect("Some");
        assert!(res.removing_replicas == vec![NodeId(3)]);
    }

    #[test]
    fn rf_check_counts_current_target_without_removing_replicas() {
        let img = img_with(&[1, 2, 3, 4], &[1, 3, 4], &[4], &[2], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 3, 4]), false, true).expect("ok");

        assert!(res.is_none());
    }

    #[test]
    fn cancel_with_leader_in_adding_reverts_leader() {
        // After a successful leader handoff during reassignment, leader=4 (an adding replica).
        // Cancel: leader should revert to whoever in reverted replicas ∩ isr.
        // replicas=[1,2,3,4], adding=[4], removing=[2,3], leader=4, isr=[1,4].
        let img = img_with_epoch(&[1, 2, 3, 4], &[1, 4], &[4], &[2, 3], 4, 11);
        let res = process_one_partition(&img, "foo", 0, None, true, true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1), // reverted replicas ∩ isr = [1]
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1)],
            leader_epoch: LeaderEpoch(6), // bumped
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::nil(); 3],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn cancel_repairs_a_preserved_leader_outside_the_reverted_isr() {
        let img = img_with_epoch(&[1, 2, 3, 4], &[1, 4], &[4], &[3], 2, 11);
        let res = process_one_partition(&img, "foo", 0, None, true, true)
            .expect("cancel is valid")
            .expect("cancel mutates");

        assert!(res.replicas == vec![NodeId(1), NodeId(2), NodeId(3)]);
        assert!(res.isr == vec![NodeId(1)]);
        assert!(res.leader == NodeId(1));
        assert!(res.leader_epoch == LeaderEpoch(6));
    }

    #[test]
    fn leader_reverting_cancel_rejects_an_exhausted_leader_epoch() {
        let mut image = img_with(&[1, 2, 3, 4], &[1, 4], &[4], &[2, 3], 4);
        let mut record = image.partition("foo", 0).expect("seeded partition").clone();
        record.leader_epoch = LeaderEpoch(i32::MAX);
        image.apply(&krabka_metadata::MetadataRecord::V1Partition(record));

        let error = process_one_partition(&image, "foo", 0, None, true, true)
            .expect_err("exhausted leader epoch must fail closed");

        assert!(error.0 == INVALID_REQUEST);
    }

    #[test]
    fn cancel_with_only_removing_replicas_is_valid() {
        let img = img_with_epoch(&[1, 2, 3], &[1, 2, 3], &[], &[3], 1, 11);
        let res = process_one_partition(&img, "foo", 0, None, true, true)
            .expect("ok")
            .expect("Some");

        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::nil(); 3],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn empty_target_rejected() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[]), true, true).unwrap_err();
        assert!(err.0 == INVALID_REPLICA_ASSIGNMENT);
    }

    // ── KFC-9: the break-glass gate over a cancel ───────────────────

    #[test]
    fn an_unapproved_cancel_is_refused_before_any_question_about_the_partition() {
        let cases = [
            (
                "a reassignment is in progress",
                img_with(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1),
            ),
            (
                "nothing to cancel",
                img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1),
            ),
        ];
        for (label, img) in cases {
            let err = process_one_partition(&img, "foo", 0, None, true, false).unwrap_err();
            check!(err.0 == POLICY_VIOLATION, "case {label}");
            check!(err.1 == CANCEL_NEEDS_APPROVAL, "case {label}");
        }
    }

    #[test]
    fn an_unknown_partition_still_answers_that_it_is_unknown() {
        // The gate never masks a request that names nothing.
        let img = MetadataImage::new(Uuid::nil());
        let err = process_one_partition(&img, "foo", 0, None, true, false).unwrap_err();
        check!(err.0 == UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn a_start_is_never_gated() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2, 4]), true, false)
            .expect("a start needs no approval")
            .expect("Some");
        check!(res.adding_replicas == vec![NodeId(4)]);
    }
}
