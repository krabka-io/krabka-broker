//! The controller's reaction to a broker that went down, and the state change
//! an election leaves behind.
//!
//! `do_failover` projects the model state onto a `PartitionRecord` and lets
//! the real `failover_one` — and, on the KIP-966 empty-ISR path, the real
//! `select_leader` — take the decision. `apply_elect` then applies it, and it
//! is the only place where committed data can be declared lost, so the loss
//! characterisation sits next to the election that causes it.
//!
//! # Two obligations, and only one of them is the ELR's
//!
//! `select_leader` reports an election as an eligible-leader-replica election
//! or as a most-complete-log one, and only the second answers `true` to
//! `ElectionBasis::loses_data`, the predicate the unclean-election counter,
//! the audit reason and KFC-9's `require` gate all read. The first is the
//! claim this model exists to check, and checking it needs the two durability
//! obligations kept apart.
//!
//! `committed` is everything that ever reached the HWM. It is the obligation a
//! consumer sees, and an unclean election can drop from it. `guarded` is the
//! prefix of `committed` that reached the HWM while the ISR still met
//! `min.insync.replicas` — the records an `acks=all` produce was acknowledged
//! for, since the produce gate refuses `acks=all` below min ISR. KIP-966's ELR
//! guarantee is about `guarded`, and only about `guarded`: a partition whose
//! ISR is under min ISR still takes `acks=1` writes and still advances its HWM
//! over them, and no replica outside the ISR is claimed to hold those.
//!
//! So an ELR election may shorten `committed`, and Kafka calls that election
//! clean; it may never shorten `guarded`, and that is the
//! `elr_election_keeps_every_guarded_record` property.

use std::collections::HashSet;

use krabka_metadata::MetadataImage;

use super::{
    bounds::{NB_U8, has, model_broker, model_offset, node},
    elr::{maintain, partition_record},
    state::{DpState, ELR_BEAT_LONGER_LOG, ELR_DROPPED_GUARDED, ELR_ELECTED},
};
use crate::{
    config_keys::RecoveryStrategy,
    leader_election::{FailoverDecision, failover_one},
    unclean_recovery::{ReplicaLogInfo, select_leader},
};

/// Apply a leader election. It sets the leader and ISR, bumps the epoch, and,
/// for an election that may lose data, characterizes any committed-data loss.
/// The new leader, which can be less complete, keeps only the committed prefix
/// that it holds with the same epoch. Any committed offset it lacks is LOST,
/// so the function flags it in `lost` and clamps the HWM to the new leader's
/// log. `guarded` is a prefix of `committed`, so it is clamped with it.
///
/// A clean election (`unclean == false`) never loses committed data, so there
/// is no truncation. An ELR election is a third case: it takes this path
/// because its winner need not be the most complete log, but it is the one
/// election here that must leave `guarded` whole. Rather than assume that, the
/// function returns how many guarded records the truncation dropped and lets
/// the caller record it.
fn apply_elect(s: &mut DpState, new_leader: u8, isr_mask: u8, unclean: bool) -> usize {
    s.leader = new_leader;
    s.isr = isr_mask;
    s.leader_epoch += 1;
    if !unclean {
        return 0;
    }
    let nl = &s.log[usize::from(new_leader)];
    let kept = s
        .committed
        .iter()
        .enumerate()
        .take_while(|&(off, e)| nl.get(off) == Some(e))
        .count();
    if kept >= s.committed.len() {
        return 0;
    }
    s.lost = true;
    s.committed.truncate(kept);
    s.hwm = s.hwm.min(model_offset(nl.len()));
    let guarded_dropped = s.guarded.len().saturating_sub(kept);
    s.guarded.truncate(kept);
    guarded_dropped
}

/// The controller's failover reaction when broker `dead` goes down. It drives
/// the real `failover_one` and applies that decision, which is a clean elect or
/// an ISR shrink. In the unclean config it also drives the real KIP-966
/// `select_leader` for the empty-ISR `Recover` path, over the eligible-leader
/// set the model has been maintaining with the real rule.
///
/// Every decision that moves the leader or the ISR is followed by [`maintain`],
/// because every controller path that submits such a change runs
/// [`ElrPublisher`](crate::elr::ElrPublisher) over it. `image` is `None` for a
/// configuration that maintains no ELR, whose set then stays empty and whose
/// recovery therefore always reaches the most-complete-log fallback.
pub(super) fn do_failover(image: Option<&MetadataImage>, s: &mut DpState, dead: u8, unclean: bool) {
    let pr = partition_record(s.leader, s.isr, s.leader_epoch);
    let alive: HashSet<krabka_audit::NodeId> = (0..NB_U8)
        .filter(|&b| has(s.live, b))
        .map(|b| krabka_audit::NodeId(node(b)))
        .collect();
    // Clean config: strategy None + unclean disabled → only ISR elections (else
    // Unavailable). Unclean config: Balanced strategy defers an empty-ISR
    // partition to KIP-966 offset-aware recovery.
    let strategy = if unclean {
        RecoveryStrategy::Balanced
    } else {
        RecoveryStrategy::None
    };
    // This model has no witness broker: every replica can lead.
    let witnesses: HashSet<krabka_audit::NodeId> = HashSet::new();
    let changed = match failover_one(
        &pr,
        krabka_audit::NodeId(node(dead)),
        &alive,
        &witnesses,
        // This model carries no ELR state: what it drives is the empty-ISR
        // `Recover` path and the real `select_best_replica` under it.
        &[],
        strategy,
        unclean,
    ) {
        FailoverDecision::Elect {
            leader,
            isr,
            unclean,
        } => {
            let isr_mask = isr
                .iter()
                .fold(0u8, |m, &n| m | (1u8 << (model_broker(n.0))));
            apply_elect(s, model_broker(leader.0), isr_mask, unclean);
            true
        }
        FailoverDecision::Recover(_) => recover(s, &witnesses),
        FailoverDecision::ShrinkIsr { isr } => {
            s.isr = isr
                .iter()
                .fold(0u8, |m, &n| m | (1u8 << (model_broker(n.0))));
            true
        }
        FailoverDecision::Unavailable | FailoverDecision::NoChange => false,
    };
    if let (true, Some(image)) = (changed, image) {
        maintain(image, s, &pr);
    }
}

/// KIP-966 offset-aware recovery: drive the REAL `select_leader` over the live
/// replicas' log info and the published eligible-leader set, and install the
/// winner with a singleton ISR.
///
/// The two rules `select_leader` orders differ in what they promise, so the
/// model records which one fired. A most-complete-log election is the audited
/// guess and may drop anything. An eligible-leader-replica election is the
/// claim, and the bits it leaves in `elr_trace` are what the properties read: that it happened at
/// all, that it really did pass over a longer surviving log, and — the one
/// that must never be set — that it dropped a record the partition had
/// acknowledged while it met min ISR.
///
/// Returns whether the partition changed, so the caller knows to republish.
fn recover(s: &mut DpState, witnesses: &HashSet<krabka_audit::NodeId>) -> bool {
    let infos: Vec<ReplicaLogInfo> = (0..NB_U8)
        .filter(|&b| has(s.live, b))
        .map(|b| ReplicaLogInfo {
            broker_id: krabka_audit::NodeId(node(b)),
            last_written_leader_epoch: s.log[usize::from(b)].last().map_or(0, |&e| i32::from(e)),
            log_end_offset: model_offset(s.log[usize::from(b)].len()),
            current_leader_epoch: i32::from(s.leader_epoch),
        })
        .collect();
    let eligible: Vec<i32> = (0..NB_U8)
        .filter(|&b| has(s.elr, b))
        .map(i32::from)
        .collect();
    // This model does carry an ISR, so give `select_leader` the real one rather
    // than an empty slice: its first rung elects an in-sync survivor cleanly,
    // and feeding it nothing would hide that rung from the model entirely.
    let in_sync: Vec<krabka_audit::NodeId> = (0..NB_U8)
        .filter(|&b| has(s.isr, b))
        .map(|b| krabka_audit::NodeId(node(b)))
        .collect();
    let Some(election) = select_leader(&infos, &in_sync, &eligible, witnesses) else {
        return false;
    };
    let winner = model_broker(election.leader.0);
    let winner_leo = model_offset(s.log[usize::from(winner)].len());
    let beat_longer = infos.iter().any(|i| i.log_end_offset > winner_leo);
    let guarded_dropped = apply_elect(s, winner, 1u8 << winner, true);
    // `loses_data` is the predicate itself: it is what the unclean-election
    // counter, the audit reason and KFC-9's `require` gate each read, so a
    // basis that answers `false` here is one production has already reported
    // as losing nothing.
    if !election.basis.loses_data() {
        s.elr_trace |= ELR_ELECTED;
        if beat_longer {
            s.elr_trace |= ELR_BEAT_LONGER_LOG;
        }
        if guarded_dropped > 0 {
            s.elr_trace |= ELR_DROPPED_GUARDED;
        }
    }
    true
}
