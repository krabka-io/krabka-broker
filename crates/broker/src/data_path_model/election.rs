//! The controller's reaction to a broker that went down, and the state change
//! an election leaves behind.
//!
//! `do_failover` projects the model state onto a `PartitionRecord` and lets
//! the real `failover_one` — and, on the KIP-966 empty-ISR path, the real
//! `select_best_replica` — take the decision. `apply_elect` then applies it,
//! and it is the only place where committed data can be declared lost, so the
//! loss characterisation sits next to the election that causes it.

use std::collections::HashSet;

use krabka_metadata::PartitionRecord;

use super::{
    bounds::{NB_U8, has, model_broker, model_offset, node},
    state::DpState,
};
use crate::{
    config_keys::RecoveryStrategy,
    leader_election::{FailoverDecision, failover_one},
    unclean_recovery::{ReplicaLogInfo, select_best_replica},
};

/// Apply a leader election. It sets the leader and ISR, bumps the epoch, and,
/// for an UNCLEAN election, characterizes any committed-data loss. The new
/// leader, which can be less complete, keeps only the committed prefix that it
/// holds with the same epoch. Any committed offset it lacks is LOST, so the
/// function flags it in `lost` and clamps the HWM to the new leader's log. A
/// clean election (`unclean == false`) never loses committed data, so there is
/// no truncation.
fn apply_elect(s: &mut DpState, new_leader: u8, isr_mask: u8, unclean: bool) {
    s.leader = new_leader;
    s.isr = isr_mask;
    s.leader_epoch += 1;
    if unclean {
        let nl = &s.log[usize::from(new_leader)];
        let kept = s
            .committed
            .iter()
            .enumerate()
            .take_while(|&(off, e)| nl.get(off) == Some(e))
            .count();
        if kept < s.committed.len() {
            s.lost = true;
            s.committed.truncate(kept);
            s.hwm = s.hwm.min(model_offset(nl.len()));
        }
    }
}

/// The controller's failover reaction when broker `dead` goes down. It drives
/// the real `failover_one` and applies that decision, which is a clean elect or
/// an ISR shrink. In the unclean config it also drives the real KIP-966
/// `select_best_replica` for the empty-ISR `Recover` path.
pub(super) fn do_failover(s: &mut DpState, dead: u8, unclean: bool) {
    let isr_nodes: Vec<krabka_audit::NodeId> = (0..NB_U8)
        .filter(|&b| has(s.isr, b))
        .map(|b| krabka_audit::NodeId(node(b)))
        .collect();
    let replica_nodes: Vec<krabka_audit::NodeId> =
        (0..NB_U8).map(|b| krabka_audit::NodeId(node(b))).collect();
    let pr = PartitionRecord {
        leader: krabka_audit::NodeId(node(s.leader)),
        replicas: replica_nodes,
        isr: isr_nodes,
        leader_epoch: krabka_metadata::LeaderEpoch(i32::from(s.leader_epoch)),
        ..Default::default()
    };
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
    match failover_one(
        &pr,
        krabka_audit::NodeId(node(dead)),
        &alive,
        &witnesses,
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
        }
        FailoverDecision::Recover(_) => {
            // KIP-966 unclean recovery: drive the REAL select_best_replica over
            // the live replicas' log info; the winner becomes leader with a
            // singleton ISR (may lose un-replicated committed data).
            let infos: Vec<ReplicaLogInfo> = (0..NB_U8)
                .filter(|&b| has(s.live, b))
                .map(|b| ReplicaLogInfo {
                    broker_id: krabka_audit::NodeId(node(b)),
                    last_written_leader_epoch: s.log[usize::from(b)]
                        .last()
                        .map_or(0, |&e| i32::from(e)),
                    log_end_offset: model_offset(s.log[usize::from(b)].len()),
                    current_leader_epoch: i32::from(s.leader_epoch),
                })
                .collect();
            if let Some(winner) = select_best_replica(&infos) {
                apply_elect(
                    s,
                    model_broker(winner.0),
                    1u8 << (model_broker(winner.0)),
                    true,
                );
            }
        }
        FailoverDecision::ShrinkIsr { isr } => {
            s.isr = isr
                .iter()
                .fold(0u8, |m, &n| m | (1u8 << (model_broker(n.0))));
        }
        FailoverDecision::Unavailable | FailoverDecision::NoChange => {}
    }
}
