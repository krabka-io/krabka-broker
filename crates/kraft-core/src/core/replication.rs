//! Serving and answering `Fetch`: follower progress, the high watermark,
//! and the divergence hint that makes a follower truncate.
//!
//! Fetch is the only replication verb in KIP-595, so the leader side, the
//! follower side, and the timer that fires when a fetch stops arriving all
//! belong to one module.

use super::QuorumStateMachine;
use crate::{
    action::{Action, TimerKind},
    role::Role,
    types::{Epoch, LogOffsetMetadata, LogView, NodeId, SimInstant},
};

#[cfg(test)]
mod tests;

impl QuorumStateMachine {
    /// Leader side: a follower fetched at `fetch_offset` and claims that it
    /// last replicated up to `fetch_epoch`.
    ///
    /// If the follower's claimed epoch extends past where that epoch ends in
    /// our log, the logs diverged. This method then replies with the truncation
    /// point. If the logs agree, it records the follower's progress and
    /// advances the HWM.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, from = from.0, fetch_epoch, fetch_offset)
    )]
    pub(super) fn handle_fetch(
        &mut self,
        log: &dyn LogView,
        from: NodeId,
        fetch_epoch: Epoch,
        fetch_offset: i64,
    ) -> Vec<Action> {
        // Only a leader tracks follower progress / serves divergence hints.
        if !self.role.is_leader() {
            return Vec::new();
        }
        // Divergence check: if the follower claims to have replicated `fetch_epoch`
        // beyond where that epoch ends in our log, it must truncate.
        if fetch_offset > 0
            && let Some(div_end) = log.end_offset_for_epoch(fetch_epoch)
            && fetch_offset > div_end
        {
            return vec![Action::TruncateTo(LogOffsetMetadata {
                offset: div_end,
                epoch: fetch_epoch,
            })];
        }
        // Consistent: record the follower's fetch offset and recompute the HWM.
        let log_end = log.end_offset();
        if let Role::Leader { replicas, .. } = &mut self.role
            && let Some(progress) = replicas.get_mut(&from)
        {
            progress.fetch_offset = fetch_offset;
        }
        let new_hwm = self.recompute_high_watermark(log_end);
        if let Role::Leader { high_watermark, .. } = &mut self.role
            && new_hwm > *high_watermark
        {
            *high_watermark = new_hwm;
            return vec![Action::AdvanceHighWatermark(new_hwm)];
        }
        Vec::new()
    }

    /// The HWM as the `majority()`-th largest match offset across the leader's
    /// own log end and every follower's acknowledged fetch offset.
    ///
    /// The current leader epoch gates the result (Raft Fig.8 and KIP-595 leader
    /// completeness): the HWM may only advance once a *current-epoch* entry has
    /// been majority-replicated. This method approximates that rule. It requires
    /// the majority offset to be strictly past `epoch_start_offset`, where this
    /// leader's first current-epoch record sits. In every other case the HWM
    /// stays unchanged. The HWM never regresses.
    ///
    /// Full per-offset epoch validation happens against the durable log. The
    /// core tracks `epoch_start_offset` as its in-memory stand-in.
    fn recompute_high_watermark(&self, log_end: i64) -> i64 {
        let Role::Leader {
            replicas,
            high_watermark,
            epoch_start_offset,
        } = &self.role
        else {
            return 0;
        };
        // Clamp inputs into the verified kernel's precondition domain: a
        // follower's acknowledged offset never legitimately exceeds the
        // leader's log end, and the leader's HWM is always within its log.
        // Both are invariants of correct operation; clamping makes them
        // locally evident instead of a distributed assumption.
        let mut follower_offsets: Vec<i64> = replicas
            .values()
            .map(|progress| progress.fetch_offset.min(log_end))
            .collect();
        let new_hwm = if self.is_voter() {
            krabka_verified::recompute_high_watermark(
                log_end,
                &follower_offsets,
                self.state.majority(),
                *epoch_start_offset,
                (*high_watermark).min(log_end),
            )
        } else {
            // A leader removed by its own VotersRecord continues serving Fetch
            // until the record commits, but its local log cannot count toward
            // the new configuration's majority.
            follower_offsets.sort_unstable_by(|a, b| b.cmp(a));
            follower_offsets
                .get(self.state.majority().saturating_sub(1))
                .copied()
                .filter(|offset| *offset > *epoch_start_offset)
                .unwrap_or(*high_watermark)
                .max(*high_watermark)
        };
        debug_assert!(
            new_hwm <= log_end,
            "HWM {new_hwm} must not exceed leader log end {log_end}"
        );
        new_hwm
    }

    /// Follower side: the leader answered our Fetch.
    ///
    /// A diverging hint means that we must truncate. Without a hint, we re-arm
    /// the fetch timer and fetch again.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, leader_id = leader_id.0, diverging = diverging.is_some())
    )]
    pub(super) fn handle_fetch_response(
        &mut self,
        leader_id: NodeId,
        _leader_epoch: Epoch,
        diverging: Option<LogOffsetMetadata>,
        now: SimInstant,
    ) -> Vec<Action> {
        if let Some(point) = diverging {
            return vec![Action::TruncateTo(point)];
        }
        let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
        vec![
            Action::SendFetch { leader_id },
            Action::ResetTimer {
                kind: TimerKind::Fetch,
                deadline: fetch_deadline,
            },
        ]
    }

    /// The fetch timer fired: a follower or observer lost contact with the leader.
    ///
    /// A voter starts an election. An observer continues to look for a leader.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, is_voter = self.is_voter())
    )]
    pub(super) fn handle_fetch_timeout(
        &mut self,
        log: &dyn LogView,
        now: SimInstant,
    ) -> Vec<Action> {
        if self.is_voter() {
            self.start_election(log, now)
        } else {
            Vec::new()
        }
    }
}
