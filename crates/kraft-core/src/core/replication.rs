//! Serving and answering `Fetch`: follower progress, the high watermark,
//! and the divergence hint that makes a follower truncate.
//!
//! Fetch is the only replication verb in KIP-595, so the leader side, the
//! follower side, and the two timers that fire when a fetch stops arriving all
//! belong to one module: the follower's fetch watchdog, and the leader's
//! check-quorum window that makes it resign when the voters stop fetching.

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
        now: SimInstant,
    ) -> Vec<Action> {
        // Only a leader tracks follower progress / serves divergence hints.
        if !self.role.is_leader() {
            return Vec::new();
        }
        // The fetch is contact from that voter whatever it asks for, so score it
        // before the divergence check: a follower being told to truncate is
        // demonstrably still talking to us, and resigning the quorum over a
        // truncation round would cost an election for no reason.
        let mut actions: Vec<Action> = self.record_quorum_contact(from, now).into_iter().collect();
        // Divergence check: if the follower claims to have replicated `fetch_epoch`
        // beyond where that epoch ends in our log, it must truncate.
        if fetch_offset > 0
            && let Some(div_end) = log.end_offset_for_epoch(fetch_epoch)
            && fetch_offset > div_end
        {
            actions.push(Action::TruncateTo(LogOffsetMetadata {
                offset: div_end,
                epoch: fetch_epoch,
            }));
            return actions;
        }
        // Consistent: record the follower's fetch offset and recompute the HWM.
        let log_end = log.end_offset();
        if let Role::Leader { replicas, .. } = &mut self.role
            && let Some(progress) = replicas.get_mut(&from)
        {
            progress.fetch_offset = fetch_offset;
            progress.last_fetch = now;
            if fetch_offset >= log_end {
                progress.last_caught_up = now;
            }
        }
        let new_hwm = self.recompute_high_watermark(log_end);
        if let Role::Leader { high_watermark, .. } = &mut self.role
            && new_hwm > *high_watermark
        {
            *high_watermark = new_hwm;
            actions.push(Action::AdvanceHighWatermark(new_hwm));
        }
        actions
    }

    /// Leader side: a follower is catching up through KIP-630 `FetchSnapshot`.
    ///
    /// The request carries no fetch offset, so it moves no replication
    /// progress. It is still contact from that voter, and Kafka scores it for
    /// check-quorum exactly as it scores a Fetch
    /// (`KafkaRaftClient.handleFetchSnapshotRequest`). Without this, a leader
    /// whose only reachable follower is mid-snapshot would resign under a
    /// perfectly healthy quorum.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, from = from.0)
    )]
    pub(super) fn handle_fetch_snapshot(&mut self, from: NodeId, now: SimInstant) -> Vec<Action> {
        if !self.role.is_leader() {
            return Vec::new();
        }
        self.record_quorum_contact(from, now).into_iter().collect()
    }

    /// Score one Fetch or `FetchSnapshot` from `from` against the check-quorum
    /// window, re-arming the timer once a majority has been heard from.
    ///
    /// This is Kafka's `LeaderState.updateCheckQuorumForFollowingVoter`. The
    /// leader itself and any observer are ignored, because neither can be part
    /// of the quorum that keeps this leader alive. Reaching the threshold
    /// empties the set and starts a fresh window, so the set is only ever a
    /// partial tally of the *current* window.
    fn record_quorum_contact(&mut self, from: NodeId, now: SimInstant) -> Option<Action> {
        if !self.runs_check_quorum() || from == self.me || !self.state.voters.contains(from) {
            return None;
        }
        let threshold = self.check_quorum_reset_threshold();
        let deadline = self.check_quorum_deadline(now);
        let Role::Leader { fetched_voters, .. } = &mut self.role else {
            return None;
        };
        fetched_voters.insert(from);
        if fetched_voters.len() < threshold {
            return None;
        }
        fetched_voters.clear();
        Some(Action::ResetTimer {
            kind: TimerKind::CheckQuorum,
            deadline,
        })
    }

    /// The check-quorum timer fired: a majority of the voters has not fetched
    /// within the window, so this leader has lost the quorum and steps down.
    ///
    /// The timer is only ever re-armed by [`Self::record_quorum_contact`] once a
    /// majority has been heard from, so reaching its deadline is itself the
    /// proof that contact was lost -- exactly as `KafkaRaftClient.pollLeader`
    /// resigns on `timeUntilCheckQuorumExpires() == 0`.
    ///
    /// This is what stops an old leader isolated by a network partition from
    /// answering as leader for its epoch: with KIP-996 pre-vote it never sees a
    /// higher-epoch vote, so nothing else would ever tell it that the majority
    /// side has moved on.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, role = self.role.name())
    )]
    pub(super) fn handle_check_quorum_timeout(&mut self, now: SimInstant) -> Vec<Action> {
        if !self.role.is_leader() || !self.runs_check_quorum() {
            return Vec::new();
        }
        self.transition_to_resigned(now)
    }

    /// Step down from the leadership of the current epoch.
    ///
    /// The epoch is not bumped: this replica gives up the leadership it holds
    /// and asks the voters to elect, which is what `EndQuorumEpoch` means. The
    /// leader id is dropped so that `DescribeQuorum`, Metadata and the broker's
    /// controller-leader view all stop naming this node, and so that a peer's
    /// KIP-996 pre-vote (which only grants while no leader is believed in) can
    /// still be granted here.
    ///
    /// Where it lands depends on whether this replica is still a voter. A voter
    /// becomes `Resigned`, and the election timer carries it into the ordinary
    /// `start_election` path. A leader an uncommitted `VotersRecord` has already
    /// removed has no vote to elect with, and no future leader will announce
    /// itself to a node outside the voter set, so `Resigned` would strand it
    /// with every timer silent. It becomes a discovering observer instead, which
    /// is where [`QuorumStateMachine::finish_local_leader_removal`] leaves a
    /// removal that does commit.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(node = self.me.0, epoch = self.state.leader_epoch, is_voter = self.is_voter())
    )]
    fn transition_to_resigned(&mut self, now: SimInstant) -> Vec<Action> {
        let epoch = self.state.leader_epoch;
        self.state.leader_id = None;
        let timer = if self.is_voter() {
            self.role = Role::Resigned;
            Action::ResetTimer {
                kind: TimerKind::Election,
                deadline: self.election_deadline(now),
            }
        } else {
            let fetch_deadline = now.saturating_add_ms(self.election_timeout_ms);
            self.role = Role::Observer {
                leader_id: None,
                fetch_deadline,
            };
            Action::ResetTimer {
                kind: TimerKind::Fetch,
                deadline: fetch_deadline,
            }
        };
        vec![
            Action::SendEndQuorumEpoch { epoch },
            Action::PersistQuorumState,
            Action::TransitionedTo(self.role.name()),
            timer,
        ]
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
            ..
        } = &self.role
        else {
            return 0;
        };
        // Clamp inputs into the verified kernel's precondition domain: a
        // follower's acknowledged offset never legitimately exceeds the
        // leader's log end, and the leader's HWM is always within its log.
        // Both are invariants of correct operation; clamping makes them
        // locally evident instead of a distributed assumption.
        let follower_offsets: Vec<i64> = replicas
            .values()
            .map(|progress| progress.fetch_offset.min(log_end))
            .collect();
        // A leader removed by its own VotersRecord continues serving Fetch
        // until the record commits, but its local log cannot count toward the
        // new configuration's majority.
        let new_hwm = krabka_verified::recompute_high_watermark(
            log_end,
            &follower_offsets,
            self.state.majority(),
            *epoch_start_offset,
            (*high_watermark).min(log_end),
            self.is_voter(),
        );
        assert2::assert!(
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
